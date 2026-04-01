"""
scripts/update_data.py
Rulat de GitHub Actions de 4x/zi.
- Descarcă ultimele 5 zile de la NASA FIRMS
- Filtrează punctele prin poligoanele UAT (uat.geojson) — o singură operație PIP
  care returnează simultan județul (cc) și UAT-ul (siruta), fără logică duală
- Deduplicare robustă prin seen_keys
- Actualizează fires_data.js, judete_timeseries.json, uat_stats.json, recent_fires.js

Strategia de atribuire județ+UAT (un singur pas):
  1. PIP exact pe toate cele 3186 poligoane UAT → județ + UAT garantat corect
  2. Fallback nearest-UAT cu threshold 0.11° (~11km) pentru puncte în apă/deltă/lacuri
     (0.11° acoperă Delta Dunării fără fals pozitive în Bulgaria/Ucraina)
  3. Dacă distanța > 0.11° → punct în afara României, ignorat

Avantaje față de varianta anterioară:
  - Județul e derivat din UAT (cc), nu dintr-un al doilea PIP pe judete_exact.geojson
  - Nu mai există discrepanțe județ vs UAT (aceeași geometrie, aceeași sursă)
  - Threshold calibrat pe cazuri reale din Delta Dunării
"""
import urllib.request, json, os, sys, hashlib
from datetime import datetime, timedelta, timezone
from shapely.geometry import Point, shape

API_KEY  = os.environ.get('FIRMS_API_KEY', '')
BBOX     = '20.26,43.62,29.72,48.27'
BASE_URL = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
DAYS     = 5
SOURCES  = {'VIIRS_SNPP_NRT':'SNPP','VIIRS_NOAA20_NRT':'NOAA20',
            'VIIRS_NOAA21_NRT':'NOAA21','MODIS_NRT':'MODIS'}

# Threshold nearest-UAT: 0.11° ≈ 11km
# Acoperă canale/lacuri/zone costiere din Delta Dunării (max ~10.4km distanță măsurată)
# Nu generează fals pozitive în Bulgaria (cel mai apropiat punct de graniță: ~14km)
NEAREST_THRESHOLD = 0.11

if not API_KEY:
    print('ERROR: FIRMS_API_KEY not set'); sys.exit(1)

import pathlib
os.chdir(pathlib.Path(__file__).parent.parent)
print(f'Working dir: {os.getcwd()}')

now_utc = datetime.now(timezone.utc)
today   = now_utc.date()
print(f'[{now_utc.isoformat()}] Start...')

# ── Încărcăm poligoanele UAT ───────────────────────────────────────────────────
# Sursa unică de adevăr: uat.geojson cu proprietăți: siruta, name, type, cc, cn
print('Încarc poligoanele UAT...')
with open('uat.geojson') as f:
    uat_gj = json.load(f)

UAT_POLYS = []
for feat in uat_gj['features']:
    if not feat.get('geometry'):
        continue
    props = feat['properties']
    UAT_POLYS.append((
        props['siruta'],   # cod SIRUTA unic per UAT
        props['name'],     # denumire UAT
        props['cc'],       # cod județ (2 litere)
        props['cn'],       # denumire județ
        shape(feat['geometry'])
    ))

# BBOX largit față de FIRMS (Delta Dunării ajunge la ~29.72°E, dar adăugăm margine)
RO_BBOX = (20.26, 43.62, 30.50, 48.27)
print(f'  {len(UAT_POLYS)} UAT-uri încărcate')

def find_uat(lat, lon):
    """
    Returnează (judet_code, uat_siruta, uat_name) sau None dacă punctul
    nu aparține României.

    Pași:
    1. Verificare rapidă BBOX
    2. PIP exact pe toate poligoanele UAT
    3. Nearest-UAT cu threshold NEAREST_THRESHOLD
    """
    if not (RO_BBOX[0] <= lon <= RO_BBOX[2] and RO_BBOX[1] <= lat <= RO_BBOX[3]):
        return None

    pt = Point(lon, lat)

    # 1. PIP exact
    for siruta, name, cc, cn, poly in UAT_POLYS:
        if poly.contains(pt):
            return (cc, siruta, name)

    # 2. Nearest cu threshold
    min_dist = float('inf')
    nearest  = None
    for siruta, name, cc, cn, poly in UAT_POLYS:
        d = pt.distance(poly)
        if d < min_dist:
            min_dist = d
            nearest  = (cc, siruta, name)

    if min_dist < NEAREST_THRESHOLD and nearest:
        return nearest

    return None  # Punct în afara României

def make_key(r):
    """Cheie unică per detecție — hash din atribute stabile."""
    raw = f"{round(r['lat'],3)}|{round(r['lon'],3)}|{r['date']}|{r['time']}|{r['source']}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]

# ── Funcții fetch/parse ───────────────────────────────────────────────────────
def fetch_csv(src, days):
    try:
        req = urllib.request.Request(
            f'{BASE_URL}/{API_KEY}/{src}/{BBOX}/{days}',
            headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read().decode('utf-8')
    except Exception as e:
        print(f'  WARN {src}: {e}'); return ''

def parse_csv(content, src_name):
    lines = [l.strip() for l in content.strip().split('\n') if l.strip()]
    if len(lines) < 2: return []
    hdr = [h.strip().upper() for h in lines[0].split(',')]
    out = []
    for line in lines[1:]:
        p = line.split(',')
        if len(p) < len(hdr): continue
        row = dict(zip(hdr, p))
        try:
            rec = {
                'lat':        float(row.get('LATITUDE', 0)),
                'lon':        float(row.get('LONGITUDE', 0)),
                'date':       row.get('ACQ_DATE', ''),
                'time':       str(row.get('ACQ_TIME','0000')).zfill(4),
                'source':     src_name,
                'satellite':  row.get('SATELLITE', ''),
                'frp':        float(row.get('FRP', 0) or 0),
                'brightness': float(row.get('BRIGHTNESS', row.get('BRIGHT_TI4',0)) or 0),
                'daynight':   row.get('DAYNIGHT', 'D'),
                'type':       int(row.get('TYPE', 0) or 0),
            }
            if rec['date'] and rec['lat'] and rec['lon']:
                out.append(rec)
        except: pass
    return out

# ── Citim datele existente ────────────────────────────────────────────────────
print('Citesc fișierele existente...')
with open('fires_data.json')        as f: fires     = json.load(f)
with open('judete_timeseries.json') as f: jt        = json.load(f)
with open('uat_stats.json')         as f: uat_stats = json.load(f)

seen_keys = set(fires.get('seen_keys', []))
print(f'  Detecții deja văzute: {len(seen_keys)}')

# ── Descărcare și filtrare ────────────────────────────────────────────────────
print(f'\nDescarc ultimele {DAYS} zile din {len(SOURCES)} surse...')
all_fetched = []
for src_key, src_name in SOURCES.items():
    recs = parse_csv(fetch_csv(src_key, DAYS), src_name)
    print(f'  {src_name}: {len(recs)} înregistrări brute')
    all_fetched.extend(recs)

# Filtrare: PIP pe UAT — returnează județ + UAT simultan
print('\nFiltrez prin poligoanele UAT...')
in_ro, out_ro = 0, 0
romania_records = []
for r in all_fetched:
    result = find_uat(r['lat'], r['lon'])
    if result:
        r['judet_code'], r['uat_siruta'], r['uat_name'] = result
        romania_records.append(r)
        in_ro += 1
    else:
        out_ro += 1
print(f'  În România: {in_ro} | În afara: {out_ro}')

# Deduplicare prin seen_keys
unique_new = []
for r in romania_records:
    k = make_key(r)
    if k not in seen_keys:
        seen_keys.add(k)
        r['_key'] = k
        unique_new.append(r)

print(f'  Noi (unice, nevăzute): {len(unique_new)}')

# ── Funcție salvare ───────────────────────────────────────────────────────────
def save_all(new_pts):
    sk_list = list(seen_keys)
    if len(sk_list) > 50000:
        sk_list = sk_list[-50000:]
    fires['seen_keys'] = sk_list

    with open('fires_data.json','w') as f:
        json.dump(fires, f, separators=(',',':'))
    with open('fires_data.js','w') as f:
        f.write('window.__FIRES__='+json.dumps(fires, separators=(',',':'))+';')
    with open('judete_timeseries.json','w') as f:
        json.dump(jt, f, separators=(',',':'))
    with open('judete_timeseries.js','w') as f:
        f.write('window.__JT__='+json.dumps(jt, separators=(',',':'))+';')
    with open('uat_stats.json','w') as f:
        json.dump(uat_stats, f, separators=(',',':'))

    # recent_fires.js
    cutoff = (today - timedelta(days=DAYS)).strftime('%Y-%m-%d')
    try:
        old_r = json.loads(
            open('recent_fires.js').read().replace('window.__RECENT__=','').rstrip(';'))
        old_pts = [p for p in old_r.get('points',[])
                   if p['date'] >= cutoff
                   and p.get('jud')
                   and len(p.get('jud','')) == 2]
    except:
        old_pts = []

    new_formatted = [{
        'lat':  round(r['lat'], 4),  'lon': round(r['lon'], 4),
        'date': r['date'],           'time': r['time'],
        'src':  r['source'],         'sat':  r['satellite'],
        'frp':  round(r['frp'], 1),  'dn':   r['daynight'],
        'jud':  r['judet_code'],
        'uat':  r.get('uat_name',''),
    } for r in new_pts]

    seen_r = set(); all_pts = []
    for p in new_formatted + old_pts:
        k = (p['lat'], p['lon'], p['date'], p['time'], p['src'])
        if k not in seen_r:
            seen_r.add(k); all_pts.append(p)
    all_pts.sort(key=lambda x: (x['date'], x['time']), reverse=True)

    obj = {
        'generated':    now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'days_covered': DAYS, 'count': len(all_pts), 'points': all_pts,
    }
    with open('recent_fires.js','w') as f:
        f.write('window.__RECENT__='+json.dumps(obj, separators=(',',':'))+';')

    print(f'  fires_data.json:        {os.path.getsize("fires_data.json")/1024:.0f} KB')
    print(f'  uat_stats.json:         {os.path.getsize("uat_stats.json")/1024:.0f} KB')
    print(f'  recent_fires.js:        {len(all_pts)} puncte')
    print(f'  seen_keys total:        {len(sk_list)}')

# ── Dacă nu avem date noi ─────────────────────────────────────────────────────
if not unique_new:
    print('\nNimic nou — actualizez timestamp și reconstruiesc judete_timeseries.')
    fires['kpis']['last_update'] = today.strftime('%Y-%m-%d')
    jt['by_jud_year']    = sorted(fires.get('judet_year',[]),
                                   key=lambda x: (x['JUDET_CODE'], x['YEAR']))
    jt['by_judet_total'] = fires.get('by_judet',[])
    jt['national_stats']['total_count'] = fires['kpis']['total']
    jt['national_stats']['mean_count']  = round(
        sum(j['count'] for j in jt['by_judet_total'])
        / max(len(jt['by_judet_total']),1), 1)
    save_all([])
    print(f'[{datetime.now(timezone.utc).isoformat()}] Done — 0 înregistrări noi')
    sys.exit(0)

# ── Actualizare fires_data ────────────────────────────────────────────────────
print(f'\nActualizez agregatele cu {len(unique_new)} detecții noi...')
fires['kpis']['total']       = fires['kpis'].get('total', 0) + len(unique_new)
fires['kpis']['last_update'] = today.strftime('%Y-%m-%d')

yr_src = {r['YEAR']: r for r in fires['by_year_src']}
hm_map = {(r['YEAR'], r['MONTH']): r['count'] for r in fires['heatmap']}
mo_map = {r['MONTH']: r for r in fires['by_month']}
gd_map = {}
for r in fires['grid']:
    k = (round(r['LAT_BIN'],1), round(r['LON_BIN'],1)); gd_map[k] = r
dn_map = {r['YEAR']: r for r in fires['daynight']}
fy_map = {r['year']: r for r in fires['frp_year']}

for r in unique_new:
    y = int(r['date'][:4]); m = int(r['date'][5:7])

    if y not in yr_src:
        yr_src[y] = {'YEAR':y,'MODIS':0,'SNPP':0,'NOAA20':0,'NOAA21':0}
    if r['source'] in yr_src[y]:
        yr_src[y][r['source']] += 1

    hm_map[(y,m)] = hm_map.get((y,m), 0) + 1

    if m in mo_map:
        old = mo_map[m]; n = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old.get('frp_max',0), r['frp']), 1)
        old['count']    = n + 1

    k = (round(r['lat']*10)/10, round(r['lon']*10)/10)
    if k in gd_map:
        old = gd_map[k]; n = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old['frp_max'], r['frp']), 1)
        old['frp_sum']  = round(old['frp_sum'] + r['frp'], 1)
        old['count']    = n + 1
    else:
        gd_map[k] = {'LAT_BIN':k[0],'LON_BIN':k[1],'count':1,
                     'frp_mean':round(r['frp'],2),'frp_max':round(r['frp'],1),
                     'frp_sum':round(r['frp'],1)}

    if y not in dn_map: dn_map[y] = {'YEAR':y,'D':0,'N':0}
    dn_map[y][r['daynight']] = dn_map[y].get(r['daynight'], 0) + 1

    if y in fy_map:
        fy_map[y]['frp_max'] = round(max(fy_map[y]['frp_max'], r['frp']), 1)
    else:
        fy_map[y] = {'year':y,'frp_mean':round(r['frp'],2),
                     'frp_sum':round(r['frp'],1),'frp_max':round(r['frp'],1)}

    fires['top_frp'].append({
        'ACQ_DATE':r['date'],'LATITUDE':r['lat'],'LONGITUDE':r['lon'],
        'FRP':r['frp'],'BRIGHTNESS':r['brightness'],'SOURCE':r['source'],
        'SATELLITE':r['satellite'],'DAYNIGHT':r['daynight'],
        'CONF_NUM':60,'TYPE':r['type'],'JUDET_NAME':r.get('judet_code','—'),
    })

fires['by_year_src'] = sorted(yr_src.values(), key=lambda x: x['YEAR'])
fires['heatmap']     = [{'YEAR':k[0],'MONTH':k[1],'count':v}
                        for k,v in sorted(hm_map.items())]
fires['by_month']    = sorted(mo_map.values(), key=lambda x: x['MONTH'])
fires['grid']        = list(gd_map.values())
fires['daynight']    = sorted(dn_map.values(), key=lambda x: x['YEAR'])
fires['frp_year']    = sorted(fy_map.values(), key=lambda x: x['year'])
fires['top_frp']     = sorted(fires['top_frp'],
                              key=lambda x: x['FRP'], reverse=True)[:20]

# ── Reconstruire judete_timeseries din fires_data ─────────────────────────────
# by_jud_year și by_judet_total — sursă unică: fires_data.json
jt['by_jud_year']    = sorted(fires.get('judet_year',[]),
                               key=lambda x: (x['JUDET_CODE'], x['YEAR']))
jt['by_judet_total'] = fires.get('by_judet',[])

# by_jud_month — păstrat incremental (nu există în fires_data)
jm_map = {(r['JUDET_CODE'],r['MONTH']): r for r in jt.get('by_jud_month',[])}
for r in unique_new:
    m    = int(r['date'][5:7])
    code = r.get('judet_code')
    if not code: continue
    km = (code, m)
    if km in jm_map:
        old = jm_map[km]; n = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['count']    = n + 1
    else:
        jm_map[km] = {'JUDET_CODE':code,'MONTH':m,
                      'count':1,'frp_mean':round(r['frp'],2)}
jt['by_jud_month']   = sorted(jm_map.values(),
                               key=lambda x: (x['JUDET_CODE'], x['MONTH']))
jt['national_stats']['total_count'] = fires['kpis']['total']
jt['national_stats']['mean_count']  = round(
    sum(j['count'] for j in jt['by_judet_total'])
    / max(len(jt['by_judet_total']),1), 1)

# ── Actualizare uat_stats ─────────────────────────────────────────────────────
# Atribuire directă prin uat_siruta din find_uat() — fără logică de aproximare
uat_map  = {u['UAT_SIRUTA']: u for u in uat_stats.get('by_uat',[])}
uaty_map = {(r['YEAR'],r['UAT_SIRUTA']): r for r in uat_stats.get('uat_year',[])}

for r in unique_new:
    y   = int(r['date'][:4])
    sir = r.get('uat_siruta')
    if not sir: continue

    if sir in uat_map:
        old = uat_map[sir]; n = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old.get('frp_max',0), r['frp']), 1)
        old['frp_sum']  = round(old.get('frp_sum',0) + r['frp'], 1)
        old['count']    = n + 1

    k = (y, sir)
    if k in uaty_map:
        old = uaty_map[k]; n = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['count']    = n + 1
    else:
        uaty_map[k] = {'YEAR':y,'UAT_SIRUTA':sir,'UAT_CC':r.get('judet_code',''),
                       'count':1,'frp_mean':round(r['frp'],2)}

uat_stats['by_uat']   = list(uat_map.values())
uat_stats['uat_year'] = list(uaty_map.values())

# ── Salvare finală ────────────────────────────────────────────────────────────
print('\nSalvez toate fișierele...')
save_all(unique_new)
print(f'\n[{datetime.now(timezone.utc).isoformat()}] Done!')
print(f'  Noi: {len(unique_new)} | Total: {fires["kpis"]["total"]:,}')
