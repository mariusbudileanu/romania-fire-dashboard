"""
scripts/update_data.py
Rulat de GitHub Actions zilnic.
Descarcă ultimele 5 zile de la NASA FIRMS, le adaugă la arhivă,
regenerează toate JSON-urile necesare dashboardului.
"""

import urllib.request
import json
import os
import sys
import io
from datetime import datetime, timedelta, timezone

# ── Configurare ────────────────────────────────────────────────────────────
API_KEY  = os.environ.get('FIRMS_API_KEY', '')
BBOX     = '20.26,43.62,29.72,48.27'
BASE_URL = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
DAYS     = 5  # maxim funcțional confirmat în teste

SOURCES = {
    'VIIRS_SNPP_NRT':  'SNPP',
    'VIIRS_NOAA20_NRT':'NOAA20',
    'VIIRS_NOAA21_NRT':'NOAA21',
    'MODIS_NRT':       'MODIS',
}

# Mapare SOURCE → TYPE_INSTRUMENT
SOURCE_INSTRUMENT = {
    'SNPP':   'VIIRS',
    'NOAA20': 'VIIRS',
    'NOAA21': 'VIIRS',
    'MODIS':  'MODIS',
}

if not API_KEY:
    print('ERROR: FIRMS_API_KEY not set')
    sys.exit(1)

print(f'[{datetime.now().isoformat()}] Încep actualizarea datelor...')

# ── Funcție fetch CSV ──────────────────────────────────────────────────────
def fetch_csv(source_key, days):
    url = f'{BASE_URL}/{API_KEY}/{source_key}/{BBOX}/{days}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode('utf-8')
    except Exception as e:
        print(f'  WARN: {source_key} fetch failed: {e}')
        return ''

def parse_csv(content, source_name):
    """Parsează CSV FIRMS și returnează lista de înregistrări."""
    lines = [l.strip() for l in content.strip().split('\n') if l.strip()]
    if len(lines) < 2:
        return []
    
    header = [h.strip().upper() for h in lines[0].split(',')]
    records = []
    
    for line in lines[1:]:
        parts = line.split(',')
        if len(parts) < len(header):
            continue
        row = dict(zip(header, parts))
        
        try:
            rec = {
                'lat':        float(row.get('LATITUDE', row.get('LAT', 0))),
                'lon':        float(row.get('LONGITUDE', row.get('LON', 0))),
                'date':       row.get('ACQ_DATE', ''),
                'time':       row.get('ACQ_TIME', ''),
                'source':     source_name,
                'satellite':  row.get('SATELLITE', ''),
                'frp':        float(row.get('FRP', 0) or 0),
                'brightness': float(row.get('BRIGHTNESS', row.get('BRIGHT_TI4', 0)) or 0),
                'confidence': row.get('CONFIDENCE', 'n'),
                'daynight':   row.get('DAYNIGHT', 'D'),
                'type':       int(row.get('TYPE', 0) or 0),
            }
            if rec['date'] and rec['lat'] and rec['lon']:
                records.append(rec)
        except (ValueError, KeyError):
            continue
    
    return records

# ── Scarcare date noi ──────────────────────────────────────────────────────
print(f'Descarc ultimele {DAYS} zile din {len(SOURCES)} surse...')
new_records = []
for source_key, source_name in SOURCES.items():
    content = fetch_csv(source_key, DAYS)
    records = parse_csv(content, source_name)
    print(f'  {source_name}: {len(records)} înregistrări')
    new_records.extend(records)

print(f'Total înregistrări noi: {len(new_records)}')

# ── Citim fires_data.json existent ────────────────────────────────────────
fires_path = 'fires_data.json'
with open(fires_path) as f:
    fires = json.load(f)

# ── Citim judete_timeseries.json ──────────────────────────────────────────
with open('judete_timeseries.json') as f:
    jt = json.load(f)

if not new_records:
    print('Nu există înregistrări noi — actualizez doar timestamp-ul.')
    fires['kpis']['last_update'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    with open(fires_path, 'w') as f:
        json.dump(fires, f, separators=(',',':'))
    print('Done.')
    sys.exit(0)

# ── Deduplicare: eliminăm înregistrări deja existente ─────────────────────
# Folosim (lat_round, lon_round, date, source) ca cheie unică
existing_keys = set()
for r in fires.get('top_frp', []):
    key = (round(float(r.get('LATITUDE',0)),3), round(float(r.get('LONGITUDE',0)),3),
           str(r.get('ACQ_DATE','')), str(r.get('SOURCE','')))
    existing_keys.add(key)

# Adăugăm cheile din grid (date agregat, nu putem deduplica exact, lăsăm)
new_unique = []
for r in new_records:
    key = (round(r['lat'],3), round(r['lon'],3), r['date'], r['source'])
    if key not in existing_keys:
        new_unique.append(r)
        existing_keys.add(key)

print(f'Înregistrări unice noi (după deduplicare): {len(new_unique)}')

if not new_unique:
    print('Toate înregistrările există deja.')
    fires['kpis']['last_update'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    with open(fires_path, 'w') as f:
        json.dump(fires, f, separators=(',',':'))
    sys.exit(0)

# ── Actualizăm agregatele ─────────────────────────────────────────────────

# 1. KPIs — total și last_update
fires['kpis']['total'] = fires['kpis'].get('total', 431009) + len(new_unique)
fires['kpis']['last_update'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

# 2. by_year_src — adăugăm la anul curent
year_src_map = {}
for row in fires['by_year_src']:
    year_src_map[row['YEAR']] = row

for r in new_unique:
    y = int(r['date'][:4])
    if y not in year_src_map:
        year_src_map[y] = {'YEAR': y, 'MODIS':0, 'SNPP':0, 'NOAA20':0, 'NOAA21':0}
    src = r['source']
    if src in year_src_map[y]:
        year_src_map[y][src] = year_src_map[y].get(src, 0) + 1

fires['by_year_src'] = sorted(year_src_map.values(), key=lambda x: x['YEAR'])

# 3. heatmap — luna × an
heatmap_map = {}
for row in fires['heatmap']:
    heatmap_map[(row['YEAR'], row['MONTH'])] = row['count']

for r in new_unique:
    try:
        d = datetime.strptime(r['date'], '%Y-%m-%d')
        key = (d.year, d.month)
        heatmap_map[key] = heatmap_map.get(key, 0) + 1
    except:
        pass

fires['heatmap'] = [{'YEAR': k[0], 'MONTH': k[1], 'count': v}
                    for k, v in sorted(heatmap_map.items())]

# 4. by_month — distribuție lunară globală
month_map = {row['MONTH']: row for row in fires['by_month']}
for r in new_unique:
    try:
        m = int(r['date'][5:7])
        if m in month_map:
            old_cnt = month_map[m]['count']
            old_frp = month_map[m]['frp_mean']
            new_cnt = old_cnt + 1
            month_map[m]['frp_mean'] = round((old_frp * old_cnt + r['frp']) / new_cnt, 2)
            month_map[m]['count']    = new_cnt
    except:
        pass
fires['by_month'] = sorted(month_map.values(), key=lambda x: x['MONTH'])

# 5. grid 0.1° — actualizăm celulele afectate
grid_map = {}
for row in fires['grid']:
    k = (round(row['LAT_BIN'],1), round(row['LON_BIN'],1))
    grid_map[k] = row

for r in new_unique:
    k = (round(r['lat']*10)/10, round(r['lon']*10)/10)
    if k in grid_map:
        old = grid_map[k]
        n   = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old['frp_max'], r['frp']), 1)
        old['frp_sum']  = round(old['frp_sum'] + r['frp'], 1)
        old['count']    = n + 1
    else:
        grid_map[k] = {
            'LAT_BIN': k[0], 'LON_BIN': k[1],
            'count': 1, 'frp_mean': round(r['frp'],2),
            'frp_max': round(r['frp'],1), 'frp_sum': round(r['frp'],1)
        }

fires['grid'] = list(grid_map.values())

# 6. frp_year — FRP mediu per an
frp_year_map = {row['year']: row for row in fires['frp_year']}
for r in new_unique:
    y = int(r['date'][:4])
    if y in frp_year_map:
        row = frp_year_year_map = frp_year_map[y]
        # Aproximare incrementală
        cnt = year_src_map.get(y, {})
        total_cnt = sum(cnt.get(s,0) for s in ['MODIS','SNPP','NOAA20','NOAA21'])
        if total_cnt > 0:
            row['frp_max'] = round(max(row['frp_max'], r['frp']), 1)

fires['frp_year'] = sorted(frp_year_map.values(), key=lambda x: x['year'])

# 7. daynight
dn_map = {}
for row in fires['daynight']:
    dn_map[row['YEAR']] = row

for r in new_unique:
    y = int(r['date'][:4])
    if y not in dn_map:
        dn_map[y] = {'YEAR': y, 'D': 0, 'N': 0}
    dn_map[y][r['daynight']] = dn_map[y].get(r['daynight'], 0) + 1

fires['daynight'] = sorted(dn_map.values(), key=lambda x: x['YEAR'])

# 8. top_frp — actualizăm dacă avem detecții cu FRP mare
if new_unique:
    # Adăugăm noile detecții la top_frp și păstrăm top 20
    for r in new_unique:
        fires['top_frp'].append({
            'ACQ_DATE':   r['date'],
            'LATITUDE':   r['lat'],
            'LONGITUDE':  r['lon'],
            'FRP':        r['frp'],
            'BRIGHTNESS': r['brightness'],
            'SOURCE':     r['source'],
            'SATELLITE':  r['satellite'],
            'DAYNIGHT':   r['daynight'],
            'CONF_NUM':   60,
            'TYPE':       r['type'],
            'JUDET_NAME': '—',
        })
    fires['top_frp'] = sorted(fires['top_frp'], key=lambda x: x['FRP'], reverse=True)[:20]

# ── Actualizăm judet_year în judete_timeseries ────────────────────────────
print('Actualizez judete_timeseries.json...')

# Nearest-centroid pentru județe (rapid, fără spatial join complet)
JUDET_CENTROIDS = {
    'AB':(46.18,23.80),'AR':(46.17,21.65),'AG':(44.95,24.87),'BC':(46.57,26.91),
    'BH':(47.05,22.08),'BN':(47.13,24.50),'BT':(47.74,26.67),'BV':(45.65,25.60),
    'BR':(45.27,27.96),'B' :(44.43,26.10),'BZ':(45.15,26.82),'CS':(45.30,22.11),
    'CL':(44.20,27.33),'CJ':(46.78,23.60),'CT':(44.18,28.65),'CV':(45.85,26.18),
    'DB':(44.93,25.45),'DJ':(44.31,23.80),'GL':(45.82,27.98),'GR':(43.90,25.97),
    'GJ':(44.95,23.27),'HR':(46.38,25.48),'HD':(45.72,22.92),'IL':(44.60,27.38),
    'IS':(47.16,27.59),'IF':(44.60,26.20),'MM':(47.65,23.88),'MH':(44.63,22.90),
    'MS':(46.55,24.65),'NT':(46.97,26.38),'OT':(44.42,24.50),'PH':(45.10,25.98),
    'SM':(47.80,22.87),'SJ':(47.20,23.06),'SB':(45.79,24.15),'SV':(47.63,25.73),
    'TR':(43.98,25.00),'TM':(45.75,21.22),'TL':(45.18,29.13),'VS':(46.64,27.73),
    'VL':(45.10,24.37),'VN':(45.70,27.00),
}

def nearest_judet(lat, lon):
    best_code, best_dist = 'DJ', float('inf')
    for code, (jlat, jlon) in JUDET_CENTROIDS.items():
        d = (lat-jlat)**2 + (lon-jlon)**2
        if d < best_dist:
            best_dist = d
            best_code = code
    return best_code

# Actualizăm by_jud_year
jy_map = {}
for row in jt['by_jud_year']:
    jy_map[(row['JUDET_CODE'], row['YEAR'])] = row

for r in new_unique:
    y = int(r['date'][:4])
    code = nearest_judet(r['lat'], r['lon'])
    key = (code, y)
    if key in jy_map:
        old = jy_map[key]
        n   = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old['frp_max'], r['frp']), 1)
        old['frp_sum']  = round(old['frp_sum'] + r['frp'], 1)
        old['count']    = n + 1
    else:
        # Găsim JUDET_NAME
        jud_name = next((j['JUDET_NAME'] for j in jt['by_judet_total'] if j['JUDET_CODE']==code), code)
        jy_map[key] = {
            'JUDET_CODE': code, 'JUDET_NAME': jud_name, 'YEAR': y,
            'count': 1, 'frp_mean': round(r['frp'],2),
            'frp_max': round(r['frp'],1), 'frp_sum': round(r['frp'],1)
        }

jt['by_jud_year'] = sorted(jy_map.values(), key=lambda x: (x['JUDET_CODE'], x['YEAR']))

# Actualizăm by_judet_total
jt_map = {j['JUDET_CODE']: j for j in jt['by_judet_total']}
for r in new_unique:
    code = nearest_judet(r['lat'], r['lon'])
    if code in jt_map:
        old = jt_map[code]
        n   = old['count']
        old['frp_mean'] = round((old['frp_mean']*n + r['frp'])/(n+1), 2)
        old['frp_max']  = round(max(old['frp_max'], r['frp']), 1)
        old['frp_sum']  = round(old['frp_sum'] + r['frp'], 1)
        old['count']    = n + 1

jt['by_judet_total'] = list(jt_map.values())

# Actualizăm national_stats
ns = jt['national_stats']
ns['total_count'] = fires['kpis']['total']
ns['mean_count']  = round(sum(j['count'] for j in jt['by_judet_total'])/len(jt['by_judet_total']),1)

# ── Salvăm fișierele ──────────────────────────────────────────────────────
print('Salvez fires_data.json...')
with open('fires_data.json', 'w') as f:
    json.dump(fires, f, separators=(',',':'))

print('Salvez judete_timeseries.json...')
with open('judete_timeseries.json', 'w') as f:
    json.dump(jt, f, separators=(',',':'))

print(f'[{datetime.now().isoformat()}] Actualizare completă!')
print(f'  Total detecții: {fires["kpis"]["total"]:,}')
print(f'  Înregistrări adăugate: {len(new_unique)}')
print(f'  Last update: {fires["kpis"]["last_update"]}')
