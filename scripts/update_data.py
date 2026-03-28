"""
scripts/update_data.py - Rulat de GitHub Actions.
Descarcă ultimele 5 zile de la NASA FIRMS, adaugă date noi (fără duplicate),
regenerează toate fișierele necesare dashboardului.
"""
import urllib.request, json, os, sys
from datetime import datetime, timedelta, timezone

API_KEY  = os.environ.get('FIRMS_API_KEY', '')
BBOX     = '20.26,43.62,29.72,48.27'
BASE_URL = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
DAYS     = 5
SOURCES  = {'VIIRS_SNPP_NRT':'SNPP','VIIRS_NOAA20_NRT':'NOAA20',
            'VIIRS_NOAA21_NRT':'NOAA21','MODIS_NRT':'MODIS'}
JUDET_C  = {
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

if not API_KEY: print('ERROR: FIRMS_API_KEY not set'); sys.exit(1)
now_utc = datetime.now(timezone.utc)
today   = now_utc.date()
print(f'[{now_utc.isoformat()}] Start...')

def fetch_csv(src, days):
    try:
        req = urllib.request.Request(f'{BASE_URL}/{API_KEY}/{src}/{BBOX}/{days}',
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
                'lat':  float(row.get('LATITUDE',0)),
                'lon':  float(row.get('LONGITUDE',0)),
                'date': row.get('ACQ_DATE',''),
                'time': str(row.get('ACQ_TIME','0000')).zfill(4),
                'source': src_name,
                'satellite': row.get('SATELLITE',''),
                'frp':  float(row.get('FRP',0) or 0),
                'brightness': float(row.get('BRIGHTNESS', row.get('BRIGHT_TI4',0)) or 0),
                'daynight': row.get('DAYNIGHT','D'),
                'type': int(row.get('TYPE',0) or 0),
            }
            if rec['date'] and rec['lat'] and rec['lon']:
                out.append(rec)
        except: pass
    return out

def nearest_judet(lat, lon):
    best, bd = 'DJ', float('inf')
    for code,(jlat,jlon) in JUDET_C.items():
        d = (lat-jlat)**2+(lon-jlon)**2
        if d < bd: bd=d; best=code
    return best

# ── Citim datele existente ─────────────────────────────────────────────────
with open('fires_data.json')        as f: fires     = json.load(f)
with open('judete_timeseries.json') as f: jt        = json.load(f)
with open('uat_stats.json')         as f: uat_stats = json.load(f)

# ── Deduplicare prin processed_dates ──────────────────────────────────────
processed_dates = set(fires.get('processed_dates', []))
nrt_dates = {(today-timedelta(days=i)).strftime('%Y-%m-%d') for i in range(DAYS+1)}
dates_to_fetch  = nrt_dates - processed_dates
print(f'De procesat: {sorted(dates_to_fetch)}')
print(f'Deja procesate: {len(processed_dates)} zile')

def save_all(unique_new):
    # fires_data
    with open('fires_data.json','w') as f: json.dump(fires,f,separators=(',',':'))
    with open('fires_data.js','w')   as f:
        f.write('window.__FIRES__='+json.dumps(fires,separators=(',',':'))+';')
    # judete_timeseries
    with open('judete_timeseries.json','w') as f: json.dump(jt,f,separators=(',',':'))
    # uat_stats
    with open('uat_stats.json','w') as f: json.dump(uat_stats,f,separators=(',',':'))
    # recent_fires
    cutoff = (today-timedelta(days=DAYS)).strftime('%Y-%m-%d')
    try:
        old_r = json.loads(open('recent_fires.js').read().replace('window.__RECENT__=','').rstrip(';'))
        old_pts = [p for p in old_r.get('points',[]) if p['date']>=cutoff]
    except: old_pts=[]
    new_pts = [{'lat':round(r['lat'],4),'lon':round(r['lon'],4),'date':r['date'],
                'time':r['time'],'src':r['source'],'sat':r['satellite'],
                'frp':round(r['frp'],1),'dn':r['daynight']} for r in unique_new]
    seen=set(); all_pts=[]
    for p in new_pts+old_pts:
        k=(p['lat'],p['lon'],p['date'],p['time'],p['src'])
        if k not in seen: seen.add(k); all_pts.append(p)
    all_pts.sort(key=lambda x:(x['date'],x['time']),reverse=True)
    obj={'generated':now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),'days_covered':DAYS,
         'count':len(all_pts),'points':all_pts}
    with open('recent_fires.js','w') as f:
        f.write('window.__RECENT__='+json.dumps(obj,separators=(',',':'))+';')
    print(f'  fires_data.json: {os.path.getsize("fires_data.json")/1024:.0f} KB')
    print(f'  uat_stats.json:  {os.path.getsize("uat_stats.json")/1024:.0f} KB')
    print(f'  recent_fires.js: {len(all_pts)} puncte')

if not dates_to_fetch:
    print('Nimic de procesat — actualizez timestamp.')
    fires['kpis']['last_update'] = today.strftime('%Y-%m-%d')
    save_all([])
    sys.exit(0)

# ── Descărcare ────────────────────────────────────────────────────────────
all_new = []
for src_key, src_name in SOURCES.items():
    recs = parse_csv(fetch_csv(src_key, DAYS), src_name)
    filt = [r for r in recs if r['date'] in dates_to_fetch]
    print(f'  {src_name}: {len(recs)} total, {len(filt)} noi')
    all_new.extend(filt)

seen=set(); unique_new=[]
for r in all_new:
    k=(round(r['lat'],3),round(r['lon'],3),r['date'],r['time'],r['source'])
    if k not in seen: seen.add(k); unique_new.append(r)
print(f'Unice noi: {len(unique_new)}')

if not unique_new:
    for d in dates_to_fetch:
        if (today-datetime.strptime(d,'%Y-%m-%d').date()).days>=6:
            processed_dates.add(d)
    fires['processed_dates'] = sorted(processed_dates)
    fires['kpis']['last_update'] = today.strftime('%Y-%m-%d')
    save_all([])
    sys.exit(0)

# ── Actualizare agregate fires_data ───────────────────────────────────────
fires['kpis']['total'] = fires['kpis'].get('total',0) + len(unique_new)
fires['kpis']['last_update'] = today.strftime('%Y-%m-%d')

yr_src = {r['YEAR']:r for r in fires['by_year_src']}
hm_map = {(r['YEAR'],r['MONTH']):r['count'] for r in fires['heatmap']}
mo_map = {r['MONTH']:r for r in fires['by_month']}
gd_map = {}
for r in fires['grid']:
    k=(round(r['LAT_BIN'],1),round(r['LON_BIN'],1)); gd_map[k]=r
dn_map = {r['YEAR']:r for r in fires['daynight']}
fy_map = {r['year']:r for r in fires['frp_year']}

for r in unique_new:
    y=int(r['date'][:4]); m=int(r['date'][5:7])
    # by_year_src
    if y not in yr_src: yr_src[y]={'YEAR':y,'MODIS':0,'SNPP':0,'NOAA20':0,'NOAA21':0}
    if r['source'] in yr_src[y]: yr_src[y][r['source']]+=1
    # heatmap
    hm_map[(y,m)]=hm_map.get((y,m),0)+1
    # by_month
    if m in mo_map:
        old=mo_map[m]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['frp_max']=round(max(old.get('frp_max',0),r['frp']),1)
        old['count']=n+1
    # grid
    k=(round(r['lat']*10)/10,round(r['lon']*10)/10)
    if k in gd_map:
        old=gd_map[k]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['frp_max']=round(max(old['frp_max'],r['frp']),1)
        old['frp_sum']=round(old['frp_sum']+r['frp'],1)
        old['count']=n+1
    else:
        gd_map[k]={'LAT_BIN':k[0],'LON_BIN':k[1],'count':1,
                   'frp_mean':round(r['frp'],2),'frp_max':round(r['frp'],1),'frp_sum':round(r['frp'],1)}
    # daynight
    if y not in dn_map: dn_map[y]={'YEAR':y,'D':0,'N':0}
    dn_map[y][r['daynight']]=dn_map[y].get(r['daynight'],0)+1
    # frp_year
    if y in fy_map:
        fy_map[y]['frp_max']=round(max(fy_map[y]['frp_max'],r['frp']),1)
    else:
        fy_map[y]={'year':y,'frp_mean':round(r['frp'],2),'frp_sum':round(r['frp'],1),'frp_max':round(r['frp'],1)}
    # top_frp
    fires['top_frp'].append({'ACQ_DATE':r['date'],'LATITUDE':r['lat'],'LONGITUDE':r['lon'],
        'FRP':r['frp'],'BRIGHTNESS':r['brightness'],'SOURCE':r['source'],'SATELLITE':r['satellite'],
        'DAYNIGHT':r['daynight'],'CONF_NUM':60,'TYPE':r['type'],'JUDET_NAME':'—'})

fires['by_year_src'] = sorted(yr_src.values(),key=lambda x:x['YEAR'])
fires['heatmap']     = [{'YEAR':k[0],'MONTH':k[1],'count':v} for k,v in sorted(hm_map.items())]
fires['by_month']    = sorted(mo_map.values(),key=lambda x:x['MONTH'])
fires['grid']        = list(gd_map.values())
fires['daynight']    = sorted(dn_map.values(),key=lambda x:x['YEAR'])
fires['frp_year']    = sorted(fy_map.values(),key=lambda x:x['year'])
fires['top_frp']     = sorted(fires['top_frp'],key=lambda x:x['FRP'],reverse=True)[:20]

# processed_dates
for d in dates_to_fetch:
    if (today-datetime.strptime(d,'%Y-%m-%d').date()).days>=6:
        processed_dates.add(d)
cutoff60=(today-timedelta(days=60)).strftime('%Y-%m-%d')
fires['processed_dates']=sorted(d for d in processed_dates if d>=cutoff60)

# ── Actualizare judete_timeseries ─────────────────────────────────────────
jy_map = {(r['JUDET_CODE'],r['YEAR']):r for r in jt['by_jud_year']}
jt_tot = {j['JUDET_CODE']:j for j in jt['by_judet_total']}
jm_map = {}
for r in jt.get('by_jud_month',[]):
    jm_map[(r['JUDET_CODE'],r['MONTH'])]=r

for r in unique_new:
    y=int(r['date'][:4]); m=int(r['date'][5:7])
    code=nearest_judet(r['lat'],r['lon'])
    # by_jud_year
    k=(code,y)
    if k in jy_map:
        old=jy_map[k]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['frp_max']=round(max(old['frp_max'],r['frp']),1)
        old['frp_sum']=round(old['frp_sum']+r['frp'],1)
        old['count']=n+1
    else:
        jname=next((j['JUDET_NAME'] for j in jt['by_judet_total'] if j['JUDET_CODE']==code),code)
        jy_map[k]={'JUDET_CODE':code,'JUDET_NAME':jname,'YEAR':y,'count':1,
                   'frp_mean':round(r['frp'],2),'frp_max':round(r['frp'],1),'frp_sum':round(r['frp'],1)}
    # by_judet_total
    if code in jt_tot:
        old=jt_tot[code]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['frp_max']=round(max(old['frp_max'],r['frp']),1)
        old['frp_sum']=round(old['frp_sum']+r['frp'],1)
        old['count']=n+1
    # by_jud_month
    km=(code,m)
    if km in jm_map:
        old=jm_map[km]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['count']=n+1
    else:
        jm_map[km]={'JUDET_CODE':code,'MONTH':m,'count':1,'frp_mean':round(r['frp'],2)}

jt['by_jud_year']    = sorted(jy_map.values(),key=lambda x:(x['JUDET_CODE'],x['YEAR']))
jt['by_judet_total'] = list(jt_tot.values())
jt['by_jud_month']   = sorted(jm_map.values(),key=lambda x:(x['JUDET_CODE'],x['MONTH']))
jt['national_stats']['total_count']=fires['kpis']['total']
jt['national_stats']['mean_count']=round(
    sum(j['count'] for j in jt['by_judet_total'])/max(len(jt['by_judet_total']),1),1)

# ── Actualizare uat_stats ─────────────────────────────────────────────────
uat_map  = {u['UAT_SIRUTA']:u for u in uat_stats.get('by_uat',[])}
uaty_map = {(r['YEAR'],r['UAT_SIRUTA']):r for r in uat_stats.get('uat_year',[])}
uat_by_cc= {}
for u in uat_stats.get('by_uat',[]):
    cc=u.get('UAT_CC','')
    if cc not in uat_by_cc: uat_by_cc[cc]=[]
    uat_by_cc[cc].append(u)

for r in unique_new:
    y=int(r['date'][:4])
    code=nearest_judet(r['lat'],r['lon'])
    cc_uats=uat_by_cc.get(code,[])
    if not cc_uats: continue
    best_uat=max(cc_uats,key=lambda u:u.get('count',0))
    sir=best_uat['UAT_SIRUTA']
    if sir in uat_map:
        old=uat_map[sir]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['frp_max']=round(max(old.get('frp_max',0),r['frp']),1)
        old['frp_sum']=round(old.get('frp_sum',0)+r['frp'],1)
        old['count']=n+1
    k=(y,sir)
    if k in uaty_map:
        old=uaty_map[k]; n=old['count']
        old['frp_mean']=round((old['frp_mean']*n+r['frp'])/(n+1),2)
        old['count']=n+1
    else:
        uaty_map[k]={'YEAR':y,'UAT_SIRUTA':sir,'UAT_CC':code,'count':1,'frp_mean':round(r['frp'],2)}

uat_stats['by_uat']   = list(uat_map.values())
uat_stats['uat_year'] = list(uaty_map.values())

# ── Salvare ───────────────────────────────────────────────────────────────
print('\nSalvez...')
save_all(unique_new)
print(f'\n[{datetime.now(timezone.utc).isoformat()}] Done!')
print(f'  Noi: {len(unique_new)} | Total: {fires["kpis"]["total"]:,}')
print(f'  Processed dates: {fires["processed_dates"]}')
