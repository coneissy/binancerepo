'''Cryptoalpha v5 paper engine. LIVE OFF.
Robust Binance Spot + USDT-M Futures monitoring with reconnect-safe WebSockets.
Futures triangular routes are enabled only when all three real Binance futures contracts exist.
No fake prices and no live orders.'''
import json, logging, os, random, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websocket

START=float(os.getenv('SIM_START_EQUITY','10')); RISK=min(.01,max(.0005,float(os.getenv('ARB_RISK_PCT','.005'))))
FEE=max(0,float(os.getenv('ARB_FEE_BPS','4'))); SLIP=max(0,float(os.getenv('ARB_SLIPPAGE_BPS','1.5'))); FUND=max(0,float(os.getenv('ARB_FUNDING_BUFFER_BPS','1')))
MIN_NET=max(0,float(os.getenv('ARB_MIN_NET_BPS','5'))); MAX_NET=max(MIN_NET,float(os.getenv('ARB_MAX_NET_BPS','150')))
STALE=max(100,int(os.getenv('ARB_STALE_MS','750'))); SCAN=max(.01,float(os.getenv('ARB_SCAN_INTERVAL','.05'))); SHARD=max(10,int(os.getenv('ARB_WS_SHARD_SIZE','20')))
MAX_NOTIONAL=max(.01,float(os.getenv('ARB_MAX_NOTIONAL_USDT','1000'))); MAX_ENTRIES=max(1,int(os.getenv('ARB_MAX_ENTRIES_PER_SCAN','20')))
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'),format='%(asctime)s %(levelname)s %(message)s'); log=logging.getLogger('cryptoalpha-v5')
lock=threading.RLock(); spot={}; fut={}; seen={}
state={'started':time.time(),'equity':START,'peak':START,'paper_pnl':0.,'spot_entries':0,'futures_entries':0,'spot_opportunities':0,'futures_opportunities':0,'wins':0,'losses':0,'scans':0,'errors':0,'spot_updates':0,'futures_updates':0,'ws_spot':False,'ws_futures':False,'spot_sockets':0,'futures_sockets':0,'spot_sockets_up':0,'futures_sockets_up':0,'last_error':'','best_spot':None,'best_futures':None,'opportunities':[]}
ASSETS='BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA NOT TON TAO STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI'.split()
ROUTES=[(a,b,'USDT') for a,b in [('ETH','BTC'),('BNB','BTC'),('SOL','BTC'),('XRP','BTC'),('ADA','BTC'),('AVAX','BTC'),('LINK','BTC'),('DOT','BTC'),('TRX','BTC'),('LTC','BTC'),('ETH','BNB'),('SOL','BNB'),('ADA','BNB')]]
SPOT_SYMBOLS=set(a+'USDT' for a in ASSETS)
for a,b,q in ROUTES: SPOT_SYMBOLS.update((a+q,b+q,a+b))
SPOT_SYMBOLS=sorted(SPOT_SYMBOLS)
BASE_FUT_SYMBOLS=set(a+'USDT' for a in ASSETS)

# Binance USDT-M only lists contracts that actually exist. Cross legs such as ETHBTC
# are not assumed; they are included only if the exchange reports them.
def valid_futures_symbols():
    try:
        req=urllib.request.Request('https://fapi.binance.com/fapi/v1/exchangeInfo',headers={'User-Agent':'cryptoalpha-v5'})
        with urllib.request.urlopen(req,timeout=8) as r: data=json.loads(r.read().decode())
        return {x['symbol'] for x in data.get('symbols',[]) if x.get('status')=='TRADING'}
    except Exception as e:
        log.warning('Futures exchangeInfo unavailable: %s',e); return set()
VALID_FUT=valid_futures_symbols(); FUT_SYMBOLS=sorted(BASE_FUT_SYMBOLS & VALID_FUT)
FUT_TRI_ROUTES=[r for r in ROUTES if all(a+b in VALID_FUT for a,b,q in [r])]
for a,b,q in FUT_TRI_ROUTES: FUT_SYMBOLS.extend([a+b,b+q,a+q])
FUT_SYMBOLS=sorted(set(FUT_SYMBOLS))

def shards(xs): return [xs[i:i+SHARD] for i in range(0,len(xs),SHARD)]
SPOT_SHARDS=shards(SPOT_SYMBOLS); FUT_SHARDS=shards(FUT_SYMBOLS)
def url(base,symbols): return base.rstrip('/')+'/stream?streams='+'/'.join(s.lower()+'@bookTicker' for s in symbols)

def worker(kind,base,store,idx,symbols,total):
    backoff=1.
    while True:
        opened=0.
        try:
            def on_open(ws):
                nonlocal opened; opened=time.monotonic()
                with lock: state[kind+'_sockets_up']+=1; state['ws_'+kind]=True
                log.info('WS CONNECTED | %s shard %d/%d | %d symbols',kind.upper(),idx+1,total,len(symbols))
            def on_message(ws,raw):
                try:
                    d=json.loads(raw).get('data',{}); s=d.get('s'); bid=float(d.get('b',0)); ask=float(d.get('a',0)); bq=float(d.get('B',0)); aq=float(d.get('A',0))
                    if s and min(bid,ask,bq,aq)>0 and ask>=bid:
                        with lock: store[s]=(bid,ask,bq,aq,time.monotonic()*1000); state[kind+'_updates']+=1
                except Exception as e:
                    with lock: state['errors']+=1; state['last_error']=str(e)
            def on_error(ws,e):
                # Reconnectable transport errors are recorded but do not kill the engine.
                with lock: state['last_error']=str(e)
            def on_close(ws,*args):
                with lock: state[kind+'_sockets_up']=max(0,state[kind+'_sockets_up']-1); state['ws_'+kind]=state[kind+'_sockets_up']>0
            # Let Binance server pings be handled by websocket-client automatically.
            # Do not run a second client-side ping watchdog on Render's free runtime.
            websocket.WebSocketApp(url(base,symbols),on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close,header=['User-Agent: cryptoalpha-v5']).run_forever(skip_utf8_validation=True)
        except Exception as e:
            with lock: state['errors']+=1; state['last_error']=str(e)
        stable=bool(opened and time.monotonic()-opened>=30); backoff=1. if stable else min(30.,backoff*2); time.sleep(backoff+random.random()*.5)

def start(kind,base,store,groups):
    state[kind+'_sockets']=len(groups)
    for i,g in enumerate(groups): threading.Thread(target=worker,args=(kind,base,store,i,g,len(groups)),daemon=True).start()

def px(store,s):
    x=store.get(s)
    return None if not x or time.monotonic()*1000-x[4]>STALE else (x[0]+x[1])/2
def throttle(k,v,ms):
    n=time.monotonic()*1000; o=seen.get(k)
    if o and n-o[0]<ms and abs(v-o[1])<.05:return False
    seen[k]=(n,v); return True

def tri(a,b,q,eq):
    pa,pab,pb=px(spot,a+q),px(spot,a+b),px(spot,b+q)
    if not all((pa,pab,pb)): return None
    ratio=pab/pa*pb; g1=(ratio-1)*10000; g2=(1/ratio-1)*10000; gross=max(g1,g2); net=gross-3*(FEE+SLIP)
    if net<MIN_NET or net>MAX_NET or not throttle('S:'+a+b+q,net,75): return None
    return {'engine':'SPOT_TRIANGULAR','type':'SPOT_TRIANGULAR','path':f'USDT->{a}->{b}->USDT' if g1>=g2 else f'USDT->{b}->{a}->USDT','symbol':a+b+q,'gross_bps':gross,'net_bps':net,'notional_usdt':min(max(.01,eq*RISK),MAX_NOTIONAL),'paper_only':True}

def basis(s,eq):
    sp,fu=px(spot,s),px(fut,s)
    if not sp or not fu:return None
    b=(fu/sp-1)*10000; net=abs(b)-2*(FEE+SLIP)-FUND
    if net<MIN_NET or net>MAX_NET or not throttle('F:'+s,net,150):return None
    return {'engine':'SPOT_FUTURES_BASIS','type':'SPOT_FUTURES_CROSS','symbol':s,'direction':'BUY_SPOT_SELL_FUTURES' if b>0 else 'SELL_SPOT_BUY_FUTURES','basis_bps':b,'gross_bps':abs(b),'net_bps':net,'notional_usdt':min(max(.01,eq*RISK),MAX_NOTIONAL),'paper_only':True}

def ftri(a,b,q,eq):
    pa,pab,pb=px(fut,a+q),px(fut,a+b),px(fut,b+q)
    if not all((pa,pab,pb)):return None
    ratio=pab/pa*pb; g1=(ratio-1)*10000; g2=(1/ratio-1)*10000; gross=max(g1,g2); net=gross-3*(FEE+SLIP)-FUND
    if net<MIN_NET or net>MAX_NET or not throttle('FT:'+a+b+q,net,75):return None
    return {'engine':'FUTURES_TRIANGULAR','type':'FUTURES_TRIANGULAR','path':f'USDT->{a}->{b}->USDT' if g1>=g2 else f'USDT->{b}->{a}->USDT','symbol':a+b+q,'gross_bps':gross,'net_bps':net,'notional_usdt':min(max(.01,eq*RISK),MAX_NOTIONAL),'paper_only':True}

def scan():
    while True:
        try:
            with lock:eq=state['equity']
            s=[x for a,b,q in ROUTES for x in [tri(a,b,q,eq)] if x]
            b=[x for x in FUT_SYMBOLS if x.endswith('USDT') for x in [basis(x,eq)] if x]
            ft=[x for a,bx,q in FUT_TRI_ROUTES for x in [ftri(a,bx,q,eq)] if x]
            s.sort(key=lambda x:x['net_bps'],reverse=True); f=(b+ft); f.sort(key=lambda x:x['net_bps'],reverse=True); allx=s+f
            with lock:
                for o in allx[:MAX_ENTRIES]:
                    n=min(o['notional_usdt'],max(.01,state['equity']*RISK),MAX_NOTIONAL); pnl=n*o['net_bps']/10000; state['equity']+=pnl; state['peak']=max(state['peak'],state['equity']); state['paper_pnl']+=pnl
                    state['spot_entries']+=o['engine']=='SPOT_TRIANGULAR'; state['futures_entries']+=o['engine']!='SPOT_TRIANGULAR'; state['wins']+=pnl>=0; state['losses']+=pnl<0
                state['scans']+=1; state['spot_opportunities']=len(s); state['futures_opportunities']=len(f); state['best_spot']=s[0] if s else None; state['best_futures']=f[0] if f else None; state['opportunities']=allx[:100]
        except Exception as e:
            with lock:state['errors']+=1;state['last_error']=str(e)
        time.sleep(SCAN)

def stats():
    with lock:
        e,p=state['equity'],state['peak']
        return {'status':'ok','engine':'cryptoalpha-v5','mode':'PAPER_ONLY','live_execution':False,'starting_equity':START,'equity':round(e,6),'compound_return_pct':round((e/START-1)*100,6),'paper_pnl':round(state['paper_pnl'],6),'spot_entries':state['spot_entries'],'futures_entries':state['futures_entries'],'paper_entries':state['spot_entries']+state['futures_entries'],'spot_opportunities':state['spot_opportunities'],'futures_opportunities':state['futures_opportunities'],'best_spot':state['best_spot'],'best_futures':state['best_futures'],'top_opportunities':state['opportunities'],'wins':state['wins'],'losses':state['losses'],'scans':state['scans'],'errors':state['errors'],'ws_spot':state['ws_spot'],'ws_futures':state['ws_futures'],'spot_sockets':state['spot_sockets'],'spot_sockets_up':state['spot_sockets_up'],'futures_sockets':state['futures_sockets'],'futures_sockets_up':state['futures_sockets_up'],'quote_updates_spot':state['spot_updates'],'quote_updates_futures':state['futures_updates'],'spot_symbols':len(SPOT_SYMBOLS),'futures_symbols':len(FUT_SYMBOLS),'triangular_routes':len(ROUTES),'futures_triangular_routes':len(FUT_TRI_ROUTES),'min_net_bps':MIN_NET,'risk_pct':RISK*100,'last_error':state['last_error'],'uptime_seconds':round(time.time()-state['started'],1),'unsupported_engines':['FUTURES_FUTURES','CEX_CEX','CEX_DEX','DEX_DEX']}

HTML='''<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Cryptoalpha</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1100px;margin:auto;padding:16px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.v{font-size:22px;font-weight:800;margin-top:5px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}table{width:100%;border-collapse:collapse}td,th{padding:7px;border-bottom:1px solid #202938;text-align:left;font-size:12px}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><h1>Cryptoalpha Multi-Engine</h1><div>PAPER ONLY · LIVE OFF</div><div class=g><div class=c><div class=l>Equity</div><div class=v id=e>$10</div></div><div class=c><div class=l>Return</div><div class=v id=r>0%</div></div><div class=c><div class=l>Spot opportunities</div><div class=v id=s>0</div></div><div class=c><div class=l>Futures opportunities</div><div class=v id=f>0</div></div></div><div class=c id=z>Loading…</div><div class=c><b>Top opportunities</b><div id=t>Waiting…</div></div></div><script>const $=x=>document.getElementById(x);const f=(x,d=2)=>Number(x||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('e').textContent='$'+f(d.equity,6);$('r').textContent=f(d.compound_return_pct,4)+'%';$('s').textContent=d.spot_opportunities;$('f').textContent=d.futures_opportunities;$('z').textContent='WS '+d.spot_sockets_up+'/'+d.spot_sockets+' spot · '+d.futures_sockets_up+'/'+d.futures_sockets+' futures · scans '+d.scans+' · updates '+d.quote_updates_spot+'/'+d.quote_updates_futures+' · errors '+d.errors+' · min '+d.min_net_bps+' bps · futures triangles '+d.futures_triangular_routes+' · last '+(d.last_error||'none');let rows=(d.top_opportunities||[]).slice(0,20).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.engine+'</td><td>'+((x.symbol||x.path)||'—')+'</td><td>'+f(x.net_bps)+'</td></tr>').join('');$('t').innerHTML=rows?'<table><tr><th>#</th><th>Engine</th><th>Market/Path</th><th>Net bps</th></tr>'+rows+'</table>':'No qualified opportunities'}catch(e){$('z').textContent='Dashboard error '+e}}u();setInterval(u,1000)</script>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  body=json.dumps(stats()).encode() if self.path.startswith('/stats.json') else HTML.encode(); self.send_response(200); self.send_header('Content-Type','application/json' if self.path.startswith('/stats.json') else 'text/html'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body)
 def log_message(self,*a):pass

def main():
 threading.Thread(target=start,args=('spot','wss://stream.binance.com:9443',spot,SPOT_SHARDS),daemon=True).start(); threading.Thread(target=start,args=('futures','wss://fstream.binance.com',fut,FUT_SHARDS),daemon=True).start(); threading.Thread(target=scan,daemon=True).start(); ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),H).serve_forever()
if __name__=='__main__':main()
