'''Cryptoalpha v6 paper engine. LIVE OFF.
Fixes: no Futures REST exchangeInfo dependency, fewer long-lived sockets, explicit ping/pong, safer reconnects.
Futures triangular routes are enabled only for contracts explicitly listed in ARB_FUT_TRI_SYMBOLS.
'''
import json, logging, os, random, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websocket

START=float(os.getenv('SIM_START_EQUITY','10'))
RISK=min(.01,max(.0005,float(os.getenv('ARB_RISK_PCT','.005'))))
FEE=max(0,float(os.getenv('ARB_FEE_BPS','4')))
SLIP=max(0,float(os.getenv('ARB_SLIPPAGE_BPS','1.5')))
FUND=max(0,float(os.getenv('ARB_FUNDING_BUFFER_BPS','1')))
MIN_NET=max(0,float(os.getenv('ARB_MIN_NET_BPS','5')))
MAX_NET=max(MIN_NET,float(os.getenv('ARB_MAX_NET_BPS','150')))
STALE=max(100,int(os.getenv('ARB_STALE_MS','750')))
SCAN=max(.01,float(os.getenv('ARB_SCAN_INTERVAL','.05')))
SHARD=max(25,int(os.getenv('ARB_WS_SHARD_SIZE','80')))
MAX_NOTIONAL=max(.01,float(os.getenv('ARB_MAX_NOTIONAL_USDT','1000')))
MAX_ENTRIES=max(1,int(os.getenv('ARB_MAX_ENTRIES_PER_SCAN','20')))
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'),format='%(asctime)s %(levelname)s %(message)s')
log=logging.getLogger('cryptoalpha-v6')
lock=threading.RLock(); spot={}; fut={}; seen={}
state={'started':time.time(),'equity':START,'peak':START,'paper_pnl':0.,'spot_entries':0,'futures_entries':0,'spot_opportunities':0,'futures_opportunities':0,'wins':0,'losses':0,'scans':0,'errors':0,'spot_updates':0,'futures_updates':0,'ws_spot':False,'ws_futures':False,'spot_sockets':0,'futures_sockets':0,'spot_sockets_up':0,'futures_sockets_up':0,'last_error':'','best_spot':None,'best_futures':None,'opportunities':[]}
ASSETS='BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA NOT TON TAO STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI'.split()
ROUTES=[(a,b,'USDT') for a,b in [('ETH','BTC'),('BNB','BTC'),('SOL','BTC'),('XRP','BTC'),('ADA','BTC'),('AVAX','BTC'),('LINK','BTC'),('DOT','BTC'),('TRX','BTC'),('LTC','BTC'),('ETH','BNB'),('SOL','BNB'),('ADA','BNB')]]
SPOT_SYMBOLS=set(a+'USDT' for a in ASSETS)
for a,b,q in ROUTES: SPOT_SYMBOLS.update((a+q,b+q,a+b))
SPOT_SYMBOLS=sorted(SPOT_SYMBOLS)
# Do not call Binance Futures exchangeInfo at boot: Render was receiving HTTP 418.
# Use only the known USDT-M contracts for basis monitoring. Cross futures are opt-in.
FUT_SYMBOLS=sorted(a+'USDT' for a in ASSETS)
FUT_TRI_RAW=os.getenv('ARB_FUT_TRI_SYMBOLS','').strip()
FUT_TRI_ROUTES=[]
if FUT_TRI_RAW:
    available=set(x.strip().upper() for x in FUT_TRI_RAW.split(',') if x.strip())
    for a,b,q in ROUTES:
        if {a+q,b+q,a+b}.issubset(available):
            FUT_TRI_ROUTES.append((a,b,q))
            FUT_SYMBOLS.extend([a+b,b+q,a+q])
FUT_SYMBOLS=sorted(set(FUT_SYMBOLS))

def shards(xs): return [xs[i:i+SHARD] for i in range(0,len(xs),SHARD)]

def stream_url(base,symbols):
    return base.rstrip('/')+'/stream?streams='+'/'.join(s.lower()+'@bookTicker' for s in symbols)

def worker(kind,base,store,idx,symbols,total):
    backoff=1.0
    while True:
        opened_at=0.0
        try:
            def on_open(ws):
                nonlocal opened_at
                opened_at=time.monotonic()
                with lock:
                    state[kind+'_sockets_up']+=1; state['ws_'+kind]=True
                log.info('WS CONNECTED | %s shard %d/%d | %d symbols',kind.upper(),idx+1,total,len(symbols))
            def on_message(ws,raw):
                try:
                    msg=json.loads(raw); d=msg.get('data',msg); s=d.get('s')
                    bid=float(d.get('b',0)); ask=float(d.get('a',0)); bq=float(d.get('B',0)); aq=float(d.get('A',0))
                    if s and bid>0 and ask>=bid and bq>0 and aq>0:
                        with lock:
                            store[s]=(bid,ask,bq,aq,time.monotonic()*1000); state[kind+'_updates']+=1
                except Exception as e:
                    with lock: state['errors']+=1; state['last_error']='message: '+str(e)
            def on_error(ws,e):
                with lock: state['last_error']=f'{kind} websocket: {e}'
            def on_close(ws,code,msg):
                with lock:
                    state[kind+'_sockets_up']=max(0,state[kind+'_sockets_up']-1)
                    state['ws_'+kind]=state[kind+'_sockets_up']>0
                log.warning('WS CLOSED | %s shard %d/%d | code=%s msg=%s',kind.upper(),idx+1,total,code,msg)
            app=websocket.WebSocketApp(stream_url(base,symbols),on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close,header=['User-Agent: cryptoalpha-v6'])
            app.run_forever(ping_interval=20,ping_timeout=10,ping_payload='cryptoalpha',skip_utf8_validation=True)
        except Exception as e:
            with lock: state['errors']+=1; state['last_error']=f'{kind} transport: {e}'
        stable=bool(opened_at and time.monotonic()-opened_at>=30)
        backoff=1.0 if stable else min(30.0,backoff*2.0)
        time.sleep(backoff+random.random()*.5)

def start(kind,base,store,groups):
    state[kind+'_sockets']=len(groups)
    for i,g in enumerate(groups):
        threading.Thread(target=worker,args=(kind,base,store,i,g,len(groups)),daemon=True).start()

def px(store,s):
    x=store.get(s)
    return None if not x or time.monotonic()*1000-x[4]>STALE else (x[0]+x[1])/2

def throttle(k,v,ms):
    n=time.monotonic()*1000; old=seen.get(k)
    if old and n-old[0]<ms and abs(v-old[1])<.05: return False
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
            s=[o for a,b,q in ROUTES for o in [tri(a,b,q,eq)] if o]
            f=[o for sym in FUT_SYMBOLS if sym.endswith('USDT') for o in [basis(sym,eq)] if o]
            ft=[o for a,b,q in FUT_TRI_ROUTES for o in [ftri(a,b,q,eq)] if o]
            s.sort(key=lambda x:x['net_bps'],reverse=True); f.sort(key=lambda x:x['net_bps'],reverse=True); allx=s+f+ft
            with lock:
                # Paper capture only; no exchange orders are sent.
                for o in allx[:MAX_ENTRIES]:
                    n=min(o['notional_usdt'],max(.01,state['equity']*RISK),MAX_NOTIONAL); pnl=n*o['net_bps']/10000
                    state['equity']+=pnl; state['peak']=max(state['peak'],state['equity']); state['paper_pnl']+=pnl
                    state['spot_entries']+=o['engine']=='SPOT_TRIANGULAR'; state['futures_entries']+=o['engine']!='SPOT_TRIANGULAR'; state['wins']+=pnl>=0; state['losses']+=pnl<0
                state['scans']+=1; state['spot_opportunities']=len(s); state['futures_opportunities']=len(f)+len(ft); state['best_spot']=s[0] if s else None; state['best_futures']=(f+ft)[0] if (f or ft) else None; state['opportunities']=allx[:100]
        except Exception as e:
            with lock: state['errors']+=1; state['last_error']='scan: '+str(e)
        time.sleep(SCAN)

def stats():
    with lock:
        e=state['equity']
        return {'status':'ok','engine':'cryptoalpha-v6','mode':'PAPER_ONLY','live_execution':False,'starting_equity':START,'equity':round(e,6),'compound_return_pct':round((e/START-1)*100,6),'paper_pnl':round(state['paper_pnl'],6),'spot_entries':state['spot_entries'],'futures_entries':state['futures_entries'],'paper_entries':state['spot_entries']+state['futures_entries'],'spot_opportunities':state['spot_opportunities'],'futures_opportunities':state['futures_opportunities'],'best_spot':state['best_spot'],'best_futures':state['best_futures'],'top_opportunities':state['opportunities'],'wins':state['wins'],'losses':state['losses'],'scans':state['scans'],'errors':state['errors'],'ws_spot':state['ws_spot'],'ws_futures':state['ws_futures'],'spot_sockets':state['spot_sockets'],'spot_sockets_up':state['spot_sockets_up'],'futures_sockets':state['futures_sockets'],'futures_sockets_up':state['futures_sockets_up'],'quote_updates_spot':state['spot_updates'],'quote_updates_futures':state['futures_updates'],'spot_symbols':len(SPOT_SYMBOLS),'futures_symbols':len(FUT_SYMBOLS),'triangular_routes':len(ROUTES),'futures_triangular_routes':len(FUT_TRI_ROUTES),'min_net_bps':MIN_NET,'risk_pct':RISK*100,'last_error':state['last_error'],'uptime_seconds':round(time.time()-state['started'],1),'unsupported_engines':['FUTURES_FUTURES','CEX_CEX','CEX_DEX','DEX_DEX']}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/stats.json'):
            body=json.dumps(stats()).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body); return
        body=b'''<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Cryptoalpha</title><body style="font:16px system-ui;max-width:900px;margin:30px auto"><h1>Cryptoalpha v6</h1><p>PAPER ONLY - LIVE OFF</p><pre id="x">loading...</pre><script>async function u(){x.textContent=JSON.stringify(await (await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json(),null,2)}u();setInterval(u,1000)</script></body>'''; self.send_response(200); self.send_header('Content-Type','text/html'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass

def main():
    start('spot','wss://stream.binance.com:9443',spot,shards(SPOT_SYMBOLS))
    start('futures','wss://fstream.binance.com',fut,shards(FUT_SYMBOLS))
    threading.Thread(target=scan,daemon=True).start()
    port=int(os.getenv('PORT','10000')); ThreadingHTTPServer(('0.0.0.0',port),Handler).serve_forever()

if __name__=='__main__': main()
