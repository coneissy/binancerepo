'''Cryptoalpha multi-engine paper arbitrage engine. LIVE OFF.

Active engines on the current data sources:
- Binance Spot triangular arbitrage
- Binance Spot <-> USDT-M Futures basis arbitrage
The design is ready for additional venues, but no fake CEX/DEX prices are generated.
'''
import json, logging, math, os, random, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websocket

START = float(os.getenv('SIM_START_EQUITY', '10'))
RISK = min(0.01, max(0.0005, float(os.getenv('ARB_RISK_PCT', '0.0025'))))
FEE = max(0.0, float(os.getenv('ARB_FEE_BPS', '4')))
SLIP = max(0.0, float(os.getenv('ARB_SLIPPAGE_BPS', '1.5')))
FUNDING = max(0.0, float(os.getenv('ARB_FUNDING_BUFFER_BPS', '1')))
MIN_NET = max(0.0, float(os.getenv('ARB_MIN_NET_BPS', '1')))
MAX_NET = max(MIN_NET, float(os.getenv('ARB_MAX_NET_BPS', '150')))
STALE = max(100, int(os.getenv('ARB_STALE_MS', '750')))
SCAN = max(0.01, float(os.getenv('ARB_SCAN_INTERVAL', '0.05')))
MAX_NOTIONAL = max(0.01, float(os.getenv('ARB_MAX_NOTIONAL_USDT', '1000')))
MAX_ENTRIES = max(1, int(os.getenv('ARB_MAX_ENTRIES_PER_SCAN', '20')))
MAX_DD = min(0.25, max(0.02, float(os.getenv('ARB_MAX_DRAWDOWN_PCT', '0.08'))))
WS_SHARD_SIZE = max(10, int(os.getenv('ARB_WS_SHARD_SIZE', '20')))

logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'), format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('cryptoalpha-v4')
lock = threading.RLock()
spot, fut = {}, {}
seen = {}

state = {
    'started': time.time(), 'equity': START, 'peak': START, 'paper_pnl': 0.0,
    'spot_entries': 0, 'futures_entries': 0, 'spot_opportunities': 0, 'futures_opportunities': 0,
    'wins': 0, 'losses': 0, 'scans': 0, 'errors': 0, 'spot_updates': 0, 'futures_updates': 0,
    'ws_spot': False, 'ws_futures': False, 'spot_sockets': 0, 'futures_sockets': 0,
    'spot_sockets_up': 0, 'futures_sockets_up': 0, 'last_error': '', 'best_spot': None,
    'best_futures': None, 'opportunities': [], 'halted': False,
}

ASSETS = '''BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ
SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA NOT TON TAO
STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI'''.split()
# Triangular routes are represented as BASE, CROSS, QUOTE. Every required spot pair is subscribed.
ROUTES = [(a,b,'USDT') for a,b in [('ETH','BTC'),('BNB','BTC'),('SOL','BTC'),('XRP','BTC'),('ADA','BTC'),
('AVAX','BTC'),('LINK','BTC'),('DOT','BTC'),('TRX','BTC'),('LTC','BTC'),('ETH','BNB'),('SOL','BNB'),('ADA','BNB')]]
SPOT_SYMBOLS = set(a+'USDT' for a in ASSETS)
for a,b,q in ROUTES:
    SPOT_SYMBOLS.update((a+q, b+q, a+b))
SPOT_SYMBOLS = sorted(SPOT_SYMBOLS)
FUT_SYMBOLS = sorted(a+'USDT' for a in ASSETS)


def shards(symbols):
    return [symbols[i:i+WS_SHARD_SIZE] for i in range(0, len(symbols), WS_SHARD_SIZE)]
SPOT_SHARDS = shards(SPOT_SYMBOLS)
FUT_SHARDS = shards(FUT_SYMBOLS)


def ws_url(base, symbols):
    return base.rstrip('/') + '/stream?streams=' + '/'.join(s.lower()+'@bookTicker' for s in symbols)


def start_group(kind, base, store, groups):
    state[kind+'_sockets'] = len(groups)
    for idx, symbols in enumerate(groups):
        threading.Thread(target=worker, args=(kind,base,store,idx,symbols,len(groups)), daemon=True).start()


def worker(kind, base, store, idx, symbols, total):
    backoff = 1.0
    while True:
        opened = 0.0
        try:
            def on_open(ws):
                nonlocal opened
                opened = time.monotonic()
                with lock:
                    state[kind+'_sockets_up'] += 1
                    state['ws_'+kind] = True
                log.info('WS CONNECTED | %s shard %d/%d | %d symbols', kind.upper(), idx+1, total, len(symbols))
            def on_message(ws, raw):
                try:
                    d = json.loads(raw).get('data', {})
                    s = d.get('s')
                    bid, ask = float(d.get('b',0)), float(d.get('a',0))
                    bq, aq = float(d.get('B',0)), float(d.get('A',0))
                    if s and min(bid,ask,bq,aq)>0 and ask>=bid:
                        with lock:
                            store[s]=(bid,ask,bq,aq,time.monotonic()*1000)
                            state[kind+'_updates'] += 1
                except Exception as e:
                    with lock: state['errors'] += 1; state['last_error']=str(e)
            def on_error(ws,e):
                with lock: state['errors'] += 1; state['last_error']=str(e)
            def on_close(ws,*args):
                with lock:
                    state[kind+'_sockets_up']=max(0,state[kind+'_sockets_up']-1)
                    state['ws_'+kind]=state[kind+'_sockets_up']>0
            websocket.WebSocketApp(ws_url(base,symbols), on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close, header=['User-Agent: cryptoalpha-multi-engine/1.0']).run_forever(
                    ping_interval=20,ping_timeout=10,ping_payload='cryptoalpha',skip_utf8_validation=True)
        except Exception as e:
            with lock: state['errors'] += 1; state['last_error']=str(e)
        stable = opened and time.monotonic()-opened >= 30
        backoff = 1.0 if stable else min(30.0,backoff*2)
        time.sleep(backoff+random.random()*0.5)


def mid(book):
    if not book or time.monotonic()*1000-book[4] > STALE: return None
    return (book[0]+book[1])/2

def px(store,symbol): return mid(store.get(symbol))

def throttle(key, value, ms):
    now=time.monotonic()*1000
    old=seen.get(key)
    if old and now-old[0]<ms and abs(value-old[1])<0.05: return False
    seen[key]=(now,value); return True


def triangular(a,b,q,eq):
    p_aq,p_ab,p_bq=px(spot,a+q),px(spot,a+b),px(spot,b+q)
    if not all((p_aq,p_ab,p_bq)): return None
    # USDT->A->B->USDT: buy A with USDT, sell A for B, sell B for USDT.
    ratio=p_ab/p_aq*p_bq
    gross1=(ratio-1)*10000
    # Reverse route uses the inverse ratio.
    gross2=(1/ratio-1)*10000
    gross=max(gross1,gross2)
    net=gross-3*(FEE+SLIP)
    if net<MIN_NET or net>MAX_NET: return None
    direction=f'USDT->{a}->{b}->USDT' if gross1>=gross2 else f'USDT->{b}->{a}->USDT'
    if not throttle('T:'+a+b+q,net,75): return None
    return {'engine':'SPOT_TRIANGULAR','type':'SPOT_TRIANGULAR','path':direction,'symbol':a+b+q,
            'gross_bps':gross,'net_bps':net,'notional_usdt':min(max(.01,eq*RISK),MAX_NOTIONAL),'paper_only':True}


def futures_cross(symbol,eq):
    sp,fu=px(spot,symbol),px(fut,symbol)
    if not sp or not fu: return None
    basis=(fu/sp-1)*10000
    net=abs(basis)-2*(FEE+SLIP)-FUNDING
    if net<MIN_NET or net>MAX_NET: return None
    direction='BUY_SPOT_SELL_FUTURES' if basis>0 else 'SELL_SPOT_BUY_FUTURES'
    if not throttle('F:'+symbol,net,150): return None
    return {'engine':'SPOT_FUTURES_BASIS','type':'SPOT_FUTURES_CROSS','symbol':symbol,'direction':direction,
            'basis_bps':basis,'gross_bps':abs(basis),'net_bps':net,
            'notional_usdt':min(max(.01,eq*RISK),MAX_NOTIONAL),'paper_only':True}


def capture(opps):
    with lock:
        if state['halted']: return
        if state['peak'] and (state['peak']-state['equity'])/state['peak']>=MAX_DD:
            state['halted']=True; return
    for o in opps[:MAX_ENTRIES]:
        with lock:
            if state['halted']: break
            n=min(o['notional_usdt'],max(.01,state['equity']*RISK),MAX_NOTIONAL)
            pnl=n*o['net_bps']/10000
            state['equity']+=pnl; state['peak']=max(state['peak'],state['equity']); state['paper_pnl']+=pnl
            if o['engine']=='SPOT_TRIANGULAR': state['spot_entries']+=1
            else: state['futures_entries']+=1
            state['wins'] += pnl>=0; state['losses'] += pnl<0


def scan_loop():
    while True:
        try:
            with lock: eq=state['equity']
            spots=[x for a,b,q in ROUTES for x in [triangular(a,b,q,eq)] if x]
            futures=[x for s in FUT_SYMBOLS for x in [futures_cross(s,eq)] if x]
            spots.sort(key=lambda x:x['net_bps'],reverse=True); futures.sort(key=lambda x:x['net_bps'],reverse=True)
            all_opps=spots+futures
            capture(all_opps)
            with lock:
                state['scans']+=1; state['spot_opportunities']=len(spots); state['futures_opportunities']=len(futures)
                state['best_spot']=spots[0] if spots else None; state['best_futures']=futures[0] if futures else None
                state['opportunities']=all_opps[:100]
        except Exception as e:
            with lock: state['errors']+=1; state['last_error']=str(e)
        time.sleep(SCAN)


def stats():
    with lock:
        e,p=state['equity'],state['peak']
        return {'status':'ok','engine':'cryptoalpha-multi-engine-v4','mode':'PAPER_ONLY','live_execution':False,
        'starting_equity':START,'equity':round(e,6),'compound_return_pct':round((e/START-1)*100,6),'paper_pnl':round(state['paper_pnl'],6),
        'spot_entries':state['spot_entries'],'futures_entries':state['futures_entries'],'paper_entries':state['spot_entries']+state['futures_entries'],
        'spot_opportunities':state['spot_opportunities'],'futures_opportunities':state['futures_opportunities'],'best_spot':state['best_spot'],
        'best_futures':state['best_futures'],'top_opportunities':state['opportunities'],'wins':state['wins'],'losses':state['losses'],
        'drawdown_pct':round(max(0,(p-e)/p)*100,5) if p else 0,'max_drawdown_pct':MAX_DD*100,'scans':state['scans'],'errors':state['errors'],
        'ws_spot':state['ws_spot'],'ws_futures':state['ws_futures'],'spot_sockets':state['spot_sockets'],'spot_sockets_up':state['spot_sockets_up'],
        'futures_sockets':state['futures_sockets'],'futures_sockets_up':state['futures_sockets_up'],'quote_updates_spot':state['spot_updates'],
        'quote_updates_futures':state['futures_updates'],'spot_symbols':len(SPOT_SYMBOLS),'futures_symbols':len(FUT_SYMBOLS),
        'triangular_routes':len(ROUTES),'min_net_bps':MIN_NET,'cost_model':{'fee_bps_per_leg':FEE,'slippage_bps_per_leg':SLIP,'funding_buffer_bps':FUNDING},
        'risk_pct':RISK*100,'halted':state['halted'],'last_error':state['last_error'],'uptime_seconds':round(time.time()-state['started'],1),
        'unsupported_engines':['CEX_CEX','FUTURES_FUTURES','CEX_DEX','DEX_DEX']}

HTML='''<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Cryptoalpha Multi-Engine</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1100px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;gap:10px}.brand{font-size:25px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:22px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #202938;text-align:left;font-size:12px}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}.top{display:block}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Multi-Engine</div><div>Real-time Binance WebSocket paper arbitrage</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Compounded equity</div><div class=v id=e>$10</div></div><div class=c><div class=l>Return</div><div class=v id=r>0%</div></div><div class=c><div class=l>Spot triangular</div><div class=v id=so>0</div></div><div class=c><div class=l>Spot/Futures basis</div><div class=v id=fo>0</div></div></div><div class=g><div class=c><div class=l>Spot WS</div><div class=v id=sw>—</div></div><div class=c><div class=l>Futures WS</div><div class=v id=fw>—</div></div><div class=c><div class=l>Min net edge</div><div class=v id=m>—</div></div><div class=c><div class=l>Paper entries</div><div class=v id=pe>0</div></div></div><div class=c><b>BEST SPOT TRIANGULAR</b><p id=bs>—</p><b>BEST FUTURES BASIS</b><p id=bf>—</p></div><div class=c><b>TOP LIVE OPPORTUNITIES</b><div id=t>Waiting for market data…</div></div><div class=c><b>ENGINE STATUS</b><p id=z>—</p></div></div><script>const $=x=>document.getElementById(x),f=(x,d=2)=>Number(x||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('e').textContent='$'+f(d.equity,6);$('r').textContent=f(d.compound_return_pct,4)+'%';$('so').textContent=d.spot_opportunities;$('fo').textContent=d.futures_opportunities;$('sw').textContent=d.spot_sockets_up+'/'+d.spot_sockets;$('fw').textContent=d.futures_sockets_up+'/'+d.futures_sockets;$('m').textContent=f(d.min_net_bps,2)+' bps';$('pe').textContent=d.paper_entries;let b=d.best_spot;$('bs').textContent=b?b.path+' · '+f(b.net_bps)+' bps':'No qualified triangular setup';b=d.best_futures;$('bf').textContent=b?b.symbol+' · '+f(b.net_bps)+' bps · '+b.direction:'No qualified futures setup';$('z').textContent='Scans '+d.scans+' · updates '+d.quote_updates_spot+'/'+d.quote_updates_futures+' · errors '+d.errors+' · reconnect health '+d.spot_sockets_up+'/'+d.spot_sockets+' spot, '+d.futures_sockets_up+'/'+d.futures_sockets+' futures · P&L $'+f(d.paper_pnl,6);let rows=(d.top_opportunities||[]).slice(0,20).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.engine+'</td><td>'+((x.symbol||x.path)||'—')+'</td><td>'+f(x.net_bps)+'</td><td>'+((x.direction)||'—')+'</td></tr>').join('');$('t').innerHTML=rows?'<table><tr><th>#</th><th>Engine</th><th>Market/Path</th><th>Net bps</th><th>Direction</th></tr>'+rows+'</table>':'No qualified opportunities yet'}catch(e){$('z').textContent='Dashboard error: '+e}}u();setInterval(u,1000)</script>'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/stats.json'):
            body=json.dumps(stats()).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body); return
        body=HTML.encode(); self.send_response(200); self.send_header('Content-Type','text/html'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass

def main():
    threading.Thread(target=start_group,args=('spot','wss://stream.binance.com:9443',spot,SPOT_SHARDS),daemon=True).start()
    threading.Thread(target=start_group,args=('futures','wss://fstream.binance.com',fut,FUT_SHARDS),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv('PORT','10000')); ThreadingHTTPServer(('0.0.0.0',port),Handler).serve_forever()

if __name__=='__main__': main()
