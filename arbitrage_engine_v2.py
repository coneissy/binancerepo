"""Cryptoalpha Arbitrage v2 - ultra-fast realistic SPOT triangular CROSS paper engine. LIVE OFF."""
import json,logging,os,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import websocket

START=float(os.getenv("SIM_START_EQUITY","10")); RISK=max(.01,min(.25,float(os.getenv("ARB_RISK_PCT",".25"))))
FEE=max(0,float(os.getenv("ARB_FEE_BPS","4"))); MIN_NET=max(0,float(os.getenv("ARB_MIN_NET_BPS","0")))
STALE=max(100,int(os.getenv("ARB_STALE_MS","750"))); SCAN=max(.01,float(os.getenv("ARB_SCAN_INTERVAL",".02")))
MAX_ENTRIES=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","200"))); MAX_NOTIONAL=max(.01,float(os.getenv("ARB_MAX_NOTIONAL_USDT","1000")))
DEPTH_USE=min(.5,max(.01,float(os.getenv("ARB_DEPTH_USE_PCT",".20"))))
logging.basicConfig(level=os.getenv("LOG_LEVEL","WARNING"),format="%(asctime)s %(levelname)s %(message)s"); log=logging.getLogger("cryptoalpha")
lock=threading.RLock(); books={}; depths={}; seen={}; dirty=set(); route_by_symbol={}
ASSETS="BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA NOT TON TAO STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI".split()
CROSS="ETHBTC BNBBTC SOLBTC XRPBTC ADABTC AVAXBTC LINKBTC DOTBTC TRXBTC LTCBTC ETHBNB SOLBNB ADABNB".split()
SYMS=list(dict.fromkeys([x+"USDT" for x in ASSETS]+CROSS))
TRI=[("ETH","BTC","USDT"),("BNB","BTC","USDT"),("SOL","BTC","USDT"),("XRP","BTC","USDT"),("ADA","BTC","USDT"),("AVAX","BTC","USDT"),("LINK","BTC","USDT"),("DOT","BTC","USDT"),("TRX","BTC","USDT"),("LTC","BTC","USDT"),("ETH","BNB","USDT"),("SOL","BNB","USDT"),("ADA","BNB","USDT")]
state={"started":time.time(),"equity":START,"peak":START,"paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"scans":0,"errors":0,"opportunities":0,"best":None,"quote_updates":0,"depth_updates":0,"ws_spot":False,"last_error":""}
for i,r in enumerate(TRI):
 for x in (r[0]+r[1],r[1]+r[2],r[0]+r[2]): route_by_symbol.setdefault(x,[]).append(i)

def ws_url():
 streams=[]
 for s in SYMS: streams += [s.lower()+"@bookTicker",s.lower()+"@depth5@100ms"]
 return "wss://stream.binance.com:443/stream?streams="+"/".join(streams)

def ws_loop():
 back=1
 while True:
  try:
   def opened(ws):
    nonlocal back; back=1; state["ws_spot"]=True; log.warning("WS CONNECTED | SPOT | symbols=%d streams=%d",len(SYMS),len(SYMS)*2)
   def message(ws,m):
    try:
     r=json.loads(m); d=r.get("data",r); s=d.get("s"); stream=r.get("stream",""); now=time.monotonic()*1000
     if not s:return
     with lock:
      if "@depth" in stream:
       b=[(float(x[0]),float(x[1])) for x in d.get("b",[])[:5] if float(x[0])>0 and float(x[1])>0]; a=[(float(x[0]),float(x[1])) for x in d.get("a",[])[:5] if float(x[0])>0 and float(x[1])>0]
       if b and a: depths[s]=(b,a,now); dirty.update(route_by_symbol.get(s,())) ; state["depth_updates"]+=1
      elif "@bookTicker" in stream:
       b=float(d.get("b",0));a=float(d.get("a",0));B=float(d.get("B",0));A=float(d.get("A",0))
       if min(b,a,B,A)>0: books[s]=(b,a,B,A,now); dirty.update(route_by_symbol.get(s,())); state["quote_updates"]+=1
    except Exception as e: state["errors"]+=1; state["last_error"]=str(e)
   def error(ws,e): state["errors"]+=1; state["last_error"]=str(e)
   def closed(ws,*a): state["ws_spot"]=False
   websocket.WebSocketApp(ws_url(),on_open=opened,on_message=message,on_error=error,on_close=closed).run_forever(ping_interval=20,ping_timeout=10)
  except Exception as e: state["errors"]+=1; state["last_error"]=str(e)
  state["ws_spot"]=False; time.sleep(back); back=min(15,back*2)

def book_px(sym,side):
 b=books.get(sym); d=depths.get(sym); now=time.monotonic()*1000
 if not b or not d or now-b[4]>STALE or now-d[2]>STALE:return None
 return d[1] if side=="buy" else d[0]

def convert(asset_from,asset_to,qty):
 if asset_from==asset_to:return qty
 direct=asset_from+asset_to; inverse=asset_to+asset_from
 if direct in books and direct in depths:
  levels=book_px(direct,"sell")
  if levels:return qty*levels[0][0]
 if inverse in books and inverse in depths:
  levels=book_px(inverse,"buy")
  if levels:return qty/levels[0][0]
 return None

def triangular(a,b,c,eq):
 n=min(max(.01,eq*RISK),MAX_NOTIONAL)
 q=convert(c,a,n)
 if q is None:return None
 q=convert(a,b,q)
 if q is None:return None
 out=convert(b,c,q)
 if out is None:return None
 gross=(out/n-1)*10000; net=gross-3*FEE
 if net<=MIN_NET:return None
 key=f"{a}:{b}:{c}"; now=time.monotonic()*1000; last=seen.get(key)
 if last and now-last[0]<75 and abs(net-last[1])<.05:return None
 seen[key]=(now,net)
 return {"type":"TRIANGULAR_CROSS","path":f"{c}->{a}->{b}->{c}","net_bps":net,"gross_bps":gross,"notional_usdt":n,"paper_only":True}

def scan_loop():
 while True:
  try:
   with lock:
    eq=state["equity"]; ids=list(dirty) if dirty else range(len(TRI)); dirty.clear()
   ops=[]
   for i in ids:
    x=triangular(*TRI[i],eq)
    if x:ops.append(x)
   ops.sort(key=lambda x:x["net_bps"],reverse=True)
   for x in ops[:MAX_ENTRIES]:
    with lock:
     n=min(x["notional_usdt"],max(.01,state["equity"]*RISK),MAX_NOTIONAL); p=n*x["net_bps"]/10000
     state["equity"]+=p; state["peak"]=max(state["peak"],state["equity"]); state["paper_pnl"]+=p; state["paper_entries"]+=1
     if p>=0:state["wins"]+=1
     else:state["losses"]+=1
   with lock: state["scans"]+=1; state["opportunities"]=len(ops); state["best"]=ops[0] if ops else state["best"]
  except Exception as e: state["errors"]+=1;state["last_error"]=str(e)
  time.sleep(SCAN)

def stats():
 with lock:
  e=state["equity"];return {"status":"ok","engine":"cryptoalpha-spot-cross-ultra","mode":"PAPER_ONLY","live_execution":False,"starting_equity":START,"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5),"paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"wins":state["wins"],"losses":state["losses"],"drawdown_pct":round(max(0,(state["peak"]-e)/state["peak"])*100,4),"scans":state["scans"],"errors":state["errors"],"opportunities":state["opportunities"],"best":state["best"],"ws_spot":state["ws_spot"],"ws_fut":False,"ws_connected":state["ws_spot"],"quote_updates":state["quote_updates"],"depth_updates":state["depth_updates"],"symbols":len(SYMS),"cross_routes":len(TRI),"last_error":state["last_error"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML='''<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Cryptoalpha Spot Cross Ultra</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1000px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between}.brand{font-size:24px;font-weight:800}.pill{padding:7px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:21px;font-weight:800;margin-top:5px}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Spot Cross Ultra</div><div>Event-driven ultra-fast spot triangular arbitrage</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Equity</div><div class=v>$<span id=e>—</span></div></div><div class=c><div class=l>Return</div><div class=v id=r>—</div></div><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Paper fills</div><div class=v id=f>—</div></div></div><div class=g><div class=c><div class=l>Cross opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Spot WS</div><div class=v id=w>—</div></div><div class=c><div class=l>Quotes</div><div class=v id=q>—</div></div><div class=c><div class=l>Depth</div><div class=v id=d>—</div></div></div><div class=c><b>ULTRA SPOT + CROSS</b><p>No Futures · persistent Spot WebSocket · event-driven route scanning · 100ms depth · 20ms fallback scan · in-memory books · monotonic stale protection · dynamic compounding sizing · low edge threshold · LIVE OFF.</p></div><div class=c><b>System</b><p id=s>Waiting…</p></div></div><script>async function u(){try{let x=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();e.textContent=Number(x.equity).toFixed(4);r.textContent=Number(x.compound_return_pct).toFixed(3)+'%';b.textContent=x.best?Number(x.best.net_bps).toFixed(2)+' bps · '+x.best.path:'—';f.textContent=x.paper_entries;o.textContent=x.opportunities;w.textContent=x.ws_spot?'CONNECTED':'RECONNECTING';q.textContent=x.quote_updates;d.textContent=x.depth_updates;s.textContent='SPOT ONLY · ultra event-driven · scans '+x.scans+' · errors '+x.errors+' · symbols '+x.symbols+' · routes '+x.cross_routes+' · uptime '+x.uptime_seconds+'s'+(x.last_error?' · '+x.last_error:'')}catch(z){s.textContent='Dashboard fetch error'}}u();setInterval(u,1000)</script>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  body=json.dumps(stats()).encode() if self.path.startswith('/stats.json') else (b'ok' if self.path.startswith('/healthz') else HTML.encode());self.send_response(200);self.send_header('Content-Type','application/json' if self.path.startswith('/stats.json') else 'text/html');self.end_headers();self.wfile.write(body)
 def log_message(self,*a):pass

def main():
 threading.Thread(target=ws_loop,daemon=True).start();threading.Thread(target=scan_loop,daemon=True).start();ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),H).serve_forever()

if __name__ == '__main__': main()
