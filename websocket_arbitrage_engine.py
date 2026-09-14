"""Cryptoalpha real-time paper arbitrage engine.
Uses Binance Spot/Futures bookTicker WebSockets for fast quote updates.
Paper only: no API keys, private endpoints, or live orders.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import websocket

REFRESH=max(.05,float(os.getenv("ARB_REFRESH_SECONDS","0.25")))
MIN_NET_BPS=float(os.getenv("ARB_MIN_NET_BPS","7"))
FEE_BPS=float(os.getenv("ARB_FEE_BPS","4"))
SLIP_BPS=float(os.getenv("ARB_SLIPPAGE_BPS","2"))
FUNDING_BPS=float(os.getenv("ARB_FUNDING_BUFFER_BPS","1"))
MIN_VOL=float(os.getenv("ARB_MIN_QUOTE_VOLUME","1000000"))
MIN_DEPTH=float(os.getenv("ARB_MIN_DEPTH_USDT","25"))
MAX_OPPS=max(10,int(os.getenv("ARB_MAX_OPPORTUNITIES","100")))
MAX_ENTRIES_PER_SCAN=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","20")))
START=float(os.getenv("SIM_START_EQUITY","10"))
BASE_RISK=min(.01,max(.0005,float(os.getenv("ARB_RISK_PCT","0.0025"))))
MAX_DD=min(.25,max(.02,float(os.getenv("ARB_MAX_DRAWDOWN_PCT","0.08"))))
MAX_QUOTE_AGE=float(os.getenv("ARB_MAX_QUOTE_AGE_SECONDS","1.5"))
MAX_NET_BPS=float(os.getenv("ARB_MAX_NET_BPS","150"))
PAPER_CAPTURE=max(0.0,min(1.0,float(os.getenv("ARB_PAPER_CAPTURE","0.10"))))
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-ws-arb")
http=requests.Session(); http.headers.update({"User-Agent":"Cryptoalpha-WebSocket-Arbitrage/1.0"})
lock=threading.RLock()
books={"spot":{},"fut":{}}
vols={"spot":{},"fut":{}}
last_market_refresh=0.0
state={"started":time.time(),"scans":0,"errors":0,"ws_reconnects":0,"last_scan":0.0,"last_error":"","opportunities":[],"best":None,
       "equity":START,"peak":START,"paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"captures_last_scan":0,
       "spot_ws":"CONNECTING","fut_ws":"CONNECTING","quote_updates":0}

SPOT_WS="wss://stream.binance.com:9443/ws/!bookTicker"
FUT_WS="wss://fstream.binance.com/ws/!bookTicker"
SPOT_API=["https://api.binance.com","https://api1.binance.com","https://api2.binance.com"]
FUT_API=["https://fapi.binance.com","https://fapi1.binance.com","https://fapi2.binance.com","https://fapi3.binance.com"]

def f(v):
    try:return float(v)
    except:return 0.0

def bps(v):return v*10000.0

def get_any(bases,path):
    last=None
    for base in bases:
        try:
            r=http.get(base+path,timeout=5); r.raise_for_status(); return r.json()
        except Exception as e:last=e
    raise last or RuntimeError("all endpoints failed")

def refresh_volumes():
    global last_market_refresh
    now=time.time()
    if now-last_market_refresh<60:return
    for venue,bases,path in (("spot",SPOT_API,"/api/v3/ticker/24hr"),("fut",FUT_API,"/fapi/v1/ticker/24hr")):
        try:
            data=get_any(bases,path)
            with lock: vols[venue]={x["symbol"]:f(x.get("quoteVolume")) for x in data if x.get("symbol")}
        except Exception as e:
            with lock: state["errors"]+=1;state["last_error"]=str(e)
    last_market_refresh=now

def on_book(venue,raw):
    try:
        d=json.loads(raw); s=d.get("s") or d.get("symbol")
        if not s:return
        bid,ask,bq,aq=f(d.get("b") or d.get("bidPrice")),f(d.get("a") or d.get("askPrice")),f(d.get("B") or d.get("bidQty")),f(d.get("A") or d.get("askQty"))
        if min(bid,ask,bq,aq)<=0:return
        with lock:
            books[venue][s]=(bid,ask,bq,aq,time.time());state["quote_updates"]+=1
    except Exception as e:
        with lock:state["errors"]+=1;state["last_error"]=str(e)

def ws_worker(venue,url):
    while True:
        def opened(ws):
            with lock:state[venue+"_ws"]="CONNECTED"
            log.info("%s WebSocket connected",venue.upper())
        def closed(ws,*args):
            with lock:state[venue+"_ws"]="DISCONNECTED";state["ws_reconnects"]+=1
        def errored(ws,err):
            with lock:state[venue+"_ws"]="ERROR";state["last_error"]=str(err)
        try:
            ws=websocket.WebSocketApp(url,on_open=opened,on_message=lambda w,m:on_book(venue,m),on_error=errored,on_close=closed)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with lock:state[venue+"_ws"]="ERROR";state["last_error"]=str(e);state["ws_reconnects"]+=1
        time.sleep(1.0)

def candidates():
    refresh_volumes(); now=time.time(); out=[]; costs=2*(FEE_BPS+SLIP_BPS)+FUNDING_BPS
    with lock: common=set(books["spot"]) & set(books["fut"]); sb=dict(books["spot"]); fb=dict(books["fut"]); sv=dict(vols["spot"]);fv=dict(vols["fut"])
    for s in common:
        if min(sv.get(s,0),fv.get(s,0))<MIN_VOL:continue
        sp=sb[s];ft=fb[s]
        if now-sp[4]>MAX_QUOTE_AGE or now-ft[4]>MAX_QUOTE_AGE:continue
        bid_s,ask_s,bq_s,aq_s=sp[:4];bid_f,ask_f,bq_f,aq_f=ft[:4]
        if bid_s>=ask_s or bid_f>=ask_f:continue
        for direction,buy,sell,buyq,sellq in (("SPOT_BUY_FUT_SELL",ask_s,bid_f,aq_s,bq_f),("FUT_BUY_SPOT_SELL",ask_f,bid_s,aq_f,bq_s)):
            gross=bps(sell/buy-1); depth=min(buy*buyq,sell*sellq); net=gross-costs
            if depth<MIN_DEPTH or net<MIN_NET_BPS or net>MAX_NET_BPS:continue
            balance=min(sv.get(s,0),fv.get(s,0))/max(max(sv.get(s,0),fv.get(s,0)),1)
            liq=min(1,math.log10(max(depth,1))/6)
            conf=min(.995,.50+min(max(net,0)/500,.30)+.12*liq+.08*balance)
            out.append({"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,"depth_usdt":depth,
                        "liquidity_usdt":min(sv.get(s,0),fv.get(s,0)),"confidence":conf,"paper_only":True,"observed_at":now})
    out.sort(key=lambda x:(x["net_bps"]*x["confidence"],x["depth_usdt"]),reverse=True)
    return out

def paper_capture(opps):
    with lock:
        if state["halted"]:return 0
        e=state["equity"];p=state["peak"]
        if p and (p-e)/p>=MAX_DD:state["halted"]=True;return 0
    captures=0
    for best in opps[:min(MAX_ENTRIES_PER_SCAN,len(opps))]:
        with lock:e=state["equity"]
        notional=max(0.01,e*BASE_RISK)
        pnl=notional*(best["net_bps"]/10000)*PAPER_CAPTURE
        with lock:
            if state["halted"]:break
            state["equity"]+=pnl;state["peak"]=max(state["peak"],state["equity"]);state["paper_pnl"]+=pnl;state["paper_entries"]+=1
            if pnl>=0:state["wins"]+=1
            else:state["losses"]+=1
        captures+=1
        log.info("PAPER WS ARB REPEAT | %s | %s | net=%.2f bps | notional=$%.4f | simulated_pnl=$%.6f",best["symbol"],best["direction"],best["net_bps"],notional,pnl)
    with lock:state["captures_last_scan"]=captures
    return captures

def scan_loop():
    while True:
        try:
            opps=candidates()
            with lock:
                state["scans"]+=1;state["last_scan"]=time.time();state["last_error"]="";state["opportunities"]=opps[:MAX_OPPS];state["best"]=opps[0] if opps else None
            paper_capture(opps)
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
            log.warning("WS ARB SCAN FAILED | %s",e)
        time.sleep(REFRESH)

def stats():
    with lock:
        e=state["equity"];p=state["peak"];o=list(state["opportunities"]);best=state["best"]
        return {"status":"ok","engine":"cryptoalpha-websocket-arbitrage","mode":"PAPER_ONLY","live_execution":False,"uptime_seconds":round(time.time()-state["started"],1),"refresh_seconds":REFRESH,
                "scans":state["scans"],"errors":state["errors"],"ws_reconnects":state["ws_reconnects"],"quote_updates":state["quote_updates"],"spot_ws":state["spot_ws"],"fut_ws":state["fut_ws"],"last_scan":state["last_scan"],"last_error":state["last_error"],
                "opportunities":len(o),"best":best,"top_opportunities":o,"min_net_bps":MIN_NET_BPS,"max_net_bps":MAX_NET_BPS,"cost_model":{"fee_bps_per_leg":FEE_BPS,"slippage_bps_per_leg":SLIP_BPS,"funding_buffer_bps":FUNDING_BPS},
                "starting_equity":START,"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,6) if START else 0,"peak_equity":round(p,6),"drawdown_pct":round(max(0,(p-e)/p)*100,4) if p else 0,"max_drawdown_pct":MAX_DD*100,
                "base_risk_pct":BASE_RISK*100,"max_entries_per_scan":MAX_ENTRIES_PER_SCAN,"paper_capture_factor":PAPER_CAPTURE,"repeat_mode":True,"paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"paper_pnl_pct":round(state["paper_pnl"]/START*100,6) if START else 0,"wins":state["wins"],"losses":state["losses"],"halted":state["halted"]}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha WebSocket Arbitrage</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:26px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.hero{grid-column:span 2}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:22px;font-weight:800;margin-top:5px}.hero .v{font-size:30px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:800px){.g{grid-template-columns:repeat(2,1fr)}.hero{grid-column:span 2}}@media(max-width:500px){.g{grid-template-columns:1fr}.hero{grid-column:span 1}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Arbitrage</div><div>Real-time WebSocket repeated-entry paper engine</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class='c hero'><div class=l>Starting $10 → Current Compounded Equity</div><div class=v id=eq>$10.000000</div><div id=ret>0.000000%</div></div><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Paper entries</div><div class=v id=e>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=d>—</div></div><div class=c><div class=l>WebSocket</div><div class=v id=w>—</div></div></div><div class=c><b>Fast engine</b><p>Live bookTicker streams · sub-second reaction · repeated qualified entries · up to <b id=r>—</b>/scan · fixed percentage sizing · no martingale · max DD <b id=q>—</b>.</p></div><div class=c><b>Top opportunities</b><div id=t>Waiting for WebSocket data…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('eq').textContent='$'+n(d.equity,6);$('ret').textContent=n(d.compound_return_pct,4)+'% compounded';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('e').textContent=d.paper_entries;$('d').textContent=n(d.drawdown_pct,3)+'%';$('w').textContent=d.spot_ws+'/'+d.fut_ws;$('r').textContent=d.max_entries_per_scan;$('q').textContent=n(d.max_drawdown_pct,1)+'%';$('z').textContent='Uptime '+n(d.uptime_seconds,0)+'s · quote updates '+d.quote_updates+' · scans '+d.scans+' · reconnects '+d.ws_reconnects+' · paper P&L $'+n(d.paper_pnl,6)+' ('+n(d.paper_pnl_pct,4)+'%) · errors '+d.errors;let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,2)+'</td><td>'+n(x.confidence*100,1)+'%</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Market</th><th>Direction</th><th>Gross</th><th>Net</th><th>Depth</th><th>Confidence</th></tr>'+q+'</table>':'No qualified opportunities.'}catch(e){$('z').textContent='OFFLINE '+e}}u();setInterval(u,1000)</script>"""
class H(BaseHTTPRequestHandler):
    def j(self,o):
        raw=json.dumps(o,separators=(',',':'),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        p=self.path.split('?',1)[0]
        if p=='/stats.json':self.j(stats())
        elif p in ('/','/stats'):
            raw=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(raw)
        elif p in ('/health','/healthz'):self.j({'status':'ok','engine':'cryptoalpha-websocket-arbitrage','paper_only':True,'live_execution':False,'spot_ws':state['spot_ws'],'fut_ws':state['fut_ws']})
        else:self.send_response(404);self.end_headers()
    def log_message(self,*a):pass

def main():
    threading.Thread(target=ws_worker,args=("spot",SPOT_WS),daemon=True).start()
    threading.Thread(target=ws_worker,args=("fut",FUT_WS),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv('PORT','10000'));log.info('Cryptoalpha WebSocket dashboard :%d | PAPER ONLY | REAL-TIME REPEAT MODE',port);ThreadingHTTPServer(('0.0.0.0',port),H).serve_forever()
if __name__=='__main__':main()
