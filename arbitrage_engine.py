"""Cryptoalpha Arbitrage - fast paper-only WebSocket engine.
No private keys, order endpoints, or live execution.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import websocket

START=float(os.getenv("SIM_START_EQUITY","10"))
RISK=min(.25,max(.01,float(os.getenv("ARB_RISK_PCT","0.10"))))
MIN_NET_BPS=float(os.getenv("ARB_MIN_NET_BPS","7"))
FEE_BPS=float(os.getenv("ARB_FEE_BPS","4"))
SLIP_BPS=float(os.getenv("ARB_SLIPPAGE_BPS","2"))
FUNDING_BPS=float(os.getenv("ARB_FUNDING_BUFFER_BPS","1"))
MIN_VOL=float(os.getenv("ARB_MIN_QUOTE_VOLUME","1000000"))
MIN_DEPTH=float(os.getenv("ARB_MIN_DEPTH_USDT","500"))
SYMBOL_LIMIT=max(20,int(os.getenv("ARB_WS_SYMBOL_LIMIT","100")))
MAX_ENTRIES=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","50")))
MAX_DD=min(.25,max(.02,float(os.getenv("ARB_MAX_DRAWDOWN_PCT","0.08"))))
STALE_MS=max(250,int(os.getenv("ARB_STALE_MS","1500")))
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-arb-ws")
http=requests.Session();http.headers.update({"User-Agent":"Cryptoalpha-Arbitrage-WS/4.0"})
lock=threading.RLock()
state={"started":time.time(),"scans":0,"errors":0,"last_error":"","equity":START,"peak":START,"paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"opportunities":[],"best":None,"last_event":0,"ws_connected":False,"symbols":[]}
books={"spot":{},"fut":{}}
volumes={"spot":{},"fut":{}}

def get_json(url,params=None):
    r=http.get(url,params=params,timeout=5);r.raise_for_status();return r.json()

def refresh_universe():
    global volumes
    try:
        sv=get_json("https://api.binance.com/api/v3/ticker/24hr")
        fv=get_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
        s={x["symbol"]:float(x.get("quoteVolume",0) or 0) for x in sv if x.get("symbol") and x.get("symbol","").endswith("USDT")}
        f={x["symbol"]:float(x.get("quoteVolume",0) or 0) for x in fv if x.get("symbol") and x.get("symbol","").endswith("USDT")}
        common=set(s)&set(f)
        ranked=sorted(common,key=lambda x:min(s[x],f[x]),reverse=True)
        chosen=[x for x in ranked if min(s[x],f[x])>=MIN_VOL][:SYMBOL_LIMIT]
        with lock:
            volumes={"spot":s,"fut":f};state["symbols"]=chosen
        return chosen
    except Exception as e:
        with lock: state["errors"]+=1;state["last_error"]=str(e)
        return state["symbols"]

def ws_url(base, symbols):
    streams="/".join((s.lower()+"@bookTicker") for s in symbols)
    return base+"/stream?streams="+streams

def ws_loop(kind,base):
    backoff=1
    while True:
        symbols=refresh_universe() if kind=="spot" else list(state["symbols"])
        if not symbols:
            time.sleep(2);continue
        try:
            url=ws_url(base,symbols)
            def on_open(ws):
                nonlocal backoff
                backoff=1
                with lock:state["ws_connected"]=True
                log.info("WS CONNECTED | %s | symbols=%d",kind,len(symbols))
            def on_message(ws,msg):
                try:
                    d=json.loads(msg).get("data",msg);s=d.get("s")
                    if not s:return
                    now=time.time()*1000
                    book=(float(d.get("b",0)),float(d.get("a",0)),float(d.get("B",0)),float(d.get("A",0)),now)
                    with lock:books[kind][s]=book;state["last_event"]=now
                except Exception:pass
            def on_error(ws,e):
                with lock:state["errors"]+=1;state["last_error"]=str(e)
            def on_close(ws,*args):
                with lock:state["ws_connected"]=False
            websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(backoff);backoff=min(30,backoff*2)

def candidates():
    now=time.time()*1000;costs=2*(FEE_BPS+SLIP_BPS)+FUNDING_BPS;out=[]
    with lock:
        syms=list(state["symbols"]);sb=dict(books["spot"]);fb=dict(books["fut"]);sv=dict(volumes["spot"]);fv=dict(volumes["fut"])
    for s in syms:
        if min(sv.get(s,0),fv.get(s,0))<MIN_VOL:continue
        a=sb.get(s);b=fb.get(s)
        if not a or not b:continue
        if now-a[4]>STALE_MS or now-b[4]>STALE_MS:continue
        bid_s,ask_s,bq_s,aq_s,_=a;bid_f,ask_f,bq_f,aq_f,_=b
        if min(bid_s,ask_s,bid_f,ask_f)<=0 or bid_s>=ask_s or bid_f>=ask_f:continue
        for direction,buy,sell,buyq,sellq in (("SPOT_BUY_FUT_SELL",ask_s,bid_f,aq_s,bq_f),("FUT_BUY_SPOT_SELL",ask_f,bid_s,aq_f,bq_s)):
            gross=(sell/buy-1)*10000;depth=min(buy*buyq,sell*sellq);net=gross-costs
            if depth<MIN_DEPTH or net<MIN_NET_BPS:continue
            balance=min(sv.get(s,0),fv.get(s,0))/max(max(sv.get(s,0),fv.get(s,0)),1)
            liq=min(1,math.log10(max(depth,1))/7)
            conf=min(.995,.50+min(max(net,0)/500,.30)+.12*liq+.08*balance)
            out.append({"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,"depth_usdt":depth,"liquidity_usdt":min(sv.get(s,0),fv.get(s,0)),"confidence":conf,"paper_only":True,"observed_at":time.time()})
    out.sort(key=lambda x:(x["net_bps"]*x["confidence"],x["depth_usdt"]),reverse=True)
    return out

def paper_capture(opps):
    with lock:
        if state["halted"]:return 0
        if state["peak"] and (state["peak"]-state["equity"])/state["peak"]>=MAX_DD:
            state["halted"]=True;return 0
    n=0
    for op in opps[:min(MAX_ENTRIES,len(opps))]:
        with lock:e=state["equity"]
        notional=max(.01,e*RISK);pnl=notional*(op["net_bps"]/10000)
        with lock:
            if state["halted"]:break
            state["equity"]+=pnl;state["peak"]=max(state["peak"],state["equity"]);state["paper_pnl"]+=pnl;state["paper_entries"]+=1
            if pnl>=0:state["wins"]+=1
            else:state["losses"]+=1
        n+=1
        log.info("PAPER WS REPEAT | %s | %s | net=%.2f bps | compounded_notional=$%.4f | pnl=$%.6f",op["symbol"],op["direction"],op["net_bps"],notional,pnl)
    return n

def scan_loop():
    last_universe=0
    while True:
        now=time.time()
        if now-last_universe>60:
            refresh_universe();last_universe=now
        try:
            ops=candidates();captures=paper_capture(ops)
            with lock:
                state["scans"]+=1;state["opportunities"]=ops[:50];state["best"]=ops[0] if ops else None;state["last_error"]=""
            if captures or ops:log.info("WS SCAN | opportunities=%d | best_net=%.2f bps | repeated_entries=%d | halted=%s",len(ops),ops[0]["net_bps"] if ops else 0,captures,state["halted"])
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(.10)

def stats():
    with lock:
        e=state["equity"];p=state["peak"]
        return {"status":"ok","engine":"cryptoalpha-arbitrage-websocket","mode":"PAPER_ONLY","live_execution":False,"starting_equity":round(START,2),"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5) if START else 0,"paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"paper_pnl_pct":round(state["paper_pnl"]/START*100,5) if START else 0,"wins":state["wins"],"losses":state["losses"],"drawdown_pct":round(max(0,(p-e)/p)*100,4) if p else 0,"max_drawdown_pct":MAX_DD*100,"scans":state["scans"],"errors":state["errors"],"opportunities":len(state["opportunities"]),"best":state["best"],"top_opportunities":state["opportunities"],"min_net_bps":MIN_NET_BPS,"risk_pct":RISK*100,"max_entries_per_scan":MAX_ENTRIES,"repeat_mode":True,"ws_connected":state["ws_connected"],"ws_symbols":len(state["symbols"]),"stale_ms":STALE_MS,"last_event":state["last_event"],"last_error":state["last_error"],"halted":state["halted"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha Fast Arbitrage</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:26px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:22px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Fast Arbitrage</div><div>Real-time WebSocket repeated-entry paper engine</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Starting $10</div><div class=v>$<span id=st>10.00</span></div></div><div class=c><div class=l>Current compounded equity</div><div class=v>$<span id=eq>—</span></div></div><div class=c><div class=l>Compound return</div><div class=v id=rt>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=dd>—</div></div></div><div class=g><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Paper entries</div><div class=v id=e>—</div></div><div class=c><div class=l>WebSocket symbols</div><div class=v id=w>—</div></div></div><div class=c><b>FAST MODE</b><p>Live bookTicker WebSockets · ~100ms evaluation · stale quotes rejected · top liquid USDT pairs · repeated qualified entries · <b id=r>—</b>% current-equity risk · no martingale.</p></div><div class=c><b>Top opportunities</b><div id=t>Waiting for WebSocket data…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('st').textContent=n(d.starting_equity);$('eq').textContent=n(d.equity);$('rt').textContent=n(d.compound_return_pct,3)+'%';$('dd').textContent=n(d.drawdown_pct,3)+'%';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('e').textContent=d.paper_entries;$('w').textContent=d.ws_symbols;$('r').textContent=n(d.risk_pct,2);$('z').textContent=(d.ws_connected?'WS CONNECTED':'WS RECONNECTING')+' · scans '+d.scans+' · errors '+d.errors+' · uptime '+n(d.uptime_seconds,0)+'s · compounded P&L $'+n(d.equity-d.starting_equity,4);let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,0)+'</td><td>'+n(x.confidence*100,1)+'%</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Market</th><th>Direction</th><th>Gross</th><th>Net</th><th>Depth</th><th>Confidence</th></tr>'+q+'</table>':'No qualified opportunities.'}catch(e){$('z').textContent='OFFLINE '+e}}u();setInterval(u,1000)</script>"""
class H(BaseHTTPRequestHandler):
    def j(self,o):
        raw=json.dumps(o,separators=(',',':'),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        p=self.path.split('?',1)[0]
        if p=='/stats.json':self.j(stats())
        elif p in ('/','/stats'):
            raw=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(raw)
        elif p in ('/health','/healthz'):self.j({'status':'ok','engine':'cryptoalpha-arbitrage-websocket','paper_only':True,'live_execution':False,'ws_connected':state['ws_connected']})
        else:self.send_response(404);self.end_headers()
    def log_message(self,*a):pass

def main():
    threading.Thread(target=lambda:ws_loop('spot','wss://stream.binance.com:9443'),daemon=True).start()
    threading.Thread(target=lambda:ws_loop('fut','wss://fstream.binance.com'),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv('PORT','10000'));log.info('Cryptoalpha Fast Arbitrage :%d | WEBSOCKET | PAPER ONLY | COMPOUNDING FROM $10',port);ThreadingHTTPServer(('0.0.0.0',port),H).serve_forever()
if __name__=='__main__':main()
