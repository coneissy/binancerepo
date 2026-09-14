"""Cryptoalpha Fast Arbitrage v2 - paper only.
Uses Binance bookTicker WebSockets for live quotes and REST only for universe/volume metadata.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import websocket

START=float(os.getenv("SIM_START_EQUITY","10"))
RISK=min(.25,max(.01,float(os.getenv("ARB_RISK_PCT","0.10"))))
MIN_NET_BPS=float(os.getenv("ARB_MIN_NET_BPS","5"))
FEE_BPS=float(os.getenv("ARB_FEE_BPS","4"))
SLIP_BPS=float(os.getenv("ARB_SLIPPAGE_BPS","2"))
FUNDING_BPS=float(os.getenv("ARB_FUNDING_BUFFER_BPS","1"))
MIN_VOL=float(os.getenv("ARB_MIN_QUOTE_VOLUME","500000"))
MIN_DEPTH=float(os.getenv("ARB_MIN_DEPTH_USDT","100"))
SYMBOL_LIMIT=max(20,int(os.getenv("ARB_WS_SYMBOL_LIMIT","100")))
MAX_ENTRIES=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","20")))
MAX_DD=min(.25,max(.02,float(os.getenv("ARB_MAX_DRAWDOWN_PCT","0.08"))))
STALE_MS=max(500,int(os.getenv("ARB_STALE_MS","2000")))
MAX_GROSS_BPS=max(20,float(os.getenv("ARB_MAX_GROSS_BPS","150")))
QUOTE_SYNC_MS=max(100,int(os.getenv("ARB_QUOTE_SYNC_MS","500")))
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-arb-v2")
http=requests.Session();http.headers.update({"User-Agent":"Cryptoalpha-Arbitrage-WS/5.0"})
lock=threading.RLock()
state={"started":time.time(),"scans":0,"errors":0,"last_error":"","equity":START,"peak":START,"paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"opportunities":[],"best":None,"last_event":0,"ws_spot":False,"ws_fut":False,"symbols":[],"quote_updates":0}
books={"spot":{},"fut":{}}
volumes={"spot":{},"fut":{}}

def get_json(url):
    r=http.get(url,timeout=5);r.raise_for_status();return r.json()

def refresh_universe():
    global volumes
    bases=[("https://api.binance.com/api/v3/ticker/24hr","spot"),("https://fapi.binance.com/fapi/v1/ticker/24hr","fut")]
    data={}
    try:
        for url,k in bases:
            try:
                rows=get_json(url);data[k]={x["symbol"]:float(x.get("quoteVolume",0) or 0) for x in rows if x.get("symbol","").endswith("USDT")}
            except Exception as e:
                log.warning("UNIVERSE %s FAILED | %s",k,e)
                data[k]=dict(volumes.get(k,{}))
        if not data["spot"] or not data["fut"]: raise RuntimeError("empty universe")
        common=set(data["spot"])&set(data["fut"])
        ranked=sorted(common,key=lambda s:min(data["spot"][s],data["fut"][s]),reverse=True)
        chosen=[s for s in ranked if min(data["spot"][s],data["fut"][s])>=MIN_VOL][:SYMBOL_LIMIT]
        with lock:
            volumes=data;state["symbols"]=chosen
        log.info("UNIVERSE READY | symbols=%d | min_volume=$%.0f",len(chosen),MIN_VOL)
        return chosen
    except Exception as e:
        with lock:state["errors"]+=1;state["last_error"]=str(e)
        return list(state["symbols"])

def ws_url(base,symbols):
    streams="/".join(s.lower()+"@bookTicker" for s in symbols)
    return base+"/stream?streams="+streams

def ws_loop(kind,base):
    backoff=1
    while True:
        with lock:symbols=list(state["symbols"])
        if not symbols:
            refresh_universe();time.sleep(1);continue
        try:
            url=ws_url(base,symbols)
            def on_open(ws):
                nonlocal backoff
                backoff=1
                with lock:
                    state["ws_spot" if kind=="spot" else "ws_fut"]=True
                log.info("WS CONNECTED | %s | symbols=%d",kind,len(symbols))
            def on_message(ws,msg):
                try:
                    root=json.loads(msg);d=root.get("data",root);s=d.get("s")
                    if not s:return
                    b=float(d.get("b",0));a=float(d.get("a",0));bq=float(d.get("B",0));aq=float(d.get("A",0))
                    if min(b,a,bq,aq)<=0:return
                    now=time.time()*1000
                    with lock:
                        books[kind][s]=(b,a,bq,aq,now);state["last_event"]=now;state["quote_updates"]+=1
                except Exception:
                    pass
            def on_error(ws,e):
                with lock:state["errors"]+=1;state["last_error"]=str(e)
            def on_close(ws,*args):
                with lock:state["ws_spot" if kind=="spot" else "ws_fut"]=False
            websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(backoff);backoff=min(20,backoff*2)

def candidates():
    now=time.time()*1000;costs=2*(FEE_BPS+SLIP_BPS)+FUNDING_BPS;out=[]
    with lock:
        syms=list(state["symbols"]);sb=dict(books["spot"]);fb=dict(books["fut"]);sv=dict(volumes["spot"]);fv=dict(volumes["fut"])
    for s in syms:
        if min(sv.get(s,0),fv.get(s,0))<MIN_VOL:continue
        a=sb.get(s);b=fb.get(s)
        if not a or not b:continue
        age_s=now-a[4];age_f=now-b[4]
        if age_s>STALE_MS or age_f>STALE_MS or abs(a[4]-b[4])>QUOTE_SYNC_MS:continue
        bid_s,ask_s,bq_s,aq_s,_=a;bid_f,ask_f,bq_f,aq_f,_=b
        if bid_s>=ask_s or bid_f>=ask_f:continue
        for direction,buy,sell,buyq,sellq in (("SPOT_BUY_FUT_SELL",ask_s,bid_f,aq_s,bq_f),("FUT_BUY_SPOT_SELL",ask_f,bid_s,aq_f,bq_s)):
            gross=(sell/buy-1)*10000
            if gross<=0 or gross>MAX_GROSS_BPS:continue
            depth=min(buy*buyq,sell*sellq);net=gross-costs
            if depth<MIN_DEPTH or net<MIN_NET_BPS:continue
            balance=min(sv.get(s,0),fv.get(s,0))/max(max(sv.get(s,0),fv.get(s,0)),1)
            liq=min(1,math.log10(max(depth,1))/7)
            conf=min(.995,.50+min(net/500,.30)+.12*liq+.08*balance)
            out.append({"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,"depth_usdt":depth,"liquidity_usdt":min(sv.get(s,0),fv.get(s,0)),"confidence":conf,"paper_only":True,"observed_at":time.time()})
    out.sort(key=lambda x:(x["net_bps"]*x["confidence"],x["depth_usdt"]),reverse=True)
    return out

def paper_capture(opps):
    with lock:
        if state["halted"]:return 0
        e=state["equity"];p=state["peak"]
        if p and (p-e)/p>=MAX_DD:state["halted"]=True;return 0
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
        if now-last_universe>300:
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
        return {"status":"ok","engine":"cryptoalpha-arbitrage-websocket-v2","mode":"PAPER_ONLY","live_execution":False,"starting_equity":round(START,2),"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5) if START else 0,"paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"paper_pnl_pct":round(state["paper_pnl"]/START*100,5) if START else 0,"wins":state["wins"],"losses":state["losses"],"drawdown_pct":round(max(0,(p-e)/p)*100,4) if p else 0,"max_drawdown_pct":MAX_DD*100,"scans":state["scans"],"errors":state["errors"],"opportunities":len(state["opportunities"]),"best":state["best"],"top_opportunities":state["opportunities"],"min_net_bps":MIN_NET_BPS,"risk_pct":RISK*100,"max_entries_per_scan":MAX_ENTRIES,"repeat_mode":True,"ws_connected":state["ws_spot"] and state["ws_fut"],"ws_spot":state["ws_spot"],"ws_fut":state["ws_fut"],"ws_symbols":len(state["symbols"]),"quote_updates":state["quote_updates"],"stale_ms":STALE_MS,"quote_sync_ms":QUOTE_SYNC_MS,"max_gross_bps":MAX_GROSS_BPS,"last_event":state["last_event"],"last_error":state["last_error"],"halted":state["halted"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha Fast Arbitrage</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:26px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:22px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Fast Arbitrage</div><div>Real-time WebSocket opportunity engine v2</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Starting $10</div><div class=v>$<span id=st>10.00</span></div></div><div class=c><div class=l>Current compounded equity</div><div class=v>$<span id=eq>—</span></div></div><div class=c><div class=l>Compound return</div><div class=v id=rt>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=dd>—</div></div></div><div class=g><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Paper entries</div><div class=v id=e>—</div></div><div class=c><div class=l>WebSocket</div><div class=v id=w>—</div></div></div><div class=c><b>FAST MODE v2</b><p>Real-time bookTicker · 100ms evaluation · synchronized quotes · stale protection · abnormal-spread protection · repeated qualified entries · fixed current-equity sizing · no martingale.</p></div><div class=c><b>Top opportunities</b><div id=t>Waiting for WebSocket data…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('st').textContent=n(d.starting_equity);$('eq').textContent=n(d.equity);$('rt').textContent=n(d.compound_return_pct,3)+'%';$('dd').textContent=n(d.drawdown_pct,3)+'%';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('e').textContent=d.paper_entries;$('w').textContent=(d.ws_spot?'S':'—')+'/'+(d.ws_fut?'F':'—');$('z').textContent=(d.ws_connected?'WS CONNECTED':'WS RECONNECTING')+' · quotes '+d.quote_updates+' · scans '+d.scans+' · errors '+d.errors+' · uptime '+n(d.uptime_seconds,0)+'s · compounded P&L $'+n(d.equity-d.starting_equity,4);let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,0)+'</td><td>'+n(x.confidence*100,1)+'%</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Market</th><th>Direction</th><th>Gross</th><th>Net</th><th>Depth</th><th>Confidence</th></tr>'+q+'</table>':'No qualified opportunities.'}catch(e){$('z').textContent='OFFLINE '+e}}u();setInterval(u,1000)</script>"""

class H(BaseHTTPRequestHandler):
    def j(self,o):
        raw=json.dumps(o,separators=(',',':'),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        p=self.path.split('?',1)[0]
        if p=='/stats.json':self.j(stats())
        elif p in ('/','/stats'):
            raw=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        elif p in ('/health','/healthz'):self.j({'status':'ok','engine':'cryptoalpha-arbitrage-websocket-v2','paper_only':True,'live_execution':False,'ws_connected':state['ws_spot'] and state['ws_fut']})
        else:self.send_response(404);self.end_headers()
    def log_message(self,*args):return

def main():
    refresh_universe()
    threading.Thread(target=ws_loop,args=('spot','wss://stream.binance.com:9443'),daemon=True).start()
    threading.Thread(target=ws_loop,args=('fut','wss://fstream.binance.com'),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv('PORT','10000'));ThreadingHTTPServer(('0.0.0.0',port),H).serve_forever()

if __name__=='__main__':main()
