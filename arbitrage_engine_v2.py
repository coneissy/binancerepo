"""
Cryptoalpha Arbitrage v2 - fast realistic paper execution, LIVE OFF.
Direct multiplexed Binance WebSockets; no REST universe polling loop.
"""
import json, logging, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websocket

START=float(os.getenv("SIM_START_EQUITY","10"))
RISK=max(.01,min(.25,float(os.getenv("ARB_RISK_PCT","0.25"))))
MIN_NET_BPS=max(0,float(os.getenv("ARB_MIN_NET_BPS","0")))
FEE_BPS=max(0,float(os.getenv("ARB_FEE_BPS","4")))
FUNDING_BPS=max(0,float(os.getenv("ARB_FUNDING_BUFFER_BPS","1")))
MIN_DEPTH=max(.01,float(os.getenv("ARB_MIN_DEPTH_USDT","10")))
SYMBOL_LIMIT=max(20,int(os.getenv("ARB_WS_SYMBOL_LIMIT","100")))
MAX_ENTRIES=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","200")))
MAX_NOTIONAL=max(.01,float(os.getenv("ARB_MAX_NOTIONAL_USDT","1000")))
STALE_MS=max(100,int(os.getenv("ARB_STALE_MS","750")))
QUOTE_SYNC_MS=max(25,int(os.getenv("ARB_QUOTE_SYNC_MS","150")))
DEPTH_LEVELS=max(1,int(os.getenv("ARB_DEPTH_LEVELS","5")))
DEPTH_USE_PCT=min(.5,max(.01,float(os.getenv("ARB_DEPTH_USE_PCT","0.20"))))
LATENCY_BPS=max(0,float(os.getenv("ARB_LATENCY_BPS","0.5")))
DEDUP_MS=max(25,int(os.getenv("ARB_DEDUP_MS","150")))
MIN_REPRICE_BPS=max(0,float(os.getenv("ARB_MIN_REPRICE_BPS","0.05")))
MAX_GROSS_BPS=max(20,float(os.getenv("ARB_MAX_GROSS_BPS","150")))
SCAN_INTERVAL=max(.02,float(os.getenv("ARB_SCAN_INTERVAL","0.05")))
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-arb-v2")
lock=threading.RLock()
state={"started":time.time(),"scans":0,"errors":0,"last_error":"","equity":START,"peak":START,
       "paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"opportunities":[],"best":None,
       "last_event":0,"ws_spot":False,"ws_fut":False,"symbols":[],"quote_updates":0,"depth_updates":0,
       "dedup_skips":0,"fills":0}
books={"spot":{},"fut":{}}
depths={"spot":{},"fut":{}}
seen={}
UNIVERSE=[x for x in """
BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT LINKUSDT DOTUSDT
TRXUSDT LTCUSDT BCHUSDT UNIUSDT NEARUSDT APTUSDT SUIUSDT FILUSDT ARBUSDT OPUSDT
INJUSDT SEIUSDT TIAUSDT PEPEUSDT WIFUSDT BONKUSDT FLOKIUSDT SHIBUSDT ETCUSDT
ATOMUSDT ICPUSDT XLMUSDT AAVEUSDT ALGOUSDT FTMUSDT RUNEUSDT MKRUSDT CRVUSDT
MATICUSDT POLUSDT JUPUSDT WLDUSDT ENAUSDT NOTUSDT TONUSDT TAOUSDT STXUSDT
GRTUSDT IMXUSDT LDOUSDT SANDUSDT MANAUSDT AXSUSDT THETAUSDT EOSUSDT
HBARUSDT VETUSDT IOTAUSDT KASUSDT PYTHUSDT JTOUSDT STRKUSDT ZKUSDT
ORDIUSDT 1000PEPEUSDT 1000SHIBUSDT 1000BONKUSDT 1000FLOKIUSDT
""".split()][:SYMBOL_LIMIT]
state["symbols"]=UNIVERSE

def stream_url(base,symbols):
    streams=[]
    for s in symbols:
        streams += [s.lower()+"@bookTicker", s.lower()+"@depth5@100ms"]
    return base + "/stream?streams=" + "/".join(streams)

def ws_loop(market,base):
    backoff=1
    url=stream_url(base,UNIVERSE)
    while True:
        try:
            def on_open(ws):
                nonlocal backoff
                backoff=1
                with lock: state["ws_spot" if market=="spot" else "ws_fut"]=True
                log.info("WS CONNECTED | %s | streams=%d",market,len(UNIVERSE)*2)
            def on_message(ws,msg):
                try:
                    root=json.loads(msg); d=root.get("data",root); s=d.get("s")
                    if not s:return
                    now=time.time()*1000; stream=(root.get("stream") or "")
                    with lock:
                        if "@depth" in stream:
                            bids=[(float(x[0]),float(x[1])) for x in d.get("b",[])[:DEPTH_LEVELS] if float(x[0])>0 and float(x[1])>0]
                            asks=[(float(x[0]),float(x[1])) for x in d.get("a",[])[:DEPTH_LEVELS] if float(x[0])>0 and float(x[1])>0]
                            if bids and asks:depths[market][s]=(bids,asks,now);state["depth_updates"]+=1
                        elif "@bookTicker" in stream:
                            b=float(d.get("b",0));a=float(d.get("a",0));bq=float(d.get("B",0));aq=float(d.get("A",0))
                            if min(b,a,bq,aq)>0:
                                books[market][s]=(b,a,bq,aq,now);state["quote_updates"]+=1;state["last_event"]=now
                except Exception:pass
            def on_error(ws,e):
                with lock:state["errors"]+=1;state["last_error"]=str(e)
                log.warning("WS ERROR | %s | %s",market,e)
            def on_close(ws,*args):
                with lock:state["ws_spot" if market=="spot" else "ws_fut"]=False
                log.warning("WS CLOSED | %s",market)
            websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(backoff);backoff=min(30,backoff*2)

def fill_vwap(levels,notional):
    rem=notional;qty=spent=0.0
    for price,q in levels:
        take=min(q,rem/price)
        if take<=0:continue
        spent+=take*price;qty+=take;rem-=take*price
        if rem<=1e-9:break
    return (spent/qty,qty,spent) if qty>0 and rem<=1e-8 else None

def candidate(s,direction,equity):
    now=time.time()*1000
    sb=books["spot"].get(s);fb=books["fut"].get(s);sd=depths["spot"].get(s);fd=depths["fut"].get(s)
    if not(sb and fb and sd and fd):return None
    if max(now-sb[4],now-fb[4],now-sd[2],now-fd[2])>STALE_MS:return None
    if max(abs(sb[4]-fb[4]),abs(sd[2]-fd[2]))>QUOTE_SYNC_MS:return None
    buy_levels,sell_levels=(sd[1],fd[0]) if direction=="SPOT_BUY_FUT_SELL" else (fd[1],sd[0])
    raw=min(sum(p*q for p,q in buy_levels),sum(p*q for p,q in sell_levels))
    if raw<MIN_DEPTH:return None
    target=min(max(.01,equity*RISK),raw*DEPTH_USE_PCT,MAX_NOTIONAL)
    buy=fill_vwap(buy_levels,target);sell=fill_vwap(sell_levels,target)
    if not(buy and sell):return None
    gross=(sell[0]/buy[0]-1)*10000
    if gross<=0 or gross>MAX_GROSS_BPS:return None
    net=gross-2*FEE_BPS-FUNDING_BPS-LATENCY_BPS
    if net<=MIN_NET_BPS:return None
    key=s+":"+direction;last=seen.get(key)
    if last and now-last[0]<DEDUP_MS and abs(net-last[1])<MIN_REPRICE_BPS:
        with lock:state["dedup_skips"]+=1
        return None
    seen[key]=(now,net)
    return {"symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,"notional_usdt":target,"buy_vwap":buy[0],"sell_vwap":sell[0],"depth_usdt":raw,"quote_age_ms":max(now-sb[4],now-fb[4],now-sd[2],now-fd[2]),"paper_only":True}

def paper_capture(ops):
    with lock:
        if state["halted"]:return 0
    n=0
    for op in ops[:MAX_ENTRIES]:
        with lock:
            e=state["equity"];notional=min(op["notional_usdt"],max(.01,e*RISK),MAX_NOTIONAL);pnl=notional*op["net_bps"]/10000
            state["equity"]+=pnl;state["peak"]=max(state["peak"],state["equity"]);state["paper_pnl"]+=pnl;state["paper_entries"]+=1;state["fills"]+=1
            if pnl>=0:state["wins"]+=1
            else:state["losses"]+=1
        n+=1
    return n

def scan_loop():
    while True:
        try:
            with lock:e=state["equity"]
            ops=[]
            for s in UNIVERSE:
                for d in ("SPOT_BUY_FUT_SELL","FUT_BUY_SPOT_SELL"):
                    x=candidate(s,d,e)
                    if x:ops.append(x)
            ops.sort(key=lambda x:(x["net_bps"],x["notional_usdt"]),reverse=True)
            captures=paper_capture(ops)
            with lock:
                state["scans"]+=1;state["opportunities"]=ops[:100];state["best"]=ops[0] if ops else None
                if state["ws_spot"] and state["ws_fut"]:state["last_error"]=""
            if captures:log.info("EXEC SCAN | opportunities=%d | fills=%d | best_net=%.2f bps",len(ops),captures,ops[0]["net_bps"])
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(SCAN_INTERVAL)

def stats():
    with lock:
        e=state["equity"];p=state["peak"]
        return {"status":"ok","engine":"cryptoalpha-arbitrage-websocket-v2-realistic","mode":"PAPER_ONLY","live_execution":False,"starting_equity":START,"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5) if START else 0,"paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"wins":state["wins"],"losses":state["losses"],"drawdown_pct":round(max(0,(p-e)/p)*100,4) if p else 0,"scans":state["scans"],"errors":state["errors"],"opportunities":len(state["opportunities"]),"best":state["best"],"top_opportunities":state["opportunities"],"min_net_bps":MIN_NET_BPS,"risk_pct":RISK*100,"max_entries_per_scan":MAX_ENTRIES,"execution_model":"depth_vwap_two_leg","depth_levels":DEPTH_LEVELS,"depth_use_pct":DEPTH_USE_PCT*100,"max_notional":MAX_NOTIONAL,"fee_bps_per_leg":FEE_BPS,"funding_bps":FUNDING_BPS,"latency_bps":LATENCY_BPS,"dedup_ms":DEDUP_MS,"ws_connected":state["ws_spot"] and state["ws_fut"],"ws_spot":state["ws_spot"],"ws_fut":state["ws_fut"],"ws_symbols":len(UNIVERSE),"quote_updates":state["quote_updates"],"depth_updates":state["depth_updates"],"dedup_skips":state["dedup_skips"],"stale_ms":STALE_MS,"quote_sync_ms":QUOTE_SYNC_MS,"max_gross_bps":MAX_GROSS_BPS,"last_event":state["last_event"],"last_error":state["last_error"],"halted":state["halted"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha Arbitrage v2</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between}.brand{font-size:24px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:21px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Arbitrage v2</div><div>Fast executable-depth paper simulator</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Starting</div><div class=v>$<span id=st>10</span></div></div><div class=c><div class=l>Equity</div><div class=v>$<span id=eq>—</span></div></div><div class=c><div class=l>Compound return</div><div class=v id=rt>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=dd>—</div></div></div><div class=g><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Executed paper fills</div><div class=v id=e>—</div></div><div class=c><div class=l>WebSockets</div><div class=v id=w>—</div></div></div><div class=c><b>FAST EXECUTION MODEL</b><p>Direct multiplexed WebSockets · Depth-5 VWAP · 100ms depth · 50ms scan loop · low edge threshold · dynamic sizing · all eligible entries up to configured risk/notional · no REST discovery loop · no martingale.</p></div><div class=c><b>Top executable opportunities</b><div id=t>Waiting for live depth…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('st').textContent=n(d.starting_equity);$('eq').textContent=n(d.equity);$('rt').textContent=n(d.compound_return_pct,3)+'%';$('dd').textContent=n(d.drawdown_pct,3)+'%';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('e').textContent=d.paper_entries;$('w').textContent=(d.ws_spot?'S':'—')+'/'+(d.ws_fut?'F':'—');$('z').textContent=(d.ws_connected?'WS CONNECTED':'WS RECONNECTING')+' · quotes '+d.quote_updates+' · depth '+d.depth_updates+' · scans '+d.scans+' · errors '+d.errors+' · uptime '+n(d.uptime_seconds,0)+'s · P&L $'+n(d.equity-d.starting_equity,4);let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.notional_usdt,2)+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>'+n(x.quote_age_ms,0)+'ms</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Symbol</th><th>Direction</th><th>Notional</th><th>Gross</th><th>Net</th><th>Age</th></tr>'+q+'</table>':'No executable positive-net opportunities yet.'}catch(e){$('z').textContent='stats error '+e}}u();setInterval(u,1000)</script>"""

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/stats.json'):
            b=json.dumps(stats()).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(b);return
        if self.path.startswith('/healthz'):
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps(stats()).encode());return
        b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(b)
    def log_message(self,*a):pass

def main():
    threading.Thread(target=ws_loop,args=("spot","wss://stream.binance.com:9443"),daemon=True).start()
    threading.Thread(target=ws_loop,args=("fut","wss://fstream.binance.com"),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv("PORT","10000"));ThreadingHTTPServer(("0.0.0.0",port),H).serve_forever()

if __name__=="__main__":main()
