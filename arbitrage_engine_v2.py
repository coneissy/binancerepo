"""Cryptoalpha Arbitrage v2 - realistic paper execution, LIVE OFF.
Uses Binance spot/futures depth streams and simulates two-leg executable fills.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import websocket

START=float(os.getenv("SIM_START_EQUITY","10"))
RISK=min(.25,max(.01,float(os.getenv("ARB_RISK_PCT","0.10"))))
MIN_NET_BPS=float(os.getenv("ARB_MIN_NET_BPS","0"))
FEE_BPS=float(os.getenv("ARB_FEE_BPS","4"))
FUNDING_BPS=float(os.getenv("ARB_FUNDING_BUFFER_BPS","1"))
MIN_VOL=float(os.getenv("ARB_MIN_QUOTE_VOLUME","500000"))
MIN_DEPTH=float(os.getenv("ARB_MIN_DEPTH_USDT","100"))
SYMBOL_LIMIT=max(20,int(os.getenv("ARB_WS_SYMBOL_LIMIT","100")))
MAX_ENTRIES=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","20")))
MAX_DD=min(.25,max(.02,float(os.getenv("ARB_MAX_DRAWDOWN_PCT","0.08"))))
STALE_MS=max(250,int(os.getenv("ARB_STALE_MS","1000")))
QUOTE_SYNC_MS=max(50,int(os.getenv("ARB_QUOTE_SYNC_MS","250")))
DEPTH_LEVELS=max(1,int(os.getenv("ARB_DEPTH_LEVELS","5")))
DEPTH_USE_PCT=min(.50,max(.01,float(os.getenv("ARB_DEPTH_USE_PCT","0.20"))))
MAX_NOTIONAL=max(.01,float(os.getenv("ARB_MAX_NOTIONAL_USDT","1000")))
LATENCY_BPS=max(0,float(os.getenv("ARB_LATENCY_BPS","1")))
DEDUP_MS=max(100,int(os.getenv("ARB_DEDUP_MS","1000")))
MIN_REPRICE_BPS=max(0,float(os.getenv("ARB_MIN_REPRICE_BPS","0.5")))
MAX_GROSS_BPS=max(20,float(os.getenv("ARB_MAX_GROSS_BPS","150")))
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-arb-v2")
http=requests.Session();http.headers.update({"User-Agent":"Cryptoalpha-Arbitrage-WS/6.0"})
lock=threading.RLock()
state={"started":time.time(),"scans":0,"errors":0,"last_error":"","equity":START,"peak":START,
       "paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"opportunities":[],"best":None,
       "last_event":0,"ws_spot":False,"ws_fut":False,"symbols":[],"quote_updates":0,"depth_updates":0,
       "dedup_skips":0,"fills":0}
books={"spot":{},"fut":{}}
depths={"spot":{},"fut":{}}
volumes={"spot":{},"fut":{}}
seen={}

def get_json(url):
    r=http.get(url,timeout=5);r.raise_for_status();return r.json()

def refresh_universe():
    global volumes
    bases=[("https://api.binance.com/api/v3/ticker/24hr","spot"),("https://fapi.binance.com/fapi/v1/ticker/24hr","fut")]
    data={}
    try:
        for url,k in bases:
            try:
                rows=get_json(url)
                data[k]={x["symbol"]:float(x.get("quoteVolume",0) or 0) for x in rows if x.get("symbol","").endswith("USDT")}
            except Exception as e:
                log.warning("UNIVERSE %s FAILED | %s",k,e);data[k]=dict(volumes.get(k,{}))
        if not data["spot"] or not data["fut"]:raise RuntimeError("empty universe")
        common=set(data["spot"])&set(data["fut"])
        ranked=sorted(common,key=lambda s:min(data["spot"][s],data["fut"][s]),reverse=True)
        chosen=[s for s in ranked if min(data["spot"][s],data["fut"][s])>=MIN_VOL][:SYMBOL_LIMIT]
        with lock:volumes=data;state["symbols"]=chosen
        log.info("UNIVERSE READY | symbols=%d | min_volume=$%.0f",len(chosen),MIN_VOL)
        return chosen
    except Exception as e:
        with lock:state["errors"]+=1;state["last_error"]=str(e)
        return list(state["symbols"])

def stream_url(base,symbols,kind):
    suffix="@depth5@100ms" if kind=="depth" else "@bookTicker"
    return base+"/stream?streams="+"/".join(s.lower()+suffix for s in symbols)

def ws_loop(kind,base):
    backoff=1
    while True:
        with lock:symbols=list(state["symbols"])
        if not symbols:refresh_universe();time.sleep(1);continue
        try:
            url=stream_url(base,symbols,"depth" if kind.endswith("_depth") else "book")
            market="spot" if kind.startswith("spot") else "fut"
            def on_open(ws):
                nonlocal backoff
                backoff=1
                with lock:state["ws_spot" if market=="spot" else "ws_fut"]=True
                log.info("WS CONNECTED | %s | symbols=%d",kind,len(symbols))
            def on_message(ws,msg):
                try:
                    root=json.loads(msg);d=root.get("data",root);s=d.get("s")
                    if not s:return
                    now=time.time()*1000
                    with lock:
                        if kind.endswith("_depth"):
                            bids=[(float(x[0]),float(x[1])) for x in d.get("b",[])[:DEPTH_LEVELS] if float(x[0])>0 and float(x[1])>0]
                            asks=[(float(x[0]),float(x[1])) for x in d.get("a",[])[:DEPTH_LEVELS] if float(x[0])>0 and float(x[1])>0]
                            if bids and asks:depths[market][s]=(bids,asks,now);state["depth_updates"]+=1
                        else:
                            b=float(d.get("b",0));a=float(d.get("a",0));bq=float(d.get("B",0));aq=float(d.get("A",0))
                            if min(b,a,bq,aq)>0:books[market][s]=(b,a,bq,aq,now);state["last_event"]=now;state["quote_updates"]+=1
                except Exception:pass
            def on_error(ws,e):
                with lock:state["errors"]+=1;state["last_error"]=str(e)
            def on_close(ws,*args):
                with lock:state["ws_spot" if market=="spot" else "ws_fut"]=False
            websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(backoff);backoff=min(20,backoff*2)

def fill_vwap(levels,notional):
    """Return average executable price, quantity consumed, and quote notional used."""
    remaining=notional;qty=0.0;spent=0.0
    for price,base_qty in levels:
        take=min(base_qty,remaining/price)
        if take<=0:continue
        spent+=take*price;qty+=take;remaining-=take*price
        if remaining<=1e-9:break
    if qty<=0 or remaining>1e-8:return None
    return spent/qty,qty,spent

def candidate_for(s,direction,equity,sv,fv):
    now=time.time()*1000
    sb=books["spot"].get(s);fb=books["fut"].get(s);sd=depths["spot"].get(s);fd=depths["fut"].get(s)
    if not sb or not fb or not sd or not fd:return None
    if now-sb[4]>STALE_MS or now-fb[4]>STALE_MS or now-sd[2]>STALE_MS or now-fd[2]>STALE_MS:return None
    if max(abs(sb[4]-fb[4]),abs(sd[2]-fd[2]))>QUOTE_SYNC_MS:return None
    if direction=="SPOT_BUY_FUT_SELL":
        buy_levels=sd[1];sell_levels=fd[0]
    else:
        buy_levels=fd[1];sell_levels=sd[0]
    raw_depth=min(sum(p*q for p,q in buy_levels),sum(p*q for p,q in sell_levels))
    if raw_depth<MIN_DEPTH:return None
    target=min(max(.01,equity*RISK),raw_depth*DEPTH_USE_PCT,MAX_NOTIONAL)
    if target<.01:return None
    buy=fill_vwap(buy_levels,target);sell=fill_vwap(sell_levels,target)
    if not buy or not sell:return None
    gross=(sell[0]/buy[0]-1)*10000
    if gross<=0 or gross>MAX_GROSS_BPS:return None
    net=gross-2*FEE_BPS-FUNDING_BPS-LATENCY_BPS
    if net<=MIN_NET_BPS:return None
    key=f"{s}:{direction}";last=seen.get(key)
    if last and now-last[0]<DEDUP_MS and abs(net-last[1])<MIN_REPRICE_BPS:return None
    seen[key]=(now,net)
    return {"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,
            "notional_usdt":target,"buy_vwap":buy[0],"sell_vwap":sell[0],"depth_usdt":raw_depth,
            "liquidity_usdt":min(sv.get(s,0),fv.get(s,0)),"confidence":min(.995,.50+min(net/500,.30)+.20*min(raw_depth/10000,1)),
            "quote_age_ms":max(now-sb[4],now-fb[4],now-sd[2],now-fd[2]),"latency_bps":LATENCY_BPS,
            "paper_only":True,"observed_at":time.time()}

def candidates(equity):
    out=[]
    with lock:
        syms=list(state["symbols"]);sv=dict(volumes["spot"]);fv=dict(volumes["fut"])
    for s in syms:
        if min(sv.get(s,0),fv.get(s,0))<MIN_VOL:continue
        for direction in ("SPOT_BUY_FUT_SELL","FUT_BUY_SPOT_SELL"):
            op=candidate_for(s,direction,equity,sv,fv)
            if op:out.append(op)
    out.sort(key=lambda x:(x["net_bps"]*x["confidence"],x["notional_usdt"]),reverse=True)
    return out

def paper_capture(opps):
    with lock:
        if state["halted"]:return 0
        e=state["equity"];p=state["peak"]
        if p and (p-e)/p>=MAX_DD:state["halted"]=True;return 0
    n=0
    for op in opps[:MAX_ENTRIES]:
        with lock:
            e=state["equity"]
            notional=min(op["notional_usdt"],max(.01,e*RISK),MAX_NOTIONAL)
            pnl=notional*op["net_bps"]/10000
            state["equity"]+=pnl;state["peak"]=max(state["peak"],state["equity"]);state["paper_pnl"]+=pnl;state["paper_entries"]+=1;state["fills"]+=1
            if pnl>=0:state["wins"]+=1
            else:state["losses"]+=1
        n+=1
        log.info("PAPER EXEC | %s | %s | notional=$%.4f | gross=%.2f bps | net=%.2f bps | pnl=$%.6f",op["symbol"],op["direction"],notional,op["gross_bps"],op["net_bps"],pnl)
    return n

def scan_loop():
    last_universe=0
    while True:
        now=time.time()
        if now-last_universe>300:refresh_universe();last_universe=now
        try:
            with lock:e=state["equity"]
            ops=candidates(e);captures=paper_capture(ops)
            with lock:
                state["scans"]+=1;state["opportunities"]=ops[:50];state["best"]=ops[0] if ops else None;state["last_error"]=""
            if captures or ops:log.info("EXEC SCAN | opportunities=%d | best_net=%.2f bps | fills=%d | dedup_skips=%d",len(ops),ops[0]["net_bps"] if ops else 0,captures,state["dedup_skips"])
        except Exception as e:
            with lock:state["errors"]+=1;state["last_error"]=str(e)
        time.sleep(.10)

def stats():
    with lock:
        e=state["equity"];p=state["peak"]
        return {"status":"ok","engine":"cryptoalpha-arbitrage-websocket-v2-realistic","mode":"PAPER_ONLY","live_execution":False,
        "starting_equity":round(START,2),"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5) if START else 0,
        "paper_entries":state["paper_entries"],"paper_pnl":round(state["paper_pnl"],6),"paper_pnl_pct":round(state["paper_pnl"]/START*100,5) if START else 0,
        "wins":state["wins"],"losses":state["losses"],"drawdown_pct":round(max(0,(p-e)/p)*100,4) if p else 0,"max_drawdown_pct":MAX_DD*100,
        "scans":state["scans"],"errors":state["errors"],"opportunities":len(state["opportunities"]),"best":state["best"],"top_opportunities":state["opportunities"],
        "min_net_bps":MIN_NET_BPS,"risk_pct":RISK*100,"max_entries_per_scan":MAX_ENTRIES,"repeat_mode":False,"execution_model":"depth_vwap_two_leg",
        "depth_levels":DEPTH_LEVELS,"depth_use_pct":DEPTH_USE_PCT*100,"max_notional":MAX_NOTIONAL,"fee_bps_per_leg":FEE_BPS,"funding_bps":FUNDING_BPS,
        "latency_bps":LATENCY_BPS,"dedup_ms":DEDUP_MS,"ws_connected":state["ws_spot"] and state["ws_fut"],"ws_spot":state["ws_spot"],"ws_fut":state["ws_fut"],
        "ws_symbols":len(state["symbols"]),"quote_updates":state["quote_updates"],"depth_updates":state["depth_updates"],"dedup_skips":state["dedup_skips"],
        "stale_ms":STALE_MS,"quote_sync_ms":QUOTE_SYNC_MS,"max_gross_bps":MAX_GROSS_BPS,"last_event":state["last_event"],"last_error":state["last_error"],
        "halted":state["halted"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha Arbitrage v2</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:25px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:21px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Arbitrage v2</div><div>Executable-depth paper simulator</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Starting</div><div class=v>$<span id=st>10.00</span></div></div><div class=c><div class=l>Equity</div><div class=v>$<span id=eq>—</span></div></div><div class=c><div class=l>Compound return</div><div class=v id=rt>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=dd>—</div></div></div><div class=g><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Executed paper fills</div><div class=v id=e>—</div></div><div class=c><div class=l>WebSockets</div><div class=v id=w>—</div></div></div><div class=c><b>REALISTIC EXECUTION MODEL</b><p>Depth-5 VWAP fills · two-leg executable notional · synchronized quotes · stale protection · latency penalty · opportunity deduplication · dynamic depth-limited sizing · compounding · no martingale.</p></div><div class=c><b>Top executable opportunities</b><div id=t>Waiting for market depth…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('st').textContent=n(d.starting_equity);$('eq').textContent=n(d.equity);$('rt').textContent=n(d.compound_return_pct,3)+'%';$('dd').textContent=n(d.drawdown_pct,3)+'%';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('e').textContent=d.paper_entries;$('w').textContent=(d.ws_spot?'S':'—')+'/'+(d.ws_fut?'F':'—');$('z').textContent=(d.ws_connected?'DEPTH WS CONNECTED':'WS RECONNECTING')+' · quotes '+d.quote_updates+' · depth '+d.depth_updates+' · scans '+d.scans+' · errors '+d.errors+' · uptime '+n(d.uptime_seconds,0)+'s · P&L $'+n(d.equity-d.starting_equity,4);let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.notional_usdt,2)+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,0)+'</td><td>'+n(x.quote_age_ms,0)+'ms</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Symbol</th><th>Direction</th><th>Notional</th><th>Gross</th><th>Net</th><th>Depth</th><th>Age</th></tr>'+q+'</table>':'No executable positive-net opportunities yet.'}catch(e){$('z').textContent='stats error '+e}}u();setInterval(u,1000)</script>"""

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/stats.json'):
            b=json.dumps(stats()).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(b);return
        if self.path.startswith('/healthz'):
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps(stats()).encode());return
        b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(b)
    def log_message(self,*a):pass

def main():
    refresh_universe()
    threading.Thread(target=ws_loop,args=("spot","wss://stream.binance.com:9443"),daemon=True).start()
    threading.Thread(target=ws_loop,args=("fut","wss://fstream.binance.com"),daemon=True).start()
    threading.Thread(target=ws_loop,args=("spot_depth","wss://stream.binance.com:9443"),daemon=True).start()
    threading.Thread(target=ws_loop,args=("fut_depth","wss://fstream.binance.com"),daemon=True).start()
    threading.Thread(target=scan_loop,daemon=True).start()
    port=int(os.getenv("PORT","10000"));ThreadingHTTPServer(("0.0.0.0",port),H).serve_forever()

if __name__=="__main__":main()
