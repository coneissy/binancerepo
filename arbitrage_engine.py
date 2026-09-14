"""Cryptoalpha Arbitrage: high-frequency paper-only Binance arbitrage.
No private keys, order endpoints, or live execution.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests

SPOT=os.getenv("BINANCE_SPOT_URL","https://api.binance.com")
FUT=os.getenv("BINANCE_FUTURES_URL","https://fapi.binance.com")
REFRESH=max(1.0,float(os.getenv("ARB_REFRESH_SECONDS","2")))
MIN_NET_BPS=float(os.getenv("ARB_MIN_NET_BPS","7"))
FEE_BPS=float(os.getenv("ARB_FEE_BPS","4"))
SLIP_BPS=float(os.getenv("ARB_SLIPPAGE_BPS","2"))
FUNDING_BPS=float(os.getenv("ARB_FUNDING_BUFFER_BPS","1"))
MIN_VOL=float(os.getenv("ARB_MIN_QUOTE_VOLUME","1000000"))
MIN_DEPTH=float(os.getenv("ARB_MIN_DEPTH_USDT","500"))
MAX_OPPS=max(10,int(os.getenv("ARB_MAX_OPPORTUNITIES","50")))
MAX_ENTRIES_PER_SCAN=max(1,int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN","50")))
START=float(os.getenv("SIM_START_EQUITY","10"))
BASE_RISK=min(.25,max(.01,float(os.getenv("ARB_RISK_PCT","0.10"))))
ENTRY_MULTIPLIER=max(1.0,float(os.getenv("ARB_ENTRY_MULTIPLIER","1")))
EFFECTIVE_RISK=BASE_RISK*ENTRY_MULTIPLIER
MAX_DD=min(.25,max(.02,float(os.getenv("ARB_MAX_DRAWDOWN_PCT","0.08"))))
TIMEOUT=5
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-arb")
http=requests.Session();http.headers.update({"User-Agent":"Cryptoalpha-Arbitrage/3.0"})
lock=threading.RLock()
state={"started":time.time(),"scans":0,"errors":0,"last_scan":0.0,"last_error":"","opportunities":[],"best":None,
       "equity":START,"peak":START,"paper_entries":0,"paper_pnl":0.0,"wins":0,"losses":0,"halted":False,"captures_last_scan":0}
SPOT_FALLBACK=[SPOT,"https://api1.binance.com","https://api2.binance.com"]
FUT_FALLBACK=[FUT,"https://fapi.binance.com","https://fapi1.binance.com","https://fapi2.binance.com","https://fapi3.binance.com"]

def get_any(bases,path,params=None):
    last=None
    for base in dict.fromkeys(bases):
        try:
            r=http.get(base+path,params=params,timeout=TIMEOUT);r.raise_for_status();return r.json(),base
        except Exception as e:last=e
    raise last or RuntimeError("all endpoints failed")

def f(v):
    try:return float(v)
    except:return 0.0

def bps(v):return v*10000.0

def market():
    st,_=get_any(SPOT_FALLBACK,"/api/v3/ticker/bookTicker")
    ft,_=get_any(FUT_FALLBACK,"/fapi/v1/ticker/bookTicker")
    sv,_=get_any(SPOT_FALLBACK,"/api/v3/ticker/24hr")
    fv,_=get_any(FUT_FALLBACK,"/fapi/v1/ticker/24hr")
    sb={x["symbol"]:(f(x.get("bidPrice")),f(x.get("askPrice")),f(x.get("bidQty")),f(x.get("askQty"))) for x in st if x.get("symbol")}
    fb={x["symbol"]:(f(x.get("bidPrice")),f(x.get("askPrice")),f(x.get("bidQty")),f(x.get("askQty"))) for x in ft if x.get("symbol")}
    svv={x["symbol"]:f(x.get("quoteVolume")) for x in sv if x.get("symbol")}
    fvv={x["symbol"]:f(x.get("quoteVolume")) for x in fv if x.get("symbol")}
    return sb,fb,svv,fvv

def candidates(sb,fb,sv,fv):
    out=[];costs=2*(FEE_BPS+SLIP_BPS)+FUNDING_BPS
    for s in sb.keys() & fb.keys():
        if min(sv.get(s,0),fv.get(s,0))<MIN_VOL:continue
        bid_s,ask_s,bq_s,aq_s=sb[s];bid_f,ask_f,bq_f,aq_f=fb[s]
        if min(bid_s,ask_s,bid_f,ask_f)<=0 or bid_s>=ask_s or bid_f>=ask_f:continue
        for direction,buy,sell,buyq,sellq in (("SPOT_BUY_FUT_SELL",ask_s,bid_f,aq_s,bq_f),("FUT_BUY_SPOT_SELL",ask_f,bid_s,aq_f,bq_s)):
            gross=bps(sell/buy-1);depth=min(buy*buyq,sell*sellq);net=gross-costs
            if depth<MIN_DEPTH or net<MIN_NET_BPS:continue
            balance=min(sv.get(s,0),fv.get(s,0))/max(max(sv.get(s,0),fv.get(s,0)),1)
            liq=min(1,math.log10(max(depth,1))/7)
            conf=min(.995,.50+min(max(net,0)/500,.30)+.12*liq+.08*balance)
            out.append({"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,"net_bps":net,"depth_usdt":depth,
                        "liquidity_usdt":min(sv.get(s,0),fv.get(s,0)),"confidence":conf,"paper_only":True,"observed_at":time.time()})
    out.sort(key=lambda x:(x["net_bps"]*x["confidence"],x["depth_usdt"]),reverse=True)
    return out

def paper_capture(opps):
    captures=0
    with lock:
        if state["halted"]:return
        e=state["equity"];p=state["peak"]
        if p and (p-e)/p>=MAX_DD:state["halted"]=True;return
    # Compound from the live simulated equity. Each paper entry uses a fixed
    # percentage of current equity; profits therefore increase the next size,
    # while losses decrease it. No martingale/doubling is used.
    for best in opps[:min(MAX_ENTRIES_PER_SCAN,len(opps))]:
        with lock:
            e=state["equity"]
        notional=max(1.0,e*BASE_RISK*ENTRY_MULTIPLIER)
        pnl=notional*(best["net_bps"]/10000)
        with lock:
            if state["halted"]:break
            state["equity"]+=pnl;state["peak"]=max(state["peak"],state["equity"]);state["paper_pnl"]+=pnl
            state["paper_entries"]+=1
            if pnl>=0:state["wins"]+=1
            else:state["losses"]+=1
        captures+=1
        log.info("PAPER ARB REPEAT | %s | %s | net=%.2f bps | compounded_notional=$%.4f | simulated_pnl=$%.6f",best["symbol"],best["direction"],best["net_bps"],notional,pnl)
    with lock:state["captures_last_scan"]=captures

def scan():
    try:
        sb,fb,sv,fv=market();opps=candidates(sb,fb,sv,fv)
        with lock:
            state["scans"]+=1;state["last_scan"]=time.time();state["last_error"]="";state["opportunities"]=opps[:MAX_OPPS];state["best"]=opps[0] if opps else None
        paper_capture(opps)
        log.info("ARB SCAN | opportunities=%d | best_net=%.2f bps | repeated_entries=%d | halted=%s",len(opps),opps[0]["net_bps"] if opps else 0,state["captures_last_scan"],state["halted"])
    except Exception as e:
        with lock:state["errors"]+=1;state["last_error"]=str(e)
        log.warning("ARB SCAN FAILED | %s",e)

def loop():
    while True:scan();time.sleep(REFRESH)

def stats():
    with lock:
        e=state["equity"];p=state["peak"];o=list(state["opportunities"]);best=state["best"]
        return {"status":"ok","engine":"cryptoalpha-arbitrage","mode":"PAPER_ONLY","live_execution":False,"uptime_seconds":round(time.time()-state["started"],1),"refresh_seconds":REFRESH,
                "scans":state["scans"],"errors":state["errors"],"last_scan":state["last_scan"],"last_error":state["last_error"],"opportunities":len(o),"best":best,"top_opportunities":o,"min_net_bps":MIN_NET_BPS,
                "cost_model":{"fee_bps_per_leg":FEE_BPS,"slippage_bps_per_leg":SLIP_BPS,"funding_buffer_bps":FUNDING_BPS},"base_risk_pct":BASE_RISK*100,"entry_multiplier":ENTRY_MULTIPLIER,"effective_risk_pct":EFFECTIVE_RISK*100,
                "max_entries_per_scan":MAX_ENTRIES_PER_SCAN,"repeat_mode":True,"starting_equity":round(START,2),"equity":round(e,2),"compound_return_pct":round((e/START-1)*100,4) if START else 0,"peak_equity":round(p,2),"drawdown_pct":round(max(0,(p-e)/p)*100,4),"max_drawdown_pct":MAX_DD*100,
                "paper_entries":state["paper_entries"],"paper_pnl_pct":round(state["paper_pnl"]/START*100,4) if START else 0,"wins":state["wins"],"losses":state["losses"],"halted":state["halted"]}

HTML="""<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'><title>Cryptoalpha Arbitrage</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:26px;font-weight:800}.pill{padding:7px 11px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:22px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:800px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:500px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Arbitrage</div><div>High-frequency repeated-entry compound paper engine</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Starting equity</div><div class=v>$<span id=st>10.00</span></div></div><div class=c><div class=l>Current compounded equity</div><div class=v>$<span id=ce>—</span></div></div><div class=c><div class=l>Compound return</div><div class=v id=cr>—</div></div><div class=c><div class=l>Drawdown</div><div class=v id=d>—</div></div></div><div class=g><div class=c><div class=l>Best net edge</div><div class=v id=b>—</div></div><div class=c><div class=l>Opportunities</div><div class=v id=o>—</div></div><div class=c><div class=l>Scans</div><div class=v id=s>—</div></div><div class=c><div class=l>Paper entries</div><div class=v id=e>—</div></div></div><div class=c><b>Compounding engine</b><p>Starting <b>$10.00</b> · each entry uses <b id=risk>—</b> of current equity · profits compound into the next entry · repeated every scan · no martingale · max DD <b id=q>—</b>.</p></div><div class=c><b>Top executable opportunities</b><div id=t>Scanning…</div></div><div class=c><b>System</b><p id=z>—</p></div></div><script>const $=i=>document.getElementById(i),n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();$('st').textContent=n(d.starting_equity);$('ce').textContent=n(d.equity);$('cr').textContent=n(d.compound_return_pct,3)+'%';$('d').textContent=n(d.drawdown_pct,3)+'%';$('b').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('o').textContent=d.opportunities;$('s').textContent=d.scans;$('e').textContent=d.paper_entries;$('risk').textContent=n(d.effective_risk_pct,2)+'%';$('q').textContent=n(d.max_drawdown_pct,1)+'%';$('z').textContent='Uptime '+n(d.uptime_seconds,0)+'s · errors '+d.errors+' · compounded P&L $'+n(d.equity-d.starting_equity,4)+' · last scan entries '+d.max_entries_per_scan;let q=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,0)+'</td><td>'+n(x.confidence*100,1)+'%</td></tr>').join('');$('t').innerHTML=q?'<table><tr><th>#</th><th>Market</th><th>Direction</th><th>Gross</th><th>Net</th><th>Depth</th><th>Confidence</th></tr>'+q+'</table>':'No qualified opportunities.'}catch(e){$('z').textContent='OFFLINE '+e}}u();setInterval(u,2000)</script>"""
class H(BaseHTTPRequestHandler):
 def j(self,o):
  raw=json.dumps(o,separators=(',',':'),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
 def do_GET(self):
  p=self.path.split('?',1)[0]
  if p=='/stats.json':self.j(stats())
  elif p in ('/','/stats'):
   raw=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(raw)
  elif p in ('/health','/healthz'):self.j({'status':'ok','engine':'cryptoalpha-arbitrage','paper_only':True,'live_execution':False})
  else:self.send_response(404);self.end_headers()
 def log_message(self,*a):pass

def main():
 threading.Thread(target=loop,name='arb-scanner',daemon=True).start()
 port=int(os.getenv('PORT','10000'));log.info('Cryptoalpha Arbitrage dashboard :%d | PAPER ONLY | COMPOUNDING FROM $10',port);ThreadingHTTPServer(('0.0.0.0',port),H).serve_forever()
if __name__=='__main__':main()
