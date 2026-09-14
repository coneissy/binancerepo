"""Cryptoalpha Arbitrage Engine - public-data paper scanner.

Scans Binance spot/futures executable quotes for cross-market basis and
triangular spot-arbitrage opportunities. PAPER ONLY: no order endpoints,
API keys, or live execution are used. Every opportunity is cost-adjusted
for configurable fees/slippage and filtered by liquidity/depth proxies.
"""
import json, logging, math, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from statistics import mean
import requests

SPOT = os.getenv("BINANCE_SPOT_URL", "https://api.binance.com")
FUT = os.getenv("BINANCE_FUTURES_URL", "https://fapi.binance.com")
REFRESH = float(os.getenv("ARB_REFRESH_SECONDS", "3"))
MIN_NET_BPS = float(os.getenv("ARB_MIN_NET_BPS", "10"))
FEE_BPS = float(os.getenv("ARB_FEE_BPS", "4"))
SLIP_BPS = float(os.getenv("ARB_SLIPPAGE_BPS", "2"))
FUNDING_BPS = float(os.getenv("ARB_FUNDING_BUFFER_BPS", "1"))
MIN_QUOTE_VOL = float(os.getenv("ARB_MIN_QUOTE_VOLUME", "1000000"))
MAX_OPPS = int(os.getenv("ARB_MAX_OPPORTUNITIES", "25"))
START = float(os.getenv("SIM_START_EQUITY", "10000"))
RISK = float(os.getenv("ARB_RISK_PCT", "0.0025"))
MAX_DD = float(os.getenv("ARB_MAX_DRAWDOWN_PCT", "0.08"))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cryptoalpha-arb")
http = requests.Session()
lock = threading.RLock()
state = {"started": time.time(), "scans": 0, "errors": 0, "opportunities": [], "best": None,
         "paper_entries": 0, "paper_wins": 0, "paper_losses": 0, "paper_pnl": 0.0,
         "equity": START, "peak": START, "last_scan": 0.0, "last_error": ""}


def get(base, path, params=None):
    r = http.get(base + path, params=params, timeout=8)
    r.raise_for_status()
    return r.json()


def bps(x):
    return x * 10000.0


def safe_float(x):
    try: return float(x)
    except Exception: return 0.0


def load_market_data():
    spot_info = get(SPOT, "/api/v3/exchangeInfo")
    fut_info = get(FUT, "/fapi/v1/exchangeInfo")
    spot_t = {x["symbol"]: x for x in get(SPOT, "/api/v3/ticker/24hr")}
    fut_t = {x["symbol"]: x for x in get(FUT, "/fapi/v1/ticker/24hr")}
    spot_q = {x["symbol"]: safe_float(x.get("quoteVolume")) for x in spot_t.values()}
    fut_q = {x["symbol"]: safe_float(x.get("quoteVolume")) for x in fut_t.values()}
    return spot_info, fut_info, spot_t, fut_t, spot_q, fut_q


def book_map(base):
    rows = get(base, "/api/v3/ticker/bookTicker" if base == SPOT else "/fapi/v1/ticker/bookTicker")
    return {x["symbol"]: (safe_float(x.get("bidPrice")), safe_float(x.get("askPrice")),
                           safe_float(x.get("bidQty")), safe_float(x.get("askQty"))) for x in rows}


def cross_market(spot_info, fut_info, spot_t, fut_t, spot_q, fut_q, sb, fb):
    ss = {x["symbol"]: x for x in spot_info.get("symbols", [])
          if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT" and x.get("isSpotTradingAllowed", True)}
    fs = {x["symbol"]: x for x in fut_info.get("symbols", [])
          if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL" and x.get("quoteAsset") == "USDT"}
    common = set(ss) & set(fs) & set(sb) & set(fb)
    out = []
    total_cost_bps = 2 * (FEE_BPS + SLIP_BPS) + FUNDING_BPS
    for s in common:
        if max(spot_q.get(s, 0), fut_q.get(s, 0)) < MIN_QUOTE_VOL: continue
        bid_s, ask_s, bq_s, aq_s = sb[s]; bid_f, ask_f, bq_f, aq_f = fb[s]
        if min(bid_s, ask_s, bid_f, ask_f) <= 0: continue
        # Buy cheaper venue and sell dearer venue; both directions are checked.
        for direction, buy, sell, buyq, sellq in (("SPOT_BUY_FUT_SELL", ask_s, bid_f, aq_s, bq_f),
                                                   ("FUT_BUY_SPOT_SELL", ask_f, bid_s, aq_f, bq_s)):
            gross = bps(sell / buy - 1.0)
            net = gross - total_cost_bps
            if net >= MIN_NET_BPS:
                depth = min(buyq * buy, sellq * sell)
                out.append({"type":"CROSS_MARKET","symbol":s,"direction":direction,"gross_bps":gross,
                            "net_bps":net,"depth_usdt":depth,"spot_bid":bid_s,"spot_ask":ask_s,
                            "futures_bid":bid_f,"futures_ask":ask_f,"liquidity_usdt":min(spot_q.get(s,0),fut_q.get(s,0)),
                            "confidence":min(0.99, 0.55 + min(net/1000,0.25) + min(math.log10(max(depth,1))/10,0.15)),
                            "paper_only":True})
    return out


def triangular(sb, spot_q):
    # USDT -> asset A -> asset B -> USDT, using only executable book sides.
    bridges = ["BTC", "ETH", "BNB", "SOL", "XRP"]
    out = []
    cost_bps = 3 * (FEE_BPS + SLIP_BPS)
    for a in bridges:
        sa = a + "USDT"
        if sa not in sb or spot_q.get(sa, 0) < MIN_QUOTE_VOL: continue
        for b in bridges:
            if b == a: continue
            sbus = b + "USDT"
            cross1 = a + b if a + b in sb else b + a if b + a in sb else None
            if not cross1 or sbus not in sb or spot_q.get(sbus, 0) < MIN_QUOTE_VOL: continue
            ba, aa, _, _ = sb[sa]; bb, ab, _, _ = sb[sbus]; bx, ax, _, _ = sb[cross1]
            if min(ba, aa, bb, ab, bx, ax) <= 0: continue
            # Cycle 1: USDT->A, A->B, B->USDT.
            # If pair is A/B, selling A for B uses bid; if B/A, buying B with A uses ask.
            start = 1.0
            a_amt = start / aa
            if cross1 == a + b: b_amt = a_amt * bx
            else: b_amt = a_amt / ax
            end = b_amt * bb
            net = bps(end - start) - cost_bps
            if net >= MIN_NET_BPS:
                out.append({"type":"TRIANGULAR","symbol":f"{a}/{b}","direction":"USDT->%s->%s->USDT"%(a,b),
                            "gross_bps":bps(end-start),"net_bps":net,"depth_usdt":min(spot_q.get(sa,0),spot_q.get(sbus,0),spot_q.get(cross1,0)),
                            "confidence":min(0.98,0.50+min(net/800,0.25)),"paper_only":True})
    return out


def scan():
    try:
        spot_info, fut_info, spot_t, fut_t, spot_q, fut_q = load_market_data()
        sb = book_map(SPOT); fb = book_map(FUT)
        opps = cross_market(spot_info, fut_info, spot_t, fut_t, spot_q, fut_q, sb, fb)
        opps += triangular(sb, spot_q)
        opps.sort(key=lambda x: (x.get("net_bps",0), x.get("depth_usdt",0)), reverse=True)
        with lock:
            state["scans"] += 1; state["last_scan"] = time.time(); state["last_error"] = ""
            state["opportunities"] = opps[:MAX_OPPS]; state["best"] = state["opportunities"][0] if state["opportunities"] else None
        log.info("ARBITRAGE SCAN | opportunities=%d | best_net_bps=%s", len(opps),
                 f"{opps[0]['net_bps']:.2f}" if opps else "0.00")
    except Exception as exc:
        with lock: state["errors"] += 1; state["last_error"] = str(exc)
        log.warning("ARBITRAGE SCAN FAILED | %s", exc)


def loop():
    while True:
        scan(); time.sleep(max(1.0, REFRESH))


def stats():
    with lock:
        equity=state["equity"]; peak=state["peak"]; opps=list(state["opportunities"]); best=state["best"]
        dd=max(0,(peak-equity)/peak) if peak else 0
        return {"status":"ok","engine":"cryptoalpha-arbitrage","mode":"PAPER_ONLY","live_execution":False,
                "uptime_seconds":round(time.time()-state["started"],1),"refresh_seconds":REFRESH,
                "scans":state["scans"],"errors":state["errors"],"last_scan":state["last_scan"],"last_error":state["last_error"],
                "opportunities":len(opps),"best":best,"top_opportunities":opps,"min_net_bps":MIN_NET_BPS,
                "cost_model":{"fee_bps_per_leg":FEE_BPS,"slippage_bps_per_leg":SLIP_BPS,"funding_buffer_bps":FUNDING_BPS},
                "risk_pct":RISK*100,"equity":round(equity,2),"drawdown_pct":round(dd*100,4),"max_drawdown_pct":MAX_DD*100,
                "paper_entries":state["paper_entries"],"paper_pnl_pct":round(state["paper_pnl"]/START*100,4)}


HTML="""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Cryptoalpha Arbitrage</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.wrap{max-width:1250px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:26px;font-weight:800}.pill{border:1px solid #31533e;border-radius:20px;padding:8px 12px}.grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:16px 0}.card{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px;margin-bottom:10px}.label{color:#8e9aae;font-size:11px;text-transform:uppercase}.value{font-size:23px;font-weight:800;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:9px;border-bottom:1px solid #202938;text-align:left}th{color:#8e9aae}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}}@media(max-width:500px){.grid{grid-template-columns:1fr}.wrap{padding:10px}}</style></head><body><div class='wrap'><div class='top'><div><div class='brand'>Cryptoalpha Arbitrage</div><div>Cross-market basis + triangular spot scanner · cost-adjusted</div></div><div class='pill'>PAPER ONLY · LIVE OFF</div></div><div class='grid'><div class='card'><div class='label'>Best net edge</div><div class='value' id='best'>—</div></div><div class='card'><div class='label'>Opportunities</div><div class='value' id='opp'>—</div></div><div class='card'><div class='label'>Scans</div><div class='value' id='scans'>—</div></div><div class='card'><div class='label'>Equity</div><div class='value' id='eq'>—</div></div><div class='card'><div class='label'>Drawdown</div><div class='value' id='dd'>—</div></div></div><div class='card'><b>Execution model</b><p>Net edge = quoted spread − fees − slippage − funding buffer. Minimum net edge <b id='min'>—</b>. Fixed risk <b id='risk'>—</b>. No doubling/martingale.</p></div><div class='card'><b>Live opportunities</b><div id='rows'>Scanning…</div></div><div class='card'><b>System</b><p id='sys'>—</p></div></div><script>const $=x=>document.getElementById(x);const n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{const d=await(await fetch('/stats.json?t='+Date.now(),{cache:'no-store'})).json();$('best').textContent=d.best?n(d.best.net_bps)+' bps':'—';$('opp').textContent=d.opportunities;$('scans').textContent=d.scans;$('eq').textContent='$'+n(d.equity);$('dd').textContent=n(d.drawdown_pct,3)+'%';$('min').textContent=n(d.min_net_bps)+' bps';$('risk').textContent=n(d.risk_pct,3)+'%';$('sys').textContent='Uptime '+n(d.uptime_seconds,0)+'s · Errors '+d.errors+' · Last scan '+(d.last_scan?new Date(d.last_scan*1000).toLocaleTimeString():'—')+(d.last_error?' · '+d.last_error:'');let r=(d.top_opportunities||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.type+'</td><td>'+x.symbol+'</td><td>'+x.direction+'</td><td>'+n(x.gross_bps)+'</td><td>'+n(x.net_bps)+'</td><td>$'+n(x.depth_usdt,0)+'</td><td>'+n(x.confidence*100,1)+'%</td></tr>').join('');$('rows').innerHTML=r?'<table><tr><th>#</th><th>Type</th><th>Market</th><th>Direction</th><th>Gross bps</th><th>Net bps</th><th>Depth proxy</th><th>Confidence</th></tr>'+r+'</table>':'No cost-adjusted opportunities above threshold.'}catch(e){$('sys').textContent='OFFLINE: '+e}}u();setInterval(u,3000)</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def send_json(self, obj):
        raw=json.dumps(obj,separators=(",",":"),default=str).encode(); self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def do_GET(self):
        p=self.path.split("?",1)[0]
        if p in ("/","/stats"):
            raw=HTML.encode(); self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(raw))); self.end_headers(); self.wfile.write(raw)
        elif p=="/stats.json": self.send_json(stats())
        elif p in ("/health","/healthz"): self.send_json({"status":"ok","engine":"cryptoalpha-arbitrage","paper_only":True,"live_execution":False})
        else: self.send_response(404); self.end_headers()
    def log_message(self,*a): pass


def main():
    threading.Thread(target=loop,name="arbitrage-scan",daemon=True).start()
    port=int(os.getenv("PORT","10000")); server=ThreadingHTTPServer(("0.0.0.0",port),Handler); log.info("Cryptoalpha Arbitrage dashboard on :%d",port); server.serve_forever()

if __name__=="__main__": main()
