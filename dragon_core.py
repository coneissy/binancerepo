"""Dragon canonical market-data core.

This module replaces the legacy v5/v6 engines. It owns Binance WebSocket
market data, shared state, configuration, and canonical cost math.
Execution/diagnostics are layered on top by arbitrage_engine_v7.py.
"""
import json, logging, os, random, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websocket

START = float(os.getenv("SIM_START_EQUITY", "10"))
RISK = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.005"))))
FEE = max(0.0, float(os.getenv("ARB_FEE_BPS", "4")))
SLIP = max(0.0, float(os.getenv("ARB_SLIPPAGE_BPS", "2")))
FUND = max(0.0, float(os.getenv("ARB_FUNDING_BUFFER_BPS", "1")))
MIN_NET = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
MAX_NET = max(MIN_NET, float(os.getenv("ARB_MAX_NET_BPS", "150")))
STALE = max(100, int(os.getenv("ARB_STALE_MS", "750")))
SCAN = max(0.01, float(os.getenv("ARB_SCAN_INTERVAL", "0.05")))
SHARD = max(25, int(os.getenv("ARB_WS_SHARD_SIZE", "40")))
MAX_NOTIONAL = max(0.01, float(os.getenv("ARB_MAX_NOTIONAL_USDT", "1000")))
MAX_ENTRIES = max(1, int(os.getenv("ARB_MAX_ENTRIES_PER_SCAN", "20")))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dragon-core")
lock = threading.RLock()
spot, fut, seen = {}, {}, {}
state = {
    "started": time.time(), "equity": START, "peak": START, "paper_pnl": 0.0,
    "spot_entries": 0, "futures_entries": 0, "spot_opportunities": 0,
    "futures_opportunities": 0, "wins": 0, "losses": 0, "scans": 0,
    "errors": 0, "spot_updates": 0, "futures_updates": 0, "ws_spot": False,
    "ws_futures": False, "spot_sockets": 0, "futures_sockets": 0,
    "spot_sockets_up": 0, "futures_sockets_up": 0, "last_error": "",
    "best_spot": None, "best_futures": None, "opportunities": []
}

ASSETS = "BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA NOT TON TAO STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI".split()
ROUTES = [(a, b, "USDT") for a, b in [("ETH","BTC"),("BNB","BTC"),("SOL","BTC"),("XRP","BTC"),("ADA","BTC"),("AVAX","BTC"),("LINK","BTC"),("DOT","BTC"),("TRX","BTC"),("LTC","BTC"),("ETH","BNB"),("SOL","BNB"),("ADA","BNB")]]
SPOT_SYMBOLS = set(a + "USDT" for a in ASSETS)
for a, b, q in ROUTES:
    SPOT_SYMBOLS.update((a + q, b + q, a + b))
SPOT_SYMBOLS = sorted(SPOT_SYMBOLS)
FUT_SYMBOLS = sorted(a + "USDT" for a in ASSETS)
FUT_TRI_RAW = os.getenv("ARB_FUT_TRI_SYMBOLS", "").strip()
FUT_TRI_ROUTES = []
if FUT_TRI_RAW:
    available = {x.strip().upper() for x in FUT_TRI_RAW.split(",") if x.strip()}
    for a, b, q in ROUTES:
        if {a + q, b + q, a + b}.issubset(available):
            FUT_TRI_ROUTES.append((a, b, q))
            FUT_SYMBOLS.extend([a + b, b + q, a + q])
FUT_SYMBOLS = sorted(set(FUT_SYMBOLS))

def cost_bps(legs, funding=False):
    """Single canonical round-trip cost model."""
    return legs * (FEE + SLIP) + (FUND if funding else 0.0)

def net_bps(gross_bps, legs, funding=False):
    return float(gross_bps) - cost_bps(legs, funding)

def target_notional_usdt(equity):
    return min(max(0.01, float(equity) * RISK), MAX_NOTIONAL)

def shards(xs):
    return [xs[i:i + SHARD] for i in range(0, len(xs), SHARD)]

def stream_url(base, symbols):
    return base.rstrip("/") + "/stream?streams=" + "/".join(s.lower() + "@bookTicker" for s in symbols)

def worker(kind, base, store, idx, symbols, total):
    backoff = 1.0
    while True:
        opened_at = 0.0
        try:
            def on_open(ws):
                nonlocal opened_at
                opened_at = time.monotonic()
                with lock:
                    state[kind + "_sockets_up"] += 1
                    state["ws_" + kind] = True
                log.info("WS CONNECTED | %s shard %d/%d | %d symbols", kind.upper(), idx + 1, total, len(symbols))
            def on_message(ws, raw):
                try:
                    d = json.loads(raw).get("data", {})
                    s = d.get("s"); bid = float(d.get("b", 0)); ask = float(d.get("a", 0))
                    bq = float(d.get("B", 0)); aq = float(d.get("A", 0))
                    if s and bid > 0 and ask >= bid and bq > 0 and aq > 0:
                        with lock:
                            store[s] = (bid, ask, bq, aq, time.monotonic() * 1000)
                            state[kind + "_updates"] += 1
                except Exception as e:
                    with lock:
                        state["errors"] += 1; state["last_error"] = "message: " + str(e)
            def on_error(ws, e):
                with lock: state["last_error"] = f"{kind} websocket: {e}"
            def on_close(ws, code, msg):
                with lock:
                    state[kind + "_sockets_up"] = max(0, state[kind + "_sockets_up"] - 1)
                    state["ws_" + kind] = state[kind + "_sockets_up"] > 0
                log.warning("WS CLOSED | %s shard %d/%d | code=%s msg=%s", kind.upper(), idx + 1, total, code, msg)
            app = websocket.WebSocketApp(stream_url(base, symbols), on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close, header=["User-Agent: dragon-core"])
            app.run_forever(ping_interval=0, ping_timeout=None, skip_utf8_validation=True)
        except Exception as e:
            with lock:
                state["errors"] += 1; state["last_error"] = f"{kind} transport: {e}"
        stable = bool(opened_at and time.monotonic() - opened_at >= 30)
        backoff = 1.0 if stable else min(30.0, backoff * 2.0)
        time.sleep(backoff + random.random() * 0.5)

def start(kind, base, store, groups):
    state[kind + "_sockets"] = len(groups)
    for i, g in enumerate(groups):
        threading.Thread(target=worker, args=(kind, base, store, i, g, len(groups)), daemon=True).start()

def quote(store, symbol):
    x = store.get(symbol)
    if not x:
        return None, "missing_quote"
    age = time.monotonic() * 1000 - x[4]
    if age > STALE:
        return None, "stale_quote"
    return x, ""

def stats():
    with lock:
        e = state["equity"]
        return {
            "status":"ok", "engine":"dragon-core", "mode":"DIAGNOSTIC", "live_execution":False,
            "starting_equity":START, "equity":round(e, 6), "compound_return_pct":round((e/START-1)*100, 6),
            "paper_pnl":round(state["paper_pnl"], 6), "spot_entries":state["spot_entries"], "futures_entries":state["futures_entries"],
            "paper_entries":state["spot_entries"] + state["futures_entries"], "spot_opportunities":state["spot_opportunities"],
            "futures_opportunities":state["futures_opportunities"], "best_spot":state["best_spot"], "best_futures":state["best_futures"],
            "top_opportunities":state["opportunities"], "wins":state["wins"], "losses":state["losses"], "scans":state["scans"],
            "errors":state["errors"], "ws_spot":state["ws_spot"], "ws_futures":state["ws_futures"],
            "spot_sockets":state["spot_sockets"], "spot_sockets_up":state["spot_sockets_up"], "futures_sockets":state["futures_sockets"],
            "futures_sockets_up":state["futures_sockets_up"], "quote_updates_spot":state["spot_updates"], "quote_updates_futures":state["futures_updates"],
            "spot_symbols":len(SPOT_SYMBOLS), "futures_symbols":len(FUT_SYMBOLS), "triangular_routes":len(ROUTES),
            "futures_triangular_routes":len(FUT_TRI_ROUTES), "min_net_bps":MIN_NET, "risk_pct":RISK*100,
            "cost_model":{"basis_2_leg_bps":cost_bps(2, True), "spot_triangle_3_leg_bps":cost_bps(3, False), "futures_triangle_3_leg_bps":cost_bps(3, True)},
            "last_error":state["last_error"], "uptime_seconds":round(time.time()-state["started"],1),
            "unsupported_engines":["FUTURES_FUTURES","CEX_CEX","CEX_DEX","DEX_DEX"]
        }

def scan():
    while True:
        time.sleep(SCAN)

def main():
    start("spot", os.getenv("BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443"), spot, shards(SPOT_SYMBOLS))
    start("futures", "wss://fstream.binance.com", fut, shards(FUT_SYMBOLS))
    threading.Thread(target=scan, daemon=True).start()
    port = int(os.getenv("PORT", "10000"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/stats.json"):
            body=json.dumps(stats()).encode(); self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body); return
        body=b"<html><body><h1>Dragon</h1><pre id='x'>loading</pre><script>async function u(){x.textContent=JSON.stringify(await (await fetch('/stats.json?'+Date.now())).json(),null,2)}u();setInterval(u,1000)</script></body></html>"
        self.send_response(200); self.send_header("Content-Type","text/html"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass

if __name__ == "__main__": main()
