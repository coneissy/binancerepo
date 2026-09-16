"""Dragon market-data core.

The universe is discovered from Binance exchangeInfo at startup rather than a
hard-coded coin list. Spot and USDT-margined futures are subscribed through
bookTicker streams. WebSocket workers fail closed, send explicit keepalives,
detect silent streams, and reconnect with exponential backoff + jitter.
"""
import json
import logging
import os
import random
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import websocket
import pricing

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
UNIVERSE_REFRESH = max(60, int(os.getenv("ARB_UNIVERSE_REFRESH_SEC", "900")))
WS_PING_INTERVAL = max(5, int(os.getenv("ARB_WS_PING_INTERVAL_SEC", "20")))
WS_PING_TIMEOUT = max(3, int(os.getenv("ARB_WS_PING_TIMEOUT_SEC", "10")))
WS_SILENCE_TIMEOUT = max(15, int(os.getenv("ARB_WS_SILENCE_TIMEOUT_SEC", "45")))
WS_BACKOFF_MAX = max(5, int(os.getenv("ARB_WS_BACKOFF_MAX_SEC", "30")))

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
    "best_spot": None, "best_futures": None, "opportunities": [],
    "universe_refreshes": 0, "universe_last_error": ""
}

SPOT_SYMBOLS = []
FUT_SYMBOLS = []
ROUTES = []
FUT_TRI_ROUTES = []


def _get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "dragon-core/1"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _load_universe():
    """Load every currently-trading USDT spot/futures market from Binance."""
    spot_base = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")
    fut_base = os.getenv("BINANCE_FAPI_BASE", "https://fapi.binance.com").rstrip("/")
    spot_info = _get_json(spot_base + "/api/v3/exchangeInfo")
    fut_info = _get_json(fut_base + "/fapi/v1/exchangeInfo")

    spot_meta = {}
    for s in spot_info.get("symbols", []):
        symbol = str(s.get("symbol", "")).upper()
        if (s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
                and s.get("isSpotTradingAllowed", True)):
            spot_meta[symbol] = s

    # Keep every USDT-quoted spot market, including symbols such as 1000PEPEUSDT.
    spot_symbols = sorted(spot_meta)

    # Build every valid USDT -> asset A -> asset B -> USDT triangle that exists
    # in the live spot exchange metadata. This replaces the old hard-coded list.
    usdt_assets = {s["baseAsset"] for s in spot_meta.values() if s.get("baseAsset")}
    pair_by_assets = {}
    for symbol, meta in spot_meta.items():
        a, b = meta.get("baseAsset"), meta.get("quoteAsset")
        if a and b:
            pair_by_assets[(a, b)] = symbol
    routes = []
    for a in sorted(usdt_assets):
        if (a, "USDT") not in pair_by_assets:
            continue
        for b in sorted(usdt_assets):
            if a == b or (b, "USDT") not in pair_by_assets:
                continue
            if (a, b) in pair_by_assets:
                routes.append((a, b, "USDT"))
            elif (b, a) in pair_by_assets:
                # The pricing engine can evaluate both directions using the
                # same executable pair, so retain the canonical asset order.
                routes.append((b, a, "USDT"))
    routes = sorted(set(routes))

    fut_symbols = sorted({
        str(s.get("symbol", "")).upper()
        for s in fut_info.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
        and s.get("contractType") == "PERPETUAL"
    })

    # Futures triangular routes are opt-in because Binance USDT-margined
    # futures normally expose single-asset USDT contracts, not spot-style
    # cross pairs. Never subscribe to a non-existent cross contract.
    tri_raw = os.getenv("ARB_FUT_TRI_SYMBOLS", "").strip()
    tri_routes = []
    if tri_raw:
        available = set(fut_symbols) | {x.strip().upper() for x in tri_raw.split(",") if x.strip()}
        for a, b, q in routes:
            if {a + q, b + q, a + b}.issubset(available):
                tri_routes.append((a, b, q))
                fut_symbols.extend([a + b, b + q, a + q])
    fut_symbols = sorted(set(fut_symbols))

    return spot_symbols, fut_symbols, routes, sorted(set(tri_routes))


def refresh_universe(initial=False):
    global SPOT_SYMBOLS, FUT_SYMBOLS, ROUTES, FUT_TRI_ROUTES
    try:
        new_spot, new_fut, new_routes, new_fut_tri = _load_universe()
        with lock:
            changed = (new_spot != SPOT_SYMBOLS or new_fut != FUT_SYMBOLS or
                       new_routes != ROUTES or new_fut_tri != FUT_TRI_ROUTES)
            SPOT_SYMBOLS, FUT_SYMBOLS, ROUTES, FUT_TRI_ROUTES = new_spot, new_fut, new_routes, new_fut_tri
            state["universe_refreshes"] += 1
            state["universe_last_error"] = ""
        log.info("BINANCE UNIVERSE | spot=%d futures=%d spot_triangles=%d futures_triangles=%d changed=%s",
                 len(new_spot), len(new_fut), len(new_routes), len(new_fut_tri), changed)
        return changed
    except Exception as e:
        with lock:
            state["universe_last_error"] = str(e)
            state["errors"] += 1
            state["last_error"] = "universe: " + str(e)
        log.exception("BINANCE UNIVERSE REFRESH FAILED")
        if initial:
            raise
        return False


def universe_refresher():
    while True:
        time.sleep(UNIVERSE_REFRESH)
        if refresh_universe(False):
            log.warning("BINANCE UNIVERSE CHANGED; restart service to resize websocket shard workers safely")


def shards(xs):
    return [xs[i:i + SHARD] for i in range(0, len(xs), SHARD)]


def stream_url(base, symbols):
    encoded = "/".join(s.lower() + "@bookTicker" for s in symbols)
    return base.rstrip("/") + "/stream?streams=" + encoded


def worker(kind, base, store, idx, symbols, total):
    backoff = 1.0
    while True:
        opened_at = 0.0
        last_message = time.monotonic()
        try:
            def on_open(ws):
                nonlocal opened_at, last_message
                opened_at = time.monotonic()
                last_message = opened_at
                with lock:
                    state[kind + "_sockets_up"] += 1
                    state["ws_" + kind] = True
                log.info("WS CONNECTED | %s shard %d/%d | %d symbols", kind.upper(), idx + 1, total, len(symbols))

            def on_message(ws, raw):
                nonlocal last_message
                last_message = time.monotonic()
                try:
                    d = json.loads(raw).get("data", {})
                    s = str(d.get("s", "")).upper()
                    bid, ask = float(d.get("b", 0)), float(d.get("a", 0))
                    bq, aq = float(d.get("B", 0)), float(d.get("A", 0))
                    if s and bid > 0 and ask >= bid and bq > 0 and aq > 0:
                        with lock:
                            store[s] = (bid, ask, bq, aq, time.monotonic() * 1000)
                            state[kind + "_updates"] += 1
                except Exception as e:
                    with lock:
                        state["errors"] += 1
                        state["last_error"] = "message: " + str(e)

            def on_error(ws, e):
                with lock:
                    state["last_error"] = f"{kind} websocket: {e}"
                log.warning("WS ERROR | %s shard %d/%d | %s", kind.upper(), idx + 1, total, e)

            def on_close(ws, code, msg):
                with lock:
                    state[kind + "_sockets_up"] = max(0, state[kind + "_sockets_up"] - 1)
                    state["ws_" + kind] = state[kind + "_sockets_up"] > 0
                log.warning("WS CLOSED | %s shard %d/%d | code=%s msg=%s", kind.upper(), idx + 1, total, code, msg)

            def on_ping(ws, message):
                nonlocal last_message
                last_message = time.monotonic()

            app = websocket.WebSocketApp(
                stream_url(base, symbols), on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close, on_ping=on_ping,
                header=["User-Agent: dragon-core"],
            )
            # Explicit client pings avoid depending on server traffic for
            # liveness. A silent stream is closed by the watchdog below.
            watchdog = threading.Thread(
                target=lambda: _socket_watchdog(app, lambda: last_message, kind, idx, total),
                daemon=True,
            )
            watchdog.start()
            app.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                skip_utf8_validation=True,
                enable_multithread=True,
            )
        except Exception as e:
            with lock:
                state["errors"] += 1
                state["last_error"] = f"{kind} transport: {e}"
            log.exception("WS TRANSPORT FAILED | %s shard %d/%d", kind.upper(), idx + 1, total)
        stable = bool(opened_at and time.monotonic() - opened_at >= 30)
        backoff = 1.0 if stable else min(WS_BACKOFF_MAX, backoff * 2.0)
        delay = backoff + random.random() * min(1.0, backoff * 0.25)
        time.sleep(delay)


def _socket_watchdog(app, last_message_fn, kind, idx, total):
    while getattr(app, "keep_running", False):
        time.sleep(5)
        if time.monotonic() - last_message_fn() > WS_SILENCE_TIMEOUT:
            log.warning("WS SILENCE TIMEOUT | %s shard %d/%d | reconnecting", kind.upper(), idx + 1, total)
            try:
                app.close()
            except Exception:
                pass
            return


def start(kind, base, store, groups):
    state[kind + "_sockets"] = len(groups)
    for i, group in enumerate(groups):
        threading.Thread(target=worker, args=(kind, base, store, i, group, len(groups)), daemon=True,
                         name=f"dragon-{kind}-ws-{i+1}").start()


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
            "status": "ok", "engine": "dragon-core", "mode": "DIAGNOSTIC", "live_execution": False,
            "starting_equity": START, "equity": round(e, 6), "compound_return_pct": round((e / START - 1) * 100, 6),
            "paper_pnl": round(state["paper_pnl"], 6), "spot_entries": state["spot_entries"], "futures_entries": state["futures_entries"],
            "paper_entries": state["spot_entries"] + state["futures_entries"], "spot_opportunities": state["spot_opportunities"],
            "futures_opportunities": state["futures_opportunities"], "best_spot": state["best_spot"], "best_futures": state["best_futures"],
            "top_opportunities": state["opportunities"], "wins": state["wins"], "losses": state["losses"], "scans": state["scans"],
            "errors": state["errors"], "ws_spot": state["ws_spot"], "ws_futures": state["ws_futures"],
            "spot_sockets": state["spot_sockets"], "spot_sockets_up": state["spot_sockets_up"], "futures_sockets": state["futures_sockets"],
            "futures_sockets_up": state["futures_sockets_up"], "quote_updates_spot": state["spot_updates"], "quote_updates_futures": state["futures_updates"],
            "spot_symbols": len(SPOT_SYMBOLS), "futures_symbols": len(FUT_SYMBOLS), "triangular_routes": len(ROUTES),
            "futures_triangular_routes": len(FUT_TRI_ROUTES), "universe_refreshes": state["universe_refreshes"],
            "universe_refresh_seconds": UNIVERSE_REFRESH, "universe_last_error": state["universe_last_error"],
            "ws_ping_interval_sec": WS_PING_INTERVAL, "ws_ping_timeout_sec": WS_PING_TIMEOUT,
            "ws_silence_timeout_sec": WS_SILENCE_TIMEOUT, "min_net_bps": MIN_NET, "risk_pct": RISK * 100,
            "cost_model": {"basis_2_leg_bps": pricing.cost_bps(2, FEE, SLIP, FUND), "spot_triangle_3_leg_bps": pricing.cost_bps(3, FEE, SLIP, 0), "futures_triangle_3_leg_bps": pricing.cost_bps(3, FEE, SLIP, FUND)},
            "last_error": state["last_error"], "uptime_seconds": round(time.time() - state["started"], 1),
            "unsupported_engines": ["FUTURES_FUTURES", "CEX_CEX", "CEX_DEX", "DEX_DEX"]
        }


def scan():
    while True:
        time.sleep(SCAN)


def main():
    refresh_universe(initial=True)
    spot_groups, fut_groups = shards(SPOT_SYMBOLS), shards(FUT_SYMBOLS)
    start("spot", os.getenv("BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443"), spot, spot_groups)
    start("futures", os.getenv("BINANCE_FUT_WS_BASE_URL", "wss://fstream.binance.com"), fut, fut_groups)
    threading.Thread(target=universe_refresher, daemon=True, name="dragon-universe-refresh").start()
    threading.Thread(target=scan, daemon=True, name="dragon-scan").start()
    port = int(os.getenv("PORT", "10000"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/stats.json"):
            body = json.dumps(stats()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = b"<html><body><h1>Dragon</h1><pre id='x'>loading</pre><script>async function u(){x.textContent=JSON.stringify(await (await fetch('/stats.json?'+Date.now())).json(),null,2)}u();setInterval(u,1000)</script></body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    main()
