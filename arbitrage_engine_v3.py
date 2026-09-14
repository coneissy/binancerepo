'''Cryptoalpha v3: Spot triangular + Spot/Futures basis-cross paper engine. LIVE OFF.'''
import json
import logging
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import websocket

START = float(os.getenv("SIM_START_EQUITY", "10"))
RISK = max(0.01, min(0.25, float(os.getenv("ARB_RISK_PCT", ".25"))))
FEE = max(0.0, float(os.getenv("ARB_FEE_BPS", "4")))
SLIP = max(0.0, float(os.getenv("ARB_SLIPPAGE_BPS", "1.5")))
MIN_NET = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "0.5")))
STALE = max(100, int(os.getenv("ARB_STALE_MS", "750")))
SCAN = max(0.01, float(os.getenv("ARB_SCAN_INTERVAL", ".02")))
MAX_NOTIONAL = max(0.01, float(os.getenv("ARB_MAX_NOTIONAL_USDT", "1000")))
WS_SHARD_SIZE = max(10, int(os.getenv("ARB_WS_SHARD_SIZE", "20")))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cryptoalpha-v3")
lock = threading.RLock()
spot = {}
fut = {}
seen = {}

state = {
    "started": time.time(), "equity": START, "peak": START, "paper_pnl": 0.0,
    "spot_entries": 0, "futures_entries": 0, "spot_opportunities": 0, "futures_opportunities": 0,
    "wins": 0, "losses": 0, "scans": 0, "errors": 0, "spot_updates": 0, "futures_updates": 0,
    "ws_spot": False, "ws_futures": False, "spot_sockets": 0, "futures_sockets": 0,
    "spot_sockets_up": 0, "futures_sockets_up": 0, "last_error": "",
    "best_spot": None, "best_futures": None,
}

ASSETS = ("BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT TRX LTC BCH UNI NEAR APT SUI FIL ARB OP INJ "
          "SEI TIA PEPE WIF BONK FLOKI SHIB ETC ATOM ICP XLM AAVE ALGO RUNE MKR CRV JUP WLD ENA "
          "NOT TON TAO STX GRT IMX LDO SAND MANA AXS THETA EOS HBAR VET IOTA PYTH JTO STRK ZK ORDI").split()
SYMS = [x + "USDT" for x in ASSETS]
CROSS = [("ETH","BTC","USDT"),("BNB","BTC","USDT"),("SOL","BTC","USDT"),("XRP","BTC","USDT"),
         ("ADA","BTC","USDT"),("AVAX","BTC","USDT"),("LINK","BTC","USDT"),("DOT","BTC","USDT"),
         ("TRX","BTC","USDT"),("LTC","BTC","USDT"),("ETH","BNB","USDT"),("SOL","BNB","USDT"),
         ("ADA","BNB","USDT")]
SHARDS = [SYMS[i:i + WS_SHARD_SIZE] for i in range(0, len(SYMS), WS_SHARD_SIZE)]


def ws_url(base, symbols):
    # Only bookTicker is required by the basis/triangular calculations.
    streams = [s.lower() + "@bookTicker" for s in symbols]
    return base.rstrip("/") + "?streams=" + "/".join(streams)


def connect_shard(kind, base, store, shard_idx, symbols):
    backoff = 1.0
    while True:
        opened_at = 0.0
        try:
            def opened(ws):
                nonlocal opened_at
                opened_at = time.monotonic()
                with lock:
                    state[f"{kind}_sockets_up"] += 1
                    state[f"ws_{kind}"] = state[f"{kind}_sockets_up"] > 0
                log.info("WS CONNECTED | %s | shard=%d/%d | symbols=%d", kind.upper(), shard_idx + 1, len(SHARDS), len(symbols))

            def message(ws, raw):
                try:
                    payload = json.loads(raw)
                    d = payload.get("data", payload)
                    symbol = d.get("s")
                    if not symbol:
                        return
                    bid = float(d.get("b", 0))
                    ask = float(d.get("a", 0))
                    bid_qty = float(d.get("B", 0))
                    ask_qty = float(d.get("A", 0))
                    if min(bid, ask, bid_qty, ask_qty) > 0 and ask >= bid:
                        with lock:
                            store[symbol] = (bid, ask, bid_qty, ask_qty, time.monotonic() * 1000.0)
                            state[f"{kind}_updates"] += 1
                except Exception as exc:
                    with lock:
                        state["errors"] += 1
                        state["last_error"] = str(exc)

            def error(ws, exc):
                with lock:
                    state["errors"] += 1
                    state["last_error"] = str(exc)
                log.warning("WS ERROR | %s | shard=%d | %s", kind.upper(), shard_idx + 1, exc)

            def closed(ws, *args):
                with lock:
                    state[f"{kind}_sockets_up"] = max(0, state[f"{kind}_sockets_up"] - 1)
                    state[f"ws_{kind}"] = state[f"{kind}_sockets_up"] > 0
                log.warning("WS CLOSED | %s | shard=%d", kind.upper(), shard_idx + 1)

            websocket.WebSocketApp(
                ws_url(base, symbols), on_open=opened, on_message=message,
                on_error=error, on_close=closed, header=["User-Agent: cryptoalpha-v3/1.1"],
            ).run_forever(ping_interval=20, ping_timeout=10, ping_payload="cryptoalpha", skip_utf8_validation=True)
        except Exception as exc:
            with lock:
                state["errors"] += 1
                state["last_error"] = str(exc)
            log.warning("WS EXCEPTION | %s | shard=%d | %s", kind.upper(), shard_idx + 1, exc)

        stable = opened_at and (time.monotonic() - opened_at >= 30.0)
        backoff = 1.0 if stable else min(30.0, backoff * 2.0)
        time.sleep(backoff + random.uniform(0.0, 0.5))


def start_ws_group(kind, base, store):
    state[f"{kind}_sockets"] = len(SHARDS)
    for i, symbols in enumerate(SHARDS):
        threading.Thread(target=connect_shard, args=(kind, base, store, i, symbols), daemon=True, name=f"ws-{kind}-{i + 1}").start()


def mid(book):
    if not book:
        return None
    if time.monotonic() * 1000.0 - float(book[4]) > STALE:
        return None
    return (float(book[0]) + float(book[1])) / 2.0


def px(store, symbol):
    return mid(store.get(symbol))


def triangular(a, b, c, eq):
    notional = min(max(0.01, eq * RISK), MAX_NOTIONAL)
    p1, p2, p3 = px(spot, a + c), px(spot, a + b), px(spot, b + c)
    if not all((p1, p2, p3)):
        return None
    implied, cross = p1 / p3, p2
    gross1 = (cross / implied - 1.0) * 10000.0
    gross2 = (implied / cross - 1.0) * 10000.0
    gross = max(gross1, gross2)
    net = gross - 3.0 * FEE - 3.0 * SLIP
    if net <= MIN_NET:
        return None
    direction = f"USDT->{a}->{b}->USDT" if gross1 >= gross2 else f"USDT->{b}->{a}->USDT"
    key, now = "S:" + a + b + c, time.monotonic() * 1000.0
    # seen[key] is (timestamp, net_bps); subtract the timestamp, not the tuple.
    if key in seen and now - seen[key][0] < 75.0 and abs(net - seen[key][1]) < 0.05:
        return None
    seen[key] = (now, net)
    return {"type":"SPOT_TRIANGULAR_CROSS","path":direction,"net_bps":net,"gross_bps":gross,"notional_usdt":notional,"paper_only":True}


def futures_cross(sym, eq):
    sp, fu = px(spot, sym), px(fut, sym)
    if not sp or not fu:
        return None
    basis = (fu / sp - 1.0) * 10000.0
    net = abs(basis) - 4.0 * FEE - 2.0 * SLIP - 1.0
    if net <= MIN_NET:
        return None
    direction = "BUY_SPOT_SELL_FUTURES" if basis > 0 else "SELL_SPOT_BUY_FUTURES"
    notional = min(max(0.01, eq * RISK), MAX_NOTIONAL)
    key, now = "F:" + sym, time.monotonic() * 1000.0
    # Same tuple-timestamp fix for futures throttling.
    if key in seen and now - seen[key][0] < 250.0 and abs(net - seen[key][1]) < 0.05:
        return None
    seen[key] = (now, net)
    return {"type":"SPOT_FUTURES_CROSS","symbol":sym,"direction":direction,"basis_bps":basis,"net_bps":net,"notional_usdt":notional,"paper_only":True}


def scan_loop():
    while True:
        try:
            with lock:
                eq = state["equity"]
            spots = [x for r in CROSS for x in [triangular(*r, eq)] if x]
            futures = [x for s in SYMS for x in [futures_cross(s, eq)] if x]
            spots.sort(key=lambda x: x["net_bps"], reverse=True)
            futures.sort(key=lambda x: x["net_bps"], reverse=True)
            for opportunity in spots[:1] + futures[:1]:
                with lock:
                    n = min(opportunity["notional_usdt"], max(0.01, state["equity"] * RISK), MAX_NOTIONAL)
                    profit = n * opportunity["net_bps"] / 10000.0
                    state["equity"] += profit
                    state["peak"] = max(state["peak"], state["equity"])
                    state["paper_pnl"] += profit
                    if opportunity["type"].startswith("SPOT_TRI"):
                        state["spot_entries"] += 1
                    else:
                        state["futures_entries"] += 1
                    state["wins"] += 1 if profit >= 0 else 0
                    state["losses"] += 1 if profit < 0 else 0
            with lock:
                state["scans"] += 1
                state["spot_opportunities"] = len(spots)
                state["futures_opportunities"] = len(futures)
                if spots:
                    state["best_spot"] = spots[0]
                if futures:
                    state["best_futures"] = futures[0]
        except Exception as exc:
            with lock:
                state["errors"] += 1
                state["last_error"] = str(exc)
            log.exception("SCAN ERROR")
        time.sleep(SCAN)


def stats():
    with lock:
        e = state["equity"]
        return {"status":"ok","engine":"cryptoalpha-spot-futures-cross-v3","mode":"PAPER_ONLY","live_execution":False,
                "starting_equity":START,"equity":round(e,6),"compound_return_pct":round((e/START-1)*100,5),"paper_pnl":round(state["paper_pnl"],6),
                "spot_entries":state["spot_entries"],"futures_entries":state["futures_entries"],"paper_entries":state["spot_entries"]+state["futures_entries"],
                "spot_opportunities":state["spot_opportunities"],"futures_opportunities":state["futures_opportunities"],"wins":state["wins"],"losses":state["losses"],
                "drawdown_pct":round(max(0,(state["peak"]-e)/state["peak"])*100,4),"scans":state["scans"],"errors":state["errors"],
                "best_spot":state["best_spot"],"best_futures":state["best_futures"],"ws_spot":state["ws_spot"],"ws_futures":state["ws_futures"],
                "spot_sockets":state["spot_sockets"],"spot_sockets_up":state["spot_sockets_up"],"futures_sockets":state["futures_sockets"],"futures_sockets_up":state["futures_sockets_up"],
                "quote_updates_spot":state["spot_updates"],"quote_updates_futures":state["futures_updates"],"symbols":len(SYMS),"cross_routes":len(CROSS),
                "last_error":state["last_error"],"uptime_seconds":round(time.time()-state["started"],1)}

HTML='''<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Cryptoalpha Spot + Futures Cross</title><style>body{margin:0;background:#080b12;color:#e8edf5;font:14px system-ui}.w{max-width:1000px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;gap:10px}.brand{font-size:24px;font-weight:800}.pill{padding:7px;border:1px solid #31533e;border-radius:20px}.g{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.c{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px}.l{font-size:11px;color:#8e9aae;text-transform:uppercase}.v{font-size:21px;font-weight:800;margin-top:5px}@media(max-width:700px){.g{grid-template-columns:repeat(2,1fr)}}@media(max-width:450px){.g{grid-template-columns:1fr}}</style><div class=w><div class=top><div><div class=brand>Cryptoalpha Spot + Futures Cross</div><div>Real-market data · paper execution · LIVE OFF</div></div><div class=pill>PAPER ONLY · LIVE OFF</div></div><div class=g><div class=c><div class=l>Equity</div><div class=v>$<span id=e>—</span></div></div><div class=c><div class=l>Compounded return</div><div class=v id=r>—</div></div><div class=c><div class=l>Spot fills</div><div class=v id=sf>—</div></div><div class=c><div class=l>Futures cross fills</div><div class=v id=ff>—</div></div></div><div class=g><div class=c><div class=l>Spot cross opps</div><div class=v id=so>—</div></div><div class=c><div class=l>Futures cross opps</div><div class=v id=fo>—</div></div><div class=c><div class=l>Spot WS</div><div class=v id=sw>—</div></div><div class=c><div class=l>Futures WS</div><div class=v id=fw>—</div></div></div><div class=c><b>CONNECTION HEALTH</b><p id=conn>Waiting…</p></div><div class=c><b>FUTURES CROSS</b><p>Spot↔USDT-M Futures basis arbitrage, with fees, slippage and funding buffer deducted. Both directions are simulated. No leverage is used by the paper engine. LIVE execution is disabled.</p><div id=b>Waiting for executable edge…</div></div><div class=c><b>System</b><p id=s>Starting…</p></div></div><script>async function u(){try{let x=await(await fetch('/stats.json?'+Date.now(),{cache:'no-store'})).json();e.textContent=Number(x.equity).toFixed(4);r.textContent=Number(x.compound_return_pct).toFixed(3)+'%';sf.textContent=x.spot_entries;ff.textContent=x.futures_entries;so.textContent=x.spot_opportunities;fo.textContent=x.futures_opportunities;sw.textContent=x.ws_spot?'CONNECTED':'RECONNECTING';fw.textContent=x.ws_futures?'CONNECTED':'RECONNECTING';conn.textContent='Spot sockets '+x.spot_sockets_up+'/'+x.spot_sockets+' · Futures sockets '+x.futures_sockets_up+'/'+x.futures_sockets+' · automatic exponential reconnect';let f=x.best_futures?x.best_futures.symbol+' · '+Number(x.best_futures.net_bps).toFixed(2)+' bps · '+x.best_futures.direction:'—';let p=x.best_spot?x.best_spot.path+' · '+Number(x.best_spot.net_bps).toFixed(2)+' bps':'—';b.textContent='Best futures: '+f+' | Best spot: '+p;s.textContent='SPOT + FUTURES CROSS · scans '+x.scans+' · errors '+x.errors+' · symbols '+x.symbols+' · routes '+x.cross_routes+' · spot updates '+x.quote_updates_spot+' · futures updates '+x.quote_updates_futures+' · uptime '+x.uptime_seconds+'s'+(x.last_error?' · '+x.last_error:'')}catch(z){s.textContent='Dashboard fetch error: '+z}}u();setInterval(u,1000)</script>'''

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/stats.json'): body=json.dumps(stats()).encode(); ct='application/json'
        elif self.path.startswith('/healthz'): body=b'ok'; ct='text/plain'
        else: body=HTML.encode(); ct='text/html'
        self.send_response(200); self.send_header('Content-Type',ct); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body)
    def log_message(self,*a): pass

def main():
    start_ws_group("spot","wss://stream.binance.com:443/stream",spot)
    start_ws_group("futures","wss://fstream.binance.com/stream",fut)
    threading.Thread(target=scan_loop,daemon=True,name="scanner").start()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),H).serve_forever()

if __name__=='__main__': main()
