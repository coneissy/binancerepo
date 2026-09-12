import json
import logging
import os
import threading
import time
from collections import defaultdict, deque

import requests
import websocket

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "100"))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.80"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "3"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.0025"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.0015"))
MAX_HOLD_SECONDS = float(os.getenv("MAX_HOLD_SECONDS", "45"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "5000000"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hft-scalper")
S = requests.Session()

prices = defaultdict(lambda: deque(maxlen=180))
trades = defaultdict(lambda: deque(maxlen=180))
positions = {}
lock = threading.RLock()


def public(path, params=None):
    r = S.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def ema(xs, n):
    k = 2.0 / (n + 1.0)
    e = xs[0]
    for x in xs[1:]:
        e = x * k + e * (1.0 - k)
    return e


def rsi(xs, n=14):
    if len(xs) <= n:
        return 50.0
    d = [xs[i] - xs[i - 1] for i in range(1, len(xs))][-n:]
    g = sum(max(x, 0.0) for x in d) / n
    l = sum(max(-x, 0.0) for x in d) / n
    if l == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + g / l)


def universe():
    info = public("/fapi/v1/exchangeInfo")
    allowed = {x["symbol"].lower() for x in info["symbols"]
               if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
               and x.get("quoteAsset") == "USDT"}
    tickers = public("/fapi/v1/ticker/24hr")
    ranked = []
    for t in tickers:
        s = t.get("symbol", "").lower()
        if s not in allowed:
            continue
        try:
            qv = float(t.get("quoteVolume", 0))
            move = abs(float(t.get("priceChangePercent", 0))) / 100.0
            if qv >= MIN_24H_QUOTE_VOLUME:
                ranked.append((qv * max(move, 0.002), s))
        except (TypeError, ValueError):
            pass
    ranked.sort(reverse=True)
    return [s for _, s in ranked[:UNIVERSE_SIZE]]


def make_stream(symbols):
    streams = []
    for s in symbols:
        # aggTrade gives every aggregated trade update with very low processing overhead.
        streams.append(f"{s}@aggTrade")
    return WS_BASE + "?streams=" + "/".join(streams)


def signal(symbol):
    with lock:
        xs = list(prices[symbol])
        ts = list(trades[symbol])
    if len(xs) < 30:
        return None
    last = xs[-1]
    e5, e9, e21 = ema(xs, 5), ema(xs, 9), ema(xs, 21)
    rr = rsi(xs)
    mom = last / xs[-4] - 1.0
    recent = ts[-20:]
    if len(recent) < 10:
        return None
    baseline = sum(ts[:-20]) / max(len(ts[:-20]), 1) if len(ts) > 20 else sum(recent) / len(recent)
    flow = (sum(recent) / len(recent)) / max(baseline, 1e-9)

    long_score = 0.0
    short_score = 0.0
    if e5 > e9 > e21 and last > e5:
        long_score += 0.35
    if e5 < e9 < e21 and last < e5:
        short_score += 0.35
    if rr >= 55:
        long_score += 0.20
    if rr <= 45:
        short_score += 0.20
    if mom >= 0.0008:
        long_score += 0.20
    if mom <= -0.0008:
        short_score += 0.20
    if flow >= 1.20:
        if mom > 0:
            long_score += 0.15
        elif mom < 0:
            short_score += 0.15
    score = max(long_score, short_score)
    if score < ENTRY_SCORE:
        return None
    return ("BUY" if long_score > short_score else "SELL", score, last)


def paper_entry(symbol, side, score, price):
    with lock:
        if symbol in positions or len(positions) >= MAX_POSITIONS:
            return
        positions[symbol] = {"side": side, "entry": price, "opened": time.time(), "score": score}
    log.warning("ENTRY %s %s score=%.2f price=%s%s", side, symbol.upper(), score, price,
                " [DRY RUN]" if DRY_RUN else "")


def manage_positions():
    now = time.time()
    exits = []
    with lock:
        snapshot = list(positions.items())
        for symbol, p in snapshot:
            px = prices[symbol][-1] if prices[symbol] else p["entry"]
            ret = (px / p["entry"] - 1.0) if p["side"] == "BUY" else (p["entry"] / px - 1.0)
            reason = None
            if ret >= TAKE_PROFIT_PCT:
                reason = "TP"
            elif ret <= -STOP_LOSS_PCT:
                reason = "SL"
            elif now - p["opened"] >= MAX_HOLD_SECONDS:
                reason = "TIME"
            if reason:
                exits.append((symbol, p, px, ret, reason))
                positions.pop(symbol, None)
    for symbol, p, px, ret, reason in exits:
        log.warning("EXIT %s %s entry=%s exit=%s return=%.4f%% held=%.1fs",
                    reason, symbol.upper(), p["entry"], px, ret * 100, now - p["opened"])


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        symbol = d.get("s", "").lower()
        price = float(d["p"])
        qty = float(d.get("q", 0))
        if not symbol:
            return
        with lock:
            prices[symbol].append(price)
            trades[symbol].append(qty * price)
        sig = signal(symbol)
        if sig:
            side, score, px = sig
            paper_entry(symbol, side, score, px)
    except Exception:
        log.exception("websocket message error")


def run_ws(url):
    while True:
        try:
            log.info("Connecting market-data websocket for 100-symbol universe")
            ws = websocket.WebSocketApp(url, on_message=on_message,
                                        on_error=lambda _, e: log.warning("WS error: %s", e),
                                        on_close=lambda _, c, m: log.warning("WS closed: %s %s", c, m))
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            log.exception("WS connection failed")
        time.sleep(2)


def main():
    if not DRY_RUN:
        raise RuntimeError("Live execution is intentionally not enabled by this engine yet; keep DRY_RUN=true while validating signals.")
    symbols = universe()
    log.warning("HFT-STYLE ENGINE STARTED: %d symbols, dry_run=%s", len(symbols), DRY_RUN)
    url = make_stream(symbols)
    threading.Thread(target=run_ws, args=(url,), daemon=True).start()
    while True:
        manage_positions()
        time.sleep(0.05)


if __name__ == "__main__":
    main()
