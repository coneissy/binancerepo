"""Event-driven, dry-run-first Binance USD-M Futures scalper.

Aggressive HFT-inspired paper scalper with a transparent PolyMorph enhancer.
This is not exchange-colocated HFT. Market data is WebSocket-first; REST is
used only during startup to build the liquid universe. Live execution remains
disabled.
"""
import json
import logging
import math
import os
import threading
import time
from collections import deque

import requests
import websocket

from polymorph_ai import decide as polymorph_decide

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "100"))
WS_SHARDS = max(1, int(os.getenv("WS_SHARDS", "5")))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.30"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "5"))
TP_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.0008"))
SL_PCT = float(os.getenv("STOP_LOSS_PCT", "0.0012"))
MAX_HOLD = float(os.getenv("MAX_HOLD_SECONDS", "8"))
MIN_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "3000000"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "15"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "0.15"))
STATE_LEN = int(os.getenv("STATE_LEN", "240"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hft-scalper")
http = requests.Session()

state = {}
positions = {}
last_entry = {}
lock = threading.RLock()
metrics = {"events": 0, "signals": 0, "entries": 0, "exits": 0, "reconnects": 0,
           "no_book": 0, "short_state": 0, "spread_reject": 0, "vol_reject": 0,
           "score_reject": 0, "long_candidates": 0, "short_candidates": 0,
           "ai_boosts": 0}


def public(path, params=None):
    r = http.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def make_state():
    return {"bid": 0.0, "ask": 0.0, "bq": 0.0, "aq": 0.0, "last": 0.0,
            "prices": deque(maxlen=STATE_LEN), "flow": deque(maxlen=STATE_LEN), "last_event": 0.0}


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
            if qv >= MIN_QV:
                ranked.append((qv * max(move, 0.002), s))
        except (TypeError, ValueError):
            continue
    ranked.sort(reverse=True)
    return [s for _, s in ranked[:UNIVERSE_SIZE]]


def stream_url(symbols):
    streams = []
    for s in symbols:
        streams.append(f"{s}@bookTicker")
        streams.append(f"{s}@aggTrade")
    return WS_BASE + "?streams=" + "/".join(streams)


def score_symbol(s):
    """Aggressive microstructure + PolyMorph score. Returns (side, score, price) or None."""
    st = state.get(s)
    if not st or st["bid"] <= 0 or st["ask"] <= 0:
        metrics["no_book"] += 1
        return None
    mid = (st["bid"] + st["ask"]) * 0.5
    spread_bps = (st["ask"] - st["bid"]) / mid * 10000.0
    if spread_bps > MAX_SPREAD_BPS:
        metrics["spread_reject"] += 1
        return None
    ps = st["prices"]
    fs = st["flow"]
    # Short warm-up: begin scoring quickly instead of waiting for a long history.
    if len(ps) < 12 or len(fs) < 6:
        metrics["short_state"] += 1
        return None

    imbalance = (st["bq"] - st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro = (st["ask"] * st["bq"] + st["bid"] * st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro_edge = (micro - mid) / mid
    momentum = ps[-1] / ps[-min(8, len(ps))] - 1.0
    recent_flow = sum(fs[-6:])
    base_slice = fs[-30:-6] if len(fs) > 6 else fs
    base_flow = sum(abs(x) for x in base_slice) / max(len(base_slice), 1)
    flow_edge = recent_flow / max(base_flow, 1e-12)
    returns = [math.log(ps[i] / ps[i - 1]) for i in range(max(1, len(ps) - 20), len(ps)) if ps[i - 1] > 0]
    vol = math.sqrt(sum(r * r for r in returns) / max(len(returns), 1))

    long_score = 0.0
    short_score = 0.0
    long_score += min(max(imbalance, 0.0), 1.0) * 0.35
    short_score += min(max(-imbalance, 0.0), 1.0) * 0.35
    long_score += min(max(micro_edge / 0.0005, 0.0), 1.0) * 0.20
    short_score += min(max(-micro_edge / 0.0005, 0.0), 1.0) * 0.20
    long_score += min(max(momentum / 0.0015, 0.0), 1.0) * 0.25
    short_score += min(max(-momentum / 0.0015, 0.0), 1.0) * 0.25
    long_flow = max(flow_edge, 0.0)
    short_flow = max(-flow_edge, 0.0)
    long_score += min(long_flow / 2.0, 1.0) * 0.20
    short_score += min(short_flow / 2.0, 1.0) * 0.20

    # Only reject a truly dead market. Aggressive mode should trade active movement.
    if vol < 0.00001:
        metrics["vol_reject"] += 1
        return None

    side = "BUY" if long_score > short_score else "SELL"
    score = max(long_score, short_score)

    # PolyMorph is a soft enhancer, never a hard gate.
    if len(ps) >= 30:
        ai = polymorph_decide(s.upper(), list(ps)[-60:])
        if ai.action == side and ai.confidence >= (2 / 3):
            score = min(1.0, score + 0.10)
            metrics["ai_boosts"] += 1
        elif ai.action == side and ai.confidence >= (1 / 3):
            score = min(1.0, score + 0.05)
            metrics["ai_boosts"] += 1

    if score < ENTRY_SCORE:
        metrics["score_reject"] += 1
        if long_score >= short_score:
            metrics["long_candidates"] += 1
        else:
            metrics["short_candidates"] += 1
        return None
    return side, score, st["ask"] if side == "BUY" else st["bid"]


def enter(s, side, score, price):
    now = time.time()
    with lock:
        if s in positions or len(positions) >= MAX_POSITIONS:
            return
        if now - last_entry.get(s, 0.0) < COOLDOWN:
            return
        positions[s] = {"side": side, "entry": price, "opened": now, "score": score}
        last_entry[s] = now
        metrics["entries"] += 1
    log.warning("ENTRY %s %s score=%.3f price=%.10g [DRY RUN]", side, s.upper(), score, price)


def manage_positions():
    now = time.time()
    exits = []
    with lock:
        for s, p in list(positions.items()):
            st = state.get(s)
            if not st:
                continue
            px = st["bid"] if p["side"] == "BUY" else st["ask"]
            if px <= 0:
                continue
            ret = px / p["entry"] - 1.0 if p["side"] == "BUY" else p["entry"] / px - 1.0
            reason = None
            if ret >= TP_PCT:
                reason = "TP"
            elif ret <= -SL_PCT:
                reason = "SL"
            elif now - p["opened"] >= MAX_HOLD:
                reason = "TIME"
            else:
                sig = score_symbol(s)
                if sig and sig[0] != p["side"] and sig[1] >= ENTRY_SCORE:
                    reason = "REVERSAL"
            if reason:
                positions.pop(s, None)
                metrics["exits"] += 1
                exits.append((s, p, px, ret, reason, now - p["opened"]))
    for s, p, px, ret, reason, held in exits:
        log.warning("EXIT %s %s entry=%.10g exit=%.10g return=%.3f%% held=%.2fs", reason, s.upper(), p["entry"], px, ret * 100, held)


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        event = d.get("e")
        s = d.get("s", "").lower()
        if not s:
            return
        st = state.get(s)
        if st is None:
            return
        metrics["events"] += 1
        if event == "bookTicker":
            st["bid"] = float(d["b"])
            st["ask"] = float(d["a"])
            st["bq"] = float(d["B"])
            st["aq"] = float(d["A"])
            st["last_event"] = time.time()
        elif event == "aggTrade":
            px = float(d["p"])
            qty = float(d["q"])
            signed = -px * qty if d.get("m") else px * qty
            st["last"] = px
            st["prices"].append(px)
            st["flow"].append(signed)
            st["last_event"] = time.time()
        else:
            return
        sig = score_symbol(s)
        if sig:
            metrics["signals"] += 1
            enter(s, *sig)
    except Exception:
        log.exception("market-data message error")


def run_ws(url, shard):
    while True:
        try:
            log.info("WS shard %d connecting", shard)
            ws = websocket.WebSocketApp(url, on_message=on_message,
                                        on_error=lambda _, e: log.warning("WS shard %d error: %s", shard, e),
                                        on_close=lambda _, c, m: log.warning("WS shard %d closed: %s %s", shard, c, m))
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            log.exception("WS shard %d failed", shard)
        metrics["reconnects"] += 1
        time.sleep(2)


def monitor():
    while True:
        manage_positions()
        time.sleep(0.01)


def main():
    if not DRY_RUN:
        raise RuntimeError("Live execution is disabled in this engine. Keep DRY_RUN=true until paper results are validated.")
    symbols = universe()
    for s in symbols:
        state[s] = make_state()
    shards = [[] for _ in range(min(WS_SHARDS, max(1, len(symbols))))]
    for i, s in enumerate(symbols):
        shards[i % len(shards)].append(s)
    log.warning("ENGINE STARTED | symbols=%d shards=%d dry_run=%s", len(symbols), len(shards), DRY_RUN)
    for i, shard_symbols in enumerate(shards, 1):
        threading.Thread(target=run_ws, args=(stream_url(shard_symbols), i), daemon=True).start()
    threading.Thread(target=monitor, name="exit-engine", daemon=True).start()
    while True:
        time.sleep(10)
        log.info("metrics events=%d signals=%d entries=%d exits=%d reconnects=%d positions=%d rejections={book:%d state:%d spread:%d vol:%d score:%d candidates:L%d/S%d ai_boosts:%d}",
                 metrics["events"], metrics["signals"], metrics["entries"], metrics["exits"], metrics["reconnects"], len(positions),
                 metrics["no_book"], metrics["short_state"], metrics["spread_reject"], metrics["vol_reject"], metrics["score_reject"],
                 metrics["long_candidates"], metrics["short_candidates"], metrics["ai_boosts"])


if __name__ == "__main__":
    main()
