"""Event-driven, dry-run-first Binance USD-M Futures meme scalper.

HFT-inspired paper mode. The strategy is meme-only, cost-aware, and uses
order-book imbalance, microprice, momentum, aggressive-flow acceleration,
volatility/regime filters, PolyMorph confirmation, and adaptive exits.
Live execution remains disabled.
"""
import json
import logging
import math
import os
import threading
import time
from collections import deque, defaultdict

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
MIN_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "15"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "0.15"))
STATE_LEN = int(os.getenv("STATE_LEN", "120"))

# Cost-aware controls. Defaults are intentionally conservative estimates for
# paper trading; tune them to the actual fee tier/slippage before live use.
FEE_PER_SIDE = float(os.getenv("FEE_PER_SIDE_PCT", "0.0004"))
SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SLIPPAGE_PCT", "0.0001"))
MIN_EDGE_MULT = float(os.getenv("MIN_EDGE_MULTIPLIER", "1.25"))
TP_VOL_MULT = float(os.getenv("TP_VOL_MULTIPLIER", "2.0"))
SL_VOL_MULT = float(os.getenv("SL_VOL_MULTIPLIER", "1.2"))
MIN_TP_PCT = float(os.getenv("MIN_TP_PCT", "0.0008"))
MAX_TP_PCT = float(os.getenv("MAX_TP_PCT", "0.0040"))
MIN_SL_PCT = float(os.getenv("MIN_SL_PCT", "0.0008"))
MAX_SL_PCT = float(os.getenv("MAX_SL_PCT", "0.0025"))
FLOW_ACCEL_MIN = float(os.getenv("FLOW_ACCEL_MIN", "1.15"))

MEME_SYMBOLS = {
    "DOGE", "SHIB", "1000SHIB", "PEPE", "1000PEPE", "FLOKI", "BONK",
    "WIF", "MEME", "MEMES", "BRETT", "TURBO", "NEIRO", "1000NEIRO",
    "PNUT", "ACT", "GOAT", "MOODENG", "MOO", "CHILLGUY", "POPCAT",
    "DOGS", "CAT", "MEW", "MYRO", "BOME", "SLERF", "SUNDOG", "MOG",
    "PONKE", "WHY", "TOSHI", "BAN", "FARTCOIN", "ARC", "JELLYJELLY",
    "PIPPIN", "SWARMS", "AVA", "AVAAI", "TRUMP", "MELANIA", "SPX",
    "GIGA", "FWOG", "GOCHU", "BROCCOLI", "BABYDOGE", "1000BABYDOGE",
    "PUMP", "DOOD", "ZEREBRO", "MOTHER", "RETARDIO",
}
MEME_PREFIXES = ("1000", "1M")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hft-scalper")
http = requests.Session()

state = {}
positions = {}
last_entry = {}
lock = threading.RLock()
metrics = {"events": 0, "signals": 0, "entries": 0, "exits": 0, "reconnects": 0,
           "no_book": 0, "short_state": 0, "spread_reject": 0, "vol_reject": 0,
           "score_reject": 0, "cost_reject": 0, "flow_reject": 0,
           "long_candidates": 0, "short_candidates": 0, "ai_boosts": 0,
           "cost_checks": 0}
performance = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "return": 0.0})


def public(path, params=None):
    r = http.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def make_state():
    return {"bid": 0.0, "ask": 0.0, "bq": 0.0, "aq": 0.0, "last": 0.0,
            "prices": deque(maxlen=STATE_LEN), "flow": deque(maxlen=STATE_LEN),
            "flow_abs": deque(maxlen=STATE_LEN), "last_event": 0.0}


def is_meme_symbol(symbol):
    base = symbol.upper().removesuffix("USDT")
    return base in MEME_SYMBOLS or base.startswith(MEME_PREFIXES)


def universe():
    info = public("/fapi/v1/exchangeInfo")
    allowed = {x["symbol"].lower() for x in info["symbols"]
               if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
               and x.get("quoteAsset") == "USDT"}
    tickers = public("/fapi/v1/ticker/24hr")
    ranked = []
    for t in tickers:
        s = t.get("symbol", "").lower()
        if s not in allowed or not is_meme_symbol(s):
            continue
        try:
            qv = float(t.get("quoteVolume", 0))
            move = abs(float(t.get("priceChangePercent", 0))) / 100.0
            if qv >= MIN_QV:
                ranked.append((qv * max(move, 0.002), s))
        except (TypeError, ValueError):
            continue
    ranked.sort(reverse=True)
    selected = [s for _, s in ranked[:UNIVERSE_SIZE]]
    log.warning("MEME UNIVERSE | matched=%d selected=%d symbols=%s", len(ranked), len(selected), ",".join(x.upper() for x in selected))
    return selected


def stream_url(symbols):
    streams = []
    for s in symbols:
        streams.append(f"{s}@bookTicker")
        streams.append(f"{s}@aggTrade")
    return WS_BASE + "?streams=" + "/".join(streams)


def _clamp(x, lo=0.0, hi=1.0):
    return min(max(x, lo), hi)


def score_symbol(s):
    st = state.get(s)
    if not st or st["bid"] <= 0 or st["ask"] <= 0:
        metrics["no_book"] += 1
        return None
    mid = (st["bid"] + st["ask"]) * 0.5
    spread_bps = (st["ask"] - st["bid"]) / mid * 10000.0
    if spread_bps > MAX_SPREAD_BPS:
        metrics["spread_reject"] += 1
        return None
    ps, fs = st["prices"], st["flow"]
    if len(ps) < 8 or len(fs) < 10:
        metrics["short_state"] += 1
        return None

    imbalance = (st["bq"] - st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro = (st["ask"] * st["bq"] + st["bid"] * st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro_edge = (micro - mid) / mid
    momentum = ps[-1] / ps[-min(8, len(ps))] - 1.0

    # Aggressive-flow pressure and acceleration. A rising buy/sell imbalance
    # is more useful for a scalper than raw cumulative flow alone.
    recent = list(fs[-5:])
    previous = list(fs[-10:-5])
    recent_flow = sum(recent)
    previous_flow = sum(previous)
    recent_abs = sum(abs(x) for x in recent) / 5.0
    previous_abs = sum(abs(x) for x in previous) / 5.0
    flow_edge = recent_flow / max(previous_abs * 5.0, 1e-12)
    flow_accel = recent_abs / max(previous_abs, 1e-12)
    directional_accel = (recent_flow - previous_flow) / max(previous_abs * 5.0, 1e-12)

    returns = [math.log(ps[i] / ps[i - 1]) for i in range(max(1, len(ps) - 12), len(ps)) if ps[i - 1] > 0]
    vol = math.sqrt(sum(r * r for r in returns) / max(len(returns), 1))
    if vol < 0.00002:
        metrics["vol_reject"] += 1
        return None

    long_score = (_clamp(imbalance) * 0.30
                  + _clamp(micro_edge / 0.0004) * 0.18
                  + _clamp(momentum / 0.0010) * 0.22
                  + _clamp(flow_edge / 1.5) * 0.18
                  + _clamp(directional_accel / 1.0) * 0.12)
    short_score = (_clamp(-imbalance) * 0.30
                   + _clamp(-micro_edge / 0.0004) * 0.18
                   + _clamp(-momentum / 0.0010) * 0.22
                   + _clamp(-flow_edge / 1.5) * 0.18
                   + _clamp(-directional_accel / 1.0) * 0.12)

    # Avoid chasing a move where aggressive flow is collapsing.
    side = "BUY" if long_score > short_score else "SELL"
    signed_flow = flow_edge if side == "BUY" else -flow_edge
    if signed_flow < -0.15:
        metrics["flow_reject"] += 1
        return None

    score = max(long_score, short_score)
    if len(ps) >= 15:
        ai = polymorph_decide(s.upper(), list(ps)[-40:])
        if ai.action == side:
            score = min(1.0, score + (0.08 if ai.confidence >= 2/3 else 0.03))
            metrics["ai_boosts"] += 1

    # Estimate whether the signal can clear fees, spread and slippage.
    round_trip_cost = 2.0 * FEE_PER_SIDE + 2.0 * SLIPPAGE_PCT + spread_bps / 10000.0
    projected_move = max(abs(momentum), abs(micro_edge) * 2.0, vol * (1.0 + score), abs(directional_accel) * vol)
    expected_edge = projected_move * (0.50 + 0.75 * score) - round_trip_cost
    metrics["cost_checks"] += 1
    if expected_edge <= round_trip_cost * (MIN_EDGE_MULT - 1.0):
        metrics["cost_reject"] += 1
        return None

    if score < ENTRY_SCORE:
        metrics["score_reject"] += 1
        metrics["long_candidates" if long_score >= short_score else "short_candidates"] += 1
        return None

    # Adaptive exits scale with current realized volatility while respecting
    # hard bounds so a sudden meme spike cannot create an absurd stop/target.
    target = _clamp(max(MIN_TP_PCT, vol * TP_VOL_MULT, abs(momentum) * 1.5), MIN_TP_PCT, MAX_TP_PCT)
    stop = _clamp(max(MIN_SL_PCT, vol * SL_VOL_MULT), MIN_SL_PCT, MAX_SL_PCT)
    # A high score deserves a little more room to run; weak signals exit faster.
    target *= 0.90 + 0.35 * score
    target = _clamp(target, MIN_TP_PCT, MAX_TP_PCT)
    return side, score, st["ask"] if side == "BUY" else st["bid"], target, stop, expected_edge


def enter(s, side, score, price, target, stop, expected_edge):
    now = time.time()
    with lock:
        if s in positions or len(positions) >= MAX_POSITIONS:
            return
        if now - last_entry.get(s, 0.0) < COOLDOWN:
            return
        positions[s] = {"side": side, "entry": price, "opened": now, "score": score,
                        "tp": target, "sl": stop, "expected_edge": expected_edge}
        last_entry[s] = now
        metrics["entries"] += 1
    log.warning("ENTRY %s %s score=%.3f edge=%.3f%% tp=%.3f%% sl=%.3f%% price=%.10g [DRY RUN]",
                side, s.upper(), score, expected_edge * 100, target * 100, stop * 100, price)


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
            reason = "TP" if ret >= p["tp"] else "SL" if ret <= -p["sl"] else "TIME" if now - p["opened"] >= MAX_HOLD else None
            if reason:
                positions.pop(s, None)
                metrics["exits"] += 1
                perf = performance[s]
                perf["trades"] += 1
                perf["return"] += ret
                perf["wins" if ret > 0 else "losses"] += 1
                exits.append((s, p, px, ret, reason, now - p["opened"]))
    for s, p, px, ret, reason, held in exits:
        log.warning("EXIT %s %s entry=%.10g exit=%.10g return=%.3f%% held=%.2fs",
                    reason, s.upper(), p["entry"], px, ret * 100, held)


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        event = d.get("e")
        s = d.get("s", "").lower()
        if not s or s not in state:
            return
        metrics["events"] += 1
        st = state[s]
        if event == "bookTicker":
            st["bid"], st["ask"] = float(d["b"]), float(d["a"])
            st["bq"], st["aq"] = float(d["B"]), float(d["A"])
            st["last_event"] = time.time()
        elif event == "aggTrade":
            px, qty = float(d["p"]), float(d["q"])
            signed = -px * qty if d.get("m") else px * qty
            st["last"] = px
            st["prices"].append(px)
            st["flow"].append(signed)
            st["flow_abs"].append(abs(signed))
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
        time.sleep(1)


def monitor():
    while True:
        manage_positions()
        time.sleep(0.01)


def main():
    if not DRY_RUN:
        raise RuntimeError("Live execution is disabled in this engine. Keep DRY_RUN=true until paper results are validated.")
    symbols = universe()
    if not symbols:
        raise RuntimeError("No Binance meme USDT perpetuals matched the configured liquidity filter.")
    for s in symbols:
        state[s] = make_state()
    shards = [[] for _ in range(min(WS_SHARDS, max(1, len(symbols))))]
    for i, s in enumerate(symbols):
        shards[i % len(shards)].append(s)
    log.warning("ENGINE STARTED | MEME-ONLY symbols=%d shards=%d dry_run=%s", len(symbols), len(shards), DRY_RUN)
    for i, shard_symbols in enumerate(shards, 1):
        threading.Thread(target=run_ws, args=(stream_url(shard_symbols), i), daemon=True).start()
    threading.Thread(target=monitor, name="exit-engine", daemon=True).start()
    while True:
        time.sleep(10)
        with lock:
            open_positions = len(positions)
            profitable = sum(v["wins"] for v in performance.values())
            closed = sum(v["trades"] for v in performance.values())
            realized = sum(v["return"] for v in performance.values())
        log.info("metrics events=%d signals=%d entries=%d exits=%d reconnects=%d positions=%d closed=%d wins=%d realized=%.3f%% rejections={book:%d state:%d spread:%d vol:%d flow:%d cost:%d score:%d candidates:L%d/S%d ai_boosts:%d}",
                 metrics["events"], metrics["signals"], metrics["entries"], metrics["exits"], metrics["reconnects"], open_positions,
                 closed, profitable, realized * 100, metrics["no_book"], metrics["short_state"], metrics["spread_reject"],
                 metrics["vol_reject"], metrics["flow_reject"], metrics["cost_reject"], metrics["score_reject"],
                 metrics["long_candidates"], metrics["short_candidates"], metrics["ai_boosts"])


if __name__ == "__main__":
    main()
