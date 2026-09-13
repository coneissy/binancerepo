"""Dry-run-first Binance USD-M Futures meme momentum/microstructure scalper.

30-second aggressive paper strategy. Live execution remains intentionally disabled.
Entries combine multi-horizon momentum, order-book imbalance, microprice, trade flow,
spread/cost checks and a fast confirmation. Exits are volatility-aware with a hard
30-second time stop, profit trailing and reversal protection.
"""
import json
import logging
import math
import os
import threading
import time
from collections import defaultdict, deque

import requests
import websocket

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
WS_SHARDS = max(1, int(os.getenv("WS_SHARDS", "8")))
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "0"))

# Aggressive, but only trade liquid enough markets.
MIN_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "500000"))
NEW_LISTING_WINDOW = float(os.getenv("NEW_LISTING_WINDOW_SECONDS", "1200"))
NEW_LISTING_MIN_QV = float(os.getenv("NEW_LISTING_MIN_QUOTE_VOLUME", "100000"))
LISTING_REFRESH_SECONDS = float(os.getenv("LISTING_REFRESH_SECONDS", "10"))

# Fast entry model: one strong confirmation is enough; weak signals still fail.
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.62"))
NEW_ENTRY_SCORE = float(os.getenv("NEW_LISTING_ENTRY_SCORE", "0.70"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "6"))
NEW_MAX_POSITIONS = int(os.getenv("NEW_LISTING_MAX_POSITIONS", "2"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "3"))
NEW_COOLDOWN = float(os.getenv("NEW_LISTING_COOLDOWN_SECONDS", "8"))

# Costs are included before an entry is accepted.
FEE_PER_SIDE = float(os.getenv("FEE_PER_SIDE_PCT", "0.0004"))
SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SLIPPAGE_PCT", "0.0001"))
MIN_EDGE_MULT = float(os.getenv("MIN_EDGE_MULTIPLIER", "1.45"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "6"))
NEW_MAX_SPREAD_BPS = float(os.getenv("NEW_LISTING_MAX_SPREAD_BPS", "8"))

# 30-second scalping exits. TP is intentionally larger than total estimated costs.
MIN_TP = float(os.getenv("MIN_TP_PCT", "0.0025"))
MAX_TP = float(os.getenv("MAX_TP_PCT", "0.0090"))
MIN_SL = float(os.getenv("MIN_SL_PCT", "0.0016"))
MAX_SL = float(os.getenv("MAX_SL_PCT", "0.0035"))
TP_VOL_MULT = float(os.getenv("TP_VOL_MULTIPLIER", "3.5"))
SL_VOL_MULT = float(os.getenv("SL_VOL_MULTIPLIER", "1.35"))
MAX_HOLD = 30.0
NEW_MAX_HOLD = float(os.getenv("NEW_LISTING_MAX_HOLD_SECONDS", "30"))
TRAIL_START = float(os.getenv("TRAIL_START_PCT", "0.0018"))
TRAIL_GIVEBACK = float(os.getenv("TRAIL_GIVEBACK_PCT", "0.0010"))

STATE_LEN = int(os.getenv("STATE_LEN", "240"))
MIN_EVENTS = int(os.getenv("MIN_EVENTS", "18"))

MEME_SYMBOLS = {
    "DOGE", "SHIB", "1000SHIB", "PEPE", "1000PEPE", "FLOKI", "BONK", "WIF",
    "MEME", "MEMES", "BRETT", "TURBO", "NEIRO", "1000NEIRO", "PNUT", "ACT", "GOAT",
    "MOODENG", "CHILLGUY", "POPCAT", "DOGS", "CAT", "MEW", "MYRO", "BOME", "SLERF",
    "SUNDOG", "MOG", "PONKE", "WHY", "TOSHI", "BAN", "FARTCOIN", "ARC", "JELLYJELLY",
    "PIPPIN", "SWARMS", "AVA", "AVAAI", "TRUMP", "MELANIA", "SPX", "GIGA", "FWOG",
    "BROCCOLI", "BABYDOGE", "1000BABYDOGE", "PUMP", "DOOD", "ZEREBRO", "MOTHER",
    "RETARDIO", "1000BONK", "1000FLOKI", "1000CAT", "1000CHEEMS", "1000SATS",
}
MEME_PREFIXES = ("1000", "1M")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("meme-scalper")
http = requests.Session()

state = {}
quote_volume = {}
first_seen = {}
known_symbols = set()
positions = {}
last_entry = {}
confirmations = defaultdict(int)
lock = threading.RLock()
metrics = defaultdict(int)
performance = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "return": 0.0})


def public(path, params=None):
    r = http.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def make_state():
    return {
        "bid": 0.0, "ask": 0.0, "bq": 0.0, "aq": 0.0, "last": 0.0,
        "prices": deque(maxlen=STATE_LEN), "flow": deque(maxlen=STATE_LEN),
        "flow_abs": deque(maxlen=STATE_LEN), "last_side": None, "last_event": 0.0,
    }


def is_meme(symbol):
    base = symbol.upper().removesuffix("USDT")
    return base in MEME_SYMBOLS or base.startswith(MEME_PREFIXES)


def exchange_symbols():
    info = public("/fapi/v1/exchangeInfo")
    result = {}
    for x in info.get("symbols", []):
        if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL" and x.get("quoteAsset") == "USDT":
            result[x["symbol"].lower()] = {"onboard": int(x.get("onboardDate", 0) or 0)}
    return result


def refresh_universe(startup=False):
    meta = exchange_symbols()
    now = time.time()
    new_meme = []
    for s, info in meta.items():
        if s in known_symbols:
            continue
        known_symbols.add(s)
        onboard = info.get("onboard", 0)
        if onboard and now - onboard / 1000.0 <= NEW_LISTING_WINDOW:
            first_seen[s] = onboard / 1000.0
        elif not onboard and not startup:
            first_seen[s] = now
        if not startup and is_meme(s):
            new_meme.append(s)
            metrics["new_listings"] += 1
            log.warning("NEW MEME LISTING DETECTED | %s", s.upper())

    tickers = public("/fapi/v1/ticker/24hr")
    ranked = []
    for t in tickers:
        s = str(t.get("symbol", "")).lower()
        if s not in meta or not is_meme(s):
            continue
        try:
            qv = float(t.get("quoteVolume", 0) or 0)
            move = abs(float(t.get("priceChangePercent", 0) or 0)) / 100.0
            quote_volume[s] = qv
            is_new = s in first_seen and now - first_seen[s] <= NEW_LISTING_WINDOW
            if qv >= (NEW_LISTING_MIN_QV if is_new else MIN_QV):
                # Liquidity and movement both matter; this is ranking, not an entry signal.
                ranked.append((qv * max(move, 0.003), s))
                if is_new:
                    log.warning("NEW LISTING ACTIVE | %s age=%.0fs qv=%.0f", s.upper(), now - first_seen[s], qv)
        except (TypeError, ValueError):
            continue
    ranked.sort(reverse=True)
    selected = [s for _, s in ranked]
    if UNIVERSE_SIZE > 0:
        selected = selected[:UNIVERSE_SIZE]
    for s in selected:
        state.setdefault(s, make_state())
    metrics["listing_refreshes"] += 1
    log.warning("MEME UNIVERSE | matched=%d selected=%d new=%d", len(ranked), len(selected), len(new_meme))
    return selected


def clamp(x, lo=0.0, hi=1.0):
    return min(max(x, lo), hi)


def nd(x, scale):
    return clamp(x / max(scale, 1e-12))


def score_symbol(s):
    st = state.get(s)
    if not st or st["bid"] <= 0 or st["ask"] <= 0:
        metrics["no_book"] += 1
        return None
    ps = list(st["prices"])
    fs = list(st["flow"])
    if len(ps) < MIN_EVENTS or len(fs) < MIN_EVENTS:
        metrics["short_state"] += 1
        return None

    now = time.time()
    is_new = s in first_seen and now - first_seen[s] <= NEW_LISTING_WINDOW
    if quote_volume.get(s, 0.0) < (NEW_LISTING_MIN_QV if is_new else MIN_QV):
        metrics["liquidity_reject"] += 1
        return None

    mid = (st["bid"] + st["ask"]) * 0.5
    spread_bps = (st["ask"] - st["bid"]) / max(mid, 1e-12) * 10000.0
    if spread_bps > (NEW_MAX_SPREAD_BPS if is_new else MAX_SPREAD_BPS):
        metrics["spread_reject"] += 1
        return None

    # Fast horizons: roughly a few ticks, ~0.5-2 seconds and a short trend window.
    m2 = ps[-1] / ps[-3] - 1.0
    m5 = ps[-1] / ps[-6] - 1.0
    m12 = ps[-1] / ps[-13] - 1.0

    recent = fs[-6:]
    prior = fs[-18:-6]
    prior_abs = sum(abs(x) for x in prior) / max(len(prior), 1)
    flow_ratio = sum(recent) / max(prior_abs * len(recent), 1e-12)
    flow_accel = (sum(recent[-3:]) - sum(recent[:3])) / max(prior_abs * 3.0, 1e-12)

    imbalance = (st["bq"] - st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro = (st["ask"] * st["bq"] + st["bid"] * st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro_edge = (micro - mid) / max(mid, 1e-12)

    returns = [math.log(ps[i] / ps[i - 1]) for i in range(max(1, len(ps) - 16), len(ps)) if ps[i - 1] > 0 and ps[i] > 0]
    vol = math.sqrt(sum(r * r for r in returns) / max(len(returns), 1))
    if vol < 0.00002 and not is_new:
        metrics["vol_reject"] += 1
        return None

    long_score = (
        nd(imbalance, 0.55) * 0.20 + nd(micro_edge, 0.00035) * 0.12
        + nd(m2, 0.00045) * 0.18 + nd(m5, 0.00075) * 0.18
        + nd(m12, 0.00120) * 0.10 + nd(flow_ratio, 1.0) * 0.15
        + nd(flow_accel, 0.8) * 0.07
    )
    short_score = (
        nd(-imbalance, 0.55) * 0.20 + nd(-micro_edge, 0.00035) * 0.12
        + nd(-m2, 0.00045) * 0.18 + nd(-m5, 0.00075) * 0.18
        + nd(-m12, 0.00120) * 0.10 + nd(-flow_ratio, 1.0) * 0.15
        + nd(-flow_accel, 0.8) * 0.07
    )

    side = "BUY" if long_score >= short_score else "SELL"
    score = max(long_score, short_score)
    signed_momentum = (m2 * 0.45 + m5 * 0.35 + m12 * 0.20) if side == "BUY" else -(m2 * 0.45 + m5 * 0.35 + m12 * 0.20)
    signed_flow = flow_ratio if side == "BUY" else -flow_ratio

    # Aggressive means fast entries, not buying directly into opposing flow.
    if signed_momentum <= 0 or signed_flow < -0.05:
        metrics["confirmation_reject"] += 1
        confirmations[s] = 0
        return None

    threshold = NEW_ENTRY_SCORE if is_new else ENTRY_SCORE
    if score < threshold:
        metrics["score_reject"] += 1
        confirmations[s] = 0
        return None

    # One confirmation: fast enough for 30s scalping while filtering single-tick spikes.
    confirmations[s] += 1
    if confirmations[s] < 1:
        metrics["confirmation_wait"] += 1
        return None

    round_trip_cost = 2.0 * FEE_PER_SIDE + 2.0 * SLIPPAGE_PCT + spread_bps / 10000.0
    projected_move = max(abs(m2), abs(m5) * 0.85, abs(m12) * 0.55, vol * (1.15 + 0.85 * score))
    expected_edge = projected_move * (0.85 + 0.90 * score) - round_trip_cost
    metrics["cost_checks"] += 1
    if expected_edge < round_trip_cost * MIN_EDGE_MULT:
        metrics["cost_reject"] += 1
        confirmations[s] = 0
        return None

    # Volatility-scaled exits, constrained to a positive reward/risk profile.
    target = clamp(max(MIN_TP, vol * TP_VOL_MULT, abs(m2) * 2.4), MIN_TP, MAX_TP)
    stop = clamp(max(MIN_SL, vol * SL_VOL_MULT), MIN_SL, MAX_SL)
    target = clamp(target * (0.92 + 0.38 * score), MIN_TP, MAX_TP)
    # Never intentionally configure a target smaller than 1.35x the stop.
    target = max(target, min(MAX_TP, stop * 1.35))
    price = st["ask"] if side == "BUY" else st["bid"]
    return side, score, price, target, stop, expected_edge, is_new


def enter(s, signal):
    side, score, price, target, stop, expected_edge, is_new = signal
    now = time.time()
    with lock:
        if s in positions or len(positions) >= MAX_POSITIONS:
            return
        if is_new and sum(1 for p in positions.values() if p.get("new_listing")) >= NEW_MAX_POSITIONS:
            return
        cd = NEW_COOLDOWN if is_new else COOLDOWN
        if now - last_entry.get(s, 0.0) < cd:
            return
        positions[s] = {
            "side": side, "entry": price, "opened": now, "score": score,
            "tp": target, "sl": stop, "expected_edge": expected_edge,
            "new_listing": is_new, "peak": price, "trailing": False,
        }
        last_entry[s] = now
        confirmations[s] = 0
        metrics["entries"] += 1
        if is_new:
            metrics["new_listing_entries"] += 1
    log.warning(
        "ENTRY %s %s score=%.3f edge=%.3f%% tp=%.3f%% sl=%.3f%% hold<=30s%s price=%.10g [DRY RUN]",
        side, s.upper(), score, expected_edge * 100, target * 100, stop * 100,
        " NEW-LISTING" if is_new else "", price,
    )


def exit_position(s, p, px, ret, reason, held):
    metrics["exits"] += 1
    bucket = "new" if p.get("new_listing") else "regular"
    performance[bucket]["trades"] += 1
    performance[bucket]["return"] += ret
    if ret > 0:
        performance[bucket]["wins"] += 1
    else:
        performance[bucket]["losses"] += 1
    metrics[f"exit_{reason.lower()}"] += 1
    log.warning("EXIT %s %s return=%.3f%% held=%.2fs", reason, s.upper(), ret * 100, held)


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
            held = now - p["opened"]
            if p["side"] == "BUY":
                p["peak"] = max(p["peak"], px)
                drawdown = p["peak"] / px - 1.0
            else:
                p["peak"] = min(p["peak"], px)
                drawdown = px / p["peak"] - 1.0

            reason = None
            if ret >= p["tp"]:
                reason = "TP"
            elif ret <= -p["sl"]:
                reason = "SL"
            elif ret >= TRAIL_START and drawdown >= TRAIL_GIVEBACK:
                reason = "TRAIL"
            elif held >= (NEW_MAX_HOLD if p.get("new_listing") else MAX_HOLD):
                reason = "TIME"
            else:
                sig = score_symbol(s)
                if sig and sig[0] != p["side"] and sig[1] >= (NEW_ENTRY_SCORE if p.get("new_listing") else ENTRY_SCORE):
                    reason = "REVERSAL"
            if reason:
                positions.pop(s, None)
                exits.append((s, p, px, ret, reason, held))
    for row in exits:
        exit_position(*row)


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        event = d.get("e")
        s = str(d.get("s", "")).lower()
        if not s or s not in state:
            return
        st = state[s]
        metrics["events"] += 1
        if event == "bookTicker":
            st["bid"] = float(d["b"]); st["ask"] = float(d["a"])
            st["bq"] = float(d["B"]); st["aq"] = float(d["A"])
            st["last_event"] = time.time()
        elif event == "aggTrade":
            px = float(d["p"]); qty = float(d["q"]); notional = px * qty
            signed = -notional if d.get("m") else notional
            st["last"] = px; st["prices"].append(px); st["flow"].append(signed); st["flow_abs"].append(abs(signed))
            st["last_event"] = time.time()
        else:
            return
        sig = score_symbol(s)
        if sig:
            metrics["signals"] += 1
            enter(s, sig)
    except Exception:
        log.exception("market-data message error")


def stream_url(symbols):
    streams = []
    for s in symbols:
        streams += [f"{s}@bookTicker", f"{s}@aggTrade"]
    return WS_BASE + "?streams=" + "/".join(streams)


def run_ws(symbols, shard_id):
    if not symbols:
        return
    url = stream_url(symbols)
    while True:
        try:
            log.info("WS shard %d connecting symbols=%d", shard_id, len(symbols))
            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=lambda _, e: log.warning("WS shard %d error: %s", shard_id, e),
                on_close=lambda _, c, m: log.warning("WS shard %d closed: %s %s", shard_id, c, m),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            log.exception("WS shard %d failed", shard_id)
        metrics["reconnects"] += 1
        time.sleep(2)


def websocket_manager():
    last_signature = None
    while True:
        try:
            current = refresh_universe(startup=not known_symbols)
            signature = tuple(current)
            if signature != last_signature:
                shards = [[] for _ in range(min(WS_SHARDS, max(1, len(current))))]
                for i, s in enumerate(current):
                    shards[i % len(shards)].append(s)
                for i, shard in enumerate(shards, 1):
                    threading.Thread(target=run_ws, args=(shard, i), daemon=True).start()
                    metrics["ws_subscriptions"] += len(shard)
                last_signature = signature
        except Exception:
            log.exception("universe refresh failed")
        time.sleep(LISTING_REFRESH_SECONDS)


def monitor():
    while True:
        manage_positions()
        time.sleep(0.04)


def print_stats():
    wins = performance["regular"]["wins"] + performance["new"]["wins"]
    trades = performance["regular"]["trades"] + performance["new"]["trades"]
    ret = performance["regular"]["return"] + performance["new"]["return"]
    wr = wins / trades * 100.0 if trades else 0.0
    log.warning(
        "STATS events=%d signals=%d entries=%d exits=%d trades=%d wins=%d winrate=%.1f%% return=%.3f%% positions=%d new_entries=%d",
        metrics["events"], metrics["signals"], metrics["entries"], metrics["exits"], trades, wins, wr, ret * 100,
        len(positions), metrics["new_listing_entries"],
    )
    log.warning(
        "REJECTS spread=%d score=%d cost=%d flow=%d vol=%d liquidity=%d short_state=%d confirm=%d",
        metrics["spread_reject"], metrics["score_reject"], metrics["cost_reject"], metrics["confirmation_reject"],
        metrics["vol_reject"], metrics["liquidity_reject"], metrics["short_state"], metrics["confirmation_wait"],
    )
    log.warning(
        "EXITS TP=%d SL=%d TRAIL=%d TIME=%d REVERSAL=%d | regular trades=%d return=%.3f%% | new trades=%d return=%.3f%%",
        metrics["exit_tp"], metrics["exit_sl"], metrics["exit_trail"], metrics["exit_time"], metrics["exit_reversal"],
        performance["regular"]["trades"], performance["regular"]["return"] * 100,
        performance["new"]["trades"], performance["new"]["return"] * 100,
    )


def main():
    if not DRY_RUN:
        raise RuntimeError("Live execution is disabled. Keep DRY_RUN=true until paper results are validated.")
    refresh_universe(startup=True)
    threading.Thread(target=websocket_manager, name="universe-manager", daemon=True).start()
    threading.Thread(target=monitor, name="exit-engine", daemon=True).start()
    log.warning("ENGINE STARTED | 30S AGGRESSIVE DRY-RUN | meme_universe=ALL new_listing_detection=ON")
    while True:
        time.sleep(10)
        print_stats()


if __name__ == "__main__":
    main()
