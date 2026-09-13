"""Dry-run-first Binance USD-M Futures meme scalper with live new-listing subscriptions."""
import json
import logging
import math
import os
import threading
import time
from collections import defaultdict, deque

import requests
import websocket
from polymorph_ai import decide as polymorph_decide

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "0"))  # 0 = every matched meme contract
WS_SHARDS = max(1, int(os.getenv("WS_SHARDS", "5")))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.30"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "5"))
TP_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.0008"))
SL_PCT = float(os.getenv("STOP_LOSS_PCT", "0.0012"))
MAX_HOLD = float(os.getenv("MAX_HOLD_SECONDS", "8"))
MIN_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "0"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "15"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "0.15"))
STATE_LEN = int(os.getenv("STATE_LEN", "120"))
FEE_PER_SIDE = float(os.getenv("FEE_PER_SIDE_PCT", "0.0004"))
SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SLIPPAGE_PCT", "0.0001"))
MIN_EDGE_MULT = float(os.getenv("MIN_EDGE_MULTIPLIER", "1.25"))
TP_VOL_MULT = float(os.getenv("TP_VOL_MULTIPLIER", "2.0"))
SL_VOL_MULT = float(os.getenv("SL_VOL_MULTIPLIER", "1.2"))
MIN_TP_PCT = float(os.getenv("MIN_TP_PCT", "0.0008"))
MAX_TP_PCT = float(os.getenv("MAX_TP_PCT", "0.0040"))
MIN_SL_PCT = float(os.getenv("MIN_SL_PCT", "0.0008"))
MAX_SL_PCT = float(os.getenv("MAX_SL_PCT", "0.0025"))

NEW_LISTING_WINDOW = float(os.getenv("NEW_LISTING_WINDOW_SECONDS", "900"))
NEW_LISTING_MIN_QV = float(os.getenv("NEW_LISTING_MIN_QUOTE_VOLUME", "250000"))
NEW_LISTING_MIN_LIVE_NOTIONAL = float(os.getenv("NEW_LISTING_MIN_LIVE_NOTIONAL", "50000"))
NEW_LISTING_SCORE = float(os.getenv("NEW_LISTING_ENTRY_SCORE", "0.42"))
NEW_LISTING_TP = float(os.getenv("NEW_LISTING_TP_PCT", "0.0025"))
NEW_LISTING_SL = float(os.getenv("NEW_LISTING_SL_PCT", "0.0018"))
NEW_LISTING_HOLD = float(os.getenv("NEW_LISTING_MAX_HOLD_SECONDS", "20"))
NEW_LISTING_COOLDOWN = float(os.getenv("NEW_LISTING_COOLDOWN_SECONDS", "1.0"))
NEW_LISTING_MAX_POSITIONS = int(os.getenv("NEW_LISTING_MAX_POSITIONS", "2"))
LISTING_REFRESH_SECONDS = float(os.getenv("LISTING_REFRESH_SECONDS", "15"))

MEME_SYMBOLS = {
    "DOGE", "SHIB", "1000SHIB", "PEPE", "1000PEPE", "FLOKI", "BONK", "WIF",
    "MEME", "MEMES", "BRETT", "TURBO", "NEIRO", "1000NEIRO", "PNUT", "ACT", "GOAT",
    "MOODENG", "MOO", "CHILLGUY", "POPCAT", "DOGS", "CAT", "MEW", "MYRO", "BOME",
    "SLERF", "SUNDOG", "MOG", "PONKE", "WHY", "TOSHI", "BAN", "FARTCOIN", "ARC",
    "JELLYJELLY", "PIPPIN", "SWARMS", "AVA", "AVAAI", "TRUMP", "MELANIA", "SPX",
    "GIGA", "FWOG", "GOCHU", "BROCCOLI", "BABYDOGE", "1000BABYDOGE", "PUMP", "DOOD",
    "ZEREBRO", "MOTHER", "RETARDIO",
}
MEME_PREFIXES = ("1000", "1M")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hft-scalper")
http = requests.Session()
state = {}
quote_volume = {}
positions = {}
last_entry = {}
known_symbols = set()
first_seen = {}
lock = threading.RLock()
ws_lock = threading.RLock()
ws_clients = {}
shard_symbols = {}
ws_request_id = 0
metrics = {
    "events": 0, "signals": 0, "entries": 0, "exits": 0, "reconnects": 0,
    "no_book": 0, "short_state": 0, "spread_reject": 0, "vol_reject": 0,
    "score_reject": 0, "cost_reject": 0, "flow_reject": 0, "liquidity_reject": 0,
    "long_candidates": 0, "short_candidates": 0, "ai_boosts": 0, "cost_checks": 0,
    "new_listings": 0, "new_listing_entries": 0, "listing_refreshes": 0,
    "ws_subscriptions": 0, "ws_subscription_errors": 0,
}
performance = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "return": 0.0})


def public(path, params=None):
    r = http.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def make_state():
    return {
        "bid": 0.0, "ask": 0.0, "bq": 0.0, "aq": 0.0, "last": 0.0,
        "prices": deque(maxlen=STATE_LEN), "flow": deque(maxlen=STATE_LEN),
        "flow_abs": deque(maxlen=STATE_LEN), "last_event": 0.0,
    }


def is_meme_symbol(symbol):
    base = symbol.upper().removesuffix("USDT")
    return base in MEME_SYMBOLS or base.startswith(MEME_PREFIXES)


def exchange_symbols():
    info = public("/fapi/v1/exchangeInfo")
    result = {}
    for x in info.get("symbols", []):
        if x.get("status") != "TRADING" or x.get("contractType") != "PERPETUAL" or x.get("quoteAsset") != "USDT":
            continue
        s = x["symbol"].lower()
        result[s] = {"onboard": int(x.get("onboardDate", 0) or 0), "pair": x.get("pair", "")}
    return result


def discover_new_listings(symbol_meta, startup=False):
    now = time.time()
    fresh = []
    for s, meta in symbol_meta.items():
        if s in known_symbols:
            continue
        known_symbols.add(s)
        onboard = meta.get("onboard", 0)
        if onboard > 0:
            age = max(0.0, now - onboard / 1000.0)
            if age <= NEW_LISTING_WINDOW:
                first_seen[s] = onboard / 1000.0
        elif not startup:
            first_seen[s] = now
        fresh.append(s)
        if not startup and is_meme_symbol(s):
            metrics["new_listings"] += 1
            log.warning("NEW MEME LISTING DETECTED | %s", s.upper())
    return fresh


def universe():
    meta = exchange_symbols()
    discover_new_listings(meta, startup=True)
    tickers = public("/fapi/v1/ticker/24hr")
    ranked = []
    for t in tickers:
        s = t.get("symbol", "").lower()
        if s not in meta or not is_meme_symbol(s):
            continue
        try:
            qv = float(t.get("quoteVolume", 0) or 0)
            move = abs(float(t.get("priceChangePercent", 0) or 0)) / 100.0
            quote_volume[s] = qv
            ranked.append((qv * max(move, 0.002), s))
            if s in first_seen and time.time() - first_seen[s] <= NEW_LISTING_WINDOW and qv >= NEW_LISTING_MIN_QV:
                log.warning("NEW LISTING ACTIVE | %s age=%.0fs qv=%.0f", s.upper(), time.time() - first_seen[s], qv)
        except (TypeError, ValueError):
            continue
    ranked.sort(reverse=True)
    selected = [s for _, s in ranked if qv_allowed(s)]
    if UNIVERSE_SIZE > 0:
        selected = selected[:UNIVERSE_SIZE]
    log.warning("MEME UNIVERSE | matched=%d selected=%d symbols=%s", len(ranked), len(selected), ",".join(x.upper() for x in selected))
    return selected


def qv_allowed(s):
    return quote_volume.get(s, 0.0) >= MIN_QV


def stream_url(symbols):
    streams = [f"{s}@bookTicker" for s in symbols] + [f"{s}@aggTrade" for s in symbols]
    return WS_BASE + "?streams=" + "/".join(streams)


def _clamp(x, lo=0.0, hi=1.0):
    return min(max(x, lo), hi)


def score_symbol(s):
    st = state.get(s)
    if not st or st["bid"] <= 0 or st["ask"] <= 0:
        metrics["no_book"] += 1
        return None
    is_new = s in first_seen and time.time() - first_seen[s] <= NEW_LISTING_WINDOW
    if not is_new and not qv_allowed(s):
        metrics["liquidity_reject"] += 1
        return None
    live_notional = sum(st["flow_abs"])
    if is_new and live_notional < NEW_LISTING_MIN_LIVE_NOTIONAL:
        metrics["liquidity_reject"] += 1
        return None
    mid = (st["bid"] + st["ask"]) * 0.5
    spread_bps = (st["ask"] - st["bid"]) / mid * 10000.0
    ps, fs = st["prices"], st["flow"]
    if spread_bps > MAX_SPREAD_BPS * (1.5 if is_new else 1.0):
        metrics["spread_reject"] += 1
        return None
    if len(ps) < 8 or len(fs) < 10:
        metrics["short_state"] += 1
        return None
    imbalance = (st["bq"] - st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro = (st["ask"] * st["bq"] + st["bid"] * st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    micro_edge = (micro - mid) / mid
    momentum = ps[-1] / ps[-min(8, len(ps))] - 1.0
    recent = list(fs[-5:])
    previous = list(fs[-10:-5])
    recent_flow, previous_flow = sum(recent), sum(previous)
    previous_abs = sum(abs(x) for x in previous) / 5.0
    flow_edge = recent_flow / max(previous_abs * 5.0, 1e-12)
    directional_accel = (recent_flow - previous_flow) / max(previous_abs * 5.0, 1e-12)
    returns = [math.log(ps[i] / ps[i - 1]) for i in range(max(1, len(ps) - 12), len(ps)) if ps[i - 1] > 0]
    vol = math.sqrt(sum(r * r for r in returns) / max(len(returns), 1))
    if vol < 0.00002 and not is_new:
        metrics["vol_reject"] += 1
        return None
    long_score = (
        _clamp(imbalance) * 0.30 + _clamp(micro_edge / 0.0004) * 0.18 +
        _clamp(momentum / 0.0010) * 0.22 + _clamp(flow_edge / 1.5) * 0.18 +
        _clamp(directional_accel) * 0.12
    )
    short_score = (
        _clamp(-imbalance) * 0.30 + _clamp(-micro_edge / 0.0004) * 0.18 +
        _clamp(-momentum / 0.0010) * 0.22 + _clamp(-flow_edge / 1.5) * 0.18 +
        _clamp(-directional_accel) * 0.12
    )
    side = "BUY" if long_score > short_score else "SELL"
    signed_flow = flow_edge if side == "BUY" else -flow_edge
    if signed_flow < -0.15:
        metrics["flow_reject"] += 1
        return None
    score = max(long_score, short_score)
    if len(ps) >= 15:
        ai = polymorph_decide(s.upper(), list(ps)[-40:])
        if ai.action == side:
            score = min(1.0, score + (0.08 if ai.confidence >= 2 / 3 else 0.03))
            metrics["ai_boosts"] += 1
    threshold = NEW_LISTING_SCORE if is_new else ENTRY_SCORE
    round_trip_cost = 2.0 * FEE_PER_SIDE + 2.0 * SLIPPAGE_PCT + spread_bps / 10000.0
    projected_move = max(abs(momentum), abs(micro_edge) * 2.0, vol * (1.0 + score), abs(directional_accel) * vol)
    expected_edge = projected_move * (0.50 + 0.75 * score) - round_trip_cost
    metrics["cost_checks"] += 1
    if expected_edge <= round_trip_cost * (MIN_EDGE_MULT - 1.0):
        metrics["cost_reject"] += 1
        return None
    if score < threshold:
        metrics["score_reject"] += 1
        metrics["long_candidates" if long_score >= short_score else "short_candidates"] += 1
        return None
    if is_new:
        target, stop = NEW_LISTING_TP, NEW_LISTING_SL
    else:
        target = _clamp(max(MIN_TP_PCT, vol * TP_VOL_MULT, abs(momentum) * 1.5, TP_PCT), MIN_TP_PCT, MAX_TP_PCT)
        stop = _clamp(max(MIN_SL_PCT, vol * SL_VOL_MULT, SL_PCT), MIN_SL_PCT, MAX_SL_PCT)
        target = _clamp(target * (0.90 + 0.35 * score), MIN_TP_PCT, MAX_TP_PCT)
    price = st["ask"] if side == "BUY" else st["bid"]
    return side, score, price, target, stop, expected_edge, is_new


def enter(s, side, score, price, target, stop, expected_edge, is_new=False):
    now = time.time()
    with lock:
        if s in positions or len(positions) >= MAX_POSITIONS:
            return
        if is_new and sum(1 for p in positions.values() if p.get("new_listing")) >= NEW_LISTING_MAX_POSITIONS:
            return
        cooldown = NEW_LISTING_COOLDOWN if is_new else COOLDOWN
        if now - last_entry.get(s, 0.0) < cooldown:
            return
        positions[s] = {
            "side": side, "entry": price, "opened": now, "score": score,
            "tp": target, "sl": stop, "expected_edge": expected_edge, "new_listing": is_new,
        }
        last_entry[s] = now
        metrics["entries"] += 1
        if is_new:
            metrics["new_listing_entries"] += 1
    log.warning(
        "ENTRY %s %s score=%.3f edge=%.3f%% tp=%.3f%% sl=%.3f%% price=%.10g%s [DRY RUN]",
        side, s.upper(), score, expected_edge * 100, target * 100, stop * 100, price,
        " NEW-LISTING-SNIPER" if is_new else "",
    )


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
            hold_limit = NEW_LISTING_HOLD if p.get("new_listing") else MAX_HOLD
            reason = "TP" if ret >= p["tp"] else "SL" if ret <= -p["sl"] else "TIME" if now - p["opened"] >= hold_limit else None
            if reason:
                positions.pop(s, None)
                metrics["exits"] += 1
                perf = performance[s]
                perf["trades"] += 1
                perf["return"] += ret
                perf["wins" if ret > 0 else "losses"] += 1
                exits.append((s, p, px, ret, reason, now - p["opened"]))
    for s, p, px, ret, reason, held in exits:
        log.warning(
            "EXIT %s %s return=%.3f%% held=%.2fs%s",
            reason, s.upper(), ret * 100, held, " NEW-LISTING" if p.get("new_listing") else "",
        )


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        event = d.get("e")
        s = d.get("s", "").lower()
        if not s or s not in state:
            return
        if event == "bookTicker":
            with lock:
                st = state[s]
                st["bid"], st["ask"] = float(d["b"]), float(d["a"])
                st["bq"], st["aq"] = float(d["B"]), float(d["A"])
                st["last_event"] = time.time()
        elif event == "aggTrade":
            px, qty = float(d["p"]), float(d["q"])
            signed = -px * qty if d.get("m") else px * qty
            with lock:
                st = state[s]
                st["last"] = px
                st["prices"].append(px)
                st["flow"].append(signed)
                st["flow_abs"].append(abs(signed))
                st["last_event"] = time.time()
        else:
            return
        metrics["events"] += 1
        sig = score_symbol(s)
        if sig:
            metrics["signals"] += 1
            enter(s, *sig)
    except Exception:
        log.exception("market-data message error")


def _next_request_id():
    global ws_request_id
    with ws_lock:
        ws_request_id += 1
        return ws_request_id


def _subscribe(ws, symbols, shard):
    if not symbols or not ws or not ws.sock or not ws.sock.connected:
        return False
    params = []
    for s in symbols:
        params.extend((f"{s}@bookTicker", f"{s}@aggTrade"))
    payload = {"method": "SUBSCRIBE", "params": params, "id": _next_request_id()}
    try:
        ws.send(json.dumps(payload))
        metrics["ws_subscriptions"] += len(symbols)
        log.warning("WS shard %d subscribed new symbols=%s", shard, ",".join(s.upper() for s in symbols))
        return True
    except Exception:
        metrics["ws_subscription_errors"] += 1
        log.exception("WS shard %d dynamic subscribe failed", shard)
        return False


def add_symbols_to_live_ws(symbols):
    fresh = [s for s in symbols if is_meme_symbol(s)]
    if not fresh:
        return
    with ws_lock:
        for s in fresh:
            state.setdefault(s, make_state())
            if any(s in items for items in shard_symbols.values()):
                continue
            if not shard_symbols:
                shard = 1
                shard_symbols[shard] = set()
            else:
                shard = min(shard_symbols, key=lambda k: len(shard_symbols[k]))
            shard_symbols[shard].add(s)
            ws = ws_clients.get(shard)
            if ws and ws.sock and ws.sock.connected:
                _subscribe(ws, [s], shard)


def run_ws(initial_symbols, shard):
    with ws_lock:
        shard_symbols.setdefault(shard, set(initial_symbols))
    while True:
        try:
            with ws_lock:
                symbols = sorted(shard_symbols.get(shard, set()))
            if not symbols:
                time.sleep(1)
                continue
            log.info("WS shard %d connecting symbols=%d", shard, len(symbols))
            ws = websocket.WebSocketApp(
                stream_url(symbols),
                on_message=on_message,
                on_error=lambda _, e: log.warning("WS shard %d error: %s", shard, e),
                on_close=lambda _, c, m: log.warning("WS shard %d closed: %s %s", shard, c, m),
                on_open=lambda w: log.info("WS shard %d opened symbols=%d", shard, len(symbols)),
            )
            with ws_lock:
                ws_clients[shard] = ws
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            log.exception("WS shard %d failed", shard)
        finally:
            with ws_lock:
                if ws_clients.get(shard) is ws if 'ws' in locals() else False:
                    ws_clients.pop(shard, None)
        metrics["reconnects"] += 1
        time.sleep(1)


def monitor():
    while True:
        manage_positions()
        time.sleep(0.01)


def listing_monitor():
    while True:
        try:
            meta = exchange_symbols()
            metrics["listing_refreshes"] += 1
            fresh = discover_new_listings(meta, startup=False)
            meme_fresh = [s for s in fresh if is_meme_symbol(s)]
            if meme_fresh:
                log.warning("NEW LISTINGS QUEUED + SUBSCRIBING | %s", ",".join(s.upper() for s in meme_fresh))
                add_symbols_to_live_ws(meme_fresh)
            else:
                # Keep all already-known meme contracts available to scoring after a reconnect.
                add_symbols_to_live_ws([s for s in known_symbols if is_meme_symbol(s)])
        except Exception:
            log.exception("listing monitor error")
        time.sleep(LISTING_REFRESH_SECONDS)


def main():
    if not DRY_RUN:
        raise RuntimeError("Live execution is disabled in this engine. Keep DRY_RUN=true until paper results are validated.")
    symbols = universe()
    if not symbols:
        raise RuntimeError("No Binance meme USDT perpetuals matched the configured universe.")
    for s in symbols:
        state[s] = make_state()
    shard_count = min(WS_SHARDS, max(1, len(symbols)))
    shards = [[] for _ in range(shard_count)]
    for i, s in enumerate(symbols):
        shards[i % shard_count].append(s)
    for i, shard in enumerate(shards, 1):
        shard_symbols[i] = set(shard)
    log.warning(
        "ENGINE STARTED | MEME-ONLY symbols=%d shards=%d dry_run=%s new_listing_window=%ss all_meme=%s",
        len(symbols), shard_count, DRY_RUN, NEW_LISTING_WINDOW, UNIVERSE_SIZE == 0,
    )
    for i, shard in enumerate(shards, 1):
        threading.Thread(target=run_ws, args=(shard, i), daemon=True).start()
    threading.Thread(target=monitor, name="exit-engine", daemon=True).start()
    threading.Thread(target=listing_monitor, name="listing-monitor", daemon=True).start()
    while True:
        time.sleep(10)
        log.info(
            "metrics events=%d signals=%d entries=%d exits=%d reconnects=%d positions=%d new_listings=%d new_entries=%d listing_refreshes=%d ws_sub=%d ws_sub_err=%d rejects={book:%d state:%d liq:%d spread:%d vol:%d flow:%d cost:%d score:%d} ai_boosts=%d",
            metrics["events"], metrics["signals"], metrics["entries"], metrics["exits"], metrics["reconnects"], len(positions),
            metrics["new_listings"], metrics["new_listing_entries"], metrics["listing_refreshes"], metrics["ws_subscriptions"], metrics["ws_subscription_errors"],
            metrics["no_book"], metrics["short_state"], metrics["liquidity_reject"], metrics["spread_reject"], metrics["vol_reject"],
            metrics["flow_reject"], metrics["cost_reject"], metrics["score_reject"], metrics["ai_boosts"],
        )


if __name__ == "__main__":
    main()
