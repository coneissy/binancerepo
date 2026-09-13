"""Ultra+ Dynamic Top-20 meme futures engine.

Dry-run first. The engine scans the Binance USD-M perpetual meme universe,
keeps a dynamic Top-20 by risk-adjusted opportunity quality, confirms entries
with multi-timeframe + microstructure evidence, and exits on thesis decay.
It is deliberately selective: no trade is better than a low-edge trade.
"""
import json
import logging
import math
import os
import threading
import time
from collections import deque
from statistics import mean

import requests
import websocket

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
TOP_N = int(os.getenv("TOP_N", "20"))
CHALLENGER_N = int(os.getenv("CHALLENGER_N", "10"))
RANK_REFRESH = float(os.getenv("RANK_REFRESH_SECONDS", "30"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "3"))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.78"))
CHALLENGER_GAP = float(os.getenv("CHALLENGER_GAP", "0.06"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "10"))
MIN_24H_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
MIN_1M_QV = float(os.getenv("MIN_1M_QUOTE_VOLUME", "50000"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "45"))
MAX_HOLD = float(os.getenv("MAX_HOLD_SECONDS", "180"))
STATE_LEN = int(os.getenv("STATE_LEN", "240"))
STATE_FILE = os.getenv("ULTRA_STATE_FILE", "ultra_state.json")
FEE_BPS = float(os.getenv("EST_FEE_BPS", "4"))
SLIPPAGE_BPS = float(os.getenv("EST_SLIPPAGE_BPS", "3"))

# Known meme bases + a conservative symbol-name fallback for new listings.
MEME_BASES = {x.strip().upper() for x in os.getenv(
    "MEME_BASES",
    "DOGE,SHIB,PEPE,FLOKI,BONK,WIF,BRETT,POPCAT,MEW,MOG,TURBO,NEIRO,ACT,
    PNUT,GOAT,MOODENG,TRUMP,MELANIA,SPX,FWOG,DEGEN,TOSHI,BOME,MYRO,SUNDOG,
    BABYDOGE,DOGS,CATI,WHY,1000SATS,1000BONK,1000PEPE,1000FLOKI,1000SHIB"
).split(",") if x.strip()}
MEME_HINTS = ("DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "MEME", "MOG", "TURBO", "PNUT", "GOAT", "POPCAT", "NEIRO", "BOME", "DOGS", "CAT", "PIG", "TRUMP", "MELANIA")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ultra-plus")
http = requests.Session()
lock = threading.RLock()
state = {}
positions = {}
last_entry = {}
metrics = {"events": 0, "signals": 0, "entries": 0, "exits": 0, "ranking": 0}
ranked_top20 = []
challengers = []
regime = "UNKNOWN"


def public(path, params=None):
    for attempt in range(3):
        try:
            r = http.get(BASE + path, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))


def ema(xs, n):
    if not xs:
        return 0.0
    k = 2.0 / (n + 1.0)
    e = float(xs[0])
    for x in xs[1:]:
        e = float(x) * k + e * (1.0 - k)
    return e


def rsi(xs, n=14):
    if len(xs) <= n:
        return 50.0
    d = [xs[i] - xs[i - 1] for i in range(1, len(xs))][-n:]
    gains = [max(x, 0.0) for x in d]
    losses = [max(-x, 0.0) for x in d]
    ag, al = mean(gains), mean(losses)
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def atr_pct(ks, n=14):
    if len(ks) <= n:
        return 0.0
    trs = []
    prev = float(ks[-n-1][4]) if len(ks) > n else float(ks[0][4])
    for k in ks[-n:]:
        h, l, c = float(k[2]), float(k[3]), float(k[4])
        trs.append(max(h - l, abs(h - prev), abs(l - prev)))
        prev = c
    last = float(ks[-1][4])
    return mean(trs) / last if last else 0.0


def make_state():
    return {
        "bid": 0.0, "ask": 0.0, "bq": 0.0, "aq": 0.0, "last": 0.0,
        "prices": deque(maxlen=STATE_LEN), "flow": deque(maxlen=STATE_LEN),
        "last_event": 0.0, "features": {}, "first_seen": time.time()
    }


def is_meme(symbol):
    base = symbol.upper().replace("USDT", "")
    if base in MEME_BASES or base.startswith("1000") and base[4:] in MEME_BASES:
        return True
    return any(h in base for h in MEME_HINTS)


def meme_universe():
    info = public("/fapi/v1/exchangeInfo")
    allowed = {
        x["symbol"] for x in info["symbols"]
        if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT" and is_meme(x["symbol"])
    }
    tickers = public("/fapi/v1/ticker/24hr")
    out = []
    for t in tickers:
        s = t.get("symbol")
        if s not in allowed:
            continue
        try:
            qv = float(t.get("quoteVolume", 0))
            change = abs(float(t.get("priceChangePercent", 0))) / 100.0
            if qv >= MIN_24H_QV:
                out.append((s.lower(), qv, change))
        except (TypeError, ValueError):
            continue
    return out


def kline_features(symbol):
    frames = {}
    for interval, limit in (("1m", 90), ("5m", 60), ("15m", 40), ("1h", 30)):
        ks = public("/fapi/v1/klines", {"symbol": symbol.upper(), "interval": interval, "limit": limit})
        if len(ks) < 25:
            return None
        closes = [float(k[4]) for k in ks]
        vols = [float(k[5]) for k in ks]
        last = closes[-1]
        frames[interval] = {
            "close": last,
            "atr": atr_pct(ks),
            "rsi": rsi(closes),
            "mom": closes[-1] / closes[-4] - 1.0,
            "mom_fast": closes[-1] / closes[-2] - 1.0,
            "vol_ratio": vols[-1] / max(mean(vols[-21:-1]), 1e-12),
            "ema9": ema(closes, 9), "ema21": ema(closes, 21), "ema50": ema(closes, 50) if len(closes) >= 50 else ema(closes, len(closes)),
            "range_pos": (last - min(float(k[3]) for k in ks[-20:])) / max(max(float(k[2]) for k in ks[-20:]) - min(float(k[3]) for k in ks[-20:]), 1e-12),
        }
    return frames


def market_regime(f):
    m1, m5, m15, h1 = f["1m"], f["5m"], f["15m"], f["1h"]
    atr = m1["atr"]
    alignment = sum([m5["ema9"] > m5["ema21"], m15["ema9"] > m15["ema21"], h1["ema9"] > h1["ema21"]])
    down_alignment = 3 - alignment
    if atr > 0.025:
        return "PANIC"
    if atr > 0.010:
        return "HIGH_VOL"
    if alignment == 3 or down_alignment == 3:
        return "TRENDING"
    if m1["vol_ratio"] >= 2.0 and abs(m1["mom"]) >= 0.003:
        return "BREAKOUT"
    if atr < 0.001:
        return "LOW_LIQUIDITY"
    return "CHOP"


def score_symbol(symbol, qv, change):
    st = state.get(symbol)
    if not st:
        st = state[symbol] = make_state()
    f = kline_features(symbol)
    if not f or st["bid"] <= 0 or st["ask"] <= 0:
        return None
    m1, m5, m15, h1 = f["1m"], f["5m"], f["15m"], f["1h"]
    mid = (st["bid"] + st["ask"]) / 2.0
    spread = (st["ask"] - st["bid"]) / mid * 10000.0
    if spread > MAX_SPREAD_BPS:
        return None
    one_min_qv = m1["close"] * mean([float(x[5]) for x in public("/fapi/v1/klines", {"symbol": symbol.upper(), "interval": "1m", "limit": 5})])
    if one_min_qv < MIN_1M_QV:
        return None
    imb = (st["bq"] - st["aq"]) / max(st["bq"] + st["aq"], 1e-12)
    flow = list(st["flow"])
    flow_acc = 0.0
    if len(flow) >= 30:
        recent = sum(flow[-6:])
        base = mean([abs(x) for x in flow[-30:-6]])
        flow_acc = recent / max(base, 1e-9)
    side = "BUY" if m1["mom"] >= 0 else "SELL"
    trend = 1.0 if ((m5["ema9"] > m5["ema21"] > m5["ema50"]) and side == "BUY") or ((m5["ema9"] < m5["ema21"] < m5["ema50"]) and side == "SELL") else 0.0
    htf = 1.0 if ((m15["ema9"] > m15["ema21"] and h1["ema9"] > h1["ema21"]) == (side == "BUY")) else 0.0
    vol = min(max((m1["vol_ratio"] - 1.0) / 2.0, 0.0), 1.0)
    mom = min(max(abs(m1["mom"]) / 0.004, 0.0), 1.0)
    flow_score = min(max(abs(imb) * 1.5 + (0.2 if (flow_acc > 1 and side == "BUY") or (flow_acc < -1 and side == "SELL") else 0.0), 0.0), 1.0)
    breakout = 1.0 if (m1["range_pos"] > 0.82 and side == "BUY") or (m1["range_pos"] < 0.18 and side == "SELL") else 0.0
    atr_quality = min(max(m1["atr"] / 0.006, 0.0), 1.0)
    liquidity = min(max(math.log10(max(qv, 1.0)) / 9.0, 0.0), 1.0) * max(0.0, 1.0 - spread / MAX_SPREAD_BPS)
    chase_penalty = 0.35 if abs(m1["mom"]) > 0.012 and m1["vol_ratio"] < 2.0 else 0.0
    htf_penalty = 0.30 if htf == 0.0 and abs(m1["mom"]) > 0.003 else 0.0
    regime_penalty = 1.0 if regime in ("PANIC", "LOW_LIQUIDITY") else (0.55 if regime == "CHOP" else 0.0)
    gross = 0.18 * trend + 0.16 * htf + 0.16 * vol + 0.16 * mom + 0.12 * flow_score + 0.08 * breakout + 0.08 * atr_quality + 0.06 * liquidity
    cost = min((FEE_BPS + SLIPPAGE_BPS) / 1000.0, 0.20)
    score = max(0.0, gross - chase_penalty - htf_penalty - regime_penalty * 0.35 - cost)
    return {
        "symbol": symbol, "side": side, "score": score, "spread_bps": spread,
        "qv": qv, "change": change, "atr": m1["atr"], "vol_ratio": m1["vol_ratio"],
        "momentum": m1["mom"], "rsi": m1["rsi"], "flow": flow_score,
        "regime": regime, "timestamp": time.time()
    }


def refresh_ranking():
    global ranked_top20, challengers, regime
    universe = meme_universe()
    candidates = []
    for symbol, qv, change in universe:
        try:
            if symbol not in state:
                state[symbol] = make_state()
            f = kline_features(symbol)
            if f:
                regime = market_regime(f)
            result = score_symbol(symbol, qv, change)
            if result:
                candidates.append(result)
        except Exception as exc:
            log.debug("rank %s failed: %s", symbol, exc)
    candidates.sort(key=lambda x: x["score"], reverse=True)
    previous = {x["symbol"]: x for x in ranked_top20}
    selected = candidates[:TOP_N]
    # Hysteresis: incumbents survive unless a challenger beats them materially.
    selected_symbols = {x["symbol"] for x in selected}
    for old in ranked_top20:
        if old["symbol"] not in selected_symbols:
            better = next((x for x in candidates if x["symbol"] == old["symbol"]), None)
            cutoff = selected[-1]["score"] if selected else 0.0
            if better and better["score"] >= cutoff - CHALLENGER_GAP:
                selected.append(better)
    selected.sort(key=lambda x: x["score"], reverse=True)
    ranked_top20 = selected[:TOP_N]
    top_symbols = {x["symbol"] for x in ranked_top20}
    challengers = [x for x in candidates if x["symbol"] not in top_symbols][:CHALLENGER_N]
    metrics["ranking"] += 1
    log.info("DYNAMIC TOP-20 | regime=%s | %s", regime, " ".join(f'{x["symbol"]}:{x["score"]:.2f}' for x in ranked_top20[:10]))


def confirm_entry(c):
    if regime in ("PANIC", "LOW_LIQUIDITY"):
        return False, "bad regime"
    if c["score"] < ENTRY_SCORE:
        return False, "score"
    if c["vol_ratio"] < 1.15 or abs(c["momentum"]) < 0.0006:
        return False, "weak acceleration"
    if c["flow"] < 0.15:
        return False, "weak order flow"
    if c["spread_bps"] > MAX_SPREAD_BPS:
        return False, "spread"
    # Prediction is only a watch state; confirmation requires fresh momentum.
    st = state[c["symbol"]]
    if time.time() - st["last_event"] > 5:
        return False, "stale tape"
    return True, "confirmed"


def enter(c):
    s = c["symbol"]
    now = time.time()
    with lock:
        if s in positions or len(positions) >= MAX_POSITIONS:
            return
        if now - last_entry.get(s, 0.0) < COOLDOWN:
            return
        ok, reason = confirm_entry(c)
        if not ok:
            return
        positions[s] = {**c, "entry": state[s]["ask"] if c["side"] == "BUY" else state[s]["bid"], "opened": now, "peak": 0.0, "mfe": 0.0, "mae": 0.0}
        last_entry[s] = now
        metrics["entries"] += 1
    log.warning("ENTRY CONFIRMED %s %s score=%.3f regime=%s vol=%.2fx mom=%.3f%% [DRY RUN]", c["side"], s.upper(), c["score"], regime, c["vol_ratio"], c["momentum"] * 100)


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
            p["mfe"] = max(p["mfe"], ret)
            p["mae"] = min(p["mae"], ret)
            age = now - p["opened"]
            reason = None
            if ret <= -max(p["atr"] * 1.15, 0.0025):
                reason = "ADAPTIVE_STOP"
            elif ret > 0.0015 and p["mfe"] - ret > 0.0010:
                reason = "MOMENTUM_DECAY"
            elif regime in ("PANIC", "LOW_LIQUIDITY"):
                reason = "REGIME_RISK"
            elif age > MAX_HOLD:
                reason = "TIME_DECAY"
            else:
                sig = next((x for x in ranked_top20 if x["symbol"] == s), None)
                if sig and sig["side"] != p["side"] and sig["score"] >= ENTRY_SCORE:
                    reason = "REVERSAL"
                elif sig and sig["score"] < ENTRY_SCORE * 0.78:
                    reason = "THESIS_DECAY"
            if reason:
                positions.pop(s, None)
                metrics["exits"] += 1
                exits.append((s, p, px, ret, reason, age))
    for s, p, px, ret, reason, age in exits:
        log.warning("EXIT %s %s ret=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%%", reason, s.upper(), ret * 100, age, p["mfe"] * 100, p["mae"] * 100)


def on_message(_, raw):
    try:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        event = d.get("e")
        s = d.get("s", "").lower()
        if not s or s not in state:
            return
        st = state[s]
        metrics["events"] += 1
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
            st["last_event"] = time.time()
    except Exception:
        log.exception("market-data message error")


def stream_url(symbols):
    streams = []
    for s in symbols:
        streams += [f"{s}@bookTicker", f"{s}@aggTrade"]
    return WS_BASE + "?streams=" + "/".join(streams)


def ws_loop(symbols):
    while True:
        try:
            ws = websocket.WebSocketApp(stream_url(symbols), on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            log.exception("websocket loop failed")
        time.sleep(2)


def ranking_loop():
    global regime
    while True:
        try:
            refresh_ranking()
            # Rebuild the live tape around Top-20 + challengers; ranking remains authoritative.
            live = [x["symbol"] for x in ranked_top20 + challengers]
            for s in live:
                state.setdefault(s, make_state())
            if live:
                threading.Thread(target=ws_loop, args=(live,), daemon=True).start()
            for c in ranked_top20:
                enter(c)
        except Exception:
            log.exception("ranking cycle failed")
        time.sleep(RANK_REFRESH)


def save_state():
    payload = {"top20": ranked_top20, "challengers": challengers, "regime": regime, "metrics": metrics, "positions": positions}
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        log.exception("state save failed")


def main():
    if not DRY_RUN:
        raise RuntimeError("Ultra+ is locked to DRY_RUN=true until paper results are validated.")
    log.warning("ULTRA+ STARTED | Dynamic Top-%d | meme universe | 1m entry | multi-timeframe confirmation | DRY RUN", TOP_N)
    threading.Thread(target=ranking_loop, name="ranking-engine", daemon=True).start()
    while True:
        manage_positions()
        save_state()
        log.info("REGIME=%s TOP20=%d CHALLENGERS=%d POS=%d events=%d entries=%d exits=%d", regime, len(ranked_top20), len(challengers), len(positions), metrics["events"], metrics["entries"], metrics["exits"])
        time.sleep(1)


if __name__ == "__main__":
    main()
