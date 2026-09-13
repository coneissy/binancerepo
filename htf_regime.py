"""Higher-timeframe market regime: 1h primary, 15m confirmation.

Used as a directional gate for the 5m setup / 1m execution engine.
Live execution remains disabled by the engine.
"""
import time

CACHE_SECONDS = 60
_cache = {"at": 0.0, "regime": "UNKNOWN", "primary": "UNKNOWN", "confirm": "UNKNOWN"}


def _ema(values, n):
    if not values:
        return 0.0
    k = 2.0 / (n + 1.0)
    e = float(values[0])
    for x in values[1:]:
        e = float(x) * k + e * (1.0 - k)
    return e


def _frame(engine, interval, limit=80):
    k = engine.api("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": interval, "limit": limit})
    closed = k[:-1]
    if len(closed) < 30:
        return None
    c = [float(x[4]) for x in closed]
    r3 = c[-1] / c[-4] - 1.0
    r6 = c[-1] / c[-7] - 1.0
    e9 = _ema(c, 9)
    e21 = _ema(c, 21)
    e55 = _ema(c, 55)
    return {"r3": r3, "r6": r6, "e9": e9, "e21": e21, "e55": e55, "up": e9 > e21 > e55, "down": e9 < e21 < e55}


def market_regime(engine):
    now = time.time()
    if now - _cache["at"] < CACHE_SECONDS:
        return _cache["regime"]
    try:
        h1 = _frame(engine, "1h", 100)
        m15 = _frame(engine, "15m", 100)
        if not h1 or not m15:
            return "UNKNOWN"

        # 1h is the primary regime. 15m must confirm direction before trading it.
        if abs(h1["r3"]) >= 0.035 or abs(h1["r6"]) >= 0.055:
            regime = "PANIC"
        elif h1["up"] and m15["up"] and h1["r6"] > 0 and m15["r3"] > 0:
            regime = "TREND_UP"
        elif h1["down"] and m15["down"] and h1["r6"] < 0 and m15["r3"] < 0:
            regime = "TREND_DOWN"
        else:
            regime = "CHOP"

        _cache.update({"at": now, "regime": regime,
                       "primary": "UP" if h1["up"] else "DOWN" if h1["down"] else "MIXED",
                       "confirm": "UP" if m15["up"] else "DOWN" if m15["down"] else "MIXED"})
        engine.log.info("HTF REGIME | 1H=%s r6=%.2f%% | 15M=%s r3=%.2f%% | regime=%s",
                        _cache["primary"], h1["r6"] * 100, _cache["confirm"], m15["r3"] * 100, regime)
        return regime
    except Exception as exc:
        engine.log.warning("HTF REGIME FAILED: %s", exc)
        return "UNKNOWN"


def install(engine):
    if getattr(engine, "_htf_installed", False):
        return
    original_score = engine.score

    def htf_score(symbol, radar, age, q, reject):
        c = original_score(symbol, radar, age, q, reject)
        if not c:
            return None
        r = _cache.get("regime", "UNKNOWN")
        # Higher timeframe is directional: 1h + 15m decide the allowed side;
        # 5m remains setup confirmation and 1m remains execution timing.
        if r == "TREND_UP" and c["side"] != "BUY":
            c["eligible"] = False
            reject["htf"] = reject.get("htf", 0) + 1
        elif r == "TREND_DOWN" and c["side"] != "SELL":
            c["eligible"] = False
            reject["htf"] = reject.get("htf", 0) + 1
        return c

    engine.score = htf_score
    engine.market_regime = lambda: market_regime(engine)
    engine._htf_installed = True
    engine.log.info("HTF GATE ON | primary=1H | confirmation=15M | setup=5M | execution=1M")
