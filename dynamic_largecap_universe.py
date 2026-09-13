"""Dynamic all-USDT perpetual universe using live Binance market data."""
import math
import time

REFRESH = 30.0
TOP_N = 10
_cache = {"at": 0.0, "symbols": []}


def discover(engine):
    """Scan every active Binance USDT perpetual, then rank the best 1m setups."""
    info = engine.api("/fapi/v1/exchangeInfo")
    ticks = {x["symbol"]: x for x in engine.api("/fapi/v1/ticker/24hr")}
    allowed = {
        m.get("symbol") for m in info.get("symbols", [])
        if m.get("status") == "TRADING"
        and m.get("contractType") == "PERPETUAL"
        and m.get("quoteAsset") == "USDT"
    }
    candidates = []
    for s in allowed:
        t = ticks.get(s, {})
        try:
            q24 = float(t.get("quoteVolume", 0.0))
            ch24 = abs(float(t.get("priceChangePercent", 0.0))) / 100.0
        except (TypeError, ValueError):
            continue
        if q24 < engine.MIN24:
            continue
        sl = s.lower()
        vol_score = min(ch24 / 0.08, 1.0)
        impulse = 0.0
        momentum = 0.0
        q1m = 0.0
        try:
            if sl not in engine.hist or len(engine.hist[sl]) < engine.WARMUP:
                engine.seed(sl, "1m", 80)
            f = engine.features(sl)
            h = engine.higher(sl)
            if f:
                vol_score = max(vol_score, min(max(f.get("atr", 0.0) / 0.0025, 0.0), 1.0))
                impulse = min(max((f.get("rv", 1.0) - 1.0) / 2.0, 0.0), 1.0)
                q1m = float(f.get("q", 0.0))
            if h:
                momentum = min(abs(h.get("mom", 0.0)) / 0.02, 1.0)
        except Exception:
            pass
        liquidity = min(math.log10(max(q24, 1.0)) / 10.0, 1.0)
        micro_volume = min(q1m / max(engine.MIN1, 1.0), 3.0) / 3.0
        dynamic = (
            0.30 * vol_score
            + 0.20 * impulse
            + 0.20 * momentum
            + 0.20 * liquidity
            + 0.10 * micro_volume
        )
        candidates.append((sl, dynamic, q24, q1m, ch24))

    candidates.sort(key=lambda x: (x[1], x[2]), reverse=True)
    selected = candidates[:TOP_N]
    _cache.update({"at": time.time(), "symbols": [x[0] for x in selected]})
    engine.log.info(
        "ALL-USDT LIVE VOLUME | scanned=%d | liquid=%d | selected=%d | %s",
        len(allowed), len(candidates), len(selected),
        " ".join(x[0].upper() for x in selected),
    )
    return selected
