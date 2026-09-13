"""Fast dynamic all-USDT perpetual universe using live Binance market data."""
import math
import time

REFRESH = 30.0
_cache = {"at": 0.0, "symbols": []}


def discover(engine):
    """Scan every active USDT perpetual cheaply, then let rank() do 1m feature scoring.

    IMPORTANT: do not seed candles for every symbol here.  That caused hundreds of
    REST calls during discovery and could block the 30-second ranking loop for many
    minutes.  We use the live 24h ticker to reduce the full universe to a liquid,
    volatile discovery pool; rank() then warms only that pool and selects execution N.
    """
    info = engine.api("/fapi/v1/exchangeInfo")
    ticks = {x.get("symbol"): x for x in engine.api("/fapi/v1/ticker/24hr")}
    now_ms = time.time() * 1000.0
    allowed = {
        m.get("symbol")
        for m in info.get("symbols", [])
        if m.get("status") == "TRADING"
        and m.get("contractType") == "PERPETUAL"
        and m.get("quoteAsset") == "USDT"
    }

    candidates = []
    for m in info.get("symbols", []):
        s = m.get("symbol", "")
        if s not in allowed:
            continue
        t = ticks.get(s, {})
        try:
            q24 = float(t.get("quoteVolume", 0.0))
            ch24 = abs(float(t.get("priceChangePercent", 0.0))) / 100.0
            onboard_ms = float(m.get("onboardDate", now_ms))
            age_days = max(0.0, (now_ms - onboard_ms) / 86400000.0)
        except (TypeError, ValueError):
            continue
        if q24 < engine.MIN24:
            continue

        # Fast universe prior. No per-symbol klines/order-book calls here.
        liquidity = min(math.log10(max(q24, 1.0)) / 10.0, 1.0)
        volatility = min(ch24 / 0.08, 1.0)
        freshness = 0.25 if age_days <= 7 else (0.10 if age_days <= 30 else 0.0)
        dynamic = 0.65 * volatility + 0.30 * liquidity + freshness
        candidates.append((s.lower(), dynamic, age_days, q24))

    candidates.sort(key=lambda x: (x[1], x[3]), reverse=True)
    # Return the discovery pool; rank() performs the expensive 1m/5m scoring only
    # on this bounded set and then keeps EXECUTION_N (currently 10).
    pool_n = max(int(getattr(engine, "DISCOVERY_N", 50)), 10)
    selected = candidates[:pool_n]
    _cache.update({"at": time.time(), "symbols": [x[0] for x in selected]})
    engine.log.info(
        "ALL-USDT LIVE VOLUME | scanned=%d | liquid=%d | discovery_pool=%d | %s",
        len(allowed), len(candidates), len(selected),
        " ".join(x[0].upper() for x in selected[:10]),
    )
    return selected
