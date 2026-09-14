"""Fast dynamic all-USDT perpetual universe using live Binance market data."""
import math
import time
import requests

REFRESH = 30.0
_cache = {"at": 0.0, "symbols": []}
_FALLBACK_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]


def _api_with_failover(engine, path, params=None):
    """Use the engine API, rotating Binance Futures REST hosts on 418/5xx/network errors."""
    current = getattr(engine, "BASE", _FALLBACK_BASES[0])
    ordered = [current] + [x for x in _FALLBACK_BASES if x != current]
    last = None
    for base in ordered:
        try:
            engine.BASE = base
            return engine.api(path, params)
        except requests.RequestException as exc:
            last = exc
            engine.log.warning("BINANCE REST FAILOVER | base=%s | path=%s | error=%s", base, path, exc)
            continue
    if last:
        raise last
    raise RuntimeError("No Binance REST endpoint available")


def discover(engine):
    """Scan every active USDT perpetual cheaply, then let rank() do 3m/5m scoring.

    IMPORTANT: do not seed candles for every symbol here. That caused hundreds of
    REST calls during discovery and could block the 30-second ranking loop for many
    minutes. We use the live 24h ticker to reduce the full universe to a liquid,
    volatile discovery pool; rank() then warms only that pool and selects execution N.
    """
    info = _api_with_failover(engine, "/fapi/v1/exchangeInfo")
    ticks_raw = _api_with_failover(engine, "/fapi/v1/ticker/24hr")
    ticks = {x.get("symbol"): x for x in ticks_raw}
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

        liquidity = min(math.log10(max(q24, 1.0)) / 10.0, 1.0)
        volatility = min(ch24 / 0.08, 1.0)
        freshness = 0.25 if age_days <= 7 else (0.10 if age_days <= 30 else 0.0)
        dynamic = 0.65 * volatility + 0.30 * liquidity + freshness
        candidates.append((s.lower(), dynamic, age_days, q24))

    candidates.sort(key=lambda x: (x[1], x[3]), reverse=True)
    pool_n = max(int(getattr(engine, "DISCOVERY_N", 50)), 10)
    selected = candidates[:pool_n]
    _cache.update({"at": time.time(), "symbols": [x[0] for x in selected]})
    engine.log.info(
        "ALL-USDT LIVE VOLUME | scanned=%d | liquid=%d | discovery_pool=%d | %s",
        len(allowed), len(candidates), len(selected),
        " ".join(x[0].upper() for x in selected[:10]),
    )
    return selected
