"""Dynamic large-cap/high-volatility universe for the HTF + 1m engine."""
import math
import time

import requests

CG_URL = "https://api.coingecko.com/api/v3/coins/markets"
POOL = max(15, int(__import__("os").getenv("LARGECAP_POOL", "30")))
TOP_N = max(5, int(__import__("os").getenv("LARGECAP_TOP_N", "10")))
REFRESH = max(300, int(__import__("os").getenv("LARGECAP_REFRESH_SECONDS", "900")))

_cache = {"at": 0.0, "symbols": []}


def _market_cap_symbols():
    now = time.time()
    if now - _cache["at"] < REFRESH and _cache["symbols"]:
        return _cache["symbols"]
    try:
        r = requests.get(
            CG_URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": POOL,
                "page": 1,
                "sparkline": "false",
            },
            timeout=8,
        )
        r.raise_for_status()
        rows = r.json()
        syms = [str(x.get("symbol", "")).upper() for x in rows if x.get("symbol")]
        _cache.update({"at": now, "symbols": syms})
        return syms
    except Exception:
        return _cache["symbols"]


def discover(engine):
    """Select 10 liquid large caps with the strongest live volatility/momentum."""
    info = engine.api("/fapi/v1/exchangeInfo")
    ticks = {x["symbol"]: x for x in engine.api("/fapi/v1/ticker/24hr")}
    allowed = {
        m.get("symbol"): m
        for m in info.get("symbols", [])
        if m.get("status") == "TRADING"
        and m.get("contractType") == "PERPETUAL"
        and m.get("quoteAsset") == "USDT"
    }
    cg_symbols = _market_cap_symbols()
    candidates = []
    for rank, base in enumerate(cg_symbols, 1):
        s = base + "USDT"
        if s not in allowed:
            continue
        t = ticks.get(s, {})
        try:
            q = float(t.get("quoteVolume", 0.0))
            ch = float(t.get("priceChangePercent", 0.0)) / 100.0
        except Exception:
            continue
        if q < engine.MIN24:
            continue
        sl = s.lower()
        try:
            if sl not in engine.hist or len(engine.hist[sl]) < engine.WARMUP:
                engine.seed(sl)
            f = engine.features(sl)
            h = engine.higher(sl)
            if not f or not h:
                continue
            vol = min(max(f["atr"] / 0.0025, 0.0), 1.5)
            impulse = min(max((f["rv"] - 1.0) / 2.0, 0.0), 1.5)
            momentum = min(abs(h["mom"]) / 0.02, 1.0)
            liquidity = min(math.log10(max(q, 1.0)) / 10.0, 1.0)
            cap_quality = 1.0 - (rank - 1) / max(len(cg_symbols) - 1, 1)
            dynamic = 0.42 * min(vol, 1.0) + 0.25 * min(impulse, 1.0) + 0.18 * momentum + 0.10 * liquidity + 0.05 * cap_quality
            candidates.append((sl, dynamic, 0.0, q))
        except Exception:
            continue
    candidates.sort(key=lambda x: x[1], reverse=True)
    selected = candidates[:TOP_N]
    engine.log.info(
        "DYNAMIC LARGE-CAP | pool=%d | selected=%d | %s",
        len(candidates), len(selected), " ".join(x[0].upper() for x in selected),
    )
    return selected
