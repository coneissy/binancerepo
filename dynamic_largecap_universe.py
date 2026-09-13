"""Dynamic large-cap/high-volatility universe for the HTF + 1m engine."""
import math
import os
import time

import requests

CG_URL = "https://api.coingecko.com/api/v3/coins/markets"
POOL = max(15, int(os.getenv("LARGECAP_POOL", "30")))
TOP_N = max(5, int(os.getenv("LARGECAP_TOP_N", "10")))
REFRESH = max(300, int(os.getenv("LARGECAP_REFRESH_SECONDS", "900")))

FALLBACK_BASES = (
    "BTC", "ETH", "BNB", "XRP", "SOL", "TRX", "DOGE", "ADA", "LINK", "AVAX",
    "SUI", "LTC", "BCH", "DOT", "UNI", "NEAR", "APT", "FIL", "ATOM", "ETC",
)
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
            headers={"User-Agent": "binance-1m-engine/1.0"},
        )
        r.raise_for_status()
        rows = r.json()
        syms = list(dict.fromkeys(str(x.get("symbol", "")).upper() for x in rows if x.get("symbol")))
        if syms:
            _cache.update({"at": now, "symbols": syms})
            return syms
    except Exception:
        pass
    return _cache["symbols"] or list(FALLBACK_BASES)


def discover(engine):
    """Select TOP_N liquid large-cap perpetuals by live volatility/momentum."""
    info = engine.api("/fapi/v1/exchangeInfo")
    ticks = {x["symbol"]: x for x in engine.api("/fapi/v1/ticker/24hr")}
    allowed = {
        m.get("symbol") for m in info.get("symbols", [])
        if m.get("status") == "TRADING" and m.get("contractType") == "PERPETUAL"
        and m.get("quoteAsset") == "USDT"
    }
    bases = _market_cap_symbols()
    candidates = []
    for rank, base in enumerate(bases, 1):
        s = base + "USDT"
        if s not in allowed:
            continue
        t = ticks.get(s, {})
        try:
            q = float(t.get("quoteVolume", 0.0))
            ch = abs(float(t.get("priceChangePercent", 0.0))) / 100.0
        except Exception:
            continue
        if q < engine.MIN24:
            continue
        sl = s.lower()
        vol_score = min(ch / 0.08, 1.0)
        impulse = 0.0
        momentum = 0.0
        try:
            if sl not in engine.hist or len(engine.hist[sl]) < engine.WARMUP:
                engine.seed(sl)
            f = engine.features(sl)
            h = engine.higher(sl)
            if f:
                vol_score = max(vol_score, min(max(f.get("atr", 0.0) / 0.0025, 0.0), 1.0))
                impulse = min(max((f.get("rv", 1.0) - 1.0) / 2.0, 0.0), 1.0)
            if h:
                momentum = min(abs(h.get("mom", 0.0)) / 0.02, 1.0)
        except Exception:
            pass
        liquidity = min(math.log10(max(q, 1.0)) / 10.0, 1.0)
        cap_quality = 1.0 - (rank - 1) / max(len(bases) - 1, 1)
        dynamic = 0.38 * vol_score + 0.22 * impulse + 0.20 * momentum + 0.15 * liquidity + 0.05 * cap_quality
        candidates.append((sl, dynamic, ch, q))
    candidates.sort(key=lambda x: (x[1], x[3]), reverse=True)
    selected = candidates[:TOP_N]
    engine.log.info(
        "DYNAMIC LARGE-CAP | pool=%d | selected=%d | %s",
        len(candidates), len(selected), " ".join(x[0].upper() for x in selected),
    )
    return selected
