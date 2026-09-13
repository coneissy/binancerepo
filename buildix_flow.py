"""Optional Buildix orderflow + smart-money confirmation for Cryptoalpha.

Buildix is used as an external intelligence layer, not as an execution venue.
The Binance engine remains the execution/data authority and DRY_RUN remains
mandatory. Missing/stale Buildix data is neutral rather than a hard failure.
"""
import os
import threading
import time
import requests

BASE = os.getenv("BUILDIX_BASE_URL", "https://www.buildix.trade/api/v1").rstrip("/")
API_KEY = os.getenv("BUILDIX_API_KEY", "").strip()
REFRESH = float(os.getenv("BUILDIX_REFRESH_SECONDS", "60"))
MAX_AGE = float(os.getenv("BUILDIX_MAX_AGE_SECONDS", "180"))
SIGNAL_WEIGHT = float(os.getenv("BUILDIX_SIGNAL_WEIGHT", "0.12"))
SMART_WEIGHT = float(os.getenv("BUILDIX_SMART_WEIGHT", "0.10"))
MIN_SMART_COVERAGE = float(os.getenv("BUILDIX_MIN_SMART_COVERAGE", "5"))
MIN_SIGNAL_SCORE = float(os.getenv("BUILDIX_MIN_SIGNAL_SCORE", "20"))

ENGINE = None
LOCK = threading.RLock()
CACHE = {"signals": {}, "smart": {}, "timestamp": 0.0, "errors": 0, "updates": 0}


def _headers():
    h = {"Accept": "application/json"}
    if API_KEY:
        # Buildix documentation has used both forms; Bearer is the current API style.
        h["Authorization"] = f"Bearer {API_KEY}"
        h["X-API-Key"] = API_KEY
    return h


def _get(path, params=None):
    r = requests.get(f"{BASE}/{path.lstrip('/')}", headers=_headers(), params=params, timeout=8)
    r.raise_for_status()
    return r.json()


def _unwrap(data):
    if isinstance(data, dict):
        for k in ("data", "signals", "positions", "pairs", "results"):
            if k in data and isinstance(data[k], (dict, list)):
                return data[k]
    return data


def _symbol_key(symbol):
    s = str(symbol or "").upper().replace("-PERP", "").replace("/USDT", "")
    for suffix in ("USDT", "USD", "PERP"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    return s


def _num(x, default=0.0):
    try: return float(x)
    except (TypeError, ValueError): return default


def _direction_score(item):
    if not isinstance(item, dict): return 0.0
    direction = str(item.get("direction", item.get("signalDirection", ""))).upper()
    raw = item.get("score", item.get("signalScore", 0))
    score = _num(raw)
    if direction in ("BUY", "LONG") and score < 0: score = -score
    if direction in ("SELL", "SHORT") and score > 0: score = -score
    if direction in ("BUY", "LONG") and score == 0: score = 50.0
    if direction in ("SELL", "SHORT") and score == 0: score = -50.0
    return max(-100.0, min(100.0, score))


def _refresh():
    try:
        raw_signals = _unwrap(_get("signals"))
        sig = {}
        if isinstance(raw_signals, dict):
            for symbol, value in raw_signals.items():
                if isinstance(value, dict):
                    v = dict(value); v.setdefault("symbol", symbol); sig[_symbol_key(symbol)] = v
        elif isinstance(raw_signals, list):
            for value in raw_signals:
                if isinstance(value, dict):
                    k = _symbol_key(value.get("symbol"));
                    if k: sig[k] = value

        smart_raw = _unwrap(_get("smart-money"))
        smart = {}
        # Aggregate individual whale positions when the endpoint returns a list.
        positions = smart_raw if isinstance(smart_raw, list) else []
        if isinstance(smart_raw, dict):
            positions = smart_raw.get("positions", smart_raw.get("data", []))
        for p in positions:
            if not isinstance(p, dict): continue
            k = _symbol_key(p.get("symbol"))
            side = str(p.get("side", "")).lower()
            notional = abs(_num(p.get("notional", p.get("notionalUsd", p.get("sizeUsd", 0)))))
            if not k or notional <= 0 or side not in ("long", "short"): continue
            z = smart.setdefault(k, {"long_usd": 0.0, "short_usd": 0.0, "positions": 0})
            z["long_usd" if side == "long" else "short_usd"] += notional
            z["positions"] += 1

        # Some Buildix versions return pre-aggregated smart-money positioning.
        if isinstance(smart_raw, dict):
            rows = smart_raw.get("data") if isinstance(smart_raw.get("data"), list) else []
            for p in rows:
                if not isinstance(p, dict): continue
                k = _symbol_key(p.get("symbol"));
                if not k: continue
                if "long_oi_usd" in p or "short_oi_usd" in p:
                    smart[k] = {
                        "long_usd": _num(p.get("long_oi_usd")),
                        "short_usd": _num(p.get("short_oi_usd")),
                        "positions": int(_num(p.get("wallets_sampled", 0))),
                        "coverage_pct": _num(p.get("coverage_pct", 0)),
                        "ratio": _num(p.get("long_short_ratio", 0)),
                    }

        with LOCK:
            CACHE.update({"signals": sig, "smart": smart, "timestamp": time.time(), "updates": CACHE["updates"] + 1})
    except Exception:
        with LOCK: CACHE["errors"] += 1


def _worker():
    while True:
        _refresh()
        time.sleep(max(15.0, REFRESH))


def _get_smart(symbol):
    k = _symbol_key(symbol)
    with LOCK:
        x = dict(CACHE["smart"].get(k, {}))
    long_usd = _num(x.get("long_usd")); short_usd = _num(x.get("short_usd"))
    total = long_usd + short_usd
    if total <= 0: return None
    bias = (long_usd - short_usd) / total
    coverage = _num(x.get("coverage_pct", 100 if x.get("positions") else 0))
    if coverage and coverage < MIN_SMART_COVERAGE: return None
    return {"bias": bias, "ratio": long_usd / max(short_usd, 1e-9), "coverage_pct": coverage, "positions": int(x.get("positions", 0))}


def score(s, radar, age, q, reject):
    base = ENGINE._buildix_original_score(s, radar, age, q, reject)
    if not base: return None
    k = _symbol_key(s)
    with LOCK:
        fresh = (time.time() - CACHE["timestamp"]) <= MAX_AGE
        sig = dict(CACHE["signals"].get(k, {})) if fresh else {}
    smart = _get_smart(s) if fresh else None
    side = 1 if base["side"] == "BUY" else -1

    buildix_score = _direction_score(sig)
    # Convert Buildix's -100..100 directional score to 0..1 aligned confidence.
    aligned_bx = max(-1.0, min(1.0, (buildix_score / 100.0) * side))
    bx_conf = abs(buildix_score) / 100.0
    smart_bias = smart["bias"] * side if smart else 0.0
    smart_conf = abs(smart_bias)

    # Only meaningful Buildix observations influence the score. Missing data is neutral.
    final = float(base["score"])
    if bx_conf >= MIN_SIGNAL_SCORE / 100.0:
        final = max(0.0, min(1.0, final + SIGNAL_WEIGHT * aligned_bx * bx_conf))
    if smart and smart_conf > 0:
        final = max(0.0, min(1.0, final + SMART_WEIGHT * smart_bias * smart_conf))

    # Strong disagreement is a risk penalty, not an automatic trade reversal.
    contradiction = (aligned_bx <= -0.55 and bx_conf >= 0.55) or (smart_bias <= -0.55 and smart_conf >= 0.55)
    if contradiction:
        final = max(0.0, final - 0.08)

    base["score"] = final
    base["buildix"] = {
        "available": bool(sig or smart),
        "signal_score": round(buildix_score, 2),
        "signal_direction": sig.get("direction", sig.get("signalDirection", "NEUTRAL")),
        "vpin": sig.get("vpin"), "cvd": sig.get("cvd", sig.get("cvdDirection")),
        "obi": sig.get("obi", sig.get("orderBookImbalance")),
        "regime": sig.get("regime"),
        "smart_bias": round(smart_bias, 4) if smart else None,
        "smart_ratio": round(smart["ratio"], 4) if smart else None,
        "smart_coverage_pct": round(smart["coverage_pct"], 2) if smart else None,
        "smart_positions": smart["positions"] if smart else 0,
        "contradiction": contradiction,
    }
    base["eligible"] = bool(base.get("eligible") and not contradiction and final >= ENGINE.ENTRY)
    return base


def install(engine):
    global ENGINE
    ENGINE = engine
    engine._buildix_original_score = engine.score
    engine.score = score
    threading.Thread(target=_worker, name="buildix-flow", daemon=True).start()
    return engine


def stats():
    with LOCK:
        age = time.time() - CACHE["timestamp"] if CACHE["timestamp"] else None
        return {
            "enabled": True,
            "api_key_configured": bool(API_KEY),
            "fresh": bool(age is not None and age <= MAX_AGE),
            "age_seconds": round(age, 1) if age is not None else None,
            "symbols_with_signals": len(CACHE["signals"]),
            "symbols_with_smart_money": len(CACHE["smart"]),
            "updates": CACHE["updates"],
            "errors": CACHE["errors"],
            "signal_weight": SIGNAL_WEIGHT,
            "smart_weight": SMART_WEIGHT,
        }
