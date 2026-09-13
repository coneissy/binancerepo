"""Adaptive upgrade layer for the ALPHA 1M paper engine.

Adds a small strategy ensemble, regime-aware adaptive weights, trailing exits,
and complete paper-trading performance accounting without enabling live execution.
"""
import os
from collections import defaultdict

ENGINE = None
_ORIGINAL_SCORE = None
_ORIGINAL_MANAGE = None

STRATEGIES = ("MOMENTUM", "BREAKOUT", "FLOW", "REVERSAL")
PERF = {r: {s: {"trades": 0, "pnl": 0.0, "wins": 0} for s in STRATEGIES} for r in ("TREND_UP", "TREND_DOWN", "CHOP", "PANIC", "UNKNOWN")}

TRAIL_ARM = float(os.getenv("TRAIL_ARM_PCT", "0.004"))
TRAIL_GAP = float(os.getenv("TRAIL_GAP_PCT", "0.0035"))
TRAIL_ATR_MULT = float(os.getenv("TRAIL_ATR_MULT", "1.15"))
MIN_ADAPT_TRADES = int(os.getenv("MIN_ADAPT_TRADES", "3"))


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def _weights(regime):
    base = {"MOMENTUM": 1.0, "BREAKOUT": 1.0, "FLOW": 1.0, "REVERSAL": 1.0}
    p = PERF.get(regime, PERF["UNKNOWN"])
    for s in STRATEGIES:
        if p[s]["trades"] >= MIN_ADAPT_TRADES:
            avg = p[s]["pnl"] / p[s]["trades"]
            wr = p[s]["wins"] / p[s]["trades"]
            base[s] = _clamp(1.0 + avg * 40.0 + (wr - .5) * .8, .35, 1.8)
    return base


def adaptive_score(s, radar, age, q, reject):
    base = _ORIGINAL_SCORE(s, radar, age, q, reject)
    if not base:
        return None
    f = ENGINE.features(s)
    h = ENGINE.higher(s)
    if not f or not h:
        return base
    side = 1 if base["side"] == "BUY" else -1
    sf = f["flow"] * side
    sb = f["book"] * side
    rv = f["rv"]
    z = f["z"]
    ret = f["ret"]
    ret5 = f["ret5"]
    accel = f["accel"]

    momentum = _clamp(.35 * _clamp(abs(ret5) / .008) + .35 * _clamp(abs(accel) / .0025) + .30 * _clamp(z / 2.5))
    breakout = _clamp(.45 * (1.0 if f["break"] else 0.0) + .30 * _clamp((rv - 1) / 1.5) + .25 * _clamp(abs(ret) / .004))
    flow = _clamp(.55 * ((sf + 1) / 2) + .25 * ((sb + 1) / 2) + .20 * _clamp((rv - 1) / 1.5))
    reversal = _clamp(.45 * _clamp(1.0 - abs(ret5) / .012) + .30 * _clamp((-sf + 1) / 2) + .25 * _clamp(1.0 - abs(h["mom"]) / .015))
    raw = {"MOMENTUM": momentum, "BREAKOUT": breakout, "FLOW": flow, "REVERSAL": reversal}
    w = _weights(ENGINE.regime)
    weighted = {k: raw[k] * w[k] for k in STRATEGIES}
    chosen = max(weighted, key=weighted.get)
    total_w = sum(w.values()) or 1.0
    ensemble = sum(weighted.values()) / total_w

    # Keep the validated base engine as the hard eligibility anchor. The
    # ensemble can improve ranking, but cannot create a trade by itself.
    final_score = _clamp(.68 * base["score"] + .32 * ensemble)
    threshold = ENGINE.ENTRY
    final_eligible = bool(base["eligible"] and final_score >= threshold)
    base.update({
        "score": final_score,
        "ensemble_score": ensemble,
        "strategy": chosen,
        "strategy_scores": raw,
        "strategy_weights": w,
        "eligible": final_eligible,
        "trail_arm": max(TRAIL_ARM, f["atr"] * TRAIL_ATR_MULT),
        "trail_gap": TRAIL_GAP,
    })
    return base


def _record(strategy, regime, net):
    strategy = strategy if strategy in STRATEGIES else "MOMENTUM"
    p = PERF.setdefault(regime, PERF["UNKNOWN"]).setdefault(strategy, {"trades": 0, "pnl": 0.0, "wins": 0})
    p["trades"] += 1
    p["pnl"] += net
    if net >= 0:
        p["wins"] += 1


def adaptive_manage():
    """Paper position manager with hard stop, target, time stop and trailing."""
    now = __import__("time").time()
    for s, p in list(ENGINE.positions.items()):
        z = ENGINE.st(s)
        px = z["bid"] if p["side"] == "BUY" else z["ask"]
        if px <= 0:
            continue
        direction = 1 if p["side"] == "BUY" else -1
        ret = (px / p["entry"] - 1) * direction
        stop = max(p["atr"] * 1.8, .0025)
        target = stop * 2.2
        p.setdefault("peak_ret", ret)
        p["peak_ret"] = max(p["peak_ret"], ret)
        trail_arm = float(p.get("trail_arm", max(TRAIL_ARM, p["atr"] * TRAIL_ATR_MULT)))
        trail_gap = float(p.get("trail_gap", TRAIL_GAP))
        trail_active = p["peak_ret"] >= trail_arm
        trail_stop = p["peak_ret"] - trail_gap if trail_active else -1.0

        reason = None
        if ret <= -stop:
            reason = "STOP"
        elif trail_active and ret <= trail_stop:
            reason = "TRAIL"
        elif ret >= target:
            reason = "TARGET"
        elif now - p["opened"] >= ENGINE.MAX_HOLD:
            reason = "TIME"
        if not reason:
            continue

        net = ret - 2 * (ENGINE.FEE + ENGINE.SLIP) / 10000
        cash_pnl = net * p["notional"]
        ENGINE.equity += cash_pnl
        ENGINE.peak_equity = max(ENGINE.peak_equity, ENGINE.equity)
        dd = max(0.0, (ENGINE.peak_equity - ENGINE.equity) / max(ENGINE.peak_equity, 1e-9))

        m = ENGINE.metrics
        m["pnl"] = float(m.get("pnl", 0.0)) + cash_pnl
        m["net_pnl"] = float(m.get("net_pnl", 0.0)) + net
        if cash_pnl >= 0:
            m["gross_profit"] = float(m.get("gross_profit", 0.0)) + cash_pnl
        else:
            m["gross_loss"] = float(m.get("gross_loss", 0.0)) + abs(cash_pnl)
        m["equity"] = ENGINE.equity
        m["peak_equity"] = ENGINE.peak_equity
        m["drawdown"] = dd
        m["exits"] = int(m.get("exits", 0)) + 1
        m["adaptive_exits"] = int(m.get("adaptive_exits", 0)) + 1
        if cash_pnl >= 0:
            m["wins"] = int(m.get("wins", 0)) + 1
        else:
            m["losses"] = int(m.get("losses", 0)) + 1
        _record(p.get("strategy", "MOMENTUM"), ENGINE.regime, net)
        ENGINE.positions.pop(s, None)
        if dd >= ENGINE.MAX_DRAWDOWN and not ENGINE.trading_halted:
            ENGINE.trading_halted = True
            m["trading_halted"] = True
        ENGINE.log.warning(
            "ALPHA EXIT %s %s strategy=%s ret=%.3f%% net=%.3f%% pnl=%.2f dd=%.2f%%",
            reason, s.upper(), p.get("strategy", "MOMENTUM"), ret * 100, net * 100, cash_pnl, dd * 100
        )


def install(engine):
    global ENGINE, _ORIGINAL_SCORE, _ORIGINAL_MANAGE
    ENGINE = engine
    _ORIGINAL_SCORE = engine.score
    _ORIGINAL_MANAGE = engine.manage
    engine.score = adaptive_score
    engine.manage = adaptive_manage
    engine.metrics.setdefault("adaptive_exits", 0)
    engine.metrics.setdefault("gross_profit", 0.0)
    engine.metrics.setdefault("gross_loss", 0.0)
    engine.metrics.setdefault("net_pnl", 0.0)
    return engine


def stats():
    return {
        "strategies": PERF,
        "trailing": {"arm_pct": TRAIL_ARM, "gap_pct": TRAIL_GAP, "atr_mult": TRAIL_ATR_MULT},
        "adaptive_exits": int(ENGINE.metrics.get("adaptive_exits", 0)) if ENGINE else 0,
    }
