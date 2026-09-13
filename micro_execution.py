"""Fast microstructure layer for Cryptoalpha.

Keeps the strongest part of the old fast scalper: sub-minute reaction speed.
It does not execute orders; it only adds a 30-second confirmation gate/boost
on top of the 1m adaptive engine while DRY_RUN remains mandatory.
"""
import os
import time
from collections import defaultdict, deque

ENGINE = None
_ORIGINAL_MESSAGE = None
_ORIGINAL_SCORE = None

WINDOW = float(os.getenv("MICRO_WINDOW_SECONDS", "30"))
MIN_TRADES = int(os.getenv("MICRO_MIN_TRADES", "8"))
BOOST = float(os.getenv("MICRO_SCORE_BOOST", "0.10"))
PENALTY = float(os.getenv("MICRO_SCORE_PENALTY", "0.10"))
FLOW_GATE = float(os.getenv("MICRO_FLOW_GATE", "0.12"))
MOM_GATE = float(os.getenv("MICRO_MOM_GATE", "0.0007"))
MAX_AGE = WINDOW * 1.5
TAPE = defaultdict(lambda: deque(maxlen=500))
STATS = {"events": 0, "boosts": 0, "penalties": 0, "gated": 0}


def _trim(s, now=None):
    now = now or time.time()
    q = TAPE[s]
    cutoff = now - WINDOW
    while q and q[0][0] < cutoff:
        q.popleft()
    return q


def _micro(s):
    q = _trim(s)
    if len(q) < MIN_TRADES:
        return None
    first = q[0][1]
    last = q[-1][1]
    total = sum(x[2] for x in q)
    buy = sum(x[2] for x in q if not x[3])
    sell = total - buy
    flow = (buy - sell) / max(total, 1e-12)
    mom = last / max(first, 1e-12) - 1.0
    velocity = mom / max((q[-1][0] - q[0][0]), 1.0)
    return {"trades": len(q), "flow": flow, "mom": mom, "velocity": velocity}


def on_message(ws, msg):
    # Feed the original websocket handler first so the engine's normal 1m
    # state remains authoritative.
    _ORIGINAL_MESSAGE(ws, msg)
    try:
        import json
        d = json.loads(msg).get("data", {})
        if d.get("e") != "aggTrade":
            return
        s = d.get("s", "").lower()
        p = float(d.get("p", 0))
        qty = float(d.get("q", 0))
        if not s or p <= 0 or qty <= 0:
            return
        TAPE[s].append((float(d.get("T", time.time() * 1000)) / 1000.0, p, p * qty, bool(d.get("m"))))
        STATS["events"] += 1
    except Exception:
        return


def score(s, radar, age, q, reject):
    base = _ORIGINAL_SCORE(s, radar, age, q, reject)
    if not base:
        return None
    m = _micro(s)
    if not m:
        base["micro"] = None
        return base
    side = 1 if base["side"] == "BUY" else -1
    aligned_flow = m["flow"] * side
    aligned_mom = m["mom"] * side
    micro_ok = aligned_flow >= FLOW_GATE and aligned_mom >= MOM_GATE
    micro_against = aligned_flow <= -FLOW_GATE and aligned_mom <= -MOM_GATE

    if micro_ok:
        base["score"] = min(1.0, base["score"] + BOOST)
        base["ensemble_score"] = min(1.0, float(base.get("ensemble_score", base["score"])) + BOOST)
        STATS["boosts"] += 1
    elif micro_against:
        base["score"] = max(0.0, base["score"] - PENALTY)
        base["ensemble_score"] = max(0.0, float(base.get("ensemble_score", base["score"])) - PENALTY)
        STATS["penalties"] += 1

    # Fast tape confirmation can upgrade a borderline 1m setup, but never
    # creates a trade from nothing: the underlying engine must already be
    # eligible and the micro tape must agree with the intended side.
    base["micro"] = {"trades": m["trades"], "flow": round(m["flow"], 4), "mom": round(m["mom"], 6), "velocity": round(m["velocity"], 8)}
    base["micro_confirmed"] = micro_ok
    base["eligible"] = bool(base.get("eligible") and micro_ok and base["score"] >= ENGINE.ENTRY)
    if not base["eligible"]:
        STATS["gated"] += 1
    return base


def install(engine):
    global ENGINE, _ORIGINAL_MESSAGE, _ORIGINAL_SCORE
    ENGINE = engine
    _ORIGINAL_MESSAGE = engine.on_message
    _ORIGINAL_SCORE = engine.score
    engine.on_message = on_message
    engine.score = score
    return engine


def stats():
    return {**STATS, "window_seconds": WINDOW, "flow_gate": FLOW_GATE, "mom_gate": MOM_GATE}
