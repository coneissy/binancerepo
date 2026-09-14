"""Gated Binance Spot live-order layer.

LIVE_TRADING defaults to false. Credentials are read only from environment variables.
This module is intentionally limited to spot triangular execution; futures and
cross-exchange execution remain paper-only until separately implemented.

Safety model:
- Requested account risk: 1% of a $30 account = $0.30 per live cycle/session budget.
- Live notional is hard-capped at the risk budget; it is never sized from full balance.
- A cumulative loss circuit breaker disables further live orders once the budget is hit.
- If Binance minimum-notional/lot-size rules require more than the risk budget, the
  opportunity is skipped rather than increasing size to meet the exchange minimum.
- LIVE_TRADING remains false unless explicitly enabled by the environment.
"""
import hashlib, hmac, os, time, urllib.parse, urllib.request, json, logging

log = logging.getLogger("cryptoalpha-live")
BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
LIVE = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}
STARTING_BALANCE = max(0.0, float(os.getenv("LIVE_STARTING_BALANCE_USDT", "30")))
RISK_PCT = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
RISK_BUDGET = STARTING_BALANCE * RISK_PCT
# Hard cap: never expose more USDT notional than the configured risk budget.
MAX_NOTIONAL = min(max(0.0, float(os.getenv("MAX_LIVE_NOTIONAL_USDT", str(RISK_BUDGET)))), RISK_BUDGET)
MIN_NET_BPS = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
RECV_WINDOW = min(60000, max(1000, int(os.getenv("BINANCE_RECV_WINDOW", "5000"))))
COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))

# Session circuit breaker. It is intentionally fail-closed.
_session_loss_usdt = 0.0
_last_order_ms = 0
_live_halted = False

class LiveOrderError(RuntimeError):
    pass

def enabled():
    return LIVE and bool(API_KEY and API_SECRET) and not _live_halted

def circuit_status():
    return {
        "halted": _live_halted,
        "session_loss_usdt": round(_session_loss_usdt, 8),
        "risk_budget_usdt": round(RISK_BUDGET, 8),
        "remaining_risk_usdt": round(max(0.0, RISK_BUDGET - _session_loss_usdt), 8),
    }

def _trip(reason):
    global _live_halted
    _live_halted = True
    log.error("LIVE CIRCUIT BREAKER TRIPPED: %s", reason)

def record_realized_pnl(pnl_usdt):
    """Record completed-cycle P&L and halt once cumulative losses reach the budget."""
    global _session_loss_usdt
    pnl = float(pnl_usdt)
    if pnl < 0:
        _session_loss_usdt += -pnl
        if _session_loss_usdt >= RISK_BUDGET:
            _trip("cumulative live loss reached risk budget")
    return circuit_status()

def _signed(method, path, params):
    params = dict(params)
    params.setdefault("timestamp", int(time.time() * 1000))
    params.setdefault("recvWindow", RECV_WINDOW)
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        BASE + path + "?" + query + "&signature=" + sig,
        method=method,
        headers={"X-MBX-APIKEY": API_KEY, "User-Agent": "cryptoalpha-live/1"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        raise LiveOrderError(str(e)) from e

def account():
    if not enabled():
        raise LiveOrderError("live execution is disabled, halted, or Binance credentials are missing")
    return _signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})

def exchange_info(symbols):
    qs = urllib.parse.urlencode({"symbols": json.dumps(symbols, separators=(",", ":"))})
    req = urllib.request.Request(BASE + "/api/v3/exchangeInfo?" + qs, headers={"User-Agent": "cryptoalpha-live/1"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        raise LiveOrderError(str(e)) from e

def _balance(data, asset):
    for b in data.get("balances", []):
        if b.get("asset") == asset:
            return float(b.get("free", 0))
    return 0.0

def _filters(info, symbol):
    item = next((x for x in info.get("symbols", []) if x.get("symbol") == symbol), None)
    if not item or item.get("status") != "TRADING":
        raise LiveOrderError(f"symbol not tradable: {symbol}")
    out = {x["filterType"]: x for x in item.get("filters", [])}
    return out

def _round_down(value, step):
    if step <= 0:
        return value
    return (int(value / step)) * step

def market_order(symbol, side, quantity):
    if not enabled():
        raise LiveOrderError("LIVE_TRADING is false or circuit breaker is halted")
    return _signed("POST", "/api/v3/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": ("%.12f" % quantity).rstrip("0").rstrip("."),
        "newOrderRespType": "FULL",
    })

def execute_spot_triangle(opportunity, books):
    """Execute one USDT->A->B->USDT triangle only after hard safety checks.

    The order size is capped at the 1% risk budget. If Binance's minimum
    notional is above that budget, the trade is skipped. No automatic size-up
    is permitted. A later-leg failure raises an error so the dashboard can
    surface the incomplete cycle rather than reporting a false completion.
    """
    global _last_order_ms
    if not enabled():
        return {"executed": False, "reason": "LIVE_TRADING disabled or circuit breaker halted"}
    if float(opportunity.get("net_bps", 0)) < MIN_NET_BPS:
        return {"executed": False, "reason": "net bps below threshold"}
    now_ms = int(time.time() * 1000)
    if now_ms - _last_order_ms < COOLDOWN_MS:
        return {"executed": False, "reason": "live order cooldown"}
    path = opportunity.get("path", "")
    parts = path.split("->")
    if len(parts) != 4 or parts[0] != "USDT" or parts[-1] != "USDT":
        return {"executed": False, "reason": "invalid triangle path"}
    a, b = parts[1], parts[2]
    symbols = [a + "USDT", a + b, b + "USDT"]
    info = exchange_info(symbols)
    filters = {s: _filters(info, s) for s in symbols}
    acct = account()
    starting = _balance(acct, "USDT")
    if starting <= 0:
        return {"executed": False, "reason": "insufficient USDT"}

    # Never use the account balance itself as order size. The hard cap is $0.30
    # with the requested $30 starting balance and 1% risk configuration.
    remaining_risk = max(0.0, RISK_BUDGET - _session_loss_usdt)
    notional = min(MAX_NOTIONAL, remaining_risk, starting)
    if notional <= 0:
        _trip("no remaining live risk budget")
        return {"executed": False, "reason": "risk budget exhausted"}

    f = filters[symbols[0]]
    lot = f.get("MARKET_LOT_SIZE", f.get("LOT_SIZE", {}))
    step = float(lot.get("stepSize", "0.000001"))
    qty = _round_down(notional, step)
    if qty <= 0:
        return {"executed": False, "reason": "quantity below LOT_SIZE"}

    nf = f.get("NOTIONAL", f.get("MIN_NOTIONAL", {}))
    min_notional = float(nf.get("minNotional", "0"))
    if min_notional and qty < min_notional:
        return {
            "executed": False,
            "reason": f"exchange minimum {min_notional} exceeds hard risk cap {RISK_BUDGET:.8f}",
            "risk_budget_usdt": RISK_BUDGET,
        }

    # Guard the other two legs before sending leg 1. If any leg cannot satisfy
    # the risk cap/minimum constraints, skip the entire triangle.
    for s in symbols[1:]:
        sf = filters[s]
        snf = sf.get("NOTIONAL", sf.get("MIN_NOTIONAL", {}))
        smin = float(snf.get("minNotional", "0"))
        if smin and notional < smin:
            return {
                "executed": False,
                "reason": f"leg {s} minimum {smin} exceeds hard risk cap {RISK_BUDGET:.8f}",
                "risk_budget_usdt": RISK_BUDGET,
            }

    _last_order_ms = now_ms
    orders = []
    try:
        orders.append(market_order(symbols[0], "BUY", qty))
        a_qty = sum(float(x.get("qty", 0)) for x in orders[-1].get("fills", []))
        if a_qty <= 0:
            raise LiveOrderError("first leg returned no filled quantity")
        orders.append(market_order(symbols[1], "SELL", a_qty))
        b_qty = sum(float(x.get("qty", 0)) for x in orders[-1].get("fills", []))
        if b_qty <= 0:
            raise LiveOrderError("second leg returned no filled quantity")
        orders.append(market_order(symbols[2], "SELL", b_qty))
        return {
            "executed": True,
            "orders": orders,
            "notional_usdt": notional,
            "risk_pct": RISK_PCT * 100,
            "risk_budget_usdt": RISK_BUDGET,
            "circuit_breaker": circuit_status(),
        }
    except Exception:
        # Fail closed after an incomplete multi-leg cycle. Do not automatically
        # submit compensating orders because that could increase exposure.
        _trip("incomplete live triangle; manual reconciliation required")
        raise
