"""Fail-closed Binance Spot live execution layer."""
import hashlib, hmac, json, logging, os, time, urllib.parse, urllib.request

log = logging.getLogger("cryptoalpha-live")
BASE = os.getenv("BINANCE_API_BASE", os.getenv("BINANCE_BASE_URL", "https://api.binance.com")).rstrip("/")
API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
LIVE = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}
STARTING_BALANCE = max(0.0, float(os.getenv("LIVE_STARTING_BALANCE_USDT", "30")))
RISK_PCT = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
STATIC_MAX_NOTIONAL = max(0.0, float(os.getenv("MAX_LIVE_NOTIONAL_USDT", "0")))
MIN_NET_BPS = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
RECV_WINDOW = min(60000, max(1000, int(os.getenv("BINANCE_RECV_WINDOW", "5000"))))
COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))
AUTH_CACHE_MS = max(5000, int(os.getenv("BINANCE_AUTH_CACHE_MS", "30000")))
_session_loss_usdt = 0.0
_last_order_ms = 0
_live_halted = False
_auth_checked_ms = 0
_auth_ok = False
_auth_error = ""

class LiveOrderError(RuntimeError):
    pass

def _http_error_message(e):
    try:
        body = e.read().decode("utf-8", "replace")
        if body:
            return body[:500]
    except Exception:
        pass
    return str(e)

def _signed(method, path, params):
    params = dict(params)
    params.setdefault("timestamp", int(time.time() * 1000))
    params.setdefault("recvWindow", RECV_WINDOW)
    query = urllib.parse.urlencode(params, doseq=True)
    signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(BASE + path + "?" + query + "&signature=" + signature, method=method,
                                 headers={"X-MBX-APIKEY": API_KEY, "User-Agent": "cryptoalpha-live/4"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        raise LiveOrderError(_http_error_message(e)) from e

def _check_auth(force=False):
    global _auth_checked_ms, _auth_ok, _auth_error
    now = int(time.time() * 1000)
    if not force and now - _auth_checked_ms < AUTH_CACHE_MS:
        return _auth_ok
    _auth_checked_ms = now
    if not API_KEY or not API_SECRET:
        _auth_ok = False
        _auth_error = "BINANCE_API_KEY or BINANCE_API_SECRET is missing"
        return False
    try:
        _signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
        _auth_ok = True
        _auth_error = ""
        log.info("BINANCE PRIVATE AUTH OK | base=%s", BASE)
    except Exception as e:
        _auth_ok = False
        _auth_error = str(e)
        log.error("BINANCE PRIVATE AUTH FAILED | %s", _auth_error)
    return _auth_ok

def enabled():
    if not LIVE or _live_halted:
        return False
    return _check_auth()

def auth_status():
    _check_auth()
    return {"configured": bool(API_KEY and API_SECRET), "live_trading": LIVE, "authenticated": _auth_ok,
            "base_url": BASE, "error": _auth_error}

def circuit_status():
    return {"halted": _live_halted, "session_loss_usdt": round(_session_loss_usdt, 8),
            "risk_pct": RISK_PCT * 100,
            "fallback_risk_budget_usdt": round(STARTING_BALANCE * RISK_PCT, 8),
            "remaining_fallback_risk_usdt": round(max(0.0, STARTING_BALANCE * RISK_PCT - _session_loss_usdt), 8)}

def _trip(reason):
    global _live_halted
    _live_halted = True
    log.error("LIVE CIRCUIT BREAKER TRIPPED: %s", reason)

def record_realized_pnl(pnl_usdt):
    global _session_loss_usdt
    pnl = float(pnl_usdt)
    if pnl < 0:
        _session_loss_usdt += -pnl
    return circuit_status()

def account():
    if not API_KEY or not API_SECRET:
        raise LiveOrderError("Binance private authentication is not configured")
    if not _check_auth():
        raise LiveOrderError("Binance private authentication is not ready")
    return _signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})

def exchange_info(symbols):
    qs = urllib.parse.urlencode({"symbols": json.dumps(symbols, separators=(",", ":"))})
    req = urllib.request.Request(BASE + "/api/v3/exchangeInfo?" + qs, headers={"User-Agent": "cryptoalpha-live/4"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        raise LiveOrderError(_http_error_message(e)) from e

def _filters(info, symbol):
    item = next((x for x in info.get("symbols", []) if x.get("symbol") == symbol), None)
    if not item or item.get("status") != "TRADING":
        raise LiveOrderError(f"symbol not tradable: {symbol}")
    return {x["filterType"]: x for x in item.get("filters", [])}

def _min_notional(filters):
    f = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
    return float(f.get("minNotional", "0") or 0)

def _step(filters):
    f = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
    return float(f.get("stepSize", "0.000001") or 0.000001)

def _round_down(value, step):
    if step <= 0:
        return value
    return int(value / step) * step

def market_order(symbol, side, quantity):
    if not enabled():
        raise LiveOrderError("LIVE_TRADING disabled, authentication failed, or circuit breaker halted")
    return _signed("POST", "/api/v3/order", {"symbol": symbol, "side": side, "type": "MARKET",
                                                "quantity": ("%.12f" % quantity).rstrip("0").rstrip("."),
                                                "newOrderRespType": "FULL"})

def _filled_qty(order):
    fills = order.get("fills") or []
    q = sum(float(x.get("qty", 0) or 0) for x in fills)
    if q > 0:
        return q
    return float(order.get("executedQty", 0) or 0)

def execute_spot_triangle(opportunity, books):
    global _last_order_ms
    if not enabled():
        return {"executed": False, "reason": "Binance private authentication is not ready"}
    if float(opportunity.get("net_bps", 0)) < MIN_NET_BPS:
        return {"executed": False, "reason": "net bps below threshold"}
    now = int(time.time() * 1000)
    if now - _last_order_ms < COOLDOWN_MS:
        return {"executed": False, "reason": "live order cooldown"}
    parts = opportunity.get("path", "").split("->")
    if len(parts) != 4 or parts[0] != "USDT" or parts[-1] != "USDT":
        return {"executed": False, "reason": "invalid triangle path"}
    a, b = parts[1], parts[2]
    symbols = [a + "USDT", a + b, b + "USDT"]
    q = {s: books.get(s) for s in symbols}
    if any(not x for x in q.values()):
        return {"executed": False, "reason": "missing live quote"}
    info = exchange_info(symbols)
    filters = {s: _filters(info, s) for s in symbols}
    acct = account()
    usdt = next((float(x.get("free", 0) or 0) for x in acct.get("balances", []) if x.get("asset") == "USDT"), 0.0)
    risk_budget = usdt * RISK_PCT
    static_cap = STATIC_MAX_NOTIONAL if STATIC_MAX_NOTIONAL > 0 else risk_budget
    minimum_notional = max((_min_notional(filters[s]) for s in symbols), default=0.0)
    # Binance-valid sizing: use the larger of the configured risk budget and
    # the symbol minimum, but never exceed available free USDT or a static cap.
    target_notional = max(risk_budget, minimum_notional)
    if STATIC_MAX_NOTIONAL > 0:
        target_notional = min(target_notional, STATIC_MAX_NOTIONAL)
    notional = min(target_notional, usdt)
    if notional <= 0:
        return {"executed": False, "reason": "no available Binance USDT balance", "binance_free_usdt": usdt,
                "risk_pct": RISK_PCT * 100, "risk_budget_usdt": risk_budget}
    if minimum_notional and notional < minimum_notional:
        return {"executed": False, "reason": f"Binance minimum notional {minimum_notional} exceeds free USDT balance {usdt:.8f}",
                "binance_free_usdt": usdt, "risk_pct": RISK_PCT * 100, "risk_budget_usdt": risk_budget,
                "minimum_notional": minimum_notional}
    ask_a = float(q[symbols[0]][1])
    step = _step(filters[symbols[0]])
    qty = _round_down(notional / ask_a, step)
    if qty <= 0:
        return {"executed": False, "reason": "quantity below market lot size"}
    _last_order_ms = now
    orders = []
    try:
        orders.append(market_order(symbols[0], "BUY", qty))
        a_qty = _filled_qty(orders[-1])
        if a_qty <= 0:
            raise LiveOrderError("first leg returned no filled quantity")
        orders.append(market_order(symbols[1], "SELL", a_qty))
        b_qty = _filled_qty(orders[-1])
        if b_qty <= 0:
            raise LiveOrderError("second leg returned no filled quantity")
        orders.append(market_order(symbols[2], "SELL", b_qty))
        return {"executed": True, "orders": orders, "notional_usdt": notional,
                "binance_free_usdt": usdt, "risk_pct": RISK_PCT * 100, "risk_budget_usdt": risk_budget,
                "binance_min_notional_usdt": minimum_notional, "circuit_breaker": circuit_status()}
    except Exception:
        _trip("incomplete live triangle; manual reconciliation required")
        raise
