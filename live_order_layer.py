"""Fail-closed Binance Spot live execution layer with recovery unwinds."""
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
import urllib.request

log = logging.getLogger("cryptoalpha-live")
BASE = os.getenv("BINANCE_API_BASE", os.getenv("BINANCE_BASE_URL", "https://api.binance.com")).rstrip("/")
API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
LIVE = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}
RISK_PCT = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
STATIC_MAX_NOTIONAL = max(0.0, float(os.getenv("MAX_LIVE_NOTIONAL_USDT", "0")))
MIN_NET_BPS = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
RECV_WINDOW = min(60000, max(1000, int(os.getenv("BINANCE_RECV_WINDOW", "5000"))))
COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))
AUTH_CACHE_MS = max(5000, int(os.getenv("BINANCE_AUTH_CACHE_MS", "30000")))
MAX_SESSION_LOSS_USDT = max(0.0, float(os.getenv("MAX_SESSION_LOSS_USDT", "0")))
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
    req = urllib.request.Request(
        BASE + path + "?" + query + "&signature=" + signature,
        method=method,
        headers={"X-MBX-APIKEY": API_KEY, "User-Agent": "cryptoalpha-live/8"},
    )
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
        log.error("BINANCE PRIVATE AUTH FAILED | credentials missing")
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
    return {
        "configured": bool(API_KEY and API_SECRET),
        "live_trading": LIVE,
        "authenticated": _auth_ok,
        "base_url": BASE,
        "error": _auth_error,
    }


def circuit_status():
    return {
        "halted": _live_halted,
        "session_loss_usdt": round(_session_loss_usdt, 8),
        "max_session_loss_usdt": MAX_SESSION_LOSS_USDT,
        "risk_pct": RISK_PCT * 100,
        "remaining_session_loss_limit_usdt": (
            None if MAX_SESSION_LOSS_USDT <= 0
            else round(max(0.0, MAX_SESSION_LOSS_USDT - _session_loss_usdt), 8)
        ),
    }


def _trip(reason):
    global _live_halted
    _live_halted = True
    log.error("LIVE CIRCUIT BREAKER TRIPPED: %s", reason)


def record_realized_pnl(pnl_usdt):
    global _session_loss_usdt
    pnl = float(pnl_usdt)
    if pnl < 0:
        _session_loss_usdt += -pnl
        if MAX_SESSION_LOSS_USDT > 0 and _session_loss_usdt >= MAX_SESSION_LOSS_USDT:
            _trip(f"session loss limit reached: {_session_loss_usdt:.8f} USDT")
    return circuit_status()


def account():
    if not API_KEY or not API_SECRET:
        raise LiveOrderError("Binance private authentication is not configured")
    if not _check_auth():
        raise LiveOrderError("Binance private authentication is not ready")
    return _signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})


def _free_balance(asset):
    data = account()
    for item in data.get("balances", []):
        if item.get("asset") == asset:
            return float(item.get("free", 0) or 0)
    return 0.0


def exchange_info(symbols):
    qs = urllib.parse.urlencode({"symbols": json.dumps(symbols, separators=(",", ":"))})
    req = urllib.request.Request(
        BASE + "/api/v3/exchangeInfo?" + qs,
        headers={"User-Agent": "cryptoalpha-live/8"},
    )
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


def _filled_qty(order):
    fills = order.get("fills") or []
    q = sum(float(x.get("qty", 0) or 0) for x in fills)
    if q > 0:
        return q
    return float(order.get("executedQty", 0) or 0)


def _require_filled(order, label, requested_qty):
    status = str(order.get("status", "")).upper()
    filled = _filled_qty(order)
    requested = float(requested_qty)
    if status != "FILLED" or filled <= 0:
        raise LiveOrderError(
            f"{label} not fully filled: status={status or 'UNKNOWN'} "
            f"executedQty={filled:.12f} requestedQty={requested:.12f}"
        )
    if filled + max(1e-12, requested * 1e-8) < requested:
        raise LiveOrderError(
            f"{label} partial fill: executedQty={filled:.12f} requestedQty={requested:.12f}"
        )
    return filled


def market_order(symbol, side, quantity):
    if quantity <= 0:
        raise LiveOrderError(f"invalid market quantity: {quantity}")
    if not enabled():
        raise LiveOrderError(
            "LIVE_TRADING disabled, authentication failed, or circuit breaker halted"
        )
    return _signed(
        "POST",
        "/api/v3/order",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": ("%.12f" % quantity).rstrip("0").rstrip("."),
            "newOrderRespType": "FULL",
        },
    )


def _unwind_asset_to_usdt(asset, source_symbol, source_filters):
    """Best-effort emergency conversion of a residual triangle asset back to USDT."""
    if not asset or asset == "USDT":
        return {"unwound": True, "asset": asset, "quantity": 0.0, "reason": "no residual asset"}

    unwind_symbol = asset + "USDT"
    try:
        info = exchange_info([unwind_symbol])
        filters = _filters(info, unwind_symbol)
        free = _free_balance(asset)
        qty = _round_down(free, _step(filters))
        if qty <= 0:
            return {"unwound": True, "asset": asset, "quantity": free, "reason": "no tradable residual balance"}

        unwind_order = market_order(unwind_symbol, "SELL", qty)
        filled = _filled_qty(unwind_order)
        if str(unwind_order.get("status", "")).upper() != "FILLED" or filled <= 0:
            raise LiveOrderError(
                f"unwind not fully filled: symbol={unwind_symbol} "
                f"status={unwind_order.get('status')} executedQty={filled:.12f} requestedQty={qty:.12f}"
            )
        log.error("EMERGENCY UNWIND OK | %s SELL %.12f -> USDT", unwind_symbol, filled)
        return {
            "unwound": True,
            "asset": asset,
            "symbol": unwind_symbol,
            "requested_qty": qty,
            "filled_qty": filled,
            "order": unwind_order,
        }
    except Exception as e:
        _trip(f"emergency unwind failed for {asset}: {e}")
        raise LiveOrderError(f"EMERGENCY UNWIND FAILED for {asset}: {e}") from e


def _recover_incomplete_triangle(symbols, sides, filled_by_leg, failed_leg):
    """Recover residual assets after any failed/partial leg, then keep the breaker tripped."""
    recovery = []
    # Each triangle leg leaves one base asset. Convert the last successfully/partially
    # acquired asset directly to USDT through its known USDT pair.
    residual_asset = None
    if failed_leg >= 2 and filled_by_leg[1] > 0:
        residual_asset = symbols[1].replace("USDT", "") if sides[1] == "SELL" else symbols[1].replace("USDT", "")
    elif failed_leg >= 1 and filled_by_leg[0] > 0:
        residual_asset = symbols[0].replace("USDT", "")

    if residual_asset:
        recovery.append(_unwind_asset_to_usdt(residual_asset, symbols[failed_leg if failed_leg < 3 else 2], None))
    else:
        recovery.append({"unwound": True, "reason": "no confirmed residual fill"})
    return recovery


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

    first, second = parts[1], parts[2]
    path = opportunity.get("path")
    if path == f"USDT->{first}->{second}->USDT":
        symbols = [first + "USDT", first + second, second + "USDT"]
        sides = ["BUY", "SELL", "SELL"]
    elif path == f"USDT->{second}->{first}->USDT":
        symbols = [second + "USDT", first + second, first + "USDT"]
        sides = ["BUY", "BUY", "SELL"]
    else:
        return {"executed": False, "reason": "unsupported triangle path"}

    books_used = [books.get(symbol) for symbol in symbols]
    if any(not book for book in books_used):
        return {"executed": False, "reason": "missing live quote"}

    info = exchange_info(symbols)
    filters = {symbol: _filters(info, symbol) for symbol in symbols}

    acct = account()
    usdt = next((float(x.get("free", 0) or 0) for x in acct.get("balances", []) if x.get("asset") == "USDT"), 0.0)
    risk_budget = usdt * RISK_PCT
    minimum_notional = max((_min_notional(filters[symbol]) for symbol in symbols), default=0.0)

    if minimum_notional > 0 and risk_budget < minimum_notional:
        return {
            "executed": False,
            "reason": f"risk budget {risk_budget:.8f} USDT is below Binance minimum notional {minimum_notional:.8f} USDT",
            "binance_free_usdt": usdt,
            "risk_pct": RISK_PCT * 100,
            "risk_budget_usdt": risk_budget,
            "minimum_notional": minimum_notional,
        }

    notional = risk_budget
    if STATIC_MAX_NOTIONAL > 0:
        notional = min(notional, STATIC_MAX_NOTIONAL)
    if notional <= 0 or notional > usdt:
        return {"executed": False, "reason": "invalid available USDT risk budget", "binance_free_usdt": usdt, "risk_budget_usdt": risk_budget}

    first_price = float(books_used[0][1])
    if first_price <= 0:
        return {"executed": False, "reason": "invalid first-leg ask price"}

    first_qty = _round_down(notional / first_price, _step(filters[symbols[0]]))
    if first_qty <= 0:
        return {"executed": False, "reason": "quantity below market lot size"}

    _last_order_ms = now
    orders = []
    filled_by_leg = [0.0, 0.0, 0.0]
    try:
        first_order = market_order(symbols[0], sides[0], first_qty)
        orders.append(first_order)
        filled_by_leg[0] = _filled_qty(first_order)
        first_filled = _require_filled(first_order, "first leg", first_qty)

        if sides[1] == "SELL":
            second_qty = first_filled
        else:
            second_ask = float(books_used[1][1])
            if second_ask <= 0:
                raise LiveOrderError("invalid second-leg ask price")
            second_qty = _round_down(first_filled / second_ask, _step(filters[symbols[1]]))
        if second_qty <= 0:
            raise LiveOrderError("second leg quantity below market lot size")

        second_order = market_order(symbols[1], sides[1], second_qty)
        orders.append(second_order)
        filled_by_leg[1] = _filled_qty(second_order)
        second_filled = _require_filled(second_order, "second leg", second_qty)

        third_qty = second_filled
        if third_qty <= 0:
            raise LiveOrderError("third leg quantity is zero")

        third_order = market_order(symbols[2], sides[2], third_qty)
        orders.append(third_order)
        filled_by_leg[2] = _filled_qty(third_order)
        third_filled = _require_filled(third_order, "third leg", third_qty)

        return {
            "executed": True,
            "orders": orders,
            "notional_usdt": notional,
            "binance_free_usdt": usdt,
            "risk_pct": RISK_PCT * 100,
            "risk_budget_usdt": risk_budget,
            "binance_min_notional_usdt": minimum_notional,
            "path": path,
            "filled_quantities": [first_filled, second_filled, third_filled],
            "circuit_breaker": circuit_status(),
        }
    except Exception as e:
        failed_leg = min(len(orders), 2)
        log.error("INCOMPLETE TRIANGLE | path=%s failed_leg=%s filled=%s error=%s", path, failed_leg + 1, filled_by_leg, e)
        try:
            recovery = _recover_incomplete_triangle(symbols, sides, filled_by_leg, failed_leg)
        except Exception as recovery_error:
            _trip(f"triangle failure plus unrecoverable residual: {recovery_error}")
            raise
        _trip(f"incomplete live triangle recovered to USDT: {e}")
        raise LiveOrderError(f"incomplete live triangle; recovery attempted: {e}; recovery={recovery}") from e
