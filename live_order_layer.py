"""Gated Binance Spot live-order layer.

LIVE_TRADING defaults to false. Credentials are read only from environment variables.
This module is intentionally limited to spot triangular execution; futures and
cross-exchange execution remain paper-only until separately implemented.
"""
import hashlib, hmac, os, time, urllib.parse, urllib.request, json, logging

log = logging.getLogger("cryptoalpha-live")
BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
LIVE = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}
MAX_NOTIONAL = max(0.0, float(os.getenv("MAX_LIVE_NOTIONAL_USDT", "30")))
RISK_PCT = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
MIN_NET_BPS = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
RECV_WINDOW = min(60000, max(1000, int(os.getenv("BINANCE_RECV_WINDOW", "5000"))))

class LiveOrderError(RuntimeError):
    pass

def enabled():
    return LIVE and bool(API_KEY and API_SECRET)

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
        raise LiveOrderError("live execution is disabled or Binance credentials are missing")
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
    if step <= 0: return value
    return (int(value / step)) * step

def market_order(symbol, side, quantity):
    if not enabled():
        raise LiveOrderError("LIVE_TRADING is false")
    return _signed("POST", "/api/v3/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": ("%.12f" % quantity).rstrip("0").rstrip("."),
        "newOrderRespType": "FULL",
    })

def execute_spot_triangle(opportunity, books):
    """Execute one USDT->A->B->USDT triangle only after hard checks.

    Uses market orders and confirms each order response before continuing.
    A failed later leg raises an error rather than pretending the cycle completed.
    """
    if not enabled():
        return {"executed": False, "reason": "LIVE_TRADING disabled"}
    if opportunity.get("engine") != "SPOT_TRIANGULAR":
        return {"executed": False, "reason": "unsupported live engine"}
    if float(opportunity.get("net_bps", 0)) < MIN_NET_BPS:
        return {"executed": False, "reason": "net bps below threshold"}
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
    notional = min(MAX_NOTIONAL, starting)
    if notional <= 0:
        return {"executed": False, "reason": "insufficient USDT"}
    # The configured 1% is a hard exposure target, not a guaranteed maximum loss.
    # Do not trade a size that is below exchange minimums.
    if starting < 5:
        return {"executed": False, "reason": "balance too small for safe live execution"}
    qty = notional
    f = filters[symbols[0]]
    lot = f.get("LOT_SIZE", {})
    qty = _round_down(qty, float(lot.get("stepSize", "0.000001")))
    if qty <= 0:
        return {"executed": False, "reason": "quantity below LOT_SIZE"}
    min_notional = float(f.get("NOTIONAL", f.get("MIN_NOTIONAL", {})).get("minNotional", "0"))
    if min_notional and qty < min_notional:
        return {"executed": False, "reason": f"below minimum notional {min_notional}"}
    orders = []
    orders.append(market_order(symbols[0], "BUY", qty))
    a_qty = sum(float(x.get("qty", 0)) for x in orders[-1].get("fills", []))
    if a_qty <= 0:
        raise LiveOrderError("first leg returned no filled quantity")
    orders.append(market_order(symbols[1], "SELL", a_qty))
    b_qty = sum(float(x.get("qty", 0)) for x in orders[-1].get("fills", []))
    if b_qty <= 0:
        raise LiveOrderError("second leg returned no filled quantity")
    orders.append(market_order(symbols[2], "SELL", b_qty))
    return {"executed": True, "orders": orders, "notional_usdt": notional, "risk_pct": RISK_PCT * 100}
