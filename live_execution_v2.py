import hashlib, hmac, json, logging, os, time, urllib.parse, urllib.request
from decimal import Decimal, ROUND_DOWN

log = logging.getLogger("cryptoalpha-live-v2")

SPOT_BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")
FUT_BASE = os.getenv("BINANCE_FUTURES_API_BASE", "https://fapi.binance.com").rstrip("/")
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Fail closed. Live execution requires BOTH explicit switches and an exact acknowledgement.
LIVE = os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() in {"1", "true", "yes", "on"}
LIVE_CONFIRMATION = os.getenv("LIVE_CONFIRMATION", "") == "I_UNDERSTAND_LIVE_RISK"
LIVE_SPOT = os.getenv("LIVE_SPOT_TRADING", "false").lower() in {"1", "true", "yes", "on"}
LIVE_FUTURES = os.getenv("LIVE_FUTURES_TRADING", "false").lower() in {"1", "true", "yes", "on"}
STARTING = max(0.0, float(os.getenv("LIVE_STARTING_BALANCE_USDT", "30")))
RISK_PCT = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.005"))))
RISK_BUDGET = STARTING * RISK_PCT
MAX_NOTIONAL = min(max(0.0, float(os.getenv("MAX_LIVE_NOTIONAL_USDT", str(RISK_BUDGET)))), RISK_BUDGET)
MIN_NET_BPS = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "5")))
MAX_STALENESS_MS = max(100, int(os.getenv("MAX_MARKET_DATA_AGE_MS", "500")))
RECV_WINDOW = min(60000, max(1000, int(os.getenv("BINANCE_RECV_WINDOW", "5000"))))
COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))

_halted = False
_loss = 0.0
_last_order = 0

class LiveOrderError(RuntimeError):
    pass

def enabled(kind=None):
    armed = LIVE and LIVE_ARMED and LIVE_CONFIRMATION and bool(API_KEY and API_SECRET) and not _halted
    if not armed:
        return False
    if kind == "spot": return LIVE_SPOT
    if kind == "futures": return LIVE_FUTURES
    return LIVE_SPOT or LIVE_FUTURES

def status():
    return {
        "mode": "LIVE_READY" if (LIVE and LIVE_ARMED and LIVE_CONFIRMATION) else "PAPER_ONLY",
        "live_enabled": bool(LIVE and LIVE_ARMED and LIVE_CONFIRMATION),
        "halted": _halted,
        "session_loss_usdt": round(_loss, 8),
        "risk_budget_usdt": round(RISK_BUDGET, 8),
        "remaining_risk_usdt": round(max(0, RISK_BUDGET-_loss), 8),
        "max_notional_usdt": round(MAX_NOTIONAL, 8),
        "spot_enabled": enabled("spot"),
        "futures_enabled": enabled("futures"),
    }

def trip(reason):
    global _halted
    _halted = True
    log.error("LIVE CIRCUIT BREAKER: %s", reason)

def record_realized_pnl(pnl):
    global _loss
    pnl = float(pnl)
    if pnl < 0:
        _loss += -pnl
        if _loss >= RISK_BUDGET: trip("cumulative live loss reached risk budget")
    return status()

def _signed(base, method, path, params):
    params = dict(params)
    params.setdefault("timestamp", int(time.time()*1000))
    params.setdefault("recvWindow", RECV_WINDOW)
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(base+path+"?"+query+"&signature="+sig, method=method, headers={"X-MBX-APIKEY":API_KEY,"User-Agent":"cryptoalpha-live-v2/1"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r: return json.loads(r.read().decode())
    except Exception as e: raise LiveOrderError(str(e)) from e

def _public(base, path, params=None):
    q=urllib.parse.urlencode(params or {}, doseq=True)
    req=urllib.request.Request(base+path+("?"+q if q else ""), headers={"User-Agent":"cryptoalpha-live-v2/1"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r: return json.loads(r.read().decode())
    except Exception as e: raise LiveOrderError(str(e)) from e

def spot_account():
    if not enabled("spot"): raise LiveOrderError("live spot disabled, unarmed, or halted")
    return _signed(SPOT_BASE,"GET","/api/v3/account",{"omitZeroBalances":"true"})

def spot_info(symbols):
    return _public(SPOT_BASE,"/api/v3/exchangeInfo",{"symbols":json.dumps(symbols,separators=(",",":"))})

def fut_account():
    if not enabled("futures"): raise LiveOrderError("live futures disabled, unarmed, or halted")
    return _signed(FUT_BASE,"GET","/fapi/v2/account",{})

def fut_info(symbols=None):
    return _public(FUT_BASE,"/fapi/v1/exchangeInfo",{})

def _symbol_info(info, symbol):
    item=next((x for x in info.get("symbols",[]) if x.get("symbol")==symbol),None)
    if not item or item.get("status")!="TRADING": raise LiveOrderError(f"symbol not tradable: {symbol}")
    return item, {x["filterType"]:x for x in item.get("filters",[])}

def _round_down(v, step):
    if step<=0:return v
    q = Decimal(str(v)); s = Decimal(str(step))
    return float((q / s).to_integral_value(rounding=ROUND_DOWN) * s)

def _qty_filter(filters):
    return filters.get("MARKET_LOT_SIZE", filters.get("LOT_SIZE",{}))

def _min_notional(filters):
    f=filters.get("NOTIONAL", filters.get("MIN_NOTIONAL",{}))
    return float(f.get("minNotional", f.get("notional", "0")))

def _balance_spot(acct, asset):
    return next((float(x.get("free",0)) for x in acct.get("balances",[]) if x.get("asset")==asset),0.0)

def _balance_fut(acct):
    return float(acct.get("availableBalance",0))

def _risk_notional(balance):
    return min(MAX_NOTIONAL, max(0.0, RISK_BUDGET-_loss), max(0.0, float(balance)))

def _guard(symbols, info, notional):
    if notional <= 0: return False, "zero available risk/notional"
    for s in symbols:
        _,f=_symbol_info(info,s)
        mn=_min_notional(f)
        if mn > 0 and notional < mn:
            return False, f"{s} minimum {mn} exceeds hard cap {RISK_BUDGET:.8f}"
    return True,""

def _market_fresh(price_store, symbol):
    row = price_store.get(symbol)
    if not row or len(row) < 2: return False
    try:
        ts = float(row[0])
        # Accept either epoch-ms or epoch-s timestamps.
        age = time.time()*1000 - ts if ts > 10_000_000_000 else time.time() - ts
        if ts <= 10_000_000_000: age *= 1000
        return 0 <= age <= MAX_STALENESS_MS
    except Exception:
        return False

def _filled_qty(order):
    q=order.get("executedQty",0)
    return float(q or 0)

def _assert_full_fill(order, leg):
    q=_filled_qty(order)
    status=order.get("status","")
    if q <= 0 or status not in {"FILLED"}:
        raise LiveOrderError(f"triangle leg {leg} not fully filled: status={status!r} qty={q}")
    return q

def spot_order(symbol, side, quantity):
    if not enabled("spot"): raise LiveOrderError("spot live execution not armed")
    if quantity <= 0: raise LiveOrderError("quantity must be positive")
    return _signed(SPOT_BASE,"POST","/api/v3/order",{"symbol":symbol,"side":side,"type":"MARKET","quantity":"%.12f"%quantity,"newOrderRespType":"FULL"})

def fut_order(symbol, side, quantity, reduce_only=False):
    if not enabled("futures"): raise LiveOrderError("futures live execution not armed")
    if quantity <= 0: raise LiveOrderError("quantity must be positive")
    p={"symbol":symbol,"side":side,"type":"MARKET","quantity":"%.12f"%quantity,"newOrderRespType":"RESULT"}
    if reduce_only:p["reduceOnly"]="true"
    return _signed(FUT_BASE,"POST","/fapi/v1/order",p)

def execute_spot_triangle(opportunity, price_store):
    global _last_order
    if not enabled("spot"): return {"executed":False,"reason":"spot live disabled or unarmed"}
    if float(opportunity.get("net_bps",0))<MIN_NET_BPS:return {"executed":False,"reason":"below threshold"}
    now=int(time.time()*1000)
    if now-_last_order<COOLDOWN_MS:return {"executed":False,"reason":"cooldown"}
    parts=opportunity.get("path","").split("->")
    if len(parts)!=4 or parts[0]!="USDT" or parts[-1]!="USDT":return {"executed":False,"reason":"invalid triangle"}
    a,b=parts[1],parts[2]; syms=[a+"USDT",a+b,b+"USDT"]
    if not all(_market_fresh(price_store,s) for s in syms): return {"executed":False,"reason":"stale or missing market data"}
    info=spot_info(syms); acct=spot_account(); balance=_balance_spot(acct,"USDT"); notional=_risk_notional(balance)
    ok,reason=_guard(syms,info,notional)
    if not ok:return {"executed":False,"reason":reason,"risk_budget_usdt":RISK_BUDGET}
    _,f=_symbol_info(info,syms[0]); lot=_qty_filter(f); step=float(lot.get("stepSize","0.000001")); min_qty=float(lot.get("minQty","0"))
    qty=_round_down(notional/float(price_store[syms[0]][1]),step)
    if qty<min_qty or qty<=0:return {"executed":False,"reason":"quantity below lot size"}
    _last_order=now; orders=[]
    try:
        o1=spot_order(syms[0],"BUY",qty); orders.append(o1); aq=_assert_full_fill(o1,1)
        o2=spot_order(syms[1],"SELL",aq); orders.append(o2); bq=_assert_full_fill(o2,2)
        o3=spot_order(syms[2],"SELL",bq); orders.append(o3); _assert_full_fill(o3,3)
        return {"executed":True,"engine":"SPOT_TRIANGULAR","orders":orders,"notional_usdt":notional}
    except Exception:
        trip("incomplete spot triangle; execution halted for manual reconciliation")
        raise

def execute_futures_triangle(opportunity, price_store):
    global _last_order
    if not enabled("futures"): return {"executed":False,"reason":"futures live disabled or unarmed"}
    if float(opportunity.get("net_bps",0))<MIN_NET_BPS:return {"executed":False,"reason":"below threshold"}
    now=int(time.time()*1000)
    if now-_last_order<COOLDOWN_MS:return {"executed":False,"reason":"cooldown"}
    parts=opportunity.get("path","").split("->")
    if len(parts)!=4 or parts[0]!="USDT" or parts[-1]!="USDT":return {"executed":False,"reason":"invalid futures triangle"}
    a,b=parts[1],parts[2]; syms=[a+"USDT",a+b,b+"USDT"]
    if not all(_market_fresh(price_store,s) for s in syms): return {"executed":False,"reason":"stale or missing market data"}
    info=fut_info(); acct=fut_account(); balance=_balance_fut(acct); notional=_risk_notional(balance); ok,reason=_guard(syms,info,notional)
    if not ok:return {"executed":False,"reason":reason,"risk_budget_usdt":RISK_BUDGET}
    _,f=_symbol_info(info,syms[0]); lot=_qty_filter(f); step=float(lot.get("stepSize","0.001")); min_qty=float(lot.get("minQty","0")); qty=_round_down(notional/float(price_store[syms[0]][1]),step)
    if qty<min_qty or qty<=0:return {"executed":False,"reason":"quantity below lot size"}
    _last_order=now; orders=[]
    try:
        o1=fut_order(syms[0],"BUY",qty); orders.append(o1); aq=_assert_full_fill(o1,1)
        o2=fut_order(syms[1],"SELL",aq); orders.append(o2); bq=_assert_full_fill(o2,2)
        o3=fut_order(syms[2],"SELL",bq); orders.append(o3); _assert_full_fill(o3,3)
        return {"executed":True,"engine":"FUTURES_TRIANGULAR","orders":orders,"notional_usdt":notional}
    except Exception:
        trip("incomplete futures triangle; execution halted for manual reconciliation")
        raise

def execute_basis(opportunity, spot_prices, fut_prices):
    # Basis execution remains disabled here because a spot short requires margin support.
    return {"executed":False,"reason":"basis execution disabled in safe spot/futures executor"}
