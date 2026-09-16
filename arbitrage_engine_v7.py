"""Dragon v7: canonical diagnostics + fail-closed Binance live spot executor."""
import os, time, logging
import dragon_core as core
from live_order_layer import enabled, execute_spot_triangle, circuit_status, auth_status

log = logging.getLogger("dragon-v7")
LIVE_COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))
_last_live_ms = 0.0
_original_stats = core.stats
core.START = float(os.getenv("SIM_START_EQUITY", "30"))
core.RISK = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
core.MIN_NET = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))
core.state.setdefault("near_misses", []); core.state.setdefault("rejection_counts", {}); core.state.setdefault("error_log", []); core.state.setdefault("diagnostic_scans", 0)

def _live_configured():
    return os.getenv("LIVE_TRADING", "false").lower() in {"1", "true", "yes", "on"}

def _record_error(message):
    with core.lock:
        core.state["errors"] += 1; core.state["last_error"] = message
        core.state["error_log"].append({"ts": time.time(), "message": message}); core.state["error_log"] = core.state["error_log"][-20:]

def _record_rejection(engine, symbol, reason, gross=0.0, net=0.0, extra=None):
    item={"engine":engine,"symbol":symbol,"reason":reason,"gross_bps":round(float(gross),4),"net_bps":round(float(net),4),"observed_at":time.time()}
    if extra: item.update(extra)
    with core.lock:
        core.state["near_misses"].append(item); core.state["near_misses"] = core.state["near_misses"][-100:]
        core.state["rejection_counts"][reason] = core.state["rejection_counts"].get(reason,0)+1

def _quote(store, symbol):
    return core.quote(store, symbol)

def _tri_diag(a,b,q,eq,venue="spot",engine="SPOT_TRIANGULAR"):
    store=core.spot if venue=="spot" else core.fut
    pa,ra=_quote(store,a+q); pab,rb=_quote(store,a+b); pb,rc=_quote(store,b+q)
    if not all((pa,pab,pb)):
        _record_rejection(engine,a+b+q,ra or rb or rc or "missing_quote"); return None
    # Executable prices: buy uses ask, sell uses bid. Evaluate both directions.
    forward=(1.0/pa[1])*pab[0]*pb[0]
    reverse=(1.0/pb[1])*(1.0/pab[1])*pa[0]
    g1=(forward-1)*10000; g2=(reverse-1)*10000; gross=max(g1,g2)
    funding = venue == "fut"
    cost = core.cost_bps(3, funding)
    net = core.net_bps(gross, 3, funding)
    path=f"USDT->{a}->{b}->USDT" if g1>=g2 else f"USDT->{b}->{a}->USDT"
    if gross<=0:
        _record_rejection(engine,a+b+q,"negative_gross_edge",gross,net,{"path":path,"cost_bps":cost}); return None
    if net<core.MIN_NET:
        _record_rejection(engine,a+b+q,"below_min_net",gross,net,{"path":path,"min_net_bps":core.MIN_NET,"cost_bps":cost}); return None
    if net>core.MAX_NET:
        _record_rejection(engine,a+b+q,"above_max_net_safety",gross,net,{"path":path,"max_net_bps":core.MAX_NET,"cost_bps":cost}); return None
    return {"engine":engine,"type":engine,"path":path,"symbol":a+b+q,"gross_bps":gross,"net_bps":net,"notional_usdt":core.target_notional_usdt(eq),"paper_only":False,"diagnostic":{"fees_bps":3*core.FEE,"slippage_bps":3*core.SLIP,"funding_bps":core.FUND if funding else 0,"total_cost_bps":cost}}

def _basis_diag(s,eq):
    sp,rs=_quote(core.spot,s); fu,rf=_quote(core.fut,s)
    if not sp or not fu:
        _record_rejection("SPOT_FUTURES_BASIS",s,rs or rf or "missing_quote"); return None
    # Two executable directions. Do not use abs(); direction comes from the winning side.
    long_spot=(fu[0]/sp[1]-1)*10000
    long_fut=(sp[0]/fu[1]-1)*10000
    gross=max(long_spot,long_fut)
    direction="BUY_SPOT_SELL_FUTURES" if long_spot>=long_fut else "SELL_SPOT_BUY_FUTURES"
    cost=core.cost_bps(2, True)
    net=core.net_bps(gross,2,True)
    if gross<=0:
        _record_rejection("SPOT_FUTURES_BASIS",s,"negative_gross_edge",gross,net,{"direction":direction,"cost_bps":cost}); return None
    if net<core.MIN_NET:
        _record_rejection("SPOT_FUTURES_BASIS",s,"below_min_net",gross,net,{"direction":direction,"min_net_bps":core.MIN_NET,"cost_bps":cost}); return None
    if net>core.MAX_NET:
        _record_rejection("SPOT_FUTURES_BASIS",s,"above_max_net_safety",gross,net,{"direction":direction,"max_net_bps":core.MAX_NET,"cost_bps":cost}); return None
    return {"engine":"SPOT_FUTURES_BASIS","type":"SPOT_FUTURES_CROSS","symbol":s,"direction":direction,"basis_bps":gross,"gross_bps":gross,"net_bps":net,"notional_usdt":core.target_notional_usdt(eq),"paper_only":True,"diagnostic":{"fees_bps":2*core.FEE,"slippage_bps":2*core.SLIP,"funding_bps":core.FUND,"total_cost_bps":cost}}

def _run_diagnostics(eq):
    with core.lock:
        core.state["near_misses"]=[]; core.state["diagnostic_scans"]+=1
    s=[_tri_diag(a,b,q,eq) for a,b,q in core.ROUTES]
    f=[_basis_diag(sym,eq) for sym in core.FUT_SYMBOLS if sym.endswith("USDT")]
    ft=[_tri_diag(a,b,q,eq,"fut","FUTURES_TRIANGULAR") for a,b,q in core.FUT_TRI_ROUTES]
    s=[x for x in s if x]; f=[x for x in f if x]; ft=[x for x in ft if x]
    s.sort(key=lambda x:x["net_bps"],reverse=True); f.sort(key=lambda x:x["net_bps"],reverse=True); ft.sort(key=lambda x:x["net_bps"],reverse=True)
    return s,f,ft

def live_scan():
    global _last_live_ms
    while True:
        try:
            with core.lock: eq=core.state["equity"]
            s,f,ft=_run_diagnostics(eq); allx=s+f+ft; now=time.monotonic()*1000
            if enabled() and s and now-_last_live_ms>=LIVE_COOLDOWN_MS:
                best=s[0]; _last_live_ms=now
                try:
                    result=execute_spot_triangle(best,core.spot)
                    if result.get("executed"):
                        with core.lock: core.state["spot_entries"]+=1
                        log.warning("LIVE SPOT TRIANGLE EXECUTED | %s | %.2f bps",best.get("path"),best.get("net_bps",0))
                except Exception as e: _record_error("LIVE ORDER: "+str(e)); log.error("LIVE ORDER FAILED: %s",e)
            elif not _live_configured():
                # Paper accounting only when live execution is disabled.
                with core.lock:
                    for o in allx[:core.MAX_ENTRIES]:
                        n=min(o["notional_usdt"],max(.01,core.state["equity"]*core.RISK),core.MAX_NOTIONAL); pnl=n*o["net_bps"]/10000
                        core.state["equity"]+=pnl; core.state["peak"]=max(core.state["peak"],core.state["equity"]); core.state["paper_pnl"]+=pnl
                        core.state["spot_entries"]+=o["engine"]=="SPOT_TRIANGULAR"; core.state["futures_entries"]+=o["engine"]!="SPOT_TRIANGULAR"; core.state["wins"]+=pnl>=0; core.state["losses"]+=pnl<0
            with core.lock:
                core.state["scans"]+=1; core.state["spot_opportunities"]=len(s); core.state["futures_opportunities"]=len(f)+len(ft); core.state["best_spot"]=s[0] if s else None; core.state["best_futures"]=(f+ft)[0] if (f or ft) else None; core.state["opportunities"]=allx[:100]
        except Exception as e:
            _record_error("scan: "+str(e)); log.exception("SCAN FAILED")
        time.sleep(core.SCAN)

def stats_v7():
    out=_original_stats(); out["engine"]="cryptoalpha-v7"; out["live_execution"]=enabled(); out["live_mode_configured"]=_live_configured(); out["live_supported_engine"]="SPOT_TRIANGULAR"; out["live_cooldown_ms"]=LIVE_COOLDOWN_MS
    out["configured_balance_usdt"]=float(os.getenv("SIM_START_EQUITY","30")); out["configured_risk_pct"]=core.RISK*100
    out["configured_risk_budget_usdt"]=round(float(os.getenv("LIVE_STARTING_BALANCE_USDT","30"))*core.RISK,8)
    static_cap=float(os.getenv("MAX_LIVE_NOTIONAL_USDT","0"))
    out["configured_live_notional_cap_usdt"]=round(max(0.0,static_cap),8)
    out["live_sizing_mode"]="BINANCE_FREE_USDT_X_RISK_PCT" if static_cap<=0 else "MIN(BINANCE_FREE_USDT_X_RISK_PCT,STATIC_CAP)"
    out["configured_min_net_bps"]=core.MIN_NET; out["live_circuit_breaker"]=circuit_status(); out["binance_auth"]=auth_status()
    with core.lock:
        out["near_misses"]=list(core.state.get("near_misses",[]))[-50:]; out["rejection_counts"]=dict(core.state.get("rejection_counts",{})); out["error_log"]=list(core.state.get("error_log",[]))[-20:]; out["diagnostic_scans"]=core.state.get("diagnostic_scans",0)
    out["diagnostic_model"]={"triangular_cost_bps":core.cost_bps(3,False),"basis_cost_bps":core.cost_bps(2,True),"futures_triangular_cost_bps":core.cost_bps(3,True),"min_net_bps":core.MIN_NET,"max_net_bps":core.MAX_NET,"stale_ms":core.STALE}
    out["live_note"]="Canonical executable bid/ask model. Basis cost is 2*(fee+slippage)+funding; spot triangle cost is 3*(fee+slippage). Negative/insufficient-net candidates are rejected and recorded as near-misses. Only SPOT_TRIANGULAR is currently live-capable."
    return out

core.stats=stats_v7
core.scan=live_scan

if __name__=="__main__": core.main()
