"""Cryptoalpha v7: gated Binance live spot executor with transparent opportunity diagnostics."""
import os, time, logging
import arbitrage_engine_v6 as v6
from live_order_layer import enabled, execute_spot_triangle, circuit_status

log = logging.getLogger("cryptoalpha-v7")
LIVE_COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))
_last_live_ms = 0.0
_original_stats = v6.stats

v6.START = float(os.getenv("SIM_START_EQUITY", "30"))
v6.RISK = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
v6.MIN_NET = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))

# Diagnostic state is deliberately separate from the qualified opportunity list.
# This makes a zero-opportunity dashboard actionable instead of hiding the reason.
v6.state.setdefault("near_misses", [])
v6.state.setdefault("rejection_counts", {})
v6.state.setdefault("error_log", [])
v6.state.setdefault("diagnostic_scans", 0)


def _live_configured():
    return os.getenv('LIVE_TRADING', 'false').lower() in {'1','true','yes','on'}


def _record_error(message):
    with v6.lock:
        v6.state['errors'] += 1
        v6.state['last_error'] = message
        v6.state['error_log'].append({'ts': time.time(), 'message': message})
        v6.state['error_log'] = v6.state['error_log'][-20:]


def _record_rejection(engine, symbol, reason, gross=0.0, net=0.0, extra=None):
    item = {
        'engine': engine, 'symbol': symbol, 'reason': reason,
        'gross_bps': round(float(gross), 4), 'net_bps': round(float(net), 4),
        'observed_at': time.time()
    }
    if extra:
        item.update(extra)
    with v6.lock:
        v6.state['near_misses'].append(item)
        v6.state['near_misses'] = v6.state['near_misses'][-100:]
        v6.state['rejection_counts'][reason] = v6.state['rejection_counts'].get(reason, 0) + 1


def _quote(store, symbol):
    x = store.get(symbol)
    if not x:
        return None, 'missing_quote'
    age = time.monotonic() * 1000 - x[4]
    if age > v6.STALE:
        return None, 'stale_quote'
    return x, ''


def _tri_diag(a, b, q, eq, venue='spot', engine='SPOT_TRIANGULAR'):
    store = v6.spot if venue == 'spot' else v6.fut
    pa,ra = _quote(store, a+q)
    pab,rb = _quote(store, a+b)
    pb,rc = _quote(store, b+q)
    if not all((pa,pab,pb)):
        reason = ra or rb or rc or 'missing_quote'
        _record_rejection(engine, a+b+q, reason)
        return None
    # Evaluate both directions using executable sides, not mid-prices.
    # USDT->A uses ask(A/USDT), A->B uses bid(A/B), B->USDT uses bid(B/USDT).
    forward = (1.0/pa[1]) * pab[0] * pb[0]
    reverse = (1.0/pb[1]) * (1.0/pab[1]) * pa[0]
    g1 = (forward - 1.0) * 10000.0
    g2 = (reverse - 1.0) * 10000.0
    gross = max(g1, g2)
    net = gross - 3.0 * (v6.FEE + v6.SLIP) - (v6.FUND if venue == 'fut' else 0.0)
    path = f'USDT->{a}->{b}->USDT' if g1 >= g2 else f'USDT->{b}->{a}->USDT'
    symbol = a+b+q
    if gross <= 0:
        _record_rejection(engine, symbol, 'negative_gross_edge', gross, net, {'path': path})
        return None
    if net < v6.MIN_NET:
        _record_rejection(engine, symbol, 'below_min_net', gross, net, {'path': path, 'min_net_bps': v6.MIN_NET})
        return None
    if net > v6.MAX_NET:
        _record_rejection(engine, symbol, 'above_max_net_safety', gross, net, {'path': path, 'max_net_bps': v6.MAX_NET})
        return None
    return {
        'engine': engine, 'type': engine, 'path': path, 'symbol': symbol,
        'gross_bps': gross, 'net_bps': net,
        'notional_usdt': min(max(.01, eq*v6.RISK), v6.MAX_NOTIONAL),
        'paper_only': True,
        'diagnostic': {'fees_bps': 3*v6.FEE, 'slippage_bps': 3*v6.SLIP, 'funding_bps': v6.FUND if venue == 'fut' else 0.0}
    }


def _basis_diag(s, eq):
    sp,rs = _quote(v6.spot, s)
    fu,rf = _quote(v6.fut, s)
    if not sp or not fu:
        _record_rejection('SPOT_FUTURES_BASIS', s, rs or rf or 'missing_quote')
        return None
    # Executable cross-market edges: sell at futures bid vs buy spot ask, or
    # sell spot bid vs buy futures ask. This avoids mid-price false positives.
    long_spot = (fu[0] / sp[1] - 1.0) * 10000.0
    long_fut = (sp[0] / fu[1] - 1.0) * 10000.0
    gross = max(long_spot, long_fut)
    net = gross - 2.0*(v6.FEE + v6.SLIP) - v6.FUND
    direction = 'BUY_SPOT_SELL_FUTURES' if long_spot >= long_fut else 'SELL_SPOT_BUY_FUTURES'
    if gross <= 0:
        _record_rejection('SPOT_FUTURES_BASIS', s, 'negative_gross_edge', gross, net, {'direction': direction})
        return None
    if net < v6.MIN_NET:
        _record_rejection('SPOT_FUTURES_BASIS', s, 'below_min_net', gross, net, {'direction': direction, 'min_net_bps': v6.MIN_NET})
        return None
    if net > v6.MAX_NET:
        _record_rejection('SPOT_FUTURES_BASIS', s, 'above_max_net_safety', gross, net, {'direction': direction, 'max_net_bps': v6.MAX_NET})
        return None
    return {'engine':'SPOT_FUTURES_BASIS','type':'SPOT_FUTURES_CROSS','symbol':s,
            'direction':direction,'basis_bps':gross,'gross_bps':gross,'net_bps':net,
            'notional_usdt':min(max(.01,eq*v6.RISK),v6.MAX_NOTIONAL),'paper_only':True,
            'diagnostic':{'fees_bps':2*v6.FEE,'slippage_bps':2*v6.SLIP,'funding_bps':v6.FUND}}


def _run_diagnostics(eq):
    # Reset per-scan diagnostics while retaining the counters.
    with v6.lock:
        v6.state['near_misses'] = []
        v6.state['diagnostic_scans'] += 1
    s = [_tri_diag(a,b,q,eq,'spot','SPOT_TRIANGULAR') for a,b,q in v6.ROUTES]
    f = [_basis_diag(sym,eq) for sym in v6.FUT_SYMBOLS if sym.endswith('USDT')]
    ft = [_tri_diag(a,b,q,eq,'fut','FUTURES_TRIANGULAR') for a,b,q in v6.FUT_TRI_ROUTES]
    s = [x for x in s if x]; f = [x for x in f if x]; ft = [x for x in ft if x]
    s.sort(key=lambda x:x['net_bps'], reverse=True)
    f.sort(key=lambda x:x['net_bps'], reverse=True)
    ft.sort(key=lambda x:x['net_bps'], reverse=True)
    return s,f,ft


def live_scan():
    global _last_live_ms
    while True:
        try:
            with v6.lock:
                eq = v6.state['equity']
            s, f, ft = _run_diagnostics(eq)
            allx = s + f + ft
            now = time.monotonic() * 1000
            if enabled() and s and now - _last_live_ms >= LIVE_COOLDOWN_MS:
                best = s[0]
                if best.get('net_bps', 0) >= v6.MIN_NET:
                    _last_live_ms = now
                    try:
                        result = execute_spot_triangle(best, v6.spot)
                        if result.get('executed'):
                            with v6.lock:
                                v6.state['spot_entries'] += 1
                            log.warning('LIVE SPOT TRIANGLE EXECUTED | %s | %.2f bps', best.get('path'), best.get('net_bps', 0))
                    except Exception as e:
                        _record_error('LIVE ORDER: ' + str(e))
                        log.error('LIVE ORDER BLOCKED/FAILED: %s', e)
            elif not _live_configured():
                with v6.lock:
                    for o in allx[:v6.MAX_ENTRIES]:
                        n = min(o['notional_usdt'], max(.01, v6.state['equity']*v6.RISK), v6.MAX_NOTIONAL)
                        pnl = n * o['net_bps'] / 10000
                        v6.state['equity'] += pnl
                        v6.state['peak'] = max(v6.state['peak'], v6.state['equity'])
                        v6.state['paper_pnl'] += pnl
                        v6.state['spot_entries'] += o['engine'] == 'SPOT_TRIANGULAR'
                        v6.state['futures_entries'] += o['engine'] != 'SPOT_TRIANGULAR'
                        v6.state['wins'] += pnl >= 0
                        v6.state['losses'] += pnl < 0
            with v6.lock:
                v6.state['scans'] += 1
                v6.state['spot_opportunities'] = len(s)
                v6.state['futures_opportunities'] = len(f) + len(ft)
                v6.state['best_spot'] = s[0] if s else None
                v6.state['best_futures'] = (f + ft)[0] if (f or ft) else None
                v6.state['opportunities'] = allx[:100]
        except Exception as e:
            _record_error('scan: ' + str(e))
            log.exception('SCAN FAILED')
        time.sleep(v6.SCAN)


def stats_v7():
    out = _original_stats()
    out['engine'] = 'cryptoalpha-v7'
    out['live_execution'] = enabled()
    out['live_mode_configured'] = _live_configured()
    out['live_supported_engine'] = 'SPOT_TRIANGULAR'
    out['live_cooldown_ms'] = LIVE_COOLDOWN_MS
    out['configured_balance_usdt'] = float(os.getenv('SIM_START_EQUITY', '30'))
    out['configured_risk_pct'] = v6.RISK * 100
    out['configured_risk_budget_usdt'] = round(float(os.getenv('LIVE_STARTING_BALANCE_USDT', '30')) * v6.RISK, 8)
    out['configured_live_notional_cap_usdt'] = round(min(max(0.0, float(os.getenv('MAX_LIVE_NOTIONAL_USDT', str(out['configured_risk_budget_usdt'])))), out['configured_risk_budget_usdt']), 8)
    out['configured_min_net_bps'] = v6.MIN_NET
    out['live_circuit_breaker'] = circuit_status()
    with v6.lock:
        out['near_misses'] = list(v6.state.get('near_misses', []))[-50:]
        out['rejection_counts'] = dict(v6.state.get('rejection_counts', {}))
        out['error_log'] = list(v6.state.get('error_log', []))[-20:]
        out['diagnostic_scans'] = v6.state.get('diagnostic_scans', 0)
    out['diagnostic_model'] = {
        'triangular_cost_bps': 3*(v6.FEE+v6.SLIP),
        'basis_cost_bps': 2*(v6.FEE+v6.SLIP)+v6.FUND,
        'min_net_bps': v6.MIN_NET,
        'max_net_bps': v6.MAX_NET,
        'stale_ms': v6.STALE
    }
    out['live_note'] = 'REAL ORDERS require LIVE_TRADING=true; safety circuit remains fail-closed; diagnostics never bypass execution safety.'
    return out

v6.stats = stats_v7
v6.scan = live_scan

if __name__ == '__main__':
    v6.main()
