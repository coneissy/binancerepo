"""Cryptoalpha v7: v6 market engine with a separately gated Binance live spot executor.
LIVE_TRADING is false unless explicitly enabled in Render. Futures/basis and futures-triangle signals remain paper-only.
"""
import os, time, logging
import arbitrage_engine_v6 as v6
from live_order_layer import enabled, execute_spot_triangle

log = logging.getLogger("cryptoalpha-v7")
LIVE_COOLDOWN_MS = max(1000, int(os.getenv("LIVE_ORDER_COOLDOWN_MS", "5000")))
_last_live_ms = 0.0
_original_stats = v6.stats

v6.START = float(os.getenv("SIM_START_EQUITY", "30"))
v6.RISK = min(0.01, max(0.0005, float(os.getenv("ARB_RISK_PCT", "0.01"))))
v6.MIN_NET = max(0.0, float(os.getenv("ARB_MIN_NET_BPS", "3")))


def live_scan():
    global _last_live_ms
    while True:
        try:
            with v6.lock:
                eq = v6.state['equity']
            s = [o for a,b,q in v6.ROUTES for o in [v6.tri(a,b,q,eq)] if o]
            f = [o for sym in v6.FUT_SYMBOLS if sym.endswith('USDT') for o in [v6.basis(sym,eq)] if o]
            ft = [o for a,b,q in v6.FUT_TRI_ROUTES for o in [v6.ftri(a,b,q,eq)] if o]
            s.sort(key=lambda x:x['net_bps'], reverse=True)
            f.sort(key=lambda x:x['net_bps'], reverse=True)
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
                        with v6.lock:
                            v6.state['errors'] += 1
                            v6.state['last_error'] = 'LIVE ORDER: ' + str(e)
                        log.error('LIVE ORDER BLOCKED/FAILED: %s', e)
            elif not enabled():
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
            with v6.lock:
                v6.state['errors'] += 1
                v6.state['last_error'] = 'scan: ' + str(e)
        time.sleep(v6.SCAN)


def stats_v7():
    out = _original_stats()
    out['engine'] = 'cryptoalpha-v7'
    out['live_execution'] = enabled()
    out['live_mode_configured'] = os.getenv('LIVE_TRADING', 'false').lower() in {'1','true','yes','on'}
    out['live_supported_engine'] = 'SPOT_TRIANGULAR'
    out['live_cooldown_ms'] = LIVE_COOLDOWN_MS
    out['configured_balance_usdt'] = float(os.getenv('SIM_START_EQUITY', '30'))
    out['configured_risk_pct'] = v6.RISK * 100
    out['configured_min_net_bps'] = v6.MIN_NET
    out['live_note'] = 'REAL ORDERS REQUIRE LIVE_TRADING=true; credentials are never read from source code.'
    return out

v6.stats = stats_v7
v6.scan = live_scan

if __name__ == '__main__':
    v6.main()
