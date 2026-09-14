'''Wrapper adding Binance USDT-M futures triangular arbitrage to v3. LIVE OFF.'''
import time
import arbitrage_engine_v3 as base

# Subscribe to the cross futures contracts required by the same route universe.
for a, b, q in base.ROUTES:
    base.FUT_SYMBOLS.append(a+b)
base.FUT_SYMBOLS = sorted(set(base.FUT_SYMBOLS))
base.FUT_SHARDS = base.shards(base.FUT_SYMBOLS)


def futures_triangular(a, b, q, eq):
    p_aq = base.px(base.fut, a+q)
    p_ab = base.px(base.fut, a+b)
    p_bq = base.px(base.fut, b+q)
    if not all((p_aq, p_ab, p_bq)):
        return None
    ratio = p_ab/p_aq*p_bq
    gross1 = (ratio-1)*10000
    gross2 = (1/ratio-1)*10000
    gross = max(gross1, gross2)
    net = gross - 3*(base.FEE+base.SLIP) - base.FUNDING
    if net < base.MIN_NET or net > base.MAX_NET:
        return None
    direction = f'USDT->{a}->{b}->USDT' if gross1 >= gross2 else f'USDT->{b}->{a}->USDT'
    if not base.throttle('FT:'+a+b+q, net, 75):
        return None
    return {
        'engine':'FUTURES_TRIANGULAR','type':'FUTURES_TRIANGULAR',
        'path':direction,'symbol':a+b+q,'gross_bps':gross,'net_bps':net,
        'notional_usdt':min(max(.01,eq*base.RISK),base.MAX_NOTIONAL),'paper_only':True
    }


def scan_loop():
    while True:
        try:
            with base.lock:
                eq=base.state['equity']
            spot=[x for a,b,q in base.ROUTES for x in [base.triangular(a,b,q,eq)] if x]
            basis=[x for s in base.FUT_SYMBOLS if s.endswith('USDT') for x in [base.futures_cross(s,eq)] if x]
            ftri=[x for a,b,q in base.ROUTES for x in [futures_triangular(a,b,q,eq)] if x]
            spot.sort(key=lambda x:x['net_bps'],reverse=True)
            basis.sort(key=lambda x:x['net_bps'],reverse=True)
            ftri.sort(key=lambda x:x['net_bps'],reverse=True)
            futures=basis+ftri
            futures.sort(key=lambda x:x['net_bps'],reverse=True)
            all_opps=spot+futures
            base.capture(all_opps)
            with base.lock:
                base.state['scans']+=1
                base.state['spot_opportunities']=len(spot)
                base.state['futures_opportunities']=len(futures)
                base.state['best_spot']=spot[0] if spot else None
                base.state['best_futures']=futures[0] if futures else None
                base.state['opportunities']=all_opps[:100]
        except Exception as e:
            with base.lock:
                base.state['errors']+=1
                base.state['last_error']=str(e)
        time.sleep(base.SCAN)

base.scan_loop = scan_loop
base.main()
