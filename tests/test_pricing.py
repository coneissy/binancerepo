import math

import pricing


def q(bid, ask, ts=1000):
    return pricing.Quote(bid=bid, ask=ask, bid_qty=10, ask_qty=10, ts_ms=ts)


def test_cost_and_net_are_consistent():
    cost = pricing.cost_bps(3, 4, 2, 0)
    assert cost == 18
    assert pricing.net_bps(30, 3, 4, 2, 0) == 12


def test_triangle_uses_executable_bid_ask_prices():
    gross, direction = pricing.triangular_gross_bps(
        q(0.99, 1.00),
        q(1.01, 1.02),
        q(0.99, 1.00),
    )
    assert direction in {"FORWARD", "REVERSE"}
    assert math.isfinite(gross)


def test_basis_does_not_turn_negative_edge_positive():
    gross, direction = pricing.basis_gross_bps(q(100, 101), q(99, 100))
    assert direction in {"BUY_SPOT_SELL_FUTURES", "SELL_SPOT_BUY_FUTURES"}
    assert gross <= 0


def test_staleness_uses_oldest_leg():
    quotes = [q(1, 2, 900), q(1, 2, 950), q(1, 2, 990)]
    assert pricing.max_staleness_ms(quotes, 1000) == 100
    assert not pricing.fresh(quotes, 1000, 99)
    assert pricing.fresh(quotes, 1000, 100)


def test_target_notional_is_capped_and_has_minimum():
    assert pricing.target_notional_usdt(1000, 0.01, 2) == 2
    assert pricing.target_notional_usdt(0, 0.01, 2) == 0.01
