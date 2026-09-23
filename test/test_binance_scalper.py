from scripts.binance_scalper import fee_aware_edge, pct_change, realized_volatility


def test_pct_change():
    assert pct_change(100.0, 101.0) == 0.01


def test_fee_aware_edge_requires_positive_net_edge():
    assert fee_aware_edge(0.0030, 0.0010, 0.0002, 0.0002) > 0
    assert fee_aware_edge(0.0020, 0.0010, 0.0002, 0.0002) < 0


def test_realized_volatility_is_zero_for_flat_prices():
    assert realized_volatility([100.0, 100.0, 100.0, 100.0]) == 0.0


def test_realized_volatility_is_positive_for_moving_prices():
    assert realized_volatility([100.0, 101.0, 100.5, 102.0]) > 0.0
