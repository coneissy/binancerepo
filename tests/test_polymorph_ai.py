from polymorph_ai import PolyMorphConfig, Signal, evaluate


def test_insufficient_data_returns_none():
    assert evaluate([100.0] * 10) is None


def test_flat_market_returns_none():
    prices = [100.0] * 60
    assert evaluate(prices) is None


def test_signal_is_bounded_and_valid():
    prices = [100 + i * 0.5 for i in range(80)]
    signal = evaluate(prices, PolyMorphConfig(min_score=0.70))
    assert isinstance(signal, Signal)
    assert signal.side in {"BUY", "SELL"}
    assert 0.70 <= signal.score <= 1.0
    assert signal.reasons
