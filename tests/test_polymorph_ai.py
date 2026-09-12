from polymorph_ai import decide


def test_short_history_holds():
    result = decide("BTCUSDT", [100.0] * 10)
    assert result.action == "HOLD"
    assert result.confidence == 0.0


def test_uptrend_is_not_sold():
    prices = [100 + i * 0.2 for i in range(40)]
    result = decide("BTCUSDT", prices)
    assert result.action in {"BUY", "HOLD"}
    assert result.votes["trend"] == "BUY"


def test_downtrend_is_not_bought_by_trend():
    prices = [110 - i * 0.2 for i in range(40)]
    result = decide("BTCUSDT", prices)
    assert result.action in {"SELL", "HOLD"}
    assert result.votes["trend"] == "SELL"
