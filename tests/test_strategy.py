from bot import atr_from_closes, ema, rsi, round_qty, signal


def test_ema_is_numeric():
    assert ema([1, 2, 3, 4, 5], 3) > 3


def test_rsi_uptrend():
    assert rsi(list(range(1, 30))) == 100.0


def test_rsi_downtrend():
    assert rsi(list(range(30, 1, -1))) == 0.0


def test_round_qty():
    assert round_qty(1.239, 0.01) == 1.23


def test_atr_proxy_is_positive():
    assert atr_from_closes([100 + i * 0.2 for i in range(30)]) > 0


def test_signal_returns_valid_value():
    xs = [100 + i * 0.2 for i in range(120)]
    assert signal(xs) in ('BUY', 'SELL', None)
