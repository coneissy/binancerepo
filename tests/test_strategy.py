from bot import atr_from_closes, ema, rsi, round_down, round_qty, risk_allowed, signal


def test_ema_is_numeric():
    assert ema([1, 2, 3, 4, 5], 3) > 3


def test_rsi_uptrend():
    assert rsi(list(range(1, 30))) == 100.0


def test_rsi_downtrend():
    assert rsi(list(range(30, 1, -1))) == 0.0


def test_round_qty():
    assert round_qty(1.239, 0.01) == 1.23


def test_round_down_price():
    assert round_down(123.456, 0.01) == 123.45


def test_atr_proxy_is_positive():
    assert atr_from_closes([100 + i * 0.2 for i in range(30)]) > 0


def test_signal_returns_valid_value():
    xs = [100 + i * 0.2 for i in range(120)]
    assert signal(xs) in ('BUY', 'SELL', None)


def test_risk_blocks_pause():
    state = {'paused': True, 'kill_switch': False, 'trades': 0, 'realized_pnl': 0, 'day_start_equity': 1000, 'last_trade': 0}
    allowed, reason = risk_allowed(state)
    assert not allowed and reason == 'paused'


def test_risk_blocks_kill_switch():
    state = {'paused': False, 'kill_switch': True, 'trades': 0, 'realized_pnl': 0, 'day_start_equity': 1000, 'last_trade': 0}
    allowed, reason = risk_allowed(state)
    assert not allowed and reason == 'kill switch active'


def test_daily_loss_is_percent_of_starting_equity():
    state = {'paused': False, 'kill_switch': False, 'trades': 0, 'realized_pnl': -10, 'day_start_equity': 1000, 'last_trade': 0}
    allowed, _ = risk_allowed(state)
    assert not allowed
