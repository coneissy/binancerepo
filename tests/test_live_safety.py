import live_order_layer as live


def test_live_execution_is_fail_closed_by_default():
    assert live.LIVE is False
    assert live.ARMED is False
    assert live.enabled() is False


def test_round_down_respects_step_size():
    assert live._round_down(1.239, 0.01) == 1.23
    assert live._round_down(0.0009, 0.001) == 0


def test_circuit_breaker_trips_at_session_loss_limit():
    old_halted = live._live_halted
    old_loss = live._session_loss_usdt
    old_limit = live.MAX_SESSION_LOSS_USDT
    try:
        live._live_halted = False
        live._session_loss_usdt = 0.0
        live.MAX_SESSION_LOSS_USDT = 1.0
        live.record_realized_pnl(-1.0)
        assert live.circuit_status()["halted"] is True
        assert live.circuit_status()["session_loss_usdt"] == 1.0
    finally:
        live._live_halted = old_halted
        live._session_loss_usdt = old_loss
        live.MAX_SESSION_LOSS_USDT = old_limit
