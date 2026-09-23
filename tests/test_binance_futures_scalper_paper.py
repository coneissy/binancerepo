import pandas as pd

from paper.binance_futures_scalper_paper import Config, entry, exit_trade, metrics


def test_long_exit_has_fee_and_net_pnl():
    cfg = Config(quantity_quote=100, fee_bps=5, slippage_bps=0)
    opened = pd.Timestamp("2026-01-01T00:00:00Z")
    pos = entry(100.0, opened, 1, cfg)
    trade = exit_trade(pos, 101.0, opened + pd.Timedelta(minutes=1), "TAKE_PROFIT", cfg)
    assert trade.gross_pnl == 1.0
    assert trade.fees == 0.1
    assert trade.net_pnl == 0.9
    assert trade.reason == "TAKE_PROFIT"


def test_short_pnl_is_directional():
    cfg = Config(quantity_quote=100, fee_bps=0, slippage_bps=0)
    opened = pd.Timestamp("2026-01-01T00:00:00Z")
    pos = entry(100.0, opened, -1, cfg)
    trade = exit_trade(pos, 99.0, opened + pd.Timedelta(minutes=1), "STOP_OR_TARGET", cfg)
    assert trade.gross_pnl == 1.0
    assert trade.net_pnl == 1.0


def test_metrics_are_realized_trade_metrics():
    cfg = Config(quantity_quote=100, fee_bps=0, slippage_bps=0)
    opened = pd.Timestamp("2026-01-01T00:00:00Z")
    p1 = entry(100.0, opened, 1, cfg)
    p2 = entry(100.0, opened, 1, cfg)
    t1 = exit_trade(p1, 101.0, opened + pd.Timedelta(minutes=1), "TP", cfg)
    t2 = exit_trade(p2, 99.0, opened + pd.Timedelta(minutes=2), "SL", cfg)
    m = metrics([t1, t2])
    assert m["trades"] == 2
    assert m["wins"] == 1
    assert m["losses"] == 1
    assert m["win_rate"] == 0.5
    assert m["net_pnl"] == 0.0
    assert m["profit_factor"] == 1.0
