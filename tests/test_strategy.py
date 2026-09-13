import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hft_scalper import atr_pct, ema, is_meme, rsi, sigmoid


def test_ema_is_numeric():
    assert ema([1, 2, 3, 4, 5], 3) > 3


def test_rsi_uptrend():
    assert rsi(list(range(1, 30))) == 100.0


def test_rsi_downtrend():
    assert rsi(list(range(30, 1, -1))) == 0.0


def test_atr_is_positive():
    ks = [[0, 0, 100.2, 99.8, 100.0, 1] for _ in range(20)]
    for i, k in enumerate(ks):
        k[2] = 100.2 + i * 0.1
        k[3] = 99.8 + i * 0.1
        k[4] = 100.0 + i * 0.1
    assert atr_pct(ks) > 0


def test_known_meme_symbol():
    assert is_meme("DOGEUSDT")
    assert is_meme("1000PEPEUSDT")


def test_non_meme_symbol():
    assert not is_meme("BTCUSDT")


def test_sigmoid_is_bounded():
    assert 0 < sigmoid(-20) < 0.5
    assert 0.5 < sigmoid(20) < 1
    assert sigmoid(0) == 0.5
