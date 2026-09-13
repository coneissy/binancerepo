from hft_scalper import atr_pct, ema, is_meme, rsi


def test_ema_is_numeric():
    assert ema([1, 2, 3, 4, 5], 3) > 3


def test_rsi_uptrend():
    assert rsi(list(range(1, 30))) == 100.0


def test_rsi_downtrend():
    assert rsi(list(range(30, 1, -1))) == 0.0


def test_atr_is_positive():
    ks = [[0, 0, 100.2, 99.8, 100.0, 1]] * 20
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
