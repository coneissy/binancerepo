"""PolyMorph AI v0.1: deterministic multi-strategy signal engine.

This module is intentionally local and deterministic. It does not place orders,
connect to an exchange, or claim profitability. The existing bot can use it as
a signal-scoring layer while DRY_RUN remains enabled.
"""
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class Signal:
    side: str
    score: float
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PolyMorphConfig:
    min_score: float = 0.70
    ema_fast: int = 9
    ema_slow: int = 21
    rsi_period: int = 14
    breakout_lookback: int = 20


def _ema(values: list[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("not enough values for EMA")
    alpha = 2.0 / (period + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result
    return result


def _rsi(values: list[float], period: int) -> float:
    if len(values) <= period:
        raise ValueError("not enough values for RSI")
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(change, 0.0) for change in changes[-period:]]
    losses = [max(-change, 0.0) for change in changes[-period:]]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    if average_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + average_gain / average_loss))


def evaluate(closes: Iterable[float], config: PolyMorphConfig = PolyMorphConfig()) -> Optional[Signal]:
    """Score trend, momentum, and breakout strategies.

    Returns BUY/SELL only when the weighted score reaches min_score.
    Otherwise returns None. This is a research signal, not financial advice.
    """
    prices = [float(value) for value in closes]
    minimum = max(config.ema_slow, config.rsi_period + 1, config.breakout_lookback + 1)
    if len(prices) < minimum:
        return None

    last = prices[-1]
    fast = _ema(prices, config.ema_fast)
    slow = _ema(prices, config.ema_slow)
    momentum = _rsi(prices, config.rsi_period)
    previous = prices[-config.breakout_lookback - 1:-1]
    high = max(previous)
    low = min(previous)

    buy_votes = 0.0
    sell_votes = 0.0
    buy_reasons: list[str] = []
    sell_reasons: list[str] = []

    if fast > slow:
        buy_votes += 0.40
        buy_reasons.append("EMA trend bullish")
    elif fast < slow:
        sell_votes += 0.40
        sell_reasons.append("EMA trend bearish")

    if momentum >= 55:
        buy_votes += 0.30
        buy_reasons.append(f"RSI {momentum:.1f} supports momentum")
    elif momentum <= 45:
        sell_votes += 0.30
        sell_reasons.append(f"RSI {momentum:.1f} supports momentum")

    if last > high:
        buy_votes += 0.30
        buy_reasons.append("breakout above lookback high")
    elif last < low:
        sell_votes += 0.30
        sell_reasons.append("breakdown below lookback low")

    if buy_votes >= config.min_score and buy_votes > sell_votes:
        return Signal("BUY", round(buy_votes, 4), tuple(buy_reasons))
    if sell_votes >= config.min_score and sell_votes > buy_votes:
        return Signal("SELL", round(sell_votes, 4), tuple(sell_reasons))
    return None
