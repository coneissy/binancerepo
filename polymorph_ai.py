"""PolyMorph AI v0.1 - safe strategy orchestration layer.

This module is intentionally demo/dry-run only. It evaluates several simple,
transparent strategies and produces a consensus decision. It does not place
orders or require API credentials.
"""
from dataclasses import dataclass, asdict
from typing import Optional, Sequence


@dataclass(frozen=True)
class Decision:
    symbol: str
    action: str  # BUY, SELL, HOLD
    confidence: float
    votes: dict
    reason: str


def _ema(values: Sequence[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("not enough prices for EMA")
    alpha = 2 / (period + 1)
    value = float(values[0])
    for price in values[1:]:
        value = alpha * float(price) + (1 - alpha) * value
    return value


def _momentum(values: Sequence[float], lookback: int = 5) -> float:
    if len(values) <= lookback:
        raise ValueError("not enough prices for momentum")
    return float(values[-1]) / float(values[-1 - lookback]) - 1


def trend_strategy(prices: Sequence[float]) -> Optional[str]:
    if len(prices) < 30:
        return None
    fast, slow = _ema(prices, 9), _ema(prices, 21)
    if fast > slow and prices[-1] > fast:
        return "BUY"
    if fast < slow and prices[-1] < fast:
        return "SELL"
    return "HOLD"


def momentum_strategy(prices: Sequence[float]) -> Optional[str]:
    change = _momentum(prices, 5)
    if change > 0.001:
        return "BUY"
    if change < -0.001:
        return "SELL"
    return "HOLD"


def mean_reversion_strategy(prices: Sequence[float]) -> Optional[str]:
    if len(prices) < 20:
        return None
    window = [float(x) for x in prices[-20:]]
    mean = sum(window) / len(window)
    deviation = (window[-1] - mean) / mean if mean else 0
    if deviation < -0.002:
        return "BUY"
    if deviation > 0.002:
        return "SELL"
    return "HOLD"


def decide(symbol: str, prices: Sequence[float]) -> Decision:
    if len(prices) < 30:
        return Decision(symbol, "HOLD", 0.0, {}, "waiting for at least 30 prices")
    votes = {
        "trend": trend_strategy(prices),
        "momentum": momentum_strategy(prices),
        "mean_reversion": mean_reversion_strategy(prices),
    }
    counts = {action: sum(v == action for v in votes.values()) for action in ("BUY", "SELL", "HOLD")}
    action = max(counts, key=counts.get)
    confidence = counts[action] / len(votes)
    if action == "HOLD" or confidence < 2 / 3:
        action = "HOLD"
    reason = "; ".join(f"{name}={vote}" for name, vote in votes.items())
    return Decision(symbol, action, confidence, votes, reason)


def decision_dict(symbol: str, prices: Sequence[float]) -> dict:
    """JSON-friendly result for a future Telegram/API integration."""
    return asdict(decide(symbol, prices))


if __name__ == "__main__":
    demo_prices = [100 + i * 0.15 for i in range(40)]
    print(decision_dict("BTCUSDT", demo_prices))
