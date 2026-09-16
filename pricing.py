"""Dragon canonical arbitrage pricing.

All opportunity math lives here. Prices are executable top-of-book prices;
slippage is an additional depth/impact allowance and must not represent the
spread already paid by crossing bid/ask.
"""
from dataclasses import dataclass
from typing import Optional

BPS = 10_000.0

@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    ts_ms: float = 0.0


def cost_bps(legs: int, fee_bps: float, slippage_bps: float,
             funding_cost_bps: float = 0.0) -> float:
    if legs < 1:
        raise ValueError("legs must be >= 1")
    return legs * (float(fee_bps) + float(slippage_bps)) + float(funding_cost_bps)


def net_bps(gross_bps: float, legs: int, fee_bps: float,
            slippage_bps: float, funding_cost_bps: float = 0.0) -> float:
    return float(gross_bps) - cost_bps(legs, fee_bps, slippage_bps, funding_cost_bps)


def funding_cost_bps(funding_rate_bps: float, holding_intervals: float,
                     direction_sign: int) -> float:
    """Convert signed funding into a cost.

    direction_sign is +1 when the position pays the quoted funding rate and
    -1 when it receives it. A negative result is funding income.
    """
    if direction_sign not in (-1, 1):
        raise ValueError("direction_sign must be -1 or 1")
    return float(funding_rate_bps) * float(holding_intervals) * direction_sign


def triangular_gross_bps(a_q: Quote, a_b: Quote, b_q: Quote):
    """Return executable gross edge for both directions.

    Forward: Q -> A (ask A/Q), A -> B (bid A/B), B -> Q (bid B/Q).
    Reverse: Q -> B (ask B/Q), B -> A (ask A/B), A -> Q (bid A/Q).
    """
    if min(a_q.ask, a_b.bid, b_q.bid, b_q.ask, a_b.ask, a_q.bid) <= 0:
        raise ValueError("invalid quote")
    forward = (1.0 / a_q.ask) * a_b.bid * b_q.bid
    reverse = (1.0 / b_q.ask) * (1.0 / a_b.ask) * a_q.bid
    forward_bps = (forward - 1.0) * BPS
    reverse_bps = (reverse - 1.0) * BPS
    if forward_bps >= reverse_bps:
        return forward_bps, "FORWARD"
    return reverse_bps, "REVERSE"


def basis_gross_bps(spot: Quote, futures: Quote):
    """Return executable basis edge and direction without abs()."""
    buy_spot_sell_futures = (futures.bid / spot.ask - 1.0) * BPS
    sell_spot_buy_futures = (spot.bid / futures.ask - 1.0) * BPS
    if buy_spot_sell_futures >= sell_spot_buy_futures:
        return buy_spot_sell_futures, "BUY_SPOT_SELL_FUTURES"
    return sell_spot_buy_futures, "SELL_SPOT_BUY_FUTURES"


def max_staleness_ms(quotes, now_ms: float) -> float:
    """Oldest leg governs freshness."""
    ts = [q.ts_ms for q in quotes if q is not None]
    if len(ts) != len(quotes) or not ts:
        return float("inf")
    return max(0.0, float(now_ms) - min(ts))


def fresh(quotes, now_ms: float, stale_ms: float) -> bool:
    return max_staleness_ms(quotes, now_ms) <= float(stale_ms)


def target_notional_usdt(equity: float, risk_pct: float, cap_usdt: float) -> float:
    return min(max(0.01, float(equity) * float(risk_pct)), max(0.01, float(cap_usdt)))
