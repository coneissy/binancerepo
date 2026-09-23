import logging
import math
import os
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Deque, Dict, List, Optional

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType, TradeType
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase, StrategyV2Base


def pct_change(old: float, new: float) -> float:
    if old <= 0:
        return 0.0
    return (new / old) - 1.0


def realized_volatility(prices: List[float]) -> float:
    if len(prices) < 3:
        return 0.0
    returns = [pct_change(a, b) for a, b in zip(prices, prices[1:]) if a > 0 and b > 0]
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(variance)


def fee_aware_edge(expected_move: float, fee_rate: float, slippage_rate: float, safety_buffer: float) -> float:
    return expected_move - (2.0 * fee_rate) - slippage_rate - safety_buffer


@dataclass
class Position:
    amount: Decimal
    entry_price: float
    opened_at: float


class BinanceScalperConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []

    # Safe default: paper trading. Change explicitly after validation.
    exchange: str = Field("binance_paper_trade")
    trading_pair: str = Field("ETH-USDT")

    # Signal
    sample_seconds: int = Field(1)
    lookback_samples: int = Field(8)
    min_momentum_pct: float = Field(0.00035)
    max_volatility_pct: float = Field(0.00150)
    min_expected_move_pct: float = Field(0.00090)

    # Cost model. Override with the account's actual Binance fee tier.
    fee_rate_pct: float = Field(0.00100)
    slippage_pct: float = Field(0.00020)
    safety_buffer_pct: float = Field(0.00020)

    # Risk / exits
    order_amount: Decimal = Field(0.01)
    take_profit_pct: float = Field(0.00250)
    stop_loss_pct: float = Field(0.00120)
    max_hold_seconds: int = Field(45)
    cooldown_seconds: int = Field(10)
    max_daily_loss_pct: float = Field(0.02)
    max_consecutive_losses: int = Field(3)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


class BinanceScalper(StrategyV2Base):
    """
    Fast, spot-oriented Binance scalper.

    Signal:
      short-term momentum + volatility guard + fee/slippage-aware expected edge.

    Execution:
      market entry, then market exit on TP, SL, time limit, or signal failure.

    The default exchange is Binance paper trade. This strategy deliberately does
    not contain DEX or cross-exchange arbitrage logic.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BinanceScalperConfig):
        super().__init__(connectors, config)
        self.config = config
        self.prices: Deque[float] = deque(maxlen=max(config.lookback_samples + 2, 20))
        self.position: Optional[Position] = None
        self.last_sample_timestamp = 0.0
        self.cooldown_until = 0.0
        self.realized_pnl = 0.0
        self.daily_loss_limit = -abs(config.max_daily_loss_pct)
        self.consecutive_losses = 0
        self.entry_order_pending = False
        self.exit_order_pending = False

    def on_tick(self):
        now = self.current_timestamp
        if now < self.last_sample_timestamp + self.config.sample_seconds:
            return

        connector = self.connectors[self.config.exchange]
        price = float(connector.get_price_by_type(self.config.trading_pair, PriceType.MidPrice))
        if price <= 0:
            return

        self.prices.append(price)
        self.last_sample_timestamp = now

        if self.position is not None:
            self.manage_position(price, now)
            return

        if self.entry_order_pending or now < self.cooldown_until:
            return

        if self.realized_pnl <= self.daily_loss_limit:
            return

        if self.consecutive_losses >= self.config.max_consecutive_losses:
            return

        if len(self.prices) < self.config.lookback_samples + 1:
            return

        self.try_entry(price, now)

    def try_entry(self, price: float, now: float):
        anchor = self.prices[-(self.config.lookback_samples + 1)]
        momentum = pct_change(anchor, price)
        volatility = realized_volatility(list(self.prices)[-self.config.lookback_samples:])
        expected_move = max(momentum, 0.0)
        net_edge = fee_aware_edge(
            expected_move,
            self.config.fee_rate_pct,
            self.config.slippage_pct,
            self.config.safety_buffer_pct,
        )

        if momentum < self.config.min_momentum_pct:
            return
        if volatility > self.config.max_volatility_pct:
            return
        if expected_move < self.config.min_expected_move_pct:
            return
        if net_edge <= 0:
            return

        self.entry_order_pending = True
        self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order_amount,
            order_type=OrderType.MARKET,
        )

        self.log_with_clock(
            logging.INFO,
            f"SCALP ENTRY signal {self.config.trading_pair} momentum={momentum:.4%} "
            f"volatility={volatility:.4%} expected_edge={net_edge:.4%}",
        )

    def manage_position(self, price: float, now: float):
        if self.exit_order_pending or self.position is None:
            return

        move = pct_change(self.position.entry_price, price)
        age = now - self.position.opened_at

        if move >= self.config.take_profit_pct:
            self.exit_position("TAKE_PROFIT")
        elif move <= -self.config.stop_loss_pct:
            self.exit_position("STOP_LOSS")
        elif age >= self.config.max_hold_seconds:
            self.exit_position("TIME_EXIT")

    def exit_position(self, reason: str):
        if self.position is None:
            return

        self.exit_order_pending = True
        self.sell(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.position.amount,
            order_type=OrderType.MARKET,
        )
        self.log_with_clock(logging.INFO, f"SCALP EXIT requested reason={reason}")

    def did_fill_order(self, event: OrderFilledEvent):
        if event.trading_pair != self.config.trading_pair:
            return

        price = float(event.price)
        amount = Decimal(event.amount)

        if event.trade_type == TradeType.BUY and self.position is None:
            self.position = Position(
                amount=amount,
                entry_price=price,
                opened_at=self.current_timestamp,
            )
            self.entry_order_pending = False
            self.log_with_clock(logging.INFO, f"SCALP LONG opened amount={amount} price={price:.8f}")
            return

        if event.trade_type == TradeType.SELL and self.position is not None:
            gross_return = pct_change(self.position.entry_price, price)
            self.realized_pnl += gross_return
            if gross_return < 0:
                self.consecutive_losses += 1
            else:
                self.consecutive_losses = 0

            self.cooldown_until = self.current_timestamp + self.config.cooldown_seconds
            self.log_with_clock(
                logging.INFO,
                f"SCALP CLOSED return={gross_return:.4%} cumulative={self.realized_pnl:.4%}",
            )
            self.position = None
            self.exit_order_pending = False

    def format_status(self) -> str:
        position = "FLAT"
        if self.position is not None:
            position = f"LONG {self.position.amount} @ {self.position.entry_price:.8f}"

        return (
            f"Binance Scalper | {self.config.trading_pair} | {position}\n"
            f"Realized P&L (gross): {self.realized_pnl:.4%} | "
            f"Consecutive losses: {self.consecutive_losses}"
        )
