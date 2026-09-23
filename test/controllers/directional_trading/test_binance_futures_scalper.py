from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import DirectionalTradingControllerBase
from controllers.directional_trading.binance_futures_scalper import (
    BinanceFuturesScalper,
    BinanceFuturesScalperConfig,
)


class TestBinanceFuturesScalper(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        self.config = BinanceFuturesScalperConfig(
            id="test_scalper",
            connector_name="binance_perpetual",
            trading_pair="BTC-USDT",
            total_amount_quote=Decimal("100"),
            leverage=3,
        )
        self.provider = MagicMock(spec=MarketDataProvider)
        self.actions_queue = AsyncMock()
        self.controller = BinanceFuturesScalper(
            config=self.config,
            market_data_provider=self.provider,
            actions_queue=self.actions_queue,
        )

    def test_defaults_are_scalping_safe(self):
        self.assertEqual(self.config.connector_name, "binance_perpetual")
        self.assertEqual(self.config.interval, "1m")
        self.assertEqual(self.config.leverage, 15)
        self.assertEqual(self.config.stop_loss, Decimal("0.004"))
        self.assertEqual(self.config.take_profit, Decimal("0.006"))
        self.assertEqual(self.config.time_limit, 900)

    async def test_does_not_signal_without_enough_history(self):
        columns = ["open", "high", "low", "close", "volume"]
        df = pd.DataFrame(
            [[100, 101, 99, 100, 1000]] * 10,
            columns=columns,
        )
        self.provider.get_candles_df.return_value = df

        await self.controller.update_processed_data()

        self.assertEqual(self.controller.processed_data["signal"], 0)
        self.assertIs(self.controller.processed_data["features"], df)

    def test_inherits_directional_executor_pipeline(self):
        self.assertIsInstance(self.controller, DirectionalTradingControllerBase)
        self.assertEqual(self.controller.max_records, 65)
