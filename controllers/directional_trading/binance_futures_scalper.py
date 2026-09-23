from decimal import Decimal\nfrom typing import List

import pandas_ta as ta  # noqa: F401
from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)


class BinanceFuturesScalperConfig(DirectionalTradingControllerConfigBase):
    """Conservative momentum scalper for Binance perpetual futures.

    Signals are generated only from completed candles. The controller combines
    EMA trend, RSI regime, MACD momentum, and ATR/volume filters to reduce
    low-quality entries. Execution and risk are handled by the base
    DirectionalTradingController and PositionExecutor.
    """

    controller_name: str = "binance_futures_scalper"

    candles_connector: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the candles connector, leave empty to use the trading connector: ",
            "prompt_on_new": True,
        },
    )
    candles_trading_pair: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the candles trading pair, leave empty to use the trading pair: ",
            "prompt_on_new": True,
        },
    )
    interval: str = Field(
        default="1m",
        json_schema_extra={
            "prompt": "Enter the candle interval (e.g., 1m, 3m, 5m): ",
            "prompt_on_new": True,
        },
    )

    # Scalping defaults: low leverage, tight barriers, and short holding time.\n    leverage: int = Field(default=15, ge=1, le=20)\n    stop_loss: Decimal = Field(default=Decimal("0.004"), gt=0)\n    take_profit: Decimal = Field(default=Decimal("0.006"), gt=0)\n    time_limit: int = Field(default=900, gt=0)\n\n    fast_ema: int = Field(default=9, gt=1)
    slow_ema: int = Field(default=21, gt=2)
    rsi_length: int = Field(default=14, gt=2)
    macd_fast: int = Field(default=12, gt=1)
    macd_slow: int = Field(default=26, gt=2)
    macd_signal: int = Field(default=9, gt=1)
    atr_length: int = Field(default=14, gt=2)

    min_atr_pct: float = Field(
        default=0.0008,
        gt=0,
        json_schema_extra={"prompt": "Minimum ATR as a fraction of price (e.g., 0.0008 = 0.08%): "},
    )
    max_atr_pct: float = Field(
        default=0.012,
        gt=0,
        json_schema_extra={"prompt": "Maximum ATR as a fraction of price (e.g., 0.012 = 1.2%): "},
    )
    min_volume_ratio: float = Field(
        default=0.8,
        gt=0,
        json_schema_extra={"prompt": "Minimum volume / average volume ratio: "},
    )
    rsi_long_min: float = Field(default=52.0, ge=0, le=100)
    rsi_long_max: float = Field(default=72.0, ge=0, le=100)
    rsi_short_min: float = Field(default=28.0, ge=0, le=100)
    rsi_short_max: float = Field(default=48.0, ge=0, le=100)
    signal_score: int = Field(
        default=4,
        ge=1,
        le=5,
        json_schema_extra={"prompt": "Minimum confirmations required for an entry (1-5): "},
    )
    volume_lookback: int = Field(default=20, gt=2)

    @field_validator("candles_connector", mode="before")
    @classmethod
    def set_candles_connector(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def set_candles_trading_pair(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("trading_pair")
        return v


class BinanceFuturesScalper(DirectionalTradingControllerBase):
    """Multi-confirmation 1m momentum scalper.

    The current candle is excluded from signal generation so an entry cannot
    be triggered by an unfinished candle. A signal is emitted only when the
    market has enough history and the volatility/volume gates pass.
    """

    def __init__(self, config: BinanceFuturesScalperConfig, *args, **kwargs):
        self.config = config
        self.max_records = max(
            config.slow_ema,
            config.rsi_length,
            config.macd_slow,
            config.macd_signal,
            config.atr_length,
            config.volume_lookback,
        ) + 30
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        df = self.market_data_provider.get_candles_df(
            connector_name=self.config.candles_connector,
            trading_pair=self.config.candles_trading_pair,
            interval=self.config.interval,
            max_records=self.max_records,
        )

        self.processed_data = {"signal": 0, "features": df}

        minimum_rows = max(
            self.config.slow_ema,
            self.config.macd_slow + self.config.macd_signal,
            self.config.atr_length,
            self.config.volume_lookback,
        ) + 5
        if df is None or len(df) < minimum_rows:
            return

        # Indicators are calculated on the full dataframe, but only the last
        # completed candle is allowed to generate a trading signal.
        df["ema_fast"] = ta.ema(df["close"], length=self.config.fast_ema)
        df["ema_slow"] = ta.ema(df["close"], length=self.config.slow_ema)
        df["rsi"] = ta.rsi(df["close"], length=self.config.rsi_length)
        macd = ta.macd(
            df["close"],
            fast=self.config.macd_fast,
            slow=self.config.macd_slow,
            signal=self.config.macd_signal,
        )
        atr = ta.atr(df["high"], df["low"], df["close"], length=self.config.atr_length)
        df["atr"] = atr

        if macd is not None:
            df["macd"] = macd.iloc[:, 0]
            df["macd_hist"] = macd.iloc[:, 1]
        else:
            df["macd"] = float("nan")
            df["macd_hist"] = float("nan")

        df["volume_avg"] = df["volume"].rolling(self.config.volume_lookback).mean()
        df["atr_pct"] = df["atr"] / df["close"]
        df["volume_ratio"] = df["volume"] / df["volume_avg"]

        # Use the last closed candle, not the currently forming candle.
        idx = -2
        row = df.iloc[idx]
        previous = df.iloc[idx - 1]

        values = [
            row["ema_fast"],
            row["ema_slow"],
            row["rsi"],
            row["macd"],
            row["macd_hist"],
            row["atr_pct"],
            row["volume_ratio"],
        ]
        if any(v != v for v in values):  # NaN check without numpy dependency
            self.processed_data["features"] = df
            return

        volatility_ok = (
            self.config.min_atr_pct <= row["atr_pct"] <= self.config.max_atr_pct
        )
        volume_ok = row["volume_ratio"] >= self.config.min_volume_ratio

        long_score = sum(
            [
                row["ema_fast"] > row["ema_slow"],
                row["close"] > row["ema_fast"],
                row["rsi"] >= self.config.rsi_long_min
                and row["rsi"] <= self.config.rsi_long_max,
                row["macd"] > 0 and row["macd_hist"] > 0,
                row["macd_hist"] > previous["macd_hist"],
            ]
        )

        short_score = sum(
            [
                row["ema_fast"] < row["ema_slow"],
                row["close"] < row["ema_fast"],
                row["rsi"] >= self.config.rsi_short_min
                and row["rsi"] <= self.config.rsi_short_max,
                row["macd"] < 0 and row["macd_hist"] < 0,
                row["macd_hist"] < previous["macd_hist"],
            ]
        )

        signal = 0
        if volatility_ok and volume_ok:
            if long_score >= self.config.signal_score and long_score > short_score:
                signal = 1
            elif short_score >= self.config.signal_score and short_score > long_score:
                signal = -1

        df["signal"] = 0
        df.iloc[idx, df.columns.get_loc("signal")] = signal

        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def get_candles_config(self) -> List[CandlesConfig]:
        return [
            CandlesConfig(
                connector=self.config.candles_connector,
                trading_pair=self.config.candles_trading_pair,
                interval=self.config.interval,
                max_records=self.max_records,
            )
        ]
