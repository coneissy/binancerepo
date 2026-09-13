"""Ultra-selective dynamic Top-20 meme dry-run profile.

15m controls the market regime, 5m controls directional bias, and 1m controls
execution. Live execution remains disabled.
"""
import os

PROFILE = {
    "DRY_RUN": "true",
    "UNIVERSE_SIZE": "20",
    "WS_SHARDS": "4",
    "MAX_SIMULTANEOUS_POSITIONS": "6",
    "NEUTRAL_MAX_POSITIONS": "3",
    "MIN_24H_QUOTE_VOLUME": "1000000",
    "ENTRY_SCORE": "0.68",
    "NEUTRAL_ENTRY_SCORE": "0.74",
    "MAX_SPREAD_BPS": "7",
    "ENTRY_COOLDOWN_SECONDS": "2.0",
    "MIN_EDGE_MULTIPLIER": "1.60",
    "MIN_TP_PCT": "0.0025",
    "MIN_SL_PCT": "0.0016",
    "MAX_TP_PCT": "0.0090",
    "MAX_SL_PCT": "0.0035",
    "TP_VOL_MULTIPLIER": "3.5",
    "SL_VOL_MULTIPLIER": "1.35",
    "MAX_HOLD_SECONDS": "30",
    "NEW_LISTING_WINDOW_SECONDS": "1200",
    "NEW_LISTING_MIN_QUOTE_VOLUME": "250000",
    "LISTING_REFRESH_SECONDS": "30",
    "MIN_BARS": "20",
}

for key, value in PROFILE.items():
    os.environ[key] = value

os.environ.setdefault("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
os.environ.setdefault("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")

from hft_scalper import main

if __name__ == "__main__":
    main()
