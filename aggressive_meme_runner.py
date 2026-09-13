"""Aggressive dry-run profile for the full meme universe + new listings."""
import os

# Faster entries/exits while retaining the strategy's spread, liquidity, edge,
# and hard live-execution safety checks.
AGGRESSIVE = {
    "DRY_RUN": "true",
    "UNIVERSE_SIZE": "0",              # every matched meme contract
    "WS_SHARDS": "10",
    "MAX_SIMULTANEOUS_POSITIONS": "15",
    "MIN_24H_QUOTE_VOLUME": "0",
    "ENTRY_SCORE": "0.22",
    "TAKE_PROFIT_PCT": "0.0006",
    "STOP_LOSS_PCT": "0.0010",
    "MAX_HOLD_SECONDS": "3",
    "MAX_SPREAD_BPS": "15",
    "ENTRY_COOLDOWN_SECONDS": "0.05",
    "MIN_EDGE_MULTIPLIER": "1.10",
    "MIN_TP_PCT": "0.0006",
    "MIN_SL_PCT": "0.0008",
    "MAX_SL_PCT": "0.0020",
    "NEW_LISTING_WINDOW_SECONDS": "900",
    "NEW_LISTING_MIN_QUOTE_VOLUME": "250000",
    "NEW_LISTING_MIN_LIVE_NOTIONAL": "50000",
    "NEW_LISTING_ENTRY_SCORE": "0.34",
    "NEW_LISTING_TP_PCT": "0.0020",
    "NEW_LISTING_SL_PCT": "0.0015",
    "NEW_LISTING_MAX_HOLD_SECONDS": "8",
    "NEW_LISTING_COOLDOWN_SECONDS": "0.25",
    "NEW_LISTING_MAX_POSITIONS": "3",
    "LISTING_REFRESH_SECONDS": "10",
}

for key, value in AGGRESSIVE.items():
    os.environ[key] = value
os.environ.setdefault("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
os.environ.setdefault("BINANCE_WS_BASE_URL", "wss://fstream.binancefuture.com/stream")

from hft_scalper import main

if __name__ == "__main__":
    main()
