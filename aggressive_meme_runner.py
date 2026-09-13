"""Aggressive dry-run profile for the full meme universe + new listings.

This profile intentionally stays paper-only. It biases toward frequent trades,
while making the entry economics a little stricter so frequency does not come
from blindly trading moves that are too small to cover estimated costs.
"""
import os

AGGRESSIVE = {
    "DRY_RUN": "true",
    "UNIVERSE_SIZE": "0",
    "WS_SHARDS": "10",
    "MAX_SIMULTANEOUS_POSITIONS": "15",
    "MIN_24H_QUOTE_VOLUME": "0",

    # Fast but not indiscriminate.
    "ENTRY_SCORE": "0.24",
    "TAKE_PROFIT_PCT": "0.0007",
    "STOP_LOSS_PCT": "0.0009",
    "MAX_HOLD_SECONDS": "2.5",
    "MAX_SPREAD_BPS": "12",
    "ENTRY_COOLDOWN_SECONDS": "0.05",

    # Keep a positive expected-cost advantage before entering.
    "MIN_EDGE_MULTIPLIER": "1.30",
    "MIN_TP_PCT": "0.0007",
    "MIN_SL_PCT": "0.0008",
    "MAX_SL_PCT": "0.0018",
    "TP_VOL_MULTIPLIER": "1.8",
    "SL_VOL_MULTIPLIER": "1.0",

    # New-listing sniper remains faster, but still requires liquidity.
    "NEW_LISTING_WINDOW_SECONDS": "900",
    "NEW_LISTING_MIN_QUOTE_VOLUME": "250000",
    "NEW_LISTING_MIN_LIVE_NOTIONAL": "50000",
    "NEW_LISTING_ENTRY_SCORE": "0.34",
    "NEW_LISTING_TP_PCT": "0.0020",
    "NEW_LISTING_SL_PCT": "0.0014",
    "NEW_LISTING_MAX_HOLD_SECONDS": "6",
    "NEW_LISTING_COOLDOWN_SECONDS": "0.20",
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
