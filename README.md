# Binance Futures Scalper V1

A conservative V1 Binance USDⓈ-M Futures scalper foundation. **It defaults to DRY_RUN=true and is not guaranteed-profitable or risk-free.** Test with a non-production account before enabling live orders.

## V1 strategy
- 1-minute candles (configurable)
- EMA 9 / EMA 21 trend filter
- RSI 14 confirmation: >=55 long, <=45 short
- Close-above/below EMA9 confirmation
- Volatility-based stop distance
- Risk-based quantity sizing (default 0.25% of account equity)
- 1.5R stop / 2.25R target
- One open position at a time
- Exchange LOT_SIZE rounding
- Binance HMAC signed REST requests
- Optional Telegram trade notifications

## Safety defaults
`DRY_RUN=true` and leverage 3 are the defaults. The daily-loss/trade-count values are configuration targets; V1 does **not** yet persist daily PnL/trade state, so those limits are not complete enforcement. Do not switch to live trading until V2 adds persistent risk-state and reconciliation.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

Never commit API keys. Use GitHub Actions secrets or hosting-provider environment variables. Binance's official API documentation is authoritative for endpoints, authentication, rate limits and supported non-production environments.

## GitHub Actions
CI validates Python syntax and runs the test suite. Manual execution is available from Actions. CI never receives trading credentials.

## V2 roadmap
Persistent daily risk accounting, order reconciliation, OHLC ATR, retries/backoff, websocket user-data reconciliation, structured trade journal, Telegram `/status`, `/pause`, `/resume`, and a hard live-trading kill switch.
