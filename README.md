# Binance Futures Scalper V1.1

A conservative Binance USDⓈ-M Futures scalper foundation. **It defaults to `DRY_RUN=true` and is not guaranteed-profitable or risk-free.** Test with a non-production account before enabling live orders.

## Strategy
- 1-minute candles (configurable)
- EMA 9 / EMA 21 trend filter
- RSI 14 confirmation: >=55 long, <=45 short
- Close-above/below EMA9 confirmation
- Close-to-close volatility stop proxy
- Risk-based quantity sizing (default 0.25% of account equity)
- 1.5R stop / 2.25R target
- One open position at a time
- Exchange `LOT_SIZE` and `PRICE_FILTER` rounding
- Binance HMAC-signed REST requests
- Retry/backoff for transient REST failures
- Optional Telegram trade notifications

## V1.1 safety controls
- Persistent local risk state in `risk_state.json`
- Daily UTC state reset
- Maximum trades per day
- Cooldown between entries
- Manual pause flag in persistent state
- Live-mode credential guard
- If stop/TP protection fails after an entry, the bot attempts an emergency reduce-only market close before failing

`MAX_DAILY_LOSS` is retained as a configuration target, but realized-PnL reconciliation is still a V2 item. Do not treat the current build as a fully autonomous risk manager.

## Configuration
See `.env.example`. The safest starting point is:

```text
DRY_RUN=true
BINANCE_LEVERAGE=3
RISK_PER_TRADE=0.0025
MAX_TRADES_PER_DAY=10
COOLDOWN_SECONDS=300
```

Never commit API keys. Use GitHub Actions secrets or hosting-provider environment variables. Binance's official API documentation is authoritative for endpoints, authentication, rate limits and supported non-production environments. citeturn0search0turn0search3

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

## GitHub Actions
CI validates Python syntax and runs the test suite. CI never receives trading credentials.

## Next hardening step before live trading
1. Reconcile realized PnL from Binance instead of relying only on local state.
2. Reconcile orders/positions after restarts.
3. Add WebSocket user-data reconciliation; Binance recommends user-data streams for timely order and position updates, and streams require keepalive/reconnection handling. citeturn0search0turn0search3
4. Add Telegram `/status`, `/pause`, `/resume`, and a hard live-trading kill switch.
5. Run extended demo/dry-run tests before any live API key is permitted.
