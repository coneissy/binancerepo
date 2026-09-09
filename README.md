# Binance Futures Scalper V2

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

## V2 safety layer
- Persistent `risk_state.json`
- Daily UTC reset with day-start equity
- Daily realized-PnL reconciliation from Binance in live/demo API mode
- Daily loss cap measured as a percentage of day-start equity
- Maximum trades per day and entry cooldown
- Persistent pause state
- Persistent kill switch
- Startup position/order reconciliation
- If a live position is found without both protective stop/TP orders, the bot attempts an emergency reduce-only market close
- If protective orders fail after entry, the bot attempts an emergency reduce-only market close
- Telegram controls: `/status`, `/risk`, `/pause`, `/resume`, `/close`, `/kill`, `/killoff`
- Telegram commands are restricted to the configured `TELEGRAM_CHAT_ID`
- Optional Binance user-data WebSocket monitor with reconnect/keepalive handling
- Continuous runtime loop instead of one-shot execution

## Telegram commands
- `/status` — mode, position, trades, realized PnL and risk state
- `/risk` — risk per trade, daily cap and remaining loss budget
- `/pause` — stop new entries while keeping existing protection orders
- `/resume` — resume entries when the kill switch is clear
- `/close` — cancel open orders and attempt a reduce-only position close
- `/kill` — activate persistent kill switch, cancel orders and attempt to close the position
- `/killoff` — clear kill switch but keep trading paused until `/resume`

## Configuration
See `.env.example`. The safest starting point is:

```text
DRY_RUN=true
BINANCE_LEVERAGE=3
RISK_PER_TRADE=0.0025
MAX_DAILY_LOSS=0.01
MAX_TRADES_PER_DAY=10
COOLDOWN_SECONDS=300
LOOP_SECONDS=30
```

Never commit API keys or secrets. Put them only in your local environment, GitHub/hosting secrets, or the platform's secret manager. Do not paste API secrets into chat.

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

## Binance WebSocket note
The user-data monitor is an optional low-latency event layer; REST startup reconciliation remains the safety source of truth. Binance's current USDⓈ-M WebSocket documentation requires user-data stream keepalive and supports reconnect handling. Verify the exact non-production WebSocket base URL supplied for the account/environment before enabling it.

## Before live trading
1. Keep `DRY_RUN=true` while validating the complete strategy.
2. Use a separate non-production/demo account and API credentials with the minimum permissions needed.
3. Confirm Telegram `/kill` works before allowing any automated order placement.
4. Run extended demo tests and inspect fills, stop/TP behavior, restart recovery and daily PnL reconciliation.
5. Only then consider a tightly limited live deployment.
