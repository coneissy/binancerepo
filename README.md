# Binance Futures Scalper V2 → PolyMorph AI v0.1

A conservative Binance USDⓈ-M Futures scalper foundation with a deterministic multi-strategy signal-scoring layer. **It defaults to `DRY_RUN=true` and is not guaranteed-profitable or risk-free.** Test with a non-production account before enabling live orders.

## PolyMorph AI v0.1

The new `polymorph_ai.py` module combines three transparent research signals:

- EMA trend: bullish or bearish direction
- RSI momentum: confirmation above 55 or below 45
- Breakout/breakdown: price relative to the previous lookback range

A signal is returned only when its weighted score reaches the configured threshold. The module is deterministic, local, and does not place orders or require an AI API key. It is designed to be integrated as a scoring layer above the existing V2 execution and safety code.

Example:

```python
from polymorph_ai import evaluate

signal = evaluate(recent_closes)
if signal:
    print(signal.side, signal.score, signal.reasons)
```

## Existing V2 strategy and safety layer

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
- Persistent `risk_state.json`
- Daily UTC reset and realized-PnL reconciliation
- Daily loss cap, max trades, and entry cooldown
- Persistent pause and kill switch
- Startup position/order reconciliation
- Emergency close attempt if protection fails
- Telegram controls: `/status`, `/risk`, `/pause`, `/resume`, `/close`, `/kill`, `/killoff`

## Safe demo configuration

See `.env.example`. Keep this configuration while developing:

```text
DRY_RUN=true
BINANCE_BASE_URL=https://demo-fapi.binance.com
BINANCE_LEVERAGE=3
RISK_PER_TRADE=0.0025
MAX_DAILY_LOSS=0.01
MAX_TRADES_PER_DAY=10
COOLDOWN_SECONDS=300
LOOP_SECONDS=30
```

Never commit API keys or secrets. Put them only in your local environment, GitHub/hosting secrets, or the platform's secret manager. Do not paste API secrets into chat.

## Run locally or in Codespaces

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m pytest -q
python bot.py
```

## GitHub Actions

CI validates Python syntax and runs the test suite. CI never receives trading credentials.

## Before live trading

1. Keep `DRY_RUN=true` while validating the complete strategy.
2. Use a separate non-production/demo account and API credentials with minimum permissions.
3. Confirm Telegram `/kill` works before allowing automated order placement.
4. Run extended demo tests and inspect fills, stop/TP behavior, restart recovery and daily PnL reconciliation.
5. Only then consider a tightly limited live deployment.
