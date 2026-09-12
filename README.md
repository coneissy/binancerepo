# Binance Futures Scalper V2 + PolyMorph AI v0.1

A conservative Binance USDⓈ-M Futures scalper foundation with a transparent PolyMorph AI v0.1 decision layer. **It defaults to `DRY_RUN=true` and is not guaranteed-profitable or risk-free.** Test with a non-production account before enabling live orders.

## PolyMorph AI v0.1

`polymorph_ai.py` adds a safe, explainable multi-strategy consensus layer:

- EMA trend strategy
- Short-term momentum strategy
- Mean-reversion strategy
- BUY / SELL / HOLD consensus
- Confidence score and per-strategy votes
- No order placement and no API credentials required
- Designed to be integrated into the existing risk and execution layer only after demo validation

Run a local smoke test:

```bash
python polymorph_ai.py
```

Run tests:

```bash
pytest -q
```

## Safe starting configuration

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

## Existing V2 safety layer

- Persistent risk state and daily UTC reset
- Daily realized-PnL reconciliation in demo/live API mode
- Daily loss cap and maximum trades per day
- Pause and persistent kill switch
- Startup position/order reconciliation
- Protective stop/TP placement with emergency-close fallback
- Telegram controls: `/status`, `/risk`, `/pause`, `/resume`, `/close`, `/kill`, `/killoff`

## Demo-first workflow

1. Keep `DRY_RUN=true`.
2. Use Binance Demo Futures credentials only.
3. Validate market data, strategy votes, risk checks, and Telegram kill controls.
4. Run extended demo testing before considering any live orders.
5. Never commit API keys or secrets.
