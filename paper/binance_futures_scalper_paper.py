from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd
import pandas_ta as ta

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"


@dataclass(frozen=True)
class Config:
    symbol: str = "BTCUSDT"
    quantity_quote: float = 100.0
    stop_loss: float = 0.004
    take_profit: float = 0.006
    time_limit_seconds: int = 900
    fee_bps: float = 5.0
    slippage_bps: float = 1.0
    signal_score: int = 4


@dataclass
class Position:
    side: int
    entry: float
    opened: pd.Timestamp
    qty: float
    entry_fee: float


@dataclass
class Trade:
    opened_at: str
    closed_at: str
    side: str
    entry_price: float
    exit_price: float
    gross_pnl: float
    fees: float
    net_pnl: float
    reason: str
    hold_seconds: float


def fetch(symbol: str, limit: int = 200) -> pd.DataFrame:
    q = urllib.parse.urlencode({"symbol": symbol, "interval": "1m", "limit": limit})
    req = urllib.request.Request(BASE_URL + "?" + q, headers={"User-Agent": "binance-scalper-paper/1.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode())
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time",
            "quote_volume", "trades", "taker_base", "taker_quote", "ignore"]
    df = pd.DataFrame(data, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c])
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


def signal(df: pd.DataFrame, cfg: Config) -> int:
    d = df.copy()
    d["ef"] = ta.ema(d.close, length=9)
    d["es"] = ta.ema(d.close, length=21)
    d["rsi"] = ta.rsi(d.close, length=14)
    macd = ta.macd(d.close, fast=12, slow=26, signal=9)
    if macd is None:
        return 0
    d["macd"] = macd.iloc[:, 0]
    d["hist"] = macd.iloc[:, 1]
    d["atr_pct"] = ta.atr(d.high, d.low, d.close, length=14) / d.close
    d["vr"] = d.volume / d.volume.rolling(20).mean()
    if len(d) < 55:
        return 0
    r, p = d.iloc[-2], d.iloc[-3]
    vals = [r.ef, r.es, r.rsi, r.macd, r.hist, r.atr_pct, r.vr]
    if any(pd.isna(x) for x in vals) or not (0.0008 <= r.atr_pct <= 0.012) or r.vr < 0.8:
        return 0
    long_score = sum([r.ef > r.es, r.close > r.ef, 52 <= r.rsi <= 72,
                      r.macd > 0 and r.hist > 0, r.hist > p.hist])
    short_score = sum([r.ef < r.es, r.close < r.ef, 28 <= r.rsi <= 48,
                       r.macd < 0 and r.hist < 0, r.hist < p.hist])
    if long_score >= cfg.signal_score and long_score > short_score:
        return 1
    if short_score >= cfg.signal_score and short_score > long_score:
        return -1
    return 0


def entry(price: float, ts: pd.Timestamp, side: int, cfg: Config) -> Position:
    slip = cfg.slippage_bps / 10000
    px = price * (1 + side * slip)
    qty = cfg.quantity_quote / px
    return Position(side, px, ts, qty, cfg.quantity_quote * cfg.fee_bps / 10000)


def exit_trade(pos: Position, price: float, ts: pd.Timestamp, reason: str, cfg: Config) -> Trade:
    slip = cfg.slippage_bps / 10000
    px = price * (1 - pos.side * slip)
    gross = pos.side * (px - pos.entry) * pos.qty
    fees = pos.entry_fee + abs(px * pos.qty) * cfg.fee_bps / 10000
    return Trade(pos.opened.isoformat(), ts.isoformat(), "LONG" if pos.side == 1 else "SHORT",
                 pos.entry, px, gross, fees, gross - fees, reason,
                 (ts - pos.opened).total_seconds())


def metrics(trades: list[Trade]) -> dict:
    pnls = [t.net_pnl for t in trades]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    equity, peak, dd = 0.0, 0.0, 0.0
    for x in pnls:
        equity += x
        peak = max(peak, equity)
        dd = max(dd, peak - equity)
    return {
        "trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(pnls) if pnls else 0.0,
        "gross_pnl": sum(t.gross_pnl for t in trades),
        "fees": sum(t.fees for t in trades),
        "net_pnl": sum(pnls),
        "profit_factor": sum(wins) / abs(sum(losses)) if losses else None,
        "expectancy": sum(pnls) / len(pnls) if pnls else 0.0,
        "max_drawdown": dd,
    }


def run(cfg: Config, ledger: Path, poll_seconds: int, duration_minutes: int | None) -> None:
    if os.getenv("PAPER_ONLY", "true").lower() != "true":
        raise RuntimeError("PAPER_ONLY must be true")
    ledger.parent.mkdir(parents=True, exist_ok=True)
    exists = ledger.exists()
    position = None
    trades: list[Trade] = []
    last_candle = None
    started = time.monotonic()
    fields = list(Trade.__annotations__.keys())

    with ledger.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        print("PAPER ONLY: public Binance market data; no order API or credentials")
        while duration_minutes is None or time.monotonic() - started < duration_minutes * 60:
            try:
                df = fetch(cfg.symbol)
                candle = df.iloc[-2]
                ts = candle.close_time
                if last_candle is not None and ts <= last_candle:
                    time.sleep(poll_seconds)
                    continue
                last_candle = ts

                if position is not None:
                    stop = position.entry * (1 - cfg.stop_loss if position.side == 1 else 1 + cfg.stop_loss)
                    target = position.entry * (1 + cfg.take_profit if position.side == 1 else 1 - cfg.take_profit)
                    hit = None
                    if position.side == 1:
                        if candle.low <= stop:
                            hit = (stop, "STOP_LOSS")
                        elif candle.high >= target:
                            hit = (target, "TAKE_PROFIT")
                    else:
                        if candle.high >= stop:
                            hit = (stop, "STOP_LOSS")
                        elif candle.low <= target:
                            hit = (target, "TAKE_PROFIT")
                    if hit is None and (ts - position.opened).total_seconds() >= cfg.time_limit_seconds:
                        hit = (float(candle.close), "TIME_LIMIT")
                    if hit:
                        t = exit_trade(position, hit[0], ts, hit[1], cfg)
                        writer.writerow(asdict(t))
                        f.flush()
                        trades.append(t)
                        print("EXIT", t.side, t.reason, "net", round(t.net_pnl, 6))
                        position = None

                if position is None:
                    s = signal(df, cfg)
                    if s:
                        position = entry(float(candle.close), ts, s, cfg)
                        print("ENTRY", "LONG" if s == 1 else "SHORT", "price", round(position.entry, 2))
                print(json.dumps(metrics(trades), sort_keys=True))
            except Exception as exc:
                print("DATA_ERROR", type(exc).__name__, str(exc))
            time.sleep(poll_seconds)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--quantity-quote", type=float, default=100.0)
    p.add_argument("--fee-bps", type=float, default=5.0)
    p.add_argument("--slippage-bps", type=float, default=1.0)
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument("--duration-minutes", type=int, default=None)
    p.add_argument("--ledger", default="data/binance_futures_scalper_paper.csv")
    a = p.parse_args()
    run(Config(symbol=a.symbol, quantity_quote=a.quantity_quote, fee_bps=a.fee_bps,
              slippage_bps=a.slippage_bps), Path(a.ledger), a.poll_seconds, a.duration_minutes)
