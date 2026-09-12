import os
import time
import logging
from dataclasses import dataclass
from statistics import mean

import requests

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("multi-scalper")

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
SCAN_SIZE = int(os.getenv("SCAN_SIZE", "100"))
SCAN_SECONDS = float(os.getenv("SCAN_SECONDS", "5"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "3"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "5000000"))
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0015"))
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.08"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "0.80"))
TP_ATR = float(os.getenv("TP_ATR", "0.65"))
SL_ATR = float(os.getenv("SL_ATR", "1.00"))

S = requests.Session()


def public(path, params=None):
    r = S.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def ema(xs, n):
    k = 2.0 / (n + 1.0)
    e = float(xs[0])
    for x in xs[1:]:
        e = float(x) * k + e * (1 - k)
    return e


def rsi(xs, n=14):
    if len(xs) <= n:
        return 50.0
    d = [xs[i] - xs[i - 1] for i in range(1, len(xs))]
    gains = [max(x, 0.0) for x in d[-n:]]
    losses = [max(-x, 0.0) for x in d[-n:]]
    ag, al = mean(gains), mean(losses)
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def atr_pct(klines, n=14):
    if len(klines) <= n:
        return 0.0
    trs = []
    prev = float(klines[0][4])
    for k in klines[-n:]:
        high, low, close = float(k[2]), float(k[3]), float(k[4])
        trs.append(max(high - low, abs(high - prev), abs(low - prev)))
        prev = close
    last = float(klines[-1][4])
    return (mean(trs) / last) if last else 0.0


def universe():
    info = public("/fapi/v1/exchangeInfo")
    allowed = {
        x["symbol"] for x in info["symbols"]
        if x.get("status") == "TRADING"
        and x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT"
    }
    tickers = public("/fapi/v1/ticker/24hr")
    ranked = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in allowed:
            continue
        try:
            qv = float(t.get("quoteVolume", 0))
            change = abs(float(t.get("priceChangePercent", 0))) / 100.0
            ranked.append((qv, change, sym))
        except (TypeError, ValueError):
            continue
    ranked = [x for x in ranked if x[0] >= MIN_QUOTE_VOLUME]
    ranked.sort(key=lambda x: (x[0] * max(x[1], 0.002)), reverse=True)
    return [x[2] for x in ranked[:SCAN_SIZE]]


def analyze(symbol):
    ks = public("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": 60})
    closes = [float(k[4]) for k in ks]
    vols = [float(k[5]) for k in ks]
    if len(closes) < 30:
        return None
    last = closes[-1]
    e5, e9, e21 = ema(closes, 5), ema(closes, 9), ema(closes, 21)
    rr = rsi(closes)
    ap = atr_pct(ks)
    if not (MIN_ATR_PCT <= ap <= MAX_ATR_PCT):
        return None
    vratio = vols[-1] / max(mean(vols[-21:-1]), 1e-12)
    mom = closes[-1] / closes[-4] - 1.0
    score_long = 0.0
    score_short = 0.0
    if e5 > e9 > e21 and last > e5:
        score_long += 0.30
    if e5 < e9 < e21 and last < e5:
        score_short += 0.30
    if rr >= 55:
        score_long += 0.20
    if rr <= 45:
        score_short += 0.20
    if mom > 0.001:
        score_long += 0.20
    if mom < -0.001:
        score_short += 0.20
    if vratio >= 1.20:
        if mom > 0:
            score_long += 0.15
        elif mom < 0:
            score_short += 0.15
    if ap >= 0.003:
        score_long += 0.05 if mom > 0 else 0.0
        score_short += 0.05 if mom < 0 else 0.0
    score = max(score_long, score_short)
    if score < MIN_SIGNAL_SCORE:
        return None
    side = "BUY" if score_long > score_short else "SELL"
    return {"symbol": symbol, "side": side, "score": round(score, 3), "price": last,
            "atr_pct": ap, "rsi": rr, "volume_ratio": vratio, "momentum": mom}


def main():
    log.warning("MULTI-COIN SCALPER STARTED | dry_run=%s | universe=%s", DRY_RUN, SCAN_SIZE)
    log.warning("This engine stays in DRY_RUN unless explicitly changed outside this code.")
    last_universe = []
    universe_refresh = 0.0
    while True:
        try:
            if time.time() - universe_refresh >= 60 or not last_universe:
                last_universe = universe()
                universe_refresh = time.time()
                log.info("Universe refreshed: %d symbols", len(last_universe))
            candidates = []
            for symbol in last_universe:
                try:
                    signal = analyze(symbol)
                    if signal:
                        candidates.append(signal)
                except requests.RequestException as exc:
                    log.debug("%s data error: %s", symbol, exc)
                except Exception:
                    log.exception("%s analysis error", symbol)
            candidates.sort(key=lambda x: x["score"], reverse=True)
            for c in candidates[:MAX_POSITIONS]:
                log.info("SETUP %s %s score=%.3f price=%s ATR=%.3f%% RSI=%.1f vol=%.2fx",
                         c["side"], c["symbol"], c["score"], c["price"], c["atr_pct"] * 100,
                         c["rsi"], c["volume_ratio"])
            if candidates:
                log.info("Top setups: %s", ", ".join(f'{x["symbol"]}:{x["score"]:.2f}' for x in candidates[:5]))
        except Exception:
            log.exception("Scanner cycle failed")
        time.sleep(SCAN_SECONDS)


if __name__ == "__main__":
    main()
