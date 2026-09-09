import hashlib, hmac, json, logging, os, time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from statistics import mean
from urllib.parse import urlencode
import requests

logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('scalper')

@dataclass
class Config:
    key: str = os.getenv('BINANCE_API_KEY', '')
    secret: str = os.getenv('BINANCE_API_SECRET', '')
    base: str = os.getenv('BINANCE_BASE_URL', 'https://demo-fapi.binance.com')
    symbol: str = os.getenv('BINANCE_SYMBOL', 'BTCUSDT')
    interval: str = os.getenv('BINANCE_INTERVAL', '1m')
    leverage: int = int(os.getenv('BINANCE_LEVERAGE', '3'))
    risk: float = float(os.getenv('RISK_PER_TRADE', '0.0025'))
    max_daily_loss: float = float(os.getenv('MAX_DAILY_LOSS', '0.01'))
    max_trades: int = int(os.getenv('MAX_TRADES_PER_DAY', '10'))
    cooldown: int = int(os.getenv('COOLDOWN_SECONDS', '300'))
    state_file: str = os.getenv('RISK_STATE_FILE', 'risk_state.json')
    dry: bool = os.getenv('DRY_RUN', 'true').lower() == 'true'

C = Config()
S = requests.Session()
if C.key:
    S.headers.update({'X-MBX-APIKEY': C.key})


def public(method, path, params=None):
    for attempt in range(3):
        try:
            r = S.request(method, C.base + path, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))


def signed(method, path, params=None):
    if not C.key or not C.secret:
        raise RuntimeError('API credentials are required for signed requests')
    p = dict(params or {})
    p['timestamp'] = int(time.time() * 1000)
    p['recvWindow'] = 5000
    q = urlencode(p)
    p['signature'] = hmac.new(C.secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return public(method, path, p)


def closes(limit=120):
    return [float(x[4]) for x in public('GET', '/fapi/v1/klines', {'symbol': C.symbol, 'interval': C.interval, 'limit': limit})]


def ema(xs, n):
    k = 2 / (n + 1)
    e = xs[0]
    for x in xs[1:]:
        e = x * k + e * (1 - k)
    return e


def rsi(xs, n=14):
    if len(xs) <= n:
        raise ValueError('Not enough data for RSI')
    d = [xs[i] - xs[i - 1] for i in range(1, len(xs))]
    g = [max(x, 0) for x in d]
    l = [max(-x, 0) for x in d]
    ag = sum(g[-n:]) / n
    al = sum(l[-n:]) / n
    if al == 0:
        return 100.0
    if ag == 0:
        return 0.0
    return 100 - 100 / (1 + ag / al)


def atr_from_closes(xs, n=14):
    if len(xs) <= n:
        raise ValueError('Not enough data for volatility')
    return mean(abs(xs[i] - xs[i - 1]) for i in range(len(xs) - n, len(xs)))


def signal(xs):
    e9, e21 = ema(xs, 9), ema(xs, 21)
    rr = rsi(xs)
    a = atr_from_closes(xs)
    last = xs[-1]
    if a <= 0:
        return None
    if e9 > e21 and rr >= 55 and last > e9:
        return 'BUY'
    if e9 < e21 and rr <= 45 and last < e9:
        return 'SELL'
    return None


def exchange_rules():
    s = next(x for x in public('GET', '/fapi/v1/exchangeInfo')['symbols'] if x['symbol'] == C.symbol)
    f = {x['filterType']: x for x in s['filters']}
    lot = f['LOT_SIZE']
    price = f.get('PRICE_FILTER', {})
    return float(lot['stepSize']), float(lot['minQty']), float(price.get('tickSize', '0.01'))


def round_down(value, step):
    d = Decimal(str(value))
    st = Decimal(str(step))
    return float((d / st).to_integral_value(rounding=ROUND_DOWN) * st)


def round_qty(q, step):
    return round_down(q, step)


def account_equity():
    return float(next(x for x in signed('GET', '/fapi/v2/balance') if x['asset'] == 'USDT')['balance'])


def position():
    rows = signed('GET', '/fapi/v2/positionRisk', {'symbol': C.symbol})
    return float(rows[0]['positionAmt'])


def today_key():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def load_state():
    state = {'date': today_key(), 'realized_pnl': 0.0, 'trades': 0, 'last_trade': 0.0, 'paused': False}
    try:
        with open(C.state_file, 'r', encoding='utf-8') as f:
            state.update(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    if state.get('date') != today_key():
        state = {'date': today_key(), 'realized_pnl': 0.0, 'trades': 0, 'last_trade': 0.0, 'paused': state.get('paused', False)}
        save_state(state)
    return state


def save_state(state):
    tmp = C.state_file + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, C.state_file)


def risk_allowed(state):
    if state.get('paused'):
        return False, 'paused'
    if state['trades'] >= C.max_trades:
        return False, 'max trades reached'
    if state['realized_pnl'] <= -(abs(C.max_daily_loss)):
        return False, 'daily loss limit reached'
    if time.time() - state.get('last_trade', 0) < C.cooldown:
        return False, 'cooldown active'
    return True, ''


def place(side, qty, stop_price, take_price):
    if C.dry:
        return {'dry_run': True, 'side': side, 'quantity': qty, 'stopPrice': stop_price, 'takePrice': take_price}
    signed('POST', '/fapi/v1/leverage', {'symbol': C.symbol, 'leverage': C.leverage})
    order = signed('POST', '/fapi/v1/order', {'symbol': C.symbol, 'side': side, 'type': 'MARKET', 'quantity': qty})
    exit_side = 'SELL' if side == 'BUY' else 'BUY'
    try:
        signed('POST', '/fapi/v1/order', {'symbol': C.symbol, 'side': exit_side, 'type': 'STOP_MARKET', 'stopPrice': stop_price, 'closePosition': 'true', 'workingType': 'MARK_PRICE'})
        signed('POST', '/fapi/v1/order', {'symbol': C.symbol, 'side': exit_side, 'type': 'TAKE_PROFIT_MARKET', 'stopPrice': take_price, 'closePosition': 'true', 'workingType': 'MARK_PRICE'})
    except Exception:
        log.exception('Exit protection placement failed; attempting emergency market close')
        try:
            signed('POST', '/fapi/v1/order', {'symbol': C.symbol, 'side': exit_side, 'type': 'MARKET', 'quantity': qty, 'reduceOnly': 'true'})
        finally:
            raise
    return order


def notify(msg):
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat = os.getenv('TELEGRAM_CHAT_ID')
    if token and chat:
        try:
            requests.post(f'https://api.telegram.org/bot{token}/sendMessage', json={'chat_id': chat, 'text': msg}, timeout=10)
        except requests.RequestException as e:
            log.warning('Telegram notification failed: %s', e)


def run_once():
    state = load_state()
    allowed, reason = risk_allowed(state)
    if not allowed:
        log.warning('Risk gate blocked trade: %s', reason)
        return None

    xs = closes()
    sig = signal(xs)
    price = xs[-1]
    log.info('%s price=%.4f signal=%s dry=%s', C.symbol, price, sig, C.dry)
    if not sig:
        return None

    eq = account_equity() if C.key and C.secret else 1000.0
    risk_cash = eq * C.risk
    stop_distance = max(atr_from_closes(xs) * 1.5, price * 0.001)
    step, minq, tick = exchange_rules()
    qty = round_qty(risk_cash / stop_distance, step)
    if qty < minq:
        log.warning('Calculated qty %.8f below exchange minimum %.8f', qty, minq)
        return None
    if not C.dry and position() != 0:
        return None

    stop = price - stop_distance if sig == 'BUY' else price + stop_distance
    take = price + stop_distance * 1.5 if sig == 'BUY' else price - stop_distance * 1.5
    stop = round_down(stop, tick)
    take = round_down(take, tick)
    result = place(sig, qty, stop, take)

    state['trades'] += 1
    state['last_trade'] = time.time()
    save_state(state)
    notify(f'Binance Scalper V1.1: {sig} {C.symbol} qty={qty} price={price:.4f} mode={"DRY" if C.dry else "LIVE"}')
    return result


if __name__ == '__main__':
    if not C.dry and (not C.key or not C.secret):
        raise SystemExit('Live mode requires BINANCE_API_KEY and BINANCE_API_SECRET')
    run_once()
