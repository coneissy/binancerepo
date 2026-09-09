import hashlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from statistics import mean
from urllib.parse import urlencode

import requests

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None

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
    poll_seconds: int = int(os.getenv('LOOP_SECONDS', '30'))
    dry: bool = os.getenv('DRY_RUN', 'true').lower() == 'true'
    telegram_token: str = os.getenv('TELEGRAM_BOT_TOKEN', '')
    telegram_chat: str = os.getenv('TELEGRAM_CHAT_ID', '')
    ws_base: str = os.getenv('BINANCE_WS_BASE_URL', 'wss://fstream.binance.com/ws')


C = Config()
S = requests.Session()
if C.key:
    S.headers.update({'X-MBX-APIKEY': C.key})


# ---------------- Binance REST ----------------
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


def open_orders():
    return signed('GET', '/fapi/v1/openOrders', {'symbol': C.symbol})


def realized_pnl_today():
    if C.dry or not C.key or not C.secret:
        return 0.0
    start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    rows = signed('GET', '/fapi/v1/income', {'symbol': C.symbol, 'incomeType': 'REALIZED_PNL', 'startTime': start, 'limit': 1000})
    return sum(float(x.get('income', 0)) for x in rows)


# ---------------- Persistent risk state ----------------
def today_key():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def default_state():
    return {
        'date': today_key(),
        'realized_pnl': 0.0,
        'day_start_equity': 0.0,
        'trades': 0,
        'last_trade': 0.0,
        'paused': False,
        'kill_switch': False,
        'ws_last_event': 0,
        'last_reconcile': 0,
    }


def load_state():
    state = default_state()
    try:
        with open(C.state_file, 'r', encoding='utf-8') as f:
            state.update(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    if state.get('date') != today_key():
        paused = bool(state.get('paused', False))
        state = default_state()
        state['paused'] = paused
    if not state.get('day_start_equity'):
        try:
            state['day_start_equity'] = account_equity() if (C.key and C.secret and not C.dry) else 1000.0
        except Exception:
            state['day_start_equity'] = 1000.0
    save_state(state)
    return state


def save_state(state):
    tmp = C.state_file + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, C.state_file)


def refresh_realized_pnl(state):
    if C.dry or not C.key or not C.secret:
        return state
    try:
        state['realized_pnl'] = realized_pnl_today()
        save_state(state)
    except Exception:
        log.exception('Could not reconcile realized PnL')
    return state


def risk_allowed(state):
    if state.get('kill_switch'):
        return False, 'kill switch active'
    if state.get('paused'):
        return False, 'paused'
    start_eq = max(float(state.get('day_start_equity', 0)), 1e-9)
    loss_limit_cash = start_eq * abs(C.max_daily_loss)
    if float(state.get('realized_pnl', 0)) <= -loss_limit_cash:
        return False, 'daily loss limit reached'
    if state['trades'] >= C.max_trades:
        return False, 'max trades reached'
    if time.time() - state.get('last_trade', 0) < C.cooldown:
        return False, 'cooldown active'
    return True, ''


# ---------------- Order safety / reconciliation ----------------
def cancel_all():
    if C.dry:
        return
    signed('DELETE', '/fapi/v1/allOpenOrders', {'symbol': C.symbol})


def emergency_close(reason='emergency'):
    if C.dry or not C.key or not C.secret:
        return {'dry_run': True, 'reason': reason}
    qty = position()
    if qty == 0:
        return {'closed': False, 'reason': 'no position'}
    side = 'SELL' if qty > 0 else 'BUY'
    result = signed('POST', '/fapi/v1/order', {
        'symbol': C.symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': abs(qty),
        'reduceOnly': 'true',
        'newOrderRespType': 'RESULT',
    })
    log.critical('Emergency close executed: %s', reason)
    notify(f'🚨 EMERGENCY CLOSE {C.symbol}: {reason}')
    return result


def reconcile_account():
    if C.dry or not C.key or not C.secret:
        return {'mode': 'dry', 'position': 0.0, 'open_orders': 0, 'protected': True}
    pos = position()
    orders = open_orders()
    protective = [o for o in orders if o.get('type') in ('STOP_MARKET', 'TAKE_PROFIT_MARKET', 'STOP', 'TAKE_PROFIT')]
    if pos != 0 and len(protective) < 2:
        log.critical('Unprotected live position detected during reconciliation')
        emergency_close('unprotected position after restart/reconcile')
        return {'position': pos, 'open_orders': len(orders), 'protected': False, 'action': 'closed'}
    return {'position': pos, 'open_orders': len(orders), 'protected': pos == 0 or len(protective) >= 2}


def place(side, qty, stop_price, take_price):
    if C.dry:
        return {'dry_run': True, 'side': side, 'quantity': qty, 'stopPrice': stop_price, 'takePrice': take_price}
    signed('POST', '/fapi/v1/leverage', {'symbol': C.symbol, 'leverage': C.leverage})
    order = signed('POST', '/fapi/v1/order', {
        'symbol': C.symbol, 'side': side, 'type': 'MARKET', 'quantity': qty,
        'newOrderRespType': 'RESULT',
    })
    exit_side = 'SELL' if side == 'BUY' else 'BUY'
    try:
        signed('POST', '/fapi/v1/order', {
            'symbol': C.symbol, 'side': exit_side, 'type': 'STOP_MARKET',
            'stopPrice': stop_price, 'closePosition': 'true', 'workingType': 'MARK_PRICE',
        })
        signed('POST', '/fapi/v1/order', {
            'symbol': C.symbol, 'side': exit_side, 'type': 'TAKE_PROFIT_MARKET',
            'stopPrice': take_price, 'closePosition': 'true', 'workingType': 'MARK_PRICE',
        })
    except Exception:
        log.exception('Exit protection placement failed; attempting emergency market close')
        try:
            emergency_close('protective SL/TP placement failure')
        finally:
            raise
    return order


# ---------------- Telegram controls ----------------
def telegram_send(msg):
    if not C.telegram_token or not C.telegram_chat:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{C.telegram_token}/sendMessage',
            json={'chat_id': C.telegram_chat, 'text': msg}, timeout=10,
        ).raise_for_status()
    except requests.RequestException as e:
        log.warning('Telegram send failed: %s', e)


notify = telegram_send


def telegram_command(cmd, state):
    if cmd == '/status':
        pnl = state.get('realized_pnl', 0.0)
        start = state.get('day_start_equity', 0.0)
        pos = 0.0
        if C.key and C.secret and not C.dry:
            try:
                pos = position()
            except Exception:
                pos = float('nan')
        return (f'🛡️ V2 STATUS\nSymbol: {C.symbol}\nMode: {"DRY" if C.dry else "LIVE"}\n'
                f'Paused: {state.get("paused")}\nKill: {state.get("kill_switch")}\n'
                f'Trades: {state.get("trades")}/{C.max_trades}\nRealized PnL: {pnl:.4f}\n'
                f'Day start equity: {start:.4f}\nPosition: {pos}')
    if cmd == '/risk':
        start = max(float(state.get('day_start_equity', 0)), 1e-9)
        limit = start * abs(C.max_daily_loss)
        remaining = max(0.0, limit + float(state.get('realized_pnl', 0)))
        return f'🛡️ RISK\nRisk/trade: {C.risk*100:.2f}%\nDaily loss cap: {C.max_daily_loss*100:.2f}%\nRemaining loss budget: {remaining:.4f}'
    if cmd == '/pause':
        state['paused'] = True
        save_state(state)
        return '⏸️ Trading paused. Existing protection orders are left intact.'
    if cmd == '/resume':
        if state.get('kill_switch'):
            return '🔒 Kill switch is active. Use /killoff after manual review.'
        state['paused'] = False
        save_state(state)
        return '▶️ Trading resumed.'
    if cmd == '/kill':
        state['kill_switch'] = True
        state['paused'] = True
        save_state(state)
        try:
            cancel_all()
            result = emergency_close('Telegram kill switch')
            return f'🛑 KILL SWITCH ACTIVE\nProtection orders cancelled and position close attempted.\n{result}'
        except Exception as exc:
            log.exception('Kill switch failed')
            return f'🛑 KILL SWITCH ACTIVE\n⚠️ Close attempt failed: {exc}'
    if cmd == '/killoff':
        state['kill_switch'] = False
        state['paused'] = True
        save_state(state)
        return '🔓 Kill switch cleared, but trading remains paused. Use /resume after review.'
    if cmd == '/close':
        try:
            cancel_all()
            return f'🔻 Close result: {emergency_close("Telegram close command")}'
        except Exception as exc:
            return f'⚠️ Close failed: {exc}'
    return 'Commands: /status /risk /pause /resume /close /kill /killoff'


def telegram_loop(stop_event):
    if not C.telegram_token or not C.telegram_chat:
        log.info('Telegram control disabled: token/chat not configured')
        return
    offset = None
    while not stop_event.is_set():
        try:
            params = {'timeout': 20, 'allowed_updates': ['message']}
            if offset is not None:
                params['offset'] = offset
            r = requests.get(f'https://api.telegram.org/bot{C.telegram_token}/getUpdates', params=params, timeout=30)
            r.raise_for_status()
            for item in r.json().get('result', []):
                offset = item['update_id'] + 1
                msg = item.get('message', {})
                chat_id = str(msg.get('chat', {}).get('id', ''))
                text = str(msg.get('text', '')).strip().split()[0] if msg.get('text') else ''
                if chat_id != str(C.telegram_chat):
                    continue
                if text:
                    state = load_state()
                    telegram_send(telegram_command(text.lower(), state))
        except Exception:
            log.exception('Telegram control loop error')
            time.sleep(5)


# ---------------- Optional Binance user-data WebSocket ----------------
class UserStreamMonitor:
    """Optional order/position event monitor.

    The monitor is intentionally observational: REST reconciliation remains the
    source of truth for startup safety. Binance documents user-data streams as
    the preferred low-latency source for order/position updates and requires
    keepalive/reconnect handling.
    """
    def __init__(self, state):
        self.state = state
        self.stop = threading.Event()
        self.thread = None

    def start(self):
        if C.dry or not C.key or not C.secret or websocket is None:
            return
        self.thread = threading.Thread(target=self.run, name='binance-user-stream', daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                data = signed('POST', '/fapi/v1/listenKey')
                listen_key = data.get('listenKey')
                if not listen_key:
                    raise RuntimeError('No listenKey returned')
                ws_url = f'{C.ws_base.rstrip("/")}/{listen_key}'
                ws = websocket.create_connection(ws_url, timeout=30)
                ws.settimeout(30)
                last_keepalive = time.time()
                while not self.stop.is_set():
                    if time.time() - last_keepalive > 45 * 60:
                        try:
                            signed('PUT', '/fapi/v1/listenKey')
                            last_keepalive = time.time()
                        except Exception:
                            log.exception('User stream keepalive failed')
                            break
                    try:
                        raw = ws.recv()
                        if not raw:
                            break
                        event = json.loads(raw)
                        self.state['ws_last_event'] = int(time.time())
                        save_state(self.state)
                        if event.get('e') in ('ORDER_TRADE_UPDATE', 'ACCOUNT_UPDATE', 'listenKeyExpired'):
                            log.info('User stream event: %s', event.get('e'))
                    except Exception as exc:
                        if 'timed out' not in str(exc).lower():
                            log.warning('User stream receive error: %s', exc)
                            break
                try:
                    ws.close()
                except Exception:
                    pass
            except Exception:
                log.exception('User stream connection failed; retrying')
                time.sleep(10)

    def close(self):
        self.stop.set()


# ---------------- Strategy runner ----------------
def run_once():
    state = load_state()
    state = refresh_realized_pnl(state)
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

    eq = account_equity() if C.key and C.secret and not C.dry else 1000.0
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
    notify(f'Binance Scalper V2: {sig} {C.symbol} qty={qty} price={price:.4f} mode={"DRY" if C.dry else "LIVE"}')
    return result


def main():
    if not C.dry and (not C.key or not C.secret):
        raise SystemExit('Live mode requires BINANCE_API_KEY and BINANCE_API_SECRET')
    state = load_state()
    if not C.dry:
        reconciliation = reconcile_account()
        state['last_reconcile'] = int(time.time())
        save_state(state)
        notify(f'🔄 Startup reconciliation: {reconciliation}')

    stop_event = threading.Event()
    tg = threading.Thread(target=telegram_loop, args=(stop_event,), name='telegram-control', daemon=True)
    tg.start()
    ws = UserStreamMonitor(state)
    ws.start()

    try:
        while not stop_event.is_set():
            try:
                run_once()
            except Exception:
                log.exception('Strategy cycle failed; no new trade this cycle')
            stop_event.wait(C.poll_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        ws.close()


if __name__ == '__main__':
    main()
