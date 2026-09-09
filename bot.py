import hashlib, hmac, logging, os, time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from statistics import mean
from urllib.parse import urlencode
import requests

logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO'), format='%(asctime)s %(levelname)s %(message)s')
log=logging.getLogger('scalper')

@dataclass
class Config:
    key:str=os.getenv('BINANCE_API_KEY','')
    secret:str=os.getenv('BINANCE_API_SECRET','')
    base:str=os.getenv('BINANCE_BASE_URL','https://demo-fapi.binance.com')
    symbol:str=os.getenv('BINANCE_SYMBOL','BTCUSDT')
    interval:str=os.getenv('BINANCE_INTERVAL','1m')
    leverage:int=int(os.getenv('BINANCE_LEVERAGE','3'))
    risk:float=float(os.getenv('RISK_PER_TRADE','0.0025'))
    max_daily_loss:float=float(os.getenv('MAX_DAILY_LOSS','0.01'))
    max_trades:int=int(os.getenv('MAX_TRADES_PER_DAY','10'))
    cooldown:int=int(os.getenv('COOLDOWN_SECONDS','300'))
    dry:bool=os.getenv('DRY_RUN','true').lower()=='true'

C=Config()
S=requests.Session()
S.headers.update({'X-MBX-APIKEY':C.key} if C.key else {})

def public(method,path,params=None):
    r=S.request(method,C.base+path,params=params,timeout=15); r.raise_for_status(); return r.json()

def signed(method,path,params=None):
    p=dict(params or {}); p['timestamp']=int(time.time()*1000); p['recvWindow']=5000
    q=urlencode(p); p['signature']=hmac.new(C.secret.encode(),q.encode(),hashlib.sha256).hexdigest()
    r=S.request(method,C.base+path,params=p,timeout=15); r.raise_for_status(); return r.json()

def closes(limit=120):
    return [float(x[4]) for x in public('GET','/fapi/v1/klines',{'symbol':C.symbol,'interval':C.interval,'limit':limit})]

def ema(xs,n):
    k=2/(n+1); e=xs[0]
    for x in xs[1:]: e=x*k+e*(1-k)
    return e

def rsi(xs,n=14):
    d=[xs[i]-xs[i-1] for i in range(1,len(xs))]; g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]
    ag=sum(g[-n:])/n; al=sum(l[-n:])/n
    if al==0:return 100.0
    return 100-100/(1+ag/al)

def atr_from_closes(xs,n=14):
    # V1 uses close-to-close true-range approximation; replace with OHLC ATR in V2.
    return mean(abs(xs[i]-xs[i-1]) for i in range(len(xs)-n,len(xs)))

def signal(xs):
    e9,e21=ema(xs,9),ema(xs,21); rr=rsi(xs); a=atr_from_closes(xs)
    if a<=0:return None
    last=xs[-1]
    if e9>e21 and rr>=55 and last>e9:return 'BUY'
    if e9<e21 and rr<=45 and last<e9:return 'SELL'
    return None

def exchange_rules():
    info=public('GET','/fapi/v1/exchangeInfo')
    s=next(x for x in info['symbols'] if x['symbol']==C.symbol)
    filters={x['filterType']:x for x in s['filters']}
    return float(filters['LOT_SIZE']['stepSize']), float(filters['LOT_SIZE']['minQty'])

def round_qty(q,step):
    d=Decimal(str(q)); st=Decimal(str(step)); return float((d/st).to_integral_value(rounding=ROUND_DOWN)*st)

def account_equity():
    rows=signed('GET','/fapi/v2/balance'); usdt=next(x for x in rows if x['asset']=='USDT'); return float(usdt['balance'])

def position():
    rows=signed('GET','/fapi/v2/positionRisk',{'symbol':C.symbol}); return float(rows[0]['positionAmt'])

def place(side,qty,stop_price,take_price):
    if C.dry:
        log.warning('DRY_RUN %s qty=%s stop=%s take=%s',side,qty,stop_price,take_price); return {'dry_run':True}
    signed('POST','/fapi/v1/leverage',{'symbol':C.symbol,'leverage':C.leverage})
    order=signed('POST','/fapi/v1/order',{'symbol':C.symbol,'side':side,'type':'MARKET','quantity':qty})
    exit_side='SELL' if side=='BUY' else 'BUY'
    signed('POST','/fapi/v1/order',{'symbol':C.symbol,'side':exit_side,'type':'STOP_MARKET','stopPrice':stop_price,'closePosition':'true','workingType':'MARK_PRICE'})
    signed('POST','/fapi/v1/order',{'symbol':C.symbol,'side':exit_side,'type':'TAKE_PROFIT_MARKET','stopPrice':take_price,'closePosition':'true','workingType':'MARK_PRICE'})
    return order

def notify(msg):
    token=os.getenv('TELEGRAM_BOT_TOKEN'); chat=os.getenv('TELEGRAM_CHAT_ID')
    if not token or not chat:return
    try: requests.post(f'https://api.telegram.org/bot{token}/sendMessage',json={'chat_id':chat,'text':msg},timeout=10)
    except Exception as e: log.warning('Telegram notification failed: %s',e)

def run_once():
    xs=closes(); sig=signal(xs); price=xs[-1]; log.info('%s price=%.4f signal=%s dry=%s',C.symbol,price,sig,C.dry)
    if not sig:return
    eq=account_equity() if C.key and C.secret else 1000.0
    risk_cash=eq*C.risk; atr=atr_from_closes(xs); stop_distance=max(atr*1.5,price*0.001)
    qty=round_qty(risk_cash/stop_distance,*exchange_rules()[:1])
    step,minq=exchange_rules()
    if qty<minq: log.warning('Calculated qty %.8f below exchange minimum %.8f',qty,minq); return
    stop=price-stop_distance if sig=='BUY' else price+stop_distance
    take=price+stop_distance*1.5 if sig=='BUY' else price-stop_distance*1.5
    side=sig
    if position()!=0: log.info('Position already open; skipping'); return
    result=place(side,qty,round(stop,4),round(take,4)); notify(f'Binance Scalper V1: {sig} {C.symbol} qty={qty} price={price:.4f} mode={"DRY" if C.dry else "LIVE"}')
    return result

if __name__=='__main__':
    if not C.dry and (not C.key or not C.secret): raise SystemExit('Live mode requires BINANCE_API_KEY and BINANCE_API_SECRET')
    run_once()
