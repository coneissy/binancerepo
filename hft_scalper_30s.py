"""Cryptoalpha 3M ICT/SMC paper scalper.

15M = context, 5M = trend/structure and key levels, 3M = execution.
Entries require a 5M key-area retest plus 3M liquidity/structure confirmation.
SL is fixed at 1%, TP fixed at 2%, and live execution is permanently disabled.
"""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
DISCOVERY_N=max(30,int(os.getenv("DISCOVERY_N","50")))
EXECUTION_N=max(5,int(os.getenv("EXECUTION_N","10")))
REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30"))
MIN24=float(os.getenv("MIN_24H_QUOTE_VOLUME","500000"))
MIN3=float(os.getenv("MIN_3M_QUOTE_VOLUME","12000"))
ENTRY=float(os.getenv("ENTRY_SCORE","0.64"))
MAX_SPREAD=float(os.getenv("MAX_SPREAD_BPS","18"))
WARMUP=max(25,int(os.getenv("WARMUP_BARS","40")))
FEE=float(os.getenv("EST_FEE_BPS","4")); SLIP=float(os.getenv("EST_SLIPPAGE_BPS","3"))
MAX_POS=min(int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","8")),EXECUTION_N)
RISK=float(os.getenv("BASE_RISK_PCT","0.0025")); START=float(os.getenv("SIM_START_EQUITY","10000"))
MAX_DRAWDOWN=float(os.getenv("MAX_DRAWDOWN_PCT","0.08"))
SL_PCT=float(os.getenv("SL_PCT","0.01")); TP_PCT=float(os.getenv("TP_PCT","0.02"))
MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","900"))
MEME=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","MAGA","LADYS","PONKE","MEW","MICHI","ANDY","SLERF","MOTHER","GIGA","MUMU","PORK","COQ","KISHU","ELON","SAMO","BANANA","CATI","HMSTR","CHILLGUY","VINE","ANIME","PENGU","HAJIMI")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-3m"); http=requests.Session(); lock=threading.RLock()
hist={}; state={}; cache={}; ranked=[]; desired=[]; positions={}; last_entry={}; regime="UNKNOWN"
equity=START; peak_equity=START; trading_halted=False
metrics={"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,"pnl":0.0,"equity":START,"peak_equity":START,"drawdown":0.0,"trading_halted":False}


def api(path,params=None):
    for i in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=8); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if i==2: raise
            time.sleep(.4*(i+1))
    raise RuntimeError("REST failure")

def st(s): return state.setdefault(s,{"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0})
def ema(xs,n):
    if not xs:return 0.0
    k=2/(n+1); e=float(xs[0])
    for x in xs[1:]:e=float(x)*k+e*(1-k)
    return e
def avg(xs,d=0.0): return mean(xs) if xs else d

def seed(s,iv="3m",limit=120):
    k=api("/fapi/v1/klines",{"symbol":s.upper(),"interval":iv,"limit":limit})
    if len(k)<WARMUP:return False
    h=deque(maxlen=180)
    for x in k[:-1]:
        q=float(x[7]); tb=float(x[10])
        h.append({"t":int(x[0]),"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"q":q,"buy":tb,"sell":max(q-tb,0.0),"n":int(x[8])})
    hist.setdefault(s,{})[iv]=h
    x=k[-1]; z=st(s); z["last"]=float(x[4]); z["bid"]=z["bid"] or z["last"]; z["ask"]=z["ask"] or z["last"]
    return True

def live_bar(s,p,q,m,ts):
    books=hist.setdefault(s,{})
    h=books.setdefault("3m",deque(maxlen=180)); b=int(ts//180000)*180000; v=p*q; z=st(s)
    if not h or h[-1]["t"]!=b:h.append({"t":b,"o":p,"h":p,"l":p,"c":p,"q":v,"buy":v if not m else 0.0,"sell":v if m else 0.0,"n":1})
    else:
        x=h[-1]; x["h"]=max(x["h"],p); x["l"]=min(x["l"],p); x["c"]=p; x["q"]+=v; x["buy"]+=v if not m else 0.0; x["sell"]+=v if m else 0.0; x["n"]+=1
    z["last"]=p

def frame_features(s,iv):
    h=hist.get(s,{}).get(iv)
    if not h or len(h)<20:return None
    a=list(h); c=[x["c"] for x in a]; q=[x["q"] for x in a]
    return {"ema9":ema(c,9),"ema21":ema(c,21),"mom":c[-1]/c[-5]-1,"rv":q[-1]/max(avg(q[-21:-1],1e-9),1e-9),"high":max(x["h"] for x in a[-12:]),"low":min(x["l"] for x in a[-12:])}

def structure(s):
    h=hist.get(s,{}).get("3m")
    if not h or len(h)<WARMUP:return None
    a=list(h); x=a[-1]; prev=a[-2]; c=[z["c"] for z in a]
    ranges=[(z["h"]-z["l"])/max(z["c"],1e-9) for z in a[-21:-1]]; atr=avg(ranges,.001)
    flow=(x["buy"]-x["sell"])/max(x["buy"]+x["sell"],1e-9)
    spread=0.0; z=st(s); mid=(z["bid"]+z["ask"])/2
    if z["bid"] and z["ask"] and mid:spread=(z["ask"]-z["bid"])/mid*10000
    # 3M swing structure / liquidity.
    swing_hi=max(z["h"] for z in a[-8:-1]); swing_lo=min(z["l"] for z in a[-8:-1])
    sweep_long=x["l"]<swing_lo and x["c"]>swing_lo
    sweep_short=x["h"]>swing_hi and x["c"]<swing_hi
    bos_long=x["c"]>swing_hi and x["c"]>x["o"]
    bos_short=x["c"]<swing_lo and x["c"]<x["o"]
    # Three-candle FVG approximation: current low above candle-2 high, or inverse.
    bull_fvg=x["l"]>a[-3]["h"]; bear_fvg=x["h"]<a[-3]["l"]
    displacement=abs(x["c"]-x["o"])/max(x["c"],1e-9)>=max(atr*1.35,.0012)
    # 5M key area: recent range edge, plus fresh imbalance/order-block approximation.
    h5=hist.get(s,{}).get("5m")
    if not h5 or len(h5)<20:return None
    b=list(h5); y=b[-1]; r5=max(z["h"] for z in b[-12:-1]); l5=min(z["l"] for z in b[-12:-1]);
    bull_ob=max((z["h"]-z["l"])/max(z["c"],1e-9) for z in b[-8:-1]) if len(b)>8 else atr
    # Key zones are represented by the 5M range edge with a volatility tolerance.
    tol=max(atr*1.8,.0015); px=x["c"]
    near_low=abs(px-l5)/max(px,1e-9)<=tol or x["l"]<=l5*(1+tol)
    near_high=abs(px-r5)/max(px,1e-9)<=tol or x["h"]>=r5*(1-tol)
    return {"atr":atr,"flow":flow,"spread":spread,"sweep_long":sweep_long,"sweep_short":sweep_short,"bos_long":bos_long,"bos_short":bos_short,"bull_fvg":bull_fvg,"bear_fvg":bear_fvg,"displacement":displacement,"near_low":near_low,"near_high":near_high,"r5":r5,"l5":l5,"close":px,"bull_5":y["c"]>y["o"],"bear_5":y["c"]<y["o"]}

def ensure_frames(s):
    for iv in ("3m","5m","15m"):
        if iv not in hist.get(s,{}):
            if not seed(s,iv,100):return False
    return True

def higher_regime(s):
    a=frame_features(s,"15m"); b=frame_features(s,"5m")
    if not a or not b:return None
    bull15=a["ema9"]>a["ema21"] and a["mom"]>0; bear15=a["ema9"]<a["ema21"] and a["mom"]<0
    bull5=b["ema9"]>b["ema21"] and b["mom"]>0; bear5=b["ema9"]<b["ema21"] and b["mom"]<0
    return {"bull15":bull15,"bear15":bear15,"bull5":bull5,"bear5":bear5,"bias":1 if bull15 and bull5 else -1 if bear15 and bear5 else 0}

def discover():
    info=api("/fapi/v1/exchangeInfo"); ticks={x["symbol"]:x for x in api("/fapi/v1/ticker/24hr")}; now=time.time(); out=[]
    for m in info.get("symbols",[]):
        s=m.get("symbol","")
        if m.get("status")!="TRADING" or m.get("contractType")!="PERPETUAL" or m.get("quoteAsset")!="USDT":continue
        t=ticks.get(s,{})
        try:q=float(t.get("quoteVolume",0)); ch=float(t.get("priceChangePercent",0))/100; age=(now*1000-float(m.get("onboardDate",now*1000)))/86400000
        except Exception:continue
        base=s.replace("USDT",""); hinted=any(k in base for k in MEME)
        if q<MIN24 and age>30 and not hinted:continue
        liq=min(math.log10(max(q,1))/8,1); mover=min(abs(ch)/.12,1); fresh=.30 if age<=7 else (.15 if age<=30 else 0); meme=.16 if hinted else 0
        radar=.30*liq+.25*mover+fresh+meme
        if hinted or age<=30 or radar>=.28:out.append((s.lower(),min(radar,1),age,q))
    out.sort(key=lambda x:(x[1],x[3]),reverse=True); return out[:DISCOVERY_N]

def score(s,radar,age,q,reject):
    if not ensure_frames(s):reject["warmup"]+=1; return None
    f=structure(s); r=higher_regime(s)
    if not f or not r:reject["structure"]+=1; return None
    if f["spread"]>MAX_SPREAD:reject["spread"]+=1; return None
    h3=hist[s]["3m"]; x=h3[-1]
    if x["q"]<MIN3:reject["volume"]+=1; return None
    # Key rule: trade only inside the 5M trend and at a 5M range edge after a retest.
    long_loc=r["bull5"] and f["near_low"]
    short_loc=r["bear5"] and f["near_high"]
    long_trigger=f["sweep_long"] or (f["bos_long"] and f["bull_fvg"])
    short_trigger=f["sweep_short"] or (f["bos_short"] and f["bear_fvg"])
    side=1 if long_loc and long_trigger and r["bull15"] else -1 if short_loc and short_trigger and r["bear15"] else 0
    if side==0:
        reject["ict"]+=1; return {"symbol":"","side":"BUY" if long_loc else "SELL" if short_loc else "NEUTRAL","score":0.0,"edge":0.0,"atr":f["atr"],"flow":f["flow"],"eligible":False,"new":age<=7,"radar":radar,"ict":False}
    sf=f["flow"]*side
    displacement=1 if f["displacement"] else 0; sweep=1 if (f["sweep_long"] if side>0 else f["sweep_short"]) else 0; fvg=1 if (f["bull_fvg"] if side>0 else f["bear_fvg"]) else 0
    trend=1.0; align=max(0,min((sf+1)/2,1)); rv=max(0,min((x["q"]/max(avg([z["q"] for z in h3[-21:-1]],1e-9),1e-9)-1)/1.5,1));
    scorev=.27*trend+.22*sweep+.18*displacement+.14*fvg+.11*align+.05*rv+.03*min(radar,1)
    if sf<.05:scorev-=.10
    scorev=max(0,min(1,scorev)); edge=(TP_PCT*10000)-(2*(FEE+SLIP)+f["spread"])
    eligible=scorev>=ENTRY and edge>=10 and sf>=.05
    if not eligible:reject["quality"]+=1
    return {"symbol":"","side":"BUY" if side>0 else "SELL","score":scorev,"edge":edge,"atr":f["atr"],"flow":sf,"z":0.0,"rv":x["q"]/max(avg([z["q"] for z in h3[-21:-1]],1e-9),1e-9),"ret":x["c"]/h3[-2]["c"]-1,"eligible":eligible,"new":age<=7,"radar":radar,"ict":True,"sweep":bool(sweep),"fvg":bool(fvg),"displacement":bool(displacement),"trend15":r["bull15"] if side>0 else r["bear15"],"trend5":r["bull5"] if side>0 else r["bear5"],"key_level":"5M_LOW_RETEST" if side>0 else "5M_HIGH_RETEST"}

def rank():
    global ranked,desired,regime
    d=discover(); reject={"warmup":0,"structure":0,"spread":0,"volume":0,"ict":0,"quality":0,"exception":0}; cand=[]
    for s,_,_,_ in d:
        try:
            c=score(s,_[0] if False else next(v[1] for v in d if v[0]==s),next(v[2] for v in d if v[0]==s),next(v[3] for v in d if v[0]==s),reject)
            if c:c["symbol"]=s;cand.append(c)
        except Exception:reject["exception"]+=1
    cand.sort(key=lambda x:(x["eligible"],x["score"],x["edge"]),reverse=True); ranked=cand[:EXECUTION_N]; desired=[x["symbol"] for x in ranked]
    btc=frame_features("btcusdt","15m") if ensure_frames("btcusdt") else None
    regime="TREND_UP" if btc and btc["ema9"]>btc["ema21"] and btc["mom"]>0 else "TREND_DOWN" if btc and btc["ema9"]<btc["ema21"] and btc["mom"]<0 else "CHOP"
    log.info("CRYPTOALPHA 3M | regime=%s | discovered=%d | ranked=%d | eligible=%d | reject=%s | %s",regime,len(d),len(ranked),sum(x["eligible"] for x in cand),reject," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["side"]}/{x.get("key_level", "-")}' for x in ranked))

def enter(c):
    global trading_halted
    s=c["symbol"]; now=time.time(); z=st(s)
    if trading_halted or not c["eligible"] or s in positions or len(positions)>=MAX_POS or now-last_entry.get(s,0)<180:return
    px=z["ask"] if c["side"]=="BUY" else z["bid"]
    if px<=0:return
    risk_cash=equity*RISK; notional=min(risk_cash/SL_PCT,equity*.30)
    positions[s]={**c,"entry":px,"notional":notional,"opened":now,"risk_cash":risk_cash,"sl_pct":SL_PCT,"tp_pct":TP_PCT}; last_entry[s]=now; metrics["entries"]+=1; metrics["signals"]+=1
    log.warning("CRYPTOALPHA ENTRY %s %s score=%.2f key=%s sweep=%s fvg=%s notional=%.2f SL=%.2f%% TP=%.2f%% [DRY RUN]",c["side"],s.upper(),c["score"],c.get("key_level"),c.get("sweep"),c.get("fvg"),notional,SL_PCT*100,TP_PCT*100)

def manage():
    global equity,peak_equity,trading_halted
    for s,p in list(positions.items()):
        z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
        if px<=0:continue
        ret=(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1)
        reason="TARGET_2R" if ret>=TP_PCT else "STOP_1PCT" if ret<=-SL_PCT else "TIME" if time.time()-p["opened"]>=MAX_HOLD else None
        if reason:
            net=ret-2*(FEE+SLIP)/10000; cash_pnl=net*p["notional"]; equity+=cash_pnl; peak_equity=max(peak_equity,equity); dd=max(0,(peak_equity-equity)/max(peak_equity,1e-9)); metrics["pnl"]+=cash_pnl; metrics["equity"]=equity; metrics["peak_equity"]=peak_equity; metrics["drawdown"]=dd; metrics["exits"]+=1
            if cash_pnl>=0:metrics["wins"]+=1
            else:metrics["losses"]+=1
            positions.pop(s,None)
            if dd>=MAX_DRAWDOWN:trading_halted=True;metrics["trading_halted"]=True
            log.warning("CRYPTOALPHA EXIT %s %s net=%.3f%% cash=%.2f equity=%.2f dd=%.2f%%",reason,s.upper(),net*100,cash_pnl,equity,dd*100)

def on_message(ws,msg):
    try:
        d=json.loads(msg).get("data",{}); e=d.get("e"); s=d.get("s","").lower()
        if e=="bookTicker":
            z=st(s);z["bid"]=float(d.get("b",0));z["ask"]=float(d.get("a",0));z["bq"]=float(d.get("B",0));z["aq"]=float(d.get("A",0))
        elif e=="aggTrade":live_bar(s,float(d["p"]),float(d["q"]),bool(d.get("m")),int(d.get("T",time.time()*1000)))
    except Exception:pass

def ws_loop():
    while True:
        try:
            syms=list(desired)
            if not syms:time.sleep(2);continue
            streams=sum(([f"{s}@aggTrade",f"{s}@bookTicker"] for s in syms),[])
            w=websocket.WebSocketApp(WS_BASE+"?streams="+"/".join(streams),on_message=on_message,on_error=lambda *_:None,on_close=lambda *_:None);w.run_forever(ping_interval=20,ping_timeout=10);time.sleep(1)
        except Exception as e:log.warning("WS reconnect: %s",e);time.sleep(2)

def main():
    if not DRY_RUN:raise RuntimeError("LIVE EXECUTION DISABLED: DRY_RUN must remain true")
    log.warning("CRYPTOALPHA 3M ICT/SMC ENGINE START | 15m context + 5m trend/key levels + 3m entry | SL=%.2f%% TP=%.2f%% RISK=%.3f%% | DRY_RUN=%s",SL_PCT*100,TP_PCT*100,RISK*100,DRY_RUN)
    threading.Thread(target=ws_loop,daemon=True).start();last=0
    while True:
        now=time.time()
        if now-last>=REFRESH:
            try:rank()
            except Exception as e:log.exception("RANK FAILED: %s",e)
            last=now
        for c in list(ranked):enter(c)
        manage();time.sleep(.5)

if __name__=="__main__":main()