"""1m meme momentum engine. PAPER/DRY-RUN ONLY.

Designed for fast operation: REST seeds closed 1m/5m candles, websocket keeps
live trade/book state, scoring is exception-safe, and diagnostics identify the
exact rejection stage. It never enables live execution.
"""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
DISCOVERY_N=max(30,int(os.getenv("DISCOVERY_N","30")))
EXECUTION_N=max(10,int(os.getenv("EXECUTION_N","10")))
REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30"))
MIN24=float(os.getenv("MIN_24H_QUOTE_VOLUME","300000"))
MIN1=float(os.getenv("MIN_1M_QUOTE_VOLUME","8000"))
ENTRY=float(os.getenv("ENTRY_SCORE","0.66"))
EDGE=float(os.getenv("MIN_EDGE_BPS","9"))
MAX_SPREAD=float(os.getenv("MAX_SPREAD_BPS","15"))
WARMUP=max(20,int(os.getenv("WARMUP_BARS","25")))
FEE=float(os.getenv("EST_FEE_BPS","4")); SLIP=float(os.getenv("EST_SLIPPAGE_BPS","3"))
MAX_POS=min(int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","10")),EXECUTION_N); MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","300"))
RISK=float(os.getenv("BASE_RISK_PCT","0.002")); START=float(os.getenv("SIM_START_EQUITY","10000"))
MEME=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","MAGA","LADYS","PONKE","MEW","MICHI","ANDY","SLERF","MOTHER","GIGA","MUMU","PORK","COQ","KISHU","ELON","SAMO","BANANA","CATI","HMSTR","CHILLGUY","VINE","ANIME","PENGU","PONS","HAJIMI")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("alpha-1m"); http=requests.Session(); lock=threading.RLock()
hist={}; state={}; cache={}; ranked=[]; desired=[]; positions={}; last_entry={}; regime="UNKNOWN"
metrics={"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,"pnl":0.0}


def api(path,params=None):
    for i in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=7); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if i==2: raise
            time.sleep(.3*(i+1))
    raise RuntimeError("REST failure")


def st(s):
    return state.setdefault(s,{"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0})


def ema(xs,n):
    if not xs:return 0.0
    k=2/(n+1); e=float(xs[0])
    for x in xs[1:]:e=float(x)*k+e*(1-k)
    return e


def avg(xs,d=0.0):
    return mean(xs) if xs else d


def seed(s,iv="1m",limit=80):
    k=api("/fapi/v1/klines",{"symbol":s.upper(),"interval":iv,"limit":limit})
    if len(k)<WARMUP:return False
    if iv=="1m":
        h=deque(maxlen=120)
        for x in k[:-1]:
            q=float(x[7]); tb=float(x[10])*float(x[4])
            h.append({"t":int(x[0]),"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"q":q,"buy":tb,"sell":max(q-tb,0.0),"n":int(x[8])})
        hist[s]=h
        x=k[-1]; z=st(s); z["last"]=float(x[4])
        z["bid"]=z["bid"] or z["last"]; z["ask"]=z["ask"] or z["last"]
        cache.setdefault(s,{})["5m_at"]=0
    else:
        c=[float(x[4]) for x in k]; q=[float(x[7]) for x in k]
        cache.setdefault(s,{})["5m"]={"e9":ema(c,9),"e21":ema(c,21),"mom":c[-1]/c[-4]-1,"rv":q[-1]/max(avg(q[-20:-1],1e-9),1e-9)}
        cache[s]["5m_at"]=time.time()
    return True


def live_bar(s,p,q,m,ts):
    h=hist.setdefault(s,deque(maxlen=120)); b=int(ts//60000)*60000; v=p*q; z=st(s)
    if not h or h[-1]["t"]!=b:h.append({"t":b,"o":p,"h":p,"l":p,"c":p,"q":v,"buy":v if not m else 0.0,"sell":v if m else 0.0,"n":1})
    else:
        x=h[-1]; x["h"]=max(x["h"],p); x["l"]=min(x["l"],p); x["c"]=p; x["q"]+=v; x["buy"]+=v if not m else 0.0; x["sell"]+=v if m else 0.0; x["n"]+=1
    z["last"]=p


def features(s):
    h=hist.get(s)
    if not h or len(h)<WARMUP:return None
    z=st(s); x=h[-1]; c=[a["c"] for a in h]; q=[a["q"] for a in h]
    prev=c[-2]; ret=x["c"]/prev-1; ret5=x["c"]/c[-6]-1; ret2=prev/c[-3]-1; accel=ret-ret2
    base=list(h)[-21:-1]; atr=avg([(a["h"]-a["l"])/max(a["c"],1e-9) for a in base],0.001)
    rv=x["q"]/max(avg(q[-21:-1]),1e-9); flow=(x["buy"]-x["sell"])/max(x["buy"]+x["sell"],1e-9)
    hi=max(a["h"] for a in base[-8:]); lo=min(a["l"] for a in base[-8:]); mid=(z["bid"]+z["ask"])/2
    spread=(z["ask"]-z["bid"])/max(mid,1e-9)*10000 if z["bid"] and z["ask"] else 0.0
    book=(z["bq"]-z["aq"])/max(z["bq"]+z["aq"],1e-9) if z["bq"]+z["aq"] else 0.0
    vol=avg([abs(c[i]/c[i-1]-1) for i in range(max(1,len(c)-12),len(c))],0.001); zm=abs(ret)/max(vol,1e-6)
    return {"ret":ret,"ret5":ret5,"accel":accel,"atr":atr,"rv":rv,"flow":flow,"spread":spread,"book":book,"break":ret>0 and x["c"]>hi or ret<0 and x["c"]<lo,"z":zm,"q":x["q"]}


def higher(s):
    c=cache.setdefault(s,{})
    if time.time()-c.get("5m_at",0)>45:
        if not seed(s,"5m",40):return None
    return c.get("5m")


def discover():
    info=api("/fapi/v1/exchangeInfo"); ticks={x["symbol"]:x for x in api("/fapi/v1/ticker/24hr")}; now=time.time(); out=[]
    for m in info.get("symbols",[]):
        s=m.get("symbol","")
        if m.get("status")!="TRADING" or m.get("contractType")!="PERPETUAL" or m.get("quoteAsset")!="USDT":continue
        t=ticks.get(s,{})
        try:q=float(t.get("quoteVolume",0)); ch=abs(float(t.get("priceChangePercent",0)))/100; age=(now*1000-float(m.get("onboardDate",now*1000)))/86400000
        except Exception:continue
        base=s.replace("USDT",""); hinted=any(k in base for k in MEME)
        if q<MIN24 and age>30 and not hinted:continue
        liq=min(math.log10(max(q,1))/8,1); mom=min(ch/.12,1); fresh=.25 if age<=7 else (.10 if age<=30 else 0); meme=.12 if hinted else 0
        radar=.38*mom+.32*liq+.18*min(q/20000000,1)+fresh+meme
        if radar>=.20 or hinted:out.append((s.lower(),min(radar,1),age,q))
    out.sort(key=lambda x:(x[1],x[3]),reverse=True); return out[:DISCOVERY_N]


def market_regime():
    try:
        if not hist.get("btcusdt") or len(hist["btcusdt"])<WARMUP:seed("btcusdt")
        h=hist.get("btcusdt"); c=[x["c"] for x in h]
        if len(c)<WARMUP:return "UNKNOWN"
        r=c[-1]/c[-4]-1; e9=ema(c,9); e21=ema(c,21)
        if abs(r)>.012:return "PANIC"
        return "TREND_UP" if e9>e21 and r>0 else "TREND_DOWN" if e9<e21 and r<0 else "CHOP"
    except Exception:return "UNKNOWN"


def score(s,radar,age,q,reject):
    f=features(s)
    if not f:reject["warmup"]+=1; return None
    if f["q"]<MIN1:reject["volume"]+=1; return None
    if f["spread"]>MAX_SPREAD:reject["spread"]+=1; return None
    h=higher(s)
    if not h:reject["5m"]+=1; return None
    side=1 if f["ret"]>=0 else -1; sf=f["flow"]*side; sb=f["book"]*side
    impulse=max(0,min((f["rv"]-1)/1.5,1)); accel=max(0,min(abs(f["accel"])/.003,1)); flow=max(0,min((sf+1)/2,1)); book=max(0,min((sb+1)/2,1)); z=max(0,min(f["z"]/3,1)); br=1 if f["break"] else 0
    trend=1 if ((h["e9"]>h["e21"])==bool(side>0)) else 0; mom=max(0,min(abs(h["mom"])/.01,1)) if ((h["mom"]*side)>0) else 0
    s=.25*impulse+.18*accel+.20*flow+.08*book+.10*z+.07*br+.07*trend+.05*mom
    if regime=="CHOP":s-=.06
    if regime=="PANIC":s-=.25
    if regime=="TREND_UP" and side>0:s+=.08
    if regime=="TREND_DOWN" and side<0:s+=.08
    s=max(0,min(1,s)); expected=max(abs(f["ret"])*18000,f["atr"]*20000,abs(f["ret5"])*12000); edge=expected-FEE-SLIP-f["spread"]
    eligible=s>=ENTRY and edge>=EDGE and impulse>=.18 and flow>=.54 and trend and regime!="PANIC"
    if not eligible:
        if s<ENTRY:reject["score"]+=1
        elif edge<EDGE:reject["edge"]+=1
        elif impulse<.18:reject["impulse"]+=1
        elif flow<.54:reject["flow"]+=1
        elif not trend:reject["trend"]+=1
        else:reject["regime"]+=1
    return {"symbol":s if False else s,"side":"BUY" if side>0 else "SELL","score":s,"edge":edge,"atr":f["atr"],"flow":sf,"book":sb,"z":f["z"],"rv":f["rv"],"ret":f["ret"],"eligible":eligible,"new":age<=7,"radar":radar}


def rank():
    global ranked,desired,regime
    d=discover(); reject={"warmup":0,"volume":0,"spread":0,"5m":0,"score":0,"edge":0,"impulse":0,"flow":0,"trend":0,"regime":0,"exception":0}
    for s,_,_,_ in d:
        try:
            if s not in hist or len(hist[s])<WARMUP:seed(s)
        except Exception as e:reject["warmup"]+=1; log.debug("seed %s: %s",s,e)
    regime=market_regime(); cand=[]
    for sym,r,a,q in d:
        try:
            c=score(sym,r,a,q,reject)
            if c:c["symbol"]=sym; cand.append(c)
        except Exception as e:
            reject["exception"]+=1; log.exception("SCORE %s failed",sym)
    cand.sort(key=lambda x:(x["eligible"],x["score"],x["edge"]),reverse=True); ranked=cand[:EXECUTION_N]; desired=[x["symbol"] for x in ranked]
    log.info("ALPHA 1M | regime=%s | discovered=%d | candidates=%d | eligible=%d | reject=%s | %s",regime,len(d),len(cand),sum(x["eligible"] for x in cand),reject," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge"]:.0f}bp/{x["side"]}' for x in ranked))


def enter(c):
    s=c["symbol"]; now=time.time(); z=st(s)
    if not c["eligible"] or s in positions or len(positions)>=MAX_POS or now-last_entry.get(s,0)<60:return
    px=z["ask"] if c["side"]=="BUY" else z["bid"]
    if px<=0:return
    notional=START*RISK/max(c["atr"]*1.8,.0025); notional=min(notional,START*.30)
    positions[s]={**c,"entry":px,"notional":notional,"opened":now}; last_entry[s]=now; metrics["entries"]+=1; metrics["signals"]+=1
    log.warning("ALPHA ENTRY %s %s score=%.2f edge=%.0fbp rv=%.2f flow=%.2f [DRY RUN]",c["side"],s.upper(),c["score"],c["edge"],c["rv"],c["flow"])


def manage():
    for s,p in list(positions.items()):
        z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
        if px<=0:continue
        ret=(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1); stop=max(p["atr"]*1.8,.0025); target=stop*2.2
        reason="TARGET" if ret>=target else "STOP" if ret<=-stop else "TIME" if time.time()-p["opened"]>=MAX_HOLD else None
        if reason:
            net=ret-2*(FEE+SLIP)/10000; metrics["pnl"]+=net; metrics["exits"]+=1
            if net>=0:metrics["wins"]+=1
            else:metrics["losses"]+=1
            positions.pop(s,None); log.warning("ALPHA EXIT %s %s net=%.3f%%",reason,s.upper(),net*100)


def on_message(ws,msg):
    try:
        d=json.loads(msg).get("data",{}); e=d.get("e"); s=d.get("s","").lower()
        if e=="bookTicker":
            z=st(s); z["bid"]=float(d.get("b",0)); z["ask"]=float(d.get("a",0)); z["bq"]=float(d.get("B",0)); z["aq"]=float(d.get("A",0))
        elif e=="aggTrade":live_bar(s,float(d["p"]),float(d["q"]),bool(d.get("m")),int(d.get("T",time.time()*1000)))
    except Exception as e:log.debug("ws message: %s",e)


def ws_loop():
    while True:
        try:
            syms=list(desired)
            if not syms:time.sleep(2);continue
            streams=sum(([f"{s}@aggTrade",f"{s}@bookTicker"] for s in syms),[])
            w=websocket.WebSocketApp(WS_BASE+"?streams="+"/".join(streams),on_message=on_message,on_error=lambda *_:None,on_close=lambda *_:None)
            w.run_forever(ping_interval=20,ping_timeout=10); time.sleep(1)
        except Exception as e:log.warning("WS reconnect: %s",e);time.sleep(2)


def main():
    if not DRY_RUN:raise RuntimeError("LIVE EXECUTION DISABLED: DRY_RUN must remain true")
    log.warning("1M ALPHA ENGINE START | discovery=%d execution=%d positions=%d | DRY_RUN=%s",DISCOVERY_N,EXECUTION_N,MAX_POS,DRY_RUN)
    threading.Thread(target=ws_loop,daemon=True).start(); last=0
    while True:
        now=time.time()
        if now-last>=REFRESH:
            try:rank()
            except Exception as e:log.exception("RANK FAILED: %s",e)
            last=now
        for c in list(ranked):enter(c)
        manage(); time.sleep(.5)

if __name__=="__main__":main()
