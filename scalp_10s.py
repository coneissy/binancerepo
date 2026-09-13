"""Ultra-fast 10-second meme futures research scalper.

Dry-run only. Uses aggTrade/bookTicker to build synthetic 10s bars and
requires 1m/5m confirmation before simulated entries. No live order path.
"""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
TOP_N=int(os.getenv("TOP_N","20")); RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30"))
MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","2"))
MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","1000000"))
MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","10"))
ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.82")); MIN_EDGE_BPS=float(os.getenv("MIN_EDGE_BPS","12"))
COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","20")); MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","20"))
FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3"))
RR=float(os.getenv("TARGET_RR","2.8")); STATE_FILE=os.getenv("ULTRA_STATE_FILE","scalp_10s_state.json")
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","1000")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("10s-scalper"); http=requests.Session(); lock=threading.RLock()
state={}; bars={}; ranked=[]; positions={}; last_entry={}; metrics={"events":0,"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,"net_pnl":0.0,"gross_profit":0.0,"gross_loss":0.0}


def public(path,params=None):
    for i in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=8); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if i==2: raise
            time.sleep(.3*(i+1))


def ema(xs,n):
    if not xs:return 0.0
    k=2/(n+1); e=float(xs[0])
    for x in xs[1:]:e=float(x)*k+e*(1-k)
    return e


def rsi(xs,n=14):
    if len(xs)<=n:return 50.0
    d=[xs[i]-xs[i-1] for i in range(1,len(xs))][-n:]; g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]
    ag,al=mean(g),mean(l)
    return 100.0 if al==0 else 100-100/(1+ag/al)


def sigmoid(x):
    x=max(-12,min(12,x)); return 1/(1+math.exp(-x))


def is_meme(s):
    b=s.upper().replace("USDT","")
    return any(h in b for h in MEME_HINTS)


def make_state():
    return {"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0,"last_event":0.0,"buy":deque(maxlen=80),"sell":deque(maxlen=80)}


def make_bar(bucket,price,qty,buy):
    return {"t":bucket,"o":price,"h":price,"l":price,"c":price,"q":price*qty,"buy":qty if buy else 0.0,"sell":0.0 if buy else qty,"n":1}


def update_bar(s,ts,price,qty,buy):
    bucket=(int(ts)//10000)*10000
    arr=bars.setdefault(s,deque(maxlen=90))
    if not arr or arr[-1]["t"]!=bucket:
        arr.append(make_bar(bucket,price,qty,buy))
    else:
        b=arr[-1]; b["h"]=max(b["h"],price); b["l"]=min(b["l"],price); b["c"]=price; b["q"]+=price*qty; b["n"]+=1
        if buy:b["buy"]+=qty
        else:b["sell"]+=qty
    st=state.setdefault(s,make_state()); st["last"]=price; st["last_event"]=time.time()
    st["buy"].append(qty if buy else 0.0); st["sell"].append(0.0 if buy else qty)


def micro_features(s):
    arr=bars.get(s,deque())
    if len(arr)<22:return None
    st=state[s]; closes=[b["c"] for b in arr]; vols=[b["q"] for b in arr]; cur=arr[-1]; prev=arr[-2]
    ret1=closes[-1]/closes[-2]-1; ret3=closes[-1]/closes[-4]-1; ret6=closes[-1]/closes[-7]-1
    accel=ret1-(closes[-2]/closes[-3]-1)
    rv=cur["q"]/max(mean(vols[-21:-1]),1e-9); trades=cur["n"]/max(mean(b["n"] for b in arr[-21:-1]),1e-9)
    buy=sum(b["buy"] for b in arr[-3:]); sell=sum(b["sell"] for b in arr[-3:]); flow=(buy-sell)/max(buy+sell,1e-12)
    prior_hi=max(b["h"] for b in arr[-11:-1]); prior_lo=min(b["l"] for b in arr[-11:-1])
    breakout=1 if cur["c"]>prior_hi else (-1 if cur["c"]<prior_lo else 0)
    ranges=[(b["h"]-b["l"])/max(b["c"],1e-12) for b in arr[-15:]]
    atr=mean(ranges); e9=ema(closes,9); e21=ema(closes,21); rs=rsi(closes)
    mid=(st["bid"]+st["ask"])/2 if st["bid"] and st["ask"] else cur["c"]
    spread=(st["ask"]-st["bid"])/max(mid,1e-12)*10000
    imb=(st["bq"]-st["aq"])/max(st["bq"]+st["aq"],1e-12)
    return {"price":cur["c"],"ret1":ret1,"ret3":ret3,"ret6":ret6,"accel":accel,"rv":rv,"trades":trades,"flow":flow,"breakout":breakout,"atr":atr,"ema9":e9,"ema21":e21,"rsi":rs,"spread":spread,"imb":imb,"qv":cur["q"]}


def higher(s):
    try:
        out={}
        for it,lim in (("1m",35),("5m",30)):
            ks=public("/fapi/v1/klines",{"symbol":s.upper(),"interval":it,"limit":lim})
            c=[float(x[4]) for x in ks]; v=[float(x[5]) for x in ks]
            if len(c)<25:return None
            out[it]={"ret":c[-1]/c[-4]-1,"e9":ema(c,9),"e21":ema(c,21),"rsi":rsi(c),"vr":v[-1]/max(mean(v[-21:-1]),1e-12)}
        return out
    except Exception:return None


def radar():
    info=public("/fapi/v1/exchangeInfo"); trad={x["symbol"] for x in info["symbols"] if x.get("status")=="TRADING" and x.get("contractType")=="PERPETUAL" and x.get("quoteAsset")=="USDT"}
    out=[]
    for t in public("/fapi/v1/ticker/24hr"):
        s=t.get("symbol");
        if s not in trad:continue
        try:
            q=float(t.get("quoteVolume",0)); ch=abs(float(t.get("priceChangePercent",0)))/100
            if q<MIN_24H_QV:continue
            meme=1.0 if is_meme(s) else 0.0
            behavior=min(ch/.10,1)*.65+min(math.log10(max(q,1))/9,1)*.20+meme*.15
            out.append((s.lower(),q,behavior))
        except (TypeError,ValueError):pass
    out.sort(key=lambda x:(x[2],x[1]),reverse=True); return out[:60]


def rank():
    global ranked
    candidates=[]
    for s,q,rad in radar():
        try:
            f=micro_features(s); h=higher(s)
            if not f or not h or f["spread"]>MAX_SPREAD_BPS:continue
            side=1 if f["ret3"]>0 else -1
            align=(h["1m"]["ret"]*side>0 and h["1m"]["e9"]*side>h["1m"]["e21"]*side and h["5m"]["e9"]*side>h["5m"]["e21"]*side)
            trigger=side*f["ret1"]>.00045 and side*f["accel"]>.00008
            flow=max(0,side*(f["flow"]*.65+f["imb"]*.35)); vol=min(f["rv"]/2.5,1); burst=min(f["trades"]/2.5,1)
            br=float(f["breakout"]==side); trend=float(align); momentum=min(abs(f["ret3"])/.0025,1)
            exhaustion=float((side>0 and f["rsi"]>84) or (side<0 and f["rsi"]<16))
            raw=1.35*trigger+1.15*flow+.95*vol+.65*burst+1.05*br+1.10*trend+.70*momentum+.25*rad-1.10*exhaustion
            score=sigmoid(raw-2.15)
            expected=max(abs(f["ret1"])*10000*2.2,abs(f["ret3"])*10000*.9,f["atr"]*10000*1.8)
            edge=expected-(FEE_BPS+SLIPPAGE_BPS+f["spread"])
            if score>=.60 and edge>0:candidates.append({"symbol":s,"side":"BUY" if side>0 else "SELL","score":score,"edge_bps":edge,"spread":f["spread"],"micro":f,"htf":h})
        except Exception as e:log.debug("rank %s: %s",s,e)
    candidates.sort(key=lambda x:(x["score"],x["edge_bps"]),reverse=True); ranked=candidates[:TOP_N]
    log.info("10S TOP-%d | %s",TOP_N," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge_bps"]:.0f}bp' for x in ranked[:10]))


def confirm(c):
    f=c["micro"]; h=c["htf"]; s=c["symbol"]; st=state.get(s)
    if not st or time.time()-st["last_event"]>2:return False
    if c["score"]<ENTRY_SCORE or c["edge_bps"]<MIN_EDGE_BPS or f["spread"]>MAX_SPREAD_BPS:return False
    side=1 if c["side"]=="BUY" else -1
    if side*f["ret1"]<.00045 or side*f["accel"]<.00008:return False
    if side*f["flow"]<.18 or side*f["imb"]<.05 or f["rv"]<1.35 or f["trades"]<1.2:return False
    if not (side*h["1m"]["ret"]>0 and side*h["5m"]["ret"]>0):return False
    if (side>0 and f["rsi"]>82) or (side<0 and f["rsi"]<18):return False
    return True


def enter(c):
    s=c["symbol"]; now=time.time()
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS or now-last_entry.get(s,0)<COOLDOWN or not confirm(c):return
        st=state[s]; entry=st["ask"] if c["side"]=="BUY" else st["bid"]; risk=max(c["micro"]["atr"]*1.25,.0012)
        target=risk*RR
        positions[s]={**c,"entry":entry,"opened":now,"risk":risk,"target":target,"mfe":0.0,"mae":0.0}; last_entry[s]=now; metrics["entries"]+=1
    log.warning("ENTRY 10S %s %s score=%.2f edge=%.0fbp SL=%.2f%% TP=%.2f%% [DRY RUN]",c["side"],s.upper(),c["score"],c["edge_bps"],risk*100,target*100)


def manage():
    now=time.time()
    for s,p in list(positions.items()):
        st=state.get(s)
        if not st or st["last"]<=0:continue
        px=st["last"]; side=1 if p["side"]=="BUY" else -1; ret=side*(px/p["entry"]-1); age=now-p["opened"]
        p["mfe"]=max(p["mfe"],ret); p["mae"]=min(p["mae"],ret)
        f=micro_features(s); reason=None
        if ret>=p["target"]:reason="TARGET"
        elif ret<=-p["risk"]:reason="STOP"
        elif age>=MAX_HOLD:reason="TIME"
        elif f:
            if side*f["flow"]<-.22 and side*f["ret1"]<0:reason="FLOW_FLIP"
            elif p["mfe"]>p["risk"]*.9 and ret<p["mfe"]*.45:reason="TRAIL"
        if reason:close(s,p,ret,reason,age)


def close(s,p,ret,reason,age):
    net=ret-2*(FEE_BPS+SLIPPAGE_BPS)/10000; metrics["exits"]+=1; metrics["net_pnl"]+=net
    if net>=0:metrics["wins"]+=1;metrics["gross_profit"]+=net
    else:metrics["losses"]+=1;metrics["gross_loss"]+=abs(net)
    pf=metrics["gross_profit"]/max(metrics["gross_loss"],1e-12); del positions[s]
    log.warning("EXIT 10S %s %s net=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%% PF=%.2f",reason,s.upper(),net*100,age,p["mfe"]*100,p["mae"]*100,pf)


def on_message(ws,msg):
    try:
        d=json.loads(msg).get("data",json.loads(msg)); typ=d.get("e"); s=d.get("s","").lower();
        if typ=="bookTicker":
            st=state.setdefault(s,make_state()); st["bid"]=float(d["b"]);st["ask"]=float(d["a"]);st["bq"]=float(d.get("B",0));st["aq"]=float(d.get("A",0))
        elif typ=="aggTrade":
            price=float(d["p"]);qty=float(d["q"]);buy=not bool(d.get("m",False));ts=float(d.get("T",time.time()*1000))
            update_bar(s,ts,price,qty,buy);metrics["events"]+=1
    except Exception as e:log.debug("ws: %s",e)


def ws_loop():
    while True:
        try:
            symbols=[x["symbol"] for x in ranked]
            if not symbols:time.sleep(2);continue
            streams="/".join(f"{s}.bookTicker/{s}.aggTrade" for s in symbols)
            url=WS_BASE+"?streams="+streams
            ws=websocket.WebSocketApp(url,on_message=on_message)
            ws.run_forever(ping_interval=15,ping_timeout=8)
        except Exception as e:log.warning("WS reconnect: %s",e)
        time.sleep(1)


def save():
    try:
        with open(STATE_FILE,"w") as f:json.dump({"metrics":metrics,"positions":positions},f,default=str)
    except Exception:pass


def main():
    if not DRY_RUN:raise RuntimeError("Live execution is intentionally disabled; DRY_RUN must remain true")
    threading.Thread(target=ws_loop,daemon=True).start(); last=0
    while True:
        now=time.time()
        if now-last>=RANK_REFRESH:
            try:rank()
            except Exception as e:log.warning("rank cycle failed: %s",e)
            last=now
        for c in ranked:
            if confirm(c):metrics["signals"]+=1;enter(c)
        manage();save();time.sleep(.20)


if __name__=="__main__":main()
