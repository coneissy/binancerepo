"""Buildix aggressive 3M paper scalper.
15M context -> 5M trend -> 3M execution. Volatility-direct entries, fixed risk, adaptive trailing stop, no martingale/doubling. Live execution remains disabled.
"""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=True
DISCOVERY_N=max(30,int(os.getenv("DISCOVERY_N","50"))); EXECUTION_N=max(5,int(os.getenv("EXECUTION_N","10")))
REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","20")); MIN24=float(os.getenv("MIN_24H_QUOTE_VOLUME","500000")); MIN3=float(os.getenv("MIN_3M_QUOTE_VOLUME","8000"))
ENTRY=float(os.getenv("ENTRY_SCORE","0.70")); QUALITY_MIN=float(os.getenv("QUALITY_MIN_SCORE","0.70")); MAX_SPREAD=float(os.getenv("MAX_SPREAD_BPS","22")); WARMUP=max(25,int(os.getenv("WARMUP_BARS","40")))
FEE=float(os.getenv("EST_FEE_BPS","4")); SLIP=float(os.getenv("EST_SLIPPAGE_BPS","3")); MAX_POS=min(int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","6")),EXECUTION_N)
RISK=float(os.getenv("BASE_RISK_PCT","0.0025")); START=float(os.getenv("SIM_START_EQUITY","10000")); MAX_DRAWDOWN=float(os.getenv("MAX_DRAWDOWN_PCT","0.08"))
SL_PCT=float(os.getenv("SL_PCT","0.01")); TP_PCT=float(os.getenv("TP_PCT","0.02")); MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","600"))
TRAIL_ACT=float(os.getenv("TRAIL_ACTIVATION_PCT","0.005")); TRAIL_NORMAL=float(os.getenv("TRAIL_NORMAL_PCT","0.006")); TRAIL_RISING=float(os.getenv("TRAIL_RISING_PCT","0.005")); TRAIL_HIGH=float(os.getenv("TRAIL_HIGH_PCT","0.004")); TRAIL_ALERT=float(os.getenv("TRAIL_ALERT_PCT","0.003"))
MEME=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","MAGA","LADYS","PONKE","MEW","MICHI","ANDY","SLERF","MOTHER","GIGA","MUMU","PORK","COQ","KISHU","ELON","SAMO","BANANA","CATI","HMSTR","CHILLGUY","VINE","ANIME","PENGU","HAJIMI")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("cryptoalpha-3m"); http=requests.Session(); lock=threading.RLock()
hist={}; state={}; cache={}; ranked=[]; desired=[]; positions={}; last_entry={}; regime="UNKNOWN"; equity=START; peak_equity=START; trading_halted=False
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
        q=float(x[7]); tb=float(x[10]); h.append({"t":int(x[0]),"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"q":q,"buy":tb,"sell":max(q-tb,0.0),"n":int(x[8])})
    hist.setdefault(s,{})[iv]=h; x=k[-1]; z=st(s); z["last"]=float(x[4]); z["bid"]=z["bid"] or z["last"]; z["ask"]=z["ask"] or z["last"]; return True

def live_bar(s,p,q,m,ts):
    h=hist.setdefault(s,{}).setdefault("3m",deque(maxlen=180)); b=int(ts//180000)*180000; v=p*q
    if not h or h[-1]["t"]!=b:h.append({"t":b,"o":p,"h":p,"l":p,"c":p,"q":v,"buy":v if not m else 0.0,"sell":v if m else 0.0,"n":1})
    else:
        x=h[-1]; x["h"]=max(x["h"],p); x["l"]=min(x["l"],p); x["c"]=p; x["q"]+=v; x["buy"]+=v if not m else 0.0; x["sell"]+=v if m else 0.0; x["n"]+=1
    st(s)["last"]=p

def frame_features(s,iv):
    h=hist.get(s,{}).get(iv)
    if not h or len(h)<20:return None
    a=list(h); c=[x["c"] for x in a]; q=[x["q"] for x in a]
    return {"ema9":ema(c,9),"ema21":ema(c,21),"mom":c[-1]/c[-5]-1,"rv":q[-1]/max(avg(q[-21:-1],1e-9),1e-9),"high":max(x["h"] for x in a[-12:]),"low":min(x["l"] for x in a[-12:])}

def structure(s):
    h=hist.get(s,{}).get("3m")
    if not h or len(h)<WARMUP:return None
    a=list(h); x=a[-1]; ranges=[(z["h"]-z["l"])/max(z["c"],1e-9) for z in a[-21:-1]]; atr=avg(ranges,.001); flow=(x["buy"]-x["sell"])/max(x["buy"]+x["sell"],1e-9); z=st(s); mid=(z["bid"]+z["ask"])/2; spread=((z["ask"]-z["bid"])/mid*10000) if z["bid"] and z["ask"] and mid else 0.0
    swing_hi=max(z["h"] for z in a[-8:-1]); swing_lo=min(z["l"] for z in a[-8:-1]); sweep_long=x["l"]<swing_lo and x["c"]>swing_lo; sweep_short=x["h"]>swing_hi and x["c"]<swing_hi; bos_long=x["c"]>swing_hi and x["c"]>x["o"]; bos_short=x["c"]<swing_lo and x["c"]<x["o"]; bull_fvg=x["l"]>a[-3]["h"]; bear_fvg=x["h"]<a[-3]["l"]; displacement=abs(x["c"]-x["o"])/max(x["c"],1e-9)>=max(atr*1.15,.0009)
    h5=hist.get(s,{}).get("5m")
    if not h5 or len(h5)<20:return None
    b=list(h5); y=b[-1]; r5=max(q["h"] for q in b[-12:-1]); l5=min(q["l"] for q in b[-12:-1]); px=x["c"]; tol=max(atr*2.5,.0020); near_low=abs(px-l5)/max(px,1e-9)<=tol or x["l"]<=l5*(1+tol); near_high=abs(px-r5)/max(px,1e-9)<=tol or x["h"]>=r5*(1-tol); rv=x["q"]/max(avg([q["q"] for q in a[-21:-1]],1e-9),1e-9); range_now=(x["h"]-x["l"])/max(x["c"],1e-9); range_ratio=range_now/max(atr,1e-9); vol_pressure=max(0,min(1,(rv-1)/2.0)); range_pressure=max(0,min(1,(range_ratio-1)/2.0)); momentum_pressure=max(0,min(1,abs(x["c"]/a[-5]["c"]-1)/.012)); volatility=min(1,.40*vol_pressure+.35*range_pressure+.25*momentum_pressure)
    return {"atr":atr,"flow":flow,"spread":spread,"sweep_long":sweep_long,"sweep_short":sweep_short,"bos_long":bos_long,"bos_short":bos_short,"bull_fvg":bull_fvg,"bear_fvg":bear_fvg,"displacement":displacement,"near_low":near_low,"near_high":near_high,"r5":r5,"l5":l5,"close":px,"bull_5":y["c"]>y["o"],"bear_5":y["c"]<y["o"],"rv":rv,"range_ratio":range_ratio,"volatility":volatility,"volatility_label":"ALERT" if volatility>=.70 else "HIGH" if volatility>=.50 else "RISING" if volatility>=.30 else "NORMAL"}

def ensure_frames(s):
    for iv in ("3m","5m","15m"):
        if iv not in hist.get(s,{}):
            if not seed(s,iv,100):return False
    return True

def higher_regime(s):
    a=frame_features(s,"15m"); b=frame_features(s,"5m")
    if not a or not b:return None
    bull15=a["ema9"]>a["ema21"] and a["mom"]>0; bear15=a["ema9"]<a["ema21"] and a["mom"]<0; bull5=b["ema9"]>b["ema21"] and b["mom"]>0; bear5=b["ema9"]<b["ema21"] and b["mom"]<0
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
        liq=min(math.log10(max(q,1))/8,1); mover=min(abs(ch)/.08,1); fresh=.30 if age<=7 else (.15 if age<=30 else 0); meme=.16 if hinted else 0; radar=min(1,.28*liq+.34*mover+fresh+meme)
        if hinted or age<=30 or radar>=.22:out.append((s.lower(),radar,age,q))
    out.sort(key=lambda x:(x[1],x[3]),reverse=True); return out[:DISCOVERY_N]

def score(s,radar,age,q,reject):
    # Stage 1: market/structure prerequisites.
    if not ensure_frames(s):reject["warmup"]+=1; return None
    f=structure(s); r=higher_regime(s)
    if not f or not r:reject["structure"]+=1; return None
    if f["spread"]>MAX_SPREAD:reject["spread"]+=1; return None
    h3=hist[s]["3m"]; x=h3[-1]
    if x["q"]<MIN3 and f["rv"]<.65:reject["volume"]+=1; return None

    # Stage 2: directional score. This stage does NOT authorize an entry.
    long_bias=r["bull15"] or r["bull5"]; short_bias=r["bear15"] or r["bear5"]; long_trigger=f["sweep_long"] or f["bos_long"] or f["bull_fvg"]; short_trigger=f["sweep_short"] or f["bos_short"] or f["bear_fvg"]; sf=f["flow"]
    long_score=.34*int(long_bias)+.20*int(long_trigger)+.14*int(f["displacement"])+.12*int(f["near_low"])+.10*max(0,min((sf+1)/2,1))+.10*f["volatility"]+.06*radar
    short_score=.34*int(short_bias)+.20*int(short_trigger)+.14*int(f["displacement"])+.12*int(f["near_high"])+.10*max(0,min((-sf+1)/2,1))+.10*f["volatility"]+.06*radar

    # Volatility-direct mode can improve the score, but can NEVER bypass the hard quality gate.
    direct_long=f["volatility"]>=.50 and x["c"]>x["o"] and (f["flow"]>=-.10 or radar>=.45)
    direct_short=f["volatility"]>=.50 and x["c"]<x["o"] and (f["flow"]<=.10 or radar>=.45)
    if not long_bias and not short_bias:
        long_score=.18*int(x["c"]>x["o"])+.24*int(long_trigger)+.20*f["volatility"]+.20*max(0,min((sf+1)/2,1))+.18*radar
        short_score=.18*int(x["c"]<x["o"])+.24*int(short_trigger)+.20*f["volatility"]+.20*max(0,min((-sf+1)/2,1))+.18*radar
    if direct_long: long_score=max(long_score,.53+.18*f["volatility"]+.08*radar)
    if direct_short: short_score=max(short_score,.53+.18*f["volatility"]+.08*radar)

    best=long_score if long_score>=short_score else short_score
    side=1 if long_score>=short_score else -1
    side_name="BUY" if side>0 else "SELL"
    if best<ENTRY:
        reject["ict"]+=1
        return {"symbol":"","side":side_name,"score":max(0,min(1,best)),"edge":0.0,"atr":f["atr"],"flow":sf*side,"eligible":False,"quality_pass":False,"quality_min":QUALITY_MIN,"new":age<=7,"radar":radar,"ict":False,"volatility":f["volatility"],"volatility_label":f["volatility_label"]}

    # Stage 3: HARD QUALITY GATE. Every condition must pass before eligible=True.
    scorev=best; sf_dir=sf*side; edge=TP_PCT*10000-(2*(FEE+SLIP)+f["spread"]); direct=(direct_long if side>0 else direct_short)
    quality_checks={
        "score":scorev>=QUALITY_MIN,
        "edge":edge>=10,
        "flow":sf_dir>=-0.15,
        "market_context":(direct or f["volatility"]>=.22 or radar>=.30 or f["displacement"]),
    }
    eligible=all(quality_checks.values())
    if not eligible:reject["quality"]+=1
    return {"symbol":"","side":side_name,"score":max(0,min(1,scorev)),"edge":edge,"atr":f["atr"],"flow":sf_dir,"z":0.0,"rv":f["rv"],"ret":x["c"]/h3[-2]["c"]-1,"eligible":eligible,"quality_pass":eligible,"quality_min":QUALITY_MIN,"quality_checks":quality_checks,"new":age<=7,"radar":radar,"ict":True,"volatility_direct":direct,"sweep":bool(f["sweep_long"] if side>0 else f["sweep_short"]),"fvg":bool(f["bull_fvg"] if side>0 else f["bear_fvg"]),"displacement":bool(f["displacement"]),"trend15":r["bull15"] if side>0 else r["bear15"],"trend5":r["bull5"] if side>0 else r["bear5"],"key_level":"5M_LOW_OR_BREAKOUT" if side>0 else "5M_HIGH_OR_BREAKOUT","volatility":f["volatility"],"volatility_label":f["volatility_label"],"range_ratio":f["range_ratio"]}

def rank():
    global ranked,desired,regime
    d=discover(); reject={"warmup":0,"structure":0,"spread":0,"volume":0,"ict":0,"quality":0,"exception":0}; cand=[]
    for s,radar,age,q in d:
        try:
            c=score(s,radar,age,q,reject)
            if c:c["symbol"]=s;cand.append(c)
        except Exception as e:reject["exception"]+=1;log.warning("SCORE FAILED | %s | %s",s.upper(),e)
    cand.sort(key=lambda x:(x["eligible"],x.get("volatility",0),x["score"],x["edge"]),reverse=True); ranked=cand[:EXECUTION_N]; desired=[x["symbol"] for x in ranked]
    try:
        if ensure_frames("btcusdt"):
            btc=frame_features("btcusdt","15m"); regime="TREND_UP" if btc and btc["ema9"]>btc["ema21"] and btc["mom"]>0 else "TREND_DOWN" if btc and btc["ema9"]<btc["ema21"] and btc["mom"]<0 else "CHOP"
    except Exception:regime="UNKNOWN"
    log.info("CRYPTOALPHA 3M | regime=%s | discovered=%d | inspected=%d | ranked=%d | eligible=%d | reject=%s | quality_min=%.2f | %s",regime,len(d),len(d),len(ranked),sum(x["eligible"] for x in cand),reject,QUALITY_MIN," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["side"]}/{x.get("volatility_label","-")}/{x.get("key_level","-")}/eligible={x["eligible"]}' for x in ranked))

def enter(c):
    global trading_halted
    s=c["symbol"]; now=time.time(); z=st(s)
    # Final defense-in-depth: entry requires the hard quality gate to be true.
    if trading_halted or not DRY_RUN or not c.get("quality_pass",False) or not c["eligible"] or c.get("score",0)<QUALITY_MIN or s in positions or len(positions)>=MAX_POS or now-last_entry.get(s,0)<120:return
    px=z["ask"] if c["side"]=="BUY" else z["bid"]
    if px<=0:return
    risk_cash=equity*RISK; notional=min(risk_cash/SL_PCT,equity*.30)
    positions[s]={**c,"entry":px,"notional":notional,"opened":now,"risk_cash":risk_cash,"sl_pct":SL_PCT,"tp_pct":TP_PCT,"peak":px,"trailing":False,"trail_stop":None}
    last_entry[s]=now; metrics["entries"]+=1; metrics["signals"]+=1
    log.warning("CRYPTOALPHA 3M ENTRY %s %s score=%.2f QUALITY_MIN=%.2f VOL=%s/%d%% DIRECT=%s radar=%d%% notional=%.2f SL=%.2f%% TP=%.2f%% TRAIL=ON FIXED-RISK NO-DOUBLING [DRY RUN]",c["side"],s.upper(),c["score"],QUALITY_MIN,c.get("volatility_label"),round(c.get("volatility",0)*100),c.get("volatility_direct",False),round(c.get("radar",0)*100),notional,SL_PCT*100,TP_PCT*100)

def trail_config(label):
    return {"NORMAL":TRAIL_NORMAL,"RISING":TRAIL_RISING,"HIGH":TRAIL_HIGH,"ALERT":TRAIL_ALERT}.get(label,TRAIL_NORMAL)

def manage():
    global equity,peak_equity,trading_halted
    for s,p in list(positions.items()):
        z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
        if px<=0:continue
        ret=(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1)
        if p["side"]=="BUY": p["peak"]=max(p["peak"],px)
        else: p["peak"]=min(p["peak"],px)
        if ret>=TRAIL_ACT:
            p["trailing"]=True; dist=trail_config(p.get("volatility_label","NORMAL")); p["trail_stop"]=p["peak"]*(1-dist) if p["side"]=="BUY" else p["peak"]*(1+dist)
        trail_hit=p["trailing"] and ((p["side"]=="BUY" and px<=p["trail_stop"]) or (p["side"]=="SELL" and px>=p["trail_stop"]))
        reason="TRAIL_VOLATILITY" if trail_hit else "TARGET_2R" if ret>=TP_PCT else "STOP_1PCT" if ret<=-SL_PCT else "TIME" if time.time()-p["opened"]>=MAX_HOLD else None
        if reason:
            net=ret-2*(FEE+SLIP)/10000; cash_pnl=net*p["notional"]; equity+=cash_pnl; peak_equity=max(peak_equity,equity); dd=max(0,(peak_equity-equity)/max(peak_equity,1e-9)); metrics.update(pnl=metrics["pnl"]+cash_pnl,equity=equity,peak_equity=peak_equity,drawdown=dd,exits=metrics["exits"]+1); metrics["wins"]+=int(cash_pnl>=0); metrics["losses"]+=int(cash_pnl<0); positions.pop(s,None)
            if dd>=MAX_DRAWDOWN:trading_halted=True;metrics["trading_halted"]=True
            log.warning("CRYPTOALPHA 3M EXIT %s %s net=%.3f%% cash=%.2f equity=%.2f dd=%.2f%%",reason,s.upper(),net*100,cash_pnl,equity,dd*100)

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
            if not syms:time.sleep(1);continue
            streams=sum(([f"{s}@aggTrade",f"{s}@bookTicker"] for s in syms),[]); w=websocket.WebSocketApp(WS_BASE+"?streams="+"/".join(streams),on_message=on_message,on_error=lambda *_:None,on_close=lambda *_:None);w.run_forever(ping_interval=20,ping_timeout=10);time.sleep(1)
        except Exception as e:log.warning("WS reconnect: %s",e);time.sleep(2)

def main():
    log.warning("CRYPTOALPHA 3M START | HARD QUALITY GATE %.2f | VOLATILITY DIRECT CANNOT BYPASS QUALITY | ADAPTIVE TRAILING | FIXED RISK %.3f%% | SL %.2f%% | TP %.2f%% | NO DOUBLING | PAPER",QUALITY_MIN,RISK*100,SL_PCT*100,TP_PCT*100)
    threading.Thread(target=ws_loop,daemon=True).start(); last=0
    while True:
        now=time.time()
        if now-last>=REFRESH:
            try:rank()
            except Exception as e:log.exception("RANK FAILED: %s",e)
            last=now
        for c in list(ranked):enter(c)
        manage();time.sleep(.35)
if __name__=="__main__":main()
