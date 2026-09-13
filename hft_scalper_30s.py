"""1-minute meme alpha research engine. DRY RUN ONLY.

Core idea: trade abnormal 1m impulses only when continuation beats exhaustion,
liquidity is acceptable, higher timeframe/regime agrees, and estimated edge
comfortably exceeds fees/slippage. No live execution is permitted.
"""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
DISCOVERY_N=int(os.getenv("DISCOVERY_N","30")); EXECUTION_N=int(os.getenv("EXECUTION_N","10"))
RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","20")); MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","10"))
MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","500000")); MIN_1M_QV=float(os.getenv("MIN_1M_QUOTE_VOLUME","15000"))
MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","10")); ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.72")); MIN_EDGE_BPS=float(os.getenv("MIN_EDGE_BPS","12"))
FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3")); COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","60")); MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","300"))
START_EQUITY=float(os.getenv("SIM_START_EQUITY","10000")); BASE_RISK=float(os.getenv("BASE_RISK_PCT","0.0025")); MAX_NOTIONAL_X=float(os.getenv("MAX_POSITION_NOTIONAL_X","0.35")); MAX_GROSS_X=float(os.getenv("MAX_GROSS_EXPOSURE_X","1.5")); MAX_SIDE_X=float(os.getenv("MAX_SAME_SIDE_EXPOSURE_X","0.9"))
DD_THROTTLE=float(os.getenv("DD_THROTTLE_PCT","0.08")); DD_STOP=float(os.getenv("DD_STOP_PCT","0.15")); VOL_CAP=float(os.getenv("VOL_CAP_PCT","0.015")); SHOCK_Z=float(os.getenv("SHOCK_Z","4.5"))
BAR_MS=60000; WARMUP=int(os.getenv("WARMUP_BARS","25")); KELLY_FRACTION=float(os.getenv("KELLY_FRACTION","0.20"))
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","1000","MAGA","LADYS","SATS","ORDI","RATS","PONKE","MEW","MICHI","ANDY","SLERF","MOTHER","GIGA","RETARDIO","MUMU","PORK","COQ","KISHU","ELON","SAMO","NEIROETH","BANANA","CATI","HMSTR","CHILLGUY","VINE","ANIME","PENGU")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s"); log=logging.getLogger("alpha-1m")
http=requests.Session(); lock=threading.RLock(); state={}; bars={}; history={}; positions={}; last_entry={}; ranked=[]; desired=[]; regime="UNKNOWN"; kill_until=0; peak=START_EQUITY
metrics={"events":0,"bars":0,"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,"net_pnl":0.0,"gross_profit":0.0,"gross_loss":0.0,"shock_kills":0}; trades=deque(maxlen=100)

def public(path,params=None):
    for i in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=8); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if i==2: raise
            time.sleep(.4*(i+1))

def ema(x,n):
    if not x:return 0.0
    k=2/(n+1); e=float(x[0])
    for v in x[1:]: e=float(v)*k+e*(1-k)
    return e

def mean_safe(x,d=0): return mean(x) if x else d

def st(s): return state.setdefault(s,{"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0})

def add_trade(s,p,q,m,ts):
    bucket=(ts//BAR_MS)*BAR_MS; z=st(s); old=bars.get(s); v=p*q
    if old is None or old["t"]!=bucket:
        if old: metrics["bars"]+=1
        bars[s]={"t":bucket,"o":p,"h":p,"l":p,"c":p,"qv":v,"buy":v if not m else 0.0,"sell":v if m else 0.0,"n":1}
    else:
        old["h"]=max(old["h"],p); old["l"]=min(old["l"],p); old["c"]=p; old["qv"]+=v; old["buy"]+=v if not m else 0.0; old["sell"]+=v if m else 0.0; old["n"]+=1
    z["last"]=p; metrics["events"]+=1

def feat(s):
    z=st(s); b=bars.get(s)
    if not b or z["bid"]<=0 or z["ask"]<=0:return None
    h=history.setdefault(s,deque(maxlen=100)); d=dict(b)
    if not h or h[-1]["t"]!=b["t"]: h.append(d)
    else:h[-1]=d
    if len(h)<WARMUP:return None
    c=[x["c"] for x in h]; q=[x["qv"] for x in h]; n=min(20,len(h)-1); base=h[-n-1:-1]; last=c[-1]; prev=c[-2]
    atr=mean_safe([(x["h"]-x["l"])/max(x["c"],1e-12) for x in base],0.0); rv=q[-1]/max(mean_safe(q[-n:-1],1e-12),1e-12); ret=last/prev-1; ret5=last/c[-6]-1 if len(c)>=6 else ret; prevret=prev/c[-3]-1 if len(c)>=3 else 0; accel=ret-prevret
    hi=max(x["h"] for x in base[-8:]); lo=min(x["l"] for x in base[-8:]); flow=(b["buy"]-b["sell"])/max(b["buy"]+b["sell"],1e-12); mid=(z["bid"]+z["ask"])/2; spread=(z["ask"]-z["bid"])/max(mid,1e-12)*10000; book=(z["bq"]-z["aq"])/max(z["bq"]+z["aq"],1e-12)
    ranges=[abs((x["c"]/h[-i-2]["c"])-1) for i,x in enumerate(h[-min(12,len(h)-1):])] if len(h)>3 else [atr]; vol=mean_safe(ranges,atr); zmove=abs(ret)/max(vol,1e-6)
    return {"c":last,"ret":ret,"ret5":ret5,"accel":accel,"atr":atr,"rv":rv,"flow":flow,"spread":spread,"book":book,"qv":b["qv"],"breakup":last>hi,"breakdn":last<lo,"z":zmove,"range":vol}

def higher(s):
    out={}
    for iv,lim in (("1m",40),("5m",30)):
        k=public("/fapi/v1/klines",{"symbol":s.upper(),"interval":iv,"limit":lim})
        if len(k)<20:return None
        c=[float(x[4]) for x in k]; v=[float(x[5]) for x in k]
        out[iv]={"e9":ema(c,9),"e21":ema(c,21),"mom":c[-1]/c[-4]-1,"vr":v[-1]/max(mean_safe(v[-20:-1],1e-12),1e-12)}
    return out

def discover():
    info=public("/fapi/v1/exchangeInfo"); ticks={x["symbol"]:x for x in public("/fapi/v1/ticker/24hr")}; now=int(time.time()*1000); out=[]
    for m in info["symbols"]:
        s=m["symbol"]
        if m.get("status")!="TRADING" or m.get("contractType")!="PERPETUAL" or m.get("quoteAsset")!="USDT":continue
        t=ticks.get(s,{})
        try:q=float(t.get("quoteVolume",0)); ch=abs(float(t.get("priceChangePercent",0)))/100; age=(now-int(m.get("onboardDate",now)))/86400000
        except:continue
        b=s.replace("USDT",""); hinted=any(x in b for x in MEME_HINTS)
        if q<MIN_24H_QV and age>30:continue
        liq=min(math.log10(max(q,1))/8,1); mom=min(ch/.10,1); new=.22 if age<=7 else (.10 if age<=30 else 0); radar=.40*mom+.32*liq+.16*min(q/20_000_000,1)+new+(.12 if hinted else 0)
        if radar>=.22 or hinted:out.append((s.lower(),min(radar,1),age,q))
    out.sort(key=lambda x:(x[1],x[3]),reverse=True); return out[:max(DISCOVERY_N,30)]

def market_regime():
    try:
        b=higher("BTCUSDT"); e=higher("ETHUSDT")
        if not b or not e:return "UNKNOWN"
        if abs(b["1m"]["mom"])>.012:return "PANIC"
        if b["1m"]["e9"]>b["1m"]["e21"] and b["5m"]["e9"]>b["5m"]["e21"] and e["5m"]["e9"]>e["5m"]["e21"]:return "TREND_UP"
        if b["1m"]["e9"]<b["1m"]["e21"] and b["5m"]["e9"]<b["5m"]["e21"] and e["5m"]["e9"]<e["5m"]["e21"]:return "TREND_DOWN"
        return "CHOP"
    except:return "UNKNOWN"

def score_candidate(s,radar,age,q):
    f=feat(s)
    if not f or f["qv"]<MIN_1M_QV or f["spread"]>MAX_SPREAD_BPS:return None
    h=higher(s)
    if not h:return None
    side="BUY" if f["ret"]>0 else "SELL"; signed=lambda x:x if side=="BUY" else -x
    impulse=min(max((f["rv"]-1)/2,0),1); accel=min(abs(f["accel"])/.004,1); flow=max(0,min((signed(f["flow"])+1)/2,1)); book=max(0,min((signed(f["book"])+1)/2,1)); z=min(f["z"]/3,1)
    breakv=1 if (f["breakup"] if side=="BUY" else f["breakdn"]) else 0
    trend=(1 if (h["1m"]["e9"]>h["1m"]["e21"])==(side=="BUY") else 0); ht=(1 if (h["5m"]["e9"]>h["5m"]["e21"])==(side=="BUY") else 0)
    # Exhaustion penalty: huge displacement without volume/flow confirmation is fade-prone.
    exhaustion=max(0,z-.9)*(1-flow)*.8 + (1 if (f["ret"]>0 and f["breakup"] and signed(f["flow"])<.05) or (f["ret"]<0 and f["breakdn"] and signed(f["flow"])<.05) else 0)*.5
    alignment=.55*trend+.45*ht; impulse_score=.28*impulse+.20*accel+.18*flow+.12*book+.10*z+.07*breakv+.05*alignment-exhaustion
    if regime=="CHOP": impulse_score-=.10
    if regime=="PANIC": impulse_score-=.50
    if regime=="TREND_UP" and side=="BUY":impulse_score+=.08
    if regime=="TREND_DOWN" and side=="SELL":impulse_score+=.08
    score=max(0,min(1,impulse_score)); expected=max(abs(f["ret"])*10000*1.8, f["atr"]*10000*2.0, abs(f["ret5"])*10000*1.2); edge=expected-(FEE_BPS+SLIPPAGE_BPS+f["spread"])
    # Entry requires impulse, confirmation and positive continuation pressure.
    eligible=score>=ENTRY_SCORE and edge>=MIN_EDGE_BPS and impulse>=.25 and flow>=.56 and book>=.51 and (trend or ht) and regime!="PANIC"
    return {"symbol":s,"side":side,"score":score,"edge_bps":edge,"atr":f["atr"],"spread":f["spread"],"flow":signed(f["flow"]),"book":signed(f["book"]),"rv":f["rv"],"z":f["z"],"new":age<=7,"eligible":eligible,"ret":f["ret"],"radar":radar,"age":age}

def equity():
    floating=0
    for s,p in positions.items():
        z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
        if px>0:floating+=p["notional"]*((px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1))
    return START_EQUITY+metrics["net_pnl"]*START_EQUITY+floating

def size(c):
    global peak
    eq=equity(); peak=max(peak,eq); dd=max(0,1-eq/max(peak,1e-9)); ddm=.5 if dd>=DD_THROTTLE else 1.0
    if dd>=DD_STOP:return 0
    if trades:
        wins=[x["net"] for x in trades if x["net"]>0]; losses=[abs(x["net"]) for x in trades if x["net"]<0]; wr=len(wins)/len(trades); b=(mean(wins)/max(mean(losses),1e-6)) if losses else 1.2; k=max(0,(wr*b-(1-wr))/max(b,1e-6))*KELLY_FRACTION
    else:k=.10
    vf=min(1,max(.15,VOL_CAP/max(c["atr"],1e-6))); ef=min(1,max(.35,c["edge_bps"]/25)); nf=.55 if c["new"] else 1; sf=min(1,max(.4,(c["score"]-.60)/.35)); stop=max(c["atr"]*1.8,.002); risk_usd=eq*BASE_RISK*max(k,.08)*ddm*vf*ef*nf*sf
    return min(risk_usd/stop,eq*MAX_NOTIONAL_X)

def exposure(side):return sum(p["notional"] for p in positions.values() if p["side"]==side)
def enter(c):
    global last_entry
    now=time.time(); s=c["symbol"]
    with lock:
        if not c["eligible"] or now-last_entry.get(s,0)<COOLDOWN or s in positions or len(positions)>=MAX_POSITIONS or now<kill_until:return
        n=size(c); eq=equity()
        if n<=0 or sum(p["notional"] for p in positions.values())+n>eq*MAX_GROSS_X or exposure(c["side"])+n>eq*MAX_SIDE_X:return
        z=st(s); px=z["ask"] if c["side"]=="BUY" else z["bid"]
        if px<=0:return
        positions[s]={**c,"entry":px,"notional":n,"opened":now,"mfe":0,"adds":0}; last_entry[s]=now; metrics["entries"]+=1; metrics["signals"]+=1
    log.warning("ALPHA ENTRY %s %s score=%.2f edge=%.0fbp rv=%.2f flow=%.2f book=%.2f z=%.2f notional=%.0f [DRY RUN]",c["side"],s.upper(),c["score"],c["edge_bps"],c["rv"],c["flow"],c["book"],c["z"],n)

def close(s,p,ret,reason):
    net=ret-(2*(FEE_BPS+SLIPPAGE_BPS)/10000); metrics["exits"]+=1; metrics["net_pnl"]+=net; trades.append({"net":net,"score":p["score"],"reason":reason})
    if net>=0:metrics["wins"]+=1; metrics["gross_profit"]+=net
    else:metrics["losses"]+=1; metrics["gross_loss"]+=abs(net)
    positions.pop(s,None); log.warning("ALPHA EXIT %s %s net=%.3f%% PF=%.2f",reason,s.upper(),net*100,metrics["gross_profit"]/max(metrics["gross_loss"],1e-12))

def manage():
    global kill_until
    now=time.time()
    # Portfolio shock: abnormal BTC move or a single meme move many ATRs beyond normal.
    shock=False
    try:
        b=higher("BTCUSDT")
        if b and abs(b["1m"]["mom"])>.015:shock=True
    except:pass
    for s,p in list(positions.items()):
        z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
        if px<=0:continue
        ret=(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1); p["mfe"]=max(p["mfe"],ret); age=now-p["opened"]; f=feat(s); risk=max(p["atr"]*1.8,.0025); target=risk*2.4
        if f and f["z"]>SHOCK_Z:shock=True
        flip=f and ((f["flow"]<-.10) if p["side"]=="BUY" else (f["flow"]>.10)); decay=f and ((f["ret"]<0) if p["side"]=="BUY" else (f["ret"]>0))
        reason=None
        if ret<=-risk:reason="STOP"
        elif ret>=target:reason="TARGET"
        elif ret>=risk and flip:reason="FLOW_REVERSAL"
        elif ret>risk*.5 and decay and age>45:reason="IMPULSE_DECAY"
        elif age>=MAX_HOLD:reason="TIME"
        if reason:close(s,p,ret,reason)
    if shock and positions:
        metrics["shock_kills"]+=1
        for s,p in list(positions.items()):
            z=st(s); px=z["bid"] if p["side"]=="BUY" else z["ask"]
            if px>0:close(s,p,(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1),"TAIL_KILL")
        kill_until=now+120; log.error("TAIL-RISK KILL: entries paused 120s")

def rank():
    global ranked,desired,regime
    regime=market_regime(); cand=[]
    for s,r,a,q in discover():
        try:
            c=score_candidate(s,r,a,q)
            if c:cand.append(c)
        except Exception as e:log.debug("candidate %s: %s",s,e)
    cand.sort(key=lambda x:(x["eligible"],x["score"],x["edge_bps"]),reverse=True); ranked=cand[:EXECUTION_N]; desired=[x["symbol"] for x in ranked]
    log.info("ALPHA 1M | regime=%s | candidates=%d | %s",regime,len(cand)," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge_bps"]:.0f}bp/{x["side"]}' for x in ranked[:10]))

def on_message(ws,msg):
    try:
        d=json.loads(msg).get("data",{}); e=d.get("e"); s=d.get("s","").lower()
        if e=="bookTicker":
            z=st(s); z["bid"]=float(d.get("b",0)); z["ask"]=float(d.get("a",0)); z["bq"]=float(d.get("B",0)); z["aq"]=float(d.get("A",0))
        elif e=="aggTrade":add_trade(s,float(d["p"]),float(d["q"]),bool(d.get("m")),int(d.get("T",time.time()*1000)))
    except Exception as e:log.debug("ws: %s",e)

def ws_loop():
    while True:
        try:
            syms=list(desired)
            if not syms:time.sleep(2);continue
            streams=[]
            for s in syms:streams += [f"{s}@aggTrade",f"{s}@bookTicker"]
            ws=websocket.WebSocketApp(WS_BASE+"?streams="+"/".join(streams),on_message=on_message,on_error=lambda w,e:log.debug("ws error %s",e),on_close=lambda w,c,m:log.info("ws reconnect"))
            ws.run_forever(ping_interval=20,ping_timeout=10); time.sleep(1)
        except Exception as e:log.warning("ws loop: %s",e);time.sleep(2)

def main():
    if not DRY_RUN:raise RuntimeError("LIVE EXECUTION DISABLED: DRY_RUN must remain true")
    log.warning("1M ALPHA ENGINE START | discovery=%d execution=%d positions=%d | DRY_RUN=%s",DISCOVERY_N,EXECUTION_N,MAX_POSITIONS,DRY_RUN)
    threading.Thread(target=ws_loop,daemon=True).start(); last=0
    while True:
        now=time.time()
        if now-last>=RANK_REFRESH:
            try:rank()
            except Exception as e:log.exception("rank failed: %s",e)
            last=now
        for c in list(ranked):enter(c)
        manage(); time.sleep(.5)

if __name__=="__main__":main()
