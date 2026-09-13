"""Dynamic meme-universe 30-second futures research engine. DRY RUN ONLY."""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
TOP_N=int(os.getenv("TOP_N","50")); RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30"))
MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","25")); MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","12"))
MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","500000")); MIN_30S_QV=float(os.getenv("MIN_30S_QUOTE_VOLUME","5000"))
ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.68")); MIN_EDGE_BPS=float(os.getenv("MIN_EDGE_BPS","8"))
FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3"))
COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","30")); MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","180"))
BAR_MS=30000; WARMUP_BARS=int(os.getenv("WARMUP_BARS","10"))
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","1000","MAGA","LADYS","SATS","ORDI","RATS","PONKE","MEW","MICHI","ANDY","SLERF","MOTHER","GIGA","RETARDIO","MUMU","PORK","COQ","KISHU","ELON","SAMO","NEIROETH","BANANA","CATI","HMSTR","CHILLGUY","VINE","ANIME")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("30s-engine"); http=requests.Session(); lock=threading.RLock()
state={}; bars={}; bar_history={}; positions={}; last_entry={}; ranked=[]; desired_symbols=[]; market_regime="UNKNOWN"
metrics={"events":0,"bars":0,"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,"net_pnl":0.0,"gross_profit":0.0,"gross_loss":0.0}

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
    for x in xs[1:]: e=float(x)*k+e*(1-k)
    return e

def rsi(xs,n=14):
    if len(xs)<=n:return 50.0
    d=[xs[i]-xs[i-1] for i in range(1,len(xs))][-n:]; g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]; ag,al=mean(g),mean(l)
    return 100.0 if al==0 else 100-100/(1+ag/al)

def sigmoid(x):
    x=max(-12,min(12,x)); return 1/(1+math.exp(-x))

def is_meme(symbol):
    b=symbol.upper().replace("USDT",""); return any(h in b for h in MEME_HINTS)

def st(sym):
    return state.setdefault(sym,{"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"flow":deque(maxlen=240),"last":0.0})

def add_trade(sym,px,qty,buyer_maker,ts):
    s=st(sym); bucket=(ts//BAR_MS)*BAR_MS; q=px*qty; old=bars.get(sym)
    if old is None or old["t"]!=bucket:
        if old: metrics["bars"]+=1
        bars[sym]={"t":bucket,"o":px,"h":px,"l":px,"c":px,"v":qty,"qv":q,"buy":q if not buyer_maker else 0.0,"sell":q if buyer_maker else 0.0,"n":1}
    else:
        b=old; b["h"]=max(b["h"],px); b["l"]=min(b["l"],px); b["c"]=px; b["v"]+=qty; b["qv"]+=q; b["buy"]+=q if not buyer_maker else 0.0; b["sell"]+=q if buyer_maker else 0.0; b["n"]+=1
    s["last"]=px; s["flow"].append(q if not buyer_maker else -q); metrics["events"]+=1


def synthetic_features(sym):
    s=st(sym); b=bars.get(sym)
    if not b or s["bid"]<=0 or s["ask"]<=0:return None
    h=bar_history.setdefault(sym,deque(maxlen=80))
    if not h or h[-1]["t"]!=b["t"]: h.append(dict(b))
    else: h[-1]=dict(b)
    if len(h)<WARMUP_BARS:return None
    closes=[x["c"] for x in h]; vols=[x["qv"] for x in h]; last=closes[-1]; prev=closes[-2]
    look=min(21,len(h)-1); base=h[-look-1:-1] if len(h)>look else h[:-1]
    ranges=[(x["h"]-x["l"])/max(x["c"],1e-12) for x in base]; atr=mean(ranges) if ranges else 0.0
    rv=vols[-1]/max(mean(vols[-look:-1]),1e-12) if len(vols)>2 else 1.0
    nbreak=min(8,len(h)-1); hi=max(x["h"] for x in h[-nbreak-1:-1]); lo=min(x["l"] for x in h[-nbreak-1:-1])
    ret1=last/prev-1; ret3=last/closes[-min(4,len(closes))]-1; prevret=prev/closes[-3]-1 if len(closes)>=3 else 0.0; accel=ret1-prevret
    e9,e21=ema(closes,min(9,len(closes))),ema(closes,min(21,len(closes))); rr=rsi(closes,min(14,len(closes)-1))
    flow=(b["buy"]-b["sell"])/max(b["buy"]+b["sell"],1e-12); trades=b["n"]/max(mean(x["n"] for x in base),1e-12) if base else 1.0
    mid=(s["bid"]+s["ask"])/2; spread=(s["ask"]-s["bid"])/max(mid,1e-12)*10000; imbalance=(s["bq"]-s["aq"])/max(s["bq"]+s["aq"],1e-12)
    return {"close":last,"ret1":ret1,"ret3":ret3,"accel":accel,"atr":atr,"rv":rv,"ema9":e9,"ema21":e21,"rsi":rr,"flow":flow,"trades":trades,"spread":spread,"imbalance":imbalance,"qv":b["qv"],"up":last>hi,"dn":last<lo}

def higher(symbol):
    out={}
    for iv,lim in (("1m",30),("5m",25)):
        ks=public("/fapi/v1/klines",{"symbol":symbol.upper(),"interval":iv,"limit":lim})
        if len(ks)<20:return None
        c=[float(k[4]) for k in ks]; v=[float(k[5]) for k in ks]
        out[iv]={"ema9":ema(c,9),"ema21":ema(c,21),"mom":c[-1]/c[-4]-1,"vr":v[-1]/max(mean(v[-min(20,len(v)-1):-1]),1e-12)}
    return out

def universe():
    info=public("/fapi/v1/exchangeInfo"); meta={x["symbol"]:x for x in info["symbols"] if x.get("status")=="TRADING" and x.get("contractType")=="PERPETUAL" and x.get("quoteAsset")=="USDT"}; tickers={t.get("symbol"):t for t in public("/fapi/v1/ticker/24hr")}; now_ms=int(time.time()*1000); out=[]
    for s,m in meta.items():
        if not is_meme(s):continue
        t=tickers.get(s,{})
        try:
            q=float(t.get("quoteVolume",0)); ch=abs(float(t.get("priceChangePercent",0)))/100
            if q<MIN_24H_QV:continue
            age_days=(now_ms-int(m.get("onboardDate",now_ms)))/86400000 if m.get("onboardDate") else 9999
            new_bonus=.18 if age_days<=7 else (.08 if age_days<=30 else 0.0)
            radar=.42*min(ch/.10,1)+.28*min(math.log10(max(q,1))/8,1)+.20*min(q/20_000_000,1)+new_bonus
            out.append((s.lower(),q,min(radar,1),age_days))
        except (TypeError,ValueError):continue
    out.sort(key=lambda x:(x[2],x[1]),reverse=True); return out[:max(TOP_N*3,120)]

def global_regime():
    try:
        b=higher("BTCUSDT"); e=higher("ETHUSDT")
        if not b or not e:return "UNKNOWN"
        if abs(b["1m"]["mom"])>.012:return "PANIC"
        if b["1m"]["ema9"]>b["1m"]["ema21"] and b["5m"]["ema9"]>b["5m"]["ema21"] and e["5m"]["ema9"]>e["5m"]["ema21"]:return "RISK_ON"
        if b["1m"]["ema9"]<b["1m"]["ema21"] and b["5m"]["ema9"]<b["5m"]["ema21"] and e["5m"]["ema9"]<e["5m"]["ema21"]:return "RISK_OFF"
        return "CHOP"
    except Exception:return "UNKNOWN"

def rank():
    global ranked,desired_symbols,market_regime
    market_regime=global_regime(); cand=[]
    for sym,q,radar,age_days in universe():
        try:
            f=synthetic_features(sym); h=higher(sym)
            if not f or not h or f["spread"]>MAX_SPREAD_BPS or f["qv"]<MIN_30S_QV:continue
            side="BUY" if f["ret1"]>=0 else "SELL"; aligned=(h["1m"]["mom"]>0)==(side=="BUY") and (h["5m"]["ema9"]>h["5m"]["ema21"])==(side=="BUY")
            trigger=f["up"] if side=="BUY" else f["dn"]; flow=max(0,(f["flow"] if side=="BUY" else -f["flow"])); book=max(0,(f["imbalance"] if side=="BUY" else -f["imbalance"]))
            vol=min(max((f["rv"]-1)/1.5,0),1); accel=min(abs(f["accel"])/.0010,1); momentum=min(abs(f["ret3"])/.003,1); trade=min(f["trades"]/1.8,1); breakout=1 if trigger else 0; trend=1 if aligned else 0
            cost=FEE_BPS+SLIPPAGE_BPS+f["spread"]; expected=max(f["atr"]*10000*1.7,abs(f["ret3"])*10000*1.3,abs(f["accel"])*10000*2); edge=expected-cost
            raw=1.05*breakout+.95*accel+.85*vol+.80*flow+.65*book+.60*momentum+.45*trend+.30*trade+.30*radar-.35*(f["rsi"]>90 and side=="BUY")-.35*(f["rsi"]<10 and side=="SELL")
            if market_regime=="CHOP":raw-=.12
            if market_regime=="PANIC":raw-=.75
            if market_regime=="RISK_ON" and side=="BUY":raw+=.10
            if market_regime=="RISK_OFF" and side=="SELL":raw+=.10
            score=sigmoid(raw-1.45)
            cand.append({"symbol":sym,"side":side,"score":score,"edge_bps":edge,"spread":f["spread"],"atr":f["atr"],"rv":f["rv"],"flow":flow,"book":book,"accel":f["accel"],"momentum":f["ret3"],"rsi":f["rsi"],"regime":market_regime,"new":age_days<=7})
        except Exception as e:log.debug("rank %s: %s",sym,e)
    cand.sort(key=lambda x:(x["score"],x["edge_bps"]),reverse=True); ranked=cand[:TOP_N]; desired_symbols=[x["symbol"] for x in ranked]
    log.info("MEME TOP-%d | MAX=%d | regime=%s | universe=%d | %s",TOP_N,MAX_POSITIONS,market_regime,len(cand)," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge_bps"]:.0f}bp/{x["side"]}' for x in ranked[:8]))

def confirm(c):
    if c["score"]<ENTRY_SCORE or c["edge_bps"]<MIN_EDGE_BPS or c["regime"]=="PANIC":return False
    return c["rv"]>=1.15 and c["flow"]>=.12 and c["book"]>=.03 and abs(c["accel"])>=.00015 and c["spread"]<=MAX_SPREAD_BPS

def enter(c):
    s=c["symbol"]; now=time.time()
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS or now-last_entry.get(s,0)<COOLDOWN or not confirm(c):return
        side_count=sum(1 for p in positions.values() if p["side"]==c["side"])
        if side_count>=15:return
        q=st(s); px=q["ask"] if c["side"]=="BUY" else q["bid"]
        if px<=0:return
        positions[s]={**c,"entry":px,"opened":now,"mfe":0.0,"mae":0.0}; last_entry[s]=now; metrics["entries"]+=1; metrics["signals"]+=1
    log.warning("ENTRY 30S %s %s score=%.2f edge=%.0fbp flow=%.2f book=%.2f [DRY RUN]",c["side"],s.upper(),c["score"],c["edge_bps"],c["flow"],c["book"])

def close(s,p,ret,reason,age):
    net=ret-(FEE_BPS+SLIPPAGE_BPS)*2/10000; metrics["exits"]+=1; metrics["net_pnl"]+=net
    if net>=0:metrics["wins"]+=1; metrics["gross_profit"]+=net
    else:metrics["losses"]+=1; metrics["gross_loss"]+=abs(net)
    pf=metrics["gross_profit"]/max(metrics["gross_loss"],1e-12); log.warning("EXIT %s %s net=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%% PF=%.2f",reason,s.upper(),net*100,age,p["mfe"]*100,p["mae"]*100,pf); positions.pop(s,None)

def manage():
    now=time.time()
    for s,p in list(positions.items()):
        q=st(s); px=q["bid"] if p["side"]=="BUY" else q["ask"]
        if px<=0:continue
        ret=(px/p["entry"]-1)*(1 if p["side"]=="BUY" else -1); age=now-p["opened"]; p["mfe"]=max(p["mfe"],ret); p["mae"]=min(p["mae"],ret); risk=max(p["atr"]*1.5,.0018); target=risk*2.7; f=synthetic_features(s); reason=None
        flow_flip=f and ((f["flow"]<-.08) if p["side"]=="BUY" else (f["flow"]>.08)); momentum_flip=f and ((f["ret1"]<0) if p["side"]=="BUY" else (f["ret1"]>0))
        if ret<=-risk:reason="STOP"
        elif ret>=target:reason="2.7R"
        elif ret>=risk*.7 and flow_flip:reason="FLOW_TRAIL"
        elif ret>0 and momentum_flip and age>20:reason="MOMENTUM_DECAY"
        elif age>=MAX_HOLD:reason="TIME"
        if reason:close(s,p,ret,reason,age)

def on_message(ws,msg):
    try:
        d=json.loads(msg).get("data",{}); typ=d.get("e"); sym=d.get("s","").lower()
        if typ=="bookTicker":
            s=st(sym); s["bid"]=float(d.get("b",0)); s["ask"]=float(d.get("a",0)); s["bq"]=float(d.get("B",0)); s["aq"]=float(d.get("A",0))
        elif typ=="aggTrade":add_trade(sym,float(d["p"]),float(d["q"]),bool(d.get("m")),int(d.get("T",time.time()*1000)))
    except Exception as e:log.debug("ws message: %s",e)

def ws_loop():
    while True:
        try:
            syms=list(desired_symbols)
            if not syms:time.sleep(2);continue
            streams=[]
            for s in syms:streams += [f"{s}@aggTrade",f"{s}@bookTicker"]
            url=WS_BASE+"?streams="+"/".join(streams)
            ws=websocket.WebSocketApp(url,on_message=on_message,on_error=lambda w,e:log.debug("ws error: %s",e),on_close=lambda w,c,m:log.info("ws reconnect"))
            ws.run_forever(ping_interval=20,ping_timeout=10); time.sleep(1)
        except Exception as e:log.warning("ws loop: %s",e);time.sleep(2)

def main():
    if not DRY_RUN:raise RuntimeError("Live execution is intentionally disabled; DRY_RUN must remain true")
    log.warning("MEME-UNIVERSE START | TOP=%d | MAX=%d | DRY_RUN=%s | warmup=%d",TOP_N,MAX_POSITIONS,DRY_RUN,WARMUP_BARS)
    threading.Thread(target=ws_loop,daemon=True).start(); last_rank=0
    while True:
        now=time.time()
        if now-last_rank>=RANK_REFRESH:
            try:rank()
            except Exception as e:log.exception("rank failed: %s",e)
            last_rank=now
        for c in list(ranked):enter(c)
        manage();time.sleep(.25)

if __name__=="__main__":main()
