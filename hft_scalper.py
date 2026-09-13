"""Ultra+ Dynamic Top-20 meme futures engine. Dry-run first."""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
TOP_N=int(os.getenv("TOP_N","20")); CHALLENGER_N=int(os.getenv("CHALLENGER_N","10"))
RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30")); MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","3"))
ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.78")); CHALLENGER_GAP=float(os.getenv("CHALLENGER_GAP","0.06"))
MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","10")); MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","1000000"))
MIN_1M_QV=float(os.getenv("MIN_1M_QUOTE_VOLUME","50000")); COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","45"))
MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","180")); STATE_LEN=int(os.getenv("STATE_LEN","240"))
STATE_FILE=os.getenv("ULTRA_STATE_FILE","ultra_state.json"); FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3"))
MEME_BASES={x.strip().upper() for x in os.getenv("MEME_BASES","DOGE,SHIB,PEPE,FLOKI,BONK,WIF,BRETT,POPCAT,MEW,MOG,TURBO,NEIRO,ACT,PNUT,GOAT,MOODENG,TRUMP,MELANIA,SPX,FWOG,DEGEN,TOSHI,BOME,MYRO,SUNDOG,BABYDOGE,DOGS,CATI,WHY,1000SATS,1000BONK,1000PEPE,1000FLOKI,1000SHIB").split(",") if x.strip()}
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("ultra-plus"); http=requests.Session(); lock=threading.RLock()
state={}; positions={}; last_entry={}; metrics={"events":0,"signals":0,"entries":0,"exits":0,"ranking":0}
ranked_top20=[]; challengers=[]; regime="UNKNOWN"; desired_symbols=[]; ws_ref=None; ws_version=0


def public(path,params=None):
    for attempt in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=10); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if attempt==2: raise
            time.sleep(.5*(attempt+1))


def ema(xs,n):
    if not xs:return 0.0
    k=2/(n+1); e=float(xs[0])
    for x in xs[1:]:e=float(x)*k+e*(1-k)
    return e


def rsi(xs,n=14):
    if len(xs)<=n:return 50.0
    d=[xs[i]-xs[i-1] for i in range(1,len(xs))][-n:]; g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]; ag,al=mean(g),mean(l)
    if al==0:return 100.0
    return 100-100/(1+ag/al)


def atr_pct(ks,n=14):
    if len(ks)<=n:return 0.0
    trs=[]; prev=float(ks[-n-1][4]) if len(ks)>n else float(ks[0][4])
    for k in ks[-n:]:
        h,l,c=map(float,(k[2],k[3],k[4])); trs.append(max(h-l,abs(h-prev),abs(l-prev))); prev=c
    last=float(ks[-1][4]); return mean(trs)/last if last else 0.0


def make_state():
    return {"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0,"prices":deque(maxlen=STATE_LEN),"flow":deque(maxlen=STATE_LEN),"last_event":0.0,"features":{}}


def is_meme(symbol):
    base=symbol.upper().replace("USDT","")
    return base in MEME_BASES or (base.startswith("1000") and base[4:] in MEME_BASES) or any(h in base for h in MEME_HINTS)


def meme_universe():
    info=public("/fapi/v1/exchangeInfo"); allowed={x["symbol"] for x in info["symbols"] if x.get("status")=="TRADING" and x.get("contractType")=="PERPETUAL" and x.get("quoteAsset")=="USDT" and is_meme(x["symbol"])}
    tickers=public("/fapi/v1/ticker/24hr"); out=[]
    for t in tickers:
        s=t.get("symbol")
        if s not in allowed:continue
        try:
            qv=float(t.get("quoteVolume",0)); change=abs(float(t.get("priceChangePercent",0)))/100
            if qv>=MIN_24H_QV:out.append((s.lower(),qv,change))
        except (TypeError,ValueError):pass
    # Rank candidates broadly first; every meme symbol remains eligible, but this controls REST load.
    out.sort(key=lambda x:x[1]*max(x[2],.002),reverse=True)
    return out[:max(TOP_N+CHALLENGER_N+10,50)]


def kline_features(symbol):
    frames={}
    for interval,limit in (("1m",90),("5m",60),("15m",40),("1h",30)):
        ks=public("/fapi/v1/klines",{"symbol":symbol.upper(),"interval":interval,"limit":limit})
        if len(ks)<25:return None
        closes=[float(k[4]) for k in ks]; vols=[float(k[5]) for k in ks]; last=closes[-1]
        hi=max(float(k[2]) for k in ks[-20:]); lo=min(float(k[3]) for k in ks[-20:])
        frames[interval]={"close":last,"atr":atr_pct(ks),"rsi":rsi(closes),"mom":closes[-1]/closes[-4]-1,"mom_fast":closes[-1]/closes[-2]-1,"vol_ratio":vols[-1]/max(mean(vols[-21:-1]),1e-12),"qv_5":last*sum(vols[-5:]),"ema9":ema(closes,9),"ema21":ema(closes,21),"ema50":ema(closes,50) if len(closes)>=50 else ema(closes,len(closes)),"range_pos":(last-lo)/max(hi-lo,1e-12)}
    return frames


def market_regime(f):
    m1,m5,m15,h1=f["1m"],f["5m"],f["15m"],f["1h"]
    alignment=sum([m5["ema9"]>m5["ema21"],m15["ema9"]>m15["ema21"],h1["ema9"]>h1["ema21"]])
    if m1["atr"]>.025:return "PANIC"
    if m1["atr"]>.010:return "HIGH_VOL"
    if alignment in (0,3):return "TRENDING"
    if m1["vol_ratio"]>=2 and abs(m1["mom"])>=.003:return "BREAKOUT"
    if m1["atr"]<.001:return "LOW_LIQUIDITY"
    return "CHOP"


def score_symbol(symbol,qv,change,f):
    st=state.setdefault(symbol,make_state())
    if st["bid"]<=0 or st["ask"]<=0:return None
    m1,m5,m15,h1=f["1m"],f["5m"],f["15m"],f["1h"]; mid=(st["bid"]+st["ask"])/2
    spread=(st["ask"]-st["bid"])/mid*10000
    if spread>MAX_SPREAD_BPS or m1["qv_5"]<MIN_1M_QV:return None
    imb=(st["bq"]-st["aq"])/max(st["bq"]+st["aq"],1e-12); flow=list(st["flow"]); flow_acc=0
    if len(flow)>=30:flow_acc=sum(flow[-6:])/max(mean([abs(x) for x in flow[-30:-6]]),1e-9)
    side="BUY" if m1["mom"]>=0 else "SELL"
    trend=1.0 if ((m5["ema9"]>m5["ema21"]>m5["ema50"]) and side=="BUY") or ((m5["ema9"]<m5["ema21"]<m5["ema50"]) and side=="SELL") else 0
    htf=1.0 if ((m15["ema9"]>m15["ema21"] and h1["ema9"]>h1["ema21"])==(side=="BUY")) else 0
    vol=min(max((m1["vol_ratio"]-1)/2,0),1); mom=min(max(abs(m1["mom"])/.004,0),1)
    flow_score=min(max(abs(imb)*1.5+(.2 if (flow_acc>1 and side=="BUY") or (flow_acc<-1 and side=="SELL") else 0),0),1)
    breakout=1.0 if (m1["range_pos"]>.82 and side=="BUY") or (m1["range_pos"]<.18 and side=="SELL") else 0
    atr_quality=min(max(m1["atr"]/.006,0),1); liquidity=min(max(math.log10(max(qv,1))/9,0),1)*max(0,1-spread/MAX_SPREAD_BPS)
    chase=.35 if abs(m1["mom"])>.012 and m1["vol_ratio"]<2 else 0; htf_pen=.30 if htf==0 and abs(m1["mom"])>.003 else 0
    reg_pen=1 if regime in ("PANIC","LOW_LIQUIDITY") else (.55 if regime=="CHOP" else 0); cost=min((FEE_BPS+SLIPPAGE_BPS)/1000,.2)
    score=max(0,.18*trend+.16*htf+.16*vol+.16*mom+.12*flow_score+.08*breakout+.08*atr_quality+.06*liquidity-chase-htf_pen-reg_pen*.35-cost)
    return {"symbol":symbol,"side":side,"score":score,"spread_bps":spread,"qv":qv,"change":change,"atr":m1["atr"],"vol_ratio":m1["vol_ratio"],"momentum":m1["mom"],"rsi":m1["rsi"],"flow":flow_score,"regime":regime,"timestamp":time.time()}


def refresh_ranking():
    global ranked_top20,challengers,regime,desired_symbols,ws_version
    candidates=[]
    for symbol,qv,change in meme_universe():
        try:
            f=kline_features(symbol)
            if f:
                regime=market_regime(f); result=score_symbol(symbol,qv,change,f)
                if result:candidates.append(result)
        except Exception as exc:log.debug("rank %s failed: %s",symbol,exc)
    candidates.sort(key=lambda x:x["score"],reverse=True)
    selected=candidates[:TOP_N]; cutoff=selected[-1]["score"] if selected else 0
    old={x["symbol"]:x for x in ranked_top20}
    for s,x in old.items():
        challenger=next((c for c in candidates if c["symbol"]==s),None)
        if challenger and challenger["score"]>=cutoff-CHALLENGER_GAP and all(c["symbol"]!=s for c in selected):selected.append(challenger)
    selected.sort(key=lambda x:x["score"],reverse=True); ranked_top20=selected[:TOP_N]; top={x["symbol"] for x in ranked_top20}
    challengers=[x for x in candidates if x["symbol"] not in top][:CHALLENGER_N]
    desired_symbols=[x["symbol"] for x in ranked_top20+challengers]; metrics["ranking"]+=1; ws_version+=1
    log.info("DYNAMIC TOP-%d | regime=%s | %s",TOP_N,regime," ".join(f'{x["symbol"]}:{x["score"]:.2f}' for x in ranked_top20[:10]))


def confirm_entry(c):
    if regime in ("PANIC","LOW_LIQUIDITY") or c["score"]<ENTRY_SCORE:return False
    if c["vol_ratio"]<1.15 or abs(c["momentum"])<.0006 or c["flow"]<.15 or c["spread_bps"]>MAX_SPREAD_BPS:return False
    return time.time()-state[c["symbol"]]["last_event"]<=5


def enter(c):
    s=c["symbol"]; now=time.time()
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS or now-last_entry.get(s,0)<COOLDOWN or not confirm_entry(c):return
        positions[s]={**c,"entry":state[s]["ask"] if c["side"]=="BUY" else state[s]["bid"],"opened":now,"mfe":0,"mae":0}; last_entry[s]=now; metrics["entries"]+=1
    log.warning("ENTRY CONFIRMED %s %s score=%.3f regime=%s vol=%.2fx mom=%.3f%% [DRY RUN]",c["side"],s.upper(),c["score"],regime,c["vol_ratio"],c["momentum"]*100)


def manage_positions():
    now=time.time(); exits=[]
    with lock:
        for s,p in list(positions.items()):
            st=state.get(s)
            if not st:continue
            px=st["bid"] if p["side"]=="BUY" else st["ask"]
            if px<=0:continue
            ret=px/p["entry"]-1 if p["side"]=="BUY" else p["entry"]/px-1; p["mfe"]=max(p["mfe"],ret); p["mae"]=min(p["mae"],ret); age=now-p["opened"]; reason=None
            if ret<=-max(p["atr"]*1.15,.0025):reason="ADAPTIVE_STOP"
            elif ret>.0015 and p["mfe"]-ret>.0010:reason="MOMENTUM_DECAY"
            elif regime in ("PANIC","LOW_LIQUIDITY"):reason="REGIME_RISK"
            elif age>MAX_HOLD:reason="TIME_DECAY"
            else:
                sig=next((x for x in ranked_top20 if x["symbol"]==s),None)
                if sig and sig["side"]!=p["side"] and sig["score"]>=ENTRY_SCORE:reason="REVERSAL"
                elif sig and sig["score"]<ENTRY_SCORE*.78:reason="THESIS_DECAY"
            if reason:positions.pop(s,None); metrics["exits"]+=1; exits.append((s,p,px,ret,reason,age))
    for s,p,px,ret,reason,age in exits:log.warning("EXIT %s %s ret=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%%",reason,s.upper(),ret*100,age,p["mfe"]*100,p["mae"]*100)


def on_message(_,raw):
    try:
        d=json.loads(raw).get("data",{}); s=d.get("s","").lower(); event=d.get("e")
        if not s or s not in state:return
        st=state[s]; metrics["events"]+=1
        if event=="bookTicker":st["bid"],st["ask"]=float(d["b"]),float(d["a"]); st["bq"],st["aq"]=float(d["B"]),float(d["A"]); st["last_event"]=time.time()
        elif event=="aggTrade":
            px,qty=float(d["p"]),float(d["q"]); st["last"]=px; st["prices"].append(px); st["flow"].append(-px*qty if d.get("m") else px*qty); st["last_event"]=time.time()
    except Exception:log.exception("market-data message error")


def ws_loop():
    global ws_ref
    local_version=-1
    while True:
        try:
            if not desired_symbols:time.sleep(1);continue
            local_version=ws_version; url=WS_BASE+"?streams="+"/".join(f"{s}@bookTicker/{s}@aggTrade" for s in desired_symbols)
            ws=websocket.WebSocketApp(url,on_message=on_message); ws_ref=ws
            threading.Thread(target=lambda: (time.sleep(RANK_REFRESH+2), ws.close()),daemon=True).start()
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception:log.exception("websocket loop failed")
        time.sleep(.5)


def save_state():
    try:
        with open(STATE_FILE,"w",encoding="utf-8") as f:json.dump({"top20":ranked_top20,"challengers":challengers,"regime":regime,"metrics":metrics,"positions":positions},f,indent=2)
    except OSError:log.exception("state save failed")


def main():
    if not DRY_RUN:raise RuntimeError("Ultra+ is locked to DRY_RUN=true until paper results are validated.")
    log.warning("ULTRA+ STARTED | Dynamic Top-%d | meme universe | 1m entry | 5m/15m/1h confirmation | DRY RUN",TOP_N)
    threading.Thread(target=ws_loop,name="market-data",daemon=True).start()
    while True:
        try:
            refresh_ranking()
            for c in ranked_top20:enter(c)
        except Exception:log.exception("ranking cycle failed")
        manage_positions(); save_state()
        log.info("REGIME=%s TOP20=%d CHALLENGERS=%d POS=%d events=%d entries=%d exits=%d",regime,len(ranked_top20),len(challengers),len(positions),metrics["events"],metrics["entries"],metrics["exits"])
        time.sleep(RANK_REFRESH)

if __name__=="__main__":main()
