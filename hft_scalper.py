"""Ultra+ algorithmic meme-futures research engine. Dry-run only."""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
TOP_N=int(os.getenv("TOP_N","20")); CHALLENGER_N=int(os.getenv("CHALLENGER_N","10"))
RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","30")); MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","3"))
ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.78")); MIN_EDGE_BPS=float(os.getenv("MIN_EDGE_BPS","10"))
MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","10")); MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","1000000"))
MIN_1M_QV=float(os.getenv("MIN_1M_QUOTE_VOLUME","50000")); COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","45"))
MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","180")); STATE_LEN=int(os.getenv("STATE_LEN","240"))
STATE_FILE=os.getenv("ULTRA_STATE_FILE","ultra_state.json"); FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3"))
MEME_BASES={x.strip().upper() for x in os.getenv("MEME_BASES","DOGE,SHIB,PEPE,FLOKI,BONK,WIF,BRETT,POPCAT,MEW,MOG,TURBO,NEIRO,ACT,PNUT,GOAT,MOODENG,TRUMP,MELANIA,SPX,FWOG,DEGEN,TOSHI,BOME,MYRO,SUNDOG,BABYDOGE,DOGS,CATI,WHY,1000SATS,1000BONK,1000PEPE,1000FLOKI,1000SHIB").split(",") if x.strip()}
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("ultra-plus"); http=requests.Session(); lock=threading.RLock()
state={}; positions={}; last_entry={}; ranked_top20=[]; challengers=[]; regime="UNKNOWN"; desired_symbols=[]
metrics={"events":0,"signals":0,"entries":0,"exits":0,"ranking":0,"wins":0,"losses":0,"net_pnl":0.0,"gross_profit":0.0,"gross_loss":0.0}


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
    trs=[]; prev=float(ks[-n-1][4])
    for k in ks[-n:]:
        h,l,c=map(float,(k[2],k[3],k[4])); trs.append(max(h-l,abs(h-prev),abs(l-prev))); prev=c
    last=float(ks[-1][4]); return mean(trs)/last if last else 0.0


def make_state():
    return {"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0,"prices":deque(maxlen=STATE_LEN),"flow":deque(maxlen=STATE_LEN),"last_event":0.0}


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
    out.sort(key=lambda x:x[1]*max(x[2],.002),reverse=True)
    return out[:max(TOP_N+CHALLENGER_N+10,50)]


def kline_features(symbol):
    frames={}
    for interval,limit in (("1m",90),("5m",60),("15m",40),("1h",30)):
        ks=public("/fapi/v1/klines",{"symbol":symbol.upper(),"interval":interval,"limit":limit})
        if len(ks)<25:return None
        closes=[float(k[4]) for k in ks]; vols=[float(k[5]) for k in ks]; last=closes[-1]
        hi=max(float(k[2]) for k in ks[-20:]); lo=min(float(k[3]) for k in ks[-20:])
        frames[interval]={"close":last,"atr":atr_pct(ks),"rsi":rsi(closes),"mom":last/closes[-4]-1,"mom_fast":last/closes[-2]-1,"vol_ratio":vols[-1]/max(mean(vols[-21:-1]),1e-12),"qv_5":last*sum(vols[-5:]),"ema9":ema(closes,9),"ema21":ema(closes,21),"ema50":ema(closes,50),"range_pos":(last-lo)/max(hi-lo,1e-12),"range_width":(hi-lo)/max(last,1e-12)}
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


def sigmoid(x):
    x=max(-12,min(12,x)); return 1/(1+math.exp(-x))


def score_symbol(symbol,qv,change,f):
    st=state.setdefault(symbol,make_state())
    if st["bid"]<=0 or st["ask"]<=0:return None
    m1,m5,m15,h1=f["1m"],f["5m"],f["15m"],f["1h"]; mid=(st["bid"]+st["ask"])/2
    spread=(st["ask"]-st["bid"])/mid*10000
    if spread>MAX_SPREAD_BPS or m1["qv_5"]<MIN_1M_QV:return None
    imb=(st["bq"]-st["aq"])/max(st["bq"]+st["aq"],1e-12); flow=list(st["flow"])
    flow_acc=sum(flow[-6:])/max(mean([abs(x) for x in flow[-30:-6]]),1e-9) if len(flow)>=30 else 0
    side="BUY" if m1["mom"]>=0 else "SELL"
    trend=1 if ((m5["ema9"]>m5["ema21"]>m5["ema50"]) and side=="BUY") or ((m5["ema9"]<m5["ema21"]<m5["ema50"]) and side=="SELL") else 0
    htf=1 if ((m15["ema9"]>m15["ema21"] and h1["ema9"]>h1["ema21"])==(side=="BUY")) else 0
    vol=min(max((m1["vol_ratio"]-1)/2,0),1); mom=min(max(abs(m1["mom"])/.004,0),1)
    flow_score=min(max(abs(imb)*1.5+(.2 if (flow_acc>1 and side=="BUY") or (flow_acc<-1 and side=="SELL") else 0),0),1)
    breakout=1 if (m1["range_pos"]>.82 and side=="BUY") or (m1["range_pos"]<.18 and side=="SELL") else 0
    atr_quality=min(max(m1["atr"]/.006,0),1); liquidity=min(max(math.log10(max(qv,1))/9,0),1)*max(0,1-spread/MAX_SPREAD_BPS)
    trend_strength=min(abs(m5["ema9"]/max(m5["ema21"],1e-12)-1)/.004,1)
    alignment=1 if ((m5["ema9"]>m5["ema21"])==(m15["ema9"]>m15["ema21"]) and (m15["ema9"]>m15["ema21"])==(h1["ema9"]>h1["ema21"])) else 0
    chase=min(abs(m1["mom"])/.02,1) if m1["vol_ratio"]<2 else 0
    exhaustion=1 if (m1["rsi"]>82 and side=="BUY") or (m1["rsi"]<18 and side=="SELL") else 0
    cost_bps=FEE_BPS+SLIPPAGE_BPS+spread
    expected_move_bps=max(m1["atr"]*10000*.65,abs(m1["mom"])*10000*.8)
    edge_bps=expected_move_bps-cost_bps
    raw=(1.35*trend+.95*htf+.75*vol+.85*mom+.65*flow_score+.55*breakout+.45*atr_quality+.45*liquidity+.55*trend_strength+.35*alignment-.55*chase-.75*exhaustion)
    score=sigmoid(raw-2.0)
    if regime in ("PANIC","LOW_LIQUIDITY"):score*=.35
    elif regime=="CHOP":score*=.72
    return {"symbol":symbol,"side":side,"score":score,"edge_bps":edge_bps,"spread_bps":spread,"qv":qv,"change":change,"atr":m1["atr"],"vol_ratio":m1["vol_ratio"],"momentum":m1["mom"],"rsi":m1["rsi"],"flow":flow_score,"trend_strength":trend_strength,"regime":regime,"timestamp":time.time()}


def refresh_ranking():
    global ranked_top20,challengers,regime,desired_symbols
    candidates=[]
    for symbol,qv,change in meme_universe():
        try:
            f=kline_features(symbol)
            if f:
                regime=market_regime(f); result=score_symbol(symbol,qv,change,f)
                if result and result["edge_bps"]>=MIN_EDGE_BPS:candidates.append(result)
        except Exception as exc:log.debug("rank %s failed: %s",symbol,exc)
    candidates.sort(key=lambda x:(x["score"],x["edge_bps"]),reverse=True)
    selected=candidates[:TOP_N]; cutoff=selected[-1]["score"] if selected else 0
    old={x["symbol"]:x for x in ranked_top20}
    for s in old:
        challenger=next((c for c in candidates if c["symbol"]==s),None)
        if challenger and challenger["score"]>=cutoff-.06 and all(c["symbol"]!=s for c in selected):selected.append(challenger)
    selected.sort(key=lambda x:x["score"],reverse=True); ranked_top20=selected[:TOP_N]; top={x["symbol"] for x in ranked_top20}
    challengers=[x for x in candidates if x["symbol"] not in top][:CHALLENGER_N]; desired_symbols=[x["symbol"] for x in ranked_top20+challengers]
    metrics["ranking"]+=1
    log.info("DYNAMIC TOP-%d | regime=%s | %s",TOP_N,regime," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge_bps"]:.0f}bp' for x in ranked_top20[:10]))


def confirm_entry(c):
    if regime in ("PANIC","LOW_LIQUIDITY") or c["score"]<ENTRY_SCORE or c["edge_bps"]<MIN_EDGE_BPS:return False
    if c["vol_ratio"]<1.15 or abs(c["momentum"])<.0006 or c["flow"]<.15 or c["spread_bps"]>MAX_SPREAD_BPS:return False
    if c["trend_strength"]<.15:return False
    return time.time()-state[c["symbol"]]["last_event"]<=5


def enter(c):
    s=c["symbol"]; now=time.time()
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS or now-last_entry.get(s,0)<COOLDOWN or not confirm_entry(c):return
        positions[s]={**c,"entry":state[s]["ask"] if c["side"]=="BUY" else state[s]["bid"],"opened":now,"mfe":0,"mae":0}; last_entry[s]=now; metrics["entries"]+=1
    log.warning("ENTRY %s %s score=%.3f edge=%.1fbp regime=%s vol=%.2fx [DRY RUN]",c["side"],s.upper(),c["score"],c["edge_bps"],regime,c["vol_ratio"])


def close_trade(s,p,px,ret,reason,age):
    net=ret-(FEE_BPS+SLIPPAGE_BPS)*2/10000
    metrics["exits"]+=1; metrics["net_pnl"]+=net
    if net>=0:metrics["wins"]+=1; metrics["gross_profit"]+=net
    else:metrics["losses"]+=1; metrics["gross_loss"]+=abs(net)
    log.warning("EXIT %s %s net=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%% PF=%.2f",reason,s.upper(),net*100,age,p["mfe"]*100,p["mae"]*100,metrics["gross_profit"]/max(metrics["gross_loss"],1e-12))


def manage_positions():
    now=time.time(); exits=[]
    with lock:
        for s,p in list(positions.items()):
            st=state.get(s)
            if not st:continue
            px=st["bid"] if p["side"]=="BUY" else st["ask"]
            if px<=0:continue
            ret=px/p["entry"]-1 if p["side"]=="BUY" else p["entry"]/px-1; p["mfe"]=max(p["mfe"],ret); p["mae"]=min(p["mae"],ret); age=now-p["opened"]; reason=None
            stop=max(p["atr"]*1.15,.0025)
            if ret<=-stop:reason="ADAPTIVE_STOP"
            elif ret>.0015 and p["mfe"]-ret>.0010:reason="MOMENTUM_DECAY"
            elif regime in ("PANIC","LOW_LIQUIDITY"):reason="REGIME_RISK"
            elif age>MAX_HOLD:reason="TIME_DECAY"
            else:
                sig=next((x for x in ranked_top20 if x["symbol"]==s),None)
                if sig and sig["side"]!=p["side"] and sig["score"]>=ENTRY_SCORE:reason="REVERSAL"
                elif sig and sig["score"]<ENTRY_SCORE*.78:reason="THESIS_DECAY"
            if reason:positions.pop(s,None); exits.append((s,p,px,ret,reason,age))
    for x in exits:close_trade(*x)


def on_message(_,raw):
    try:
        d=json.loads(raw).get("data",{}); s=d.get("s","").lower(); event=d.get("e")
        if not s:return
        st=state.setdefault(s,make_state()); now=time.time(); st["last_event"]=now; metrics["events"]+=1
        if event=="bookTicker":
            st["bid"]=float(d.get("b",0)); st["ask"]=float(d.get("a",0)); st["bq"]=float(d.get("B",0)); st["aq"]=float(d.get("A",0))
        elif event=="aggTrade":
            qty=float(d.get("q",0)); px=float(d.get("p",0)); buy=not bool(d.get("m")); st["last"]=px; st["prices"].append(px); st["flow"].append(qty if buy else -qty)
            if s in {x["symbol"] for x in ranked_top20}:metrics["signals"]+=1
    except Exception as exc:log.debug("ws message error: %s",exc)


def ws_loop():
    global ws_ref
    while True:
        try:
            streams=[]
            for s in desired_symbols:streams.extend([f"{s}@bookTicker",f"{s}@aggTrade"])
            if not streams:time.sleep(2);continue
            url=WS_BASE+"?streams="+"/".join(streams); ws_ref=websocket.WebSocketApp(url,on_message=on_message)
            ws_ref.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as exc:log.warning("WS reconnect: %s",exc)
        time.sleep(2)


def save_state():
    try:
        payload={"metrics":metrics,"timestamp":time.time()}; open(STATE_FILE,"w").write(json.dumps(payload,indent=2))
    except Exception as exc:log.debug("state save: %s",exc)


def main():
    if not DRY_RUN:raise RuntimeError("Live execution is intentionally disabled; DRY_RUN must remain true")
    log.warning("ULTRA+ ALGORITHMIC ENGINE START | DRY RUN ONLY")
    threading.Thread(target=ws_loop,daemon=True).start()
    last_rank=0
    while True:
        try:
            if time.time()-last_rank>=RANK_REFRESH:refresh_ranking();last_rank=time.time()
            for c in ranked_top20:
                if confirm_entry(c):enter(c)
            manage_positions(); save_state(); time.sleep(.5)
        except Exception as exc:log.exception("engine loop: %s",exc);time.sleep(2)


if __name__=="__main__":main()
