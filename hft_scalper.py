"""Adaptive Top-20 meme futures research engine. Dry-run only."""
import json, logging, math, os, threading, time
from collections import deque
from statistics import mean
import requests, websocket

BASE=os.getenv("BINANCE_BASE_URL","https://demo-fapi.binance.com")
WS_BASE=os.getenv("BINANCE_WS_BASE_URL","wss://fstream.binance.com/stream")
DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
TOP_N=int(os.getenv("TOP_N","20")); CHALLENGER_N=int(os.getenv("CHALLENGER_N","10"))
RANK_REFRESH=float(os.getenv("RANK_REFRESH_SECONDS","45")); MAX_POSITIONS=int(os.getenv("MAX_SIMULTANEOUS_POSITIONS","3"))
MIN_24H_QV=float(os.getenv("MIN_24H_QUOTE_VOLUME","1500000")); MIN_5M_QV=float(os.getenv("MIN_5M_QUOTE_VOLUME","75000"))
MAX_SPREAD_BPS=float(os.getenv("MAX_SPREAD_BPS","12")); ENTRY_SCORE=float(os.getenv("ENTRY_SCORE","0.72"))
MIN_EDGE_BPS=float(os.getenv("MIN_EDGE_BPS","12")); COOLDOWN=float(os.getenv("ENTRY_COOLDOWN_SECONDS","60"))
MAX_HOLD=float(os.getenv("MAX_HOLD_SECONDS","240")); FEE_BPS=float(os.getenv("EST_FEE_BPS","4")); SLIPPAGE_BPS=float(os.getenv("EST_SLIPPAGE_BPS","3"))
STATE_FILE=os.getenv("ULTRA_STATE_FILE","ultra_state.json"); STATE_LEN=180
MEME_HINTS=("DOGE","SHIB","PEPE","FLOKI","BONK","WIF","MEME","MOG","TURBO","PNUT","GOAT","POPCAT","NEIRO","BOME","DOGS","CAT","PIG","TRUMP","MELANIA","BRETT","MOODENG","ACT","SPX","FWOG","DEGEN","TOSHI","MYRO","SUNDOG","BABY","WHY","1000")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("adaptive-top20")
http=requests.Session(); lock=threading.RLock()
state={}; positions={}; last_entry={}; ranked_top20=[]; challengers=[]; desired_symbols=[]
market_regime_name="UNKNOWN"
metrics={"events":0,"signals":0,"entries":0,"exits":0,"ranking":0,"wins":0,"losses":0,"net_pnl":0.0,"gross_profit":0.0,"gross_loss":0.0}


def public(path,params=None):
    for attempt in range(3):
        try:
            r=http.get(BASE+path,params=params,timeout=10); r.raise_for_status(); return r.json()
        except requests.RequestException:
            if attempt==2: raise
            time.sleep(.4*(attempt+1))


def ema(xs,n):
    if not xs:return 0.0
    k=2/(n+1); e=float(xs[0])
    for x in xs[1:]: e=float(x)*k+e*(1-k)
    return e


def rsi(xs,n=14):
    if len(xs)<=n:return 50.0
    d=[xs[i]-xs[i-1] for i in range(1,len(xs))][-n:]
    g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]; ag,al=mean(g),mean(l)
    if al==0:return 100.0
    return 100-100/(1+ag/al)


def atr_pct(ks,n=14):
    if len(ks)<=n:return 0.0
    trs=[]; prev=float(ks[-n-1][4])
    for k in ks[-n:]:
        h,l,c=map(float,(k[2],k[3],k[4])); trs.append(max(h-l,abs(h-prev),abs(l-prev))); prev=c
    last=float(ks[-1][4]); return mean(trs)/last if last else 0.0


def sigmoid(x):
    x=max(-12,min(12,x)); return 1/(1+math.exp(-x))


def make_state():
    return {"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0,"prices":deque(maxlen=STATE_LEN),"flow":deque(maxlen=STATE_LEN),"last_event":0.0}


def is_meme(symbol):
    base=symbol.upper().replace("USDT","")
    return any(h in base for h in MEME_HINTS)


def meme_universe():
    """Broad radar: known memes plus liquid/high-volatility new listings are eligible."""
    info=public("/fapi/v1/exchangeInfo")
    trading={x["symbol"]:x for x in info["symbols"] if x.get("status")=="TRADING" and x.get("contractType")=="PERPETUAL" and x.get("quoteAsset")=="USDT"}
    tickers=public("/fapi/v1/ticker/24hr"); out=[]
    for t in tickers:
        s=t.get("symbol"); meta=trading.get(s)
        if not meta: continue
        try:
            qv=float(t.get("quoteVolume",0)); ch=abs(float(t.get("priceChangePercent",0)))/100; price=float(t.get("lastPrice",0))
            if qv<MIN_24H_QV or price<=0: continue
            base=s.replace("USDT",""); known=is_meme(s)
            behavior=min(1.0,max(0.0,ch/.12))*0.55+min(1.0,max(0.0,math.log10(qv)/10))*0.25+(0.20 if known else 0.0)
            out.append((s.lower(),qv,ch,behavior))
        except (TypeError,ValueError): pass
    out.sort(key=lambda x:(x[3],x[1]*max(x[2],.003)),reverse=True)
    return out[:max(TOP_N+CHALLENGER_N+20,60)]


def kline_features(symbol):
    frames={}
    for interval,limit in (("1m",90),("5m",60),("15m",50),("1h",40)):
        ks=public("/fapi/v1/klines",{"symbol":symbol.upper(),"interval":interval,"limit":limit})
        if len(ks)<25:return None
        closes=[float(k[4]) for k in ks]; vols=[float(k[5]) for k in ks]; last=closes[-1]
        hi=max(float(k[2]) for k in ks[-20:]); lo=min(float(k[3]) for k in ks[-20:])
        frames[interval]={"close":last,"atr":atr_pct(ks),"rsi":rsi(closes),"mom":last/closes[-4]-1,"fast":last/closes[-2]-1,"vol_ratio":vols[-1]/max(mean(vols[-21:-1]),1e-12),"qv5":last*sum(vols[-5:]),"ema9":ema(closes,9),"ema21":ema(closes,21),"ema50":ema(closes,50),"range_pos":(last-lo)/max(hi-lo,1e-12),"range_width":(hi-lo)/max(last,1e-12)}
    return frames


def regime_from_market():
    try:
        btc=kline_features("BTCUSDT"); eth=kline_features("ETHUSDT")
        if not btc or not eth:return "UNKNOWN"
        m1=btc["1m"]; m5=btc["5m"]; e5=eth["5m"]
        if m1["atr"]>.018:return "PANIC"
        if m1["atr"]>.007:return "HIGH_VOL"
        if m5["ema9"]>m5["ema21"] and e5["ema9"]>e5["ema21"]:return "RISK_ON"
        if m5["ema9"]<m5["ema21"] and e5["ema9"]<e5["ema21"]:return "RISK_OFF"
        return "CHOP"
    except Exception:
        return "UNKNOWN"


def score_symbol(symbol,qv,radar,f):
    st=state.setdefault(symbol,make_state())
    if st["bid"]<=0 or st["ask"]<=0:return None
    m1,m5,m15,h1=f["1m"],f["5m"],f["15m"],f["1h"]
    mid=(st["bid"]+st["ask"])/2; spread=(st["ask"]-st["bid"])/mid*10000
    if spread>MAX_SPREAD_BPS or m1["qv5"]<MIN_5M_QV:return None
    side="BUY" if m1["mom"]>0 else "SELL"
    trend=((m5["ema9"]>m5["ema21"]>m5["ema50"]) if side=="BUY" else (m5["ema9"]<m5["ema21"]<m5["ema50"]))
    htf=((m15["ema9"]>m15["ema21"] and h1["ema9"]>h1["ema21"]) if side=="BUY" else (m15["ema9"]<m15["ema21"] and h1["ema9"]<h1["ema21"]))
    vol=min(max((m1["vol_ratio"]-1)/2,0),1); mom=min(abs(m1["mom"])/.0035,1); fast=min(abs(m1["fast"])/.002,1)
    trend_strength=min(abs(m5["ema9"]/max(m5["ema21"],1e-12)-1)/.0035,1)
    alignment=int(((m5["ema9"]>m5["ema21"])==(m15["ema9"]>m15["ema21"])==(h1["ema9"]>h1["ema21"])))
    breakout=int((m1["range_pos"]>.80 and side=="BUY") or (m1["range_pos"]<.20 and side=="SELL"))
    imb=(st["bq"]-st["aq"])/max(st["bq"]+st["aq"],1e-12)
    flow=list(st["flow"]); recent=sum(flow[-8:]); baseline=mean([abs(x) for x in flow[-40:-8]]) if len(flow)>=40 else 0
    flow_edge=min(abs(imb)*1.4,1)+(.25 if baseline and abs(recent)>baseline*1.2 and ((recent>0)==(side=="BUY")) else 0)
    flow_edge=min(flow_edge,1)
    exhaustion=int((m1["rsi"]>84 and side=="BUY") or (m1["rsi"]<16 and side=="SELL"))
    chase=min(abs(m1["mom"])/.018,1) if m1["vol_ratio"]<2 else 0
    liquidity=min(1,math.log10(max(qv,1))/10)*max(0,1-spread/MAX_SPREAD_BPS)
    atr_quality=min(max(m1["atr"]/.004,0),1)
    market_bonus={"RISK_ON":.12,"HIGH_VOL":.06,"BREAKOUT":.10,"CHOP":-.10,"RISK_OFF":-.04,"PANIC":-.25,"UNKNOWN":0}.get(market_regime_name,0)
    raw=(1.30*trend+1.10*htf+.75*vol+.80*mom+.35*fast+.75*flow_edge+.60*breakout+.45*trend_strength+.35*alignment+.35*liquidity+.25*radar+.35*atr_quality+market_bonus-.65*chase-.90*exhaustion)
    score=sigmoid(raw-2.05)
    expected=max(m1["atr"]*10000*.75,abs(m1["mom"])*10000*.90,abs(m1["fast"])*10000*.65)
    edge=expected-(FEE_BPS+SLIPPAGE_BPS+spread)
    return {"symbol":symbol,"side":side,"score":score,"edge_bps":edge,"spread_bps":spread,"qv":qv,"atr":m1["atr"],"vol_ratio":m1["vol_ratio"],"momentum":m1["mom"],"flow":flow_edge,"trend_strength":trend_strength,"rsi":m1["rsi"],"regime":market_regime_name,"timestamp":time.time()}


def refresh_ranking():
    global ranked_top20,challengers,desired_symbols,market_regime_name
    market_regime_name=regime_from_market(); candidates=[]
    for symbol,qv,ch,radar in meme_universe():
        try:
            f=kline_features(symbol); c=score_symbol(symbol,qv,radar,f) if f else None
            if c and c["edge_bps"]>=MIN_EDGE_BPS:candidates.append(c)
        except Exception as exc:log.debug("rank %s failed: %s",symbol,exc)
    candidates.sort(key=lambda x:(x["score"],x["edge_bps"]),reverse=True)
    old={x["symbol"]:x for x in ranked_top20}; selected=candidates[:TOP_N]
    cutoff=selected[-1]["score"] if selected else 0
    for s in old:
        c=next((x for x in candidates if x["symbol"]==s),None)
        if c and c["score"]>=cutoff-.05 and c not in selected:selected.append(c)
    selected.sort(key=lambda x:x["score"],reverse=True); ranked_top20=selected[:TOP_N]
    top={x["symbol"] for x in ranked_top20}; challengers=[x for x in candidates if x["symbol"] not in top][:CHALLENGER_N]
    desired_symbols=[x["symbol"] for x in ranked_top20+challengers]
    metrics["ranking"]+=1
    log.info("TOP-%d | regime=%s | %s",TOP_N,market_regime_name," ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["edge_bps"]:.0f}bp' for x in ranked_top20[:10]))


def confirm_entry(c):
    regime=market_regime_name
    if regime=="PANIC" or c["score"]<ENTRY_SCORE or c["edge_bps"]<MIN_EDGE_BPS:return False
    if c["vol_ratio"]<1.20 or abs(c["momentum"])<.0007 or c["flow"]<.12 or c["trend_strength"]<.12:return False
    if c["spread_bps"]>MAX_SPREAD_BPS:return False
    if regime=="CHOP" and c["score"]<.80:return False
    return time.time()-state[c["symbol"]]["last_event"]<=5


def enter(c):
    s=c["symbol"]; now=time.time()
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS or now-last_entry.get(s,0)<COOLDOWN:return
        if not confirm_entry(c):return
        entry=state[s]["ask"] if c["side"]=="BUY" else state[s]["bid"]
        positions[s]={**c,"entry":entry,"opened":now,"mfe":0.0,"mae":0.0}; last_entry[s]=now; metrics["entries"]+=1
    log.warning("ENTRY %s %s score=%.3f edge=%.1fbp regime=%s vol=%.2fx [DRY RUN]",c["side"],s.upper(),c["score"],c["edge_bps"],c["regime"],c["vol_ratio"])


def close_trade(s,p,ret,reason,age):
    net=ret-(FEE_BPS+SLIPPAGE_BPS)*2/10000; metrics["exits"]+=1; metrics["net_pnl"]+=net
    if net>=0:metrics["wins"]+=1; metrics["gross_profit"]+=net
    else:metrics["losses"]+=1; metrics["gross_loss"]+=abs(net)
    pf=metrics["gross_profit"]/max(metrics["gross_loss"],1e-12)
    log.warning("EXIT %s %s net=%.3f%% held=%.1fs MFE=%.3f%% MAE=%.3f%% PF=%.2f",reason,s.upper(),net*100,age,p["mfe"]*100,p["mae"]*100,pf)


def manage_positions():
    now=time.time(); exits=[]
    with lock:
        for s,p in list(positions.items()):
            st=state.get(s)
            if not st:continue
            px=st["bid"] if p["side"]=="BUY" else st["ask"]
            if px<=0:continue
            ret=px/p["entry"]-1 if p["side"]=="BUY" else p["entry"]/px-1
            p["mfe"]=max(p["mfe"],ret); p["mae"]=min(p["mae"],ret); age=now-p["opened"]
            stop=max(p["atr"]*1.25,.0028); reason=None
            if ret<=-stop:reason="ATR_STOP"
            elif p["mfe"]>.0018 and p["mfe"]-ret>max(.0010,p["mfe"]*.45):reason="TRAIL_DECAY"
            elif age>MAX_HOLD:reason="TIME_EXIT"
            elif market_regime_name=="PANIC":reason="PANIC_EXIT"
            else:
                sig=next((x for x in ranked_top20 if x["symbol"]==s),None)
                if sig and sig["side"]!=p["side"] and sig["score"]>.78:reason="REVERSAL"
                elif sig and sig["score"]<.55 and age>30:reason="THESIS_DECAY"
            if reason:positions.pop(s,None); exits.append((s,p,ret,reason,age))
    for x in exits:close_trade(*x)


def on_message(_,raw):
    try:
        d=json.loads(raw).get("data",{}); s=d.get("s","").lower(); event=d.get("e")
        if not s:return
        st=state.setdefault(s,make_state()); st["last_event"]=time.time(); metrics["events"]+=1
        if event=="bookTicker":
            st["bid"]=float(d.get("b",0)); st["ask"]=float(d.get("a",0)); st["bq"]=float(d.get("B",0)); st["aq"]=float(d.get("A",0))
        elif event=="aggTrade":
            qty=float(d.get("q",0)); px=float(d.get("p",0)); buy=not bool(d.get("m")); st["last"]=px; st["prices"].append(px); st["flow"].append(qty if buy else -qty)
            if s in {x["symbol"] for x in ranked_top20}:metrics["signals"]+=1
    except Exception as exc:log.debug("ws message error: %s",exc)


def ws_loop():
    global ws_ref
    current=[]
    while True:
        try:
            target=list(desired_symbols)
            if target!=current:
                current=target
                if not current:time.sleep(2);continue
                streams=[v for s in current for v in (f"{s}@bookTicker",f"{s}@aggTrade")]
                url=WS_BASE+"?streams="+"/".join(streams)
                ws_ref=websocket.WebSocketApp(url,on_message=on_message)
                ws_ref.run_forever(ping_interval=20,ping_timeout=10)
            else:time.sleep(1)
        except Exception as exc:log.warning("WS reconnect: %s",exc)
        time.sleep(2)


def save_state():
    try:open(STATE_FILE,"w").write(json.dumps({"metrics":metrics,"regime":market_regime_name,"timestamp":time.time()},indent=2))
    except Exception as exc:log.debug("state save: %s",exc)


def main():
    if not DRY_RUN:raise RuntimeError("Live execution is intentionally disabled; DRY_RUN must remain true")
    log.warning("ADAPTIVE TOP-20 ENGINE START | DRY RUN ONLY")
    threading.Thread(target=ws_loop,daemon=True).start(); last_rank=0
    while True:
        try:
            if time.time()-last_rank>=RANK_REFRESH:refresh_ranking();last_rank=time.time()
            for c in list(ranked_top20):
                if confirm_entry(c):enter(c)
            manage_positions(); save_state(); time.sleep(.5)
        except Exception as exc:log.exception("engine loop: %s",exc);time.sleep(2)

if __name__=="__main__":main()
