"""Dry-run Binance USD-M meme scalper: 15m regime + 5m bias + 1m execution."""
import json, logging, math, os, threading, time
from collections import defaultdict, deque
import requests, websocket

BASE = os.getenv("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
WS_BASE = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/stream")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
TOP_N = 20
WS_SHARDS = max(1, int(os.getenv("WS_SHARDS", "4")))
MIN_QV = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
NEW_LISTING_WINDOW = float(os.getenv("NEW_LISTING_WINDOW_SECONDS", "1200"))
NEW_LISTING_MIN_QV = float(os.getenv("NEW_LISTING_MIN_QUOTE_VOLUME", "250000"))
LISTING_REFRESH_SECONDS = float(os.getenv("LISTING_REFRESH_SECONDS", "30"))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "0.68"))
NEUTRAL_ENTRY_SCORE = float(os.getenv("NEUTRAL_ENTRY_SCORE", "0.74"))
MAX_POSITIONS = int(os.getenv("MAX_SIMULTANEOUS_POSITIONS", "6"))
NEUTRAL_MAX_POSITIONS = int(os.getenv("NEUTRAL_MAX_POSITIONS", "3"))
COOLDOWN = float(os.getenv("ENTRY_COOLDOWN_SECONDS", "2.0"))
FEE_PER_SIDE = float(os.getenv("FEE_PER_SIDE_PCT", "0.0004"))
SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SLIPPAGE_PCT", "0.0001"))
MIN_EDGE_MULT = float(os.getenv("MIN_EDGE_MULTIPLIER", "1.60"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "7"))
MIN_TP = float(os.getenv("MIN_TP_PCT", "0.0025"))
MAX_TP = float(os.getenv("MAX_TP_PCT", "0.0090"))
MIN_SL = float(os.getenv("MIN_SL_PCT", "0.0016"))
MAX_SL = float(os.getenv("MAX_SL_PCT", "0.0035"))
TP_VOL_MULT = float(os.getenv("TP_VOL_MULTIPLIER", "3.5"))
SL_VOL_MULT = float(os.getenv("SL_VOL_MULTIPLIER", "1.35"))
MAX_HOLD = float(os.getenv("MAX_HOLD_SECONDS", "30"))
TRAIL_START = float(os.getenv("TRAIL_START_PCT", "0.0018"))
TRAIL_GIVEBACK = float(os.getenv("TRAIL_GIVEBACK_PCT", "0.0010"))
STATE_LEN = int(os.getenv("STATE_LEN", "240"))
MIN_BARS = int(os.getenv("MIN_BARS", "20"))

MEME_SYMBOLS = {"DOGE","SHIB","1000SHIB","PEPE","1000PEPE","FLOKI","BONK","WIF","MEME","MEMES","BRETT","TURBO","NEIRO","1000NEIRO","PNUT","ACT","GOAT","MOODENG","CHILLGUY","POPCAT","DOGS","CAT","MEW","MYRO","BOME","SLERF","SUNDOG","MOG","PONKE","WHY","TOSHI","BAN","FARTCOIN","ARC","JELLYJELLY","PIPPIN","SWARMS","AVA","AVAAI","TRUMP","MELANIA","SPX","GIGA","FWOG","BROCCOLI","BABYDOGE","1000BABYDOGE","PUMP","DOOD","ZEREBRO","MOTHER","RETARDIO","1000BONK","1000FLOKI","1000CAT","1000CHEEMS","1000SATS"}
MEME_PREFIXES = ("1000","1M")

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("meme-scalper")
http = requests.Session()
state, quote_volume, first_seen, known_symbols = {}, {}, {}, set()
selected_symbols, positions, last_entry = [], {}, {}
confirmations = defaultdict(int)
performance = defaultdict(lambda: {"trades":0,"wins":0,"losses":0,"return":0.0})
metrics = defaultdict(int)
market_regime = "NEUTRAL"
lock = threading.RLock()

def public(path, params=None):
    r = http.get(BASE + path, params=params, timeout=10); r.raise_for_status(); return r.json()

def make_state():
    return {"bid":0.0,"ask":0.0,"bq":0.0,"aq":0.0,"last":0.0,"prices":deque(maxlen=STATE_LEN),"flow":deque(maxlen=STATE_LEN),"bars":deque(maxlen=STATE_LEN),"bar":None,"last_event":0.0}

def is_meme(symbol):
    base = symbol.upper().removesuffix("USDT")
    return base in MEME_SYMBOLS or base.startswith(MEME_PREFIXES)

def exchange_symbols():
    info = public("/fapi/v1/exchangeInfo")
    return {x["symbol"].lower(): {"onboard":int(x.get("onboardDate",0) or 0)} for x in info.get("symbols",[]) if x.get("status")=="TRADING" and x.get("contractType")=="PERPETUAL" and x.get("quoteAsset")=="USDT"}

def norm(values, value):
    if not values: return 0.0
    lo, hi = min(values), max(values)
    if hi <= lo: return 0.5
    return (value-lo)/(hi-lo)

def refresh_universe(startup=False):
    global selected_symbols
    meta, now = exchange_symbols(), time.time()
    for s, info in meta.items():
        if s not in known_symbols:
            known_symbols.add(s); onboard=info.get("onboard",0)
            if onboard and now-onboard/1000 <= NEW_LISTING_WINDOW: first_seen[s]=onboard/1000
            elif not onboard and not startup:
                first_seen[s]=now
                if is_meme(s): metrics["new_listings"]+=1; log.warning("NEW MEME LISTING | %s",s.upper())
    candidates=[]
    for t in public("/fapi/v1/ticker/24hr"):
        s=str(t.get("symbol","" )).lower()
        if s not in meta or not is_meme(s): continue
        try:
            qv=float(t.get("quoteVolume",0) or 0); last=float(t.get("lastPrice",0) or 0); high=float(t.get("highPrice",0) or 0); low=float(t.get("lowPrice",0) or 0); change=float(t.get("priceChangePercent",0) or 0)/100
            if last<=0: continue
            is_new=s in first_seen and now-first_seen[s] <= NEW_LISTING_WINDOW
            if qv < (NEW_LISTING_MIN_QV if is_new else MIN_QV): continue
            quote_volume[s]=qv; st=state.get(s); spread=None
            if st and st["bid"]>0 and st["ask"]>0:
                mid=(st["bid"]+st["ask"])/2; spread=(st["ask"]-st["bid"])/max(mid,1e-12)*10000
            liq=math.log1p(qv)
            if spread is not None: liq*=max(0.05,1-min(spread,30)/30)
            candidates.append({"symbol":s,"volatility":max((high-low)/last,abs(change)),"volume":qv,"liquidity":liq,"momentum":abs(change)})
        except (TypeError,ValueError): continue
    if not candidates: selected_symbols=[]; return []
    vols=[x["volatility"] for x in candidates]; vs=[math.log1p(x["volume"]) for x in candidates]; ls=[x["liquidity"] for x in candidates]; ms=[x["momentum"] for x in candidates]
    for x in candidates:
        nv,nv2,nl,nm=norm(vols,x["volatility"]),norm(vs,math.log1p(x["volume"])),norm(ls,x["liquidity"]),norm(ms,x["momentum"])
        x["rank_score"]=0.40*nv+0.25*nv2+0.20*nl+0.15*nm; x["quality"]=min(nv,nv2,nl,nm)
    candidates.sort(key=lambda x:(x["rank_score"],x["quality"],x["volatility"]),reverse=True)
    selected=candidates[:TOP_N]; selected_symbols=[x["symbol"] for x in selected]
    for s in selected_symbols: state.setdefault(s,make_state())
    metrics["ranking_refreshes"]+=1; metrics["universe_size"]=len(selected_symbols)
    log.warning("DYNAMIC TOP-20 | candidates=%d selected=%d leaders=%s",len(candidates),len(selected),",".join(x["symbol"].upper() for x in selected[:5]))
    return selected_symbols

def update_bar(st, ts, price, qty):
    minute=int(ts//60)*60; b=st["bar"]
    if b is None or b[0]!=minute:
        if b is not None: st["bars"].append(tuple(b))
        st["bar"]=[minute,price,price,price,price,price*qty]
    else:
        b[2]=max(b[2],price); b[3]=min(b[3],price); b[4]=price; b[5]+=price*qty

def closes(s):
    st=state.get(s)
    if not st: return []
    a=[b[4] for b in st["bars"]]
    if st["bar"] is not None: a.append(st["bar"][4])
    return a

def tf_return(s, minutes):
    c=closes(s)
    if len(c)<minutes+1: return None
    return c[-1]/c[-1-minutes]-1

def compute_regime():
    btc=tf_return("btcusdt",15); vals=[tf_return(s,5) for s in selected_symbols]; vals=[v for v in vals if v is not None]
    breadth=sum(v>0 for v in vals)/len(vals) if vals else 0.5
    if btc is None: return "NEUTRAL",breadth,None
    if btc<=-0.006 or breadth<0.25: return "RISK_OFF",breadth,btc
    if btc<=-0.002 or breadth<0.40: return "NEUTRAL",breadth,btc
    if btc>=0.002 and breadth>=0.55: return "RISK_ON",breadth,btc
    return "NEUTRAL",breadth,btc

def refresh_regime():
    global market_regime
    regime,breadth,btc=compute_regime()
    if regime!=market_regime: log.warning("MARKET REGIME | %s -> %s | BTC15=%s breadth5=%.0f%%",market_regime,regime,"n/a" if btc is None else f"{btc*100:.2f}%",breadth*100)
    market_regime=regime; metrics["regime_risk_off"]=int(regime=="RISK_OFF")

def clamp(x,lo=0.0,hi=1.0): return min(max(x,lo),hi)
def nd(x,scale): return clamp(x/max(scale,1e-12))

def score_symbol(s):
    if s not in selected_symbols or s=="btcusdt": metrics["outside_top20"]+=1; return None
    st=state.get(s)
    if not st or st["bid"]<=0 or st["ask"]<=0: metrics["no_book"]+=1; return None
    ps=closes(s); fs=list(st["flow"])
    if len(ps)<MIN_BARS or len(fs)<18: metrics["short_state"]+=1; return None
    spread=(st["ask"]-st["bid"])/max((st["ask"]+st["bid"])/2,1e-12)*10000
    if spread>MAX_SPREAD_BPS: metrics["spread_reject"]+=1; return None
    if quote_volume.get(s,0)<MIN_QV: metrics["liquidity_reject"]+=1; return None
    m1=tf_return(s,1) or 0; m5=tf_return(s,5) or 0; m12=tf_return(s,12) or 0; m2=tf_return(s,2) or 0
    m1_acc=m1-m2/2
    recent=fs[-6:]; prior=fs[-18:-6]; pa=sum(abs(x) for x in prior)/max(len(prior),1)
    flow=sum(recent)/max(pa*len(recent),1e-12); accel=(sum(recent[-3:])-sum(recent[:3]))/max(pa*3,1e-12)
    imbalance=(st["bq"]-st["aq"])/max(st["bq"]+st["aq"],1e-12); mid=(st["bid"]+st["ask"])/2
    micro=(st["ask"]*st["bq"]+st["bid"]*st["aq"])/max(st["bq"]+st["aq"],1e-12); micro_edge=(micro-mid)/max(mid,1e-12)
    returns=[math.log(ps[i]/ps[i-1]) for i in range(max(1,len(ps)-16),len(ps)) if ps[i-1]>0 and ps[i]>0]
    vol=math.sqrt(sum(r*r for r in returns)/max(len(returns),1))
    if vol<0.00002: metrics["vol_reject"]+=1; return None
    bias="BUY" if m5>0 else "SELL" if m5<0 else None
    if not bias: metrics["bias_reject"]+=1; return None
    long_score=nd(imbalance,.55)*.18+nd(micro_edge,.00035)*.10+nd(m1,.00045)*.20+nd(m5,.0015)*.17+nd(m12,.0025)*.08+nd(flow,1)*.18+nd(accel,.8)*.09+nd(m1_acc,.0003)*.08
    short_score=nd(-imbalance,.55)*.18+nd(-micro_edge,.00035)*.10+nd(-m1,.00045)*.20+nd(-m5,.0015)*.17+nd(-m12,.0025)*.08+nd(-flow,1)*.18+nd(-accel,.8)*.09+nd(-m1_acc,.0003)*.08
    side="BUY" if long_score>=short_score else "SELL"; score=max(long_score,short_score)
    if side!=bias: metrics["bias_reject"]+=1; confirmations[s]=0; return None
    signed_momentum=(m1*.50+m5*.30+m12*.20) if side=="BUY" else -(m1*.50+m5*.30+m12*.20); signed_flow=flow if side=="BUY" else -flow
    if signed_momentum<=0 or signed_flow<-.05: metrics["confirmation_reject"]+=1; confirmations[s]=0; return None
    if market_regime=="RISK_OFF": metrics["regime_reject"]+=1; confirmations[s]=0; return None
    threshold=NEUTRAL_ENTRY_SCORE if market_regime=="NEUTRAL" else ENTRY_SCORE
    if score<threshold: metrics["score_reject"]+=1; confirmations[s]=0; return None
    confirmations[s]+=1
    if confirmations[s]<2: metrics["confirmation_wait"]+=1; return None
    cost=2*FEE_PER_SIDE+2*SLIPPAGE_PCT+spread/10000; projected=max(abs(m1),abs(m5)*.70,abs(m12)*.45,vol*(1.15+.85*score)); edge=projected*(.85+.90*score)-cost
    metrics["cost_checks"]+=1
    if edge<cost*MIN_EDGE_MULT: metrics["cost_reject"]+=1; confirmations[s]=0; return None
    target=clamp(max(MIN_TP,vol*TP_VOL_MULT,abs(m1)*2.4),MIN_TP,MAX_TP); stop=clamp(max(MIN_SL,vol*SL_VOL_MULT),MIN_SL,MAX_SL)
    target=clamp(target*(.92+.38*score),MIN_TP,MAX_TP); target=max(target,min(MAX_TP,stop*1.35)); price=st["ask"] if side=="BUY" else st["bid"]
    return side,score,price,target,stop,edge

def enter(s,signal):
    side,score,price,target,stop,edge=signal; now=time.time()
    with lock:
        cap=NEUTRAL_MAX_POSITIONS if market_regime=="NEUTRAL" else MAX_POSITIONS
        if s in positions or len(positions)>=cap or now-last_entry.get(s,0)<COOLDOWN: return
        positions[s]={"side":side,"entry":price,"opened":now,"score":score,"tp":target,"sl":stop,"expected_edge":edge,"peak":price}; last_entry[s]=now; confirmations[s]=0; metrics["entries"]+=1
    log.warning("ENTRY %s %s regime=%s score=%.3f edge=%.3f%% tp=%.3f%% sl=%.3f%% hold<=%.0fs [DRY RUN]",side,s.upper(),market_regime,score,edge*100,target*100,stop*100,MAX_HOLD)

def exit_position(s,p,px,ret,reason,held):
    metrics["exits"]+=1; performance["all"]["trades"]+=1; performance["all"]["return"]+=ret; performance["all"]["wins"]+=ret>0; performance["all"]["losses"]+=ret<=0; metrics[f"exit_{reason.lower()}"]+=1; log.warning("EXIT %s %s return=%.3f%% held=%.2fs",reason,s.upper(),ret*100,held)

def manage_positions():
    now=time.time(); exits=[]
    with lock:
        for s,p in list(positions.items()):
            st=state.get(s)
            if not st: continue
            px=st["bid"] if p["side"]=="BUY" else st["ask"]
            if px<=0: continue
            ret=px/p["entry"]-1 if p["side"]=="BUY" else p["entry"]/px-1; held=now-p["opened"]
            if p["side"]=="BUY": p["peak"]=max(p["peak"],px); draw=p["peak"]/px-1
            else: p["peak"]=min(p["peak"],px); draw=px/p["peak"]-1
            reason=None
            if ret>=p["tp"]: reason="TP"
            elif ret<=-p["sl"]: reason="SL"
            elif ret>=TRAIL_START and draw>=TRAIL_GIVEBACK: reason="TRAIL"
            elif held>=MAX_HOLD: reason="TIME"
            else:
                sig=score_symbol(s)
                if sig and sig[0]!=p["side"] and sig[1]>=ENTRY_SCORE: reason="REVERSAL"
            if reason: positions.pop(s,None); exits.append((s,p,px,ret,reason,held))
    for row in exits: exit_position(*row)

def on_message(_,raw):
    try:
        msg=json.loads(raw); d=msg.get("data",msg); event=d.get("e"); s=str(d.get("s","" )).lower()
        if not s or (s!="btcusdt" and s not in selected_symbols): return
        st=state.setdefault(s,make_state()); metrics["events"]+=1
        if event=="bookTicker": st["bid"]=float(d["b"]); st["ask"]=float(d["a"]); st["bq"]=float(d["B"]); st["aq"]=float(d["A"])
        elif event=="aggTrade":
            px=float(d["p"]); qty=float(d["q"]); signed=-px*qty if d.get("m") else px*qty; st["last"]=px; st["prices"].append(px); st["flow"].append(signed); update_bar(st,time.time(),px,qty)
        else: return
        st["last_event"]=time.time(); sig=score_symbol(s)
        if sig: metrics["signals"]+=1; enter(s,sig)
    except Exception: log.exception("market-data message error")

def stream_url(symbols):
    streams=[]
    for s in symbols: streams += [f"{s}@bookTicker",f"{s}@aggTrade"]
    return WS_BASE+"?streams="+"/".join(streams)

def run_ws(symbols,shard_id,stop_event):
    if not symbols: return
    url=stream_url(symbols)
    while not stop_event.is_set():
        try:
            log.info("WS shard %d connecting symbols=%d",shard_id,len(symbols)); ws=websocket.WebSocketApp(url,on_message=on_message,on_error=lambda _,e:log.warning("WS shard %d error: %s",shard_id,e),on_close=lambda _,c,m:log.warning("WS shard %d closed: %s %s",shard_id,c,m)); ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception: log.exception("WS shard %d failed",shard_id)
        metrics["reconnects"]+=1; stop_event.wait(2)

def websocket_manager():
    last_signature=None; ws_stop=None
    while True:
        try:
            current=refresh_universe(); refresh_regime(); stream_symbols=list(current)+["btcusdt"]; signature=tuple(stream_symbols)
            if signature!=last_signature:
                if ws_stop is not None: ws_stop.set()
                ws_stop=threading.Event(); shards=[[] for _ in range(min(WS_SHARDS,max(1,len(stream_symbols))))]
                for i,s in enumerate(stream_symbols): shards[i%len(shards)].append(s)
                for i,shard in enumerate(shards,1): threading.Thread(target=run_ws,args=(shard,i,ws_stop),daemon=True).start()
                last_signature=signature; metrics["ws_subscriptions"]=len(stream_symbols)
        except Exception: log.exception("universe refresh failed")
        time.sleep(LISTING_REFRESH_SECONDS)

def monitor():
    while True: refresh_regime(); manage_positions(); time.sleep(.25)

def print_stats():
    t=performance["all"]["trades"]; w=performance["all"]["wins"]; ret=performance["all"]["return"]; wr=w/t*100 if t else 0
    log.warning("STATS regime=%s top20=%d events=%d signals=%d entries=%d exits=%d trades=%d winrate=%.1f%% return=%.3f%% positions=%d",market_regime,len(selected_symbols),metrics["events"],metrics["signals"],metrics["entries"],metrics["exits"],t,wr,ret*100,len(positions))
    log.warning("REJECTS spread=%d bias=%d score=%d cost=%d flow=%d vol=%d regime=%d wait=%d",metrics["spread_reject"],metrics["bias_reject"],metrics["score_reject"],metrics["cost_reject"],metrics["confirmation_reject"],metrics["vol_reject"],metrics["regime_reject"],metrics["confirmation_wait"])

def main():
    if not DRY_RUN: raise RuntimeError("Live execution is disabled. Keep DRY_RUN=true until paper results are validated.")
    state["btcusdt"]=make_state(); refresh_universe(startup=True); refresh_regime(); threading.Thread(target=websocket_manager,name="universe-manager",daemon=True).start(); threading.Thread(target=monitor,name="exit-engine",daemon=True).start(); log.warning("ENGINE STARTED | TOP-20 | 15m REGIME + 5m BIAS + 1m EXECUTION | DRY-RUN")
    while True: time.sleep(10); print_stats()

if __name__=="__main__": main()
