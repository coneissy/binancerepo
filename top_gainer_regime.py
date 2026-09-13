"""Top-gainer breadth regime for the 1m meme engine."""
import os
from statistics import median

TOP_GAINERS_N=max(10,int(os.getenv("TOP_GAINERS_N","20")))


def market_regime(engine):
    """Classify regime from the current top 24h gainers using live 1m breadth."""
    try:
        info=engine.api("/fapi/v1/exchangeInfo")
        ticks=engine.api("/fapi/v1/ticker/24hr")
        allowed={m.get("symbol") for m in info.get("symbols",[]) if m.get("status")=="TRADING" and m.get("contractType")=="PERPETUAL" and m.get("quoteAsset")=="USDT"}
        gainers=[]
        for t in ticks:
            s=t.get("symbol","")
            if s not in allowed: continue
            try:
                ch=float(t.get("priceChangePercent",0.0)); q=float(t.get("quoteVolume",0.0))
            except Exception:
                continue
            if ch>0 and q>=engine.MIN24:
                gainers.append((s.lower(),ch,q))
        gainers=sorted(gainers,key=lambda x:x[1],reverse=True)[:TOP_GAINERS_N]
        returns=[]; aligned=[]
        for s,_,_ in gainers:
            try:
                if s not in engine.hist or len(engine.hist[s])<engine.WARMUP:
                    if not engine.seed(s): continue
                f=engine.features(s)
                h=engine.higher(s)
                if not f or not h: continue
                r=float(f["ret"])
                returns.append(r)
                aligned.append(1 if ((h["e9"]>h["e21"] and r>0) or (h["e9"]<h["e21"] and r<0)) else 0)
            except Exception:
                continue
        if len(returns)<max(5,TOP_GAINERS_N//4):
            return "UNKNOWN"
        med=median(returns)
        breadth_up=sum(r>0 for r in returns)/len(returns)
        breadth_down=sum(r<0 for r in returns)/len(returns)
        align=sum(aligned)/len(aligned) if aligned else 0.0
        if breadth_down>=0.70 and med<=-0.0035:
            return "PANIC"
        if breadth_up>=0.60 and med>=0.0012 and align>=0.50:
            return "TREND_UP"
        if breadth_down>=0.60 and med<=-0.0012 and align>=0.50:
            return "TREND_DOWN"
        return "CHOP"
    except Exception:
        return "UNKNOWN"
