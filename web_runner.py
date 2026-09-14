import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import hft_scalper_30s as engine
import top_gainer_regime
import dynamic_largecap_universe
import buildix_flow

# Cryptoalpha 3M architecture: 15M context -> 5M trend/ICT-SMC -> 3M execution.
# Do NOT install the legacy 1H/15M -> 5M/1M HTF module here.
engine.discover = lambda: dynamic_largecap_universe.discover(engine)
top_gainer_regime.install_symbol_guard(engine)
buildix_flow.install(engine)

# Binance can return HTTP 418/HTML from one Futures host. Rotate hosts at the
# core API boundary so discovery AND candle seeding use the same healthy host.
_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]
_original_api = engine.api

def _api_failover(path, params=None):
    current = getattr(engine, "BASE", _BASES[0])
    ordered = [current] + [b for b in _BASES if b != current]
    last = None
    for base in ordered:
        try:
            engine.BASE = base
            return _original_api(path, params)
        except (requests.RequestException, ValueError) as exc:
            last = exc
            engine.log.warning("BINANCE API FAILOVER | base=%s | path=%s | error=%s", base, path, exc)
    if last:
        raise last
    raise RuntimeError("No Binance Futures REST endpoint available")

engine.api = _api_failover

# --- HTF freshness / REST-load guard ---------------------------------------
# The old core only seeded 5M/15M once. That made HTF context stale while 3M
# bars continued live. Refresh only the small execution candidate set instead
# of repeatedly fetching candles for the whole discovery universe.
_FRAME_AT = {}
_FRAME_LOCK = threading.RLock()
_FRAME_REFRESH = {"3m": 0.0, "5m": 90.0, "15m": 240.0}
_ORIGINAL_SEED = engine.seed

def _fresh_seed(symbol, interval, limit=100, force=False):
    now = time.time()
    key = (symbol, interval)
    with _FRAME_LOCK:
        due = force or interval == "3m" or now - _FRAME_AT.get(key, 0.0) >= _FRAME_REFRESH[interval]
        if not due and interval in engine.hist.get(symbol, {}):
            return True
    try:
        ok = _ORIGINAL_SEED(symbol, interval, limit)
        if ok:
            with _FRAME_LOCK:
                _FRAME_AT[key] = time.time()
        return ok
    except Exception as exc:
        engine.log.warning("FRAME REFRESH FAILED | %s %s | %s", symbol.upper(), interval, exc)
        return interval in engine.hist.get(symbol, {})

def _ensure_frames(symbol):
    # 3M is live-updated by aggTrade; seed only when a symbol first enters the
    # execution set.  5M/15M are periodically refreshed to keep HTF context
    # current without hammering Binance.
    for interval in ("3m", "5m", "15m"):
        if not _fresh_seed(symbol, interval, 100):
            return False
    return True

engine.ensure_frames = _ensure_frames

# Rank only the highest-radar discovery candidates. This preserves a dynamic
# broad universe while limiting expensive candle/indicator work to the actual
# execution shortlist.
def _optimized_rank():
    d = engine.discover()
    reject = {"warmup":0,"structure":0,"spread":0,"volume":0,"ict":0,"quality":0,"exception":0}
    # Discovery already returns radar-sorted candidates. Inspect a little more
    # than the execution count so a few weak setups do not empty the board.
    inspect_n = max(engine.EXECUTION_N, min(len(d), engine.EXECUTION_N + 5))
    pool = d[:inspect_n]
    cand = []
    for s, radar, age, q in pool:
        try:
            c = engine.score(s, radar, age, q, reject)
            if c:
                c["symbol"] = s
                cand.append(c)
        except Exception as exc:
            reject["exception"] += 1
            engine.log.warning("SCORE FAILED | %s | %s", s.upper(), exc)
    cand.sort(key=lambda x:(x.get("eligible",False),x.get("score",0),x.get("edge",0)), reverse=True)
    engine.ranked = cand[:engine.EXECUTION_N]
    engine.desired = [x["symbol"] for x in engine.ranked]

    try:
        if _ensure_frames("btcusdt"):
            btc = engine.frame_features("btcusdt","15m")
            engine.regime = "TREND_UP" if btc and btc["ema9"] > btc["ema21"] and btc["mom"] > 0 else "TREND_DOWN" if btc and btc["ema9"] < btc["ema21"] and btc["mom"] < 0 else "CHOP"
        else:
            engine.regime = "UNKNOWN"
    except Exception:
        engine.regime = "UNKNOWN"

    engine.log.info(
        "CRYPTOALPHA 3M | regime=%s | discovered=%d | inspected=%d | ranked=%d | eligible=%d | reject=%s | %s",
        engine.regime, len(d), len(pool), len(engine.ranked),
        sum(x.get("eligible",False) for x in cand), reject,
        " ".join(f'{x["symbol"]}:{x["score"]:.2f}/{x["side"]}/{x.get("key_level","-")}' for x in engine.ranked)
    )

engine.rank = _optimized_rank

START_TIME = time.time()

def get_stats():
    with engine.lock:
        m = dict(engine.metrics); ranked = list(engine.ranked); regime = engine.regime
        equity = float(engine.equity); peak = float(engine.peak_equity)
        halted = bool(engine.trading_halted); positions = list(engine.positions.values())
    closed = int(m.get('wins', 0)) + int(m.get('losses', 0))
    dd = max(0, (peak - equity) / peak) if peak > 0 else 0
    return {
        'status':'ok','service':'cryptoalpha-3m-engine','engine':'cryptoalpha-3m-ict-smc',
        'dry_run':True,'uptime_seconds':round(time.time()-START_TIME,1),'regime':regime,
        'regime_source':'15M_CONTEXT_5M_TREND','entry_timeframe':'3M',
        'universe_mode':'MEME_TOP_GAINERS_TOP_MOVERS_NEW_LISTINGS','universe_ranked':len(ranked),
        'discovery_n':engine.DISCOVERY_N,'execution_n':engine.EXECUTION_N,
        'top_10':[x.get('symbol','').upper() for x in ranked[:10]],
        'top_10_detail':[{k:x.get(k) for k in ('symbol','side','score','edge','flow','eligible','new','ict','sweep','fvg','displacement','trend15','trend5','key_level','buildix')} for x in ranked[:10]],
        'open_positions':len(positions),'entries':int(m.get('entries',0)),'exits':int(m.get('exits',0)),
        'wins':int(m.get('wins',0)),'losses':int(m.get('losses',0)),
        'win_rate_pct':round((m.get('wins',0)/closed)*100,2) if closed else 0,
        'net_pnl_pct':round(float(m.get('pnl',0))/max(engine.START,1e-9)*100,4),
        'equity':round(equity,2),'peak_equity':round(peak,2),'drawdown_pct':round(dd*100,4),
        'trading_halted':halted,'risk_pct':engine.RISK*100,'sl_pct':engine.SL_PCT*100,
        'tp_pct':engine.TP_PCT*100,'signals':int(m.get('signals',0)),
        'market_events':sum(x.get('n',0) for b in engine.hist.values() for x in b.get('3m',[])) if engine.hist else 0,
        'completed_bars':sum(len(b.get('3m',[])) for b in engine.hist.values()) if engine.hist else 0,
        'buildix':buildix_flow.stats()
    }

DASHBOARD = r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Cryptoalpha 3M</title><style>body{margin:0;background:#080b12;color:#e8edf5;font-family:system-ui,sans-serif}.wrap{max-width:1200px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-size:25px;font-weight:800}.sub{color:#8e9aae;font-size:13px}.live{padding:7px 11px;border:1px solid #254d35;border-radius:999px;color:#6ee7a0}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px}.card{background:#10151f;border:1px solid #202938;border-radius:12px;padding:14px;margin-top:10px}.label{font-size:11px;color:#8e9aae;text-transform:uppercase}.value{font-size:24px;font-weight:800;margin-top:6px}.good{color:#6ee7a0}.bad{color:#fb7185}table{width:100%;border-collapse:collapse;font-size:12px}th,td{text-align:left;padding:8px;border-bottom:1px solid #202938}th{color:#8e9aae}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}}@media(max-width:500px){.grid{grid-template-columns:1fr 1fr}.value{font-size:20px}}</style></head><body><div class="wrap"><div class="top"><div><div class="brand">Cryptoalpha 3M</div><div class="sub">15M context → 5M ICT/SMC → 3M execution · 1% SL · 2% TP · paper</div></div><div class="live" id="live">CONNECTING</div></div><div class="grid"><div class="card"><div class="label">Equity</div><div class="value" id="eq">—</div></div><div class="card"><div class="label">P&L</div><div class="value" id="pnl">—</div></div><div class="card"><div class="label">Win rate</div><div class="value" id="wr">—</div></div><div class="card"><div class="label">Drawdown</div><div class="value" id="dd">—</div></div></div><div class="card"><b>Architecture</b><p id="arch">15M context → 5M trend/ICT-SMC → 3M execution</p><p>Risk <b id="risk">—</b> · SL <b id="sl">—</b> · TP <b id="tp">—</b> · Open <b id="op">—</b></p><p id="bx">Buildix —</p></div><div class="card"><b>Top opportunities</b><div id="rows">Waiting…</div></div><div class="card"><b>Performance</b><p id="perf">—</p></div></div><script>const $=x=>document.getElementById(x);const n=(v,d=2)=>Number(v||0).toFixed(d);async function u(){try{let d=await (await fetch('/stats.json?x='+Date.now(),{cache:'no-store'})).json();$('live').textContent='PAPER · LIVE';$('eq').textContent='$'+n(d.equity);$('pnl').textContent=n(d.net_pnl_pct,3)+'%';$('pnl').className='value '+(d.net_pnl_pct>=0?'good':'bad');$('wr').textContent=n(d.win_rate_pct)+'%';$('dd').textContent=n(d.drawdown_pct,3)+'%';$('arch').textContent='15M context → 5M ICT/SMC → 3M execution · regime '+d.regime;$('risk').textContent=n(d.risk_pct,3)+'%';$('sl').textContent=n(d.sl_pct)+'%';$('tp').textContent=n(d.tp_pct)+'%';$('op').textContent=d.open_positions;let b=d.buildix||{};$('bx').textContent='Buildix: '+(b.fresh?'FRESH':'WAITING')+' · signals '+b.symbols_with_signals+' · smart money '+b.symbols_with_smart_money+' · updates '+b.updates+' · errors '+b.errors;$('perf').textContent='Entries '+d.entries+' · Exits '+d.exits+' · Wins '+d.wins+' · Losses '+d.losses+' · Signals '+d.signals+' · Bars '+d.completed_bars;let r=(d.top_10_detail||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+x.symbol+'</td><td>'+x.side+'</td><td>'+n(x.score)+'</td><td>'+x.key_level+'</td><td>'+x.sweep+'</td><td>'+x.fvg+'</td><td>'+x.displacement+'</td><td>'+(x.buildix?.available?'YES':'—')+'</td></tr>').join('');$('rows').innerHTML=r?'<table><tr><th>#</th><th>Symbol</th><th>Side</th><th>Score</th><th>Key level</th><th>Sweep</th><th>FVG</th><th>Disp.</th><th>Buildix</th></tr>'+r+'</table>':'No qualified setups yet.'}catch(e){$('live').textContent='OFFLINE'}}u();setInterval(u,3000)</script></body></html>'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=self.path.split('?',1)[0]
        if path in ('/','/health','/healthz'):
            self._json({'status':'ok','service':'cryptoalpha-3m-engine','engine':'cryptoalpha-3m-ict-smc','dry_run':True})
        elif path=='/stats.json': self._json(get_stats())
        elif path=='/stats':
            raw=DASHBOARD.encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        else:self.send_response(404);self.end_headers()
    def _json(self,p):
        raw=json.dumps(p,separators=(',',':')).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def log_message(self,*a):pass

def main():
    port=int(os.getenv('PORT','10000'))
    threading.Thread(target=engine.main,name='cryptoalpha-3m',daemon=True).start()
    server=ThreadingHTTPServer(('0.0.0.0',port),Handler)
    print('Cryptoalpha 3M dashboard on :%d'%port,flush=True)
    server.serve_forever()
if __name__=='__main__':main()