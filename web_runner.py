import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import hft_scalper_30s as engine
import top_gainer_regime
import htf_regime
import dynamic_largecap_universe
import alpha_upgrade
import micro_execution

htf_regime.install(engine)
engine.discover = lambda: dynamic_largecap_universe.discover(engine)
top_gainer_regime.install_symbol_guard(engine)
alpha_upgrade.install(engine)
micro_execution.install(engine)

START_TIME = time.time()


def get_stats():
    with engine.lock:
        m = dict(engine.metrics)
        open_positions = len(engine.positions)
        ranked = list(engine.ranked)
        regime = engine.regime
        equity = float(engine.equity)
        peak_equity = float(engine.peak_equity)
        trading_halted = bool(engine.trading_halted)
    exits = int(m.get('exits', 0)); wins = int(m.get('wins', 0)); losses = int(m.get('losses', 0))
    gross_profit = float(m.get('gross_profit', 0.0)); gross_loss = float(m.get('gross_loss', 0.0))
    closed = wins + losses
    drawdown = max(0.0, (peak_equity - equity) / peak_equity) if peak_equity > 0 else 0.0
    return {
        'status': 'ok', 'service': 'cryptoalpha-1m-engine', 'engine': 'cryptoalpha-1m-adaptive-micro', 'dry_run': True,
        'uptime_seconds': round(time.time() - START_TIME, 1), 'regime': regime,
        'regime_source': '1H_PRIMARY_15M_CONFIRM', 'universe_mode': 'ALL_USDT_PERPETUAL_LIVE_VOLUME',
        'universe_ranked': len(ranked), 'discovery_n': engine.DISCOVERY_N, 'execution_n': engine.EXECUTION_N,
        'top_10': [x.get('symbol', '').upper() for x in ranked[:10]],
        'top_10_detail': [{k:x.get(k) for k in ('symbol','side','score','ensemble_score','strategy','edge','flow','z','rv','eligible','new','micro_confirmed','micro')} for x in ranked[:10]],
        'open_positions': open_positions,
        'entries': int(m.get('entries', 0)), 'exits': exits, 'wins': wins, 'losses': losses,
        'win_rate_pct': round((wins / closed) * 100, 2) if closed else 0.0,
        'net_pnl_pct': round(float(m.get('net_pnl', m.get('pnl', 0.0))) * 100, 4),
        'gross_profit_pct': round(gross_profit * 100, 4), 'gross_loss_pct': round(gross_loss * 100, 4),
        'profit_factor': round(gross_profit / gross_loss, 3) if gross_loss > 0 else (None if gross_profit == 0 else 999.0),
        'signals': int(m.get('signals', 0)), 'market_events': int(m.get('events', 0)),
        'completed_bars': int(m.get('bars', 0)), 'equity': round(equity, 2), 'peak_equity': round(peak_equity, 2),
        'drawdown_pct': round(drawdown * 100, 4), 'trading_halted': trading_halted,
        'adaptive_layer': alpha_upgrade.stats(),
        'micro_execution_layer': micro_execution.stats(),
    }

DASHBOARD = r'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cryptoalpha — Live Paper Dashboard</title>
<style>
:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#080b12;color:#e8edf5;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
.wrap{max-width:1250px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:15px;align-items:center;flex-wrap:wrap;margin-bottom:18px}.brand{font-size:25px;font-weight:800}.sub{color:#8e9aae;font-size:13px;margin-top:3px}.live{padding:7px 11px;border:1px solid #254d35;border-radius:999px;color:#6ee7a0;background:#0c1b13;font-size:12px;font-weight:700}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:#10151f;border:1px solid #202938;border-radius:12px;padding:15px}.label{font-size:11px;color:#8e9aae;text-transform:uppercase;letter-spacing:.08em}.value{font-size:25px;font-weight:800;margin-top:7px}.small{font-size:12px;color:#8e9aae;margin-top:4px}.section{margin-top:14px}.section h2{font-size:15px;margin:0 0 9px}.two{display:grid;grid-template-columns:1fr 1fr;gap:10px}.pill{display:inline-block;padding:4px 8px;border-radius:7px;background:#182131;color:#cbd5e1;margin:3px 3px 0 0;font-size:12px}.good{color:#6ee7a0}.bad{color:#fb7185}.warn{color:#fbbf24}table{width:100%;border-collapse:collapse;font-size:12px}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid #202938}th{color:#8e9aae;font-weight:600}.right{text-align:right}.footer{color:#687386;font-size:11px;margin:14px 2px}.empty{color:#8e9aae;padding:10px 0}.statusline{display:flex;gap:7px;flex-wrap:wrap}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.two{grid-template-columns:1fr}}@media(max-width:520px){.grid{grid-template-columns:1fr 1fr}.value{font-size:21px}.wrap{padding:11px}}
</style></head><body><div class="wrap">
<div class="top"><div><div class="brand">Cryptoalpha</div><div class="sub">1M adaptive engine · 30s microstructure execution · paper trading</div></div><div class="live" id="live">CONNECTING</div></div>
<div class="grid">
<div class="card"><div class="label">Equity</div><div class="value" id="equity">—</div><div class="small" id="peak">Peak —</div></div>
<div class="card"><div class="label">Net P&L</div><div class="value" id="pnl">—</div><div class="small" id="pf">Profit factor —</div></div>
<div class="card"><div class="label">Win rate</div><div class="value" id="wr">—</div><div class="small" id="wl">W — / L —</div></div>
<div class="card"><div class="label">Drawdown</div><div class="value" id="dd">—</div><div class="small" id="halt">Trading —</div></div>
</div>
<div class="section two">
<div class="card"><h2>Engine status</h2><div class="statusline"><span class="pill" id="regime">Regime —</span><span class="pill" id="positions">Open —</span><span class="pill" id="signals">Signals —</span><span class="pill" id="bars">Bars —</span></div><div class="small" id="uptime">Uptime —</div><div class="small" id="mode">Mode —</div></div>
<div class="card"><h2>Architecture</h2><div class="small">Universe: <b id="universe">—</b></div><div class="small">Regime: <b id="regimesource">—</b></div><div class="small">Adaptive: <b id="adaptive">—</b></div><div class="small">Micro execution: <b id="micro">—</b></div></div>
</div>
<div class="section card"><h2>Top ranked opportunities</h2><div id="ranked" class="empty">Waiting for engine data…</div></div>
<div class="section card"><h2>Performance</h2><div class="statusline"><span class="pill" id="entries">Entries —</span><span class="pill" id="exits">Exits —</span><span class="pill" id="gp">Gross profit —</span><span class="pill" id="gl">Gross loss —</span><span class="pill" id="events">Market events —</span></div></div>
<div class="footer">Auto-refresh: 3 seconds · DRY_RUN=true · Dashboard reads the engine's live in-memory paper statistics. P&L is not a guarantee of future performance.</div>
</div><script>
const $=id=>document.getElementById(id);const pct=v=>((Number(v)||0).toFixed(3)+'%');
function esc(s){return String(s??'').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\\':'&#92;','"':'&quot;'}[c]))}
function fmt(v){return v==null?'—':Number(v).toFixed(3)}
async function update(){try{let r=await fetch('/stats.json?x='+Date.now(),{cache:'no-store'});if(!r.ok)throw Error();let d=await r.json();
$('live').textContent=d.dry_run?'PAPER · LIVE':'LIVE';$('live').className='live';$('equity').textContent='$'+Number(d.equity||0).toFixed(2);$('peak').textContent='Peak $'+Number(d.peak_equity||0).toFixed(2);
$('pnl').textContent=pct(d.net_pnl_pct);$('pnl').className='value '+(d.net_pnl_pct>=0?'good':'bad');$('pf').textContent='Profit factor '+(d.profit_factor==null?'—':d.profit_factor);
$('wr').textContent=pct(d.win_rate_pct);$('wl').textContent='W '+d.wins+' / L '+d.losses;$('dd').textContent=pct(d.drawdown_pct);$('dd').className='value '+(d.drawdown_pct>5?'bad':'good');$('halt').textContent='Trading '+(d.trading_halted?'HALTED':'ACTIVE');
$('regime').textContent='Regime: '+(d.regime||'—');$('positions').textContent='Open: '+d.open_positions;$('signals').textContent='Signals: '+d.signals;$('bars').textContent='Bars: '+d.completed_bars;$('uptime').textContent='Uptime '+Math.floor(d.uptime_seconds/60)+'m '+Math.floor(d.uptime_seconds%60)+'s';$('mode').textContent='Mode: '+(d.dry_run?'PAPER / NO LIVE ORDERS':'LIVE');
$('universe').textContent=d.universe_mode+' · '+d.universe_ranked+' ranked';$('regimesource').textContent=d.regime_source;$('adaptive').textContent=d.adaptive_layer?'ACTIVE':'—';$('micro').textContent=d.micro_execution_layer?'ACTIVE':'—';
$('entries').textContent='Entries: '+d.entries;$('exits').textContent='Exits: '+d.exits;$('gp').textContent='Gross profit: '+pct(d.gross_profit_pct);$('gl').textContent='Gross loss: '+pct(d.gross_loss_pct);$('events').textContent='Market events: '+d.market_events;
let rows=(d.top_10_detail||[]).map((x,i)=>'<tr><td>'+(i+1)+'</td><td class="mono">'+esc((x.symbol||'').toUpperCase())+'</td><td>'+esc(x.side||'—')+'</td><td>'+fmt(x.score)+'</td><td>'+fmt(x.ensemble_score)+'</td><td>'+esc(x.strategy||'—')+'</td><td>'+fmt(x.edge)+'</td><td>'+esc(x.micro_confirmed?'YES':'—')+'</td></tr>').join('');
$('ranked').innerHTML=rows?'<table><thead><tr><th>#</th><th>Symbol</th><th>Side</th><th>Score</th><th>Ensemble</th><th>Strategy</th><th>Edge</th><th>30s confirm</th></tr></thead><tbody>'+rows+'</tbody></table>':'<div class="empty">No ranked opportunities yet.</div>';
}catch(e){$('live').textContent='OFFLINE';$('live').className='live bad'}}update();setInterval(update,3000);
</script></body></html>'''


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        if path in ('/', '/health', '/healthz'):
            self._json({'status':'ok','service':'cryptoalpha-1m-engine','engine':'cryptoalpha-1m-adaptive-micro','dry_run':True,'regime_source':'1H_PRIMARY_15M_CONFIRM','universe_mode':'ALL_USDT_PERPETUAL_LIVE_VOLUME'})
        elif path == '/stats.json': self._json(get_stats())
        elif path == '/stats':
            raw=DASHBOARD.encode(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
        else: self.send_response(404); self.end_headers()
    def _json(self,payload):
        raw=json.dumps(payload,separators=(',',':')).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def log_message(self, format, *args): return


def main():
    port=int(os.getenv('PORT','10000')); worker=threading.Thread(target=engine.main,name='cryptoalpha-1m',daemon=True); worker.start(); server=ThreadingHTTPServer(('0.0.0.0',port),HealthHandler); print('Health/stats server for Cryptoalpha 1M engine listening on :%d'%port,flush=True); server.serve_forever()

if __name__ == '__main__': main()
