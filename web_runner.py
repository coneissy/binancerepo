import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import hft_scalper_30s as engine
import top_gainer_regime
import htf_regime
import dynamic_largecap_universe

# Higher timeframe architecture: 1h primary regime, 15m confirmation,
# 5m setup confirmation and 1m execution timing.
htf_regime.install(engine)
# Dynamic universe: top large-cap pool, then select the 10 with strongest
# live volatility, relative volume and momentum.
engine.discover = lambda: dynamic_largecap_universe.discover(engine)
# Keep the existing duplicate-symbol cooldown guard.
top_gainer_regime.install_symbol_guard(engine)

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

    exits = int(m.get('exits', 0))
    wins = int(m.get('wins', 0))
    losses = int(m.get('losses', 0))
    gross_profit = float(m.get('gross_profit', 0.0))
    gross_loss = float(m.get('gross_loss', 0.0))
    closed = wins + losses
    drawdown = max(0.0, (peak_equity - equity) / peak_equity) if peak_equity > 0 else 0.0

    return {
        'status': 'ok',
        'service': 'binance-1m-engine',
        'engine': 'alpha-htf-1m',
        'dry_run': True,
        'uptime_seconds': round(time.time() - START_TIME, 1),
        'regime': regime,
        'regime_source': '1H_PRIMARY_15M_CONFIRM',
        'universe_mode': 'TOP_LARGECAP_HIGH_VOL_10',
        'largecap_pool': dynamic_largecap_universe.POOL,
        'universe_ranked': len(ranked),
        'discovery_n': engine.DISCOVERY_N,
        'execution_n': engine.EXECUTION_N,
        'top_10': [x.get('symbol', '').upper() for x in ranked[:10]],
        'open_positions': open_positions,
        'entries': int(m.get('entries', 0)),
        'exits': exits,
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round((wins / closed) * 100, 2) if closed else 0.0,
        'net_pnl_pct': round(float(m.get('net_pnl', m.get('pnl', 0.0))) * 100, 4),
        'gross_profit_pct': round(gross_profit * 100, 4),
        'gross_loss_pct': round(gross_loss * 100, 4),
        'profit_factor': round(gross_profit / gross_loss, 3) if gross_loss > 0 else (None if gross_profit == 0 else 999.0),
        'signals': int(m.get('signals', 0)),
        'market_events': int(m.get('events', 0)),
        'completed_bars': int(m.get('bars', 0)),
        'equity': round(equity, 2),
        'peak_equity': round(peak_equity, 2),
        'drawdown_pct': round(drawdown * 100, 4),
        'trading_halted': trading_halted,
    }


DASHBOARD = '''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Binance HTF Alpha Engine</title>
<style>
body{margin:0;background:#080b12;color:#e9eef7;font-family:Arial,sans-serif}.wrap{max-width:1100px;margin:auto;padding:20px}
h1{margin:0;font-size:25px}.sub{color:#8e9aad;margin:6px 0 20px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.card{background:#111722;border:1px solid #202a3a;border-radius:14px;padding:16px}.label{color:#8996aa;font-size:12px;text-transform:uppercase}.value{font-size:27px;font-weight:700;margin-top:7px}
.good{color:#45d483}.bad{color:#ff667a}.warn{color:#ffc857}.bar{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
table{width:100%;border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #202a3a;text-align:left}th{color:#8996aa;font-size:12px}
.pill{padding:5px 9px;border-radius:20px;background:#1d2736;font-size:12px}.section{margin-top:16px}.muted{color:#8996aa}.refresh{font-size:12px;color:#8996aa}
@media(max-width:750px){.grid{grid-template-columns:repeat(2,1fr)}.value{font-size:22px}}
</style></head><body><div class="wrap">
<div class="bar"><div><h1>⚡ Binance HTF Alpha Engine</h1><div class="sub">Top Large-Cap Universe · High Volatility · 1H regime · 15M confirmation · 5M setup · 1M execution · Paper Trading</div></div><div id="status" class="pill">CONNECTING</div></div>
<div class="grid">
<div class="card"><div class="label">Net P&L</div><div id="pnl" class="value">—</div></div>
<div class="card"><div class="label">Win Rate</div><div id="win" class="value">—</div></div>
<div class="card"><div class="label">Profit Factor</div><div id="pf" class="value">—</div></div>
<div class="card"><div class="label">Market Regime</div><div id="regime" class="value">—</div></div>
<div class="card"><div class="label">Entries</div><div id="entries" class="value">—</div></div>
<div class="card"><div class="label">Exits</div><div id="exits" class="value">—</div></div>
<div class="card"><div class="label">Open Positions</div><div id="open" class="value">—</div></div>
<div class="card"><div class="label">Ranked Universe</div><div id="universe" class="value">—</div></div>
<div class="card"><div class="label">Equity</div><div id="equity" class="value">—</div></div>
<div class="card"><div class="label">Drawdown</div><div id="dd" class="value">—</div></div>
<div class="card"><div class="label">Signals</div><div id="signals" class="value">—</div></div>
<div class="card"><div class="label">Bars</div><div id="bars" class="value">—</div></div>
</div>
<div class="section card"><div class="bar"><b>Top 10 Large-Cap / High-Volatility Candidates</b><span id="refresh" class="refresh">Auto refresh 3s</span></div><table><thead><tr><th>#</th><th>Symbol</th></tr></thead><tbody id="coins"></tbody></table></div>
<div class="section card"><div class="bar"><b>HTF Engine Activity</b></div><div class="muted" id="activity">Loading...</div></div>
</div>
<script>
async function update(){try{let r=await fetch('/stats.json?x='+Date.now(),{cache:'no-store'});let d=await r.json();
status.textContent=d.dry_run?'● PAPER / DRY RUN':'LIVE';status.className='pill';
let p=d.net_pnl_pct;pnl.textContent=(p>=0?'+':'')+p.toFixed(4)+'%';pnl.className='value '+(p>=0?'good':'bad');
win.textContent=d.win_rate_pct.toFixed(2)+'%';pf.textContent=d.profit_factor===null?'—':d.profit_factor.toFixed(3);regime.textContent=(d.regime||'UNKNOWN');
entries.textContent=d.entries;exits.textContent=d.exits;open.textContent=d.open_positions;universe.textContent=d.universe_ranked+'/'+d.discovery_n;
equity.textContent=d.equity.toFixed(2);dd.textContent=d.drawdown_pct.toFixed(3)+'%';signals.textContent=d.signals;bars.textContent=d.completed_bars;
coins.innerHTML=(d.top_10||[]).map((s,i)=>'<tr><td>'+(i+1)+'</td><td><b>'+s+'</b></td></tr>').join('')||'<tr><td colspan="2" class="muted">No ranked symbols yet</td></tr>';
activity.textContent='Universe: top '+d.largecap_pool+' market-cap pool → dynamic high-volatility top 10 · Architecture: 1H primary → 15M confirmation → 5M setup → 1M execution · Regime: '+d.regime+' · Signals: '+d.signals+' · Completed 1m bars: '+d.completed_bars+' · Uptime: '+Math.round(d.uptime_seconds/60)+' min'+(d.trading_halted?' · TRADING HALTED':'');
}catch(e){status.textContent='OFFLINE';status.className='pill bad';}}
update();setInterval(update,3000);
</script></body></html>'''


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split('?', 1)[0]
        if path in ('/', '/health', '/healthz'):
            payload = {
                'status': 'ok',
                'service': 'binance-1m-engine',
                'engine': 'alpha-htf-1m',
                'dry_run': True,
                'regime_source': '1H_PRIMARY_15M_CONFIRM',
                'universe_mode': 'TOP_LARGECAP_HIGH_VOL_10',
            }
            self._json(payload)
        elif path == '/stats.json':
            self._json(get_stats())
        elif path == '/stats':
            raw = DASHBOARD.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, payload):
        raw = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):
        return


def main():
    port = int(os.getenv('PORT', '10000'))
    worker = threading.Thread(target=engine.main, name='alpha-1m', daemon=True)
    worker.start()
    server = ThreadingHTTPServer(('0.0.0.0', port), HealthHandler)
    print('Health/stats server for HTF alpha engine listening on :%d' % port, flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
