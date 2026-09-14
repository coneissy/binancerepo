import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen, Request

ENGINE_URL = os.getenv('ENGINE_URL', 'https://binance-30s-engine.onrender.com').rstrip('/')

HTML = r'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Cryptoalpha Trading Dashboard</title>
<style>
:root{--bg:#070a0f;--panel:#0e141d;--panel2:#111925;--line:#202b3a;--text:#edf3fb;--muted:#8492a6;--good:#27d17f;--warn:#f4b740;--bad:#ff5d6c;--blue:#5aa7ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80% -10%,#17243a 0,#070a0f 38%);color:var(--text);font:14px Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1280px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px}.brand{display:flex;gap:12px;align-items:center}.logo{width:42px;height:42px;border-radius:12px;background:linear-gradient(135deg,#5aa7ff,#7d6cff);display:grid;place-items:center;font-weight:900}.title{font-size:23px;font-weight:850}.sub{color:var(--muted);font-size:12px;margin-top:3px}.badge{border:1px solid #244b3b;background:#0d2119;color:#69e6a6;border-radius:999px;padding:8px 12px;font-weight:750;font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.grid2{display:grid;grid-template-columns:1.2fr .8fr;gap:10px;margin-top:10px}.card{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:14px;padding:15px;box-shadow:0 12px 30px #0004}.label{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}.value{font-size:25px;font-weight:850;margin-top:7px}.delta{font-size:12px;margin-top:4px}.good{color:var(--good)}.warn{color:var(--warn)}.bad{color:var(--bad)}.blue{color:var(--blue)}.section{font-weight:800;margin-bottom:12px}.hero{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.heroMain{font-size:18px;font-weight:800}.mono{font-variant-numeric:tabular-nums}.meter{height:7px;background:#18212d;border-radius:9px;overflow:hidden;margin-top:12px}.fill{height:100%;background:linear-gradient(90deg,#27d17f,#5aa7ff);width:0}.tableWrap{overflow:auto}table{width:100%;border-collapse:collapse;min-width:680px}th,td{padding:10px 8px;border-bottom:1px solid var(--line);text-align:left;font-size:12px}th{color:var(--muted);font-weight:650;text-transform:uppercase;font-size:10px;letter-spacing:.06em}.tag{display:inline-flex;border:1px solid #2a384b;border-radius:7px;padding:4px 7px;color:#cbd7e7;background:#0b1119}.status{display:grid;grid-template-columns:1fr 1fr;gap:8px}.statusItem{background:#0a1017;border:1px solid var(--line);padding:10px;border-radius:10px}.statusDot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--good);margin-right:6px}.footer{color:var(--muted);font-size:11px;margin-top:12px}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}}@media(max-width:520px){.wrap{padding:10px}.grid{grid-template-columns:1fr}.top{align-items:flex-start}.title{font-size:19px}.badge{font-size:10px;padding:7px}}
</style></head><body><div class="wrap">
<div class="top"><div class="brand"><div class="logo">CA</div><div><div class="title">Cryptoalpha Trading Dashboard</div><div class="sub">Live market telemetry · paper execution · Binance Spot + USDT-M Futures</div></div></div><div class="badge" id="mode">● PAPER ONLY · LIVE OFF</div></div>
<div class="grid">
<div class="card"><div class="label">Equity</div><div class="value mono" id="equity">$10.000000</div><div class="delta good" id="pnl">+$0.000000</div></div>
<div class="card"><div class="label">Compounded return</div><div class="value mono" id="return">0.0000%</div><div class="delta" id="draw">Drawdown 0.00%</div></div>
<div class="card"><div class="label">Qualified opportunities</div><div class="value mono" id="opps">0</div><div class="delta" id="split">Spot 0 · Futures 0</div></div>
<div class="card"><div class="label">Paper entries</div><div class="value mono" id="entries">0</div><div class="delta" id="wl">Wins 0 · Losses 0</div></div>
</div>
<div class="grid2">
<div class="card"><div class="section">Best executable-looking setup</div><div class="hero"><div><div class="label">Spot triangular</div><div class="heroMain" id="bestSpot">Waiting for market data…</div></div><div class="tag mono" id="spotBps">— bps</div></div><div class="meter"><div class="fill" id="spotFill"></div></div><div class="footer" id="spotMeta">No qualified setup yet</div></div>
<div class="card"><div class="section">Connection health</div><div class="status"><div class="statusItem"><span class="statusDot"></span>Spot WS <b id="spotWs">0/0</b></div><div class="statusItem"><span class="statusDot"></span>Futures WS <b id="futWs">0/0</b></div><div class="statusItem">Spot updates <b id="spotUpd">0</b></div><div class="statusItem">Futures updates <b id="futUpd">0</b></div></div><div class="footer" id="health">Connecting…</div></div>
</div>
<div class="card" style="margin-top:10px"><div class="section">Top opportunities</div><div class="tableWrap"><table><thead><tr><th>Engine</th><th>Market / Path</th><th>Direction</th><th>Gross</th><th>Net</th><th>Notional</th></tr></thead><tbody id="rows"><tr><td colspan="6">Waiting for live telemetry…</td></tr></tbody></table></div></div>
<div class="grid2">
<div class="card"><div class="section">Futures basis leader</div><div id="bestFut">Waiting for market data…</div><div class="footer" id="futMeta">—</div></div>
<div class="card"><div class="section">Engine controls & limits</div><div class="status"><div class="statusItem">Min net <b id="minNet">—</b></div><div class="statusItem">Risk <b id="risk">—</b></div><div class="statusItem">Symbols <b id="symbols">—</b></div><div class="statusItem">Routes <b id="routes">—</b></div></div><div class="footer" id="engine">Engine status: —</div></div>
</div>
<div class="footer">Dashboard refreshes every 1 second. Data is proxied from the running engine; this dashboard does not enable live order execution.</div>
</div>
<script>
const $=id=>document.getElementById(id); const n=(x,d=2)=>Number(x||0).toFixed(d); const money=x=>'$'+n(x,6);
function setText(id,v){$(id).textContent=v}
async function refresh(){try{const r=await fetch('/api/stats?ts='+Date.now(),{cache:'no-store'});const d=await r.json();
setText('equity',money(d.equity));setText('pnl',(d.paper_pnl>=0?'+':'')+money(d.paper_pnl));setText('return',n(d.compound_return_pct,4)+'%');setText('draw','Drawdown '+n(d.drawdown_pct,2)+'%');
setText('opps',(d.spot_opportunities||0)+(d.futures_opportunities||0));setText('split','Spot '+(d.spot_opportunities||0)+' · Futures '+(d.futures_opportunities||0));setText('entries',d.paper_entries||0);setText('wl','Wins '+(d.wins||0)+' · Losses '+(d.losses||0));
const s=d.best_spot,f=d.best_futures;setText('bestSpot',s?(s.path||s.symbol):'No qualified triangular setup');setText('spotBps',s?n(s.net_bps)+' bps':'— bps');setText('spotMeta',s?'Gross '+n(s.gross_bps)+' bps · Notional '+money(s.notional_usdt):'No qualified setup yet');$('spotFill').style.width=Math.min(100,Math.max(3,(s?s.net_bps:0)*4))+'%';
setText('bestFut',f?(f.symbol+' · '+n(f.net_bps)+' bps'): 'No qualified futures setup');setText('futMeta',f?(f.direction+' · basis '+n(f.basis_bps)+' bps · notional '+money(f.notional_usdt)):'—');
setText('spotWs',(d.spot_sockets_up||0)+'/'+(d.spot_sockets||0));setText('futWs',(d.futures_sockets_up||0)+'/'+(d.futures_sockets||0));setText('spotUpd',d.quote_updates_spot||0);setText('futUpd',d.quote_updates_futures||0);setText('health',(d.ws_spot?'Spot connected':'Spot reconnecting')+' · '+(d.ws_futures?'Futures connected':'Futures reconnecting')+' · errors '+(d.errors||0));
setText('minNet',n(d.min_net_bps)+' bps');setText('risk',n(d.risk_pct,3)+'%');setText('symbols',(d.spot_symbols||0)+' spot / '+(d.futures_symbols||0)+' futures');setText('routes',d.triangular_routes||0);setText('engine',(d.halted?'HALTED':'RUNNING')+' · '+n(d.uptime_seconds,0)+'s uptime · '+n(d.scans,0)+' scans');
const rows=(d.top_opportunities||[]).slice(0,25).map(x=>'<tr><td><span class="tag">'+(x.engine||'—')+'</span></td><td>'+((x.path||x.symbol)||'—')+'</td><td>'+(x.direction||'—')+'</td><td>'+n(x.gross_bps)+' bps</td><td class="good">'+n(x.net_bps)+' bps</td><td>'+money(x.notional_usdt)+'</td></tr>').join('');$('rows').innerHTML=rows||'<tr><td colspan="6">No qualified opportunities at the moment</td></tr>';
}catch(e){setText('health','Dashboard connection error: '+e.message)}} refresh(); setInterval(refresh,1000);
</script></body></html>'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/api/stats'):
            try:
                req=Request(ENGINE_URL+'/stats.json',headers={'User-Agent':'Cryptoalpha-Dashboard/1.0'})
                with urlopen(req,timeout=5) as r: body=r.read()
                self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body)
            except Exception as e:
                body=json.dumps({'error':str(e)}).encode(); self.send_response(502); self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(body)
            return
        body=HTML.encode(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass

if __name__=='__main__':
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()
