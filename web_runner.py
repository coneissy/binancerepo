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

DASHBOARD = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Cryptoalpha 1M</title><style>body{font-family:system-ui;margin:20px;max-width:1100px}pre{white-space:pre-wrap;word-break:break-word}</style></head><body><h1>Cryptoalpha 1M</h1><p>Dynamic universe · regime engine · adaptive strategy ensemble · 30s microstructure confirmation · trailing exits · paper mode</p><pre id="x">Loading...</pre><script>async function u(){try{let r=await fetch('/stats.json?x='+Date.now(),{cache:'no-store'});x.textContent=JSON.stringify(await r.json(),null,2)}catch(e){x.textContent='OFFLINE'}}u();setInterval(u,3000)</script></body></html>'''

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
