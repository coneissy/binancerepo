import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import hft_scalper_30s

START_TIME = time.time()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/', '/health', '/healthz'):
            payload = {
                "status": "ok",
                "service": "binance-30s-engine",
                "dry_run": True,
            }
        elif self.path == '/stats':
            with hft_scalper_30s.lock:
                m = dict(hft_scalper_30s.metrics)
                open_positions = len(hft_scalper_30s.positions)
                ranked_count = len(hft_scalper_30s.ranked)
                regime = hft_scalper_30s.market_regime
                top_symbols = [x["symbol"].upper() for x in hft_scalper_30s.ranked[:10]]

            exits = int(m.get("exits", 0))
            wins = int(m.get("wins", 0))
            losses = int(m.get("losses", 0))
            gross_profit = float(m.get("gross_profit", 0.0))
            gross_loss = float(m.get("gross_loss", 0.0))
            closed = wins + losses
            payload = {
                "status": "ok",
                "service": "binance-30s-engine",
                "dry_run": True,
                "uptime_seconds": round(time.time() - START_TIME, 1),
                "regime": regime,
                "universe_ranked": ranked_count,
                "top_10": top_symbols,
                "open_positions": open_positions,
                "entries": int(m.get("entries", 0)),
                "exits": exits,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": round((wins / closed) * 100, 2) if closed else 0.0,
                "net_pnl_pct": round(float(m.get("net_pnl", 0.0)) * 100, 4),
                "gross_profit_pct": round(gross_profit * 100, 4),
                "gross_loss_pct": round(gross_loss * 100, 4),
                "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else (None if gross_profit == 0 else 999.0),
                "signals": int(m.get("signals", 0)),
                "market_events": int(m.get("events", 0)),
                "completed_bars": int(m.get("bars", 0)),
            }
        else:
            self.send_response(404)
            self.end_headers()
            return

        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
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
    worker = threading.Thread(target=hft_scalper_30s.main, name='scalp-30s', daemon=True)
    worker.start()
    server = ThreadingHTTPServer(('0.0.0.0', port), HealthHandler)
    print(f'Health/stats server listening on :{port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
