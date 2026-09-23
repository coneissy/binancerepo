from __future__ import annotations

import os
import threading
from pathlib import Path

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from binance_futures_scalper_paper import Config, run

LEDGER = Path(os.getenv("PAPER_LEDGER", "data/binance_futures_scalper_paper.csv"))
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))


def start_runner() -> None:
    cfg = Config(
        symbol=os.getenv("SYMBOL", "BTCUSDT"),
        quantity_quote=float(os.getenv("QUANTITY_QUOTE", "100")),
        fee_bps=float(os.getenv("FEE_BPS", "5")),
        slippage_bps=float(os.getenv("SLIPPAGE_BPS", "1")),
    )
    run(
        cfg,
        LEDGER,
        poll_seconds=int(os.getenv("POLL_SECONDS", "10")),
        duration_minutes=None,
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = b'{"status":"ok","paper_only":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            from dashboard import page
            body = page().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    if os.getenv("PAPER_ONLY", "true").lower() != "true":
        raise RuntimeError("PAPER_ONLY must be true")

    threading.Thread(target=start_runner, daemon=True, name="paper-runner").start()
    print(f"Paper scalper dashboard listening on {HOST}:{PORT}; ledger={LEDGER}")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
