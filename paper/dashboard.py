from __future__ import annotations

import csv
import html
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LEDGER = Path(os.getenv("PAPER_LEDGER", "data/binance_futures_scalper_paper.csv"))
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))


def load_trades():
    if not LEDGER.exists():
        return []
    with LEDGER.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def metrics(trades):
    pnls = [float(t["net_pnl"]) for t in trades]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    equity = peak = drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(pnls) if pnls else 0.0,
        "gross_pnl": sum(float(t["gross_pnl"]) for t in trades),
        "fees": sum(float(t["fees"]) for t in trades),
        "net_pnl": sum(pnls),
        "profit_factor": sum(wins) / abs(sum(losses)) if losses else None,
        "expectancy": sum(pnls) / len(pnls) if pnls else 0.0,
        "max_drawdown": drawdown,
    }


def page():
    trades = load_trades()
    m = metrics(trades)
    cards = [
        ("Trades", str(m["trades"])),
        ("Win rate", f'{m["win_rate"] * 100:.1f}%'),
        ("Net P&L", "$" + f'{m["net_pnl"]:.4f}'),
        ("Profit factor", "n/a" if m["profit_factor"] is None else f'{m["profit_factor"]:.2f}'),
        ("Expectancy", "$" + f'{m["expectancy"]:.4f}'),
        ("Max drawdown", "$" + f'{m["max_drawdown"]:.4f}'),
    ]
    card_html = "".join(
        '<div class="card"><div class="label">' + html.escape(k) +
        '</div><div class="value">' + html.escape(v) + '</div></div>'
        for k, v in cards
    )
    rows = ""
    for t in reversed(trades[-30:]):
        rows += "<tr>"
        for k in ("closed_at", "side", "entry_price", "exit_price", "net_pnl", "reason"):
            rows += "<td>" + html.escape(str(t.get(k, ""))) + "</td>"
        rows += "</tr>"
    if not rows:
        rows = '<tr><td colspan="6" class="empty">No paper trades recorded yet.</td></tr>'
    return """<!doctype html>
<html><head><meta charset="utf-8"><meta http-equiv="refresh" content="10">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Binance Scalper Paper Dashboard</title>
<style>
body{margin:0;background:#0d1117;color:#e6edf3;font:14px system-ui,sans-serif}
main{max-width:1200px;margin:0 auto;padding:28px}
h1{margin:0 0 6px;font-size:28px}.sub{color:#8b949e;margin-bottom:24px}
.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:16px}
.label{color:#8b949e;font-size:12px}.value{font-size:22px;margin-top:8px}
.panel{margin-top:20px;background:#161b22;border:1px solid #30363d;border-radius:12px;padding:16px;overflow:auto}
table{width:100%;border-collapse:collapse}th,td{padding:9px;border-bottom:1px solid #30363d;text-align:left;white-space:nowrap}
th{color:#8b949e;font-size:12px}.empty{text-align:center;color:#8b949e;padding:30px}
.badge{display:inline-block;padding:4px 8px;border:1px solid #30363d;border-radius:999px;color:#7ee787}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main>
<h1>Binance Scalper</h1>
<div class="sub"><span class="badge">PAPER ONLY</span> · BTCUSDT · 1m · auto-refresh 10s</div>
<div class="grid">""" + card_html + """</div>
<div class="panel"><h2>Recent simulated trades</h2>
<table><thead><tr><th>Closed</th><th>Side</th><th>Entry</th><th>Exit</th><th>Net P&L</th><th>Reason</th></tr></thead>
<tbody>""" + rows + """</tbody></table></div></main></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "paper_only": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = page().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"Paper dashboard listening on {HOST}:{PORT}; ledger={LEDGER}")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
