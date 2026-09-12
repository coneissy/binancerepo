import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import hft_scalper


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/', '/health', '/healthz'):
            self.send_response(404)
            self.end_headers()
            return
        payload = b'{"status":"ok","service":"binance-hft-scalper","dry_run":true}'
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def main():
    port = int(os.getenv('PORT', '10000'))
    worker = threading.Thread(target=hft_scalper.main, name='hft-scalper', daemon=True)
    worker.start()
    server = ThreadingHTTPServer(('0.0.0.0', port), HealthHandler)
    print(f'Health server listening on :{port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
