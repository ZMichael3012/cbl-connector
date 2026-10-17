import hashlib
import http.server

MAX_BODY = 1 << 20


class Handler(http.server.BaseHTTPRequestHandler):
    """Logs the BTCPay-Sig header and the body hash of every delivery."""

    def do_POST(self):
        try:
            length = min(int(self.headers.get('Content-Length', 0)), MAX_BODY)
        except ValueError:
            length = 0
        body = self.rfile.read(length)
        signature = self.headers.get('BTCPay-Sig', '-')
        digest = hashlib.sha256(body).hexdigest()
        print(f'BTCPAY-WEBHOOK {signature} BODY-SHA256 {digest}', flush=True)
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, *args):
        pass


http.server.HTTPServer(('0.0.0.0', 80), Handler).serve_forever()
