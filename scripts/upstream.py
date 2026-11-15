"""Small observable upstream for the local demonstration, not a limiter."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class Handler(BaseHTTPRequestHandler):
    admissions = 0
    lock = threading.Lock()

    def do_GET(self):
        path = urlsplit(self.path).path
        with self.lock:
            Handler.admissions += 1
            number = Handler.admissions
        if path == "/slow":
            time.sleep(1)
        status = 502 if path == "/fail" else 200
        body = json.dumps({"path": path, "upstream_request": number}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            self.log_message("client disconnected after upstream execution")

    def log_message(self, message, *args):
        print(message % args, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
