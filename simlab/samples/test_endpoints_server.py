"""Deterministic local HTTP test endpoints (loopback only).

Endpoints:
  /ok          -> immediate 200
  /delay?ms=N  -> 200 after N milliseconds (default 100)
  /hang        -> never responds (client must time out and close)
  /flaky       -> 200, but closes the connection abruptly afterwards

Connection-refused behaviour is exercised by pointing the plan at a port
where nothing listens (sample uses 127.0.0.1:19099).

Run:  python samples/test_endpoints_server.py [port]   (default 18081)
"""
from __future__ import annotations

import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, body: bytes, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/ok":
            self._send(b"ok")
        elif parsed.path == "/delay":
            ms = int(parse_qs(parsed.query).get("ms", ["100"])[0])
            time.sleep(ms / 1000.0)
            self._send(b"delayed")
        elif parsed.path == "/hang":
            time.sleep(3600)  # client is expected to give up and close
        elif parsed.path == "/flaky":
            self._send(b"flaky")
            self.connection.close()
        else:
            self._send(b"not found", 404)

    def log_message(self, *args):  # keep stdout deterministic
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18081
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"test endpoints listening on http://127.0.0.1:{port} "
          f"(/ok /delay?ms=N /hang /flaky)")
    server.serve_forever()
