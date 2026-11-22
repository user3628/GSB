from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator
from urllib.parse import parse_qs, urlsplit

import pytest


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ProbeHandler(BaseHTTPRequestHandler):
    broken_slow_responses = 0

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path == "/ok":
            body = b"OK"
            status = 200
        elif parsed.path == "/bad":
            body = b"DOWN"
            status = 503
        elif parsed.path == "/wrong":
            body = b"NOT_READY"
            status = 200
        elif parsed.path == "/slow":
            delay = int(parse_qs(parsed.query).get("delay_ms", ["700"])[0]) / 1000
            time.sleep(delay)
            body = b"OK"
            status = 200
        else:
            body = b"NOT_FOUND"
            status = 404
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            if parsed.path == "/slow":
                type(self).broken_slow_responses += 1


@pytest.fixture
def http_endpoint() -> Iterator[str]:
    ProbeHandler.broken_slow_responses = 0
    port = free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), ProbeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def tcp_endpoint() -> Iterator[str]:
    port = free_port()
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(16)
    stop = threading.Event()

    def accept_loop() -> None:
        listener.settimeout(0.2)
        while not stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with connection:
                try:
                    connection.settimeout(0.1)
                    connection.recv(32)
                except OSError:
                    pass

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    try:
        yield f"tcp://127.0.0.1:{port}"
    finally:
        stop.set()
        listener.close()
        thread.join(timeout=1)


@pytest.fixture
def refused_tcp_endpoint() -> str:
    return f"tcp://127.0.0.1:{free_port()}"
