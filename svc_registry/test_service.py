from __future__ import annotations

import argparse
import asyncio
import socketserver
import threading
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse


app = FastAPI(title="Local probe target", version="1.0.0")


@app.get("/health/ok", response_class=PlainTextResponse)
async def health_ok() -> str:
    return "OK"


@app.get("/health/bad", response_class=PlainTextResponse)
async def health_bad() -> PlainTextResponse:
    return PlainTextResponse("DOWN", status_code=503)


@app.get("/health/wrong-marker", response_class=PlainTextResponse)
async def wrong_marker() -> str:
    return "NOT_READY"


@app.get("/health/slow", response_class=PlainTextResponse)
async def health_slow(delay_ms: int = Query(default=1500, ge=1, le=30_000)) -> str:
    await asyncio.sleep(delay_ms / 1000)
    return "OK"


@app.get("/health/echo", response_class=PlainTextResponse)
async def health_echo(marker: str = "OK") -> str:
    return marker


class _TcpHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:  # pragma: no cover - exercised through real sockets
        try:
            self.request.settimeout(0.2)
            try:
                self.request.recv(64)
            except OSError:
                pass
        finally:
            self.request.close()


class _ThreadingTcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def start_tcp_server(host: str, port: int) -> _ThreadingTcpServer:
    server = _ThreadingTcpServer((host, port), _TcpHandler)
    thread = threading.Thread(target=server.serve_forever, name="probe-tcp-server", daemon=True)
    thread.start()
    return server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local deterministic HTTP/TCP probe target")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=28765)
    parser.add_argument("--tcp-port", type=int, default=28766)
    args = parser.parse_args(argv)
    tcp_server = start_tcp_server(args.host, args.tcp_port)
    try:
        import uvicorn

        uvicorn.run(app, host=args.host, port=args.http_port, log_level="warning")
    finally:
        tcp_server.shutdown()
        tcp_server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
