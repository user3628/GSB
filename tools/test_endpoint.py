from __future__ import annotations

import argparse
import asyncio
import socket
import socketserver
import threading
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


app = FastAPI(title="Local-only failure-injection endpoint")
stats: dict[str, Any] = {
    "start_requests": 0,
    "execute_requests": 0,
    "hang_requests": 0,
    "hang_disconnects": 0,
    "active_requests": 0,
    "completed_requests": 0,
    "refused_connections": 0,
}
stats_lock = threading.Lock()


def _inc(key: str, amount: int = 1) -> None:
    with stats_lock:
        stats[key] = int(stats.get(key, 0)) + amount


def _bounded_duration(duration_us: int) -> float:
    return max(0.0, min(float(duration_us), 10_000_000.0)) / 1_000_000.0


@app.get("/health")
def health() -> dict[str, Any]:
    with stats_lock:
        return {"ok": True, "service": "local-test-endpoint", "stats": dict(stats)}


@app.get("/stats")
def get_stats() -> dict[str, Any]:
    with stats_lock:
        return dict(stats)


@app.post("/reset-stats")
def reset_stats() -> dict[str, Any]:
    with stats_lock:
        for key in stats:
            stats[key] = 0
        return dict(stats)


@app.get("/start")
async def start(duration_us: int = 0, instance_id: str = "", function_id: str = "") -> JSONResponse:
    _inc("start_requests")
    _inc("active_requests")
    started = time.monotonic()
    await asyncio.sleep(_bounded_duration(duration_us))
    _inc("completed_requests")
    _inc("active_requests", -1)
    return JSONResponse(
        {
            "ok": True,
            "mode": "success",
            "instance_id": instance_id,
            "function_id": function_id,
            "elapsed_us": int((time.monotonic() - started) * 1_000_000),
        }
    )


@app.get("/execute")
async def execute(duration_us: int = 0, call_id: str = "", instance_id: str = "") -> JSONResponse:
    _inc("execute_requests")
    _inc("active_requests")
    started = time.monotonic()
    await asyncio.sleep(_bounded_duration(duration_us))
    _inc("completed_requests")
    _inc("active_requests", -1)
    return JSONResponse(
        {
            "ok": True,
            "mode": "success",
            "call_id": call_id,
            "instance_id": instance_id,
            "elapsed_us": int((time.monotonic() - started) * 1_000_000),
        }
    )


@app.get("/hang")
async def hang(request: Request, duration_us: int = 10_000_000) -> JSONResponse:
    """Hold the response open, but stop as soon as the client closes the socket."""
    _inc("hang_requests")
    _inc("active_requests")
    started = time.monotonic()
    deadline = started + _bounded_duration(duration_us or 10_000_000)
    try:
        while time.monotonic() < deadline:
            if await request.is_disconnected():
                _inc("hang_disconnects")
                return JSONResponse({"ok": False, "mode": "client_disconnected"}, status_code=499)
            await asyncio.sleep(0.01)
        return JSONResponse({"ok": True, "mode": "hang_completed"}, status_code=200)
    finally:
        _inc("active_requests", -1)


class _RefusingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _RefusingHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        _inc("refused_connections")
        try:
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
        except OSError:
            pass
        self.request.close()


def start_refusing_server(host: str, port: int) -> _RefusingServer:
    server = _RefusingServer((host, port), _RefusingHandler)
    thread = threading.Thread(target=server.serve_forever, name="refusing-server", daemon=True)
    thread.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the loopback-only HTTP test endpoint")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--refuse-port", type=int, default=8766)
    args = parser.parse_args()
    start_refusing_server(args.host, args.refuse_port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

