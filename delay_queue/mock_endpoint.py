from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse


class EndpointState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.total_requests = 0
        self.flaky_attempts: dict[str, int] = {}
        self.requests: list[dict[str, Any]] = []

    def enter(self, record: dict[str, Any]) -> None:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.total_requests += 1
            self.requests.append(record)

    def leave(self) -> None:
        with self.lock:
            self.active -= 1

    def next_flaky(self, task_id: str) -> int:
        with self.lock:
            count = self.flaky_attempts.get(task_id, 0) + 1
            self.flaky_attempts[task_id] = count
            return count

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "active": self.active,
                "max_active": self.max_active,
                "total_requests": self.total_requests,
                "flaky_attempts": dict(self.flaky_attempts),
                "requests": list(self.requests),
            }

    def reset(self) -> None:
        with self.lock:
            self.active = 0
            self.max_active = 0
            self.total_requests = 0
            self.flaky_attempts.clear()
            self.requests.clear()


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DeterministicLocalEndpoint/1.0"

    @property
    def state(self) -> EndpointState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", False):  # type: ignore[attr-defined]
            super().log_message(fmt, *args)

    def _json(self, status: int, value: dict[str, Any]) -> None:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/healthz":
            self._json(200, {"ok": True})
            return
        if path == "/stats":
            self._json(200, self.state.snapshot())
            return
        if path == "/requests":
            snapshot = self.state.snapshot()
            self._json(200, {"requests": snapshot["requests"]})
            return
        self._json(404, {"ok": False, "code": "E_NOT_FOUND"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/reset":
            self.state.reset()
            self._json(200, {"ok": True})
            return
        if path != "/dispatch":
            self._json(404, {"ok": False, "code": "E_NOT_FOUND"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._json(400, {"ok": False, "code": "E_BAD_JSON"})
            return

        task_id = str(body.get("task_id", ""))
        inner = body.get("payload") if isinstance(body.get("payload"), dict) else {}
        mode = str(inner.get("mode", ""))
        if not mode:
            lower = task_id.lower()
            for candidate in ("hang", "fail", "flaky", "slow"):
                if candidate in lower:
                    mode = candidate
                    break
        if not mode:
            mode = "success"

        record = {
            "task_id": task_id,
            "queue": body.get("queue"),
            "round_id": body.get("round_id"),
            "attempt_no": body.get("attempt_no"),
            "mode": mode,
            "received_at_unix_ms": int(time.time() * 1_000),
        }
        self.state.enter(record)
        try:
            if mode == "hang":
                time.sleep(float(inner.get("hang_seconds", 30.0)))
            elif mode == "slow":
                time.sleep(float(inner.get("sleep_seconds", 0.15)))
            elif mode == "fail":
                self._json(
                    int(inner.get("status", 503)),
                    {"ok": False, "task_id": task_id, "mode": mode},
                )
                return
            elif mode == "flaky":
                attempt = self.state.next_flaky(task_id)
                if attempt == 1:
                    self._json(500, {"ok": False, "task_id": task_id, "attempt": attempt})
                    return
            elif mode == "status":
                status = int(inner.get("status", 500))
                self._json(status, {"ok": 200 <= status < 300, "task_id": task_id})
                return
            self._json(200, {"ok": True, "task_id": task_id, "mode": mode})
        finally:
            self.state.leave()


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def handle_error(self, request: Any, client_address: Any) -> None:
        return


def start_server(
    host: str = "127.0.0.1", port: int = 8765, verbose: bool = False
) -> tuple[ThreadingHTTPServer, threading.Thread]:
    server = QuietThreadingHTTPServer((host, port), MockHandler)
    server.state = EndpointState()  # type: ignore[attr-defined]
    server.verbose = verbose  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic local dispatch endpoint")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    server, thread = start_server(args.host, args.port, args.verbose)
    host, port = server.server_address[:2]
    print(f"mock endpoint listening on http://{host}:{port}/dispatch", flush=True)
    try:
        thread.join()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()

