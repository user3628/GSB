"""Deterministic local latency/error service used only for validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import numpy as np


class ServiceStats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.total_requests = 0
        self.injected_errors = 0
        self.injected_timeouts = 0
        self.by_stream: dict[str, dict[str, int]] = {}

    def begin(self, stream: str) -> None:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.total_requests += 1
            stream_stats = self.by_stream.setdefault(
                stream,
                {"active": 0, "max_active": 0, "total_requests": 0, "last_concurrency": 0},
            )
            stream_stats["active"] += 1
            stream_stats["max_active"] = max(stream_stats["max_active"], stream_stats["active"])
            stream_stats["total_requests"] += 1

    def end(self, stream: str, *, error: bool, timeout: bool, concurrency: int) -> None:
        with self.lock:
            self.active -= 1
            if error:
                self.injected_errors += 1
            if timeout:
                self.injected_timeouts += 1
            stream_stats = self.by_stream.setdefault(
                stream,
                {"active": 0, "max_active": 0, "total_requests": 0, "last_concurrency": 0},
            )
            stream_stats["active"] -= 1
            stream_stats["last_concurrency"] = concurrency

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "active": self.active,
                "max_active": self.max_active,
                "total_requests": self.total_requests,
                "injected_errors": self.injected_errors,
                "injected_timeouts": self.injected_timeouts,
                "by_stream": json.loads(json.dumps(self.by_stream)),
            }

    def reset(self) -> None:
        with self.lock:
            self.active = 0
            self.max_active = 0
            self.total_requests = 0
            self.injected_errors = 0
            self.injected_timeouts = 0
            self.by_stream.clear()


class LoadHandler(BaseHTTPRequestHandler):
    server_version = "DeterministicLoad/1.0"
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json(200, {"ok": True, "service": "deterministic-load", "time": time.time()})
            return
        if parsed.path == "/stats":
            self._json(200, {"ok": True, "stats": self._stats().snapshot()})
            return
        if parsed.path == "/reset":
            self._stats().reset()
            self._json(200, {"ok": True, "stats": self._stats().snapshot()})
            return
        if parsed.path != "/load":
            self._json(
                404,
                {"ok": False, "error": {"code": "E_NOT_FOUND", "message": "unknown path"}},
            )
            return
        self._handle_load(parse_qs(parsed.query, keep_blank_values=True))

    def _handle_load(self, query: dict[str, list[str]]) -> None:
        try:
            params = _parse_load_params(query)
        except ValueError as exc:
            self._json(
                400,
                {"ok": False, "error": {"code": "E_BAD_REQUEST", "message": str(exc)}},
            )
            return

        stream = params["stream"]
        stats = self._stats()
        stats.begin(stream)
        error_injected = False
        timeout_injected = False
        response: dict[str, Any] | None = None
        try:
            latency_us, error_injected, timeout_injected = _generate_sample(params)
            if timeout_injected:
                time.sleep(params["hang_ms"] / 1000)
            elif params["sleep_ms"] > 0:
                time.sleep(params["sleep_ms"] / 1000)
            response = {
                "ok": not error_injected,
                "code": "E_INJECTED_ERROR" if error_injected else None,
                "stream": stream,
                "index": params["index"],
                "concurrency": params["concurrency"],
                "latency_us": latency_us,
                "error_injected": error_injected,
                "timeout_injected": timeout_injected,
                "seed": params["seed"],
            }
        finally:
            stats.end(
                stream,
                error=error_injected,
                timeout=timeout_injected,
                concurrency=params["concurrency"],
            )
        if response is None:
            self.close_connection = True
            return
        try:
            self._json(503 if error_injected else 200, response)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.close_connection = True

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _stats(self) -> ServiceStats:
        return getattr(self.server, "stats")

    def log_message(self, fmt: str, *args: Any) -> None:
        return


class DeterministicServiceServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 512

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, LoadHandler)
        self.stats = ServiceStats()


def _parse_load_params(query: dict[str, list[str]]) -> dict[str, Any]:
    def one(name: str, default: str | None = None) -> str:
        values = query.get(name)
        if values:
            return values[-1]
        if default is None:
            raise ValueError(f"missing query parameter: {name}")
        return default

    try:
        seed = int(one("seed"))
        index = int(one("index"))
        concurrency = int(one("concurrency"))
        overload_ref = int(one("overload_ref_concurrency", "1"))
        hang_ms = int(one("hang_ms", "1500"))
        sleep_ms = int(one("sleep_ms", "0"))
        kind = one("kind", "lognormal")
        median_ms = float(one("median_ms", "10"))
        sigma = float(one("sigma", "0.25"))
        overload_power = float(one("overload_power", "1.0"))
        overload_slope = float(one("overload_slope", "0"))
        error_rate = float(one("error_rate", "0"))
        timeout_rate = float(one("timeout_rate", "0"))
    except (TypeError, ValueError) as exc:
        raise ValueError("load parameters must be numeric") from exc
    if seed < 0 or index < 0 or concurrency <= 0 or overload_ref <= 0:
        raise ValueError("seed/index must be non-negative and concurrency/ref positive")
    if kind not in {"lognormal", "normal"}:
        raise ValueError("kind must be lognormal or normal")
    if median_ms <= 0 or sigma < 0 or overload_power < 0 or overload_slope < 0:
        raise ValueError("distribution parameters are out of range")
    if not 0 <= error_rate <= 1 or not 0 <= timeout_rate <= 1:
        raise ValueError("error_rate and timeout_rate must be in [0,1]")
    if hang_ms < 0 or sleep_ms < 0:
        raise ValueError("hang_ms and sleep_ms must be non-negative")
    return {
        "seed": seed,
        "stream": one("stream", "default"),
        "index": index,
        "concurrency": concurrency,
        "kind": kind,
        "median_ms": median_ms,
        "sigma": sigma,
        "overload_ref_concurrency": overload_ref,
        "overload_power": overload_power,
        "overload_slope": overload_slope,
        "error_rate": error_rate,
        "timeout_rate": timeout_rate,
        "hang_ms": hang_ms,
        "sleep_ms": sleep_ms,
    }


def _generate_sample(params: dict[str, Any]) -> tuple[int, bool, bool]:
    digest = hashlib.sha256(
        f"{params['seed']}|{params['stream']}|{params['index']}".encode("utf-8")
    ).digest()
    derived_seed = int.from_bytes(digest[:16], "big")
    rng = np.random.Generator(np.random.PCG64(derived_seed))
    z = float(rng.standard_normal())
    if params["kind"] == "lognormal":
        base_ms = params["median_ms"] * math.exp(params["sigma"] * z)
    else:
        base_ms = max(0.0, params["median_ms"] + params["sigma"] * z)
    ratio = params["concurrency"] / params["overload_ref_concurrency"]
    overload_factor = max(1.0, ratio) ** params["overload_power"]
    knee_factor = 1.0 + max(0.0, ratio - 1.0) * params["overload_slope"]
    latency_us = int(base_ms * overload_factor * knee_factor * 1000.0 + 0.5)
    error_injected = bool(rng.random() < params["error_rate"])
    timeout_injected = bool(rng.random() < params["timeout_rate"])
    return latency_us, error_injected, timeout_injected


def start_server(host: str = "127.0.0.1", port: int = 0) -> tuple[DeterministicServiceServer, threading.Thread]:
    server = DeterministicServiceServer((host, port))
    thread = threading.Thread(target=server.serve_forever, name="deterministic-service", daemon=True)
    thread.start()
    return server, thread


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic local load-test target")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    server = DeterministicServiceServer((args.host, args.port))
    print(f"deterministic service listening on http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

