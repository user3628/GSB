"""Bounded-concurrency HTTP sampling against the deterministic local service."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .errors import Conflict


class Sampler:
    def __init__(self, *, plan: dict[str, Any], stage: dict[str, Any], stream_id: str) -> None:
        self.plan = plan
        self.stage = stage
        self.stream_id = stream_id
        self._in_flight = 0
        self._peak_in_flight = 0
        self._lock = threading.Lock()

    def sample(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        sample_size = int(self.stage["sample_size"])
        concurrency = int(self.stage["concurrency"])
        workers = min(concurrency, sample_size)
        started = time.monotonic()
        results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sampler") as executor:
            futures = [executor.submit(self._one_request, index) for index in range(sample_size)]
            for future in futures:
                results.append(future.result())
        wall_seconds = time.monotonic() - started
        results.sort(key=lambda item: item["index"])
        diagnostics = {
            "requested_sample_size": sample_size,
            "returned_sample_count": len(results),
            "concurrency_limit": concurrency,
            "sampler_peak_in_flight": self._peak_in_flight,
            "wall_seconds": wall_seconds,
            "stream_id": self.stream_id,
        }
        return results, diagnostics

    def _one_request(self, index: int) -> dict[str, Any]:
        with self._lock:
            self._in_flight += 1
            self._peak_in_flight = max(self._peak_in_flight, self._in_flight)
            if self._in_flight > self.stage["concurrency"]:
                raise Conflict(
                    "sampler exceeded the configured concurrency limit",
                    details={
                        "concurrency_limit": self.stage["concurrency"],
                        "in_flight": self._in_flight,
                    },
                )
        try:
            return self._do_http(index)
        finally:
            with self._lock:
                self._in_flight -= 1

    def _do_http(self, index: int) -> dict[str, Any]:
        timeout_ms = int(self.stage["request_timeout_ms"])
        timeout_seconds = timeout_ms / 1000
        target = urllib.parse.urlsplit(self.plan["target"])
        distribution = self.stage["distribution"]
        query = urllib.parse.parse_qsl(target.query, keep_blank_values=True)
        query.extend(
            [
                ("seed", str(self.plan["seed"])),
                ("stream", self.stream_id),
                ("index", str(index)),
                ("concurrency", str(self.stage["concurrency"])),
                ("kind", distribution["kind"]),
                ("median_ms", distribution["median_ms"]),
                ("sigma", distribution["sigma"]),
                (
                    "overload_ref_concurrency",
                    str(distribution["overload_ref_concurrency"]),
                ),
                ("overload_power", distribution["overload_power"]),
                ("overload_slope", distribution["overload_slope"]),
                ("error_rate", self.stage["error_rate"]),
                ("timeout_rate", self.stage["timeout_rate"]),
                ("hang_ms", str(self.stage["hang_ms"])),
                ("sleep_ms", str(self.stage.get("service_sleep_ms", 0))),
            ]
        )
        path = target.path or "/load"
        path += "?" + urllib.parse.urlencode(query)
        started = time.monotonic()
        connection: http.client.HTTPConnection | None = None
        try:
            if target.scheme == "https":
                connection = http.client.HTTPSConnection(
                    target.hostname, target.port, timeout=timeout_seconds
                )
            else:
                connection = http.client.HTTPConnection(
                    target.hostname, target.port, timeout=timeout_seconds
                )
            connection.request(
                "GET",
                path,
                headers={"Accept": "application/json", "Connection": "close"},
            )
            response = connection.getresponse()
            body = response.read()
            wall_us = int((time.monotonic() - started) * 1_000_000)
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            latency_us = int(payload.get("latency_us", 0))
            ok = response.status == 200 and bool(payload.get("ok", True))
            return {
                "index": index,
                "latency_us": max(0, latency_us),
                "ok": ok,
                "error_kind": None if ok else _http_error_kind(payload),
                "http_status": response.status,
                "timed_out": bool(payload.get("timeout_injected", False)),
                "wall_us": wall_us,
                "error_code": payload.get("code"),
            }
        except (socket.timeout, TimeoutError):
            return {
                "index": index,
                "latency_us": timeout_ms * 1000,
                "ok": False,
                "error_kind": "timeout",
                "http_status": None,
                "timed_out": True,
                "wall_us": int((time.monotonic() - started) * 1_000_000),
                "error_code": "E_REQUEST_TIMEOUT",
            }
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            return {
                "index": index,
                "latency_us": 0,
                "ok": False,
                "error_kind": "network",
                "http_status": None,
                "timed_out": False,
                "wall_us": int((time.monotonic() - started) * 1_000_000),
                "error_code": type(exc).__name__,
            }
        finally:
            if connection is not None:
                connection.close()


def _http_error_kind(payload: dict[str, Any]) -> str:
    if payload.get("timeout_injected"):
        return "timeout"
    code = payload.get("code")
    if code == "E_INJECTED_ERROR":
        return "injected_error"
    return "http_error"

