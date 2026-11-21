from __future__ import annotations

import http.client
import json
import socket
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlsplit

import numpy as np

from .errors import BusinessError


LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
FAULT_MODES = {"mirror", "ok", "status", "hang"}


def _validate_target(url: str) -> tuple[str, int, str]:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or (parsed.hostname or "").lower() not in LOOPBACK_HOSTS:
        raise BusinessError(
            "E_SNAPSHOT_INVALID",
            "Replay target must be an http URL bound to a loopback host",
            {"target": url},
        )
    if parsed.query:
        path = f"{parsed.path}?{parsed.query}"
    else:
        path = parsed.path or "/"
    return parsed.hostname or "127.0.0.1", parsed.port or 80, path


def _quantiles(values: list[int]) -> dict[str, int | float]:
    if not values:
        return {"p50": 0, "p95": 0, "p99": 0, "mean": 0.0}
    array = np.asarray(values, dtype=np.int64)
    return {
        "p50": int(np.percentile(array, 50, method="nearest")),
        "p95": int(np.percentile(array, 95, method="nearest")),
        "p99": int(np.percentile(array, 99, method="nearest")),
        "mean": round(float(array.mean()), 6),
    }


def _send_operation(
    operation: dict[str, Any],
    *,
    host: str,
    port: int,
    path: str,
    timeout_ms: int,
    fault_mode: str,
    fault_status: int,
    hang_ms: int,
) -> dict[str, Any]:
    connection = http.client.HTTPConnection(host, port, timeout=max(0.001, timeout_ms / 1000.0))
    started = time.perf_counter_ns()
    timed_out = False
    error: str | None = None
    status_code: int | None = None
    response_bytes = 0
    try:
        body = json.dumps(
            {
                "operation": operation,
                "record": {
                    "timestamp_ms": operation["timestamp_ms"],
                    "connection_id": operation["connection_id"],
                    "source_service": operation["source_service"],
                    "destination_service": operation["destination_service"],
                    "method": operation["method"],
                    "path_template": operation["path_template"],
                    "status_code": operation["original_status_code"],
                    "response_bytes": operation["original_response_bytes"],
                    "duration_us": operation["original_duration_us"],
                },
            },
            ensure_ascii=False,
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Mirror-Fault-Mode": fault_mode,
            "X-Mirror-Fault-Status": str(fault_status),
            "X-Mirror-Hang-Ms": str(hang_ms),
        }
        connection.request("POST", path, body=body, headers=headers)
        response = connection.getresponse()
        status_code = response.status
        response_bytes = len(response.read())
    except (socket.timeout, TimeoutError) as exc:
        timed_out = True
        error = f"E_REPLAY_TIMEOUT: {type(exc).__name__}: {exc}"
    except (OSError, http.client.HTTPException) as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        # HTTPConnection.close() closes the underlying socket. This is deliberate:
        # a timed-out request must not be allowed to keep occupying the connection.
        connection.close()
    elapsed_us = (time.perf_counter_ns() - started) // 1000
    success = status_code is not None and status_code < 400 and error is None
    return {
        "operation_id": operation["operation_id"],
        "record_key": operation["record_key"],
        "copy_index": operation["copy_index"],
        "connection_id": operation["connection_id"],
        "status_code": status_code,
        "success": success,
        "timed_out": timed_out,
        "error": error,
        "elapsed_us": elapsed_us,
        "response_bytes": response_bytes,
    }


def execute_batches(
    batches: list[dict[str, Any]],
    *,
    target_url: str,
    timeout_ms: int,
    concurrency_limit: int,
    fault_mode: str = "mirror",
    fault_status: int = 200,
    hang_ms: int = 0,
) -> dict[str, Any]:
    if fault_mode not in FAULT_MODES:
        raise BusinessError("E_SNAPSHOT_INVALID", "Unsupported replay fault mode", {"fault_mode": fault_mode})
    if type(fault_status) is not int or not 100 <= fault_status <= 599:
        raise BusinessError("E_SNAPSHOT_INVALID", "fault_status must be between 100 and 599")
    if type(hang_ms) is not int or hang_ms < 0:
        raise BusinessError("E_SNAPSHOT_INVALID", "hang_ms must be a non-negative integer")
    host, port, path = _validate_target(target_url)
    started_at = time.time_ns()
    all_results: list[dict[str, Any]] = []
    batch_results: list[dict[str, Any]] = []
    for batch in batches:
        batch_started = time.perf_counter_ns()
        by_connection: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for operation in batch["operations"]:
            by_connection[operation["connection_id"]].append(operation)
        for operations in by_connection.values():
            operations.sort(
                key=lambda item: (
                    item["timestamp_ms"],
                    item["sampling_sequence"],
                    item["record_key"],
                    item["copy_index"],
                )
            )

        def run_connection(operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                _send_operation(
                    operation,
                    host=host,
                    port=port,
                    path=path,
                    timeout_ms=timeout_ms,
                    fault_mode=fault_mode,
                    fault_status=fault_status,
                    hang_ms=hang_ms,
                )
                for operation in operations
            ]

        workers = min(concurrency_limit, max(1, len(by_connection)))
        results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mirror-replay") as executor:
            futures = [executor.submit(run_connection, operations) for operations in by_connection.values()]
            for future in futures:
                results.extend(future.result())
        order = {operation["operation_id"]: index for index, operation in enumerate(batch["operations"])}
        results.sort(key=lambda item: order[item["operation_id"]])
        all_results.extend(results)
        batch_elapsed_us = (time.perf_counter_ns() - batch_started) // 1000
        batch_results.append(
            {
                "batch_id": batch["batch_id"],
                "sequence": batch["sequence"],
                "attempted": len(results),
                "success_count": sum(1 for result in results if result["success"]),
                "failure_count": sum(1 for result in results if not result["success"]),
                "timeout_count": sum(1 for result in results if result["timed_out"]),
                "elapsed_us": batch_elapsed_us,
                "connection_workers": workers,
                "results": results,
            }
        )

    statuses = Counter(str(result["status_code"]) for result in all_results if result["status_code"] is not None)
    attempts = len(all_results)
    successes = sum(1 for result in all_results if result["success"])
    timeouts = sum(1 for result in all_results if result["timed_out"])
    errors = sum(1 for result in all_results if result["error"] and not result["timed_out"])
    return {
        "status": "completed",
        "target_url": target_url,
        "fault_mode": fault_mode,
        "timeout_ms": timeout_ms,
        "started_at_ns": started_at,
        "elapsed_us": (time.time_ns() - started_at) // 1000,
        "attempted": attempts,
        "success_count": successes,
        "failure_count": attempts - successes,
        "timeout_count": timeouts,
        "transport_error_count": errors,
        "success_rate": round(successes / attempts, 6) if attempts else 0.0,
        "status_distribution": dict(sorted(statuses.items(), key=lambda item: int(item[0]))),
        "latency_us": _quantiles([result["elapsed_us"] for result in all_results]),
        "response_bytes_total": sum(result["response_bytes"] for result in all_results),
        "batch_results": batch_results,
        "results": all_results,
    }
