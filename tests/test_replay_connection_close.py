from __future__ import annotations

import socket
import threading

from mirror_judge.replay import execute_batches


def test_timeout_closes_the_underlying_socket():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()
    closed_after_timeout = threading.Event()
    received_request = threading.Event()

    def hanging_endpoint() -> None:
        connection, _ = listener.accept()
        connection.settimeout(3.0)
        try:
            chunk = connection.recv(4096)
            if chunk:
                received_request.set()
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    closed_after_timeout.set()
                    break
        except (socket.timeout, TimeoutError):
            pass
        finally:
            connection.close()
            listener.close()

    thread = threading.Thread(target=hanging_endpoint, daemon=True)
    thread.start()
    operation = {
        "operation_id": "b-test:op:0001",
        "record_key": "batch:1",
        "sampling_sequence": "stable-sequence",
        "copy_index": 1,
        "connection_id": "conn-timeout",
        "timestamp_ms": 1764000000000,
        "source_service": "edge-gateway",
        "destination_service": "catalog-service",
        "method": "GET",
        "path_template": "/api/catalog/items/{item_id}",
        "original_status_code": 200,
        "original_response_bytes": 128,
        "original_duration_us": 5000,
    }
    batch = {"batch_id": "b-test", "sequence": 1, "operations": [operation]}
    result = execute_batches(
        [batch],
        target_url=f"http://127.0.0.1:{port}/_test/replay",
        timeout_ms=100,
        concurrency_limit=1,
    )
    assert result["timeout_count"] == 1
    assert result["failure_count"] == 1
    assert result["results"][0]["timed_out"] is True
    assert received_request.wait(timeout=2.0)
    assert closed_after_timeout.wait(timeout=2.0), "timed-out client did not close the underlying socket"
    thread.join(timeout=2.0)
