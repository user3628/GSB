from __future__ import annotations

import socket
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import LabConfig, NodeConfig
from .errors import AppError, ErrorCode
from .models import (
    FAULT_CODES,
    RETRYABLE_CODES,
    FaultClass,
    GradientBlock,
    InjectionSpec,
    InjectionType,
    classify_code,
)


@dataclass(slots=True)
class FaultObservation:
    code: ErrorCode
    classification: FaultClass
    retryable: bool
    message: str
    elapsed_ms: float
    details: dict[str, Any] = field(default_factory=dict)
    submission_sequence: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def success(
        cls,
        *,
        message: str,
        elapsed_ms: float,
        details: dict[str, Any],
        submission_sequence: list[GradientBlock],
    ) -> "FaultObservation":
        return cls(
            code=ErrorCode.OK,
            classification=FaultClass.OK,
            retryable=False,
            message=message,
            elapsed_ms=elapsed_ms,
            details=details,
            submission_sequence=[block.to_payload() for block in submission_sequence],
        )

    @classmethod
    def failure(
        cls,
        *,
        code: ErrorCode,
        message: str,
        elapsed_ms: float,
        details: dict[str, Any],
        sequence: list[GradientBlock] | None = None,
    ) -> "FaultObservation":
        return cls(
            code=code,
            classification=classify_code(code),
            retryable=code in RETRYABLE_CODES,
            message=message,
            elapsed_ms=elapsed_ms,
            details=details,
            submission_sequence=[block.to_payload() for block in (sequence or [])],
        )

    def to_dict(self, *, include_sequence: bool = False) -> dict[str, Any]:
        result = {
            "code": self.code.value,
            "classification": self.classification.value,
            "retryable": self.retryable,
            "message": self.message,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "details": self.details,
            "submission_sequence": [
                {
                    key: value
                    for key, value in block.items()
                    if key != "tensor" or include_sequence
                }
                for block in self.submission_sequence
            ],
        }
        if include_sequence:
            result["submission_sequence"] = self.submission_sequence
        return result


def _fault_code(injection_type: InjectionType) -> ErrorCode:
    return FAULT_CODES[injection_type]


def _socket_http_get(host: str, port: int, path: str, timeout: float) -> tuple[bytes, str]:
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Connection: close\r\n"
        "Accept: application/json\r\n"
        "\r\n"
    ).encode("ascii")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.sendall(request)
        chunks: list[bytes] = []
        while True:
            try:
                chunk = sock.recv(65536)
            except ConnectionResetError:
                if chunks:
                    return b"".join(chunks), "connection_reset_after_partial_body"
                raise
            if not chunk:
                break
            chunks.append(chunk)
        response = b"".join(chunks)
    return response, "connection_closed"


def _try_connection_refused(node: NodeConfig, timeout: float) -> FaultObservation:
    started = time.perf_counter()
    code = _fault_code(InjectionType.CONNECTION_REFUSED)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(("127.0.0.1", node.refused_port))
    except ConnectionRefusedError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=code,
            message="The target refused a real TCP connection",
            elapsed_ms=elapsed,
            details={
                "operation": "tcp_connect",
                "target": f"127.0.0.1:{node.refused_port}",
                "errno": getattr(exc, "errno", None),
                "socket_closed": True,
            },
        )
    except OSError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=ErrorCode.INJECTOR_UNAVAILABLE,
            message="Unexpected socket failure during connection-refused injection",
            elapsed_ms=elapsed,
            details={"operation": "tcp_connect", "reason": str(exc), "socket_closed": True},
        )
    elapsed = (time.perf_counter() - started) * 1000
    return FaultObservation.failure(
        code=ErrorCode.INJECTOR_UNAVAILABLE,
        message="The configured refused port unexpectedly accepted a connection",
        elapsed_ms=elapsed,
        details={"target": f"127.0.0.1:{node.refused_port}", "socket_closed": True},
    )


def _try_timeout(node: NodeConfig, timeout: float) -> FaultObservation:
    started = time.perf_counter()
    code = _fault_code(InjectionType.RESPONSE_TIMEOUT)
    try:
        _socket_http_get("127.0.0.1", node.port, "/debug/hang", timeout)
    except (TimeoutError, socket.timeout) as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=code,
            message="The target did not respond before the read timeout",
            elapsed_ms=elapsed,
            details={
                "operation": "http_get_then_recv",
                "target": f"http://127.0.0.1:{node.port}/debug/hang",
                "timeout_seconds": timeout,
                "socket_closed": True,
                "exception": type(exc).__name__,
            },
        )
    except OSError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=ErrorCode.INJECTOR_UNAVAILABLE,
            message="Unexpected socket failure during timeout injection",
            elapsed_ms=elapsed,
            details={"operation": "http_get_then_recv", "reason": str(exc), "socket_closed": True},
        )
    elapsed = (time.perf_counter() - started) * 1000
    return FaultObservation.failure(
        code=ErrorCode.INJECTOR_UNAVAILABLE,
        message="The timeout target responded unexpectedly",
        elapsed_ms=elapsed,
        details={"target": f"http://127.0.0.1:{node.port}/debug/hang", "socket_closed": True},
    )


def _try_truncated(node: NodeConfig, timeout: float) -> FaultObservation:
    started = time.perf_counter()
    code = _fault_code(InjectionType.CONTENT_TRUNCATED)
    try:
        response, closure = _socket_http_get("127.0.0.1", node.port, "/debug/truncated", timeout)
    except (TimeoutError, socket.timeout):
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=ErrorCode.INJECTOR_UNAVAILABLE,
            message="The truncation endpoint timed out before a body arrived",
            elapsed_ms=elapsed,
            details={
                "target": f"http://127.0.0.1:{node.port}/debug/truncated",
                "socket_closed": True,
            },
        )
    except OSError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return FaultObservation.failure(
            code=ErrorCode.INJECTOR_UNAVAILABLE,
            message="Unexpected socket failure during truncation injection",
            elapsed_ms=elapsed,
            details={"reason": str(exc), "socket_closed": True},
        )

    header_blob, separator, body = response.partition(b"\r\n\r\n")
    headers: dict[str, str] = {}
    for line in header_blob.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
    declared = int(headers.get("content-length", "0") or 0)
    elapsed = (time.perf_counter() - started) * 1000
    if not separator or declared <= 0:
        return FaultObservation.failure(
            code=ErrorCode.INJECTOR_UNAVAILABLE,
            message="The truncation endpoint did not declare a positive Content-Length",
            elapsed_ms=elapsed,
            details={
                "declared_content_length": declared,
                "received_bytes": len(body),
                "socket_closed": True,
            },
        )
    if len(body) < declared:
        return FaultObservation.failure(
            code=code,
            message="The real HTTP response body was shorter than Content-Length",
            elapsed_ms=elapsed,
            details={
                "operation": "read_response_body",
                "target": f"http://127.0.0.1:{node.port}/debug/truncated",
                "declared_content_length": declared,
                "received_bytes": len(body),
                "missing_bytes": declared - len(body),
                "closure": closure,
                "socket_closed": True,
            },
        )
    return FaultObservation.failure(
        code=ErrorCode.INJECTOR_UNAVAILABLE,
        message="The truncation endpoint returned a complete response",
        elapsed_ms=elapsed,
        details={"declared_content_length": declared, "received_bytes": len(body)},
    )


def fetch_node_block(
    node: NodeConfig, round_no: int, shard_id: int, timeout: float
) -> GradientBlock:
    url = f"http://127.0.0.1:{node.port}/v1/blocks/{round_no}/{shard_id}"
    try:
        with httpx.Client(trust_env=False, timeout=timeout) as client:
            response = client.get(url)
            response.raise_for_status()
            return GradientBlock.from_payload(response.json())
    except AppError:
        raise
    except Exception as exc:
        raise AppError(
            ErrorCode.INJECTOR_UNAVAILABLE,
            "Normal node submission failed",
            {"node_id": node.node_id, "url": url, "reason": str(exc)},
        ) from exc


def _successful_submission(
    node: NodeConfig,
    round_no: int,
    block_count: int,
    timeout: float,
) -> FaultObservation:
    started = time.perf_counter()
    fetched = [
        fetch_node_block(node, round_no, shard_id, timeout)
        for shard_id in range(block_count)
    ]
    elapsed = (time.perf_counter() - started) * 1000
    return FaultObservation.success(
        message="All gradient blocks were submitted in shard order",
        elapsed_ms=elapsed,
        details={
            "operation": "http_block_submission",
            "target": f"http://127.0.0.1:{node.port}",
            "submitted_blocks": len(fetched),
            "socket_closed": True,
        },
        submission_sequence=fetched,
    )


def _sequence_failure(
    node: NodeConfig,
    round_no: int,
    block_count: int,
    injection_type: InjectionType,
    target_shard: int,
    timeout: float,
) -> FaultObservation:
    started = time.perf_counter()
    fetched = [
        fetch_node_block(node, round_no, shard_id, timeout)
        for shard_id in range(block_count)
    ]
    code = _fault_code(injection_type)
    if injection_type is InjectionType.DUPLICATE_SUBMISSION:
        duplicate = fetched[target_shard]
        sequence = [*fetched, duplicate]
        return FaultObservation.failure(
            code=code,
            message="The real submission sequence contains a duplicate block",
            elapsed_ms=(time.perf_counter() - started) * 1000,
            details={
                "operation": "http_block_submission",
                "target": f"http://127.0.0.1:{node.port}",
                "target_shard": target_shard,
                "sequence_length": len(sequence),
                "duplicate_occurrences": 2,
            },
            sequence=sequence,
        )

    if len(fetched) < 2:
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "Order mismatch requires at least two shards",
            {"target_node": node.node_id},
        )
    later = target_shard + 1 if target_shard + 1 < len(fetched) else target_shard - 1
    if later < target_shard:
        sequence = list(fetched)
        sequence[0], sequence[1] = sequence[1], sequence[0]
    else:
        sequence = list(fetched)
        sequence[target_shard], sequence[later] = sequence[later], sequence[target_shard]
    return FaultObservation.failure(
        code=code,
        message="The real submission sequence has shards out of order",
        elapsed_ms=(time.perf_counter() - started) * 1000,
        details={
            "operation": "http_block_submission",
            "target": f"http://127.0.0.1:{node.port}",
            "target_shard": target_shard,
            "observed_order": [block.shard_id for block in sequence],
            "expected_order": sorted(block.shard_id for block in sequence),
        },
        sequence=sequence,
    )


def perform_injection(
    config: LabConfig,
    spec: InjectionSpec,
    *,
    attempt_no: int,
    block_count: int,
) -> FaultObservation:
    node = config.node(spec.target_node)
    timeout = float(spec.params.get("timeout_seconds", config.socket_timeout_seconds))
    if not 0 < timeout <= 60:
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "params.timeout_seconds must be between 0 and 60",
            {"field": "params.timeout_seconds"},
        )

    if not spec.applies_on_attempt(attempt_no):
        return _successful_submission(
            node, spec.round, block_count, config.request_timeout_seconds
        )

    if spec.injection_type is InjectionType.CONNECTION_REFUSED:
        return _try_connection_refused(node, timeout)
    if spec.injection_type is InjectionType.RESPONSE_TIMEOUT:
        return _try_timeout(node, timeout)
    if spec.injection_type is InjectionType.CONTENT_TRUNCATED:
        return _try_truncated(node, timeout)
    if spec.injection_type in {InjectionType.DUPLICATE_SUBMISSION, InjectionType.ORDER_MISMATCH}:
        return _sequence_failure(
            node,
            spec.round,
            block_count,
            spec.injection_type,
            spec.target_shard,
            config.request_timeout_seconds,
        )
    raise AppError(
        ErrorCode.INJECTION_SPEC_INVALID,
        "Unsupported injection type",
        {"type": str(spec.injection_type)},
    )


def normal_submission(
    config: LabConfig,
    *,
    node_id: str,
    round_no: int,
) -> FaultObservation:
    node = config.node(node_id)
    return _successful_submission(
        node, round_no, config.shards_per_node, config.request_timeout_seconds
    )


def validate_injection_against_config(
    config: LabConfig, spec: InjectionSpec, round_no: int
) -> None:
    if spec.target_node not in config.node_ids:
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "Target node is not registered",
            {"target_node": spec.target_node, "registered_nodes": list(config.node_ids)},
        )
    if spec.round != round_no:
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "Injection round must equal the batch round",
            {"injection_round": spec.round, "batch_round": round_no},
        )
    if not 0 <= spec.target_shard < config.shards_per_node:
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "Target shard is outside the registered range",
            {"target_shard": spec.target_shard, "shards_per_node": config.shards_per_node},
        )
    if spec.injection_type in {InjectionType.DUPLICATE_SUBMISSION, InjectionType.ORDER_MISMATCH}:
        if config.shards_per_node < 2:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Sequence faults require at least two shards",
                {"shards_per_node": config.shards_per_node},
            )
    spec.applies_on_attempt(1)
    if "timeout_seconds" in spec.params:
        try:
            timeout = float(spec.params["timeout_seconds"])
        except (TypeError, ValueError) as exc:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "params.timeout_seconds must be numeric",
                {"field": "params.timeout_seconds"},
            ) from exc
        if not 0 < timeout <= 60:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "params.timeout_seconds must be between 0 and 60",
                {"field": "params.timeout_seconds"},
            )

