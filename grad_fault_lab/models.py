from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Iterable

import numpy as np

from .errors import AppError, ErrorCode


class InjectionType(StrEnum):
    CONNECTION_REFUSED = "connection_refused"
    RESPONSE_TIMEOUT = "response_timeout"
    CONTENT_TRUNCATED = "content_truncated"
    DUPLICATE_SUBMISSION = "duplicate_submission"
    ORDER_MISMATCH = "order_mismatch"


class FaultClass(StrEnum):
    OK = "OK"
    RETRYABLE = "RETRYABLE"
    FATAL = "FATAL"


FAULT_CODES: dict[InjectionType, ErrorCode] = {
    InjectionType.CONNECTION_REFUSED: ErrorCode.FAULT_CONNECTION_REFUSED,
    InjectionType.RESPONSE_TIMEOUT: ErrorCode.FAULT_RESPONSE_TIMEOUT,
    InjectionType.CONTENT_TRUNCATED: ErrorCode.FAULT_CONTENT_TRUNCATED,
    InjectionType.DUPLICATE_SUBMISSION: ErrorCode.FAULT_DUPLICATE_SUBMISSION,
    InjectionType.ORDER_MISMATCH: ErrorCode.FAULT_ORDER_MISMATCH,
}

RETRYABLE_CODES = {
    ErrorCode.FAULT_CONNECTION_REFUSED,
    ErrorCode.FAULT_RESPONSE_TIMEOUT,
}

FATAL_CODES = {
    ErrorCode.BLOCK_VALIDATION_FAILED,
    ErrorCode.FAULT_CONTENT_TRUNCATED,
    ErrorCode.FAULT_DUPLICATE_SUBMISSION,
    ErrorCode.FAULT_ORDER_MISMATCH,
}


def classify_code(code: ErrorCode | str) -> FaultClass:
    value = ErrorCode(code)
    if value is ErrorCode.OK:
        return FaultClass.OK
    if value in RETRYABLE_CODES:
        return FaultClass.RETRYABLE
    if value in FATAL_CODES:
        return FaultClass.FATAL
    return FaultClass.FATAL


def is_retryable(code: ErrorCode | str) -> bool:
    return classify_code(code) is FaultClass.RETRYABLE


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    max_retries: int = 0
    backoff_seconds: float = 0.05
    backoff_multiplier: float = 2.0

    def backoff_for_retry(self, retry_number: int) -> float:
        return self.backoff_seconds * (self.backoff_multiplier ** (retry_number - 1))


@dataclass(slots=True, frozen=True)
class InjectionSpec:
    injection_type: InjectionType
    target_node: str
    target_shard: int
    round: int
    retry: RetryPolicy
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Any) -> "InjectionSpec":
        if not isinstance(payload, dict):
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Injection specification must be a JSON object",
                {"received_type": type(payload).__name__},
            )
        try:
            raw_type = payload["type"]
            injection_type = InjectionType(raw_type)
            target_node = str(payload["target_node"])
            target_shard = int(payload["target_shard"])
            injection_round = int(payload["round"])
            retry_payload = payload.get("retry", {})
            retry = RetryPolicy(
                max_retries=int(retry_payload.get("max_retries", 0)),
                backoff_seconds=float(retry_payload.get("backoff_seconds", 0.05)),
                backoff_multiplier=float(retry_payload.get("backoff_multiplier", 2.0)),
            )
            params = payload.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
        except (KeyError, TypeError, ValueError) as exc:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Injection specification could not be parsed",
                {"reason": str(exc)},
            ) from exc

        if not target_node:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Target node is required",
                {"field": "target_node"},
            )
        if target_shard < 0:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Target shard must be non-negative",
                {"field": "target_shard"},
            )
        if injection_round < 0:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Injection round must be non-negative",
                {"field": "round"},
            )
        if not 0 <= retry.max_retries <= 8:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "max_retries must be between 0 and 8",
                {"field": "retry.max_retries"},
            )
        if not 0 <= retry.backoff_seconds <= 5:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "backoff_seconds must be between 0 and 5",
                {"field": "retry.backoff_seconds"},
            )
        if not 1 <= retry.backoff_multiplier <= 10:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "backoff_multiplier must be between 1 and 10",
                {"field": "retry.backoff_multiplier"},
            )
        return cls(
            injection_type=injection_type,
            target_node=target_node,
            target_shard=target_shard,
            round=injection_round,
            retry=retry,
            params=dict(params),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.injection_type.value,
            "target_node": self.target_node,
            "target_shard": self.target_shard,
            "round": self.round,
            "retry": {
                "max_retries": self.retry.max_retries,
                "backoff_seconds": self.retry.backoff_seconds,
                "backoff_multiplier": self.retry.backoff_multiplier,
            },
            "params": self.params,
        }

    def applies_on_attempt(self, attempt_no: int) -> bool:
        configured = self.params.get("apply_attempts", [1])
        try:
            attempts = {int(value) for value in configured}
        except (TypeError, ValueError) as exc:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "params.apply_attempts must contain integers",
                {"field": "params.apply_attempts"},
            ) from exc
        return attempt_no in attempts


@dataclass(slots=True)
class GradientBlock:
    node_id: str
    round: int
    shard_id: int
    sample_weight: int
    tensor: np.ndarray

    def validate(self, *, shape: tuple[int, ...], max_shard: int) -> None:
        if self.round < 0:
            self._invalid("round must be non-negative")
        if not 0 <= self.shard_id <= max_shard:
            self._invalid("shard_id is outside the registered range")
        if self.sample_weight <= 0:
            self._invalid("sample_weight must be positive")
        if tuple(self.tensor.shape) != shape:
            self._invalid(f"tensor shape {self.tensor.shape} does not equal {shape}")
        if self.tensor.dtype.kind not in "fiu":
            self._invalid("tensor must be numeric")
        if not np.all(np.isfinite(self.tensor)):
            self._invalid("tensor contains NaN or infinity")

    def _invalid(self, reason: str) -> None:
        raise AppError(
            ErrorCode.BLOCK_VALIDATION_FAILED,
            "Gradient block validation failed",
            {
                "reason": reason,
                "node_id": self.node_id,
                "round": self.round,
                "shard_id": self.shard_id,
            },
        )

    def checksum(self) -> str:
        digest = hashlib.sha256()
        digest.update(self.node_id.encode())
        digest.update(b"\0")
        digest.update(str(self.round).encode())
        digest.update(b"\0")
        digest.update(str(self.shard_id).encode())
        digest.update(b"\0")
        digest.update(str(self.sample_weight).encode())
        digest.update(np.asarray(self.tensor, dtype="<f8").tobytes())
        return digest.hexdigest()

    def descriptor(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "round": self.round,
            "shard_id": self.shard_id,
            "sample_weight": self.sample_weight,
            "shape": list(self.tensor.shape),
            "checksum": self.checksum(),
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            **self.descriptor(),
            "tensor": np.asarray(self.tensor, dtype=float).tolist(),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "GradientBlock":
        try:
            tensor = np.asarray(payload["tensor"], dtype=np.float64)
            block = cls(
                node_id=str(payload["node_id"]),
                round=int(payload["round"]),
                shard_id=int(payload["shard_id"]),
                sample_weight=int(payload["sample_weight"]),
                tensor=tensor,
            )
            expected = payload.get("checksum")
            if expected is not None and expected != block.checksum():
                raise AppError(
                    ErrorCode.BLOCK_VALIDATION_FAILED,
                    "Gradient block checksum mismatch",
                    {"node_id": block.node_id, "shard_id": block.shard_id},
                )
            return block
        except KeyError as exc:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Gradient block is missing a required field",
                {"field": str(exc)},
            ) from exc
        except (TypeError, ValueError) as exc:
            raise AppError(
                ErrorCode.BLOCK_VALIDATION_FAILED,
                "Gradient block could not be parsed",
                {"reason": str(exc)},
            ) from exc


def blocks_by_node(blocks: Iterable[GradientBlock]) -> dict[str, list[GradientBlock]]:
    grouped: dict[str, list[GradientBlock]] = {}
    for block in blocks:
        grouped.setdefault(block.node_id, []).append(block)
    return grouped


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()

