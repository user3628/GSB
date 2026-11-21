"""Stable business codes shared by the API, tests, and experiment report."""

from __future__ import annotations

from dataclasses import dataclass


OK = "OK"
E_BAD_REQUEST = "E_BAD_REQUEST"
E_NOT_FOUND = "E_NOT_FOUND"
E_PARTITION_INVALID = "E_PARTITION_INVALID"
E_TOPIC_CONFLICT = "E_TOPIC_CONFLICT"
E_IDEMPOTENCY_CONFLICT = "E_IDEMPOTENCY_CONFLICT"
E_LEASE_HELD = "E_LEASE_HELD"
E_LEASE_INVALID = "E_LEASE_INVALID"
E_LEASE_EXPIRED = "E_LEASE_EXPIRED"
E_DUPLICATE_ACK = "E_DUPLICATE_ACK"
E_SEQUENCE_MISMATCH = "E_SEQUENCE_MISMATCH"
E_TXN_NOT_FOUND = "E_TXN_NOT_FOUND"
E_TXN_FINALIZED = "E_TXN_FINALIZED"
E_TXN_TIMEOUT = "E_TXN_TIMEOUT"
E_TXN_UNREACHABLE = "E_TXN_UNREACHABLE"
E_TXN_UNKNOWN = "E_TXN_UNKNOWN"
E_TXN_CONFLICT = "E_TXN_CONFLICT"
E_TXN_REPLAY = "E_TXN_REPLAY"
E_FAULT_PRECONDITION = "E_FAULT_PRECONDITION"
E_INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    OK: 200,
    E_BAD_REQUEST: 400,
    E_NOT_FOUND: 404,
    E_PARTITION_INVALID: 400,
    E_TOPIC_CONFLICT: 409,
    E_IDEMPOTENCY_CONFLICT: 409,
    E_LEASE_HELD: 409,
    E_LEASE_INVALID: 409,
    E_LEASE_EXPIRED: 409,
    E_DUPLICATE_ACK: 409,
    E_SEQUENCE_MISMATCH: 409,
    E_TXN_NOT_FOUND: 404,
    E_TXN_FINALIZED: 409,
    E_TXN_TIMEOUT: 504,
    E_TXN_UNREACHABLE: 502,
    E_TXN_UNKNOWN: 409,
    E_TXN_CONFLICT: 409,
    E_TXN_REPLAY: 200,
    E_FAULT_PRECONDITION: 409,
    E_INTERNAL: 500,
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.code}: {self.message}"


def ok(data) -> dict:
    return {"ok": True, "code": OK, "data": data}


def fail(error: BusinessError) -> dict:
    return {
        "ok": False,
        "code": error.code,
        "message": error.message,
        "details": error.details or {},
    }

