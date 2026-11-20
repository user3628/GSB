"""Deterministic helpers: SHA-256, timestamps, id generation."""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_now_iso() -> str:
    # Microsecond precision, explicit UTC; lexicographic order matches time order.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_session_id() -> str:
    return "sess-" + uuid.uuid4().hex


def new_intent_id() -> str:
    return "intent-" + uuid.uuid4().hex


def object_etag(object_digest: str, part_count: int) -> str:
    # ETag semantics: digest-derived identifier with multipart marker suffix.
    return f'"{object_digest[:16]}-{part_count}"'
