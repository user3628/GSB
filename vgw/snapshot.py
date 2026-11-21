from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from .errors import GatewayError
from .models import (
    PatchDocument,
    SnapshotDocument,
    model_dump_canonical,
    parse_endpoint,
    validate_loopback_address,
)


@dataclass(frozen=True, slots=True)
class PatchApplication:
    snapshot: SnapshotDocument
    affected_sessions: tuple[str, ...]
    changed_endpoints: tuple[str, ...]
    changed_bindings: tuple[str, ...]


def canonical_json(data: Any) -> bytes:
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def compute_digest(document: SnapshotDocument | PatchDocument) -> str:
    body = model_dump_canonical(document, exclude_digest=True)
    return "sha256:" + hashlib.sha256(canonical_json(body)).hexdigest()


def validate_snapshot(raw: dict[str, Any], *, current_version: int | None = None) -> SnapshotDocument:
    try:
        snapshot = SnapshotDocument.model_validate(raw)
    except ValidationError as exc:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "snapshot model validation failed",
            {"errors": _validation_errors(exc)},
        ) from exc
    expected = compute_digest(snapshot)
    if snapshot.digest != expected:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "snapshot checksum mismatch",
            {"declared": snapshot.digest, "computed": expected},
        )
    if current_version is not None and snapshot.version <= current_version:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "snapshot version is not monotonically increasing",
            {"current_version": current_version, "incoming_version": snapshot.version},
        )
    _validate_endpoint_refs(snapshot)
    return snapshot


def validate_patch(raw: dict[str, Any]) -> PatchDocument:
    try:
        patch = PatchDocument.model_validate(raw)
    except ValidationError as exc:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "patch model validation failed",
            {"errors": _validation_errors(exc)},
        ) from exc
    expected = compute_digest(patch)
    if patch.digest != expected:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "patch checksum mismatch",
            {"declared": patch.digest, "computed": expected},
        )
    for name, endpoint in patch.egress_endpoints.items():
        try:
            validate_loopback_address(endpoint, f"egress_endpoints.{name}")
        except ValueError as exc:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                str(exc),
                {"endpoint": name},
            ) from exc
    return patch


def apply_patch(current: SnapshotDocument, patch: PatchDocument) -> PatchApplication:
    if patch.base_version != current.version:
        raise GatewayError(
            "E_STALE_BASE",
            "patch base version is not the current snapshot version",
            {"base_version": patch.base_version, "current_version": current.version},
        )
    old_data = model_dump_canonical(current, exclude_digest=True)
    data = copy.deepcopy(old_data)
    changed_endpoint_names = sorted(patch.egress_endpoints)
    data["egress_endpoints"].update(patch.egress_endpoints)
    changed_bindings = sorted(patch.bindings)
    for session, binding in patch.bindings.items():
        data["bindings"][session] = model_dump_canonical(binding)
    data["version"] = patch.version
    data["generated_at"] = patch.generated_at
    data.pop("digest", None)
    candidate_raw = dict(data)
    candidate_raw["digest"] = _digest_for_raw(candidate_raw)
    try:
        candidate = SnapshotDocument.model_validate(candidate_raw)
    except ValidationError as exc:
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "patched snapshot validation failed",
            {"errors": _validation_errors(exc)},
        ) from exc
    if candidate.digest != compute_digest(candidate):
        raise GatewayError(
            "E_SNAPSHOT_INVALID",
            "patched snapshot checksum mismatch",
            {},
        )
    affected = set(changed_bindings)
    for session, binding in candidate.bindings.items():
        if binding.egress in changed_endpoint_names:
            affected.add(session)
    for session in changed_bindings:
        if session not in candidate.bindings:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                "patch references an unknown or removed session",
                {"session_id": session},
            )
    return PatchApplication(
        snapshot=candidate,
        affected_sessions=tuple(sorted(affected)),
        changed_endpoints=tuple(changed_endpoint_names),
        changed_bindings=tuple(changed_bindings),
    )


def binding_endpoint(snapshot: SnapshotDocument, session_id: str) -> tuple[str, int, str]:
    binding = snapshot.bindings.get(session_id)
    if binding is None:
        raise GatewayError(
            "E_FRAME_INVALID",
            "session is not present in the active snapshot",
            {"session_id": session_id, "snapshot_version": snapshot.version},
        )
    address = snapshot.egress_endpoints[binding.egress]
    host, port = parse_endpoint(address)
    return host, port, address


def _digest_for_raw(raw: dict[str, Any]) -> str:
    body = copy.deepcopy(raw)
    body.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json(body)).hexdigest()


def _validate_endpoint_refs(snapshot: SnapshotDocument) -> None:
    for name, address in snapshot.egress_endpoints.items():
        try:
            parse_endpoint(address)
        except ValueError as exc:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                str(exc),
                {"endpoint": name, "address": address},
            ) from exc


def _validation_errors(exc: ValidationError) -> list[dict[str, Any]]:
    return [
        {
            "loc": list(item.get("loc", ())),
            "type": item.get("type"),
            "msg": item.get("msg"),
        }
        for item in exc.errors(include_url=False)
    ]

