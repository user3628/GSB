from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Iterable

from .errors import BusinessError, spec_invalid
from .models import Application, Cluster, MAX_SQLITE_INT, SnapshotSpec


_CHECKSUM_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_ID_MAX_LEN = 256
_VERSION_MAX_LEN = 256


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise spec_invalid(f"{name} must be a JSON object", field=name)
    return value


def _require_list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise spec_invalid(f"{name} must be a JSON array", field=name)
    return value


def _require_text(value: Any, name: str, *, max_len: int = _ID_MAX_LEN) -> str:
    if not isinstance(value, str) or not value.strip():
        raise spec_invalid(f"{name} must be a non-empty string", field=name)
    if len(value) > max_len:
        raise spec_invalid(f"{name} is too long", field=name, max_length=max_len)
    return value


def _require_int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise spec_invalid(f"{name} must be an integer", field=name)
    if value < minimum:
        raise spec_invalid(f"{name} must be >= {minimum}", field=name, value=value)
    if value > MAX_SQLITE_INT:
        raise spec_invalid(
            f"{name} overflows the SQLite 64-bit integer range",
            field=name,
            value=value,
            max_value=MAX_SQLITE_INT,
        )
    return value


def _parse_timestamp(value: Any) -> str:
    text = _require_text(value, "generated_at", max_len=128)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise spec_invalid(
            "generated_at must be an ISO-8601 timestamp", field="generated_at", value=text
        ) from exc
    return text


def canonical_checksum_payload(
    version: int,
    generated_at: str,
    clusters: Iterable[Cluster],
    applications: Iterable[Application],
) -> dict[str, Any]:
    return {
        "version": version,
        "generated_at": generated_at,
        "clusters": [
            {
                "cluster_id": cluster.cluster_id,
                "replica_quota": cluster.replica_quota,
            }
            for cluster in sorted(clusters, key=lambda item: item.cluster_id)
        ],
        "applications": [
            app.to_dict()
            for app in sorted(applications, key=lambda item: item.app_id)
        ],
    }


def compute_checksum(
    version: int,
    generated_at: str,
    clusters: Iterable[Cluster],
    applications: Iterable[Application],
) -> str:
    payload = canonical_checksum_payload(version, generated_at, clusters, applications)
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_checksum(value: Any, expected: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not _CHECKSUM_RE.fullmatch(value):
        raise spec_invalid("checksum must be a 64-character SHA-256 hex string")
    if value.lower() != expected:
        raise spec_invalid(
            "snapshot checksum does not match its canonical content",
            expected=expected,
            actual=value.lower(),
        )


def parse_snapshot_document(payload: dict[str, Any], *, base_version: int | None = None) -> SnapshotSpec:
    document = _require_mapping(payload, "snapshot")
    version = _require_int(document.get("version"), "version", minimum=1)
    generated_at = _parse_timestamp(document.get("generated_at"))

    cluster_items = _require_list(document.get("clusters"), "clusters")
    if not cluster_items:
        raise spec_invalid("snapshot must contain at least one cluster")
    clusters: list[Cluster] = []
    cluster_ids: set[str] = set()
    for index, raw_cluster in enumerate(cluster_items):
        item = _require_mapping(raw_cluster, f"clusters[{index}]")
        cluster_id = _require_text(item.get("cluster_id"), f"clusters[{index}].cluster_id")
        if cluster_id in cluster_ids:
            raise spec_invalid("cluster identifiers must be unique", cluster_id=cluster_id)
        cluster_ids.add(cluster_id)
        replica_quota = _require_int(
            item.get("replica_quota"), f"clusters[{index}].replica_quota", minimum=1
        )
        clusters.append(Cluster(cluster_id=cluster_id, replica_quota=replica_quota))

    application_items = _require_list(document.get("applications"), "applications")
    applications: list[Application] = []
    app_ids: set[str] = set()
    for index, raw_app in enumerate(application_items):
        item = _require_mapping(raw_app, f"applications[{index}]")
        app_id = _require_text(item.get("app_id"), f"applications[{index}].app_id")
        if app_id in app_ids:
            raise spec_invalid("application identifiers must be unique", app_id=app_id)
        app_ids.add(app_id)
        cluster_id = _require_text(item.get("cluster_id"), f"applications[{index}].cluster_id")
        if cluster_id not in cluster_ids:
            raise spec_invalid(
                "application references an unknown cluster",
                app_id=app_id,
                cluster_id=cluster_id,
            )
        desired_version = _require_text(
            item.get("desired_version"), f"applications[{index}].desired_version",
            max_len=_VERSION_MAX_LEN,
        )
        effective_version = _require_text(
            item.get("effective_version"),
            f"applications[{index}].effective_version",
            max_len=_VERSION_MAX_LEN,
        )
        replica_budget = _require_int(
            item.get("replica_budget"), f"applications[{index}].replica_budget", minimum=0
        )
        raw_dependencies = _require_list(
            item.get("dependencies", []), f"applications[{index}].dependencies"
        )
        dependencies: list[str] = []
        dependency_ids: set[str] = set()
        for dep_index, raw_dep in enumerate(raw_dependencies):
            dependency = _require_text(
                raw_dep, f"applications[{index}].dependencies[{dep_index}]"
            )
            if dependency in dependency_ids:
                raise spec_invalid(
                    "dependency identifiers must be unique per application",
                    app_id=app_id,
                    dependency=dependency,
                )
            dependency_ids.add(dependency)
            dependencies.append(dependency)
        applications.append(
            Application(
                app_id=app_id,
                cluster_id=cluster_id,
                desired_version=desired_version,
                effective_version=effective_version,
                replica_budget=replica_budget,
                dependencies=tuple(sorted(dependencies)),
            )
        )

    for app in applications:
        for dependency in app.dependencies:
            if dependency not in app_ids:
                raise spec_invalid(
                    "application references a missing dependency",
                    app_id=app.app_id,
                    dependency=dependency,
                )

    # Detect aggregate quota overflow early rather than losing precision in SQLite.
    quota_by_cluster = {cluster.cluster_id: cluster.replica_quota for cluster in clusters}
    total_by_cluster: dict[str, int] = {}
    for app in applications:
        total_by_cluster[app.cluster_id] = (
            total_by_cluster.get(app.cluster_id, 0) + app.replica_budget
        )
        if total_by_cluster[app.cluster_id] > MAX_SQLITE_INT:
            raise spec_invalid(
                "aggregate replica budget overflows the 64-bit integer range",
                cluster_id=app.cluster_id,
            )

    checksum = compute_checksum(version, generated_at, clusters, applications)
    _validate_checksum(document.get("checksum"), checksum)
    return SnapshotSpec(
        version=version,
        generated_at=generated_at,
        clusters=tuple(sorted(clusters, key=lambda item: item.cluster_id)),
        applications=tuple(sorted(applications, key=lambda item: item.app_id)),
        checksum=checksum,
        base_version=base_version,
    )


def parse_increment_document(
    payload: dict[str, Any], *, path_base_version: int
) -> dict[str, Any]:
    document = _require_mapping(payload, "increment")
    base_version = _require_int(document.get("base_version"), "base_version", minimum=1)
    if base_version != path_base_version:
        raise BusinessError(
            "E_STALE_BASE",
            "increment base_version does not match the request path",
            {"base_version": base_version, "path_base_version": path_base_version},
        )
    version = _require_int(document.get("version"), "version", minimum=1)
    if version <= base_version:
        raise spec_invalid(
            "increment version must be greater than base_version",
            version=version,
            base_version=base_version,
        )
    generated_at = _parse_timestamp(document.get("generated_at"))
    changes = _require_list(document.get("changes"), "changes")
    if not changes:
        raise spec_invalid("increment must contain at least one local change")

    parsed_changes: list[dict[str, Any]] = []
    seen: set[str] = set()
    allowed_fields = {
        "app_id",
        "desired_version",
        "effective_version",
        "replica_budget",
        "dependencies",
        "cluster_id",
        "rollback",
    }
    for index, raw_change in enumerate(changes):
        item = _require_mapping(raw_change, f"changes[{index}]")
        unknown = set(item) - allowed_fields
        if unknown:
            raise spec_invalid(
                "increment change contains unsupported fields",
                app_id=item.get("app_id"),
                unknown_fields=sorted(unknown),
            )
        app_id = _require_text(item.get("app_id"), f"changes[{index}].app_id")
        if app_id in seen:
            raise spec_invalid("an application may appear only once per increment", app_id=app_id)
        seen.add(app_id)
        change: dict[str, Any] = {"app_id": app_id}
        if "desired_version" in item:
            change["desired_version"] = _require_text(
                item["desired_version"], f"changes[{index}].desired_version",
                max_len=_VERSION_MAX_LEN,
            )
        if "effective_version" in item:
            change["effective_version"] = _require_text(
                item["effective_version"], f"changes[{index}].effective_version",
                max_len=_VERSION_MAX_LEN,
            )
        if "replica_budget" in item:
            change["replica_budget"] = _require_int(
                item["replica_budget"], f"changes[{index}].replica_budget", minimum=0
            )
        if "dependencies" in item:
            deps = _require_list(item["dependencies"], f"changes[{index}].dependencies")
            parsed_deps = [
                _require_text(dep, f"changes[{index}].dependencies[{dep_index}]")
                for dep_index, dep in enumerate(deps)
            ]
            if len(parsed_deps) != len(set(parsed_deps)):
                raise spec_invalid(
                    "dependency identifiers must be unique per application", app_id=app_id
                )
            change["dependencies"] = sorted(parsed_deps)
        if "cluster_id" in item:
            change["cluster_id"] = _require_text(
                item["cluster_id"], f"changes[{index}].cluster_id"
            )
        if "rollback" in item:
            if not isinstance(item["rollback"], bool):
                raise spec_invalid(
                    "rollback must be a boolean", app_id=app_id, field="rollback"
                )
            change["rollback"] = item["rollback"]
        parsed_changes.append(change)

    if "checksum" in document:
        value = document["checksum"]
        if not isinstance(value, str) or not _CHECKSUM_RE.fullmatch(value):
            raise spec_invalid("checksum must be a 64-character SHA-256 hex string")
    if "base_checksum" in document:
        value = document["base_checksum"]
        if not isinstance(value, str) or not _CHECKSUM_RE.fullmatch(value):
            raise spec_invalid("base_checksum must be a 64-character SHA-256 hex string")
    return {
        "base_version": base_version,
        "version": version,
        "generated_at": generated_at,
        "changes": parsed_changes,
        "checksum": document.get("checksum"),
        "base_checksum": document.get("base_checksum"),
    }


def validate_checksum_value(value: str | None, expected: str) -> None:
    _validate_checksum(value, expected)


def application_from_storage(
    app_id: str,
    cluster_id: str,
    desired_version: str,
    effective_version: str,
    replica_budget: int,
    dependencies: list[str] | tuple[str, ...],
) -> Application:
    return Application(
        app_id=app_id,
        cluster_id=cluster_id,
        desired_version=desired_version,
        effective_version=effective_version,
        replica_budget=replica_budget,
        dependencies=tuple(sorted(dependencies)),
    )
