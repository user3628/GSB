from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any

from .db import Database, InstanceRow, canonical_json, now_ms
from .errors import E_CONFLICTING_BATCH, E_DB_ERROR, E_FORMAT_CORRUPT, BusinessError
from .faults import maybe_crash
from .formats import decode_snapshot, detect_format, decide_compatibility
from .models import NormalizedProbeResult, NormalizedRegistration, Snapshot
from .normalization import NormalizationBatch, normalize_probe_results, normalize_registrations
from .policies import TimePolicy, WeightPolicy


OUTCOME_LABELS = {"override": "覆盖", "skip": "跳过", "reject": "拒绝", "append": "追加"}


@dataclass(frozen=True)
class ImportInput:
    batch_id: str
    data: bytes | str | list[bytes | str]
    expected_format: str = "auto"
    expected_kind: str | None = None
    weight_policy: WeightPolicy = WeightPolicy()
    time_policy: TimePolicy = TimePolicy()


def _hash_payload(data: bytes | str | list[bytes | str]) -> str:
    digest = hashlib.sha256()
    shards = data if isinstance(data, list) else [data]
    for shard in shards:
        encoded = shard.encode("utf-8") if isinstance(shard, str) else shard
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _content_hash(
    data: bytes | str | list[bytes | str],
    *,
    expected_format: str,
    expected_kind: str | None,
    weight_policy: WeightPolicy,
    time_policy: TimePolicy,
) -> str:
    material = canonical_json(
        {
            "payload_sha256": _hash_payload(data),
            "expected_format": expected_format,
            "expected_kind": expected_kind,
            "weight_policy": weight_policy.to_dict(),
            "time_policy": time_policy.to_dict(),
        }
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _same_registration(left: NormalizedRegistration, right: NormalizedRegistration) -> bool:
    fields = (
        "endpoint",
        "weight_permille",
        "lease_ttl_ms",
        "probe_method",
        "probe_window_ms",
    )
    return all(getattr(left, field) == getattr(right, field) for field in fields) and canonical_json(
        left.probe_params
    ) == canonical_json(right.probe_params)


def resolve_conflict(
    incoming: NormalizedRegistration,
    existing: NormalizedRegistration,
) -> tuple[str, str, dict[str, Any]]:
    if incoming.lease_version > existing.lease_version:
        return (
            "override",
            "lease_version_higher",
            {"incoming_lease_version": incoming.lease_version, "existing_lease_version": existing.lease_version},
        )
    if incoming.lease_version < existing.lease_version:
        return (
            "skip",
            "lease_version_lower",
            {"incoming_lease_version": incoming.lease_version, "existing_lease_version": existing.lease_version},
        )
    if incoming.registered_at_ms > existing.registered_at_ms:
        return (
            "override",
            "last_registered_at_newer",
            {"incoming_last_registered_at_ms": incoming.registered_at_ms, "existing_last_registered_at_ms": existing.registered_at_ms},
        )
    if incoming.registered_at_ms < existing.registered_at_ms:
        return (
            "skip",
            "last_registered_at_older",
            {"incoming_last_registered_at_ms": incoming.registered_at_ms, "existing_last_registered_at_ms": existing.registered_at_ms},
        )
    if incoming.weight_permille > existing.weight_permille:
        return (
            "override",
            "weight_higher",
            {"incoming_weight_permille": incoming.weight_permille, "existing_weight_permille": existing.weight_permille},
        )
    if incoming.weight_permille < existing.weight_permille:
        return (
            "skip",
            "weight_lower",
            {"incoming_weight_permille": incoming.weight_permille, "existing_weight_permille": existing.weight_permille},
        )
    if _same_registration(incoming, existing):
        return ("skip", "identical_record", {})
    return (
        "reject",
        "ambiguous_complete_tie",
        {
            "tie_fields": ["lease_version", "last_registered_at_ms", "weight_permille"],
            "message": "records differ after all declared tie breakers; batch must not choose arbitrarily",
        },
    )


class SnapshotImporter:
    def __init__(self, database: Database) -> None:
        self.db = database
        self._batch_locks: dict[str, asyncio.Lock] = {}

    def negotiate(
        self,
        data: bytes | str | list[bytes | str],
        *,
        expected_format: str = "auto",
        expected_kind: str | None = None,
        weight_policy: WeightPolicy | None = None,
        time_policy: TimePolicy | None = None,
    ) -> dict[str, Any]:
        weight_policy = weight_policy or WeightPolicy()
        time_policy = time_policy or TimePolicy()
        snapshot, info, negotiation = decode_snapshot(
            data,
            expected_format=expected_format,
            expected_kind=expected_kind,
        )
        batch = self._normalize(snapshot, weight_policy, time_policy)
        details = batch.details()
        return {
            "ok": True,
            "detected": info.to_dict(),
            "negotiation": negotiation,
            "record_count": len(snapshot.records),
            "preview": {
                "records": details["records"][:5],
                "degradation_details": details["degradation_details"],
                "weight_rounding": details["weight_rounding"],
                "time_truncation": details["time_truncation"],
            },
        }

    def _normalize(
        self,
        snapshot: Snapshot,
        weight_policy: WeightPolicy,
        time_policy: TimePolicy,
    ) -> NormalizationBatch:
        if snapshot.kind == "registration":
            return normalize_registrations(
                list(snapshot.records),  # type: ignore[arg-type]
                weight_policy=weight_policy,
                time_policy=time_policy,
            )
        return normalize_probe_results(
            list(snapshot.records),  # type: ignore[arg-type]
            weight_policy=weight_policy,
            time_policy=time_policy,
        )

    async def import_snapshot(self, request: ImportInput) -> dict[str, Any]:
        if not request.batch_id or not request.batch_id.strip():
            raise BusinessError(E_DB_ERROR, "batch_id must not be empty")
        content_hash = _content_hash(
            request.data,
            expected_format=request.expected_format,
            expected_kind=request.expected_kind,
            weight_policy=request.weight_policy,
            time_policy=request.time_policy,
        )
        snapshot, info, negotiation = decode_snapshot(
            request.data,
            expected_format=request.expected_format,
            expected_kind=request.expected_kind,
        )
        normalized = self._normalize(snapshot, request.weight_policy, request.time_policy)
        lock = self._batch_locks.setdefault(request.batch_id, asyncio.Lock())
        async with lock:
            existing_batch = self._get_batch(request.batch_id)
            if existing_batch is not None:
                if existing_batch["content_hash"] != content_hash:
                    raise BusinessError(
                        E_CONFLICTING_BATCH,
                        "batch_id was already used with different content",
                        details={
                            "batch_id": request.batch_id,
                            "existing_content_hash": existing_batch["content_hash"],
                            "submitted_content_hash": content_hash,
                        },
                        http_status=409,
                    )
                if existing_batch["status"] in ("COMMITTED", "REJECTED"):
                    report = json.loads(existing_batch["report_json"])
                    return {
                        **report,
                        "replayed": True,
                        "executed": False,
                        "message": "batch already executed; stored report returned",
                    }
                if existing_batch["status"] == "STAGING":
                    raise BusinessError(
                        "E_IMPORT_IN_PROGRESS",
                        "batch is staged but not complete; run recovery before retrying",
                        details={"batch_id": request.batch_id},
                        http_status=409,
                    )
                # ABORTED batches were never committed and may be retried with identical content.

            intent_id = self.db.start_intent(
                "IMPORT",
                request.batch_id,
                {
                    "batch_id": request.batch_id,
                    "content_hash": content_hash,
                    "format": info.format,
                    "version": info.version,
                    "kind": info.kind,
                },
            )
            staged_at = now_ms()
            with self.db.transaction() as connection:
                if existing_batch is None:
                    connection.execute(
                        """
                        INSERT INTO import_batches (
                            batch_id, content_hash, status, format, version, kind, created_at_ms
                        ) VALUES (?, ?, 'STAGING', ?, ?, ?, ?)
                        """,
                        (request.batch_id, content_hash, info.format, info.version, info.kind, staged_at),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE import_batches SET status = 'STAGING', report_json = NULL,
                            created_at_ms = ?, completed_at_ms = NULL
                        WHERE batch_id = ?
                        """,
                        (staged_at, request.batch_id),
                    )
            maybe_crash("import_after_stage")

            if snapshot.kind == "registration":
                try:
                    return self._import_registrations(
                        request=request,
                        intent_id=intent_id,
                        info=info,
                        negotiation=negotiation,
                        normalization=normalized,
                        content_hash=content_hash,
                    )
                except BaseException as exc:
                    self._abort_staged_import(request.batch_id, intent_id, exc)
                    raise
            try:
                return self._import_probe_results(
                    request=request,
                    intent_id=intent_id,
                    info=info,
                    negotiation=negotiation,
                    normalization=normalized,
                    content_hash=content_hash,
                )
            except BaseException as exc:
                self._abort_staged_import(request.batch_id, intent_id, exc)
                raise

    def _abort_staged_import(self, batch_id: str, intent_id: int, exc: BaseException) -> None:
        try:
            with self.db.transaction() as connection:
                connection.execute(
                    """
                    UPDATE import_batches SET status = 'ABORTED', report_json = ?, completed_at_ms = ?
                    WHERE batch_id = ? AND status = 'STAGING'
                    """,
                    (
                        canonical_json(
                            {
                                "ok": False,
                                "batch_id": batch_id,
                                "status": "aborted",
                                "error_code": "E_IMPORT_ABORTED",
                                "message": str(exc),
                                "atomic_rollback": True,
                            }
                        ),
                        now_ms(),
                        batch_id,
                    ),
                )
        finally:
            self.db.abort_intent(intent_id, {"error": str(exc)})

    def _base_report(
        self,
        *,
        request: ImportInput,
        info: Any,
        negotiation: dict[str, Any],
        normalization: NormalizationBatch,
        content_hash: str,
    ) -> dict[str, Any]:
        details = normalization.details()
        return {
            "ok": True,
            "batch_id": request.batch_id,
            "content_hash": content_hash,
            "status": "committed",
            "replayed": False,
            "executed": True,
            "detected": info.to_dict(),
            "negotiation": negotiation,
            "record_count": len(normalization.records),
            "outcomes": [],
            "degradation_details": details["degradation_details"],
            "weight_rounding": details["weight_rounding"],
            "time_truncation": details["time_truncation"],
            "applied_count": 0,
            "skipped_count": 0,
            "rejected_count": 0,
        }

    def _import_registrations(
        self,
        *,
        request: ImportInput,
        intent_id: int,
        info: Any,
        negotiation: dict[str, Any],
        normalization: NormalizationBatch,
        content_hash: str,
    ) -> dict[str, Any]:
        report = self._base_report(
            request=request,
            info=info,
            negotiation=negotiation,
            normalization=normalization,
            content_hash=content_hash,
        )
        incoming = [record for record in normalization.records if isinstance(record, NormalizedRegistration)]
        existing_rows = {row.key: row for row in self.db.list_instances()}
        working: dict[tuple[str, str], NormalizedRegistration] = {
            key: row.as_normalized() for key, row in existing_rows.items()
        }
        final_applies: dict[tuple[str, str], NormalizedRegistration] = {}
        rejected = False
        for index, record in enumerate(incoming):
            existing = working.get(record.key)
            if existing is None:
                outcome = "override"
                basis = "new_record"
                evidence = {"create": True}
                final_applies[record.key] = record
                working[record.key] = record
            else:
                outcome, basis, evidence = resolve_conflict(record, existing)
                if outcome == "override":
                    final_applies[record.key] = record
                    working[record.key] = record
                elif outcome == "reject":
                    rejected = True
            report["outcomes"].append(
                {
                    "record_index": index,
                    "service_name": record.service_name,
                    "instance_id": record.instance_id,
                    "outcome": outcome,
                    "outcome_label": OUTCOME_LABELS[outcome],
                    "basis": basis,
                    "evidence": evidence,
                }
            )
            if outcome == "override":
                report["applied_count"] += 1
            elif outcome == "skip":
                report["skipped_count"] += 1
            else:
                report["rejected_count"] += 1

        completed_at = now_ms()
        with self.db.transaction() as connection:
            if rejected:
                report["status"] = "rejected"
                report["ok"] = False
                report["error_code"] = "E_IMPORT_REJECTED"
                report["message"] = "one or more records were rejected; no instance changes were applied"
                report["applied_count"] = 0
                report["atomic_rollback"] = True
            else:
                for key, record in final_applies.items():
                    current = self.db.get_instance(key[0], key[1], connection=connection)
                    record_version = 1 if current is None else current.record_version + 1
                    stored = replace(
                        record,
                        last_heartbeat_at_ms=completed_at,
                    )
                    self.db.write_instance(
                        connection,
                        stored,
                        granted_at_ms=completed_at,
                        health_status="UNKNOWN",
                        health_reason=f"imported by batch {request.batch_id}; awaiting probe",
                        record_version=record_version,
                        source="import",
                    )
                report["atomic_rollback"] = False
            maybe_crash("import_before_commit")
            report_json = canonical_json(report)
            connection.execute(
                """
                UPDATE import_batches SET status = ?, report_json = ?, completed_at_ms = ?
                WHERE batch_id = ?
                """,
                (
                    "REJECTED" if rejected else "COMMITTED",
                    report_json,
                    completed_at,
                    request.batch_id,
                ),
            )
            self.db.finish_intent(connection, intent_id, "COMMITTED", {"report_status": report["status"]})
        return report

    def _import_probe_results(
        self,
        *,
        request: ImportInput,
        intent_id: int,
        info: Any,
        negotiation: dict[str, Any],
        normalization: NormalizationBatch,
        content_hash: str,
    ) -> dict[str, Any]:
        report = self._base_report(
            request=request,
            info=info,
            negotiation=negotiation,
            normalization=normalization,
            content_hash=content_hash,
        )
        records = [record for record in normalization.records if isinstance(record, NormalizedProbeResult)]
        completed_at = now_ms()
        round_id = f"import:{request.batch_id}"
        with self.db.transaction() as connection:
            for index, record in enumerate(records):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO probe_history (
                        round_id, service_name, instance_id, probe_method, checked_at_ms,
                        success, status, outcome, latency_ms, error_code, detail_json,
                        weight_before, weight_after, lease_version, health_status,
                        record_version, created_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        round_id,
                        record.service_name,
                        record.instance_id,
                        record.probe_method,
                        record.checked_at_ms,
                        1 if record.success else 0,
                        record.status,
                        record.outcome,
                        record.latency_ms,
                        record.error_code,
                        canonical_json(
                            {
                                "endpoint": record.endpoint,
                                "probe_params": record.probe_params,
                                "probe_window_ms": record.probe_window_ms,
                                "detail": record.detail,
                            }
                        ),
                        record.weight_permille,
                        record.weight_permille,
                        record.lease_version or 0,
                        record.status,
                        0,
                        completed_at,
                    ),
                )
                report["outcomes"].append(
                    {
                        "record_index": index,
                        "service_name": record.service_name,
                        "instance_id": record.instance_id,
                        "outcome": "append",
                        "outcome_label": OUTCOME_LABELS["append"],
                        "basis": "probe_snapshot_history",
                    }
                )
                report["applied_count"] += 1
            maybe_crash("import_probe_before_commit")
            connection.execute(
                """
                UPDATE import_batches SET status = 'COMMITTED', report_json = ?, completed_at_ms = ?
                WHERE batch_id = ?
                """,
                (canonical_json(report), completed_at, request.batch_id),
            )
            self.db.finish_intent(connection, intent_id, "COMMITTED", {"report_status": "committed"})
        return report

    def _get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM import_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self._get_batch(batch_id)
        if row is None:
            raise BusinessError(
                "E_NOT_FOUND",
                "import batch was not found",
                details={"batch_id": batch_id},
                http_status=404,
            )
        if row.get("report_json"):
            row["report"] = json.loads(row["report_json"])
        return row
