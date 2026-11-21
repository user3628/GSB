"""Application service: validation, single-writer commits and read snapshots."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping

from .errors import BusinessError, PolicyError
from .models import BatchRequest, LogRecordIn, SpanRecordIn
from .policy import Policy, canonical_json
from .sampling import (
    Record,
    aggregate,
    build_report_rows,
    counts_digest,
    decide_records,
    decision_digest,
)
from .storage import Storage, utc_now

logger = logging.getLogger(__name__)


class _BatchCommittedElsewhere(Exception):
    """Internal signal: another writer committed the same batch while we waited."""


class SamplerService:
    """Single-writer coordinator with atomic, versioned derived snapshots."""

    def __init__(self, db_path: str | Path, *, recover_on_start: bool = True) -> None:
        self.storage = Storage(db_path)
        self._writer_lock = threading.RLock()
        self.storage.initialize()
        if recover_on_start:
            with self._writer_lock:
                self.storage.recover_after_forced_termination()

    # ------------------------------------------------------------------
    # Policy operations
    # ------------------------------------------------------------------
    def submit_policy(
        self,
        document: Mapping[str, Any],
        *,
        crash_hook: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        try:
            policy = Policy.from_mapping(document)
        except PolicyError as exc:
            raise BusinessError(
                "E_POLICY_INVALID",
                str(exc),
                status_code=422,
                details={"reason": "policy_snapshot_invalid"},
            ) from exc

        with self._writer_lock:
            current = self.storage.current_policy_version()
            if current is not None and policy.version <= current:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "policy version must be greater than the current version",
                    status_code=409,
                    details={"submitted_version": policy.version, "current_version": current},
                )

            intent_id = self.storage.begin_intent(
                "POLICY_COMMIT",
                policy_version=policy.version,
                details={"digest": policy.digest},
            )
            try:
                with self.storage.write_transaction() as connection:
                    inside_current = self.storage.current_policy_version(connection)
                    if inside_current is not None and policy.version <= inside_current:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "policy version became stale before commit",
                            status_code=409,
                            details={
                                "submitted_version": policy.version,
                                "current_version": inside_current,
                            },
                        )
                    if self.storage.get_policy_row_in_tx(connection, policy.version) is not None:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "policy version already exists",
                            status_code=409,
                            details={"submitted_version": policy.version},
                        )

                    self.storage.insert_policy(connection, policy)
                    records = self.storage.load_all_records(connection)
                    derived = self._derive(records, policy)
                    self.storage.replace_derived_state(
                        connection,
                        policy=policy,
                        decisions=derived["decisions"],
                        windows=derived["windows"],
                        count_rows=derived["count_rows"],
                        report_rows=derived["report_rows"],
                        record_count=derived["record_count"],
                        leader_count=derived["leader_count"],
                        duplicate_count=derived["duplicate_count"],
                        decision_digest=derived["decision_digest"],
                        counts_digest=derived["counts_digest"],
                        report_digest=derived["report_digest"],
                        crash_hook=crash_hook,
                    )
                    self.storage.set_current_policy(connection, policy.version)
                    self.storage.complete_intent(connection, intent_id)
                logger.info("committed policy version %s", policy.version)
            except BaseException as exc:
                self._safe_abort_intent(intent_id, type(exc).__name__)
                raise

            state = self.storage.get_policy_state(policy.version)
            return {
                "status": "COMMITTED",
                "intent_id": intent_id,
                "policy": policy.as_summary(),
                "derived_revision": state["commit"]["revision"] if state.get("commit") else None,
                "record_count": state["commit"]["record_count"] if state.get("commit") else 0,
                "decision_states": state.get("decision_states", {}),
            }

    def get_policy(self, version: int | None = None) -> dict[str, Any]:
        if version is None:
            current = self.storage.current_policy_version()
            if current is None:
                raise BusinessError(
                    "E_POLICY_NOT_FOUND",
                    "no sampling policy has been committed",
                    status_code=404,
                )
            version = current
        row = self.storage.get_policy_row(version)
        if row is None:
            raise BusinessError(
                "E_POLICY_NOT_FOUND",
                f"policy version {version} does not exist",
                status_code=404,
                details={"policy_version": version},
            )
        current = self.storage.current_policy_version()
        return {
            "policy_version": version,
            "is_current": current == version,
            "policy": json.loads(row["document_json"]),
            "committed_at": row["committed_at"],
        }

    def get_sampling_rate(
        self, version: int | None, service: str, level: str
    ) -> dict[str, Any]:
        policy_response = self.get_policy(version)
        try:
            policy = Policy.from_mapping(policy_response["policy"])
        except PolicyError as exc:
            raise BusinessError(
                "E_POLICY_INVALID",
                "stored policy snapshot is invalid",
                status_code=500,
                details={"reason": str(exc)},
            ) from exc
        normalized_level = level.upper()
        return {
            "policy_version": policy.version,
            "service": service,
            "level": normalized_level,
            "head_rate": policy.head_rate(service, normalized_level),
            "defined": policy.has_stratum(service, normalized_level),
            "tail_sampling": policy_response["policy"].get("tail_sampling", {}),
            "dedup_window_ms": policy.dedup_window_ms,
            "seed": policy.seed,
        }

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------
    def submit_batch(self, request: BatchRequest) -> dict[str, Any]:
        records = self._validate_and_build_records(request)
        content_digest = self._batch_content_digest(request)

        with self._writer_lock:
            existing = self.storage.get_batch(request.batch_id)
            if existing is not None:
                return self._existing_batch_result(existing, content_digest)

            current = self.storage.current_policy_version()
            if current is None:
                raise BusinessError(
                    "E_POLICY_NOT_FOUND",
                    "commit a sampling policy before submitting records",
                    status_code=404,
                )
            if request.policy_version is not None and request.policy_version != current:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "batch policy_version must identify the current committed policy",
                    status_code=409,
                    details={
                        "requested_policy_version": request.policy_version,
                        "current_version": current,
                    },
                )

            intent_id = self.storage.begin_intent(
                "BATCH_COMMIT",
                policy_version=current,
                batch_id=request.batch_id,
                details={"content_digest": content_digest, "record_count": len(records)},
            )
            try:
                with self.storage.write_transaction() as connection:
                    inside_existing = self.storage.get_batch_in_tx(connection, request.batch_id)
                    if inside_existing is not None:
                        if inside_existing["content_digest"] == content_digest:
                            raise _BatchCommittedElsewhere
                        raise BusinessError(
                            "E_CONFLICTING_RECORD",
                            "batch_id already exists with different content",
                            status_code=409,
                            details={
                                "batch_id": request.batch_id,
                                "existing_digest": inside_existing["content_digest"],
                                "submitted_digest": content_digest,
                            },
                        )
                    inside_current = self.storage.current_policy_version(connection)
                    if inside_current is None or inside_current != current:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "current policy changed while waiting for the writer lock",
                            status_code=409,
                            details={
                                "observed_version": current,
                                "current_version": inside_current,
                            },
                        )

                    policy_row = self.storage.get_policy_row_in_tx(connection, current)
                    if policy_row is None:
                        raise BusinessError(
                            "E_POLICY_NOT_FOUND",
                            f"current policy {current} is missing",
                            status_code=500,
                        )
                    policy = Policy.from_mapping(json.loads(policy_row["document_json"]))
                    self.storage.insert_batch(
                        connection,
                        batch_id=request.batch_id,
                        content_digest=content_digest,
                        policy_version=current,
                        record_count=len(records),
                        ingested_at=utc_now(),
                    )
                    self.storage.insert_records(connection, records)
                    all_records = self.storage.load_all_records(connection)
                    derived = self._derive(all_records, policy)
                    self.storage.replace_derived_state(
                        connection,
                        policy=policy,
                        decisions=derived["decisions"],
                        windows=derived["windows"],
                        count_rows=derived["count_rows"],
                        report_rows=derived["report_rows"],
                        record_count=derived["record_count"],
                        leader_count=derived["leader_count"],
                        duplicate_count=derived["duplicate_count"],
                        decision_digest=derived["decision_digest"],
                        counts_digest=derived["counts_digest"],
                        report_digest=derived["report_digest"],
                    )
                    self.storage.complete_intent(connection, intent_id)
                logger.info(
                    "committed batch %s with %s records under policy %s",
                    request.batch_id,
                    len(records),
                    current,
                )
            except _BatchCommittedElsewhere:
                self._safe_abort_intent(intent_id, "duplicate_batch_reused")
                committed = self.storage.get_batch(request.batch_id)
                if committed is None:
                    raise
                logger.info("reused concurrently committed batch %s", request.batch_id)
                return self._existing_batch_result(committed, content_digest)
            except BusinessError:
                self._safe_abort_intent(intent_id, "business_error")
                raise
            except BaseException as exc:
                self._safe_abort_intent(intent_id, type(exc).__name__)
                raise

            batch = self.storage.get_batch(request.batch_id)
            assert batch is not None
            return self._batch_result(batch, replay=False)

    def get_batch_result(self, batch_id: str) -> dict[str, Any]:
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            raise BusinessError(
                "E_BATCH_NOT_FOUND",
                f"batch {batch_id!r} does not exist",
                status_code=404,
                details={"batch_id": batch_id},
            )
        return self._batch_result(batch, replay=True)

    def _existing_batch_result(
        self, existing: dict[str, Any], content_digest: str
    ) -> dict[str, Any]:
        if existing["content_digest"] != content_digest:
            raise BusinessError(
                "E_CONFLICTING_RECORD",
                "batch_id already exists with different content",
                status_code=409,
                details={
                    "batch_id": existing["batch_id"],
                    "existing_digest": existing["content_digest"],
                    "submitted_digest": content_digest,
                },
            )
        return self._batch_result(existing, replay=True)

    def _batch_result(self, batch: dict[str, Any], *, replay: bool) -> dict[str, Any]:
        current = self.storage.current_policy_version()
        version = current if current is not None else batch["policy_version"]
        decisions = self.storage.get_batch_decisions(batch["batch_id"], version)
        if not decisions and version != batch["policy_version"]:
            version = batch["policy_version"]
            decisions = self.storage.get_batch_decisions(batch["batch_id"], version)
        state_counts = Counter(decision["state"] for decision in decisions)
        path_counts = Counter(decision["path"] for decision in decisions)
        return {
            "batch_id": batch["batch_id"],
            "status": batch["status"],
            "execution": "REPLAYED_COMMITTED_RESULT" if replay else "COMMITTED_ONCE",
            "replay": replay,
            "ingested_policy_version": batch["policy_version"],
            "reported_policy_version": version,
            "content_digest": batch["content_digest"],
            "record_count": batch["record_count"],
            "decision_states": dict(sorted(state_counts.items())),
            "decision_paths": dict(sorted(path_counts.items())),
            "decisions": decisions,
        }

    # ------------------------------------------------------------------
    # Reports and recovery
    # ------------------------------------------------------------------
    def get_bias_report(
        self, version: int | None, service: str, level: str
    ) -> dict[str, Any]:
        policy_response = self.get_policy(version)
        policy_version = int(policy_response["policy_version"])
        normalized_level = level.upper()
        report = self.storage.get_bias_report(policy_version, service, normalized_level)
        if report is None:
            raise BusinessError(
                "E_INSUFFICIENT_SAMPLES",
                "no local population exists for the requested stratum",
                status_code=422,
                details={
                    "policy_version": policy_version,
                    "service": service,
                    "level": normalized_level,
                    "reason": "NO_POPULATION",
                },
            )
        if report["status"] == "INSUFFICIENT":
            raise BusinessError(
                "E_INSUFFICIENT_SAMPLES",
                "the stratum does not have enough emitted samples for a bias estimate",
                status_code=422,
                details={
                    "policy_version": policy_version,
                    "service": service,
                    "level": normalized_level,
                    "reason": report["insufficient_reason"],
                    "population_count": report["population_count"],
                    "emitted_count": report["emitted_count"],
                    "min_samples": report["min_samples"],
                },
            )
        return {
            "policy_version": policy_version,
            "service": service,
            "level": normalized_level,
            "report": report,
            "kahan_note": "weighted sums use compensated Kahan accumulation",
            "error_bound_note": "confidence_95_bound is 1.96 * sqrt(estimated HT variance)",
        }

    def get_policy_state(self, version: int | None = None) -> dict[str, Any]:
        if version is None:
            version = self.storage.current_policy_version()
            if version is None:
                raise BusinessError(
                    "E_POLICY_NOT_FOUND",
                    "no sampling policy has been committed",
                    status_code=404,
                )
        state = self.storage.get_policy_state(version)
        if not state.get("exists"):
            raise BusinessError(
                "E_POLICY_NOT_FOUND",
                f"policy version {version} does not exist",
                status_code=404,
                details={"policy_version": version},
            )
        if state.get("commit") is None:
            raise BusinessError(
                "E_STATE_INCOMPLETE",
                "policy has no atomically committed derived snapshot",
                status_code=500,
                details={"policy_version": version},
            )
        return state

    def get_recovery_report(self) -> dict[str, Any]:
        report = self.storage.recovery_report()
        if report is None:
            with self._writer_lock:
                report = self.storage.recover_after_forced_termination()
        return report

    def recover(self) -> dict[str, Any]:
        with self._writer_lock:
            return self.storage.recover_after_forced_termination()

    # ------------------------------------------------------------------
    # Internal derivation
    # ------------------------------------------------------------------
    def _derive(self, records: list[Record], policy: Policy) -> dict[str, Any]:
        decisions, windows = decide_records(records, policy)
        counters = aggregate(records, decisions, policy)
        report_rows = build_report_rows(counters, policy)
        leader_count = sum(1 for decision in decisions if not decision.duplicate)
        duplicate_count = len(decisions) - leader_count
        report_digest = hashlib.sha256(canonical_json(report_rows).encode("utf-8")).hexdigest()
        return {
            "decisions": decisions,
            "windows": windows,
            "count_rows": report_rows,
            "report_rows": report_rows,
            "record_count": len(records),
            "leader_count": leader_count,
            "duplicate_count": duplicate_count,
            "decision_digest": decision_digest(decisions),
            "counts_digest": counts_digest(report_rows),
            "report_digest": report_digest,
        }

    def _validate_and_build_records(self, request: BatchRequest) -> list[Record]:
        if not request.logs and not request.spans:
            raise BusinessError(
                "E_RECORD_INVALID",
                "a batch must contain at least one log or span record",
                status_code=422,
                details={"reason": "EMPTY_BATCH"},
            )
        self._validate_monotonic(request.logs, "logs")
        self._validate_monotonic(request.spans, "spans")

        seen: set[tuple[str, str, str]] = set()
        records: list[Record] = []
        for source_index, row in enumerate(request.logs):
            self._validate_record_identity(row, "log", source_index, seen)
            records.append(
                Record(
                    record_uid=self._record_uid(request.batch_id, "log", source_index),
                    batch_id=request.batch_id,
                    kind="log",
                    source_index=source_index,
                    timestamp_ms=row.timestamp_ms,
                    service=row.service,
                    level=row.level,
                    trace_id=row.trace_id,
                    span_id=row.span_id,
                    parent_span_id=row.parent_span_id,
                    message_fingerprint=row.message_fingerprint,
                    latency_us=0,
                    error=False,
                    sample_weight=row.sample_weight,
                )
            )
        for source_index, row in enumerate(request.spans):
            self._validate_record_identity(row, "span", source_index, seen)
            records.append(
                Record(
                    record_uid=self._record_uid(request.batch_id, "span", source_index),
                    batch_id=request.batch_id,
                    kind="span",
                    source_index=source_index,
                    timestamp_ms=row.timestamp_ms,
                    service=row.service,
                    level=row.level,
                    trace_id=row.trace_id,
                    span_id=row.span_id,
                    parent_span_id=row.parent_span_id,
                    message_fingerprint=row.message_fingerprint,
                    latency_us=row.latency_us,
                    error=row.error,
                    sample_weight=row.sample_weight,
                )
            )
        return records

    def _validate_monotonic(
        self, rows: list[LogRecordIn] | list[SpanRecordIn], stream_name: str
    ) -> None:
        previous: int | None = None
        for index, row in enumerate(rows):
            if previous is not None and row.timestamp_ms < previous:
                raise BusinessError(
                    "E_RECORD_INVALID",
                    "record timestamps must be non-decreasing within each stream",
                    status_code=422,
                    details={
                        "reason": "TIMESTAMP_NOT_MONOTONIC",
                        "stream": stream_name,
                        "row_index": index,
                        "previous_timestamp_ms": previous,
                        "timestamp_ms": row.timestamp_ms,
                    },
                )
            previous = row.timestamp_ms

    def _validate_record_identity(
        self,
        row: LogRecordIn | SpanRecordIn,
        kind: str,
        source_index: int,
        seen: set[tuple[str, str, str]],
    ) -> None:
        identity = (kind, row.trace_id, row.span_id)
        if identity in seen:
            raise BusinessError(
                "E_RECORD_INVALID",
                "duplicate (kind, trace_id, span_id) within the batch",
                status_code=422,
                details={
                    "reason": "DUPLICATE_RECORD_IDENTITY",
                    "kind": kind,
                    "row_index": source_index,
                    "trace_id": row.trace_id,
                    "span_id": row.span_id,
                },
            )
        seen.add(identity)
        if row.parent_span_id == row.span_id:
            raise BusinessError(
                "E_RECORD_INVALID",
                "span_id must not equal parent_span_id",
                status_code=422,
                details={
                    "reason": "SELF_PARENT_SPAN",
                    "kind": kind,
                    "row_index": source_index,
                },
            )

    def _batch_content_digest(self, request: BatchRequest) -> str:
        payload = request.model_dump(mode="json", exclude={"policy_version"})
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def _record_uid(self, batch_id: str, kind: str, source_index: int) -> str:
        return f"{batch_id}:{kind}:{source_index:08d}"

    def _safe_abort_intent(self, intent_id: str, reason: str) -> None:
        try:
            self.storage.abort_intent(intent_id, reason)
        except Exception:  # pragma: no cover - best effort during an error path
            logger.exception("failed to abort intent %s", intent_id)
