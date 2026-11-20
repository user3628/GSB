from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .db import Database, canonical_json, policies_snapshot_json
from .errors import AppError, format_incompatible
from .formats import (
    COMPATIBILITY_MATRIX,
    LATEST_BINARY_VERSION,
    LATEST_JSONL_VERSION,
    decode_policies,
    encode_binary,
    encode_jsonl,
    encode_jsonl_shards,
)
from .models import Observation, Policy, normalize_observation, normalize_policy, now_ms
from .state_machine import (
    DELAYED,
    DEGRADED,
    HEALTHY,
    LEGAL_TRANSITIONS,
    RESTORED,
    STATES,
    decide_state,
    deduplicate_observations,
    ensure_transition,
    effective_action,
    priority_band,
    replay_decisions,
)
from .versions import parse_version


DEFAULT_DB_PATH = Path(
    os.environ.get(
        "RG_DB_PATH",
        Path(__file__).resolve().parents[1] / "data" / "replication_guard.sqlite3",
    )
)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


class ReplicationService:
    def __init__(self, db_path: str | Path | None = None):
        self.db = Database(db_path or DEFAULT_DB_PATH)
        self.writer_lock = threading.RLock()
        self.started = False
        self.crash_points = {
            point.strip()
            for point in os.environ.get("RG_CRASH_POINT", "").split(",")
            if point.strip()
        }

    def startup(self) -> None:
        with self.writer_lock:
            if self.started:
                return
            self.db.initialize()
            self.recover()
            self.started = True

    def _maybe_crash(self, point: str) -> None:
        if point in self.crash_points:
            os._exit(91 if point == "after_intent" else 92)

    def _begin_intent(self, batch_id: str, operation: str, details: Mapping[str, Any]) -> str:
        intent_id = _new_id("intent")
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, batch_id, operation, status, created_at_ms, details_json)
                VALUES (?, ?, ?, 'PREPARED', ?, ?)
                """,
                (intent_id, batch_id, operation, now_ms(), canonical_json(dict(details))),
            )
        self._maybe_crash("after_intent")
        return intent_id

    def _finish_success(
        self,
        connection: sqlite3.Connection,
        *,
        intent_id: str,
        batch_id: str,
        operation: str,
        result: Mapping[str, Any],
    ) -> None:
        timestamp = now_ms()
        connection.execute(
            "UPDATE intents SET status = 'COMMITTED', finished_at_ms = ? WHERE intent_id = ?",
            (timestamp, intent_id),
        )
        connection.execute(
            """
            INSERT INTO applied_batches(batch_id, operation, status, result_json, error_json, created_at_ms, finished_at_ms)
            VALUES (?, ?, 'COMMITTED', ?, NULL, ?, ?)
            """,
            (batch_id, operation, canonical_json(dict(result)), timestamp, timestamp),
        )

    def _record_failure(
        self,
        *,
        intent_id: str,
        batch_id: str,
        operation: str,
        error: AppError,
    ) -> None:
        payload = error.payload()
        error_json = canonical_json(
            {
                "code": error.code,
                "message": error.message,
                "details": error.details,
                "statusCode": error.status_code,
            }
        )
        timestamp = now_ms()
        with self.db.transaction() as connection:
            connection.execute(
                """
                UPDATE intents SET status = 'ABORTED', finished_at_ms = ?, error_json = ?
                WHERE intent_id = ? AND status = 'PREPARED'
                """,
                (timestamp, error_json, intent_id),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO applied_batches(
                    batch_id, operation, status, result_json, error_json, created_at_ms, finished_at_ms
                ) VALUES (?, ?, 'REJECTED', ?, ?, ?, ?)
                """,
                (batch_id, operation, canonical_json(payload), error_json, timestamp, timestamp),
            )

    def _reuse_batch(self, batch_id: str) -> dict[str, Any] | None:
        connection = self.db.connect()
        try:
            row = connection.execute(
                "SELECT * FROM applied_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        if row["status"] == "COMMITTED":
            result = json.loads(row["result_json"])
            result["deduplicated"] = True
            result["batchId"] = batch_id
            return result
        stored = json.loads(row["error_json"] or "{}")
        code = stored.get("code", "E_BATCH_ABORTED" if row["status"] == "ABORTED" else "E_BATCH_REJECTED")
        message = stored.get("message", f"批次 {batch_id} 已中止，不能重复执行")
        details = dict(stored.get("details") or {})
        details.update({"batchId": batch_id, "deduplicated": True, "storedBatchStatus": row["status"]})
        raise AppError(
            code,
            message,
            status_code=int(stored.get("statusCode", 409)),
            details=details,
        )

    def _execute_write(
        self,
        *,
        batch_id: str | None,
        operation: str,
        details: Mapping[str, Any],
        work,
    ) -> dict[str, Any]:
        self.startup()
        batch_id = batch_id or _new_id("batch")
        with self.writer_lock:
            reused = self._reuse_batch(batch_id)
            if reused is not None:
                return reused
            intent_id = self._begin_intent(batch_id, operation, details)
            try:
                result = work(batch_id, intent_id)
                return result
            except AppError as exc:
                self._record_failure(
                    intent_id=intent_id,
                    batch_id=batch_id,
                    operation=operation,
                    error=exc,
                )
                raise
            except Exception as exc:
                wrapped = AppError(
                    "E_INTERNAL",
                    f"内部写入失败：{type(exc).__name__}: {exc}",
                    status_code=500,
                    details={"operation": operation, "batchId": batch_id},
                )
                self._record_failure(
                    intent_id=intent_id,
                    batch_id=batch_id,
                    operation=operation,
                    error=wrapped,
                )
                raise wrapped from exc

    # ------------------------------------------------------------------
    # Read APIs
    # ------------------------------------------------------------------
    def list_policies(self) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            policies = self.db.read_policies(connection)
            return {
                "ok": True,
                "datasetVersion": self.db.dataset_version(connection),
                "policies": [policy.to_output_dict() for policy in policies],
            }

    def get_policy(self, slave_id: str) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            row = connection.execute("SELECT * FROM policies WHERE slave_id = ?", (slave_id,)).fetchone()
            if row is None:
                raise AppError("E_NOT_FOUND", f"策略 {slave_id} 不存在", status_code=404)
            return {
                "ok": True,
                "datasetVersion": self.db.dataset_version(connection),
                "policy": self.db.row_to_policy(row).to_output_dict(),
            }

    def export_policies(self, fmt: str, *, version: int | None = None, shards: int = 1) -> tuple[Any, dict[str, Any]]:
        self.startup()
        with self.db.reader() as connection:
            policies = self.db.read_policies(connection)
            dataset_version = self.db.dataset_version(connection)
        normalized = fmt.lower()
        metadata: dict[str, Any] = {
            "format": normalized,
            "datasetVersion": dataset_version,
            "policyCount": len(policies),
        }
        if normalized in {"binary", "bin", "policy-binary"}:
            selected_version = version if version is not None else LATEST_BINARY_VERSION
            content, truncations = encode_binary(policies, version=selected_version)
            metadata.update(
                {
                    "format": "policy-binary",
                    "version": selected_version,
                    "truncations": truncations,
                    "timePrecision": "seconds" if selected_version == 1 else "milliseconds",
                }
            )
            return content, metadata
        if normalized in {"jsonl", "ndjson", "json-lines", "policy-jsonl"}:
            selected_version = version if version is not None else LATEST_JSONL_VERSION
            shard_map = encode_jsonl_shards(policies, shard_count=shards, version=selected_version)
            metadata.update(
                {
                    "format": "policy-jsonl",
                    "version": selected_version,
                    "shardCount": shards,
                    "shardNames": sorted(shard_map),
                }
            )
            if shards == 1:
                return next(iter(shard_map.values())), metadata
            return shard_map, metadata
        raise format_incompatible("未知导出格式", format=fmt, supported=["binary", "jsonl"])

    def list_observations(self, *, batch_id: str | None = None) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            if batch_id:
                rows = connection.execute(
                    "SELECT * FROM observations WHERE batch_id = ? ORDER BY slave_id, sampled_at_ms, binlog_gap DESC, id",
                    (batch_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM observations ORDER BY slave_id, sampled_at_ms, binlog_gap DESC, id"
                ).fetchall()
            return {
                "ok": True,
                "observationBatchId": batch_id,
                "observations": [
                    {
                        **dict(row),
                        "reported_version_segments": list(
                            parse_version(str(row["reported_version"])).segments
                        ),
                    }
                    for row in rows
                ],
            }

    def list_states(self) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            rows = connection.execute("SELECT * FROM states ORDER BY slave_id").fetchall()
            return {
                "ok": True,
                "stateVersion": self.db.state_version(connection),
                "states": [
                    {
                        "slaveId": row["slave_id"],
                        "state": row["state"],
                        "effectiveAction": row["effective_action"],
                        "lastSampledAtMs": row["last_sampled_at_ms"],
                        "lastObservation": json.loads(row["last_observation_json"])
                        if row["last_observation_json"]
                        else None,
                        "trigger": json.loads(row["trigger_json"]),
                        "updatedAtMs": row["updated_at_ms"],
                    }
                    for row in rows
                ],
            }

    # ------------------------------------------------------------------
    # Policy writes and dual-format import/export
    # ------------------------------------------------------------------
    def import_policies(
        self,
        data: bytes | str | Sequence[str],
        *,
        format_hint: str = "auto",
        batch_id: str | None = None,
        source_name: str | None = None,
        expected_base_dataset_version: int | None = None,
    ) -> dict[str, Any]:
        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            if isinstance(data, (list, tuple)):
                payload: bytes | str = "\n".join(str(shard) for shard in data)
                hints = [shard for shard in data]
            else:
                payload = data
                hints = [data]
            decoded = decode_policies(payload, format_hint)
            negotiation = dict(decoded.negotiation)
            negotiation["degradationCount"] = len(decoded.degradations)
            negotiation["timeNormalizationCount"] = len(decoded.time_normalizations)
            negotiation["ignoredOptionalFieldCount"] = len(decoded.ignored_optional_fields)

            with self.db.transaction() as connection:
                current_version = self.db.dataset_version(connection)
                if (
                    expected_base_dataset_version is not None
                    and expected_base_dataset_version != current_version
                ):
                    raise AppError(
                        "E_STALE_BASE",
                        "导入基础版本已过期",
                        status_code=409,
                        details={
                            "expectedBaseDatasetVersion": expected_base_dataset_version,
                            "currentDatasetVersion": current_version,
                        },
                    )

                existing = {policy.slave_id: policy for policy in self.db.read_policies(connection)}
                virtual = dict(existing)
                outcomes: list[dict[str, Any]] = []
                apply: dict[str, Policy] = {}
                rejected = False
                for incoming in decoded.policies:
                    current = virtual.get(incoming.slave_id)
                    if current is None:
                        outcome = "ADDED"
                        reason = "新策略标识"
                        virtual[incoming.slave_id] = incoming
                        apply[incoming.slave_id] = incoming
                    elif incoming.canonical_key() == current.canonical_key():
                        outcome = "SKIP"
                        reason = "内容完全相同，幂等跳过"
                    else:
                        incoming_precedence = incoming.precedence()
                        current_precedence = current.precedence()
                        if incoming_precedence > current_precedence:
                            outcome = "OVERWRITE"
                            reason = "冲突裁决采用更高策略修订号、再更高优先级、再更新写入时间"
                            virtual[incoming.slave_id] = incoming
                            apply[incoming.slave_id] = incoming
                        elif incoming_precedence < current_precedence:
                            outcome = "SKIP"
                            reason = "现有策略冲突优先级更高"
                        else:
                            outcome = "REJECT"
                            reason = "版本号、优先级与写入时间完全并列但内容不同，无法确定性裁决"
                            rejected = True
                    outcomes.append(
                        {
                            "policyId": incoming.slave_id,
                            "outcome": outcome,
                            "reason": reason,
                            "incomingPolicyVersion": incoming.policy_version,
                            "incomingPriority": incoming.priority,
                            "incomingUpdatedAtMs": incoming.updated_at_ms,
                            "existingPolicyVersion": current.policy_version if current else None,
                            "existingPriority": current.priority if current else None,
                            "existingUpdatedAtMs": current.updated_at_ms if current else None,
                        }
                    )

                counts = {name: 0 for name in ("ADDED", "OVERWRITE", "SKIP", "REJECT")}
                for outcome in outcomes:
                    counts[outcome["outcome"]] += 1
                import_report = {
                    "sourceName": source_name,
                    "inputPolicyCount": len(decoded.policies),
                    "outcomes": outcomes,
                    "counts": counts,
                }
                if rejected:
                    raise AppError(
                        "E_IMPORT_CONFLICT",
                        "导入批次存在无法确定性裁决的冲突，整批回滚",
                        status_code=409,
                        details={
                            "negotiation": negotiation,
                            "importReport": import_report,
                            "atomicRollback": True,
                        },
                    )

                for policy in apply.values():
                    self.db.upsert_policy(connection, policy)
                changed = bool(apply)
                final_version = (
                    self.db.capture_dataset_version(connection, reason=f"IMPORT:{active_batch_id}")
                    if changed
                    else current_version
                )
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "POLICY_IMPORT",
                    "status": "APPLIED" if changed else "NO_CHANGES",
                    "datasetVersion": final_version,
                    "negotiation": negotiation,
                    "importReport": import_report,
                    "degradations": decoded.degradations,
                    "timeNormalizations": decoded.time_normalizations,
                    "ignoredOptionalFields": decoded.ignored_optional_fields,
                    "sourceName": source_name,
                    "appliedAtMs": now_ms(),
                }
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="POLICY_IMPORT",
                    result=result,
                )
                self._maybe_crash("before_commit")
            self._maybe_crash("after_commit")
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="POLICY_IMPORT",
            details={"formatHint": format_hint, "sourceName": source_name},
            work=work,
        )

    def upsert_policy(
        self,
        raw_policy: Mapping[str, Any],
        *,
        slave_id: str | None = None,
        base_dataset_version: int | None = None,
        batch_id: str | None = None,
        operation: str = "POLICY_UPSERT",
    ) -> dict[str, Any]:
        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            data = dict(raw_policy)
            if slave_id is not None:
                body_id = data.get("slave_id", data.get("slaveId", data.get("policyId", data.get("id"))))
                if body_id is not None and str(body_id) != slave_id:
                    raise AppError(
                        "E_POLICY_INVALID",
                        "路径中的从库标识与请求体不一致",
                        status_code=422,
                        details={"pathSlaveId": slave_id, "bodySlaveId": body_id},
                    )
                data["slave_id"] = slave_id
            policy = normalize_policy(data)
            with self.db.transaction() as connection:
                current_version = self.db.dataset_version(connection)
                if base_dataset_version is not None and base_dataset_version != current_version:
                    raise AppError(
                        "E_STALE_BASE",
                        "策略编辑基础版本已过期",
                        status_code=409,
                        details={
                            "baseDatasetVersion": base_dataset_version,
                            "currentDatasetVersion": current_version,
                        },
                    )
                self.db.upsert_policy(connection, policy)
                final_version = self.db.capture_dataset_version(
                    connection, reason=f"{operation}:{active_batch_id}"
                )
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": operation,
                    "datasetVersion": final_version,
                    "policy": policy.to_output_dict(),
                    "appliedAtMs": now_ms(),
                }
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation=operation,
                    result=result,
                )
                self._maybe_crash("before_commit")
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation=operation,
            details={"slaveId": slave_id, "baseDatasetVersion": base_dataset_version},
            work=work,
        )

    def delete_policy(
        self,
        slave_id: str,
        *,
        base_dataset_version: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            with self.db.transaction() as connection:
                current_version = self.db.dataset_version(connection)
                if base_dataset_version is not None and base_dataset_version != current_version:
                    raise AppError(
                        "E_STALE_BASE",
                        "删除基础版本已过期",
                        status_code=409,
                        details={
                            "baseDatasetVersion": base_dataset_version,
                            "currentDatasetVersion": current_version,
                        },
                    )
                exists = connection.execute(
                    "SELECT 1 FROM policies WHERE slave_id = ?", (slave_id,)
                ).fetchone()
                if exists is None:
                    raise AppError("E_NOT_FOUND", f"策略 {slave_id} 不存在", status_code=404)
                connection.execute("DELETE FROM policies WHERE slave_id = ?", (slave_id,))
                final_version = self.db.capture_dataset_version(
                    connection, reason=f"POLICY_DELETE:{active_batch_id}"
                )
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "POLICY_DELETE",
                    "datasetVersion": final_version,
                    "deletedPolicyId": slave_id,
                    "appliedAtMs": now_ms(),
                }
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="POLICY_DELETE",
                    result=result,
                )
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="POLICY_DELETE",
            details={"slaveId": slave_id, "baseDatasetVersion": base_dataset_version},
            work=work,
        )

    # ------------------------------------------------------------------
    # Observation ingestion
    # ------------------------------------------------------------------
    def ingest_observations(
        self,
        raw_observations: Sequence[Mapping[str, Any]],
        *,
        batch_id: str | None = None,
        source_name: str | None = None,
    ) -> dict[str, Any]:
        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            if not isinstance(raw_observations, Sequence) or isinstance(raw_observations, (str, bytes)):
                raise AppError("E_BAD_REQUEST", "observations 必须是数组", status_code=400)
            if not raw_observations:
                raise AppError("E_BAD_REQUEST", "observations 不能为空", status_code=400)
            observations = [normalize_observation(raw) for raw in raw_observations]
            with self.db.transaction() as connection:
                ingested = now_ms()
                ids: list[int] = []
                for observation in observations:
                    cursor = connection.execute(
                        """
                        INSERT INTO observations(
                            batch_id, slave_id, sampled_at_ms, binlog_gap, delay_ms, reported_version, ingested_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            active_batch_id,
                            observation.slave_id,
                            observation.sampled_at_ms,
                            observation.binlog_gap,
                            observation.delay_ms,
                            observation.reported_version,
                            ingested,
                        ),
                    )
                    ids.append(int(cursor.lastrowid))
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "OBSERVATION_INGEST",
                    "observationBatchId": active_batch_id,
                    "observationCount": len(observations),
                    "slaveCount": len({item.slave_id for item in observations}),
                    "observationIds": ids,
                    "sourceName": source_name,
                    "ingestedAtMs": ingested,
                }
                connection.execute(
                    "INSERT INTO observation_batches(batch_id, status, result_json, created_at_ms) VALUES (?, 'APPLIED', ?, ?)",
                    (active_batch_id, canonical_json(result), ingested),
                )
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="OBSERVATION_INGEST",
                    result=result,
                )
                self._maybe_crash("before_commit")
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="OBSERVATION_INGEST",
            details={"count": len(raw_observations), "sourceName": source_name},
            work=work,
        )

    # ------------------------------------------------------------------
    # State adjudication
    # ------------------------------------------------------------------
    @staticmethod
    def _read_state(connection: sqlite3.Connection, slave_id: str) -> str:
        row = connection.execute("SELECT state FROM states WHERE slave_id = ?", (slave_id,)).fetchone()
        return str(row["state"]) if row is not None else HEALTHY

    def recalculate(
        self,
        *,
        batch_id: str | None = None,
        observation_batch_id: str | None = None,
        window_size: int = 2,
        base_dataset_version: int | None = None,
    ) -> dict[str, Any]:
        window_size = int(window_size)
        if window_size < 1 or window_size > 100:
            raise AppError(
                "E_BAD_REQUEST",
                "window_size 必须在 1 到 100 之间",
                status_code=400,
                details={"windowSize": window_size},
            )

        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            with self.db.transaction() as connection:
                current_version = self.db.dataset_version(connection)
                if base_dataset_version is not None and base_dataset_version != current_version:
                    raise AppError(
                        "E_STALE_BASE",
                        "状态重算基础版本已过期",
                        status_code=409,
                        details={
                            "baseDatasetVersion": base_dataset_version,
                            "currentDatasetVersion": current_version,
                        },
                    )
                policies = self.db.read_policies(connection)
                if observation_batch_id:
                    observation_rows = connection.execute(
                        "SELECT * FROM observations WHERE batch_id = ? ORDER BY id", (observation_batch_id,)
                    ).fetchall()
                else:
                    observation_rows = connection.execute(
                        "SELECT * FROM observations ORDER BY id"
                    ).fetchall()
                observations = [
                    Observation(
                        slave_id=row["slave_id"],
                        sampled_at_ms=int(row["sampled_at_ms"]),
                        binlog_gap=int(row["binlog_gap"]),
                        delay_ms=int(row["delay_ms"]),
                        reported_version=str(row["reported_version"]),
                    )
                    for row in observation_rows
                ]
                deduped = deduplicate_observations(observations)
                by_slave: dict[str, list[Observation]] = {}
                for observation in deduped:
                    by_slave.setdefault(observation.slave_id, []).append(observation)
                policy_by_slave = {policy.slave_id: policy for policy in policies}
                states_before = {
                    policy.slave_id: self._read_state(connection, policy.slave_id) for policy in policies
                }
                results: list[dict[str, Any]] = []
                unmatched: list[dict[str, Any]] = []
                for policy in sorted(policies, key=lambda item: item.slave_id):
                    slave_observations = by_slave.get(policy.slave_id)
                    if not slave_observations:
                        continue
                    decision = decide_state(
                        policy,
                        slave_observations,
                        state_before=states_before[policy.slave_id],
                        window_size=window_size,
                    )
                    row = decision.as_dict()
                    row.update(
                        {
                            "replicationGroup": policy.replication_group,
                            "delayThresholdMs": policy.delay_threshold_ms,
                            "tolerableDelayMs": policy.tolerable_delay_ms,
                            "configuredAction": policy.degradation_action,
                            "priority": policy.priority,
                            "priorityBand": priority_band(policy.priority),
                        }
                    )
                    results.append(row)
                    timestamp = now_ms()
                    connection.execute(
                        """
                        INSERT INTO states(
                            slave_id, state, last_sampled_at_ms, last_observation_json,
                            effective_action, trigger_json, updated_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(slave_id) DO UPDATE SET
                            state = excluded.state,
                            last_sampled_at_ms = excluded.last_sampled_at_ms,
                            last_observation_json = excluded.last_observation_json,
                            effective_action = excluded.effective_action,
                            trigger_json = excluded.trigger_json,
                            updated_at_ms = excluded.updated_at_ms
                        """,
                        (
                            policy.slave_id,
                            decision.state_after,
                            decision.selected_observation["sampled_at_ms"]
                            if decision.selected_observation
                            else None,
                            canonical_json(decision.selected_observation)
                            if decision.selected_observation
                            else None,
                            decision.effective_action,
                            canonical_json(decision.trigger),
                            timestamp,
                        ),
                    )
                    if decision.state_before != decision.state_after:
                        connection.execute(
                            """
                            INSERT INTO state_events(
                                slave_id, from_state, to_state, reason, batch_id, created_at_ms, details_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                policy.slave_id,
                                decision.state_before,
                                decision.state_after,
                                str(decision.trigger.get("code", "AUTO_TRANSITION")),
                                active_batch_id,
                                timestamp,
                                canonical_json(row),
                            ),
                        )
                for slave_id in sorted(set(by_slave) - set(policy_by_slave)):
                    unmatched.append(
                        {
                            "slaveId": slave_id,
                            "businessCode": "E_POLICY_MISSING",
                            "message": "存在观测但无对应治理策略",
                        }
                    )

                state_version = self.db.state_version(connection) + 1
                self.db.set_meta(connection, "state_version", state_version)
                recalc_id = _new_id("recalc")
                snapshot = {
                    "window_size": window_size,
                    "policies": [policy.to_internal_dict() for policy in sorted(policies, key=lambda p: p.slave_id)],
                    "observations": [observation.to_dict() for observation in observations],
                    "states_before": states_before,
                    "observation_batch_id": observation_batch_id,
                    "dataset_version": current_version,
                }
                business_codes = sorted(
                    {row["businessCode"] for row in results if row.get("businessCode")}
                    | {row["businessCode"] for row in unmatched}
                )
                manual_review_count = sum(1 for row in results if row["requiresManualReview"])
                severity = {"DEGRADED": 3, "DELAYED": 2, "RESTORED": 1, "HEALTHY": 0}
                action_arbitration = sorted(
                    [
                        {
                            "slaveId": row["slaveId"],
                            "replicationGroup": row["replicationGroup"],
                            "state": row["stateAfter"],
                            "priority": row["priority"],
                            "priorityBand": row["priorityBand"],
                            "effectiveAction": row["effectiveAction"],
                            "triggerCode": row["trigger"].get("code"),
                        }
                        for row in results
                        if row["effectiveAction"] not in {"NONE"}
                    ],
                    key=lambda item: (item["priority"], -severity.get(item["state"], 0), item["slaveId"]),
                )
                state_counts: dict[str, int] = {state: 0 for state in STATES}
                for row in results:
                    state_counts[row["stateAfter"]] += 1
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "STATE_RECALCULATE",
                    "recalcId": recalc_id,
                    "datasetVersion": current_version,
                    "stateVersion": state_version,
                    "observationBatchId": observation_batch_id,
                    "observationsConsidered": len(observations),
                    "deduplicatedObservationCount": len(deduped),
                    "windowSize": window_size,
                    "results": results,
                    "unmatchedObservations": unmatched,
                    "counts": state_counts,
                    "manualReviewCount": manual_review_count,
                    "actionArbitration": action_arbitration,
                    "businessCodes": business_codes,
                    "businessCode": (
                        "E_VERSION_AHEAD"
                        if "E_VERSION_AHEAD" in business_codes
                        else business_codes[0]
                        if business_codes
                        else None
                    ),
                    "recalculatedAtMs": now_ms(),
                }
                connection.execute(
                    "INSERT INTO recalc_runs(recalc_id, batch_id, snapshot_json, result_json, created_at_ms) VALUES (?, ?, ?, ?, ?)",
                    (recalc_id, active_batch_id, canonical_json(snapshot), canonical_json(result), now_ms()),
                )
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="STATE_RECALCULATE",
                    result=result,
                )
                self._maybe_crash("before_commit")
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="STATE_RECALCULATE",
            details={
                "observationBatchId": observation_batch_id,
                "windowSize": window_size,
                "baseDatasetVersion": base_dataset_version,
            },
            work=work,
        )

    def transition_state(
        self,
        slave_id: str,
        target_state: str,
        *,
        batch_id: str | None = None,
        base_state_version: int | None = None,
        reason: str = "MANUAL_TRANSITION",
    ) -> dict[str, Any]:
        target = str(target_state).strip().upper()

        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            with self.db.transaction() as connection:
                current_state_version = self.db.state_version(connection)
                if base_state_version is not None and base_state_version != current_state_version:
                    raise AppError(
                        "E_STALE_BASE",
                        "状态迁移基础版本已过期",
                        status_code=409,
                        details={
                            "baseStateVersion": base_state_version,
                            "currentStateVersion": current_state_version,
                        },
                    )
                current_state = self._read_state(connection, slave_id)
                ensure_transition(current_state, target)
                policy_row = connection.execute(
                    "SELECT * FROM policies WHERE slave_id = ?", (slave_id,)
                ).fetchone()
                if policy_row is not None:
                    policy = self.db.row_to_policy(policy_row)
                    action, action_reason = effective_action(target, policy)
                else:
                    action, action_reason = (
                        "NONE" if target == HEALTHY else "MANUAL_TRANSITION",
                        "无对应策略，按手工状态迁移执行",
                    )
                next_version = current_state_version + 1
                timestamp = now_ms()
                trigger = {"code": reason, "manual": True, "targetState": target}
                connection.execute(
                    """
                    INSERT INTO states(
                        slave_id, state, last_sampled_at_ms, last_observation_json,
                        effective_action, trigger_json, updated_at_ms
                    ) VALUES (?, ?, NULL, NULL, ?, ?, ?)
                    ON CONFLICT(slave_id) DO UPDATE SET
                        state = excluded.state,
                        effective_action = excluded.effective_action,
                        trigger_json = excluded.trigger_json,
                        updated_at_ms = excluded.updated_at_ms
                    """,
                    (slave_id, target, action, canonical_json(trigger), timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO state_events(
                        slave_id, from_state, to_state, reason, batch_id, created_at_ms, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        slave_id,
                        current_state,
                        target,
                        reason,
                        active_batch_id,
                        timestamp,
                        canonical_json(trigger),
                    ),
                )
                self.db.set_meta(connection, "state_version", next_version)
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "STATE_TRANSITION",
                    "slaveId": slave_id,
                    "fromState": current_state,
                    "toState": target,
                    "effectiveAction": action,
                    "actionReason": action_reason,
                    "stateVersion": next_version,
                    "persisted": True,
                }
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="STATE_TRANSITION",
                    result=result,
                )
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="STATE_TRANSITION",
            details={"slaveId": slave_id, "targetState": target, "reason": reason},
            work=work,
        )

    # ------------------------------------------------------------------
    # Editable and replayable verdicts
    # ------------------------------------------------------------------
    @staticmethod
    def _adjudications_for(connection: sqlite3.Connection, recalc_id: str) -> dict[str, dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM adjudications WHERE recalc_id = ?", (recalc_id,)
        ).fetchall()
        return {row["slave_id"]: dict(row) for row in rows}

    @classmethod
    def _with_adjudications(cls, result: dict[str, Any], edits: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        enriched = json.loads(canonical_json(result))
        for row in enriched.get("results", []):
            edit = edits.get(row["slaveId"])
            if edit is None:
                continue
            row["computedEffectiveAction"] = row.get("effectiveAction")
            row["effectiveAction"] = edit["effective_action"]
            row["manualAdjudication"] = {
                "note": edit["note"],
                "editedAtMs": edit["edited_at_ms"],
                "baseDatasetVersion": edit["base_dataset_version"],
            }
            row["adjudicationEdited"] = True
        return enriched

    def list_verdicts(self, *, limit: int = 50) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            rows = connection.execute(
                "SELECT * FROM recalc_runs ORDER BY created_at_ms DESC, recalc_id LIMIT ?", (limit,)
            ).fetchall()
            runs = []
            for row in rows:
                result = json.loads(row["result_json"])
                edits = self._adjudications_for(connection, row["recalc_id"])
                runs.append(
                    {
                        "recalcId": row["recalc_id"],
                        "batchId": row["batch_id"],
                        "createdAtMs": row["created_at_ms"],
                        "result": self._with_adjudications(result, edits),
                    }
                )
            return {"ok": True, "runs": runs}

    def get_verdict(self, recalc_id: str) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            row = connection.execute(
                "SELECT * FROM recalc_runs WHERE recalc_id = ?", (recalc_id,)
            ).fetchone()
            if row is None:
                raise AppError("E_NOT_FOUND", f"裁决 {recalc_id} 不存在", status_code=404)
            edits = self._adjudications_for(connection, recalc_id)
            return {
                "ok": True,
                "recalcId": recalc_id,
                "batchId": row["batch_id"],
                "result": self._with_adjudications(json.loads(row["result_json"]), edits),
            }

    def edit_adjudication(
        self,
        recalc_id: str,
        slave_id: str,
        *,
        effective_action_value: str,
        note: str,
        batch_id: str | None = None,
        base_dataset_version: int | None = None,
    ) -> dict[str, Any]:
        action_value = str(effective_action_value).strip().upper()
        if not action_value:
            raise AppError("E_BAD_REQUEST", "effectiveAction 不能为空", status_code=400)

        def work(active_batch_id: str, intent_id: str) -> dict[str, Any]:
            with self.db.transaction() as connection:
                current_version = self.db.dataset_version(connection)
                if base_dataset_version is not None and base_dataset_version != current_version:
                    raise AppError(
                        "E_STALE_BASE",
                        "裁决编辑基础版本已过期",
                        status_code=409,
                        details={
                            "baseDatasetVersion": base_dataset_version,
                            "currentDatasetVersion": current_version,
                        },
                    )
                run = connection.execute(
                    "SELECT result_json FROM recalc_runs WHERE recalc_id = ?", (recalc_id,)
                ).fetchone()
                if run is None:
                    raise AppError("E_NOT_FOUND", f"裁决 {recalc_id} 不存在", status_code=404)
                known_slave = any(
                    row.get("slaveId") == slave_id
                    for row in json.loads(run["result_json"]).get("results", [])
                )
                if not known_slave:
                    raise AppError("E_NOT_FOUND", f"裁决中不存在从库 {slave_id}", status_code=404)
                timestamp = now_ms()
                connection.execute(
                    """
                    INSERT INTO adjudications(
                        recalc_id, slave_id, effective_action, note, edited_at_ms, base_dataset_version
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(recalc_id, slave_id) DO UPDATE SET
                        effective_action = excluded.effective_action,
                        note = excluded.note,
                        edited_at_ms = excluded.edited_at_ms,
                        base_dataset_version = excluded.base_dataset_version
                    """,
                    (recalc_id, slave_id, action_value, note, timestamp, base_dataset_version),
                )
                result = {
                    "ok": True,
                    "batchId": active_batch_id,
                    "operation": "ADJUDICATION_EDIT",
                    "recalcId": recalc_id,
                    "slaveId": slave_id,
                    "effectiveAction": action_value,
                    "note": note,
                    "editedAtMs": timestamp,
                    "datasetVersion": current_version,
                }
                self._finish_success(
                    connection,
                    intent_id=intent_id,
                    batch_id=active_batch_id,
                    operation="ADJUDICATION_EDIT",
                    result=result,
                )
            return result

        return self._execute_write(
            batch_id=batch_id,
            operation="ADJUDICATION_EDIT",
            details={"recalcId": recalc_id, "slaveId": slave_id, "effectiveAction": action_value},
            work=work,
        )

    def replay_verdict(self, recalc_id: str) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            run_row = connection.execute(
                "SELECT * FROM recalc_runs WHERE recalc_id = ?", (recalc_id,)
            ).fetchone()
            if run_row is None:
                raise AppError("E_NOT_FOUND", f"裁决 {recalc_id} 不存在", status_code=404)
            snapshot = json.loads(run_row["snapshot_json"])
            original = json.loads(run_row["result_json"])
            edits = self._adjudications_for(connection, recalc_id)
        replayed_rows = replay_decisions(snapshot)
        policy_map = {
            policy.slave_id: policy
            for policy in (Policy(**raw) for raw in snapshot.get("policies", []))
        }
        for row in replayed_rows:
            policy = policy_map.get(row["slaveId"])
            if policy is None:
                continue
            row.update(
                {
                    "replicationGroup": policy.replication_group,
                    "delayThresholdMs": policy.delay_threshold_ms,
                    "tolerableDelayMs": policy.tolerable_delay_ms,
                    "configuredAction": policy.degradation_action,
                    "priority": policy.priority,
                    "priorityBand": priority_band(policy.priority),
                }
            )
        deterministic_match = canonical_json(replayed_rows) == canonical_json(original.get("results", []))
        return {
            "ok": True,
            "operation": "VERDICT_REPLAY",
            "recalcId": recalc_id,
            "sourceBatchId": run_row["batch_id"],
            "replayed": True,
            "deterministicMatch": deterministic_match,
            "replayScope": "PERSISTED_RUN_SNAPSHOT_ONLY",
            "results": replayed_rows,
            "manualAdjudicationsApplied": len(edits),
        }

    # ------------------------------------------------------------------
    # Recovery and inspection
    # ------------------------------------------------------------------
    def _latest_complete_snapshot(self, connection: sqlite3.Connection) -> tuple[int, str] | None:
        rows = connection.execute(
            "SELECT version, snapshot_json FROM dataset_versions ORDER BY version DESC"
        ).fetchall()
        for row in rows:
            try:
                json.loads(row["snapshot_json"])
            except json.JSONDecodeError:
                continue
            return int(row["version"]), str(row["snapshot_json"])
        return None

    def _restore_snapshot(self, connection: sqlite3.Connection, version: int, snapshot_json: str) -> None:
        connection.execute("DELETE FROM policies")
        for raw in json.loads(snapshot_json):
            self.db.upsert_policy(connection, normalize_policy(raw))
        self.db.set_meta(connection, "dataset_version", version)

    def recover(self) -> dict[str, Any]:
        with self.writer_lock:
            with self.db.transaction() as connection:
                unfinished = connection.execute(
                    "SELECT * FROM intents WHERE status IN ('PREPARED', 'IN_PROGRESS') ORDER BY created_at_ms, intent_id"
                ).fetchall()
                aborted: list[dict[str, Any]] = []
                timestamp = now_ms()
                for intent in unfinished:
                    intent_payload = {
                        "intentId": intent["intent_id"],
                        "batchId": intent["batch_id"],
                        "operation": intent["operation"],
                        "createdAtMs": intent["created_at_ms"],
                        "status": "ABORTED",
                        "fact": "强制终止后发现的未提交意图",
                    }
                    aborted.append(intent_payload)
                    error = AppError(
                        "E_BATCH_ABORTED",
                        f"批次 {intent['batch_id']} 因进程中断被标记为已中止",
                        status_code=409,
                        details={
                            "intentId": intent["intent_id"],
                            "operation": intent["operation"],
                            "recoveredOnStartup": True,
                        },
                    )
                    connection.execute(
                        "UPDATE intents SET status = 'ABORTED', finished_at_ms = ?, error_json = ? WHERE intent_id = ?",
                        (timestamp, canonical_json({**error.payload(), "statusCode": 409}), intent["intent_id"]),
                    )
                    if intent["batch_id"]:
                        connection.execute(
                            """
                            INSERT OR IGNORE INTO applied_batches(
                                batch_id, operation, status, result_json, error_json, created_at_ms, finished_at_ms
                            ) VALUES (?, ?, 'ABORTED', ?, ?, ?, ?)
                            """,
                            (
                                intent["batch_id"],
                                intent["operation"],
                                canonical_json(error.payload()),
                                canonical_json(
                                    {
                                        "code": error.code,
                                        "message": error.message,
                                        "details": error.details,
                                        "statusCode": 409,
                                    }
                                ),
                                timestamp,
                                timestamp,
                            ),
                        )

                current_version = self.db.dataset_version(connection)
                db_snapshot = self.db.current_policy_storage_snapshot(connection)
                latest = self._latest_complete_snapshot(connection)
                restored = False
                last_complete_version: int | None = None
                integrity_note = "数据集与最后完整快照一致"
                if latest is None:
                    connection.execute(
                        "INSERT INTO dataset_versions(version, reason, created_at_ms, snapshot_json) VALUES (?, ?, ?, ?)",
                        (1, "RECOVERY_INITIAL_EMPTY", timestamp, "[]"),
                    )
                    current_version = 1
                    db_snapshot = "[]"
                    last_complete_version = 1
                    integrity_note = "无完整快照，创建空数据集基线"
                else:
                    last_complete_version, snapshot_json = latest
                    expected_snapshot = canonical_json(
                        sorted(json.loads(snapshot_json), key=lambda item: item["slave_id"])
                    )
                    if expected_snapshot != db_snapshot:
                        self._restore_snapshot(connection, last_complete_version, snapshot_json)
                        restored = True
                        current_version = last_complete_version
                        integrity_note = "检测到未覆盖的策略数据集差异，已回滚到最后完整快照"

                report = {
                    "ok": True,
                    "operation": "RECOVERY_SCAN",
                    "generatedAtMs": timestamp,
                    "scope": "PERSISTED_FACTS_ONLY",
                    "forcedTerminationExitReportAvailable": False,
                    "unfinishedIntents": aborted,
                    "abortedBatches": [item["batchId"] for item in aborted if item.get("batchId")],
                    "integrity": {
                        "currentDatasetVersion": current_version,
                        "lastCompleteDatasetVersion": last_complete_version,
                        "restoredFromSnapshot": restored,
                        "note": integrity_note,
                    },
                    "notes": [
                        "恢复报告只列出 SQLite 中已持久化的意图、批次与快照事实。",
                        "不承诺进程被强制终止时生成完整退出报告。",
                    ],
                }
                connection.execute(
                    "INSERT INTO recovery_events(created_at_ms, report_json) VALUES (?, ?)",
                    (timestamp, canonical_json(report)),
                )
            return report

    def recovery_report(self) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return {
                    "ok": True,
                    "operation": "RECOVERY_SCAN",
                    "scope": "PERSISTED_FACTS_ONLY",
                    "unfinishedIntents": [],
                    "abortedBatches": [],
                    "notes": ["尚无恢复扫描记录"],
                }
            report = json.loads(row["report_json"])
            report["latestPersistedRecoveryReport"] = True
            return report

    def list_intents(self, *, limit: int = 100) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            rows = connection.execute(
                "SELECT * FROM intents ORDER BY created_at_ms DESC, intent_id DESC LIMIT ?", (limit,)
            ).fetchall()
            return {
                "ok": True,
                "intents": [
                    {
                        "intentId": row["intent_id"],
                        "batchId": row["batch_id"],
                        "operation": row["operation"],
                        "status": row["status"],
                        "createdAtMs": row["created_at_ms"],
                        "finishedAtMs": row["finished_at_ms"],
                        "details": json.loads(row["details_json"]),
                        "error": json.loads(row["error_json"]) if row["error_json"] else None,
                    }
                    for row in rows
                ],
            }

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        self.startup()
        with self.db.reader() as connection:
            row = connection.execute(
                "SELECT * FROM applied_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise AppError("E_NOT_FOUND", f"批次 {batch_id} 不存在", status_code=404)
            return {
                "ok": True,
                "batchId": row["batch_id"],
                "operation": row["operation"],
                "status": row["status"],
                "createdAtMs": row["created_at_ms"],
                "finishedAtMs": row["finished_at_ms"],
                "result": json.loads(row["result_json"]),
                "error": json.loads(row["error_json"]) if row["error_json"] else None,
            }

    def state_machine_description(self) -> dict[str, Any]:
        return {
            "ok": True,
            "states": list(STATES),
            "legalTransitions": {state: sorted(targets) for state, targets in LEGAL_TRANSITIONS.items()},
            "windowDriven": True,
            "actionRules": {
                "HEALTHY": "NONE",
                "DELAYED": "HIGH priority (0-33) uses configured action; otherwise MONITOR",
                "DEGRADED": "configured action; NONE is raised to READ_ONLY",
                "RESTORED": "RESTORE_SERVICE",
                "VERSION_AHEAD_WITHOUT_FORWARD_COMPAT": "MANUAL_REVIEW and E_VERSION_AHEAD",
            },
            "compatibilityMatrix": COMPATIBILITY_MATRIX,
        }

