from __future__ import annotations

import json
import math
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from .crash import crash_hook
from .db import Database
from .errors import BusinessError
from .numeric import NUMERIC_CONTRACT, ShardDerivation, aggregate_derivations, derive_shard
from .samples import LoadedShard, SampleCatalog
from .util import (
    ensure_finite_number,
    json_digest,
    normalize_precision,
    parse_precision_assignments,
    require_mapping,
    sha256_hex,
    utc_now,
)


SUPPORTED_SNAPSHOT_FORMAT = 1
SUPPORTED_POLICY_FORMAT = 1


@dataclass(frozen=True)
class PolicyView:
    version: int
    loss_scale: float
    loss_scale_initial: float
    growth_factor: float
    assignments: dict[str, str]
    origin: str
    base_version: int | None
    commit_id: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "loss_scale": self.loss_scale,
            "loss_scale_initial": self.loss_scale_initial,
            "growth_factor": self.growth_factor,
            "shards": dict(sorted(self.assignments.items())),
            "origin": self.origin,
            "base_version": self.base_version,
            "commit_id": self.commit_id,
        }


@dataclass
class SnapshotResource:
    policy_version: int
    snapshot_seq: int
    path: str
    released: bool = False

    def release(self) -> None:
        self.released = True


@dataclass
class PreparedShard:
    loaded: LoadedShard
    requested_precision: str | None


@dataclass
class PreparedCheckpoint:
    commit_id: str
    request_hash: str
    round_number: int
    scheduler_position: dict[str, Any]
    data_cursor: dict[str, Any]
    requested_loss_scale: float | None
    requested_policy_version: int | None
    shards: list[PreparedShard]


class CheckpointService:
    """Single-writer checkpoint/policy coordinator backed entirely by SQLite."""

    def __init__(
        self,
        db_path: str | Path,
        samples_dir: str | Path,
        *,
        recover_on_start: bool = True,
    ) -> None:
        self.db = Database(db_path)
        self.catalog = SampleCatalog(samples_dir)
        self._snapshot_lock = threading.RLock()
        self._snapshot_resources: dict[int, SnapshotResource] = {}
        self._last_recovery_report: dict[str, Any] | None = None
        if recover_on_start:
            self._last_recovery_report = self.recover_pending()

    # ------------------------------------------------------------------
    # Policy and operation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _require_commit_id(body: dict[str, Any]) -> str:
        commit_id = body.get("commit_id")
        if not isinstance(commit_id, str) or not commit_id.strip():
            raise BusinessError("E_INVALID_REQUEST", "commit_id is required")
        return commit_id.strip()

    @staticmethod
    def _request_hash(kind: str, body: dict[str, Any]) -> str:
        return json_digest({"kind": kind, "body": body})

    def _lookup_operation(
        self,
        conn: sqlite3.Connection,
        *,
        commit_id: str,
        kind: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT kind, request_hash, response_json FROM commit_operations WHERE commit_id = ?",
            (commit_id,),
        ).fetchone()
        if row is None:
            return None
        if row["kind"] != kind or row["request_hash"] != request_hash:
            raise BusinessError(
                "E_CONFLICTING_COMMIT",
                "commit_id was already used with different content",
                details={"commit_id": commit_id, "existing_kind": row["kind"]},
            )
        return json.loads(row["response_json"])

    def _read_existing_operation(
        self, *, commit_id: str, kind: str, request_hash: str
    ) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            return self._lookup_operation(
                conn, commit_id=commit_id, kind=kind, request_hash=request_hash
            )

    def _create_intent(
        self,
        *,
        kind: str,
        target_id: str,
        request_hash: str,
        payload: dict[str, Any],
    ) -> str:
        intent_id = f"intent-{uuid.uuid4().hex}"
        with self.db.write_transaction() as conn:
            conn.execute(
                """
                INSERT INTO intents(intent_id, kind, target_id, request_hash, payload_json, status, started_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    intent_id,
                    kind,
                    target_id,
                    request_hash,
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                    utc_now(),
                ),
            )
        crash_hook("after_intent")
        return intent_id

    def _finish_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        *,
        resolution: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            "UPDATE intents SET status='completed', finished_at=?, resolution_json=? WHERE intent_id=?",
            (
                utc_now(),
                json.dumps(resolution or {}, sort_keys=True, separators=(",", ":")),
                intent_id,
            ),
        )

    def _abort_intent(self, intent_id: str, exc: BaseException) -> None:
        detail = {
            "type": type(exc).__name__,
            "message": str(exc),
            "code": getattr(exc, "code", None),
        }
        try:
            with self.db.write_transaction() as conn:
                row = conn.execute("SELECT status FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
                if row is not None and row["status"] == "pending":
                    conn.execute(
                        "UPDATE intents SET status='aborted', finished_at=?, error_json=? WHERE intent_id=?",
                        (
                            utc_now(),
                            json.dumps(detail, sort_keys=True, separators=(",", ":")),
                            intent_id,
                        ),
                    )
        except Exception:
            # A second failure must not hide the original business error.
            pass

    def _record_operation(
        self,
        conn: sqlite3.Connection,
        *,
        commit_id: str,
        kind: str,
        request_hash: str,
        response: dict[str, Any],
    ) -> None:
        now = utc_now()
        conn.execute(
            """
            INSERT INTO commit_operations(commit_id, kind, request_hash, response_json, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                commit_id,
                kind,
                request_hash,
                json.dumps(response, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                now,
                now,
            ),
        )

    def _load_policy(self, conn: sqlite3.Connection, version: int) -> PolicyView:
        policy_row = conn.execute(
            "SELECT * FROM policies WHERE policy_version=?", (version,)
        ).fetchone()
        if policy_row is None:
            raise BusinessError(
                "E_NOT_FOUND",
                "policy version does not exist",
                details={"policy_version": version},
            )
        assignments = {
            row["shard_id"]: row["precision"]
            for row in conn.execute(
                "SELECT shard_id, precision FROM policy_precisions WHERE policy_version=?",
                (version,),
            ).fetchall()
        }
        return PolicyView(
            version=int(policy_row["policy_version"]),
            loss_scale=float(policy_row["loss_scale"]),
            loss_scale_initial=float(policy_row["loss_scale_initial"]),
            growth_factor=float(policy_row["growth_factor"]),
            assignments=assignments,
            origin=str(policy_row["origin"]),
            base_version=policy_row["base_version"],
            commit_id=policy_row["commit_id"],
        )

    def _current_policy(self, conn: sqlite3.Connection) -> PolicyView | None:
        state = conn.execute("SELECT current_version FROM policy_state WHERE singleton=1").fetchone()
        if state is None or int(state["current_version"]) == 0:
            return None
        return self._load_policy(conn, int(state["current_version"]))

    def _require_policy(self, conn: sqlite3.Connection) -> tuple[int, PolicyView]:
        state = conn.execute("SELECT current_version FROM policy_state WHERE singleton=1").fetchone()
        if state is None or int(state["current_version"]) == 0:
            raise BusinessError("E_POLICY_REQUIRED", "submit a precision policy before a checkpoint")
        version = int(state["current_version"])
        return version, self._load_policy(conn, version)

    def _validate_complete_assignments(self, assignments: dict[str, str]) -> None:
        formal = set(self.catalog.formal_shard_ids)
        unknown = sorted(set(assignments) - set(self.catalog.shards))
        non_formal = sorted(set(assignments) - formal)
        missing = sorted(formal - set(assignments))
        if unknown or non_formal:
            raise BusinessError(
                "E_UNKNOWN_SHARD",
                "policy references non-formal or unknown shards",
                details={"shard_ids": sorted(set(unknown) | set(non_formal))},
            )
        if missing:
            raise BusinessError(
                "E_INVALID_POLICY",
                "a full policy snapshot must assign every formal shard",
                details={"missing_shard_ids": missing},
            )
        if set(assignments) != formal:
            raise BusinessError("E_INVALID_POLICY", "policy is not a complete formal assignment")

    def _parse_policy_fields(
        self,
        body: dict[str, Any],
        *,
        base: PolicyView | None = None,
        partial: bool,
    ) -> tuple[float, float, float]:
        loss_scaling = body.get("loss_scaling")
        if loss_scaling is not None and not isinstance(loss_scaling, dict):
            raise BusinessError("E_INVALID_LOSS_SCALE", "loss_scaling must be an object")
        nested = loss_scaling or {}

        def pick(*names: str, default: Any = None) -> Any:
            for name in names:
                if name in body and body[name] is not None:
                    return body[name]
            for name in names:
                if name in nested and nested[name] is not None:
                    return nested[name]
            return default

        raw_loss = pick("loss_scale", "current_loss_scale", "current")
        if raw_loss is None:
            if base is None:
                raw_loss = pick("initial_loss_scale", "loss_scale_initial", "initial")
                if raw_loss is None:
                    raise BusinessError("E_INVALID_REQUEST", "loss_scale is required")
                loss_scale = ensure_finite_number(raw_loss, "loss_scale")
            else:
                loss_scale = base.loss_scale
        else:
            loss_scale = ensure_finite_number(raw_loss, "loss_scale")
        if loss_scale <= 0:
            raise BusinessError("E_INVALID_LOSS_SCALE", "loss_scale must be positive")

        raw_initial = pick("loss_scale_initial", "initial_loss_scale", "initial")
        if raw_initial is None:
            loss_scale_initial = base.loss_scale_initial if base is not None else loss_scale
        else:
            loss_scale_initial = ensure_finite_number(raw_initial, "loss_scale_initial")
        if loss_scale_initial <= 0:
            raise BusinessError("E_INVALID_LOSS_SCALE", "loss_scale_initial must be positive")

        raw_growth = pick("growth_factor", "growth")
        if raw_growth is None:
            growth_factor = base.growth_factor if base is not None else 2.0
        else:
            growth_factor = ensure_finite_number(raw_growth, "growth_factor")
        if growth_factor < 1.0:
            raise BusinessError("E_INVALID_LOSS_SCALE", "growth_factor must be at least 1.0")

        if partial:
            # A partial increment only needs to carry the fields it changes; the
            # base values above make the resulting snapshot complete anyway.
            return loss_scale, loss_scale_initial, growth_factor
        return loss_scale, loss_scale_initial, growth_factor

    def _insert_policy(
        self,
        conn: sqlite3.Connection,
        *,
        version: int,
        origin: str,
        base_version: int | None,
        loss_scale: float,
        loss_scale_initial: float,
        growth_factor: float,
        assignments: dict[str, str],
        commit_id: str,
        request_hash: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO policies(
                policy_version, format_version, origin, base_version, loss_scale,
                loss_scale_initial, growth_factor, commit_id, request_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version,
                SUPPORTED_POLICY_FORMAT,
                origin,
                base_version,
                loss_scale,
                loss_scale_initial,
                growth_factor,
                commit_id,
                request_hash,
                utc_now(),
            ),
        )
        conn.executemany(
            "INSERT INTO policy_precisions(policy_version, shard_id, precision) VALUES (?, ?, ?)",
            [(version, shard_id, precision) for shard_id, precision in sorted(assignments.items())],
        )

    @staticmethod
    def _policy_response(policy: PolicyView) -> dict[str, Any]:
        return policy.as_dict()

    # ------------------------------------------------------------------
    # Policy submission
    # ------------------------------------------------------------------
    def submit_policy_snapshot(self, body: dict[str, Any]) -> dict[str, Any]:
        require_mapping(body, "body")
        commit_id = self._require_commit_id(body)
        kind = "policy_snapshot"
        request_hash = self._request_hash(kind, body)
        existing = self._read_existing_operation(
            commit_id=commit_id, kind=kind, request_hash=request_hash
        )
        if existing is not None:
            return existing

        raw_version = body.get(
            "version", body.get("policy_version", body.get("snapshot_version"))
        )
        if isinstance(raw_version, bool) or not isinstance(raw_version, int):
            raise BusinessError("E_INVALID_REQUEST", "version must be an integer")
        version = int(raw_version)
        format_version = body.get("format_version", SUPPORTED_POLICY_FORMAT)
        if format_version != SUPPORTED_POLICY_FORMAT:
            raise BusinessError(
                "E_SNAPSHOT_VERSION",
                "unsupported policy snapshot format version",
                details={"received": format_version, "supported": SUPPORTED_SNAPSHOT_FORMAT},
            )
        raw_assignments = body.get(
            "shards",
            body.get(
                "shard_precisions",
                body.get("assignments", body.get("precision_assignments")),
            ),
        )
        if raw_assignments is None:
            raise BusinessError("E_INVALID_POLICY", "shards precision assignments are required")
        assignments = parse_precision_assignments(raw_assignments)
        self._validate_complete_assignments(assignments)
        loss_scale, loss_scale_initial, growth_factor = self._parse_policy_fields(
            body, partial=False
        )

        intent_id = self._create_intent(
            kind=kind,
            target_id=commit_id,
            request_hash=request_hash,
            payload={
                "version": version,
                "format_version": format_version,
                "assignments": assignments,
                "loss_scale": loss_scale,
                "loss_scale_initial": loss_scale_initial,
                "growth_factor": growth_factor,
            },
        )
        try:
            with self.db.write_transaction() as conn:
                reused = self._lookup_operation(
                    conn, commit_id=commit_id, kind=kind, request_hash=request_hash
                )
                if reused is not None:
                    self._finish_intent(conn, intent_id, resolution={"reused": True})
                    response = reused
                else:
                    current = self._current_policy(conn)
                    current_version = current.version if current is not None else 0
                    if version <= current_version:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "policy version must be greater than the current version",
                            details={"version": version, "current_version": current_version},
                        )
                    if version <= 0:
                        raise BusinessError("E_INVALID_REQUEST", "version must be positive")
                    self._insert_policy(
                        conn,
                        version=version,
                        origin="full",
                        base_version=current_version or None,
                        loss_scale=loss_scale,
                        loss_scale_initial=loss_scale_initial,
                        growth_factor=growth_factor,
                        assignments=assignments,
                        commit_id=commit_id,
                        request_hash=request_hash,
                    )
                    invalidation = self._derive_for_policy_locked(
                        conn,
                        new_policy=self._load_policy(conn, version),
                        old_policy_version=current_version or None,
                        intent_id=intent_id,
                    )
                    conn.execute(
                        "UPDATE policy_state SET current_version=?, last_completed_intent_id=?, updated_at=? WHERE singleton=1",
                        (version, intent_id, utc_now()),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_completed_intent', ?)",
                        (intent_id,),
                    )
                    policy = self._load_policy(conn, version)
                    response = {
                        "commit_id": commit_id,
                        "policy": self._policy_response(policy),
                        "replaced_policy_version": current_version or None,
                        "derived_rebuild": invalidation,
                        "intent_id": intent_id,
                    }
                    self._record_operation(
                        conn,
                        commit_id=commit_id,
                        kind=kind,
                        request_hash=request_hash,
                        response=response,
                    )
                    self._finish_intent(conn, intent_id, resolution={"version": version})
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise
        return response

    def submit_policy_increment(self, body: dict[str, Any]) -> dict[str, Any]:
        require_mapping(body, "body")
        commit_id = self._require_commit_id(body)
        kind = "policy_increment"
        request_hash = self._request_hash(kind, body)
        existing = self._read_existing_operation(
            commit_id=commit_id, kind=kind, request_hash=request_hash
        )
        if existing is not None:
            return existing

        base_version = body.get("base_version", body.get("base_policy_version"))
        version = body.get("version", body.get("policy_version", body.get("new_version")))
        if isinstance(base_version, bool) or not isinstance(base_version, int):
            raise BusinessError("E_INVALID_REQUEST", "base_version must be an integer")
        if isinstance(version, bool) or not isinstance(version, int):
            raise BusinessError("E_INVALID_REQUEST", "version must be an integer")
        affected = body.get(
            "shards",
            body.get(
                "affected_shards",
                body.get("shard_precisions", body.get("precision_updates", body.get("updates"))),
            ),
        )
        has_loss_change = any(
            key in body
            for key in (
                "loss_scale",
                "current_loss_scale",
                "loss_scale_initial",
                "initial_loss_scale",
                "growth_factor",
                "loss_scaling",
            )
        )
        if affected is None:
            if not has_loss_change:
                raise BusinessError(
                    "E_INVALID_POLICY", "an increment must affect shards or loss-scale fields"
                )
            affected_assignments: dict[str, str] = {}
        else:
            affected_assignments = parse_precision_assignments(affected)
        unknown = sorted(set(affected_assignments) - set(self.catalog.shards))
        if unknown:
            raise BusinessError(
                "E_UNKNOWN_SHARD", "increment references unknown shards", details={"shard_ids": unknown}
            )
        if not affected_assignments and not has_loss_change:
            raise BusinessError("E_INVALID_POLICY", "increment does not affect any state")

        # Read-only preflight gives a deterministic E_STALE_BASE/E_STALE_SNAPSHOT
        # before creating an intent. The transaction repeats every check.
        with self.db.read_transaction() as conn:
            current = self._current_policy(conn)
            if current is None:
                raise BusinessError("E_POLICY_REQUIRED", "submit a full policy first")
            if base_version != current.version:
                raise BusinessError(
                    "E_STALE_BASE",
                    "increment base_version is not the current policy version",
                    details={"base_version": base_version, "current_version": current.version},
                )
            if version <= current.version:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "increment version must be greater than the current version",
                    details={"version": version, "current_version": current.version},
                )
            preflight_loss = self._parse_policy_fields(body, base=current, partial=True)
            preflight_assignments = dict(current.assignments)
            preflight_assignments.update(affected_assignments)
            self._validate_complete_assignments(preflight_assignments)

        intent_id = self._create_intent(
            kind=kind,
            target_id=commit_id,
            request_hash=request_hash,
            payload={
                "base_version": base_version,
                "version": version,
                "affected_shards": affected_assignments,
                "loss_scale": preflight_loss[0],
                "loss_scale_initial": preflight_loss[1],
                "growth_factor": preflight_loss[2],
            },
        )
        try:
            with self.db.write_transaction() as conn:
                reused = self._lookup_operation(
                    conn, commit_id=commit_id, kind=kind, request_hash=request_hash
                )
                if reused is not None:
                    self._finish_intent(conn, intent_id, resolution={"reused": True})
                    response = reused
                else:
                    current = self._current_policy(conn)
                    if current is None:
                        raise BusinessError("E_POLICY_REQUIRED", "submit a full policy first")
                    if base_version != current.version:
                        raise BusinessError(
                            "E_STALE_BASE",
                            "increment base_version is not the current policy version",
                            details={"base_version": base_version, "current_version": current.version},
                        )
                    if version <= current.version:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "increment version must be greater than the current version",
                            details={"version": version, "current_version": current.version},
                        )
                    if version <= 0:
                        raise BusinessError("E_INVALID_REQUEST", "version must be positive")
                    loss_scale, loss_scale_initial, growth_factor = self._parse_policy_fields(
                        body, base=current, partial=True
                    )
                    merged = dict(current.assignments)
                    merged.update(affected_assignments)
                    self._validate_complete_assignments(merged)
                    self._insert_policy(
                        conn,
                        version=version,
                        origin="increment",
                        base_version=base_version,
                        loss_scale=loss_scale,
                        loss_scale_initial=loss_scale_initial,
                        growth_factor=growth_factor,
                        assignments=merged,
                        commit_id=commit_id,
                        request_hash=request_hash,
                    )
                    changed_precisions = sorted(
                        shard_id
                        for shard_id, precision in affected_assignments.items()
                        if current.assignments.get(shard_id) != precision
                    )
                    changed_loss_scale = any(
                        not math.isclose(left, right, rel_tol=0.0, abs_tol=0.0)
                        for left, right in (
                            (loss_scale, current.loss_scale),
                            (loss_scale_initial, current.loss_scale_initial),
                            (growth_factor, current.growth_factor),
                        )
                    )
                    invalidation = self._derive_for_policy_locked(
                        conn,
                        new_policy=self._load_policy(conn, version),
                        old_policy_version=current.version,
                        intent_id=intent_id,
                    )
                    conn.execute(
                        "UPDATE policy_state SET current_version=?, last_completed_intent_id=?, updated_at=? WHERE singleton=1",
                        (version, intent_id, utc_now()),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_completed_intent', ?)",
                        (intent_id,),
                    )
                    policy = self._load_policy(conn, version)
                    response = {
                        "commit_id": commit_id,
                        "policy": self._policy_response(policy),
                        "base_version": base_version,
                        "affected_shards": sorted(affected_assignments),
                        "changed_shards": invalidation["changed_shards"],
                        "changed_precisions": changed_precisions,
                        "loss_scale_changed": changed_loss_scale,
                        "derived_rebuild": invalidation,
                        "intent_id": intent_id,
                    }
                    self._record_operation(
                        conn,
                        commit_id=commit_id,
                        kind=kind,
                        request_hash=request_hash,
                        response=response,
                    )
                    self._finish_intent(conn, intent_id, resolution={"version": version})
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise
        return response

    def load_policy_snapshot_file(
        self,
        path: str | Path,
        *,
        commit_id: str | None = None,
    ) -> dict[str, Any]:
        snapshot_path = Path(path).resolve()
        try:
            raw = snapshot_path.read_bytes()
        except OSError as exc:
            raise BusinessError(
                "E_INVALID_SNAPSHOT",
                "snapshot file cannot be read",
                details={"path": str(snapshot_path)},
            ) from exc
        try:
            snapshot = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot file is not valid JSON") from exc
        if not isinstance(snapshot, dict):
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot root must be an object")
        format_version = snapshot.get("format_version")
        if format_version != SUPPORTED_SNAPSHOT_FORMAT:
            raise BusinessError(
                "E_SNAPSHOT_VERSION",
                "unsupported snapshot file format version",
                details={"received": format_version, "supported": SUPPORTED_SNAPSHOT_FORMAT},
            )
        snapshot_seq = snapshot.get("snapshot_seq", snapshot.get("sequence"))
        if isinstance(snapshot_seq, bool) or not isinstance(snapshot_seq, int) or snapshot_seq <= 0:
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot_seq must be a positive integer")
        generated_at = snapshot.get("generated_at", snapshot.get("created_at"))
        if not isinstance(generated_at, str):
            raise BusinessError("E_INVALID_SNAPSHOT", "generated_at must be an ISO timestamp")
        try:
            datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BusinessError("E_INVALID_SNAPSHOT", "generated_at is not an ISO timestamp") from exc
        policy_data = snapshot.get("policy")
        if policy_data is None:
            policy_data = {
                key: snapshot.get(key)
                for key in (
                    "version",
                    "policy_version",
                    "loss_scale",
                    "loss_scale_initial",
                    "initial_loss_scale",
                    "growth_factor",
                    "loss_scaling",
                    "shards",
                    "shard_precisions",
                    "assignments",
                    "precision_assignments",
                )
                if key in snapshot
            }
        if not isinstance(policy_data, dict):
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot.policy must be an object")
        raw_version = policy_data.get("version", policy_data.get("policy_version"))
        if isinstance(raw_version, bool) or not isinstance(raw_version, int):
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot policy version must be an integer")
        version = int(raw_version)
        raw_assignments = policy_data.get(
            "shards",
            policy_data.get(
                "shard_precisions",
                policy_data.get("assignments", policy_data.get("precision_assignments")),
            ),
        )
        if raw_assignments is None:
            raise BusinessError("E_INVALID_SNAPSHOT", "snapshot has no precision assignments")
        assignments = parse_precision_assignments(raw_assignments)
        self._validate_complete_assignments(assignments)
        loss_scale, loss_scale_initial, growth_factor = self._parse_policy_fields(
            policy_data, partial=False
        )
        file_sha256 = hashlib_sha256(raw)
        deterministic_commit = commit_id or f"snapshot-file-{snapshot_seq}"
        body_for_hash = {
            "snapshot_seq": snapshot_seq,
            "format_version": format_version,
            "generated_at": generated_at,
            "policy": policy_data,
            "file_sha256": file_sha256,
        }
        kind = "policy_snapshot_file"
        request_hash = self._request_hash(kind, body_for_hash)
        existing = self._read_existing_operation(
            commit_id=deterministic_commit, kind=kind, request_hash=request_hash
        )
        if existing is not None:
            return existing
        with self.db.read_transaction() as conn:
            state = conn.execute(
                "SELECT current_version, last_snapshot_seq FROM policy_state WHERE singleton=1"
            ).fetchone()
            current_version = int(state["current_version"]) if state else 0
            last_snapshot_seq = int(state["last_snapshot_seq"]) if state else 0
            if snapshot_seq <= last_snapshot_seq:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "snapshot_seq is not monotonic",
                    details={"snapshot_seq": snapshot_seq, "last_snapshot_seq": last_snapshot_seq},
                )
            if version <= current_version:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "snapshot policy version must be greater than the current version",
                    details={"version": version, "current_version": current_version},
                )

        intent_id = self._create_intent(
            kind=kind,
            target_id=deterministic_commit,
            request_hash=request_hash,
            payload={
                "path": str(snapshot_path),
                "snapshot_seq": snapshot_seq,
                "policy_version": version,
                "file_sha256": file_sha256,
            },
        )
        old_resource: SnapshotResource | None = None
        try:
            with self.db.write_transaction() as conn:
                reused = self._lookup_operation(
                    conn, commit_id=deterministic_commit, kind=kind, request_hash=request_hash
                )
                if reused is not None:
                    self._finish_intent(conn, intent_id, resolution={"reused": True})
                    response = reused
                else:
                    state = conn.execute(
                        "SELECT current_version, last_snapshot_seq FROM policy_state WHERE singleton=1"
                    ).fetchone()
                    current_version = int(state["current_version"]) if state else 0
                    last_snapshot_seq = int(state["last_snapshot_seq"]) if state else 0
                    if snapshot_seq <= last_snapshot_seq:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "snapshot_seq is not monotonic",
                            details={
                                "snapshot_seq": snapshot_seq,
                                "last_snapshot_seq": last_snapshot_seq,
                            },
                        )
                    if version <= current_version:
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "snapshot policy version must be greater than the current version",
                            details={"version": version, "current_version": current_version},
                        )
                    self._insert_policy(
                        conn,
                        version=version,
                        origin="file",
                        base_version=current_version or None,
                        loss_scale=loss_scale,
                        loss_scale_initial=loss_scale_initial,
                        growth_factor=growth_factor,
                        assignments=assignments,
                        commit_id=deterministic_commit,
                        request_hash=request_hash,
                    )
                    conn.execute(
                        """
                        INSERT INTO policy_snapshot_files(
                            snapshot_seq, policy_version, format_version, generated_at, path,
                            file_sha256, loaded_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_seq,
                            version,
                            format_version,
                            generated_at,
                            str(snapshot_path),
                            file_sha256,
                            utc_now(),
                        ),
                    )
                    invalidation = self._derive_for_policy_locked(
                        conn,
                        new_policy=self._load_policy(conn, version),
                        old_policy_version=current_version or None,
                        intent_id=intent_id,
                    )
                    conn.execute(
                        """
                        UPDATE policy_state
                        SET current_version=?, last_snapshot_seq=?, last_completed_intent_id=?, updated_at=?
                        WHERE singleton=1
                        """,
                        (version, snapshot_seq, intent_id, utc_now()),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_completed_intent', ?)",
                        (intent_id,),
                    )
                    policy = self._load_policy(conn, version)
                    response = {
                        "commit_id": deterministic_commit,
                        "policy": self._policy_response(policy),
                        "snapshot_file": {
                            "path": str(snapshot_path),
                            "snapshot_seq": snapshot_seq,
                            "format_version": format_version,
                            "generated_at": generated_at,
                            "file_sha256": file_sha256,
                        },
                        "replaced_policy_version": current_version or None,
                        "derived_rebuild": invalidation,
                        "intent_id": intent_id,
                    }
                    self._record_operation(
                        conn,
                        commit_id=deterministic_commit,
                        kind=kind,
                        request_hash=request_hash,
                        response=response,
                    )
                    self._finish_intent(
                        conn,
                        intent_id,
                        resolution={"version": version, "snapshot_seq": snapshot_seq},
                    )
                    with self._snapshot_lock:
                        old_resource = self._snapshot_resources.get(current_version)
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise

        # The SQLite switch has committed. Only now is the previous in-memory
        # snapshot resource released, so a failed load always leaves it intact.
        released: list[int] = []
        with self._snapshot_lock:
            if old_resource is not None:
                old_resource.release()
                self._snapshot_resources.pop(old_resource.policy_version, None)
                released.append(old_resource.snapshot_seq)
            self._snapshot_resources[version] = SnapshotResource(
                policy_version=version,
                snapshot_seq=snapshot_seq,
                path=str(snapshot_path),
            )
        response["released_snapshot_sequences"] = released
        return response

    # ------------------------------------------------------------------
    # Checkpoint submission
    # ------------------------------------------------------------------
    def _parse_checkpoint_shards(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        raw = body.get("shards", body.get("shard_ids"))
        if raw is None:
            return [{"shard_id": shard_id} for shard_id in self.catalog.formal_shard_ids]
        entries: list[dict[str, Any]] = []
        if isinstance(raw, dict):
            for shard_id, spec in raw.items():
                item: dict[str, Any] = {"shard_id": shard_id}
                if isinstance(spec, str):
                    item["precision"] = spec
                elif spec is not None:
                    if not isinstance(spec, dict):
                        raise BusinessError("E_INVALID_REQUEST", "shard specs must be objects")
                    item.update(spec)
                    item["shard_id"] = shard_id
                entries.append(item)
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    entries.append({"shard_id": item})
                elif isinstance(item, dict):
                    entries.append(dict(item))
                else:
                    raise BusinessError("E_INVALID_REQUEST", "shards entries must be strings or objects")
        else:
            raise BusinessError("E_INVALID_REQUEST", "shards must be a list or object")
        return entries

    @staticmethod
    def _digest_expectation(entry: dict[str, Any], kind: str) -> str | None:
        aliases = {
            "fp16": (
                "expected_fp16_digest",
                "fp16_digest",
                "fp16_sha256",
                "expected_fp16_sha256",
                "fp16_content_digest",
                "fp16_content_sha256",
            ),
            "master": (
                "expected_fp32_master_digest",
                "fp32_master_digest",
                "master_digest",
                "fp32_digest",
                "expected_master_digest",
                "fp32_master_sha256",
                "master_sha256",
                "fp32_content_digest",
            ),
            "momentum": (
                "expected_momentum_digest",
                "momentum_digest",
                "momentum_sha256",
                "expected_momentum_sha256",
                "momentum_content_digest",
            ),
        }
        for key in aliases[kind]:
            value = entry.get(key)
            if value is not None:
                return str(value)
        nested = entry.get("expected_digests", entry.get("checksums"))
        if isinstance(nested, dict):
            nested_aliases = {
                "fp16": ("fp16", "fp16_sha256", "content"),
                "master": ("fp32_master", "master", "fp32", "master_sha256"),
                "momentum": ("momentum", "momentum_sha256"),
            }
            for key in nested_aliases[kind]:
                if nested.get(key) is not None:
                    return str(nested[key])
        return None

    def _prepare_checkpoint(self, body: dict[str, Any]) -> PreparedCheckpoint:
        require_mapping(body, "body")
        commit_id = self._require_commit_id(body)
        kind = "checkpoint"
        request_hash = self._request_hash(kind, body)
        existing = self._read_existing_operation(
            commit_id=commit_id, kind=kind, request_hash=request_hash
        )
        if existing is not None:
            raise _ExistingOperation(existing)

        round_number = body.get("round", body.get("round_number", body.get("epoch")))
        if isinstance(round_number, bool) or not isinstance(round_number, int) or round_number < 0:
            raise BusinessError("E_INVALID_REQUEST", "round must be a non-negative integer")
        scheduler = body.get(
            "scheduler_position",
            body.get("scheduler", body.get("lr_scheduler_position", body.get("lr_scheduler"))),
        )
        if not isinstance(scheduler, dict):
            raise BusinessError("E_INVALID_REQUEST", "scheduler_position must be an object")
        cursor = body.get(
            "data_cursor",
            body.get("data_loader_cursor", body.get("dataloader_cursor", body.get("cursor"))),
        )
        if not isinstance(cursor, dict):
            raise BusinessError("E_INVALID_REQUEST", "data_cursor must be an object")
        requested_loss_scale = None
        if body.get("loss_scale") is not None:
            requested_loss_scale = ensure_finite_number(body["loss_scale"], "loss_scale")
            if requested_loss_scale <= 0:
                raise BusinessError("E_INVALID_LOSS_SCALE", "loss_scale must be positive")
        requested_policy_version = body.get("policy_version")
        if requested_policy_version is not None:
            if isinstance(requested_policy_version, bool) or not isinstance(requested_policy_version, int):
                raise BusinessError("E_INVALID_REQUEST", "policy_version must be an integer")

        shard_entries = self._parse_checkpoint_shards(body)
        if not shard_entries:
            raise BusinessError("E_INVALID_REQUEST", "checkpoint must contain at least one shard")
        seen: set[str] = set()
        prepared: list[PreparedShard] = []
        for entry in shard_entries:
            shard_id = entry.get("shard_id", entry.get("id"))
            if not isinstance(shard_id, str) or not shard_id:
                raise BusinessError("E_INVALID_REQUEST", "each shard requires shard_id")
            if shard_id in seen:
                raise BusinessError(
                    "E_INVALID_REQUEST", "duplicate shard in checkpoint", details={"shard_id": shard_id}
                )
            seen.add(shard_id)
            loaded = self.catalog.load_shard(shard_id)
            requested_precision = (
                normalize_precision(entry["precision"]) if entry.get("precision") is not None else None
            )
            expected_pairs = (
                ("fp16", loaded.fp16_digest),
                ("master", loaded.master_digest),
                ("momentum", loaded.momentum_digest),
            )
            for kind_name, actual_digest in expected_pairs:
                expected = self._digest_expectation(entry, kind_name)
                if expected is not None and expected != actual_digest:
                    raise BusinessError(
                        "E_CHECKSUM_MISMATCH",
                        f"{kind_name} checksum mismatch; checkpoint was not submitted",
                        details={
                            "shard_id": shard_id,
                            "digest_kind": kind_name,
                            "expected": expected,
                            "actual": actual_digest,
                        },
                    )
            prepared.append(
                PreparedShard(loaded=loaded, requested_precision=requested_precision)
            )
        return PreparedCheckpoint(
            commit_id=commit_id,
            request_hash=request_hash,
            round_number=int(round_number),
            scheduler_position=scheduler,
            data_cursor=cursor,
            requested_loss_scale=requested_loss_scale,
            requested_policy_version=requested_policy_version,
            shards=prepared,
        )

    def submit_checkpoint(self, body: dict[str, Any]) -> dict[str, Any]:
        try:
            prepared = self._prepare_checkpoint(body)
        except _ExistingOperation as existing:
            return existing.response

        intent_id = self._create_intent(
            kind="checkpoint",
            target_id=prepared.commit_id,
            request_hash=prepared.request_hash,
            payload={
                "round": prepared.round_number,
                "shard_ids": [item.loaded.shard_id for item in prepared.shards],
            },
        )
        try:
            with self.db.write_transaction() as conn:
                reused = self._lookup_operation(
                    conn,
                    commit_id=prepared.commit_id,
                    kind="checkpoint",
                    request_hash=prepared.request_hash,
                )
                if reused is not None:
                    self._finish_intent(conn, intent_id, resolution={"reused": True})
                    response = reused
                else:
                    current_version, policy = self._require_policy(conn)
                    if (
                        prepared.requested_policy_version is not None
                        and prepared.requested_policy_version != current_version
                    ):
                        raise BusinessError(
                            "E_STALE_BASE",
                            "checkpoint references a non-current policy version",
                            details={
                                "policy_version": prepared.requested_policy_version,
                                "current_version": current_version,
                            },
                        )
                    if prepared.requested_loss_scale is not None and not math.isclose(
                        prepared.requested_loss_scale,
                        policy.loss_scale,
                        rel_tol=0.0,
                        abs_tol=0.0,
                    ):
                        raise BusinessError(
                            "E_LOSS_SCALE_MISMATCH",
                            "checkpoint loss_scale differs from the active policy",
                            details={
                                "checkpoint_loss_scale": prepared.requested_loss_scale,
                                "policy_loss_scale": policy.loss_scale,
                            },
                        )
                    for item in prepared.shards:
                        shard_id = item.loaded.shard_id
                        if shard_id not in policy.assignments:
                            raise BusinessError(
                                "E_INVALID_POLICY",
                                "checkpoint shard has no active precision assignment",
                                details={"shard_id": shard_id},
                            )
                        if (
                            item.requested_precision is not None
                            and item.requested_precision != policy.assignments[shard_id]
                        ):
                            raise BusinessError(
                                "E_INVALID_PRECISION",
                                "checkpoint precision conflicts with the active policy",
                                details={
                                    "shard_id": shard_id,
                                    "requested": item.requested_precision,
                                    "active": policy.assignments[shard_id],
                                },
                            )

                    state = conn.execute(
                        "SELECT latest_seq FROM checkpoint_state WHERE singleton=1"
                    ).fetchone()
                    next_seq = int(state["latest_seq"]) + 1
                    manifest_shards = []
                    for item in prepared.shards:
                        shard = item.loaded
                        manifest_shards.append(
                            {
                                "shard_id": shard.shard_id,
                                "shape": list(shard.master.shape),
                                "numel": int(shard.master.size),
                                "fp16_digest": shard.fp16_digest,
                                "master_digest": shard.master_digest,
                                "momentum_digest": shard.momentum_digest,
                                "precision": policy.assignments[shard.shard_id],
                                "full": True,
                            }
                        )
                    manifest_digest = json_digest(
                        {
                            "round": prepared.round_number,
                            "scheduler_position": prepared.scheduler_position,
                            "data_cursor": prepared.data_cursor,
                            "loss_scale": _float_identity(policy.loss_scale),
                            "policy_version": current_version,
                            "shards": manifest_shards,
                        }
                    )
                    now = utc_now()
                    conn.execute(
                        """
                        INSERT INTO checkpoints(
                            commit_id, seq, round_number, scheduler_position_json, data_cursor_json,
                            loss_scale_at_commit, policy_version_at_commit, manifest_digest,
                            request_hash, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            prepared.commit_id,
                            next_seq,
                            prepared.round_number,
                            _json_text(prepared.scheduler_position),
                            _json_text(prepared.data_cursor),
                            policy.loss_scale,
                            current_version,
                            manifest_digest,
                            prepared.request_hash,
                            now,
                        ),
                    )
                    conn.executemany(
                        """
                        INSERT INTO checkpoint_shards(
                            commit_id, shard_id, ordinal, shape_json, numel, fp16_blob,
                            master_blob, momentum_blob, fp16_digest, master_digest,
                            momentum_digest, precision_at_commit, source_file
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                prepared.commit_id,
                                item.loaded.shard_id,
                                ordinal,
                                _json_text(list(item.loaded.master.shape)),
                                int(item.loaded.master.size),
                                item.loaded.fp16_copy.tobytes(order="C"),
                                item.loaded.master.tobytes(order="C"),
                                item.loaded.momentum.tobytes(order="C"),
                                item.loaded.fp16_digest,
                                item.loaded.master_digest,
                                item.loaded.momentum_digest,
                                policy.assignments[item.loaded.shard_id],
                                item.loaded.source_file,
                            )
                            for ordinal, item in enumerate(prepared.shards)
                        ],
                    )
                    checkpoint_row = conn.execute(
                        "SELECT * FROM checkpoints WHERE commit_id=?", (prepared.commit_id,)
                    ).fetchone()
                    checkpoint_derivation = self._derive_checkpoint_locked(
                        conn,
                        checkpoint_row=checkpoint_row,
                        policy=policy,
                        old_policy_version=None,
                        intent_id=intent_id,
                        crash_point="mid_checkpoint_rebuild",
                    )
                    conn.execute(
                        """
                        UPDATE checkpoint_state
                        SET latest_commit_id=?, latest_seq=?, last_completed_intent_id=?, updated_at=?
                        WHERE singleton=1
                        """,
                        (prepared.commit_id, next_seq, intent_id, utc_now()),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO metadata(key, value) VALUES ('last_completed_intent', ?)",
                        (intent_id,),
                    )
                    response = {
                        "commit_id": prepared.commit_id,
                        "checkpoint_version": next_seq,
                        "round": prepared.round_number,
                        "policy_version": current_version,
                        "loss_scale": policy.loss_scale,
                        "manifest_digest": manifest_digest,
                        "shards": manifest_shards,
                        "partial_shards_excluded": list(
                            self.catalog.manifest.get("partial_shard_ids", [])
                        ),
                        "formal_manifest_only": True,
                        "derived": checkpoint_derivation["aggregate"],
                        "invalidations": checkpoint_derivation["invalidations"],
                        "intent_id": intent_id,
                    }
                    self._record_operation(
                        conn,
                        commit_id=prepared.commit_id,
                        kind="checkpoint",
                        request_hash=prepared.request_hash,
                        response=response,
                    )
                    self._finish_intent(
                        conn,
                        intent_id,
                        resolution={"checkpoint_version": next_seq},
                    )
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise
        return response

    # ------------------------------------------------------------------
    # Derivations and invalidation
    # ------------------------------------------------------------------
    @staticmethod
    def _decode_blob(blob: bytes, shape: list[int], dtype: np.dtype) -> np.ndarray:
        array = np.frombuffer(blob, dtype=dtype)
        expected = 1
        for dimension in shape:
            expected *= int(dimension)
        if array.size != expected:
            raise BusinessError(
                "E_CHECKSUM_MISMATCH",
                "stored checkpoint shard has an invalid payload length",
                details={"expected": expected, "actual": int(array.size)},
            )
        return array.reshape(tuple(shape)).copy()

    def _derive_checkpoint_locked(
        self,
        conn: sqlite3.Connection,
        *,
        checkpoint_row: sqlite3.Row,
        policy: PolicyView,
        old_policy_version: int | None,
        intent_id: str,
        crash_point: str,
    ) -> dict[str, Any]:
        commit_id = str(checkpoint_row["commit_id"])
        checkpoint_version = int(checkpoint_row["seq"])
        scheduler = json.loads(checkpoint_row["scheduler_position_json"])
        cursor = json.loads(checkpoint_row["data_cursor_json"])
        shard_rows = conn.execute(
            "SELECT * FROM checkpoint_shards WHERE commit_id=? ORDER BY ordinal",
            (commit_id,),
        ).fetchall()
        old_rows: dict[str, sqlite3.Row] = {}
        if old_policy_version is not None:
            old_rows = {
                row["shard_id"]: row
                for row in conn.execute(
                    "SELECT * FROM derived_shards WHERE commit_id=? AND policy_version=?",
                    (commit_id, old_policy_version),
                ).fetchall()
            }

        derivations: list[tuple[str, ShardDerivation]] = []
        invalidations: list[dict[str, Any]] = []
        changed_shards: list[str] = []
        changed_components = [
            "master_derived_summary",
            "consistency_result",
            "recovery_baseline",
            "memory_estimate",
        ]
        now = utc_now()
        for index, shard_row in enumerate(shard_rows):
            shard_id = str(shard_row["shard_id"])
            shape = json.loads(shard_row["shape_json"])
            master = self._decode_blob(shard_row["master_blob"], shape, np.dtype("<f4"))
            fp16_copy = self._decode_blob(shard_row["fp16_blob"], shape, np.dtype("<f2"))
            momentum = self._decode_blob(shard_row["momentum_blob"], shape, np.dtype("<f4"))
            precision = policy.assignments.get(
                shard_id, str(shard_row["precision_at_commit"])
            )
            derivation = derive_shard(
                shard_id=shard_id,
                master=master,
                fp16_copy=fp16_copy,
                momentum=momentum,
                precision=precision,
                loss_scale=policy.loss_scale,
                loss_scale_initial=policy.loss_scale_initial,
                growth_factor=policy.growth_factor,
                stored_master_digest=str(shard_row["master_digest"]),
                stored_fp16_digest=str(shard_row["fp16_digest"]),
                stored_momentum_digest=str(shard_row["momentum_digest"]),
                checkpoint_version=checkpoint_version,
                scheduler_position=scheduler,
                data_cursor=cursor,
            )
            derivations.append((shard_id, derivation))
            old = old_rows.get(shard_id)
            reasons: list[str] = []
            if old is None:
                reasons.append("new_checkpoint_or_policy_derivation")
            else:
                old_details = json.loads(old["details_json"])
                if old["precision"] != derivation.precision:
                    reasons.append("precision_changed")
                if not math.isclose(
                    float(old["loss_scale"]),
                    derivation.loss_scale,
                    rel_tol=0.0,
                    abs_tol=0.0,
                ):
                    reasons.append("loss_scale_changed")
                if old_details.get("master_digest") != str(shard_row["master_digest"]):
                    reasons.append("master_digest_changed")
                if old_details.get("momentum_digest") != str(shard_row["momentum_digest"]):
                    reasons.append("momentum_digest_changed")
                if old["dependency_fingerprint"] != derivation.dependency_fingerprint:
                    reasons.append("dependency_fingerprint_changed")
            changed = old is None or old["dependency_fingerprint"] != derivation.dependency_fingerprint
            if changed:
                changed_shards.append(shard_id)
                invalidations.append(
                    {
                        "checkpoint_id": commit_id,
                        "checkpoint_version": checkpoint_version,
                        "shard_id": shard_id,
                        "target_kind": "shard_derived",
                        "reasons": sorted(set(reasons)) or ["derived_value_changed"],
                        "components": changed_components,
                        "old_policy_version": old_policy_version,
                        "new_policy_version": policy.version,
                    }
                )
                conn.execute(
                    """
                    INSERT INTO derived_invalidations(
                        intent_id, commit_id, policy_version, shard_id, target_kind,
                        reasons_json, old_policy_version, new_policy_version, created_at
                    ) VALUES (?, ?, ?, ?, 'shard_derived', ?, ?, ?, ?)
                    """,
                    (
                        intent_id,
                        commit_id,
                        policy.version,
                        shard_id,
                        _json_text(sorted(set(reasons)) or ["derived_value_changed"]),
                        old_policy_version,
                        policy.version,
                        now,
                    ),
                )
            conn.execute(
                """
                INSERT INTO derived_shards(
                    commit_id, policy_version, shard_id, precision, loss_scale,
                    restore_digest, summary_digest, dependency_fingerprint,
                    consistency_ok, consistency_reason, max_abs, max_rel, mean_abs,
                    cumulative_rounding_error, memory_bytes, recovery_baseline_digest,
                    details_json, invalidated, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    commit_id,
                    policy.version,
                    shard_id,
                    derivation.precision,
                    derivation.loss_scale,
                    derivation.restore_digest,
                    derivation.summary_digest,
                    derivation.dependency_fingerprint,
                    1 if derivation.consistency_ok else 0,
                    derivation.consistency_reason,
                    derivation.max_abs,
                    derivation.max_rel,
                    derivation.mean_abs,
                    derivation.cumulative_rounding_error,
                    derivation.memory_bytes,
                    derivation.recovery_baseline_digest,
                    _json_text(derivation.details),
                    now,
                ),
            )
            if index == 0:
                crash_hook(crash_point)

        aggregate_values = aggregate_derivations(
            derivations,
            checkpoint_version=checkpoint_version,
            scheduler_position=scheduler,
            data_cursor=cursor,
        )
        old_aggregate = None
        if old_policy_version is not None:
            old_aggregate = conn.execute(
                "SELECT * FROM derived_aggregates WHERE commit_id=? AND policy_version=?",
                (commit_id, old_policy_version),
            ).fetchone()
        aggregate_changed = bool(changed_shards) or old_aggregate is None
        conn.execute(
            """
            INSERT INTO derived_aggregates(
                commit_id, policy_version, shard_count, restore_digest, summary_digest,
                consistency_ok, max_abs, max_rel, cumulative_rounding_error, memory_bytes,
                recovery_baseline_digest, dependency_fingerprint, invalidated, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                commit_id,
                policy.version,
                aggregate_values["shard_count"],
                aggregate_values["restore_digest"],
                aggregate_values["summary_digest"],
                1 if aggregate_values["consistency_ok"] else 0,
                aggregate_values["max_abs"],
                aggregate_values["max_rel"],
                aggregate_values["cumulative_rounding_error"],
                aggregate_values["memory_bytes"],
                aggregate_values["recovery_baseline_digest"],
                aggregate_values["dependency_fingerprint"],
                now,
            ),
        )
        for shard_id, derivation in derivations:
            for derived_kind in (
                "master_derived_summary",
                "consistency_result",
                "recovery_baseline",
                "memory_estimate",
            ):
                conn.execute(
                    """
                    INSERT INTO dependency_edges(
                        commit_id, policy_version, shard_id, derived_kind,
                        dependency_key, input_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        commit_id,
                        policy.version,
                        shard_id,
                        derived_kind,
                        f"shard:{shard_id}:{derived_kind}",
                        derivation.dependency_fingerprint,
                    ),
                )
            conn.execute(
                """
                INSERT INTO dependency_edges(
                    commit_id, policy_version, shard_id, derived_kind,
                    dependency_key, input_fingerprint
                ) VALUES (?, ?, ?, 'aggregate', ?, ?)
                """,
                (
                    commit_id,
                    policy.version,
                    shard_id,
                    f"aggregate:shard:{shard_id}",
                    derivation.dependency_fingerprint,
                ),
            )
        if aggregate_changed:
            aggregate_reasons = []
            if old_aggregate is None:
                aggregate_reasons.append("new_checkpoint_or_policy_derivation")
            if changed_shards:
                aggregate_reasons.append("dependent_shard_changed")
            invalidations.append(
                {
                    "checkpoint_id": commit_id,
                    "checkpoint_version": checkpoint_version,
                    "shard_id": None,
                    "target_kind": "aggregate",
                    "reasons": sorted(set(aggregate_reasons)),
                    "affected_shards": sorted(set(changed_shards)),
                    "old_policy_version": old_policy_version,
                    "new_policy_version": policy.version,
                }
            )
            conn.execute(
                """
                INSERT INTO derived_invalidations(
                    intent_id, commit_id, policy_version, shard_id, target_kind,
                    reasons_json, old_policy_version, new_policy_version, created_at
                ) VALUES (?, ?, ?, NULL, 'aggregate', ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    commit_id,
                    policy.version,
                    _json_text(sorted(set(aggregate_reasons))),
                    old_policy_version,
                    policy.version,
                    now,
                ),
            )
        if old_policy_version is not None:
            conn.execute(
                "UPDATE derived_shards SET invalidated=1 WHERE commit_id=? AND policy_version=?",
                (commit_id, old_policy_version),
            )
            conn.execute(
                "UPDATE derived_aggregates SET invalidated=1 WHERE commit_id=? AND policy_version=?",
                (commit_id, old_policy_version),
            )
        aggregate = {
            **aggregate_values,
            "checkpoint_id": commit_id,
            "policy_version": policy.version,
            "invalidated": False,
        }
        return {
            "checkpoint_id": commit_id,
            "checkpoint_version": checkpoint_version,
            "changed_shards": sorted(set(changed_shards)),
            "aggregate_changed": aggregate_changed,
            "aggregate": aggregate,
            "invalidations": invalidations,
        }

    def _derive_for_policy_locked(
        self,
        conn: sqlite3.Connection,
        *,
        new_policy: PolicyView,
        old_policy_version: int | None,
        intent_id: str,
    ) -> dict[str, Any]:
        checkpoints = conn.execute("SELECT * FROM checkpoints ORDER BY seq").fetchall()
        results: list[dict[str, Any]] = []
        all_invalidations: list[dict[str, Any]] = []
        changed_shards: set[str] = set()
        for checkpoint_row in checkpoints:
            result = self._derive_checkpoint_locked(
                conn,
                checkpoint_row=checkpoint_row,
                policy=new_policy,
                old_policy_version=old_policy_version,
                intent_id=intent_id,
                crash_point="mid_rebuild",
            )
            results.append(result)
            all_invalidations.extend(result["invalidations"])
            changed_shards.update(result["changed_shards"])
        return {
            "policy_version": new_policy.version,
            "old_policy_version": old_policy_version,
            "checkpoint_count": len(checkpoints),
            "changed_shards": sorted(changed_shards),
            "checkpoints": results,
            "invalidations": all_invalidations,
        }

    # ------------------------------------------------------------------
    # Read-side API
    # ------------------------------------------------------------------
    def get_current_policy(self) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            policy = self._current_policy(conn)
            if policy is None:
                raise BusinessError("E_POLICY_REQUIRED", "no policy has been submitted")
            state = conn.execute(
                "SELECT last_snapshot_seq FROM policy_state WHERE singleton=1"
            ).fetchone()
            payload = self._policy_response(policy)
            payload["last_snapshot_seq"] = int(state["last_snapshot_seq"])
            return payload

    def _resolve_checkpoint(self, conn: sqlite3.Connection, ref: str | int) -> sqlite3.Row:
        text = str(ref)
        if text.isdigit():
            row = conn.execute("SELECT * FROM checkpoints WHERE seq=?", (int(text),)).fetchone()
        else:
            row = conn.execute("SELECT * FROM checkpoints WHERE commit_id=?", (text,)).fetchone()
        if row is None:
            raise BusinessError(
                "E_NOT_FOUND", "checkpoint not found", details={"checkpoint": text}
            )
        return row

    def get_checkpoint_derived(
        self, ref: str | int, *, policy_version: int | None = None
    ) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            checkpoint = self._resolve_checkpoint(conn, ref)
            state = conn.execute(
                "SELECT current_version FROM policy_state WHERE singleton=1"
            ).fetchone()
            current_policy_version = int(state["current_version"]) if state else 0
            selected_policy_version = (
                int(policy_version) if policy_version is not None else current_policy_version
            )
            if selected_policy_version <= 0:
                raise BusinessError("E_POLICY_REQUIRED", "no policy has been submitted")
            aggregate = conn.execute(
                "SELECT * FROM derived_aggregates WHERE commit_id=? AND policy_version=?",
                (checkpoint["commit_id"], selected_policy_version),
            ).fetchone()
            if aggregate is None:
                raise BusinessError(
                    "E_NOT_FOUND",
                    "derived aggregate does not exist for this checkpoint and policy version",
                    details={
                        "commit_id": checkpoint["commit_id"],
                        "policy_version": selected_policy_version,
                    },
                )
            shard_rows = conn.execute(
                """
                SELECT ds.*, cs.ordinal
                FROM derived_shards ds
                JOIN checkpoint_shards cs
                  ON cs.commit_id = ds.commit_id AND cs.shard_id = ds.shard_id
                WHERE ds.commit_id=? AND ds.policy_version=?
                ORDER BY cs.ordinal
                """,
                (checkpoint["commit_id"], selected_policy_version),
            ).fetchall()
            if len(shard_rows) != int(aggregate["shard_count"]):
                raise BusinessError(
                    "E_INTERNAL",
                    "derived shard rows are incomplete; refusing to expose a partial view",
                    details={
                        "expected": int(aggregate["shard_count"]),
                        "actual": len(shard_rows),
                    },
                )
            if selected_policy_version == current_policy_version and int(aggregate["invalidated"]) != 0:
                raise BusinessError(
                    "E_INTERNAL",
                    "current derived aggregate is invalidated; refusing to expose stale state",
                )
            policy = self._load_policy(conn, selected_policy_version)
            shards = []
            for row in shard_rows:
                details = json.loads(row["details_json"])
                if selected_policy_version == current_policy_version:
                    active_precision = policy.assignments.get(row["shard_id"])
                    if int(row["invalidated"]) != 0 or active_precision != row["precision"]:
                        raise BusinessError(
                            "E_INTERNAL",
                            "current derived shard is stale relative to the active policy",
                            details={"shard_id": row["shard_id"]},
                        )
                shards.append(
                    {
                        "shard_id": row["shard_id"],
                        "precision": row["precision"],
                        "loss_scale": float(row["loss_scale"]),
                        "master_digest": details.get("master_digest"),
                        "fp16_digest": details.get("fp16_digest"),
                        "momentum_digest": details.get("momentum_digest"),
                        "restore_digest": row["restore_digest"],
                        "derived_summary_digest": row["summary_digest"],
                        "master_derived_summary_digest": row["summary_digest"],
                        "dependency_fingerprint": row["dependency_fingerprint"],
                        "consistency": {
                            "ok": bool(row["consistency_ok"]),
                            "reason": row["consistency_reason"],
                        },
                        "consistency_check": {
                            "ok": bool(row["consistency_ok"]),
                            "reason": row["consistency_reason"],
                        },
                        "deviations": {
                            "max_absolute": float(row["max_abs"]),
                            "max_relative": float(row["max_rel"]),
                            "mean_absolute": float(row["mean_abs"]),
                            "cumulative_rounding_error": float(
                                row["cumulative_rounding_error"]
                            ),
                        },
                        "memory_bytes": int(row["memory_bytes"]),
                        "memory_estimate_bytes": int(row["memory_bytes"]),
                        "recovery_baseline_digest": row["recovery_baseline_digest"],
                        "recovery_baseline": {
                            "checkpoint_version": int(checkpoint["seq"]),
                            "shard_id": row["shard_id"],
                            "precision": row["precision"],
                            "loss_scale": float(row["loss_scale"]),
                            "loss_scale_initial": policy.loss_scale_initial,
                            "growth_factor": policy.growth_factor,
                            "master_digest": details.get("master_digest"),
                            "fp16_digest": details.get("fp16_digest"),
                            "momentum_digest": details.get("momentum_digest"),
                            "restore_digest": row["restore_digest"],
                            "scheduler_position": json.loads(
                                checkpoint["scheduler_position_json"]
                            ),
                            "data_cursor": json.loads(checkpoint["data_cursor_json"]),
                            "memory_bytes": int(row["memory_bytes"]),
                        },
                        "invalidated": bool(row["invalidated"]),
                        "details": details,
                    }
                )
            invalidation_rows = conn.execute(
                """
                SELECT * FROM derived_invalidations
                WHERE commit_id=? AND policy_version=?
                ORDER BY id
                """,
                (checkpoint["commit_id"], selected_policy_version),
            ).fetchall()
            return {
                "checkpoint": {
                    "commit_id": checkpoint["commit_id"],
                    "version": int(checkpoint["seq"]),
                    "round": int(checkpoint["round_number"]),
                    "scheduler_position": json.loads(checkpoint["scheduler_position_json"]),
                    "data_cursor": json.loads(checkpoint["data_cursor_json"]),
                    "loss_scale_at_commit": float(checkpoint["loss_scale_at_commit"]),
                    "policy_version_at_commit": int(checkpoint["policy_version_at_commit"]),
                    "manifest_digest": checkpoint["manifest_digest"],
                },
                "policy": {
                    "version": selected_policy_version,
                    "loss_scale": policy.loss_scale,
                    "loss_scale_initial": policy.loss_scale_initial,
                    "growth_factor": policy.growth_factor,
                    "assignments": policy.assignments,
                },
                "is_current_policy": selected_policy_version == current_policy_version,
                "aggregate": {
                    "shard_count": int(aggregate["shard_count"]),
                    "restore_digest": aggregate["restore_digest"],
                    "derived_summary_digest": aggregate["summary_digest"],
                    "master_derived_summary_digest": aggregate["summary_digest"],
                    "consistency_ok": bool(aggregate["consistency_ok"]),
                    "max_absolute": float(aggregate["max_abs"]),
                    "max_relative": float(aggregate["max_rel"]),
                    "cumulative_rounding_error": float(
                        aggregate["cumulative_rounding_error"]
                    ),
                    "memory_bytes": int(aggregate["memory_bytes"]),
                    "memory_estimate_bytes": int(aggregate["memory_bytes"]),
                    "recovery_baseline_digest": aggregate["recovery_baseline_digest"],
                    "dependency_fingerprint": aggregate["dependency_fingerprint"],
                    "invalidated": bool(aggregate["invalidated"]),
                },
                "shards": shards,
                "invalidations": [
                    {
                        "id": int(row["id"]),
                        "checkpoint_id": row["commit_id"],
                        "shard_id": row["shard_id"],
                        "target_kind": row["target_kind"],
                        "reasons": json.loads(row["reasons_json"]),
                        "old_policy_version": row["old_policy_version"],
                        "new_policy_version": row["new_policy_version"],
                        "created_at": row["created_at"],
                    }
                    for row in invalidation_rows
                ],
                "numeric_contract": NUMERIC_CONTRACT,
            }

    def get_checkpoint_manifest(self, ref: str | int = "latest") -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            if str(ref) == "latest":
                state = conn.execute(
                    "SELECT latest_commit_id FROM checkpoint_state WHERE singleton=1"
                ).fetchone()
                if state is None or state["latest_commit_id"] is None:
                    raise BusinessError("E_NOT_FOUND", "no checkpoint has been committed")
                checkpoint = self._resolve_checkpoint(conn, state["latest_commit_id"])
            else:
                checkpoint = self._resolve_checkpoint(conn, ref)
            rows = conn.execute(
                "SELECT * FROM checkpoint_shards WHERE commit_id=? ORDER BY ordinal",
                (checkpoint["commit_id"],),
            ).fetchall()
            return {
                "commit_id": checkpoint["commit_id"],
                "version": int(checkpoint["seq"]),
                "round": int(checkpoint["round_number"]),
                "scheduler_position": json.loads(checkpoint["scheduler_position_json"]),
                "data_cursor": json.loads(checkpoint["data_cursor_json"]),
                "loss_scale": float(checkpoint["loss_scale_at_commit"]),
                "policy_version": int(checkpoint["policy_version_at_commit"]),
                "manifest_digest": checkpoint["manifest_digest"],
                "shards": [
                    {
                        "shard_id": row["shard_id"],
                        "shape": json.loads(row["shape_json"]),
                        "numel": int(row["numel"]),
                        "fp16_digest": row["fp16_digest"],
                        "fp32_master_digest": row["master_digest"],
                        "momentum_digest": row["momentum_digest"],
                        "precision": row["precision_at_commit"],
                        "full": True,
                    }
                    for row in rows
                ],
            }

    def get_policy_derived(self, policy_version: int) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            policy = self._load_policy(conn, int(policy_version))
            state = conn.execute(
                "SELECT current_version FROM policy_state WHERE singleton=1"
            ).fetchone()
            current_version = int(state["current_version"]) if state else 0
            rows = conn.execute(
                """
                SELECT da.*, c.seq AS checkpoint_seq
                FROM derived_aggregates da
                JOIN checkpoints c ON c.commit_id = da.commit_id
                WHERE da.policy_version=?
                ORDER BY c.seq
                """,
                (int(policy_version),),
            ).fetchall()
            return {
                "policy": self._policy_response(policy),
                "is_current_policy": int(policy_version) == current_version,
                "checkpoints": [
                    {
                        "checkpoint_id": row["commit_id"],
                        "checkpoint_version": int(row["checkpoint_seq"]),
                        "derived_summary_digest": row["summary_digest"],
                        "restore_digest": row["restore_digest"],
                        "consistency_ok": bool(row["consistency_ok"]),
                        "max_absolute": float(row["max_abs"]),
                        "max_relative": float(row["max_rel"]),
                        "cumulative_rounding_error": float(
                            row["cumulative_rounding_error"]
                        ),
                        "memory_bytes": int(row["memory_bytes"]),
                        "invalidated": bool(row["invalidated"]),
                    }
                    for row in rows
                ],
            }

    def list_intents(self) -> list[dict[str, Any]]:
        rows = self.db.fetch_all("SELECT * FROM intents ORDER BY started_at, intent_id")
        return [
            {
                "intent_id": row["intent_id"],
                "kind": row["kind"],
                "target_id": row["target_id"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "error": json.loads(row["error_json"]) if row["error_json"] else None,
                "resolution": json.loads(row["resolution_json"])
                if row["resolution_json"]
                else None,
            }
            for row in rows
        ]

    def list_commit_operations(self) -> list[dict[str, Any]]:
        rows = self.db.fetch_all("SELECT * FROM commit_operations ORDER BY completed_at")
        return [
            {
                "commit_id": row["commit_id"],
                "kind": row["kind"],
                "request_hash": row["request_hash"],
                "completed_at": row["completed_at"],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Forced-termination recovery
    # ------------------------------------------------------------------
    def recover_pending(self) -> dict[str, Any]:
        report_id = f"recovery-{uuid.uuid4().hex}"
        with self.db.write_transaction() as conn:
            pending = conn.execute(
                "SELECT * FROM intents WHERE status='pending' ORDER BY started_at, intent_id"
            ).fetchall()
            facts: list[dict[str, Any]] = []
            completed_count = 0
            aborted_count = 0
            for intent in pending:
                operation = conn.execute(
                    "SELECT commit_id FROM commit_operations WHERE commit_id=?",
                    (intent["target_id"],),
                ).fetchone()
                metadata = conn.execute(
                    "SELECT value FROM metadata WHERE key='last_completed_intent'"
                ).fetchone()
                finished_by_metadata = (
                    metadata is not None and metadata["value"] == intent["intent_id"]
                )
                if operation is not None or finished_by_metadata:
                    status = "completed"
                    resolution = {
                        "resolution": "durable_commit_operation_found",
                        "commit_id": intent["target_id"],
                    }
                    completed_count += 1
                    conn.execute(
                        "UPDATE intents SET status='completed', finished_at=?, resolution_json=? WHERE intent_id=?",
                        (utc_now(), _json_text(resolution), intent["intent_id"]),
                    )
                else:
                    status = "aborted"
                    resolution = {
                        "resolution": "no_durable_commit_operation; transaction rolled back or never committed",
                        "commit_id": intent["target_id"],
                    }
                    aborted_count += 1
                    conn.execute(
                        "UPDATE intents SET status='aborted', finished_at=?, error_json=?, resolution_json=? WHERE intent_id=?",
                        (
                            utc_now(),
                            _json_text(
                                {
                                    "code": "E_RECOVERED_ABORTED_INTENT",
                                    "message": "intent had no durable completed commit operation",
                                }
                            ),
                            _json_text(resolution),
                            intent["intent_id"],
                        ),
                    )
                facts.append(
                    {
                        "intent_id": intent["intent_id"],
                        "kind": intent["kind"],
                        "target_id": intent["target_id"],
                        "previous_status": "pending",
                        "resolved_status": status,
                        "started_at": intent["started_at"],
                        "finished_at": utc_now(),
                        "resolution": resolution,
                    }
                )
            policy_state = conn.execute(
                "SELECT current_version FROM policy_state WHERE singleton=1"
            ).fetchone()
            checkpoint_state = conn.execute(
                "SELECT latest_commit_id, latest_seq FROM checkpoint_state WHERE singleton=1"
            ).fetchone()
            current_policy_version = int(policy_state["current_version"]) if policy_state else 0
            latest_seq = int(checkpoint_state["latest_seq"]) if checkpoint_state else 0
            latest_commit_id = checkpoint_state["latest_commit_id"] if checkpoint_state else None
            derived_consistent = True
            derived_fact: dict[str, Any] = {
                "latest_checkpoint_present": latest_commit_id is not None,
                "latest_checkpoint_version": latest_seq,
            }
            if latest_commit_id is not None and current_policy_version > 0:
                aggregate = conn.execute(
                    "SELECT * FROM derived_aggregates WHERE commit_id=? AND policy_version=?",
                    (latest_commit_id, current_policy_version),
                ).fetchone()
                if aggregate is None or int(aggregate["invalidated"]) != 0:
                    derived_consistent = False
                    derived_fact["reason"] = "current derived aggregate missing or invalidated"
                else:
                    shard_count = conn.execute(
                        "SELECT COUNT(*) AS n FROM derived_shards WHERE commit_id=? AND policy_version=? AND invalidated=0",
                        (latest_commit_id, current_policy_version),
                    ).fetchone()["n"]
                    if int(shard_count) != int(aggregate["shard_count"]):
                        derived_consistent = False
                        derived_fact["reason"] = "derived shard row count mismatch"
                        derived_fact["observed_shard_rows"] = int(shard_count)
                        derived_fact["expected_shard_rows"] = int(aggregate["shard_count"])
            derived_fact["consistent"] = derived_consistent
            facts_payload = {
                "report_id": report_id,
                "scope": "startup_reconciliation_from_sqlite_facts",
                "guarantees_complete_exit_report": False,
                "warning": "A killed process cannot emit an exit report; this report only describes durable SQLite facts observed during recovery.",
                "scanned_pending": len(pending),
                "completed_count": completed_count,
                "aborted_count": aborted_count,
                "current_policy_version": current_policy_version,
                "last_complete_checkpoint_version": latest_seq,
                "latest_checkpoint_commit_id": latest_commit_id,
                "derived_consistency": derived_fact,
                "intents": facts,
            }
            now = utc_now()
            conn.execute(
                """
                INSERT INTO recovery_reports(
                    report_id, scanned_pending, completed_count, aborted_count,
                    last_complete_checkpoint_version, current_policy_version, scope,
                    guarantees_complete_exit_report, facts_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    report_id,
                    len(pending),
                    completed_count,
                    aborted_count,
                    latest_seq,
                    current_policy_version,
                    "startup_reconciliation_from_sqlite_facts",
                    _json_text(facts_payload),
                    now,
                ),
            )
        report = dict(facts_payload)
        report["created_at"] = now
        self._last_recovery_report = report
        return report

    def get_recovery_report(self, report_id: str | None = None) -> dict[str, Any]:
        if report_id is None:
            row = self.db.fetch_one(
                "SELECT * FROM recovery_reports ORDER BY created_at DESC, rowid DESC LIMIT 1"
            )
        else:
            row = self.db.fetch_one(
                "SELECT * FROM recovery_reports WHERE report_id=?", (report_id,)
            )
        if row is None:
            raise BusinessError("E_NOT_FOUND", "recovery report not found")
        payload = json.loads(row["facts_json"])
        payload["created_at"] = row["created_at"]
        payload["persisted_report_columns"] = {
            "scanned_pending": int(row["scanned_pending"]),
            "completed_count": int(row["completed_count"]),
            "aborted_count": int(row["aborted_count"]),
            "last_complete_checkpoint_version": int(
                row["last_complete_checkpoint_version"]
            ),
            "current_policy_version": int(row["current_policy_version"]),
            "scope": row["scope"],
            "guarantees_complete_exit_report": bool(
                row["guarantees_complete_exit_report"]
            ),
        }
        return payload

    def health(self) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            policy = conn.execute(
                "SELECT current_version FROM policy_state WHERE singleton=1"
            ).fetchone()
            checkpoint = conn.execute(
                "SELECT latest_commit_id, latest_seq FROM checkpoint_state WHERE singleton=1"
            ).fetchone()
            return {
                "status": "ok",
                "database": str(self.db.path),
                "samples": str(self.catalog.root),
                "current_policy_version": int(policy["current_version"]),
                "latest_checkpoint_version": int(checkpoint["latest_seq"]),
                "latest_checkpoint_id": checkpoint["latest_commit_id"],
                "numeric_contract": NUMERIC_CONTRACT,
            }


class _ExistingOperation(Exception):
    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__("existing operation")
        self.response = response


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _float_identity(value: float) -> str:
    return float(value).hex()


def hashlib_sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()
