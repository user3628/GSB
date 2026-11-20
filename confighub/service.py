from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import httpx

from .errors import ConfigHubError, ErrorCode
from .models import ALLOWED_TRANSITIONS, PushState, SampleSpec, Snapshot
from .pipeline import compute_pipeline, missing_keys, required_keys, validate_sample_spec
from .storage import Storage
from .util import (
    canonical_json,
    differing_fields,
    json_loads,
    normalize_config,
    sha256_text,
    utc_now_iso,
    validate_loopback_endpoint,
)


def snapshot_digest(version: int, generated_at: str, config: dict[str, Any]) -> str:
    normalized = normalize_config(config)
    return sha256_text(
        canonical_json(
            {
                "version": int(version),
                "generated_at": generated_at,
                "config": normalized,
            }
        )
    )


class ConfigService:
    """Single-process coordinator; SQLite provides durable multi-process facts.

    Writer operations are serialized by SQLite BEGIN IMMEDIATE plus an in-process
    lock. Read transactions use WAL snapshots and therefore never wait for a
    writer to expose an intermediate confirmation state.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.storage = Storage(db_path)
        self._snapshot_guard = threading.Lock()
        self._push_lock_guard = threading.Lock()
        self._push_locks: dict[int, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Reads and small conversions
    # ------------------------------------------------------------------
    def latest_version(self) -> int:
        row = self.storage.query_one("SELECT value FROM meta WHERE key='current_version'")
        return int(row["value"]) if row else 0

    def get_snapshot(self, version: int) -> dict[str, Any] | None:
        row = self.storage.query_one("SELECT * FROM snapshots WHERE version=?", (int(version),))
        if row is None:
            return None
        return {
            "version": int(row["version"]),
            "generated_at": row["generated_at"],
            "config": json_loads(row["config_json"], {}),
            "digest": row["digest"],
            "created_at": row["created_at"],
        }

    def latest_snapshot(self) -> dict[str, Any] | None:
        version = self.latest_version()
        return self.get_snapshot(version) if version else None

    def _client_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "client_id": row["client_id"],
            "config_keys": json_loads(row["config_keys_json"], []),
            "endpoint_url": row["endpoint_url"],
            "timeout_seconds": float(row["timeout_seconds"]),
            "sample": {
                "seed": int(row["sample_seed"]),
                "sample_size": int(row["sample_size"]),
                "feature_count": int(row["feature_count"]),
            },
            "applied_version": (
                int(row["applied_version"]) if row["applied_version"] is not None else None
            ),
            "updated_at": row["updated_at"],
        }

    def get_client(self, client_id: str) -> dict[str, Any] | None:
        row = self.storage.query_one("SELECT * FROM clients WHERE client_id=?", (client_id,))
        return self._client_from_row(row) if row else None

    def list_clients(self) -> list[dict[str, Any]]:
        rows = self.storage.query_all("SELECT * FROM clients ORDER BY client_id")
        return [self._client_from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Snapshot and subscription writers
    # ------------------------------------------------------------------
    def submit_snapshot(
        self,
        version: int,
        config: dict[str, Any],
        *,
        digest: str | None = None,
        generated_at: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "version must be a positive integer", status_code=422
            )
        normalized = normalize_config(config)
        generated = generated_at or utc_now_iso()
        try:
            parsed = datetime.fromisoformat(generated.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "generated_at must be ISO-8601", status_code=422
            ) from exc
        if parsed.tzinfo is None:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "generated_at must include a timezone", status_code=422
            )

        calculated = snapshot_digest(version, generated, normalized)
        if digest is not None and str(digest).lower() != calculated:
            raise ConfigHubError(
                ErrorCode.E_SNAPSHOT_INVALID,
                "snapshot digest does not match version/generated_at/config",
                details={"expected_digest": calculated, "received_digest": digest},
            )

        with self._snapshot_guard:
            current = self.latest_version()
            if version <= current:
                raise ConfigHubError(
                    ErrorCode.E_STALE_SNAPSHOT,
                    "snapshot version must be greater than the current committed version",
                    details={"current_version": current, "received_version": version},
                )

            intent_id = self.storage.begin_intent(
                "SNAPSHOT_SUBMIT", f"version:{version}", {"version": version, "digest": calculated}
            )
            now = utc_now_iso()
            try:
                with self.storage.write_transaction() as connection:
                    current_row = connection.execute(
                        "SELECT value FROM meta WHERE key='current_version'"
                    ).fetchone()
                    current_in_tx = int(current_row["value"]) if current_row else 0
                    if version <= current_in_tx:
                        raise ConfigHubError(
                            ErrorCode.E_STALE_SNAPSHOT,
                            "snapshot version is no longer newer than the committed version",
                            details={"current_version": current_in_tx, "received_version": version},
                        )
                    connection.execute(
                        """
                        INSERT INTO snapshots(version, generated_at, config_json, digest, created_at)
                        VALUES(?, ?, ?, ?, ?)
                        """,
                        (version, generated, canonical_json(normalized), calculated, now),
                    )
                    connection.execute(
                        "UPDATE meta SET value=? WHERE key='current_version'", (str(version),)
                    )
            except ConfigHubError:
                self.storage.finish_intent(intent_id, "ABORTED")
                raise
            except Exception:
                self.storage.finish_intent(intent_id, "ABORTED")
                raise
            self.storage.finish_intent(intent_id, "COMMITTED")

        return Snapshot(version, generated, normalized, calculated).as_dict()

    def register_client(
        self,
        client_id: str,
        config_keys: list[str] | tuple[str, ...],
        endpoint_url: str,
        *,
        timeout_seconds: float = 1.0,
        sample_seed: int = 20240501,
        sample_size: int = 64,
        feature_count: int = 3,
    ) -> dict[str, Any]:
        if not isinstance(client_id, str) or not client_id.strip():
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "client_id must be a non-empty string", status_code=422
            )
        keys = tuple(dict.fromkeys(config_keys))
        if not keys or any(not isinstance(key, str) or not key for key in keys):
            raise ConfigHubError(
                ErrorCode.E_VALIDATION,
                "config_keys must be a non-empty list of non-empty strings",
                status_code=422,
            )
        validate_loopback_endpoint(endpoint_url)
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "timeout_seconds must be numeric", status_code=422
            )
        if not (0.001 <= float(timeout_seconds) <= 60.0):
            raise ConfigHubError(
                ErrorCode.E_VALIDATION,
                "timeout_seconds must be in [0.001, 60]",
                status_code=422,
            )
        spec = SampleSpec(seed=sample_seed, sample_size=sample_size, feature_count=feature_count)
        validate_sample_spec(spec)

        intent_id = self.storage.begin_intent(
            "CLIENT_UPSERT", f"client:{client_id}", {"client_id": client_id}
        )
        now = utc_now_iso()
        try:
            with self.storage.write_transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO clients(client_id, config_keys_json, endpoint_url, timeout_seconds,
                                        sample_seed, sample_size, feature_count, applied_version,
                                        updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
                    ON CONFLICT(client_id) DO UPDATE SET
                        config_keys_json=excluded.config_keys_json,
                        endpoint_url=excluded.endpoint_url,
                        timeout_seconds=excluded.timeout_seconds,
                        sample_seed=excluded.sample_seed,
                        sample_size=excluded.sample_size,
                        feature_count=excluded.feature_count,
                        updated_at=excluded.updated_at
                    """,
                    (
                        client_id,
                        canonical_json(list(keys)),
                        endpoint_url,
                        float(timeout_seconds),
                        spec.seed,
                        spec.sample_size,
                        spec.feature_count,
                        now,
                    ),
                )
        except Exception:
            self.storage.finish_intent(intent_id, "ABORTED")
            raise
        self.storage.finish_intent(intent_id, "COMMITTED")
        return self.get_client(client_id) or {}

    # ------------------------------------------------------------------
    # Push state machine helpers
    # ------------------------------------------------------------------
    def _push_lock(self, version: int) -> asyncio.Lock:
        with self._push_lock_guard:
            return self._push_locks.setdefault(version, asyncio.Lock())

    def _transition(
        self,
        connection: Any,
        client_id: str,
        version: int,
        target: PushState,
        reason: str,
    ) -> PushState:
        row = connection.execute(
            "SELECT state FROM push_records WHERE client_id=? AND version=?",
            (client_id, version),
        ).fetchone()
        if row is None:
            raise ConfigHubError(
                ErrorCode.E_NOT_FOUND,
                "push record does not exist",
                status_code=404,
                details={"client_id": client_id, "version": version},
            )
        current = PushState(row["state"])
        if target not in ALLOWED_TRANSITIONS[current]:
            raise ConfigHubError(
                ErrorCode.E_INVALID_TRANSITION,
                f"illegal transition {current.value} -> {target.value}",
                details={
                    "client_id": client_id,
                    "version": version,
                    "from_state": current.value,
                    "to_state": target.value,
                },
            )
        now = utc_now_iso()
        connection.execute(
            "UPDATE push_records SET state=?, updated_at=? WHERE client_id=? AND version=?",
            (target.value, now, client_id, version),
        )
        connection.execute(
            """
            INSERT INTO push_transitions(client_id, version, from_state, to_state, reason, occurred_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (client_id, version, current.value, target.value, reason, now),
        )
        return target

    def transition_push_state(
        self,
        client_id: str,
        version: int,
        to_state: PushState | str,
        reason: str = "administrative",
    ) -> dict[str, Any]:
        """Administrative transition helper.

        ACKED and APPLIED are intentionally not independently committable: the
        confirmation path writes those two transitions together with the client
        version in one transaction to preserve the read visibility invariant.
        """

        target = PushState(to_state)
        if target in {PushState.ACKED, PushState.APPLIED}:
            raise ConfigHubError(
                ErrorCode.E_INVALID_TRANSITION,
                "ACKED/APPLIED must be committed atomically by the acknowledgement path",
                details={"to_state": target.value},
            )
        row = self.storage.query_one(
            "SELECT state FROM push_records WHERE client_id=? AND version=?",
            (client_id, version),
        )
        if row is None:
            raise ConfigHubError(
                ErrorCode.E_NOT_FOUND,
                "push record does not exist",
                status_code=404,
                details={"client_id": client_id, "version": version},
            )
        current = PushState(row["state"])
        if target not in ALLOWED_TRANSITIONS[current]:
            raise ConfigHubError(
                ErrorCode.E_INVALID_TRANSITION,
                f"illegal transition {current.value} -> {target.value}",
                details={
                    "client_id": client_id,
                    "version": version,
                    "from_state": current.value,
                    "to_state": target.value,
                },
            )
        intent_id = self.storage.begin_intent(
            "STATE_TRANSITION",
            f"client:{client_id}:version:{version}",
            {"client_id": client_id, "version": version, "to_state": target.value},
        )
        try:
            with self.storage.write_transaction() as connection:
                self._transition(connection, client_id, version, target, reason)
        except Exception:
            self.storage.finish_intent(intent_id, "ABORTED")
            raise
        self.storage.finish_intent(intent_id, "COMMITTED")
        row = self.storage.query_one(
            "SELECT * FROM push_records WHERE client_id=? AND version=?",
            (client_id, version),
        )
        return {"client_id": client_id, "version": version, "state": row["state"]}

    def _abort_push(self, client_id: str, version: int, code: str, message: str) -> None:
        try:
            with self.storage.write_transaction() as connection:
                row = connection.execute(
                    "SELECT state FROM push_records WHERE client_id=? AND version=?",
                    (client_id, version),
                ).fetchone()
                if row is None:
                    return
                current = PushState(row["state"])
                if current in {PushState.PENDING, PushState.PUSHED, PushState.ACKED}:
                    self._transition(
                        connection,
                        client_id,
                        version,
                        PushState.ABORTED,
                        f"{code}: {message}",
                    )
                connection.execute(
                    "UPDATE push_records SET last_error=?, updated_at=? WHERE client_id=? AND version=?",
                    (code, utc_now_iso(), client_id, version),
                )
        except ConfigHubError:
            # A concurrent acknowledgement may already have made the record
            # terminal. The durable state, not the raced error, wins.
            return

    # ------------------------------------------------------------------
    # Push orchestration
    # ------------------------------------------------------------------
    async def push_version(
        self,
        version: int,
        *,
        timeout_seconds: float | None = None,
        client_ids: list[str] | tuple[str, ...] | None = None,
        content_tag: str = "default",
        crash_hook: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "version must be a positive integer", status_code=422
            )
        lock = self._push_lock(version)
        async with lock:
            return await self._push_version_locked(
                version,
                timeout_seconds=timeout_seconds,
                client_ids=client_ids,
                content_tag=content_tag,
                crash_hook=crash_hook,
            )

    def push_version_sync(self, version: int, **kwargs: Any) -> dict[str, Any]:
        return asyncio.run(self.push_version(version, **kwargs))

    async def _push_version_locked(
        self,
        version: int,
        *,
        timeout_seconds: float | None,
        client_ids: list[str] | tuple[str, ...] | None,
        content_tag: str,
        crash_hook: Callable[[str], None] | None,
    ) -> dict[str, Any]:
        snapshot = self.get_snapshot(version)
        if snapshot is None:
            raise ConfigHubError(
                ErrorCode.E_NOT_FOUND,
                "snapshot does not exist",
                status_code=404,
                details={"version": version},
            )
        selected_rows = self.storage.query_all("SELECT * FROM clients ORDER BY client_id")
        selected_clients = [self._client_from_row(row) for row in selected_rows]
        if client_ids is not None:
            wanted = list(dict.fromkeys(client_ids))
            by_id = {client["client_id"]: client for client in selected_clients}
            unknown = [client_id for client_id in wanted if client_id not in by_id]
            if unknown:
                raise ConfigHubError(
                    ErrorCode.E_NOT_FOUND,
                    "one or more clients are not registered",
                    status_code=404,
                    details={"unknown_clients": unknown},
                )
            selected_clients = [by_id[client_id] for client_id in wanted]
        if not selected_clients:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION, "no clients selected for push", status_code=422
            )

        requested_ids = [client["client_id"] for client in selected_clients]
        request_digest = sha256_text(
            canonical_json(
                {
                    "version": version,
                    "snapshot_digest": snapshot["digest"],
                    "clients": [
                        {
                            "client_id": client["client_id"],
                            "config_keys": client["config_keys"],
                            "endpoint_url": client["endpoint_url"],
                            "sample": client["sample"],
                        }
                        for client in selected_clients
                    ],
                    "content_tag": content_tag,
                }
            )
        )
        existing = self.storage.query_one("SELECT * FROM batches WHERE target_version=?", (version,))
        if existing is not None:
            return await self._reuse_batch(existing, request_digest)

        for client in selected_clients:
            if timeout_seconds is not None:
                if not (0.001 <= float(timeout_seconds) <= 60.0):
                    raise ConfigHubError(
                        ErrorCode.E_VALIDATION,
                        "timeout_seconds must be in [0.001, 60]",
                        status_code=422,
                    )
                client["timeout_seconds"] = float(timeout_seconds)

        batch_id = f"batch-v{version}-{uuid.uuid4().hex[:12]}"
        intent_id = self.storage.begin_intent(
            "PUSH_ROUND",
            f"version:{version}",
            {
                "version": version,
                "batch_id": batch_id,
                "request_digest": request_digest,
                "client_ids": requested_ids,
            },
        )

        prepared: dict[str, dict[str, Any]] = {}
        validation_errors: dict[str, str] = {}
        snapshot_config = snapshot["config"]
        for client in selected_clients:
            client_id = client["client_id"]
            keys = list(client["config_keys"])
            absent = missing_keys(snapshot_config, keys)
            sample = SampleSpec(**client["sample"])
            required = list(required_keys(sample))
            subscribed_missing = [key for key in required if key not in keys]
            if not any(key in keys for key in ("normalization_scale", "scale", "ratio")):
                subscribed_missing.append("normalization_scale")
            if absent or subscribed_missing:
                validation_errors[client_id] = "E_PUSH_PAYLOAD_INVALID"
                prepared[client_id] = {
                    "client_id": client_id,
                    "version": version,
                    "batch_id": batch_id,
                    "url": client["endpoint_url"],
                    "body": {},
                    "payload_digest": sha256_text(
                        canonical_json(
                            {
                                "client_id": client_id,
                                "version": version,
                                "missing_config_keys": absent,
                                "missing_subscription_keys": subscribed_missing,
                            }
                        )
                    ),
                    "timeout_seconds": client["timeout_seconds"],
                    "missing_config_keys": absent,
                    "missing_subscription_keys": subscribed_missing,
                }
                continue

            effective_config = {key: snapshot_config[key] for key in keys}
            previous_config: dict[str, float] = {}
            if client["applied_version"] is not None:
                previous = self.get_snapshot(client["applied_version"])
                if previous:
                    previous_config = previous["config"]
            changed_keys = [
                key
                for key in keys
                if key not in previous_config or previous_config[key] != effective_config[key]
            ]
            body = {
                "batch_id": batch_id,
                "client_id": client_id,
                "version": version,
                "snapshot_digest": snapshot["digest"],
                "config": effective_config,
                "changed_keys": changed_keys,
                "subscribed_keys": keys,
                "sample": sample.as_dict(),
            }
            prepared[client_id] = {
                "client_id": client_id,
                "version": version,
                "batch_id": batch_id,
                "url": client["endpoint_url"],
                "body": body,
                "payload_digest": sha256_text(canonical_json(body)),
                "timeout_seconds": float(client["timeout_seconds"]),
            }

        now = utc_now_iso()
        try:
            with self.storage.write_transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO batches(batch_id, target_version, state, request_digest, created_at)
                    VALUES(?, ?, 'RUNNING', ?, ?)
                    """,
                    (batch_id, version, request_digest, now),
                )
                for client_id in requested_ids:
                    record = prepared[client_id]
                    connection.execute(
                        """
                        INSERT INTO push_records(client_id, version, batch_id, state, payload_json,
                                                 payload_digest, attempt_count, created_at, updated_at)
                        VALUES(?, ?, ?, 'PENDING', ?, ?, 0, ?, ?)
                        """,
                        (
                            client_id,
                            version,
                            batch_id,
                            canonical_json(record),
                            record["payload_digest"],
                            now,
                            now,
                        ),
                    )
                for client_id in requested_ids:
                    if client_id in validation_errors:
                        self._transition(
                            connection,
                            client_id,
                            version,
                            PushState.ABORTED,
                            validation_errors[client_id],
                        )
                        connection.execute(
                            "UPDATE push_records SET last_error=? WHERE client_id=? AND version=?",
                            (validation_errors[client_id], client_id, version),
                        )
                    else:
                        self._transition(
                            connection,
                            client_id,
                            version,
                            PushState.PUSHED,
                            "dispatched",
                        )
                        connection.execute(
                            "UPDATE push_records SET attempt_count=attempt_count+1 WHERE client_id=? AND version=?",
                            (client_id, version),
                        )
        except sqlite3.IntegrityError as exc:
            self.storage.finish_intent(intent_id, "ABORTED")
            existing = self.storage.query_one(
                "SELECT * FROM batches WHERE target_version=?", (version,)
            )
            if existing is not None:
                return await self._reuse_batch(existing, request_digest)
            raise exc
        except Exception:
            self.storage.finish_intent(intent_id, "ABORTED")
            raise

        if crash_hook is not None:
            crash_hook("after_dispatch_records_committed")

        send_tasks = [
            self._send_one(prepared[client_id])
            for client_id in requested_ids
            if client_id not in validation_errors
        ]
        try:
            outcomes = await asyncio.gather(*send_tasks)
        except BaseException:
            for client_id in requested_ids:
                self._abort_push(client_id, version, "E_PUSH_ROUND_FAILED", "push round interrupted")
            self._mark_batch_aborted(batch_id, "E_PUSH_ROUND_FAILED", "push round interrupted")
            self.storage.finish_intent(intent_id, "ABORTED")
            raise

        outcome_map = {outcome["client_id"]: outcome for outcome in outcomes}
        for client_id in requested_ids:
            if client_id in validation_errors:
                outcome_map[client_id] = {
                    "client_id": client_id,
                    "state": PushState.ABORTED.value,
                    "advanced": False,
                    "code": validation_errors[client_id],
                    "message": "push payload is missing required configuration keys",
                    "elapsed_ms": 0.0,
                }

        report = self._finalize_batch(batch_id, version, intent_id, outcome_map)
        return report

    async def _reuse_batch(self, existing: dict[str, Any], request_digest: str) -> dict[str, Any]:
        if existing["request_digest"] != request_digest:
            raise ConfigHubError(
                ErrorCode.E_CONFLICTING_PUSH,
                "the same client/version was already pushed with different content",
                details={
                    "batch_id": existing["batch_id"],
                    "target_version": existing["target_version"],
                    "existing_request_digest": existing["request_digest"],
                    "received_request_digest": request_digest,
                },
            )
        deadline = time.monotonic() + 65.0
        current = existing
        while current["state"] == "RUNNING" and time.monotonic() < deadline:
            await asyncio.sleep(0.03)
            loaded = self.storage.query_one(
                "SELECT * FROM batches WHERE batch_id=?", (current["batch_id"],)
            )
            if loaded is not None:
                current = loaded
        if current["state"] == "RUNNING":
            raise ConfigHubError(
                ErrorCode.E_BATCH_IN_PROGRESS,
                "another process is still executing this batch",
                details={"batch_id": current["batch_id"]},
            )
        report = json_loads(current["report_json"], {}) if current["report_json"] else {
            "batch_id": current["batch_id"],
            "target_version": int(current["target_version"]),
            "state": current["state"],
        }
        report = dict(report)
        report["reused"] = True
        report["state"] = current["state"]
        return report

    async def _send_one(self, record: dict[str, Any]) -> dict[str, Any]:
        client_id = record["client_id"]
        version = int(record["version"])
        timeout = float(record["timeout_seconds"])
        started = time.perf_counter()
        try:
            # A fresh pool + Connection: close makes timeout handling close the
            # actual loopback TCP connection instead of merely abandoning a wait.
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(timeout, connect=min(timeout, 1.0)),
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
                headers={"Connection": "close"},
            ) as client:
                response = await client.post(record["url"], json=record["body"])
                await response.aread()
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
                if response.status_code != 200:
                    raise ConfigHubError(
                        ErrorCode.E_PUSH_FAILED,
                        f"client endpoint returned HTTP {response.status_code}",
                        details={"client_id": client_id, "status_code": response.status_code},
                    )
                try:
                    acknowledgement = response.json()
                except ValueError as exc:
                    raise ConfigHubError(
                        ErrorCode.E_RESULT_INVALID,
                        "client endpoint did not return valid JSON",
                        details={"client_id": client_id},
                    ) from exc
                if acknowledgement.get("status") != "ACK":
                    raise ConfigHubError(
                        ErrorCode.E_RESULT_INVALID,
                        "client endpoint did not acknowledge the push",
                        details={"client_id": client_id, "response": acknowledgement},
                    )
                result = acknowledgement.get("result")
                if not isinstance(result, dict):
                    raise ConfigHubError(
                        ErrorCode.E_RESULT_INVALID,
                        "client acknowledgement is missing result",
                        details={"client_id": client_id},
                    )
                applied = await asyncio.to_thread(
                    self.apply_ack,
                    client_id,
                    version,
                    result,
                    batch_id=record["batch_id"],
                    expected_target_version=version,
                    source="push_response",
                    elapsed_ms=elapsed_ms,
                )
                return {
                    "client_id": client_id,
                    "state": PushState.APPLIED.value,
                    "advanced": True,
                    "code": "OK",
                    "message": "acknowledged and applied",
                    "elapsed_ms": elapsed_ms,
                    "result_digest": result.get("result_digest"),
                    "ack": applied,
                }
        except httpx.TimeoutException as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
            self._abort_push(
                client_id,
                version,
                ErrorCode.E_PUSH_TIMEOUT,
                "push timed out; loopback connection was closed",
            )
            return {
                "client_id": client_id,
                "state": PushState.ABORTED.value,
                "advanced": False,
                "code": ErrorCode.E_PUSH_TIMEOUT,
                "message": "push timed out and the underlying connection was closed",
                "elapsed_ms": elapsed_ms,
                "timeout_seconds": timeout,
                "connection_closed": True,
                "exception": type(exc).__name__,
            }
        except ConfigHubError as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
            self._abort_push(client_id, version, exc.code, exc.message)
            return {
                "client_id": client_id,
                "state": PushState.ABORTED.value,
                "advanced": False,
                "code": exc.code,
                "message": exc.message,
                "elapsed_ms": elapsed_ms,
                "details": exc.details,
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
            self._abort_push(
                client_id,
                version,
                ErrorCode.E_PUSH_FAILED,
                f"push failed: {type(exc).__name__}: {exc}",
            )
            return {
                "client_id": client_id,
                "state": PushState.ABORTED.value,
                "advanced": False,
                "code": ErrorCode.E_PUSH_FAILED,
                "message": str(exc),
                "elapsed_ms": elapsed_ms,
            }

    def _mark_batch_aborted(self, batch_id: str, code: str, message: str) -> None:
        with self.storage.write_transaction() as connection:
            connection.execute(
                "UPDATE batches SET state='ABORTED', completed_at=? WHERE batch_id=? AND state='RUNNING'",
                (utc_now_iso(), batch_id),
            )
            connection.execute(
                "UPDATE batches SET report_json=? WHERE batch_id=?",
                (
                    canonical_json(
                        {
                            "batch_id": batch_id,
                            "state": "ABORTED",
                            "code": code,
                            "message": message,
                        }
                    ),
                    batch_id,
                ),
            )

    def _finalize_batch(
        self,
        batch_id: str,
        version: int,
        push_intent_id: str,
        outcome_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        rows = self.storage.query_all(
            """
            SELECT p.*, c.applied_version
            FROM push_records p JOIN clients c ON c.client_id=p.client_id
            WHERE p.batch_id=? ORDER BY p.client_id
            """,
            (batch_id,),
        )
        # Any still-in-flight row is durably aborted before the round is sealed.
        for row in rows:
            if row["state"] in {
                PushState.PENDING.value,
                PushState.PUSHED.value,
                PushState.ACKED.value,
            }:
                self._abort_push(
                    row["client_id"],
                    version,
                    "E_PUSH_ROUND_INCOMPLETE",
                    "push round finished without a confirmed client result",
                )

        rows = self.storage.query_all(
            """
            SELECT p.*, c.applied_version
            FROM push_records p JOIN clients c ON c.client_id=p.client_id
            WHERE p.batch_id=? ORDER BY p.client_id
            """,
            (batch_id,),
        )
        advanced: list[dict[str, Any]] = []
        not_advanced: list[dict[str, Any]] = []
        for row in rows:
            result_row = self.storage.query_one(
                "SELECT result_digest FROM results WHERE batch_id=? AND client_id=? AND eligible=1",
                (batch_id, row["client_id"]),
            )
            outcome = outcome_map.get(row["client_id"], {})
            if row["state"] == PushState.APPLIED.value and result_row is not None:
                advanced.append(
                    {
                        "client_id": row["client_id"],
                        "picked_version": version,
                        "result_digest": result_row["result_digest"],
                        "applied_version": row["applied_version"],
                    }
                )
            else:
                code = row["last_error"] or outcome.get("code") or "E_NOT_ADVANCED"
                not_advanced.append(
                    {
                        "client_id": row["client_id"],
                        "state": row["state"],
                        "code": code,
                        "reason": outcome.get("message")
                        or self._reason_for_code(code),
                        "elapsed_ms": outcome.get("elapsed_ms"),
                        "timeout_seconds": outcome.get("timeout_seconds"),
                        "connection_closed": outcome.get("connection_closed"),
                    }
                )

        consistency = self.verify_batch(batch_id)
        report = {
            "ok": True,
            "batch_id": batch_id,
            "target_version": version,
            "state": "COMPLETED",
            "reused": False,
            "advanced": advanced,
            "not_advanced": not_advanced,
            "degraded": bool(not_advanced),
            "degradation_policy": "advance only acknowledged clients; retain last committed version for others",
            "completion_rule": "one batch attempt per snapshot version; duplicate triggers reuse this report",
            "consistency": consistency,
            "visibility_violations": self.storage.visibility_invariant_violations(),
            "completed_at": utc_now_iso(),
        }

        finalize_intent = self.storage.begin_intent(
            "BATCH_FINALIZE", f"batch:{batch_id}", {"batch_id": batch_id, "version": version}
        )
        try:
            with self.storage.write_transaction() as connection:
                connection.execute(
                    "UPDATE batches SET state='COMPLETED', completed_at=?, report_json=? WHERE batch_id=?",
                    (report["completed_at"], canonical_json(report), batch_id),
                )
        except Exception:
            self.storage.finish_intent(finalize_intent, "ABORTED")
            self.storage.finish_intent(push_intent_id, "ABORTED")
            raise
        self.storage.finish_intent(finalize_intent, "COMMITTED")
        self.storage.finish_intent(push_intent_id, "COMMITTED")
        return report

    @staticmethod
    def _reason_for_code(code: str) -> str:
        return {
            ErrorCode.E_PUSH_TIMEOUT: "client push exceeded its independent timeout window",
            ErrorCode.E_STALE_ACK: "client acknowledged a version that was already superseded",
            "E_PUSH_PAYLOAD_INVALID": "snapshot lacks keys required by the client pipeline",
            ErrorCode.E_RESULT_INVALID: "client result failed deterministic verification",
            ErrorCode.E_PUSH_FAILED: "client endpoint failed before acknowledgement",
        }.get(code, "client was not confirmed for the target version")

    # ------------------------------------------------------------------
    # Confirmation / result transaction
    # ------------------------------------------------------------------
    def apply_ack(
        self,
        client_id: str,
        version: int,
        result: dict[str, Any],
        *,
        batch_id: str | None = None,
        expected_target_version: int | None = None,
        source: str = "external",
        elapsed_ms: float | None = None,
    ) -> dict[str, Any]:
        if not isinstance(result, dict):
            raise ConfigHubError(
                ErrorCode.E_RESULT_INVALID, "result must be an object", status_code=422
            )
        intent_id = self.storage.begin_intent(
            "ACK_APPLY",
            f"client:{client_id}:version:{version}",
            {
                "client_id": client_id,
                "version": version,
                "batch_id": batch_id,
                "result_digest": result.get("result_digest"),
                "source": source,
            },
        )
        post_error: ConfigHubError | None = None
        outcome: dict[str, Any] | None = None
        try:
            with self.storage.write_transaction() as connection:
                client_row = connection.execute(
                    "SELECT * FROM clients WHERE client_id=?", (client_id,)
                ).fetchone()
                if client_row is None:
                    raise ConfigHubError(
                        ErrorCode.E_NOT_FOUND,
                        "client is not registered",
                        status_code=404,
                        details={"client_id": client_id},
                    )
                snapshot_row = connection.execute(
                    "SELECT * FROM snapshots WHERE version=?", (version,)
                ).fetchone()
                push_row = connection.execute(
                    "SELECT * FROM push_records WHERE client_id=? AND version=?",
                    (client_id, version),
                ).fetchone()
                latest_row = connection.execute(
                    "SELECT value FROM meta WHERE key='current_version'"
                ).fetchone()
                latest = int(latest_row["value"]) if latest_row else 0
                applied_version = (
                    int(client_row["applied_version"])
                    if client_row["applied_version"] is not None
                    else None
                )
                observed_version = result.get("config_version", version)
                try:
                    observed_version = int(observed_version)
                except (TypeError, ValueError):
                    observed_version = None
                received_digest = result.get("result_digest")
                received_digest = str(received_digest) if received_digest is not None else None
                effective_batch = batch_id or (push_row["batch_id"] if push_row else None)

                if push_row is None:
                    self._insert_ack(
                        connection,
                        client_id,
                        version,
                        effective_batch,
                        accepted=False,
                        code=ErrorCode.E_STALE_ACK,
                        message="no push record exists for this client/version",
                        observed_version=observed_version,
                        result_digest=received_digest,
                        result=result,
                    )
                    post_error = ConfigHubError(
                        ErrorCode.E_STALE_ACK,
                        "acknowledgement has no active push record",
                        details={"client_id": client_id, "version": version},
                    )
                elif applied_version is not None and applied_version > version:
                    self._insert_ack(
                        connection,
                        client_id,
                        version,
                        effective_batch,
                        accepted=False,
                        code=ErrorCode.E_STALE_ACK,
                        message="a newer version is already applied for this client",
                        observed_version=observed_version,
                        result_digest=received_digest,
                        result=result,
                    )
                    self._record_observation(
                        connection,
                        effective_batch,
                        client_id,
                        observed_version,
                        received_digest,
                        result,
                        eligible=False,
                        reason="STALE_ACK_NEWER_VERSION_APPLIED",
                    )
                    self._abort_if_active(connection, client_id, version, ErrorCode.E_STALE_ACK)
                    post_error = ConfigHubError(
                        ErrorCode.E_STALE_ACK,
                        "late acknowledgement cannot overwrite a newer applied version",
                        details={
                            "client_id": client_id,
                            "version": version,
                            "applied_version": applied_version,
                        },
                    )
                elif latest > version:
                    self._insert_ack(
                        connection,
                        client_id,
                        version,
                        effective_batch,
                        accepted=False,
                        code=ErrorCode.E_STALE_ACK,
                        message="a newer snapshot has already replaced this version",
                        observed_version=observed_version,
                        result_digest=received_digest,
                        result=result,
                    )
                    self._record_observation(
                        connection,
                        effective_batch,
                        client_id,
                        observed_version,
                        received_digest,
                        result,
                        eligible=False,
                        reason="STALE_ACK_SUPERSEDED_SNAPSHOT",
                    )
                    self._abort_if_active(connection, client_id, version, ErrorCode.E_STALE_ACK)
                    post_error = ConfigHubError(
                        ErrorCode.E_STALE_ACK,
                        "late acknowledgement was recorded but cannot replace the current version",
                        details={"client_id": client_id, "version": version, "latest_version": latest},
                    )
                else:
                    push_state = PushState(push_row["state"])
                    if push_state is PushState.ABORTED:
                        self._insert_ack(
                            connection,
                            client_id,
                            version,
                            effective_batch,
                            accepted=False,
                            code=ErrorCode.E_STALE_ACK,
                            message="push record was already aborted",
                            observed_version=observed_version,
                            result_digest=received_digest,
                            result=result,
                        )
                        self._record_observation(
                            connection,
                            effective_batch,
                            client_id,
                            observed_version,
                            received_digest,
                            result,
                            eligible=False,
                            reason="STALE_ACK_ABORTED_PUSH",
                        )
                        post_error = ConfigHubError(
                            ErrorCode.E_STALE_ACK,
                            "late acknowledgement was recorded in history only",
                            details={"client_id": client_id, "version": version},
                        )
                    elif push_state is PushState.PENDING:
                        raise ConfigHubError(
                            ErrorCode.E_INVALID_TRANSITION,
                            "PENDING -> ACKED is not a legal committed transition",
                            details={"client_id": client_id, "version": version},
                        )
                    elif push_state is PushState.APPLIED:
                        prior_result = connection.execute(
                            """
                            SELECT result_digest FROM results
                            WHERE batch_id=? AND client_id=? AND version=? AND eligible=1
                            """,
                            (effective_batch, client_id, version),
                        ).fetchone()
                        prior_digest = prior_result["result_digest"] if prior_result else None
                        if prior_digest == received_digest:
                            self._insert_ack(
                                connection,
                                client_id,
                                version,
                                effective_batch,
                                accepted=True,
                                code="DUPLICATE_ACK",
                                message="duplicate acknowledgement is idempotent",
                                observed_version=observed_version,
                                result_digest=received_digest,
                                result=result,
                            )
                            outcome = {
                                "ok": True,
                                "duplicate": True,
                                "client_id": client_id,
                                "version": version,
                                "state": PushState.APPLIED.value,
                                "result_digest": prior_digest,
                                "source": source,
                            }
                        else:
                            self._insert_ack(
                                connection,
                                client_id,
                                version,
                                effective_batch,
                                accepted=False,
                                code=ErrorCode.E_CONFLICTING_ACK,
                                message="duplicate acknowledgement has different result content",
                                observed_version=observed_version,
                                result_digest=received_digest,
                                result=result,
                            )
                            post_error = ConfigHubError(
                                ErrorCode.E_CONFLICTING_ACK,
                                "already-applied push cannot be overwritten by a conflicting acknowledgement",
                                details={"client_id": client_id, "version": version},
                            )
                    elif push_state in {PushState.PUSHED, PushState.ACKED}:
                        if observed_version != version:
                            self._insert_ack(
                                connection,
                                client_id,
                                version,
                                effective_batch,
                                accepted=False,
                                code=ErrorCode.E_STALE_ACK,
                                message="client picked an older configuration version",
                                observed_version=observed_version,
                                result_digest=received_digest,
                                result=result,
                            )
                            self._record_observation(
                                connection,
                                effective_batch,
                                client_id,
                                observed_version,
                                received_digest,
                                result,
                                eligible=False,
                                reason="CLIENT_PICKED_STALE_VERSION",
                            )
                            self._abort_if_active(connection, client_id, version, ErrorCode.E_STALE_ACK)
                            post_error = ConfigHubError(
                                ErrorCode.E_STALE_ACK,
                                "client result used a stale picked version and was not applied",
                                details={
                                    "client_id": client_id,
                                    "target_version": version,
                                    "picked_version": observed_version,
                                },
                            )
                        else:
                            verified = self._verify_result(client_row, snapshot_row, version, result)
                            if not verified:
                                self._insert_ack(
                                    connection,
                                    client_id,
                                    version,
                                    effective_batch,
                                    accepted=False,
                                    code=ErrorCode.E_RESULT_INVALID,
                                    message="result digest does not match deterministic recomputation",
                                    observed_version=observed_version,
                                    result_digest=received_digest,
                                    result=result,
                                )
                                self._abort_if_active(
                                    connection, client_id, version, ErrorCode.E_RESULT_INVALID
                                )
                                post_error = ConfigHubError(
                                    ErrorCode.E_RESULT_INVALID,
                                    "client result failed deterministic verification",
                                    details={"client_id": client_id, "version": version},
                                )
                            else:
                                self._insert_ack(
                                    connection,
                                    client_id,
                                    version,
                                    effective_batch,
                                    accepted=True,
                                    code="ACKED",
                                    message="result verified; applying version atomically",
                                    observed_version=observed_version,
                                    result_digest=received_digest,
                                    result=result,
                                )
                                if push_state is PushState.PUSHED:
                                    self._transition(
                                        connection,
                                        client_id,
                                        version,
                                        PushState.ACKED,
                                        "deterministic result verified",
                                    )
                                self._record_observation(
                                    connection,
                                    effective_batch,
                                    client_id,
                                    version,
                                    received_digest,
                                    result,
                                    eligible=True,
                                    reason=None,
                                )
                                connection.execute(
                                    "UPDATE clients SET applied_version=?, updated_at=? WHERE client_id=?",
                                    (version, utc_now_iso(), client_id),
                                )
                                self._transition(
                                    connection,
                                    client_id,
                                    version,
                                    PushState.APPLIED,
                                    "client version advanced atomically with acknowledgement",
                                )
                                outcome = {
                                    "ok": True,
                                    "duplicate": False,
                                    "client_id": client_id,
                                    "version": version,
                                    "state": PushState.APPLIED.value,
                                    "picked_version": observed_version,
                                    "result_digest": received_digest,
                                    "source": source,
                                    "elapsed_ms": elapsed_ms,
                                }
                    else:
                        raise ConfigHubError(
                            ErrorCode.E_INVALID_TRANSITION,
                            f"unhandled push state {push_state.value}",
                        )
        except Exception:
            self.storage.finish_intent(intent_id, "ABORTED")
            raise

        self.storage.finish_intent(intent_id, "COMMITTED")
        if post_error is not None:
            raise post_error
        return outcome or {"ok": True, "client_id": client_id, "version": version}

    def _abort_if_active(self, connection: Any, client_id: str, version: int, code: str) -> None:
        row = connection.execute(
            "SELECT state FROM push_records WHERE client_id=? AND version=?",
            (client_id, version),
        ).fetchone()
        if row is not None and PushState(row["state"]) in {
            PushState.PENDING,
            PushState.PUSHED,
            PushState.ACKED,
        }:
            self._transition(connection, client_id, version, PushState.ABORTED, code)
            connection.execute(
                "UPDATE push_records SET last_error=? WHERE client_id=? AND version=?",
                (code, client_id, version),
            )

    def _insert_ack(
        self,
        connection: Any,
        client_id: str,
        version: int,
        batch_id: str | None,
        *,
        accepted: bool,
        code: str,
        message: str,
        observed_version: int | None,
        result_digest: str | None,
        result: dict[str, Any] | None,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO ack_records(client_id, version, batch_id, accepted, code, message,
                                    observed_version, result_digest, result_json, received_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                version,
                batch_id,
                1 if accepted else 0,
                code,
                message,
                observed_version,
                result_digest,
                canonical_json(result) if result is not None else None,
                utc_now_iso(),
            ),
        )
        return int(cursor.lastrowid)

    def _record_observation(
        self,
        connection: Any,
        batch_id: str | None,
        client_id: str,
        observed_version: int | None,
        result_digest: str | None,
        result: dict[str, Any] | None,
        *,
        eligible: bool,
        reason: str | None,
    ) -> None:
        if batch_id is None or observed_version is None or result is None or result_digest is None:
            return
        snapshot_exists = connection.execute(
            "SELECT 1 FROM snapshots WHERE version=?", (observed_version,)
        ).fetchone()
        batch_exists = connection.execute(
            "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if snapshot_exists is None or batch_exists is None:
            return
        connection.execute(
            """
            INSERT INTO results(batch_id, client_id, version, result_digest, result_json,
                                eligible, exclusion_reason, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id, client_id, version) DO NOTHING
            """,
            (
                batch_id,
                client_id,
                observed_version,
                result_digest,
                canonical_json(result),
                1 if eligible else 0,
                reason,
                utc_now_iso(),
            ),
        )

    def _verify_result(
        self,
        client_row: dict[str, Any],
        snapshot_row: dict[str, Any],
        version: int,
        result: dict[str, Any],
    ) -> bool:
        keys = json_loads(client_row["config_keys_json"], [])
        config = json_loads(snapshot_row["config_json"], {})
        effective = {key: config[key] for key in keys if key in config}
        spec = SampleSpec(
            seed=int(client_row["sample_seed"]),
            sample_size=int(client_row["sample_size"]),
            feature_count=int(client_row["feature_count"]),
        )
        try:
            expected = compute_pipeline(spec, version, effective)
            observed_version = int(result.get("config_version", -1))
        except (ConfigHubError, TypeError, ValueError):
            return False
        return (
            result.get("result_digest") == expected["result_digest"]
            and observed_version == version
        )

    # ------------------------------------------------------------------
    # Result consistency verification
    # ------------------------------------------------------------------
    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = self.storage.query_one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        if row is None:
            return None
        result = dict(row)
        result["report"] = json_loads(row["report_json"], None) if row["report_json"] else None
        return result

    def verify_batch(self, batch_id: str) -> dict[str, Any]:
        batch = self.storage.query_one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        if batch is None:
            raise ConfigHubError(
                ErrorCode.E_NOT_FOUND,
                "batch does not exist",
                status_code=404,
                details={"batch_id": batch_id},
            )
        target_version = int(batch["target_version"])
        result_rows = self.storage.query_all(
            """
            SELECT r.*,
                   (SELECT a.id FROM ack_records a
                    WHERE a.client_id=r.client_id AND a.version=r.version
                      AND a.batch_id=r.batch_id AND a.accepted=1
                    ORDER BY a.id DESC LIMIT 1) AS confirmation_id,
                   p.state AS push_state,
                   c.applied_version AS client_current_version,
                   s.version AS snapshot_version
            FROM results r
            LEFT JOIN push_records p
              ON p.client_id=r.client_id AND p.version=r.version
            LEFT JOIN clients c ON c.client_id=r.client_id
            LEFT JOIN snapshots s ON s.version=r.version
            WHERE r.batch_id=? ORDER BY r.client_id, r.version
            """,
            (batch_id,),
        )
        push_rows = self.storage.query_all(
            "SELECT client_id, version, state, last_error FROM push_records WHERE batch_id=? ORDER BY client_id",
            (batch_id,),
        )
        observed: list[dict[str, Any]] = []
        for row in result_rows:
            result_json = json_loads(row["result_json"], {})
            push_state = row["push_state"]
            client_version = row["client_current_version"]
            client_version_matches = (
                client_version is not None and int(client_version) >= int(row["version"])
            )
            eligible = (
                bool(row["eligible"])
                and int(row["version"]) == target_version
                and row["confirmation_id"] is not None
                and push_state == PushState.APPLIED.value
                and row["snapshot_version"] is not None
                and client_version_matches
            )
            observed.append(
                {
                    "client_id": row["client_id"],
                    "picked_version": int(row["version"]),
                    "result_digest": row["result_digest"],
                    "result": result_json,
                    "confirmed": row["confirmation_id"] is not None,
                    "eligible": eligible,
                    "stored_eligible": bool(row["eligible"]),
                    "exclusion_reason": row["exclusion_reason"],
                    "push_state": push_state,
                    "snapshot_exists": row["snapshot_version"] is not None,
                    "client_applied_version": client_version,
                }
            )

        eligible = [row for row in observed if row["eligible"]]
        reference = eligible[0] if eligible else (observed[0] if observed else None)
        for row in observed:
            row["difference_fields"] = (
                differing_fields(reference["result"], row["result"])
                if reference is not None and row["result_digest"] != reference["result_digest"]
                else []
            )
            row["inconsistent_with_reference"] = bool(row["difference_fields"])

        inconsistent_details = [
            {
                "client_id": row["client_id"],
                "picked_version": row["picked_version"],
                "result_digest": row["result_digest"],
                "reference_client_id": reference["client_id"] if reference else None,
                "reference_version": reference["picked_version"] if reference else None,
                "reference_result_digest": reference["result_digest"] if reference else None,
                "difference_fields": row["difference_fields"],
                "confirmed": row["confirmed"],
                "eligible_for_final": row["eligible"],
                "exclusion_reason": row["exclusion_reason"],
            }
            for row in observed
            if row["inconsistent_with_reference"]
        ]

        eligible_digests = sorted({row["result_digest"] for row in eligible})
        if not eligible:
            conclusion = "INSUFFICIENT_CONFIRMATIONS"
        elif len(eligible_digests) > 1:
            conclusion = "INCONSISTENT"
        else:
            conclusion = "CONSISTENT"
        observed_digests = sorted({row["result_digest"] for row in observed})
        observed_versions = sorted({row["picked_version"] for row in observed})
        if not observed:
            observed_conclusion = "INSUFFICIENT_OBSERVATIONS"
        elif len(observed_digests) > 1:
            observed_conclusion = "INCONSISTENT"
        else:
            observed_conclusion = "CONSISTENT"

        result_clients = {row["client_id"] for row in result_rows}
        unconfirmed_clients = [
            {
                "client_id": row["client_id"],
                "state": row["state"],
                "code": row["last_error"] or "E_NO_CONFIRMED_RESULT",
            }
            for row in push_rows
            if row["client_id"] not in result_clients
        ]
        excluded = [
            {
                "client_id": row["client_id"],
                "picked_version": row["picked_version"],
                "reason": row["exclusion_reason"]
                or ("VERSION_NOT_TARGET" if int(row["picked_version"]) != target_version else "UNCONFIRMED"),
            }
            for row in observed
            if not row["eligible"]
        ]
        coverage_complete = len(eligible) == len(push_rows)
        overall_consistent = (
            conclusion == "CONSISTENT"
            and coverage_complete
            and observed_conclusion == "CONSISTENT"
        )

        return {
            "batch_id": batch_id,
            "target_version": target_version,
            "conclusion": conclusion,
            "consistent": overall_consistent,
            "final_consistent": conclusion == "CONSISTENT",
            "observed_conclusion": observed_conclusion,
            "observed_consistent": observed_conclusion == "CONSISTENT",
            "observed_versions": observed_versions,
            "mixed_versions": len(observed_versions) > 1,
            "final_version": target_version if eligible else None,
            "final_result_digest": eligible_digests[0] if conclusion == "CONSISTENT" else None,
            "eligible_clients": [row["client_id"] for row in eligible],
            "eligible_versions": sorted({row["picked_version"] for row in eligible}),
            "coverage_complete": coverage_complete,
            "degraded": not coverage_complete,
            "excluded_results": excluded,
            "unconfirmed_clients": unconfirmed_clients,
            "inconsistent_details": inconsistent_details,
            "observed_results": observed,
            "rule": "final conclusion includes only results whose snapshot exists, acknowledgement was accepted, push state is APPLIED, and version equals the batch target",
        }

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def recover(self) -> dict[str, Any]:
        recovery_intent = self.storage.begin_intent(
            "RECOVERY_SCAN", "startup", {"source": "committed SQLite facts"}
        )
        started_before = self.storage.list_started_intents()
        started_before = [row for row in started_before if row["intent_id"] != recovery_intent]

        running_batches = self.storage.query_all(
            "SELECT * FROM batches WHERE state='RUNNING' ORDER BY created_at"
        )
        aborted_batches: list[dict[str, Any]] = []
        for batch in running_batches:
            batch_id = batch["batch_id"]
            with self.storage.write_transaction() as connection:
                rows = connection.execute(
                    "SELECT client_id, state, last_error FROM push_records WHERE batch_id=?",
                    (batch_id,),
                ).fetchall()
                aborted_clients: list[dict[str, Any]] = []
                for row in rows:
                    state = PushState(row["state"])
                    if state in {PushState.PENDING, PushState.PUSHED, PushState.ACKED}:
                        self._transition(
                            connection,
                            row["client_id"],
                            int(batch["target_version"]),
                            PushState.ABORTED,
                            "RECOVERY_INCOMPLETE_BATCH",
                        )
                        connection.execute(
                            "UPDATE push_records SET last_error='RECOVERY_INCOMPLETE_BATCH' WHERE client_id=? AND version=?",
                            (row["client_id"], batch["target_version"]),
                        )
                        state = PushState.ABORTED
                    aborted_clients.append(
                        {"client_id": row["client_id"], "state": state.value}
                    )
                completed_at = utc_now_iso()
                connection.execute(
                    "UPDATE batches SET state='ABORTED', completed_at=?, report_json=? WHERE batch_id=?",
                    (
                        completed_at,
                        canonical_json(
                            {
                                "batch_id": batch_id,
                                "target_version": int(batch["target_version"]),
                                "state": "ABORTED",
                                "reason": "RECOVERY_INCOMPLETE_BATCH",
                                "clients": aborted_clients,
                            }
                        ),
                        batch_id,
                    ),
                )
            aborted_batches.append(
                {
                    "batch_id": batch_id,
                    "target_version": int(batch["target_version"]),
                    "reason": "RECOVERY_INCOMPLETE_BATCH",
                    "clients": aborted_clients,
                }
            )

        intent_resolutions: list[dict[str, Any]] = []
        for intent in started_before:
            resolution = self._resolve_intent(intent)
            self.storage.finish_intent(intent["intent_id"], resolution)
            intent_resolutions.append(
                {
                    "intent_id": intent["intent_id"],
                    "kind": intent["kind"],
                    "aggregate_key": intent["aggregate_key"],
                    "resolution": resolution,
                }
            )

        clients = self.storage.query_all(
            """
            SELECT c.client_id, c.applied_version,
                   (SELECT MAX(p.version) FROM push_records p
                    WHERE p.client_id=c.client_id AND p.state='APPLIED') AS last_applied_push_version,
                   (SELECT COUNT(*) FROM results r
                    WHERE r.client_id=c.client_id AND r.eligible=1
                      AND r.version=c.applied_version) AS confirmed_result_count
            FROM clients c ORDER BY c.client_id
            """
        )
        client_facts: list[dict[str, Any]] = []
        for row in clients:
            applied = row["applied_version"]
            applied_push = row["last_applied_push_version"]
            fact_consistent = applied is None or (
                applied_push is not None
                and int(applied_push) >= int(applied)
                and int(row["confirmed_result_count"]) > 0
            )
            client_facts.append(
                {
                    "client_id": row["client_id"],
                    "applied_version": applied,
                    "last_applied_push_version": applied_push,
                    "confirmed_result_count": int(row["confirmed_result_count"]),
                    "fact_consistent": fact_consistent,
                }
            )
        client_facts_consistent = all(row["fact_consistent"] for row in client_facts)
        push_states = self.storage.query_all(
            """
            SELECT client_id, version, state, last_error FROM push_records
            WHERE state IN ('PENDING','PUSHED','ACKED') ORDER BY client_id, version
            """
        )
        violations = self.storage.visibility_invariant_violations()
        report = {
            "ok": not violations and client_facts_consistent,
            "recovered_at": utc_now_iso(),
            "basis": "only committed SQLite rows; uncommitted writes are absent by transaction semantics",
            "incomplete_intents": intent_resolutions,
            "aborted_batches": aborted_batches,
            "client_last_complete_versions": client_facts,
            "client_facts_consistent": client_facts_consistent,
            "remaining_nonterminal_pushes": push_states,
            "visibility_violations": violations,
            "conclusion": (
                "NO_RECOVERY_REQUIRED"
                if not aborted_batches and not intent_resolutions and not push_states
                else "RECOVERED_INCOMPLETE_WORK"
            ),
        }
        report_intent = self.storage.begin_intent(
            "RECOVERY_REPORT", "startup", {"conclusion": report["conclusion"]}
        )
        self.storage.save_recovery_report(report)
        self.storage.finish_intent(report_intent, "COMMITTED")
        self.storage.finish_intent(recovery_intent, "COMMITTED")
        return report

    def _resolve_intent(self, intent: dict[str, Any]) -> str:
        kind = intent["kind"]
        payload = intent.get("payload", {})
        if kind == "SNAPSHOT_SUBMIT":
            version = payload.get("version")
            row = self.storage.query_one("SELECT digest FROM snapshots WHERE version=?", (version,))
            return "RECONCILED" if row and row["digest"] == payload.get("digest") else "ABORTED"
        if kind == "PUSH_ROUND":
            batch_id = payload.get("batch_id")
            row = self.storage.query_one("SELECT state FROM batches WHERE batch_id=?", (batch_id,))
            if row and row["state"] in {"COMPLETED", "ABORTED"}:
                return "RECONCILED"
            return "ABORTED"
        if kind == "BATCH_FINALIZE":
            batch_id = payload.get("batch_id")
            row = self.storage.query_one("SELECT state FROM batches WHERE batch_id=?", (batch_id,))
            return "RECONCILED" if row and row["state"] == "COMPLETED" else "ABORTED"
        if kind == "ACK_APPLY":
            row = self.storage.query_one(
                "SELECT 1 FROM results WHERE client_id=? AND version=? AND eligible=1",
                (payload.get("client_id"), payload.get("version")),
            )
            return "RECONCILED" if row else "ABORTED"
        if kind == "CLIENT_UPSERT":
            row = self.storage.query_one(
                "SELECT 1 FROM clients WHERE client_id=?", (payload.get("client_id"),)
            )
            return "RECONCILED" if row else "ABORTED"
        if kind == "STATE_TRANSITION":
            return "ABORTED"
        return "ABORTED"

