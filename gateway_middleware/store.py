from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .config import Settings
from .errors import (
    E_BATCH_NOT_FOUND,
    E_INVALID_TRANSITION,
    E_REQUEST_NOT_FOUND,
    E_SNAPSHOT_INVALID,
    DomainError,
)
from .models import (
    AuthRegistry,
    BatchResult,
    BucketDelta,
    Decision,
    QuotaGroup,
    TokenState,
    WindowEstimate,
    canonical_json,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    status TEXT NOT NULL CHECK (status = 'COMMITTED'),
    kind TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_groups (
    group_name TEXT PRIMARY KEY,
    short_limit_weight INTEGER NOT NULL CHECK (short_limit_weight > 0),
    long_limit_weight INTEGER NOT NULL CHECK (long_limit_weight > 0),
    target_weight_per_sec INTEGER NOT NULL CHECK (target_weight_per_sec > 0)
);

CREATE TABLE IF NOT EXISTS client_groups (
    client_id TEXT PRIMARY KEY,
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_name)
);

CREATE TABLE IF NOT EXISTS token_states (
    token_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_name),
    expires_at_ms INTEGER NOT NULL,
    digest_hex TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    issued_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'ABORTED')),
    base_version INTEGER,
    commit_version INTEGER,
    intent_id TEXT,
    event_count INTEGER NOT NULL,
    decided_event_count INTEGER NOT NULL DEFAULT 0,
    reused_event_count INTEGER NOT NULL DEFAULT 0,
    auth_denied_count INTEGER NOT NULL DEFAULT 0,
    token_expired_count INTEGER NOT NULL DEFAULT 0,
    rate_limited_count INTEGER NOT NULL DEFAULT 0,
    allowed_count INTEGER NOT NULL DEFAULT 0,
    submitted_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'ROLLED_BACK')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id);

CREATE TABLE IF NOT EXISTS batch_events (
    batch_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    event_fingerprint TEXT NOT NULL,
    PRIMARY KEY (batch_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_batch_events_request ON batch_events(request_id);

CREATE TABLE IF NOT EXISTS decisions (
    request_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    event_fingerprint TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    quota_group TEXT,
    weight INTEGER NOT NULL,
    auth_ok INTEGER NOT NULL,
    auth_code TEXT,
    auth_reason TEXT NOT NULL,
    allowed INTEGER NOT NULL,
    decision_code TEXT NOT NULL,
    decision_reasons_json TEXT NOT NULL,
    rate_evaluated INTEGER NOT NULL,
    trigger_window TEXT,
    short_event_count INTEGER,
    short_exact_weight INTEGER,
    short_estimated_weight INTEGER,
    short_confidence_low INTEGER,
    short_confidence_high INTEGER,
    short_sample_rate_num INTEGER,
    short_sample_rate_den INTEGER,
    short_sample_method TEXT,
    short_exact_limit INTEGER,
    short_max_window_events INTEGER,
    short_variance INTEGER,
    long_event_count INTEGER,
    long_exact_weight INTEGER,
    long_estimated_weight INTEGER,
    long_confidence_low INTEGER,
    long_confidence_high INTEGER,
    long_sample_rate_num INTEGER,
    long_sample_rate_den INTEGER,
    long_sample_method TEXT,
    long_exact_limit INTEGER,
    long_max_window_events INTEGER,
    long_variance INTEGER,
    short_quota INTEGER,
    long_quota INTEGER,
    quota_scale_ppm INTEGER,
    read_version INTEGER NOT NULL,
    window_version INTEGER NOT NULL,
    token_version INTEGER NOT NULL,
    commit_version INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_batch ON decisions(batch_id);
CREATE INDEX IF NOT EXISTS idx_decisions_client ON decisions(client_id, route_key);

CREATE TABLE IF NOT EXISTS window_events (
    global_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    timestamp_ms INTEGER NOT NULL,
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    weight INTEGER NOT NULL,
    admitted INTEGER NOT NULL,
    commit_version INTEGER NOT NULL,
    UNIQUE (batch_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_window_events_key_time
    ON window_events(route_key, client_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_window_events_time ON window_events(timestamp_ms);

CREATE TABLE IF NOT EXISTS window_buckets (
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    window_name TEXT NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    bucket_end_ms INTEGER NOT NULL,
    version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    weight_sum INTEGER NOT NULL,
    admitted_count INTEGER NOT NULL,
    admitted_weight_sum INTEGER NOT NULL,
    PRIMARY KEY (route_key, client_id, window_name, bucket_start_ms)
);

CREATE TABLE IF NOT EXISTS quota_states (
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    quota_group TEXT NOT NULL,
    short_limit_weight INTEGER NOT NULL,
    long_limit_weight INTEGER NOT NULL,
    scale_ppm INTEGER NOT NULL,
    updated_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (route_key, client_id)
);

CREATE TABLE IF NOT EXISTS quota_state_history (
    version INTEGER NOT NULL,
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    quota_group TEXT NOT NULL,
    short_limit_weight INTEGER NOT NULL,
    long_limit_weight INTEGER NOT NULL,
    scale_ppm INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (version, route_key, client_id)
);

CREATE TABLE IF NOT EXISTS window_bucket_deltas (
    version INTEGER NOT NULL,
    route_key TEXT NOT NULL,
    client_id TEXT NOT NULL,
    window_name TEXT NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    bucket_end_ms INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    weight_sum INTEGER NOT NULL,
    admitted_count INTEGER NOT NULL,
    admitted_weight_sum INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (version, route_key, client_id, window_name, bucket_start_ms)
);

CREATE INDEX IF NOT EXISTS idx_bucket_deltas_lookup
    ON window_bucket_deltas(route_key, client_id, window_name, bucket_start_ms, version);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    created_at_ms INTEGER NOT NULL,
    state_version INTEGER NOT NULL,
    scanned_intents INTEGER NOT NULL,
    rolled_back_batches INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    complete_exit_report INTEGER NOT NULL
);
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class RecoveryReport:
    report_id: str
    created_at_ms: int
    state_version: int
    scanned_intents: int
    rolled_back_batches: int
    details: dict[str, Any]
    complete_exit_report: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "created_at_ms": self.created_at_ms,
            "state_version": self.state_version,
            "scanned_intents": self.scanned_intents,
            "rolled_back_batches": self.rolled_back_batches,
            "details": self.details,
            "complete_exit_report": self.complete_exit_report,
            "facts_only": True,
        }


class Store:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db_path = Path(settings.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=self.settings.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.settings.busy_timeout_ms}")
        return connection

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("ROLLBACK")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('state_version', '0')"
            )
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('token_seed_version', '0')"
            )
            connection.execute(
                "INSERT OR IGNORE INTO snapshots(version, status, kind, created_at_ms) "
                "VALUES (0, 'COMMITTED', 'genesis', ?)",
                (_now_ms(),),
            )
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def current_version(self) -> int:
        with self.read() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = 'state_version'"
            ).fetchone()
            if row is None:
                raise DomainError(E_SNAPSHOT_INVALID, "state_version metadata is missing")
            return int(row["value"])

    def validate_snapshot(self, version: int | None = None) -> int:
        with self.read() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = 'state_version'"
            ).fetchone()
            if row is None:
                raise DomainError(E_SNAPSHOT_INVALID, "state_version metadata is missing")
            current = int(row["value"])
            selected = current if version is None else int(version)
            if selected < 0 or selected > current:
                raise DomainError(
                    E_SNAPSHOT_INVALID,
                    "requested snapshot version is outside the committed range",
                    {"requested": selected, "current": current},
                )
            count = int(
                connection.execute("SELECT COUNT(*) AS n FROM snapshots").fetchone()["n"]
            )
            if count != current + 1:
                raise DomainError(
                    E_SNAPSHOT_INVALID,
                    "snapshot table has gaps or is incomplete",
                    {"observed_rows": count, "current": current},
                )
            seen = connection.execute(
                "SELECT version FROM snapshots WHERE version = ?", (selected,)
            ).fetchone()
            if seen is None:
                raise DomainError(
                    E_SNAPSHOT_INVALID,
                    "requested snapshot is not committed",
                    {"requested": selected},
                )
            return selected

    def seed_local_registry(
        self,
        *,
        quota_groups: Sequence[QuotaGroup],
        client_groups: dict[str, str],
        tokens: Sequence[TokenState],
        allow_existing: bool = False,
    ) -> int:
        self.initialize()
        with self.write() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = 'state_version'"
            ).fetchone()
            base_version = int(row["value"])
            token_count = int(
                connection.execute("SELECT COUNT(*) AS n FROM token_states").fetchone()["n"]
            )
            group_count = int(
                connection.execute("SELECT COUNT(*) AS n FROM quota_groups").fetchone()["n"]
            )
            if allow_existing and (token_count or group_count):
                return base_version
            if token_count or group_count:
                raise DomainError(
                    E_INVALID_TRANSITION,
                    "registry is not empty; refusing to replace a pinned token snapshot",
                    {"current_version": base_version},
                )
            for group in quota_groups:
                connection.execute(
                    "INSERT INTO quota_groups(group_name, short_limit_weight, "
                    "long_limit_weight, target_weight_per_sec) VALUES (?, ?, ?, ?)",
                    (
                        group.name,
                        group.short_limit_weight,
                        group.long_limit_weight,
                        group.target_weight_per_sec,
                    ),
                )
            for client_id, group_name in sorted(client_groups.items()):
                connection.execute(
                    "INSERT INTO client_groups(client_id, quota_group) VALUES (?, ?)",
                    (client_id, group_name),
                )
            for token in tokens:
                connection.execute(
                    "INSERT INTO token_states(token_id, client_id, quota_group, "
                    "expires_at_ms, digest_hex, active, issued_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        token.token_id,
                        token.client_id,
                        token.quota_group,
                        token.expires_at_ms,
                        token.digest_hex,
                        1 if token.active else 0,
                        token.issued_at_ms,
                    ),
                )
            target_version = base_version + 1
            now = _now_ms()
            connection.execute(
                "INSERT INTO snapshots(version, status, kind, created_at_ms) "
                "VALUES (?, 'COMMITTED', 'token_seed', ?)",
                (target_version, now),
            )
            connection.execute(
                "UPDATE meta SET value = ? WHERE key = 'state_version'",
                (str(target_version),),
            )
            connection.execute(
                "UPDATE meta SET value = ? WHERE key = 'token_seed_version'",
                (str(target_version),),
            )
            return target_version

    def load_auth_registry(self, version: int) -> AuthRegistry:
        self.validate_snapshot(version)
        with self.read() as connection:
            seed_row = connection.execute(
                "SELECT value FROM meta WHERE key = 'token_seed_version'"
            ).fetchone()
            token_seed_version = int(seed_row["value"]) if seed_row else 0
            if version < token_seed_version:
                return AuthRegistry(
                    tokens={},
                    client_groups={},
                    quota_groups={},
                    token_version=version,
                    local_secret=self.settings.local_secret,
                )
            groups = {
                row["group_name"]: QuotaGroup(
                    name=row["group_name"],
                    short_limit_weight=int(row["short_limit_weight"]),
                    long_limit_weight=int(row["long_limit_weight"]),
                    target_weight_per_sec=int(row["target_weight_per_sec"]),
                )
                for row in connection.execute("SELECT * FROM quota_groups")
            }
            clients = {
                row["client_id"]: row["quota_group"]
                for row in connection.execute("SELECT client_id, quota_group FROM client_groups")
            }
            tokens = {
                row["token_id"]: TokenState(
                    token_id=row["token_id"],
                    client_id=row["client_id"],
                    quota_group=row["quota_group"],
                    expires_at_ms=int(row["expires_at_ms"]),
                    digest_hex=row["digest_hex"],
                    active=bool(row["active"]),
                    issued_at_ms=int(row["issued_at_ms"]),
                )
                for row in connection.execute("SELECT * FROM token_states")
            }
        return AuthRegistry(
            tokens=tokens,
            client_groups=clients,
            quota_groups=groups,
            token_version=version,
            local_secret=self.settings.local_secret,
        )

    def get_batch_row(self, batch_id: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_existing_requests(self, request_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        if not request_ids:
            return output
        with self.read() as connection:
            for chunk in _chunks(list(dict.fromkeys(request_ids)), 400):
                placeholders = ",".join("?" for _ in chunk)
                rows = connection.execute(
                    f"SELECT request_id, event_fingerprint FROM decisions "
                    f"WHERE request_id IN ({placeholders})",
                    chunk,
                )
                for row in rows:
                    output[row["request_id"]] = dict(row)
        return output

    def load_quota_states(
        self, keys: Iterable[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        output: dict[tuple[str, str], dict[str, Any]] = {}
        key_list = list(keys)
        if not key_list:
            return output
        with self.read() as connection:
            for chunk in _chunks(key_list, 200):
                clauses = " OR ".join("(route_key = ? AND client_id = ?)" for _ in chunk)
                params: list[str] = []
                for route_key, client_id in chunk:
                    params.extend((route_key, client_id))
                rows = connection.execute(
                    f"SELECT * FROM quota_states WHERE {clauses}", params
                )
                for row in rows:
                    output[(row["route_key"], row["client_id"])] = dict(row)
        return output

    def load_window_events(
        self,
        keys: Sequence[tuple[str, str]],
        *,
        min_timestamp_ms: int,
        max_timestamp_ms: int,
        version: int | None = None,
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        selected_version = self.validate_snapshot(version)
        output: dict[tuple[str, str], list[dict[str, Any]]] = {key: [] for key in keys}
        if not keys:
            return output
        with self.read() as connection:
            for chunk in _chunks(list(keys), 150):
                values = ",".join("(?, ?)" for _ in chunk)
                params: list[Any] = [min_timestamp_ms, max_timestamp_ms, selected_version]
                for route_key, client_id in chunk:
                    params.extend((route_key, client_id))
                rows = connection.execute(
                    "SELECT global_seq, batch_id, event_seq, request_id, timestamp_ms, "
                    "route_key, client_id, weight, admitted, commit_version "
                    "FROM window_events "
                    "WHERE timestamp_ms >= ? AND timestamp_ms <= ? AND commit_version <= ? "
                    f"AND (route_key, client_id) IN (VALUES {values}) "
                    "ORDER BY timestamp_ms, global_seq",
                    params,
                )
                for row in rows:
                    item = dict(row)
                    output.setdefault((row["route_key"], row["client_id"]), []).append(item)
        return output

    def load_events_for_key(
        self,
        route_key: str,
        client_id: str,
        *,
        min_timestamp_ms: int,
        max_timestamp_ms: int,
        version: int,
    ) -> list[dict[str, Any]]:
        self.validate_snapshot(version)
        with self.read() as connection:
            rows = connection.execute(
                "SELECT global_seq, batch_id, event_seq, request_id, timestamp_ms, "
                "route_key, client_id, weight, admitted, commit_version "
                "FROM window_events WHERE route_key = ? AND client_id = ? "
                "AND timestamp_ms >= ? AND timestamp_ms <= ? AND commit_version <= ? "
                "ORDER BY timestamp_ms, global_seq",
                (route_key, client_id, min_timestamp_ms, max_timestamp_ms, version),
            )
            return [dict(row) for row in rows]

    def create_intent(
        self,
        *,
        batch_id: str,
        fingerprint: str,
        base_version: int,
        target_version: int,
        event_count: int,
        payload: dict[str, Any],
    ) -> str:
        intent_id = uuid.uuid4().hex
        now = _now_ms()
        with self.write() as connection:
            current = int(
                connection.execute(
                    "SELECT value FROM meta WHERE key = 'state_version'"
                ).fetchone()["value"]
            )
            if current != base_version:
                raise DomainError(
                    E_SNAPSHOT_INVALID,
                    "state version changed before intent creation",
                    {"expected": base_version, "observed": current},
                )
            connection.execute(
                "INSERT INTO batches(batch_id, fingerprint, status, base_version, "
                "commit_version, intent_id, event_count, submitted_at_ms) "
                "VALUES (?, ?, 'PENDING', ?, NULL, ?, ?, ?)",
                (batch_id, fingerprint, base_version, intent_id, event_count, now),
            )
            connection.execute(
                "INSERT INTO intents(intent_id, batch_id, fingerprint, base_version, "
                "target_version, payload_json, status, created_at_ms, updated_at_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)",
                (
                    intent_id,
                    batch_id,
                    fingerprint,
                    base_version,
                    target_version,
                    canonical_json(payload),
                    now,
                    now,
                ),
            )
        return intent_id

    def persist_commit(
        self,
        *,
        batch_id: str,
        intent_id: str,
        base_version: int,
        target_version: int,
        decisions: Sequence[Decision],
        window_events: Sequence[tuple[Any, ...]],
        bucket_deltas: Sequence[BucketDelta],
        quota_states: Sequence[dict[str, Any]],
        batch_event_rows: Sequence[dict[str, Any]],
        summary: dict[str, int],
    ) -> None:
        now = _now_ms()
        with self.write() as connection:
            current = int(
                connection.execute(
                    "SELECT value FROM meta WHERE key = 'state_version'"
                ).fetchone()["value"]
            )
            if current != base_version:
                raise DomainError(
                    E_SNAPSHOT_INVALID,
                    "state version changed before committing the batch",
                    {"expected": base_version, "observed": current},
                )
            intent = connection.execute(
                "SELECT status FROM intents WHERE intent_id = ? AND batch_id = ?",
                (intent_id, batch_id),
            ).fetchone()
            if intent is None or intent["status"] != "PENDING":
                raise DomainError(
                    E_INVALID_TRANSITION,
                    "batch intent is missing or no longer pending",
                    {"batch_id": batch_id, "intent_id": intent_id},
                )

            connection.executemany(
                "INSERT INTO batch_events(batch_id, event_seq, request_id, event_fingerprint) "
                "VALUES (?, ?, ?, ?)",
                (
                    (batch_id, event_seq, request_id, event_fingerprint)
                    for event_seq, request_id, event_fingerprint in batch_event_rows
                ),
            )

            connection.executemany(
                _DECISION_INSERT_SQL,
                (_decision_tuple(decision) for decision in decisions if not decision.reused),
            )

            if window_events:
                connection.executemany(
                    "INSERT INTO window_events(batch_id, event_seq, request_id, "
                    "timestamp_ms, route_key, client_id, weight, admitted, commit_version) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        (
                            event[0],
                            event[1],
                            event[2],
                            event[3],
                            event[4],
                            event[5],
                            event[6],
                            1 if event[7] else 0,
                            target_version,
                        )
                        for event in window_events
                    ),
                )

            if bucket_deltas:
                connection.executemany(
                    "INSERT INTO window_buckets(route_key, client_id, window_name, "
                    "bucket_start_ms, bucket_end_ms, version, updated_at_ms, event_count, "
                    "weight_sum, admitted_count, admitted_weight_sum) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(route_key, client_id, window_name, bucket_start_ms) DO UPDATE SET "
                    "bucket_end_ms = excluded.bucket_end_ms, "
                    "version = excluded.version, "
                    "updated_at_ms = MAX(window_buckets.updated_at_ms, excluded.updated_at_ms), "
                    "event_count = window_buckets.event_count + excluded.event_count, "
                    "weight_sum = window_buckets.weight_sum + excluded.weight_sum, "
                    "admitted_count = window_buckets.admitted_count + excluded.admitted_count, "
                    "admitted_weight_sum = window_buckets.admitted_weight_sum + "
                    "excluded.admitted_weight_sum",
                    [
                        (
                            delta.route_key,
                            delta.client_id,
                            delta.window_name,
                            delta.bucket_start_ms,
                            delta.bucket_end_ms,
                            target_version,
                            delta.updated_at_ms,
                            delta.event_count,
                            delta.weight_sum,
                            delta.admitted_count,
                            delta.admitted_weight_sum,
                        )
                        for delta in bucket_deltas
                    ],
                )

            if bucket_deltas:
                connection.executemany(
                    "INSERT INTO window_bucket_deltas(version, route_key, client_id, "
                    "window_name, bucket_start_ms, bucket_end_ms, event_count, weight_sum, "
                    "admitted_count, admitted_weight_sum, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            target_version,
                            delta.route_key,
                            delta.client_id,
                            delta.window_name,
                            delta.bucket_start_ms,
                            delta.bucket_end_ms,
                            delta.event_count,
                            delta.weight_sum,
                            delta.admitted_count,
                            delta.admitted_weight_sum,
                            delta.updated_at_ms,
                        )
                        for delta in bucket_deltas
                    ],
                )

            if quota_states:
                connection.executemany(
                    "INSERT INTO quota_states(route_key, client_id, quota_group, "
                    "short_limit_weight, long_limit_weight, scale_ppm, updated_version, "
                    "updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(route_key, client_id) DO UPDATE SET "
                    "quota_group = excluded.quota_group, "
                    "short_limit_weight = excluded.short_limit_weight, "
                    "long_limit_weight = excluded.long_limit_weight, "
                    "scale_ppm = excluded.scale_ppm, "
                    "updated_version = excluded.updated_version, "
                    "updated_at_ms = excluded.updated_at_ms",
                    [
                        (
                            state["route_key"],
                            state["client_id"],
                            state["quota_group"],
                            state["short_limit_weight"],
                            state["long_limit_weight"],
                            state["scale_ppm"],
                            target_version,
                            state["updated_at_ms"],
                        )
                        for state in quota_states
                    ],
                )
                connection.executemany(
                    "INSERT INTO quota_state_history(version, route_key, client_id, quota_group, "
                    "short_limit_weight, long_limit_weight, scale_ppm, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            target_version,
                            state["route_key"],
                            state["client_id"],
                            state["quota_group"],
                            state["short_limit_weight"],
                            state["long_limit_weight"],
                            state["scale_ppm"],
                            state["updated_at_ms"],
                        )
                        for state in quota_states
                    ],
                )

            connection.execute(
                "UPDATE batches SET status = 'COMMITTED', commit_version = ?, "
                "decided_event_count = ?, reused_event_count = ?, auth_denied_count = ?, "
                "token_expired_count = ?, rate_limited_count = ?, allowed_count = ?, "
                "completed_at_ms = ? WHERE batch_id = ? AND status = 'PENDING'",
                (
                    target_version,
                    summary["decided_event_count"],
                    summary["reused_event_count"],
                    summary["auth_denied_count"],
                    summary["token_expired_count"],
                    summary["rate_limited_count"],
                    summary["allowed_count"],
                    now,
                    batch_id,
                ),
            )
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', updated_at_ms = ? "
                "WHERE intent_id = ? AND status = 'PENDING'",
                (now, intent_id),
            )
            connection.execute(
                "INSERT INTO snapshots(version, status, kind, created_at_ms) "
                "VALUES (?, 'COMMITTED', 'batch', ?)",
                (target_version, now),
            )
            connection.execute(
                "UPDATE meta SET value = ? WHERE key = 'state_version'",
                (str(target_version),),
            )

    def abort_intent(self, *, batch_id: str, intent_id: str, reason: str) -> None:
        now = _now_ms()
        with self.write() as connection:
            connection.execute(
                "UPDATE batches SET status = 'ABORTED', completed_at_ms = ? "
                "WHERE batch_id = ? AND status = 'PENDING'",
                (now, batch_id),
            )
            connection.execute(
                "UPDATE intents SET status = 'ROLLED_BACK', updated_at_ms = ? "
                "WHERE intent_id = ? AND status = 'PENDING'",
                (now, intent_id),
            )
            payload = canonical_json({"reason": reason})
            connection.execute(
                "UPDATE intents SET payload_json = ? WHERE intent_id = ?",
                (payload, intent_id),
            )

    def recover_pending(self) -> RecoveryReport:
        report_id = uuid.uuid4().hex
        now = _now_ms()
        with self.write() as connection:
            current = int(
                connection.execute(
                    "SELECT value FROM meta WHERE key = 'state_version'"
                ).fetchone()["value"]
            )
            pending = connection.execute(
                "SELECT intent_id, batch_id, fingerprint, base_version, target_version "
                "FROM intents WHERE status = 'PENDING' ORDER BY created_at_ms, intent_id"
            ).fetchall()
            pending_facts = [dict(row) for row in pending]
            for row in pending:
                connection.execute(
                    "UPDATE batches SET status = 'ABORTED', completed_at_ms = ? "
                    "WHERE batch_id = ? AND status = 'PENDING'",
                    (now, row["batch_id"]),
                )
                connection.execute(
                    "UPDATE intents SET status = 'ROLLED_BACK', updated_at_ms = ?, "
                    "payload_json = ? WHERE intent_id = ? AND status = 'PENDING'",
                    (
                        now,
                        canonical_json(
                            {
                                "recovered": True,
                                "reason": "PROCESS_TERMINATED_BEFORE_DECISION_COMMIT",
                                "base_version": int(row["base_version"]),
                                "target_version": int(row["target_version"]),
                            }
                        ),
                        row["intent_id"],
                    ),
                )
            details = {
                "pending_intents": pending_facts,
                "recovery_rule": (
                    "A PENDING intent has no visible window or decision writes because the "
                    "decision transaction is atomic. Recovery marks the batch ABORTED and keeps "
                    "the last committed snapshot."
                ),
                "state_version_unchanged": current,
                "forced_termination_exit_report_available": False,
            }
            connection.execute(
                "INSERT INTO recovery_reports(report_id, created_at_ms, state_version, "
                "scanned_intents, rolled_back_batches, details_json, complete_exit_report) "
                "VALUES (?, ?, ?, ?, ?, ?, 0)",
                (
                    report_id,
                    now,
                    current,
                    len(pending_facts),
                    len(pending_facts),
                    canonical_json(details),
                ),
            )
        return RecoveryReport(
            report_id=report_id,
            created_at_ms=now,
            state_version=current,
            scanned_intents=len(pending_facts),
            rolled_back_batches=len(pending_facts),
            details=details,
            complete_exit_report=False,
        )

    def latest_recovery_report(self) -> RecoveryReport:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at_ms DESC, rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise DomainError(E_BATCH_NOT_FOUND, "no recovery report has been persisted")
            return _recovery_from_row(row)

    def get_recovery_report(self, report_id: str) -> RecoveryReport:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_reports WHERE report_id = ?", (report_id,)
            ).fetchone()
            if row is None:
                raise DomainError(E_BATCH_NOT_FOUND, "recovery report not found", {"report_id": report_id})
            return _recovery_from_row(row)

    def get_decision(self, request_id: str) -> Decision:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM decisions WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise DomainError(
                    E_REQUEST_NOT_FOUND,
                    "request decision not found",
                    {"request_id": request_id},
                )
            return _decision_from_row(row)

    def get_batch_result(self, batch_id: str, *, replayed: bool = False) -> BatchResult:
        with self.read() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise DomainError(E_BATCH_NOT_FOUND, "batch not found", {"batch_id": batch_id})
            if batch["status"] == "PENDING":
                raise DomainError(
                    E_INVALID_TRANSITION,
                    "batch is pending recovery and has no visible decision snapshot",
                    {"batch_id": batch_id},
                )
            if batch["status"] == "ABORTED":
                raise DomainError(
                    E_INVALID_TRANSITION,
                    "batch was aborted after an interrupted decision",
                    {"batch_id": batch_id},
                )
            rows = connection.execute(
                "SELECT be.event_seq AS submitted_event_seq, d.* "
                "FROM batch_events be JOIN decisions d ON d.request_id = be.request_id "
                "WHERE be.batch_id = ? ORDER BY be.event_seq",
                (batch_id,),
            ).fetchall()
            decisions: list[Decision] = []
            for row in rows:
                decision = _decision_from_row(row)
                submitted_seq = int(row["submitted_event_seq"])
                decision.reused = replayed or decision.batch_id != batch_id or decision.event_seq != submitted_seq
                decision.submitted_batch_id = batch_id
                decision.submitted_event_seq = submitted_seq
                decisions.append(decision)
            return BatchResult(
                batch_id=batch_id,
                batch_fingerprint=batch["fingerprint"],
                status=batch["status"],
                base_version=int(batch["base_version"]),
                commit_version=int(batch["commit_version"]) if batch["commit_version"] is not None else None,
                replayed=replayed,
                event_count=int(batch["event_count"]),
                decided_event_count=int(batch["decided_event_count"]),
                reused_event_count=int(batch["reused_event_count"]),
                auth_denied_count=int(batch["auth_denied_count"]),
                token_expired_count=int(batch["token_expired_count"]),
                rate_limited_count=int(batch["rate_limited_count"]),
                allowed_count=int(batch["allowed_count"]),
                decisions=decisions,
            )

    def load_bucket_rows(
        self,
        route_key: str,
        client_id: str,
        *,
        min_timestamp_ms: int,
        max_timestamp_ms: int,
        version: int,
    ) -> list[dict[str, Any]]:
        selected = self.validate_snapshot(version)
        with self.read() as connection:
            current = int(
                connection.execute(
                    "SELECT value FROM meta WHERE key = 'state_version'"
                ).fetchone()["value"]
            )
            if selected == current:
                rows = connection.execute(
                    "SELECT * FROM window_buckets WHERE route_key = ? AND client_id = ? "
                    "AND bucket_end_ms > ? AND bucket_start_ms <= ? AND version <= ? "
                    "ORDER BY window_name, bucket_start_ms",
                    (route_key, client_id, min_timestamp_ms, max_timestamp_ms, selected),
                )
                return [dict(row) for row in rows]
            rows = connection.execute(
                "SELECT MAX(version) AS version, route_key, client_id, window_name, "
                "bucket_start_ms, MAX(bucket_end_ms) AS bucket_end_ms, "
                "MAX(updated_at_ms) AS updated_at_ms, SUM(event_count) AS event_count, "
                "SUM(weight_sum) AS weight_sum, SUM(admitted_count) AS admitted_count, "
                "SUM(admitted_weight_sum) AS admitted_weight_sum "
                "FROM window_bucket_deltas WHERE route_key = ? AND client_id = ? "
                "AND bucket_end_ms > ? AND bucket_start_ms <= ? AND version <= ? "
                "GROUP BY route_key, client_id, window_name, bucket_start_ms "
                "ORDER BY window_name, bucket_start_ms",
                (route_key, client_id, min_timestamp_ms, max_timestamp_ms, selected),
            )
            return [dict(row) for row in rows]

    def max_event_timestamp(self, version: int) -> int | None:
        self.validate_snapshot(version)
        with self.read() as connection:
            row = connection.execute(
                "SELECT MAX(timestamp_ms) AS max_ts FROM window_events WHERE commit_version <= ?",
                (version,),
            ).fetchone()
            return int(row["max_ts"]) if row and row["max_ts"] is not None else None

    def get_quota_state(
        self, route_key: str, client_id: str, *, version: int | None = None
    ) -> dict[str, Any] | None:
        with self.read() as connection:
            if version is None:
                row = connection.execute(
                    "SELECT * FROM quota_states WHERE route_key = ? AND client_id = ?",
                    (route_key, client_id),
                ).fetchone()
            else:
                current = int(
                    connection.execute(
                        "SELECT value FROM meta WHERE key = 'state_version'"
                    ).fetchone()["value"]
                )
                if version == current:
                    row = connection.execute(
                        "SELECT * FROM quota_states WHERE route_key = ? AND client_id = ? "
                        "AND updated_version <= ?",
                        (route_key, client_id, version),
                    ).fetchone()
                else:
                    row = connection.execute(
                        "SELECT version AS updated_version, route_key, client_id, quota_group, "
                        "short_limit_weight, long_limit_weight, scale_ppm, updated_at_ms "
                        "FROM quota_state_history WHERE route_key = ? AND client_id = ? "
                        "AND version <= ? ORDER BY version DESC LIMIT 1",
                        (route_key, client_id, version),
                    ).fetchone()
            return dict(row) if row else None

    def snapshot_info(self, version: int | None = None) -> dict[str, Any]:
        selected = self.validate_snapshot(version)
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE version = ?", (selected,)
            ).fetchone()
            return dict(row)


_DECISION_INSERT_SQL = """
INSERT INTO decisions(
    request_id, batch_id, event_seq, event_fingerprint, timestamp_ms, route_key,
    client_id, quota_group, weight, auth_ok, auth_code, auth_reason, allowed,
    decision_code, decision_reasons_json, rate_evaluated, trigger_window,
    short_event_count, short_exact_weight, short_estimated_weight,
    short_confidence_low, short_confidence_high, short_sample_rate_num,
    short_sample_rate_den, short_sample_method, short_exact_limit,
    short_max_window_events, short_variance,
    long_event_count, long_exact_weight, long_estimated_weight,
    long_confidence_low, long_confidence_high, long_sample_rate_num,
    long_sample_rate_den, long_sample_method, long_exact_limit,
    long_max_window_events, long_variance,
    short_quota, long_quota, quota_scale_ppm,
    read_version, window_version, token_version, commit_version, created_at_ms
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


def _decision_tuple(decision: Decision) -> tuple[Any, ...]:
    short = decision.short
    long = decision.long
    return (
        decision.request_id,
        decision.batch_id,
        decision.event_seq,
        decision.event_fingerprint,
        decision.timestamp_ms,
        decision.route_key,
        decision.client_id,
        decision.quota_group,
        decision.weight,
        1 if decision.auth_ok else 0,
        decision.auth_code,
        decision.auth_reason,
        1 if decision.allowed else 0,
        decision.decision_code,
        canonical_json(list(decision.decision_reasons)),
        1 if decision.rate_evaluated else 0,
        decision.trigger_window,
        short.event_count if short else None,
        short.exact_weight if short else None,
        short.estimated_weight if short else None,
        short.confidence_low if short else None,
        short.confidence_high if short else None,
        short.sample_rate_num if short else None,
        short.sample_rate_den if short else None,
        short.method if short else None,
        short.exact_limit if short else None,
        short.max_window_events if short else None,
        short.variance if short else None,
        long.event_count if long else None,
        long.exact_weight if long else None,
        long.estimated_weight if long else None,
        long.confidence_low if long else None,
        long.confidence_high if long else None,
        long.sample_rate_num if long else None,
        long.sample_rate_den if long else None,
        long.method if long else None,
        long.exact_limit if long else None,
        long.max_window_events if long else None,
        long.variance if long else None,
        decision.short_quota,
        decision.long_quota,
        decision.quota_scale_ppm,
        decision.read_version,
        decision.window_version,
        decision.token_version,
        decision.commit_version,
        decision.created_at_ms,
    )


def _decision_from_row(row: sqlite3.Row | dict[str, Any]) -> Decision:
    def window(prefix: str) -> WindowEstimate | None:
        if row[f"{prefix}_event_count"] is None:
            return None
        return WindowEstimate(
            window=prefix,
            event_count=int(row[f"{prefix}_event_count"]),
            exact_weight=int(row[f"{prefix}_exact_weight"]),
            estimated_weight=int(row[f"{prefix}_estimated_weight"]),
            confidence_low=int(row[f"{prefix}_confidence_low"]),
            confidence_high=int(row[f"{prefix}_confidence_high"]),
            sample_rate_num=int(row[f"{prefix}_sample_rate_num"]),
            sample_rate_den=int(row[f"{prefix}_sample_rate_den"]),
            method=str(row[f"{prefix}_sample_method"]),
            exact_limit=int(row[f"{prefix}_exact_limit"]),
            max_window_events=int(row[f"{prefix}_max_window_events"]),
            variance=int(row[f"{prefix}_variance"]),
        )

    return Decision(
        request_id=row["request_id"],
        batch_id=row["batch_id"],
        event_seq=int(row["event_seq"]),
        event_fingerprint=row["event_fingerprint"],
        timestamp_ms=int(row["timestamp_ms"]),
        route_key=row["route_key"],
        client_id=row["client_id"],
        quota_group=row["quota_group"],
        weight=int(row["weight"]),
        auth_ok=bool(row["auth_ok"]),
        auth_code=row["auth_code"],
        auth_reason=row["auth_reason"],
        allowed=bool(row["allowed"]),
        decision_code=row["decision_code"],
        decision_reasons=tuple(json.loads(row["decision_reasons_json"])),
        rate_evaluated=bool(row["rate_evaluated"]),
        trigger_window=row["trigger_window"],
        short=window("short"),
        long=window("long"),
        short_quota=int(row["short_quota"]) if row["short_quota"] is not None else None,
        long_quota=int(row["long_quota"]) if row["long_quota"] is not None else None,
        quota_scale_ppm=int(row["quota_scale_ppm"]) if row["quota_scale_ppm"] is not None else None,
        read_version=int(row["read_version"]),
        window_version=int(row["window_version"]),
        token_version=int(row["token_version"]),
        commit_version=int(row["commit_version"]),
        created_at_ms=int(row["created_at_ms"]),
    )


def _recovery_from_row(row: sqlite3.Row) -> RecoveryReport:
    return RecoveryReport(
        report_id=row["report_id"],
        created_at_ms=int(row["created_at_ms"]),
        state_version=int(row["state_version"]),
        scanned_intents=int(row["scanned_intents"]),
        rolled_back_batches=int(row["rolled_back_batches"]),
        details=json.loads(row["details_json"]),
        complete_exit_report=bool(row["complete_exit_report"]),
    )


def _chunks(values: list[Any], size: int) -> Iterator[list[Any]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]
