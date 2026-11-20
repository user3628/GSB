from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import EngineConfig
from .crash import CrashController
from .errors import BusinessError, E_SNAPSHOT_INVALID


LOGGER = logging.getLogger(__name__)
SNAPSHOT_FORMAT = "DWE_SNAPSHOT_V1"
SNAPSHOT_NAME_RE = re.compile(r"^v(\d{12})\.json$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def checksum_for_snapshot(version: int, config_fingerprint: str, body: dict[str, Any]) -> str:
    payload = canonical_json(
        {
            "format": SNAPSHOT_FORMAT,
            "version": version,
            "config_fingerprint": config_fingerprint,
            "body": body,
        }
    ).encode("utf-8")
    return sha256_bytes(payload)


@dataclass(slots=True)
class SnapshotInfo:
    version: int
    path: Path
    checksum: str
    created_at: str
    intent_id: str | None
    batch_id: str | None
    envelope: dict[str, Any]

    @property
    def body(self) -> dict[str, Any]:
        return self.envelope["body"]

    def public_dict(self) -> dict[str, Any]:
        state = self.body
        return {
            "version": self.version,
            "file": self.path.name,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "intent_id": self.intent_id,
            "batch_id": self.batch_id,
            "state_version": state.get("version"),
            "watermark_ms": state.get("watermark_ms"),
            "window_count": len(state.get("windows", {})),
            "late_queue_length": len(state.get("late_queue", [])),
        }


@dataclass(slots=True)
class SnapshotScan:
    latest: SnapshotInfo | None
    valid: list[SnapshotInfo]
    corrupt: list[dict[str, str]]
    temporary_files: list[str]
    observed_max_version: int
    had_snapshot_files: bool


class SnapshotStore:
    def __init__(self, runtime_dir: str | Path, config: EngineConfig, crash: CrashController | None = None):
        self.runtime_dir = Path(runtime_dir).resolve()
        self.snapshot_dir = self.runtime_dir / "snapshots"
        self.recovery_dir = self.runtime_dir / "recovery"
        self.db_path = self.runtime_dir / "store.sqlite3"
        self.config = config
        self.crash = crash or CrashController()
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.recovery_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    batch_id TEXT,
                    content_hash TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','committed','aborted')),
                    baseline_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    finished_at TEXT,
                    detail TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
                CREATE TABLE IF NOT EXISTS snapshots (
                    version INTEGER PRIMARY KEY,
                    path TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    intent_id TEXT,
                    batch_id TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )

    def record_intent_start(
        self,
        *,
        intent_id: str,
        batch_id: str | None,
        content_hash: str | None,
        kind: str,
        baseline_version: int,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO intents(
                    intent_id, batch_id, content_hash, kind, status,
                    baseline_version, created_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
                """,
                (intent_id, batch_id, content_hash, kind, baseline_version, utc_now()),
            )

    def mark_intent_finished(self, intent_id: str, status: str, detail: str | None = None) -> None:
        if status not in {"committed", "aborted"}:
            raise ValueError(f"Invalid intent terminal status: {status}")
        with self._connection() as connection:
            connection.execute(
                "UPDATE intents SET status=?, finished_at=?, detail=? WHERE intent_id=?",
                (status, utc_now(), detail, intent_id),
            )

    def pending_intents(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM intents WHERE status='pending' ORDER BY created_at, intent_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def intent_history(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM intents ORDER BY created_at DESC, intent_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_snapshot(
        self,
        *,
        version: int,
        path: Path,
        checksum: str,
        intent_id: str | None,
        batch_id: str | None,
        created_at: str,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO snapshots(version, path, checksum, intent_id, batch_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(version) DO UPDATE SET
                    path=excluded.path,
                    checksum=excluded.checksum,
                    intent_id=excluded.intent_id,
                    batch_id=excluded.batch_id,
                    created_at=excluded.created_at
                """,
                (version, str(path), checksum, intent_id, batch_id, created_at),
            )

    def snapshot_records(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM snapshots ORDER BY version DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def build_envelope(self, body: dict[str, Any], created_at: str | None = None) -> dict[str, Any]:
        version = int(body["version"])
        fingerprint = str(body.get("config_fingerprint", self.config.fingerprint))
        checksum = checksum_for_snapshot(version, fingerprint, body)
        return {
            "format": SNAPSHOT_FORMAT,
            "version": version,
            "created_at": created_at or utc_now(),
            "config_fingerprint": fingerprint,
            "checksum_algorithm": "sha256",
            "checksum": checksum,
            "body": body,
        }

    def write_snapshot(
        self,
        body: dict[str, Any],
        *,
        intent_id: str | None,
        batch_id: str | None,
        kind: str,
    ) -> SnapshotInfo:
        version = int(body["version"])
        envelope = self.build_envelope(body)
        envelope["intent_id"] = intent_id
        envelope["batch_id"] = batch_id
        final_path = self.snapshot_dir / f"v{version:012d}.json"
        temp_path = self.snapshot_dir / f".v{version:012d}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        encoded = canonical_json(envelope).encode("utf-8")
        try:
            with temp_path.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            self.crash.maybe_crash(
                "after_snapshot_temp", batch_id=batch_id, kind=kind
            )
            os.replace(temp_path, final_path)
            self._fsync_directory(self.snapshot_dir)
            self.crash.maybe_crash(
                "after_snapshot_replace", batch_id=batch_id, kind=kind
            )
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

        checksum = envelope["checksum"]
        try:
            self.record_snapshot(
                version=version,
                path=final_path,
                checksum=checksum,
                intent_id=intent_id,
                batch_id=batch_id,
                created_at=envelope["created_at"],
            )
        except sqlite3.Error:
            LOGGER.exception("Snapshot file is committed but SQLite catalog update failed")

        return SnapshotInfo(
            version=version,
            path=final_path,
            checksum=checksum,
            created_at=envelope["created_at"],
            intent_id=intent_id,
            batch_id=batch_id,
            envelope=envelope,
        )

    def _validate_envelope(
        self,
        envelope: Any,
        *,
        expected_version: int | None = None,
        expected_fingerprint: str | None = None,
    ) -> SnapshotInfo:
        if not isinstance(envelope, dict):
            raise ValueError("snapshot root is not an object")
        if envelope.get("format") != SNAPSHOT_FORMAT:
            raise ValueError("unsupported snapshot format")
        version = envelope.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise ValueError("snapshot version is invalid")
        if expected_version is not None and version != expected_version:
            raise ValueError("snapshot filename/version mismatch")
        body = envelope.get("body")
        if not isinstance(body, dict):
            raise ValueError("snapshot body is invalid")
        if body.get("version") != version:
            raise ValueError("snapshot body version mismatch")
        fingerprint = envelope.get("config_fingerprint")
        if not isinstance(fingerprint, str):
            raise ValueError("snapshot config fingerprint is missing")
        expected = expected_fingerprint or self.config.fingerprint
        if fingerprint != expected:
            raise ValueError("snapshot config fingerprint mismatch")
        if body.get("config_fingerprint") != fingerprint:
            raise ValueError("snapshot body config fingerprint mismatch")
        checksum = envelope.get("checksum")
        if not isinstance(checksum, str):
            raise ValueError("snapshot checksum is missing")
        actual = checksum_for_snapshot(version, fingerprint, body)
        if actual != checksum:
            raise ValueError("snapshot checksum mismatch")
        return SnapshotInfo(
            version=version,
            path=Path(""),
            checksum=checksum,
            created_at=str(envelope.get("created_at", "")),
            intent_id=envelope.get("intent_id"),
            batch_id=envelope.get("batch_id"),
            envelope=envelope,
        )

    def load_version(self, version: int) -> SnapshotInfo:
        path = self.snapshot_dir / f"v{version:012d}.json"
        if not path.is_file():
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "Snapshot version not found",
                details={"version": version},
            )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            info = self._validate_envelope(raw, expected_version=version)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "Snapshot validation failed",
                details={"version": version, "reason": str(exc)},
            ) from exc
        info.path = path
        info.intent_id = raw.get("intent_id") or info.intent_id
        info.batch_id = raw.get("batch_id") or info.batch_id
        return info

    def load_external(self, path: Path) -> SnapshotInfo:
        selected = path.expanduser().resolve()
        if not selected.is_file():
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "Snapshot file not found",
                details={"path": str(selected)},
            )
        try:
            raw = json.loads(selected.read_text(encoding="utf-8"))
            info = self._validate_envelope(raw)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "Snapshot validation failed",
                details={"path": str(selected), "reason": str(exc)},
            ) from exc
        info.path = selected
        info.intent_id = raw.get("intent_id") or info.intent_id
        info.batch_id = raw.get("batch_id") or info.batch_id
        return info

    def scan_latest(self) -> SnapshotScan:
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        entries = list(self.snapshot_dir.iterdir())
        temporary = sorted(entry.name for entry in entries if entry.name.endswith(".tmp"))
        candidates: list[tuple[int, Path]] = []
        for entry in entries:
            match = SNAPSHOT_NAME_RE.match(entry.name)
            if match and entry.is_file():
                candidates.append((int(match.group(1)), entry))
        candidates.sort(reverse=True)
        observed_max = candidates[0][0] if candidates else 0
        valid: list[SnapshotInfo] = []
        corrupt: list[dict[str, str]] = []
        for version, path in candidates:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                info = self._validate_envelope(raw, expected_version=version)
                info.path = path
                # Prefer explicit envelope-level intent metadata when present.
                info.intent_id = raw.get("intent_id") or info.intent_id
                info.batch_id = raw.get("batch_id") or info.batch_id
                valid.append(info)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                corrupt.append({"file": path.name, "version": str(version), "reason": str(exc)})
        return SnapshotScan(
            latest=valid[0] if valid else None,
            valid=valid,
            corrupt=corrupt,
            temporary_files=temporary,
            observed_max_version=observed_max,
            had_snapshot_files=bool(candidates),
        )

    def list_snapshots(self) -> list[dict[str, Any]]:
        scan = self.scan_latest()
        rows: list[dict[str, Any]] = []
        seen_versions = set()
        for info in scan.valid:
            rows.append(info.public_dict())
            seen_versions.add(info.version)
        for record in self.snapshot_records():
            if record["version"] in seen_versions:
                continue
            rows.append(
                {
                    "version": record["version"],
                    "file": Path(record["path"]).name,
                    "checksum": record["checksum"],
                    "created_at": record["created_at"],
                    "intent_id": record["intent_id"],
                    "batch_id": record["batch_id"],
                    "file_present": Path(record["path"]).is_file(),
                }
            )
        rows.sort(key=lambda row: row["version"], reverse=True)
        return rows

    def write_recovery_report(self, report: dict[str, Any]) -> Path:
        return self.atomic_write_json(self.recovery_dir / "latest.json", report)

    def atomic_write_json(self, path: Path, value: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        encoded = canonical_json(value).encode("utf-8")
        try:
            with temp.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
            self._fsync_directory(path.parent)
        finally:
            if temp.exists():
                try:
                    temp.unlink()
                except OSError:
                    pass
        return path

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name == "nt":
            return
        try:
            descriptor = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))
