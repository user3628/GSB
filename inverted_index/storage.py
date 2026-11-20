from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import BusinessError, bad_path, unknown_path
from .paths import split_field_path, validate_field_paths
from .text import extract_postings, normalize_term


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class BatchResult:
    version: int
    batch_id: str
    inserted: int
    updated: int
    deleted: int
    affected_paths: tuple[str, ...]
    segment_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "batch_id": self.batch_id,
            "inserted": self.inserted,
            "updated": self.updated,
            "deleted": self.deleted,
            "affected_paths": list(self.affected_paths),
            "segment_id": self.segment_id,
        }


@dataclass(frozen=True)
class PostingEntry:
    doc_id: str
    term_frequency: int
    positions: tuple[int, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "term_frequency": self.term_frequency,
            "positions": list(self.positions),
        }


@dataclass(frozen=True)
class PostingResponse:
    version: int
    field_path: str
    term: str
    entries: tuple[PostingEntry, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "field": self.field_path,
            "term": self.term,
            "entries": [entry.as_dict() for entry in self.entries],
        }


@dataclass(frozen=True)
class SearchClause:
    kind: str
    field_path: str
    term: str | None = None
    terms: tuple[str, ...] = ()


@dataclass(frozen=True)
class SearchHit:
    doc_id: str
    document: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"_id": self.doc_id, "document": self.document}


@dataclass(frozen=True)
class SearchResponse:
    version: int
    total: int
    limit: int
    offset: int
    items: tuple[SearchHit, ...]
    operator: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset,
            "operator": self.operator,
            "items": [item.as_dict() for item in self.items],
        }


@dataclass
class MergeResult:
    operation_id: str
    target_segment_id: str
    source_segment_ids: tuple[str, ...]
    copied_postings: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "target_segment_id": self.target_segment_id,
            "source_segment_ids": list(self.source_segment_ids),
            "copied_postings": self.copied_postings,
        }


class IndexStore:
    """SQLite-backed document store with a versioned inverted index.

    Query leases and version publication are coordinated in process. One store
    instance is intended to own a database file at a time; SQLite WAL still
    protects readers against an uncommitted batch.
    """

    def __init__(
        self,
        db_path: str | Path,
        index_fields: Iterable[str],
        *,
        busy_timeout_ms: int = 10_000,
        failpoint_hook: Callable[[str], None] | None = None,
        query_hook: Callable[[int], None] | None = None,
    ) -> None:
        if str(db_path) == ":memory:":
            raise ValueError("A file-backed SQLite database is required")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = busy_timeout_ms
        self._failpoint_hook = failpoint_hook
        self._query_hook = query_hook
        self._write_lock = threading.RLock()
        self._view_condition = threading.Condition(threading.RLock())
        self._active_queries: Counter[int] = Counter()
        self._retiring_versions: set[int] = set()
        self._retired_versions: set[int] = set()
        self._known_versions: set[int] = set()
        self._published_version = 0

        self._initialize(index_fields)
        with self._view_condition:
            current = self._read_current_version()
            self._published_version = current
        self.recover()

    # ------------------------------------------------------------------
    # Database primitives
    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize(self, index_fields: Iterable[str]) -> None:
        fields = validate_field_paths(index_fields)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS versions (
                    version INTEGER PRIMARY KEY,
                    operation_id TEXT NOT NULL UNIQUE,
                    committed_at TEXT NOT NULL,
                    retired INTEGER NOT NULL DEFAULT 0,
                    retired_at TEXT
                );

                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    committed_version INTEGER,
                    outcome_json TEXT
                );

                CREATE TABLE IF NOT EXISTS index_fields (
                    path TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS segments (
                    segment_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    min_version INTEGER NOT NULL,
                    max_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    merged_into TEXT,
                    merge_operation_id TEXT
                );

                CREATE TABLE IF NOT EXISTS postings (
                    posting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    segment_id TEXT NOT NULL REFERENCES segments(segment_id),
                    field_path TEXT NOT NULL,
                    term TEXT NOT NULL,
                    doc_id TEXT NOT NULL,
                    start_version INTEGER NOT NULL,
                    end_version INTEGER,
                    term_frequency INTEGER NOT NULL,
                    positions_json TEXT NOT NULL,
                    CHECK (start_version >= 0),
                    CHECK (end_version IS NULL OR end_version > start_version),
                    CHECK (term_frequency >= 1)
                );

                CREATE INDEX IF NOT EXISTS idx_postings_lookup
                    ON postings(field_path, term, start_version, end_version, doc_id);
                CREATE INDEX IF NOT EXISTS idx_postings_doc_path
                    ON postings(doc_id, field_path, end_version);
                CREATE INDEX IF NOT EXISTS idx_postings_segment
                    ON postings(segment_id);

                CREATE TABLE IF NOT EXISTS document_versions (
                    doc_id TEXT NOT NULL,
                    start_version INTEGER NOT NULL,
                    end_version INTEGER,
                    document_json TEXT NOT NULL,
                    PRIMARY KEY (doc_id, start_version),
                    CHECK (end_version IS NULL OR end_version > start_version)
                );

                CREATE INDEX IF NOT EXISTS idx_documents_visible
                    ON document_versions(doc_id, start_version, end_version);

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    report_json TEXT NOT NULL
                );
                """
            )
            now = utc_now()
            conn.execute(
                "INSERT OR IGNORE INTO versions(version, operation_id, committed_at) "
                "VALUES(0, 'bootstrap', ?)",
                (now,),
            )
            for path in fields:
                conn.execute(
                    "INSERT OR IGNORE INTO index_fields(path, created_at) VALUES(?, ?)",
                    (path, now),
                )
            rows = conn.execute("SELECT version FROM versions").fetchall()
            with self._view_condition:
                self._known_versions = {int(row["version"]) for row in rows}
                retired = conn.execute("SELECT version FROM versions WHERE retired = 1").fetchall()
                self._retired_versions = {int(row["version"]) for row in retired}
        finally:
            conn.close()

    def _read_current_version(self) -> int:
        conn = self._connect()
        try:
            row = conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM versions").fetchone()
            return int(row["v"])
        finally:
            conn.close()

    def _hit_failpoint(self, name: str) -> None:
        configured = os.environ.get("INVERTED_INDEX_FAILPOINT", "")
        if configured == name:
            marker = os.environ.get("INVERTED_INDEX_FAILPOINT_MARKER")
            if marker:
                marker_path = Path(marker)
                marker_path.parent.mkdir(parents=True, exist_ok=True)
                marker_path.write_text(
                    _json({"failpoint": name, "pid": os.getpid(), "reached_at": utc_now()}),
                    encoding="utf-8",
                )
            hold = float(os.environ.get("INVERTED_INDEX_FAILPOINT_HOLD_SECONDS", "30"))
            time.sleep(max(hold, 0.0))
        if self._failpoint_hook is not None:
            self._failpoint_hook(name)

    def _ensure_known_path(self, path: str) -> None:
        split_field_path(path)
        conn = self._connect()
        try:
            row = conn.execute("SELECT 1 FROM index_fields WHERE path = ?", (path,)).fetchone()
        finally:
            conn.close()
        if row is None:
            raise unknown_path(path)

    # ------------------------------------------------------------------
    # Version view / leases
    # ------------------------------------------------------------------
    def current_version(self) -> int:
        with self._view_condition:
            return self._published_version

    def _resolve_version_locked(self, requested: int | None) -> int:
        version = self._published_version if requested is None else int(requested)
        if version < 0:
            raise BusinessError("E_VERSION_NOT_FOUND", "Version does not exist", status_code=404)
        if version in self._retiring_versions or version in self._retired_versions:
            raise BusinessError(
                "E_VERSION_RETIRED",
                "Version has been retired",
                status_code=410,
                details={"version": version},
            )
        if version not in self._known_versions:
            raise BusinessError(
                "E_VERSION_NOT_FOUND",
                "Version does not exist",
                status_code=404,
                details={"version": version},
            )
        return version

    @contextmanager
    def _query_view(self, requested: int | None) -> Iterator[int]:
        with self._view_condition:
            version = self._resolve_version_locked(requested)
            self._active_queries[version] += 1
        try:
            if self._query_hook is not None:
                self._query_hook(version)
            yield version
        finally:
            with self._view_condition:
                self._active_queries[version] -= 1
                if self._active_queries[version] <= 0:
                    del self._active_queries[version]
                self._view_condition.notify_all()

    def query_activity(self) -> dict[str, Any]:
        with self._view_condition:
            return {
                "active_by_version": {str(k): v for k, v in sorted(self._active_queries.items())},
                "retiring_versions": sorted(self._retiring_versions),
            }

    def list_versions(self) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT version, committed_at, retired, retired_at FROM versions ORDER BY version"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Batch writes
    # ------------------------------------------------------------------
    @staticmethod
    def _document_id(document: Any) -> str:
        if not isinstance(document, dict):
            raise BusinessError("E_INVALID_DOCUMENT", "Each document must be a JSON object")
        doc_id = document.get("_id")
        if not isinstance(doc_id, str) or not doc_id or doc_id != doc_id.strip():
            raise BusinessError(
                "E_BAD_ID",
                "Document _id must be a non-empty string without surrounding whitespace",
            )
        return doc_id

    @staticmethod
    def _serialize_document(document: dict[str, Any]) -> str:
        try:
            return _json(document)
        except (TypeError, ValueError) as exc:
            raise BusinessError(
                "E_INVALID_DOCUMENT",
                "Document is not valid JSON",
                details={"reason": str(exc)},
            ) from exc

    def _active_document(self, conn: sqlite3.Connection, doc_id: str) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT document_json FROM document_versions "
            "WHERE doc_id = ? AND end_version IS NULL ORDER BY start_version DESC LIMIT 1",
            (doc_id,),
        ).fetchone()
        return None if row is None else json.loads(row["document_json"])

    def write_batch(
        self,
        *,
        inserts: Sequence[dict[str, Any]] = (),
        updates: Sequence[dict[str, Any]] = (),
        deletes: Sequence[str] = (),
        batch_id: str | None = None,
    ) -> BatchResult:
        batch_id = batch_id or f"batch-{uuid.uuid4().hex}"
        if not isinstance(batch_id, str) or not batch_id or batch_id != batch_id.strip():
            raise BusinessError("E_BAD_BATCH_ID", "batch_id must be a non-empty string")
        operation_id = f"batch:{batch_id}"
        payload = {
            "batch_id": batch_id,
            "inserts": len(inserts),
            "updates": len(updates),
            "deletes": len(deletes),
        }

        with self._write_lock:
            self._create_operation(operation_id, "batch", payload)
            self._hit_failpoint("batch:after_intent")
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                seen: set[str] = set()
                prepared_inserts: list[tuple[str, dict[str, Any], str]] = []
                prepared_updates: list[tuple[str, dict[str, Any], str, dict[str, Any]]] = []
                prepared_deletes: list[tuple[str, dict[str, Any]]] = []

                for document in inserts:
                    doc_id = self._document_id(document)
                    if doc_id in seen:
                        raise BusinessError(
                            "E_DUPLICATE_ID",
                            "Duplicate _id within a batch",
                            details={"doc_id": doc_id},
                        )
                    seen.add(doc_id)
                    existing = self._active_document(conn, doc_id)
                    if existing is not None:
                        raise BusinessError(
                            "E_DUPLICATE_ID",
                            "Document _id already exists; use updates for replacement",
                            status_code=409,
                            details={"doc_id": doc_id},
                        )
                    prepared_inserts.append((doc_id, document, self._serialize_document(document)))

                for document in updates:
                    doc_id = self._document_id(document)
                    if doc_id in seen:
                        raise BusinessError(
                            "E_DUPLICATE_ID",
                            "Duplicate _id within a batch",
                            details={"doc_id": doc_id},
                        )
                    seen.add(doc_id)
                    existing = self._active_document(conn, doc_id)
                    if existing is None:
                        raise BusinessError(
                            "E_DOC_NOT_FOUND",
                            "Cannot update a document that does not exist",
                            status_code=404,
                            details={"doc_id": doc_id},
                        )
                    prepared_updates.append(
                        (doc_id, document, self._serialize_document(document), existing)
                    )

                for doc_id in deletes:
                    if not isinstance(doc_id, str) or not doc_id:
                        raise BusinessError("E_BAD_ID", "delete ids must be non-empty strings")
                    if doc_id in seen:
                        raise BusinessError(
                            "E_DUPLICATE_ID",
                            "Duplicate _id within a batch",
                            details={"doc_id": doc_id},
                        )
                    seen.add(doc_id)
                    existing = self._active_document(conn, doc_id)
                    if existing is None:
                        raise BusinessError(
                            "E_DOC_NOT_FOUND",
                            "Cannot delete a document that does not exist",
                            status_code=404,
                            details={"doc_id": doc_id},
                        )
                    prepared_deletes.append((doc_id, existing))

                if not prepared_inserts and not prepared_updates and not prepared_deletes:
                    raise BusinessError("E_EMPTY_BATCH", "A write batch must contain at least one change")

                row = conn.execute("SELECT COALESCE(MAX(version), 0) + 1 AS v FROM versions").fetchone()
                new_version = int(row["v"])
                segment_id = f"seg-{new_version}-{uuid.uuid4().hex[:10]}"
                now = utc_now()
                conn.execute(
                    "INSERT INTO versions(version, operation_id, committed_at) VALUES(?, ?, ?)",
                    (new_version, operation_id, now),
                )
                conn.execute(
                    "INSERT INTO segments(segment_id, status, min_version, max_version, created_at) "
                    "VALUES(?, 'active', ?, ?, ?)",
                    (segment_id, new_version, new_version, now),
                )

                fields = self._indexed_fields(conn)
                affected_paths: set[str] = set()

                for doc_id, document, document_json in prepared_inserts:
                    conn.execute(
                        "INSERT INTO document_versions(doc_id, start_version, end_version, document_json) "
                        "VALUES(?, ?, NULL, ?)",
                        (doc_id, new_version, document_json),
                    )
                    extracted = extract_postings(document, fields)
                    self._insert_postings(conn, segment_id, doc_id, extracted, new_version)
                    affected_paths.update(path for path, _term in extracted)

                for doc_id, document, document_json, old_document in prepared_updates:
                    conn.execute(
                        "UPDATE document_versions SET end_version = ? "
                        "WHERE doc_id = ? AND end_version IS NULL",
                        (new_version, doc_id),
                    )
                    conn.execute(
                        "INSERT INTO document_versions(doc_id, start_version, end_version, document_json) "
                        "VALUES(?, ?, NULL, ?)",
                        (doc_id, new_version, document_json),
                    )
                    old_extracted = extract_postings(old_document, fields)
                    new_extracted = extract_postings(document, fields)
                    changed_paths = {
                        path
                        for path in {key[0] for key in set(old_extracted) | set(new_extracted)}
                        if self._path_terms(old_extracted, path) != self._path_terms(new_extracted, path)
                    }
                    for path in changed_paths:
                        conn.execute(
                            "UPDATE postings SET end_version = ? "
                            "WHERE doc_id = ? AND field_path = ? AND end_version IS NULL",
                            (new_version, doc_id, path),
                        )
                    changed_extracted = {
                        key: positions
                        for key, positions in new_extracted.items()
                        if key[0] in changed_paths
                    }
                    self._insert_postings(conn, segment_id, doc_id, changed_extracted, new_version)
                    affected_paths.update(changed_paths)

                for doc_id, _old_document in prepared_deletes:
                    conn.execute(
                        "UPDATE document_versions SET end_version = ? "
                        "WHERE doc_id = ? AND end_version IS NULL",
                        (new_version, doc_id),
                    )
                    rows = conn.execute(
                        "SELECT DISTINCT field_path FROM postings "
                        "WHERE doc_id = ? AND end_version IS NULL",
                        (doc_id,),
                    ).fetchall()
                    deleted_paths = {row["field_path"] for row in rows}
                    conn.execute(
                        "UPDATE postings SET end_version = ? "
                        "WHERE doc_id = ? AND end_version IS NULL",
                        (new_version, doc_id),
                    )
                    affected_paths.update(deleted_paths)

                self._hit_failpoint("batch:after_changes")
                outcome = {
                    "version": new_version,
                    "segment_id": segment_id,
                    "affected_paths": sorted(affected_paths),
                }
                conn.execute(
                    "UPDATE operations SET status = 'completed', updated_at = ?, "
                    "committed_version = ?, outcome_json = ? WHERE operation_id = ?",
                    (utc_now(), new_version, _json(outcome), operation_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                conn.close()
                # A normal exception means the request failed and is explicitly
                # aborted. A BaseException simulating process death leaves the
                # pending intent for startup recovery.
                self._abort_operation_on_exception(operation_id)
                raise
            else:
                conn.close()

            with self._view_condition:
                self._published_version = new_version
                self._known_versions.add(new_version)
                self._view_condition.notify_all()

        return BatchResult(
            version=new_version,
            batch_id=batch_id,
            inserted=len(prepared_inserts),
            updated=len(prepared_updates),
            deleted=len(prepared_deletes),
            affected_paths=tuple(sorted(affected_paths)),
            segment_id=segment_id,
        )

    def _abort_operation_on_exception(self, operation_id: str) -> None:
        # Do not turn a simulated hard kill (BaseException) into a clean abort;
        # it must remain a pending intent for recovery.
        import sys

        exc = sys.exc_info()[1]
        if exc is not None and not isinstance(exc, Exception):
            return
        try:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE operations SET status = 'aborted', updated_at = ?, outcome_json = ? "
                    "WHERE operation_id = ? AND status = 'pending'",
                    (utc_now(), _json({"reason": type(exc).__name__ if exc else "error"}), operation_id),
                )
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    def _create_operation(self, operation_id: str, kind: str, payload: dict[str, Any]) -> None:
        now = utc_now()
        conn = self._connect()
        try:
            try:
                conn.execute(
                    "INSERT INTO operations(operation_id, kind, status, payload_json, created_at, updated_at) "
                    "VALUES(?, ?, 'pending', ?, ?, ?)",
                    (operation_id, kind, _json(payload), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise BusinessError(
                    "E_BATCH_CONFLICT",
                    "This operation id has already been used",
                    status_code=409,
                    details={"operation_id": operation_id},
                ) from exc
        finally:
            conn.close()

    def _indexed_fields(self, conn: sqlite3.Connection) -> tuple[str, ...]:
        rows = conn.execute("SELECT path FROM index_fields ORDER BY path").fetchall()
        return tuple(row["path"] for row in rows)

    @staticmethod
    def _path_terms(extracted: dict[tuple[str, str], tuple[int, ...]], path: str) -> dict[str, tuple[int, ...]]:
        return {
            term: positions
            for (field_path, term), positions in extracted.items()
            if field_path == path
        }

    @staticmethod
    def _insert_postings(
        conn: sqlite3.Connection,
        segment_id: str,
        doc_id: str,
        extracted: dict[tuple[str, str], tuple[int, ...]],
        version: int,
    ) -> None:
        for (field_path, term), positions in sorted(extracted.items()):
            conn.execute(
                "INSERT INTO postings(segment_id, field_path, term, doc_id, start_version, "
                "end_version, term_frequency, positions_json) VALUES(?, ?, ?, ?, ?, NULL, ?, ?)",
                (
                    segment_id,
                    field_path,
                    term,
                    doc_id,
                    version,
                    len(positions),
                    _json(list(positions)),
                ),
            )

    # ------------------------------------------------------------------
    # Reads and search
    # ------------------------------------------------------------------
    @staticmethod
    def _visible_posting_rows(
        conn: sqlite3.Connection, field_path: str, term: str, version: int
    ) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT doc_id, term_frequency, positions_json FROM postings "
            "WHERE field_path = ? AND term = ? AND start_version <= ? "
            "AND (end_version IS NULL OR end_version > ?) ORDER BY doc_id",
            (field_path, term, version, version),
        ).fetchall()

    def get_postings(self, field_path: str, term: str, version: int | None = None) -> PostingResponse:
        self._ensure_known_path(field_path)
        normalized_term = normalize_term(term)
        with self._query_view(version) as resolved:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                rows = self._visible_posting_rows(conn, field_path, normalized_term, resolved)
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()
        entries = tuple(
            PostingEntry(
                doc_id=row["doc_id"],
                term_frequency=int(row["term_frequency"]),
                positions=tuple(int(p) for p in json.loads(row["positions_json"])),
            )
            for row in rows
        )
        return PostingResponse(resolved, field_path, normalized_term, entries)

    @staticmethod
    def _phrase_docs(positions_by_term: list[dict[str, tuple[int, ...]]]) -> set[str]:
        if not positions_by_term:
            return set()
        common_docs = set(positions_by_term[0])
        for mapping in positions_by_term[1:]:
            common_docs.intersection_update(mapping)
        matched: set[str] = set()
        for doc_id in common_docs:
            position_sets = [set(mapping[doc_id]) for mapping in positions_by_term]
            for start in position_sets[0]:
                if all(start + offset in position_sets[offset] for offset in range(1, len(position_sets))):
                    matched.add(doc_id)
                    break
        return matched

    def _clause_docs(
        self, conn: sqlite3.Connection, clause: SearchClause, version: int
    ) -> set[str]:
        self._ensure_known_path(clause.field_path)
        if clause.kind == "term":
            if clause.term is None:
                raise BusinessError("E_BAD_QUERY", "term clause requires term")
            term = normalize_term(clause.term)
            rows = self._visible_posting_rows(conn, clause.field_path, term, version)
            return {row["doc_id"] for row in rows}
        if clause.kind == "phrase":
            if not clause.terms:
                raise BusinessError("E_BAD_QUERY", "phrase clause requires at least one term")
            mappings: list[dict[str, tuple[int, ...]]] = []
            for raw_term in clause.terms:
                term = normalize_term(raw_term)
                rows = self._visible_posting_rows(conn, clause.field_path, term, version)
                mappings.append(
                    {
                        row["doc_id"]: tuple(int(p) for p in json.loads(row["positions_json"]))
                        for row in rows
                    }
                )
            return self._phrase_docs(mappings)
        raise BusinessError("E_BAD_QUERY", f"Unknown clause kind: {clause.kind!r}")

    def search(
        self,
        *,
        clauses: Sequence[SearchClause],
        operator: str = "AND",
        limit: int = 20,
        offset: int = 0,
        version: int | None = None,
    ) -> SearchResponse:
        operator = operator.upper()
        if operator not in {"AND", "OR"}:
            raise BusinessError("E_BAD_QUERY", "operator must be AND or OR")
        if not clauses:
            raise BusinessError("E_BAD_QUERY", "At least one clause is required")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > 10_000:
            raise BusinessError("E_BAD_QUERY", "limit must be between 1 and 10000")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise BusinessError("E_BAD_QUERY", "offset must be non-negative")
        if any(clause.kind not in {"term", "phrase"} for clause in clauses):
            raise BusinessError("E_BAD_QUERY", "only term and phrase clauses are supported")
        for clause in clauses:
            self._ensure_known_path(clause.field_path)

        with self._query_view(version) as resolved:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                clause_sets = [self._clause_docs(conn, clause, resolved) for clause in clauses]
                if operator == "AND":
                    ids = set.intersection(*clause_sets) if clause_sets else set()
                else:
                    ids = set.union(*clause_sets) if clause_sets else set()
                ordered_ids = sorted(ids)
                total = len(ordered_ids)
                page_ids = ordered_ids[offset : offset + limit]
                hits: list[SearchHit] = []
                for doc_id in page_ids:
                    document = self.get_document_on_connection(conn, doc_id, resolved)
                    if document is not None:
                        hits.append(SearchHit(doc_id=doc_id, document=document))
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()
        return SearchResponse(
            version=resolved,
            total=total,
            limit=limit,
            offset=offset,
            items=tuple(hits),
            operator=operator,
        )

    @staticmethod
    def get_document_on_connection(
        conn: sqlite3.Connection, doc_id: str, version: int
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT document_json FROM document_versions "
            "WHERE doc_id = ? AND start_version <= ? AND (end_version IS NULL OR end_version > ?) "
            "ORDER BY start_version DESC LIMIT 1",
            (doc_id, version, version),
        ).fetchone()
        return None if row is None else json.loads(row["document_json"])

    def get_document(self, doc_id: str, version: int | None = None) -> dict[str, Any] | None:
        with self._query_view(version) as resolved:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                document = self.get_document_on_connection(conn, doc_id, resolved)
                conn.execute("COMMIT")
                return document
            finally:
                conn.close()

    # ------------------------------------------------------------------
    # Retirement
    # ------------------------------------------------------------------
    def retire_versions(self, *, retain_versions: int = 3) -> dict[str, Any]:
        if not isinstance(retain_versions, int) or isinstance(retain_versions, bool) or retain_versions < 1:
            raise BusinessError("E_BAD_RETENTION", "retain_versions must be at least 1")
        with self._write_lock, self._view_condition:
            current = self._published_version
            cutoff = current - retain_versions
            candidates = {
                version
                for version in self._known_versions
                if version <= cutoff
                and version not in self._retired_versions
                and version not in self._retiring_versions
                and self._active_queries.get(version, 0) == 0
            }
            self._retiring_versions.update(candidates)

        retired_now: list[int] = []
        if candidates:
            first_retained = cutoff + 1
            with self._view_condition:
                minimum_active = min(self._active_queries) if self._active_queries else None
            safe_reclaim_boundary = first_retained
            if minimum_active is not None:
                safe_reclaim_boundary = min(safe_reclaim_boundary, minimum_active)
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                placeholders = ",".join("?" for _ in candidates)
                now = utc_now()
                conn.execute(
                    f"UPDATE versions SET retired = 1, retired_at = ? "
                    f"WHERE version IN ({placeholders}) AND retired = 0",
                    (now, *sorted(candidates)),
                )
                # Data wholly outside the retained version window is no longer
                # observable by a legal query and can be reclaimed. Boundary
                # intervals are retained because they may span the cutoff. An
                # active older query lowers the safe boundary further.
                conn.execute(
                    "DELETE FROM postings WHERE end_version IS NOT NULL AND end_version <= ?",
                    (safe_reclaim_boundary,),
                )
                conn.execute(
                    "DELETE FROM document_versions WHERE end_version IS NOT NULL AND end_version <= ?",
                    (safe_reclaim_boundary,),
                )
                conn.execute("COMMIT")
                retired_now = sorted(candidates)
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                with self._view_condition:
                    self._retiring_versions.difference_update(candidates)
                    self._view_condition.notify_all()
                raise
            finally:
                conn.close()

        with self._view_condition:
            self._retired_versions.update(retired_now)
            self._retiring_versions.difference_update(candidates)
            self._view_condition.notify_all()
        return {
            "retired_versions": retired_now,
            "retain_versions": retain_versions,
            "current_version": current,
        }

    # ------------------------------------------------------------------
    # Segments and merge
    # ------------------------------------------------------------------
    def list_segments(self, *, include_merged: bool = True) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            where = "" if include_merged else "WHERE s.status = 'active'"
            rows = conn.execute(
                "SELECT s.segment_id, s.status, s.min_version, s.max_version, s.created_at, "
                "s.merged_into, s.merge_operation_id, COUNT(p.posting_id) AS posting_count "
                f"FROM segments s LEFT JOIN postings p ON p.segment_id = s.segment_id "
                f"{where} GROUP BY s.segment_id ORDER BY s.min_version, s.segment_id"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_index_fields(self) -> list[str]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT path FROM index_fields ORDER BY path").fetchall()
            return [row["path"] for row in rows]
        finally:
            conn.close()

    def merge_segments(self, source_segment_ids: Sequence[str] | None = None) -> MergeResult:
        operation_id = f"merge:{uuid.uuid4().hex}"
        target_segment_id = f"seg-merge-{operation_id.split(':', 1)[1][:12]}"
        with self._write_lock:
            conn = self._connect()
            try:
                if source_segment_ids is None:
                    rows = conn.execute(
                        "SELECT segment_id FROM segments WHERE status = 'active' "
                        "ORDER BY min_version, segment_id LIMIT 4"
                    ).fetchall()
                else:
                    rows = [{"segment_id": sid} for sid in source_segment_ids]
                source_ids = [str(row["segment_id"]) for row in rows]
                if len(set(source_ids)) < 2:
                    raise BusinessError(
                        "E_NOT_ENOUGH_SEGMENTS",
                        "At least two distinct active segments are required for a merge",
                        status_code=409,
                    )
                placeholders = ",".join("?" for _ in source_ids)
                source_rows = conn.execute(
                    f"SELECT segment_id, status, min_version, max_version FROM segments "
                    f"WHERE segment_id IN ({placeholders})",
                    tuple(source_ids),
                ).fetchall()
                by_id = {row["segment_id"]: row for row in source_rows}
                if len(by_id) != len(source_ids) or any(
                    by_id[sid]["status"] != "active" for sid in source_ids
                ):
                    raise BusinessError(
                        "E_SEGMENT_NOT_ACTIVE",
                        "All merge sources must exist and be active",
                        status_code=409,
                        details={"source_segment_ids": source_ids},
                    )
            finally:
                conn.close()

            payload = {"source_segment_ids": source_ids, "target_segment_id": target_segment_id}
            self._create_operation(operation_id, "merge", payload)
            self._hit_failpoint("merge:after_intent")

            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                now = utc_now()
                min_version = min(int(by_id[sid]["min_version"]) for sid in source_ids)
                max_version = max(int(by_id[sid]["max_version"]) for sid in source_ids)
                conn.execute(
                    "INSERT INTO segments(segment_id, status, min_version, max_version, created_at, "
                    "merge_operation_id) VALUES(?, 'active', ?, ?, ?, ?)",
                    (target_segment_id, min_version, max_version, now, operation_id),
                )
                placeholders = ",".join("?" for _ in source_ids)
                posting_rows = conn.execute(
                    f"SELECT field_path, term, doc_id, start_version, end_version, "
                    f"term_frequency, positions_json FROM postings "
                    f"WHERE segment_id IN ({placeholders}) "
                    f"ORDER BY field_path, term, doc_id, start_version",
                    tuple(source_ids),
                ).fetchall()
                compacted = self._compact_posting_rows(posting_rows)
                conn.executemany(
                    "INSERT INTO postings(segment_id, field_path, term, doc_id, start_version, "
                    "end_version, term_frequency, positions_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            target_segment_id,
                            row["field_path"],
                            row["term"],
                            row["doc_id"],
                            row["start_version"],
                            row["end_version"],
                            row["term_frequency"],
                            row["positions_json"],
                        )
                        for row in compacted
                    ],
                )
                conn.execute(
                    f"DELETE FROM postings WHERE segment_id IN ({placeholders})", tuple(source_ids)
                )
                conn.execute(
                    f"UPDATE segments SET status = 'merged', merged_into = ?, merge_operation_id = ? "
                    f"WHERE segment_id IN ({placeholders})",
                    (target_segment_id, operation_id, *source_ids),
                )
                outcome = {
                    "target_segment_id": target_segment_id,
                    "source_segment_ids": source_ids,
                    "copied_postings": len(compacted),
                }
                conn.execute(
                    "UPDATE operations SET status = 'completed', updated_at = ?, outcome_json = ? "
                    "WHERE operation_id = ?",
                    (utc_now(), _json(outcome), operation_id),
                )
                self._hit_failpoint("merge:after_copy")
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                conn.close()
                self._abort_operation_on_exception(operation_id)
                raise
            else:
                conn.close()

        return MergeResult(
            operation_id=operation_id,
            target_segment_id=target_segment_id,
            source_segment_ids=tuple(source_ids),
            copied_postings=len(compacted),
        )

    @staticmethod
    def _compact_posting_rows(rows: Sequence[sqlite3.Row]) -> list[dict[str, Any]]:
        """Coalesce adjacent intervals only when tf and positions are identical."""

        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (row["field_path"], row["term"], row["doc_id"])
            grouped.setdefault(key, []).append(dict(row))
        output: list[dict[str, Any]] = []
        for key in sorted(grouped):
            pieces = grouped[key]
            current: dict[str, Any] | None = None
            for piece in pieces:
                if current is None:
                    current = dict(piece)
                    continue
                if (
                    current["end_version"] == piece["start_version"]
                    and current["term_frequency"] == piece["term_frequency"]
                    and current["positions_json"] == piece["positions_json"]
                ):
                    current["end_version"] = piece["end_version"]
                else:
                    output.append(current)
                    current = dict(piece)
            if current is not None:
                output.append(current)
        return output

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def _latest_report_row(self) -> sqlite3.Row | None:
        conn = self._connect()
        try:
            return conn.execute(
                "SELECT created_at, report_json FROM recovery_reports "
                "ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()

    def recovery_report(self) -> dict[str, Any] | None:
        row = self._latest_report_row()
        if row is None:
            return None
        report = json.loads(row["report_json"])
        report["created_at"] = row["created_at"]
        return report

    def recover(self) -> dict[str, Any]:
        started_at = utc_now()
        actions: list[dict[str, Any]] = []
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT operation_id, kind, payload_json FROM operations "
                    "WHERE status = 'pending' ORDER BY created_at, operation_id"
                ).fetchall()
                for operation in pending:
                    operation_id = operation["operation_id"]
                    kind = operation["kind"]
                    payload = json.loads(operation["payload_json"])
                    if kind == "batch":
                        version_row = conn.execute(
                            "SELECT version FROM versions WHERE operation_id = ?", (operation_id,)
                        ).fetchone()
                        if version_row is not None:
                            decision = "committed-version-present"
                            version = int(version_row["version"])
                            conn.execute(
                                "UPDATE operations SET status = 'completed', updated_at = ?, "
                                "committed_version = ? WHERE operation_id = ?",
                                (utc_now(), version, operation_id),
                            )
                        else:
                            decision = "aborted-uncommitted-batch"
                            version = None
                            conn.execute(
                                "UPDATE operations SET status = 'aborted', updated_at = ?, outcome_json = ? "
                                "WHERE operation_id = ?",
                                (utc_now(), _json({"reason": decision}), operation_id),
                            )
                        actions.append(
                            {
                                "operation_id": operation_id,
                                "kind": kind,
                                "decision": decision,
                                "committed_version": version,
                            }
                        )
                    elif kind == "merge":
                        target_id = payload.get("target_segment_id")
                        source_ids = list(payload.get("source_segment_ids", []))
                        target = conn.execute(
                            "SELECT status, merge_operation_id FROM segments WHERE segment_id = ?",
                            (target_id,),
                        ).fetchone()
                        if target is not None and target["status"] == "active" and target["merge_operation_id"] == operation_id:
                            decision = "merge-already-committed"
                            conn.execute(
                                "UPDATE operations SET status = 'completed', updated_at = ? WHERE operation_id = ?",
                                (utc_now(), operation_id),
                            )
                        else:
                            decision = "discarded-uncommitted-merge"
                            if target_id:
                                conn.execute("DELETE FROM postings WHERE segment_id = ?", (target_id,))
                                conn.execute("DELETE FROM segments WHERE segment_id = ?", (target_id,))
                            if source_ids:
                                placeholders = ",".join("?" for _ in source_ids)
                                conn.execute(
                                    f"UPDATE segments SET status = 'active', merged_into = NULL, "
                                    f"merge_operation_id = NULL WHERE segment_id IN ({placeholders})",
                                    tuple(source_ids),
                                )
                            conn.execute(
                                "UPDATE operations SET status = 'aborted', updated_at = ?, outcome_json = ? "
                                "WHERE operation_id = ?",
                                (utc_now(), _json({"reason": decision}), operation_id),
                            )
                        actions.append(
                            {
                                "operation_id": operation_id,
                                "kind": kind,
                                "decision": decision,
                                "target_segment_id": target_id,
                                "source_segment_ids": source_ids,
                            }
                        )
                    else:
                        decision = "aborted-unknown-operation-kind"
                        conn.execute(
                            "UPDATE operations SET status = 'aborted', updated_at = ? WHERE operation_id = ?",
                            (utc_now(), operation_id),
                        )
                        actions.append(
                            {"operation_id": operation_id, "kind": kind, "decision": decision}
                        )

                current_row = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) AS v FROM versions"
                ).fetchone()
                last_version = int(current_row["v"])
                active_segments = conn.execute(
                    "SELECT COUNT(*) AS n FROM segments WHERE status = 'active'"
                ).fetchone()["n"]
                report = {
                    "started_at": started_at,
                    "completed_at": utc_now(),
                    "pending_operations_found": len(pending),
                    "last_committed_version": last_version,
                    "active_segment_count": int(active_segments),
                    "actions": actions,
                    "note": "Report contains only facts persisted in SQLite before restart.",
                }
                conn.execute(
                    "INSERT INTO recovery_reports(created_at, report_json) VALUES(?, ?)",
                    (report["completed_at"], _json(report)),
                )
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()
        return report
