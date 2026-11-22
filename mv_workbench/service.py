from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .engine import execute_view, plan_materialization
from .errors import WorkbenchError, fail
from .graph import ViewGraph, build_graph, graph_payload
from .samples import load_json, sample_paths
from .states import (
    FRESH,
    PARTIAL,
    REFRESHING,
    STALE,
    STATES,
    assert_transition,
)
from .store import (
    Database,
    current_graph_version,
    flatten_groups,
    insert_commit,
    insert_invalidation_event,
    insert_state_transition,
    latest_commit_seq,
    load_all_view_states,
    load_base_rows,
    load_base_snapshot,
    load_base_table_row,
    load_graph_history,
    load_graph_row,
    load_latest_view_rows,
    load_view_groups,
    load_view_state,
    load_view_version,
    max_view_version,
    missing_view,
    table_names,
    update_view_state,
)
from .util import canonical_json, digest_json, require_list, require_str, utc_now


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SAMPLES_DIR = PROJECT_ROOT / "samples"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "workbench.db"


class WorkbenchService:
    """Single-writer domain service backed by SQLite facts."""

    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        *,
        samples_dir: str | Path = DEFAULT_SAMPLES_DIR,
        auto_seed: bool = True,
        crash_point: str | None = None,
    ) -> None:
        self.db = Database(db_path)
        self.samples_dir = Path(samples_dir)
        self.auto_seed = auto_seed
        self.crash_point = crash_point or os.environ.get("MV_WORKBENCH_CRASH_POINT")
        self.writer_lock = threading.RLock()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle and seed
    # ------------------------------------------------------------------
    def initialize(self) -> dict[str, Any]:
        with self.writer_lock:
            if self._initialized:
                return self.recovery_report()["latest"]
            self.db.initialize_schema()
            report = self._recover_locked()
            if self.auto_seed and self._graph_version() is None:
                self._seed_locked()
            self._initialized = True
            return report

    def _graph_version(self) -> int | None:
        with self.db.read_transaction() as connection:
            return current_graph_version(connection)

    def _seed_locked(self) -> None:
        paths = sample_paths(self.samples_dir)
        if not paths["base_tables"].exists() or not paths["view_definitions"].exists():
            fail(
                "E_SAMPLE_MISSING",
                "Deterministic sample JSON is missing.",
                context={"samples_dir": str(self.samples_dir)},
                status_code=500,
            )
        base_doc = load_json(paths["base_tables"])
        definitions = load_json(paths["view_definitions"])
        table_specs = self._decode_base_sample(base_doc)
        base_names = {table["name"] for table in table_specs}
        graph = build_graph(definitions, base_names)

        with self.db.write_transaction() as connection:
            commit_seq = insert_commit(
                connection,
                "SEED",
                {
                    "base_tables": sorted(base_names),
                    "graph_version": 1,
                    "definitions_digest": graph.digest,
                },
            )
            for table in table_specs:
                self._insert_initial_table(connection, table, commit_seq)
            connection.execute(
                "INSERT INTO graph_versions(graph_version, definitions_json, edges_json, "
                "layers_json, digest, commit_seq, created_at) VALUES (1, ?, ?, ?, ?, ?, ?)",
                (
                    canonical_json(definitions),
                    canonical_json(graph.edges),
                    canonical_json(graph.layers),
                    graph.digest,
                    commit_seq,
                    utc_now(),
                ),
            )
            for view_name in graph.order:
                gaps = [{"code": "NOT_MATERIALIZED", "reason": "initial graph seed"}]
                connection.execute(
                    "INSERT INTO view_states(view_name, graph_version, state, "
                    "materialized_version, state_commit_seq, gaps_json, updated_at) "
                    "VALUES (?, 1, ?, NULL, ?, ?, ?)",
                    (view_name, STALE, commit_seq, canonical_json(gaps), utc_now()),
                )
                insert_state_transition(
                    connection,
                    view_name=view_name,
                    from_state="INITIAL",
                    to_state=STALE,
                    reason="Seeded graph requires the first full refresh.",
                    gaps=gaps,
                    commit_seq=commit_seq,
                )

    def _decode_base_sample(self, document: Any) -> list[dict[str, Any]]:
        if not isinstance(document, dict) or not isinstance(document.get("tables"), list):
            fail(
                "E_INVALID_REQUEST",
                "Base table sample has an invalid shape.",
                context={"source": "base_tables.json"},
                status_code=500,
            )
        result: list[dict[str, Any]] = []
        for table in document["tables"]:
            if not isinstance(table, dict):
                fail(
                    "E_INVALID_REQUEST",
                    "Base table sample entry must be an object.",
                    status_code=500,
                )
            name = require_str(table.get("name"), field="base table name")
            columns = table.get("columns")
            if not isinstance(columns, list) or not columns:
                fail("E_INVALID_REQUEST", "Base table columns are missing.", status_code=500)
            data = table.get("columns_data")
            row_versions = table.get("row_versions")
            deleted = table.get("deleted")
            if not isinstance(data, dict) or not isinstance(row_versions, list) or not isinstance(deleted, list):
                fail("E_INVALID_REQUEST", "Base table columnar data is malformed.", status_code=500)
            lengths = {len(data.get(column.get("name"), [])) for column in columns}
            lengths.add(len(row_versions))
            lengths.add(len(deleted))
            if len(lengths) != 1:
                fail(
                    "E_INVALID_REQUEST",
                    "Base table column arrays have inconsistent lengths.",
                    context={"table": name, "lengths": sorted(lengths)},
                    status_code=500,
                )
            row_count = lengths.pop()
            rows: list[dict[str, Any]] = []
            for index in range(row_count):
                row = {column["name"]: data[column["name"]][index] for column in columns}
                self._validate_row(columns, row, table_name=name, require_complete=True)
                rows.append(
                    {
                        "data": row,
                        "row_version": int(row_versions[index]),
                        "deleted": bool(deleted[index]),
                    }
                )
            result.append(
                {
                    "name": name,
                    "columns": columns,
                    "version": int(table.get("version", 1)),
                    "generated_at": str(table.get("generated_at", utc_now())),
                    "rows": rows,
                }
            )
        return result

    def _insert_initial_table(
        self, connection: sqlite3.Connection, table: dict[str, Any], commit_seq: int
    ) -> None:
        name = table["name"]
        connection.execute(
            "INSERT INTO base_tables(name, columns_json, current_version, created_at) "
            "VALUES (?, ?, ?, ?)",
            (name, canonical_json(table["columns"]), table["version"], utc_now()),
        )
        for row in table["rows"]:
            row_id = canonical_json(row["data"][table["columns"][0]["name"]])
            content_digest = digest_json(
                {
                    "data": row["data"],
                    "deleted": row["deleted"],
                    "row_version": row["row_version"],
                }
            )
            connection.execute(
                "INSERT INTO base_rows(table_name, row_id, row_version, data_json, "
                "content_digest, deleted, updated_commit_seq) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    row_id,
                    row["row_version"],
                    canonical_json(row["data"]),
                    content_digest,
                    int(row["deleted"]),
                    commit_seq,
                ),
            )
            connection.execute(
                "INSERT INTO base_row_history(table_name, row_id, row_version, data_json, "
                "content_digest, deleted, valid_from_version, valid_to_version, commit_seq) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                (
                    name,
                    row_id,
                    row["row_version"],
                    canonical_json(row["data"]),
                    content_digest,
                    int(row["deleted"]),
                    table["version"],
                    commit_seq,
                ),
            )
        snapshot = self._snapshot_payload(connection, name)
        connection.execute(
            "INSERT INTO base_snapshots(table_name, version, generated_at, columns_json, "
            "null_bitmap_json, content_digest, row_count, live_count, commit_seq) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                name,
                table["version"],
                table["generated_at"],
                canonical_json(table["columns"]),
                canonical_json(snapshot["null_bitmap"]),
                snapshot["digest"],
                snapshot["row_count"],
                snapshot["live_count"],
                commit_seq,
            ),
        )

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------
    def _validate_row(
        self,
        columns: list[dict[str, Any]],
        row: dict[str, Any],
        *,
        table_name: str,
        require_complete: bool,
        deletion: bool = False,
    ) -> None:
        allowed = {column["name"] for column in columns}
        unknown = sorted(set(row) - allowed - {"_deleted"})
        if unknown:
            fail(
                "E_INVALID_REQUEST",
                "Batch row contains unknown columns.",
                context={"table": table_name, "unknown_columns": unknown},
            )
        primary_key = columns[0]["name"]
        if primary_key not in row:
            fail(
                "E_INVALID_REQUEST",
                "Batch row is missing its primary key.",
                context={"table": table_name, "primary_key": primary_key},
            )
        for column in columns:
            name = column["name"]
            if name not in row:
                if require_complete and not column["nullable"] and not deletion:
                    fail(
                        "E_INVALID_REQUEST",
                        "A non-deleted row must contain every non-nullable column.",
                        context={"table": table_name, "column": name},
                    )
                continue
            value = row[name]
            if value is None:
                if not column["nullable"]:
                    fail(
                        "E_INVALID_REQUEST",
                        "Null is not allowed for this column.",
                        context={"table": table_name, "column": name},
                    )
                continue
            expected = column["type"]
            valid = False
            if expected == "integer":
                valid = isinstance(value, int) and not isinstance(value, bool)
            elif expected == "number":
                valid = isinstance(value, (int, float)) and not isinstance(value, bool)
            elif expected == "string":
                valid = isinstance(value, str)
            elif expected == "boolean":
                valid = isinstance(value, bool)
            if not valid:
                fail(
                    "E_INVALID_REQUEST",
                    "Column value has the wrong data type.",
                    context={
                        "table": table_name,
                        "column": name,
                        "expected": expected,
                        "actual": type(value).__name__,
                    },
                )

    def _snapshot_payload(
        self, connection: sqlite3.Connection, table_name: str
    ) -> dict[str, Any]:
        table = load_base_table_row(connection, table_name)
        if table is None:
            fail("E_NOT_FOUND", "Base table does not exist.", context={"table": table_name}, status_code=404)
        rows = load_base_rows(connection, table_name, include_deleted=True)
        columns = table["columns"]
        null_bitmap = {
            column["name"]: [
                row["data"].get(column["name"]) is None
                for row in rows
            ]
            for column in columns
        }
        canonical_rows = [
            {
                "row_id": row["row_id"],
                "row_version": row["row_version"],
                "deleted": row["deleted"],
                "data": row["data"],
            }
            for row in rows
        ]
        return {
            "null_bitmap": null_bitmap,
            "digest": digest_json({"columns": columns, "rows": canonical_rows}),
            "row_count": len(rows),
            "live_count": sum(1 for row in rows if not row["deleted"]),
        }

    def _invalidate_views(
        self,
        connection: sqlite3.Connection,
        *,
        graph: ViewGraph,
        graph_version: int,
        views: set[str],
        depths: dict[str, int],
        commit_seq: int,
        trigger_type: str,
        trigger_id: str,
        reason: str,
        gap_code: str,
    ) -> None:
        for view_name in graph.order:
            if view_name not in views:
                continue
            state = load_view_state(connection, view_name)
            if state is None:
                continue
            gaps = [
                {
                    "code": gap_code,
                    "trigger_type": trigger_type,
                    "trigger_id": trigger_id,
                }
            ]
            assert_transition(state["state"], STALE, view_name)
            update_view_state(
                connection,
                view_name=view_name,
                graph_version=graph_version,
                state=STALE,
                materialized_version=state["materialized_version"],
                gaps=gaps,
                commit_seq=commit_seq,
                reason=reason,
                old_state=state["state"],
            )
            insert_invalidation_event(
                connection,
                commit_seq=commit_seq,
                trigger_type=trigger_type,
                trigger_id=trigger_id,
                view_name=view_name,
                depth=depths.get(view_name, 0),
                action="STALE",
                affected_groups=[],
                recomputed_groups=None,
                reason=reason,
            )

    # ------------------------------------------------------------------
    # Base table reads and incremental batches
    # ------------------------------------------------------------------
    def list_base_tables(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as connection:
            rows = connection.execute("SELECT name FROM base_tables ORDER BY name").fetchall()
            output = []
            for row in rows:
                table = load_base_table_row(connection, row["name"])
                snapshot = load_base_snapshot(connection, row["name"])
                output.append(
                    {
                        "name": row["name"],
                        "current_version": table["current_version"],
                        "columns": table["columns"],
                        "snapshot": snapshot,
                    }
                )
            return output

    def get_base_table(self, table_name: str, version: int | None = None) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            table = load_base_table_row(connection, table_name)
            if table is None:
                fail(
                    "E_NOT_FOUND",
                    "Base table does not exist.",
                    context={"table": table_name},
                    status_code=404,
                )
            if version is None:
                version = table["current_version"]
            snapshot = load_base_snapshot(connection, table_name, version)
            if snapshot is None:
                fail(
                    "E_VERSION_RETIRED",
                    "The requested base-table snapshot was never committed.",
                    context={"table": table_name, "version": version},
                    status_code=410,
                )
            history_rows = connection.execute(
                "SELECT * FROM base_row_history WHERE table_name = ? "
                "AND valid_from_version <= ? "
                "AND (valid_to_version IS NULL OR valid_to_version > ?) ORDER BY row_id",
                (table_name, version, version),
            ).fetchall()
            rows = [
                {
                    "row_id": row["row_id"],
                    "row_version": int(row["row_version"]),
                    "deleted": bool(row["deleted"]),
                    "data": json.loads(row["data_json"]),
                    "content_digest": row["content_digest"],
                }
                for row in history_rows
            ]
            return {
                "table": table_name,
                "current_version": table["current_version"],
                "requested_version": version,
                "snapshot": snapshot,
                "rows": rows,
            }

    def submit_batch(self, batch: dict[str, Any], *, table_name: str | None = None) -> dict[str, Any]:
        if not isinstance(batch, dict):
            fail(
                "E_STALE_BASE",
                "Batch body must be a JSON object; no state was changed.",
                status_code=409,
            )
        if "batches" in batch:
            fail(
                "E_INVALID_REQUEST",
                "Submit exactly one incremental batch per request.",
                context={"field": "batches"},
            )
        target = require_str(batch.get("table", table_name), field="batch.table")
        if table_name is not None and target != table_name:
            fail(
                "E_INVALID_REQUEST",
                "Batch table does not match the URL table.",
                context={"url_table": table_name, "batch_table": target},
            )
        base_version = batch.get("base_version")
        if isinstance(base_version, bool) or not isinstance(base_version, int):
            fail(
                "E_STALE_BASE",
                "Batch base_version is missing or invalid; no state was changed.",
                context={"table": target, "base_version": base_version},
                status_code=409,
            )
        rows = require_list(batch.get("rows"), field="batch.rows")
        if not rows:
            fail("E_INVALID_REQUEST", "A batch must contain at least one row.", context={"table": target})
        batch_id = str(batch.get("batch_id") or ("batch-" + digest_json(batch)[:12]))
        generated_at = str(batch.get("generated_at") or utc_now())

        with self.writer_lock:
            with self.db.read_transaction() as connection:
                table = load_base_table_row(connection, target)
                if table is None:
                    fail(
                        "E_NOT_FOUND",
                        "Base table does not exist.",
                        context={"table": target},
                        status_code=404,
                    )
                if base_version != table["current_version"]:
                    fail(
                        "E_STALE_BASE",
                        "Batch base_version is not the current base-table version.",
                        context={
                            "table": target,
                            "base_version": base_version,
                            "current_version": table["current_version"],
                            "batch_id": batch_id,
                        },
                        status_code=409,
                    )
                graph_row = load_graph_row(connection)
                if graph_row is None:
                    fail("E_NOT_FOUND", "No view graph is committed.", status_code=404)
                graph = build_graph(graph_row["definitions"], table_names(connection))
                existing_rows = {
                    row["row_id"]: {
                        "data": json.loads(row["data_json"]),
                        "deleted": bool(row["deleted"]),
                        "row_version": int(row["row_version"]),
                    }
                    for row in connection.execute(
                        "SELECT row_id, data_json, deleted, row_version FROM base_rows "
                        "WHERE table_name = ?",
                        (target,),
                    ).fetchall()
                }

            columns = table["columns"]
            primary_key = columns[0]["name"]
            normalized: list[tuple[dict[str, Any], bool]] = []
            seen_ids: set[str] = set()
            for raw_row in rows:
                if not isinstance(raw_row, dict):
                    fail(
                        "E_INVALID_REQUEST",
                        "Each batch row must be an object.",
                        context={"table": target},
                    )
                if "_deleted" in raw_row and not isinstance(raw_row["_deleted"], bool):
                    fail(
                        "E_INVALID_REQUEST",
                        "_deleted must be a boolean when supplied.",
                        context={"table": target, "row": raw_row.get(primary_key)},
                    )
                deleted = bool(raw_row.get("_deleted", False))
                row_id = canonical_json(raw_row.get(primary_key))
                if row_id in seen_ids:
                    fail(
                        "E_INVALID_REQUEST",
                        "A batch cannot address the same row twice.",
                        context={"table": target, "row_id": row_id},
                    )
                seen_ids.add(row_id)
                data = {key: value for key, value in raw_row.items() if key != "_deleted"}
                existing = existing_rows.get(row_id)
                if existing is not None:
                    merged = dict(existing["data"])
                    merged.update(data)
                    data = merged
                self._validate_row(
                    columns,
                    data,
                    table_name=target,
                    require_complete=not deleted,
                    deletion=deleted,
                )
                normalized.append((data, deleted))

            direct_roots = {
                view_name
                for view_name, bases in graph.base_dependencies.items()
                if target in bases
            }
            affected = graph.downstream(direct_roots) if direct_roots else set()
            depths = graph.depth_from(direct_roots)
            next_version = base_version + 1
            intent_id = f"batch:{batch_id}:{uuid.uuid4().hex[:10]}"

            with self.db.write_transaction() as connection:
                connection.execute(
                    "INSERT INTO intents(intent_id, kind, status, payload_digest, "
                    "details_json, created_at, updated_at, commit_seq) "
                    "VALUES (?, 'BASE_BATCH', 'RUNNING', ?, ?, ?, ?, NULL)",
                    (
                        intent_id,
                        digest_json({"batch": batch, "table": target}),
                        canonical_json(
                            {
                                "batch_id": batch_id,
                                "table": target,
                                "base_version": base_version,
                                "next_version": next_version,
                                "selected_views": sorted(affected),
                            }
                        ),
                        utc_now(),
                        utc_now(),
                    ),
                )
            self._maybe_crash("after_batch_intent")

            try:
                with self.db.write_transaction() as connection:
                    table_now = load_base_table_row(connection, target)
                    if table_now is None or table_now["current_version"] != base_version:
                        fail(
                            "E_STALE_BASE",
                            "Base version changed before the batch transaction.",
                            context={"table": target, "base_version": base_version},
                            status_code=409,
                        )
                    graph_now = load_graph_row(connection)
                    if graph_now is None:
                        fail("E_NOT_FOUND", "No view graph is committed.", status_code=404)
                    graph = build_graph(graph_now["definitions"], table_names(connection))
                    commit_seq = insert_commit(
                        connection,
                        "BASE_BATCH",
                        {
                            "batch_id": batch_id,
                            "table": target,
                            "base_version": base_version,
                            "committed_version": next_version,
                            "affected_views": sorted(affected),
                        },
                    )
                    change_facts: list[dict[str, Any]] = []
                    for data, deleted in normalized:
                        row_id = canonical_json(data[primary_key])
                        existing = connection.execute(
                            "SELECT * FROM base_rows WHERE table_name = ? AND row_id = ?",
                            (target, row_id),
                        ).fetchone()
                        if existing is not None:
                            connection.execute(
                                "UPDATE base_row_history SET valid_to_version = ? "
                                "WHERE table_name = ? AND row_id = ? AND valid_to_version IS NULL",
                                (next_version, target, row_id),
                            )
                            previous_data = json.loads(existing["data_json"])
                            if deleted:
                                merged = dict(previous_data)
                                merged.update({key: value for key, value in data.items() if key != primary_key})
                                data = merged
                            row_version = int(existing["row_version"]) + 1
                        else:
                            row_version = 1
                        content_digest = digest_json(
                            {"data": data, "deleted": deleted, "row_version": row_version}
                        )
                        connection.execute(
                            "INSERT OR REPLACE INTO base_rows(table_name, row_id, row_version, "
                            "data_json, content_digest, deleted, updated_commit_seq) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                target,
                                row_id,
                                row_version,
                                canonical_json(data),
                                content_digest,
                                int(deleted),
                                commit_seq,
                            ),
                        )
                        connection.execute(
                            "INSERT INTO base_row_history(table_name, row_id, row_version, "
                            "data_json, content_digest, deleted, valid_from_version, "
                            "valid_to_version, commit_seq) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                            (
                                target,
                                row_id,
                                row_version,
                                canonical_json(data),
                                content_digest,
                                int(deleted),
                                next_version,
                                commit_seq,
                            ),
                        )
                        change_facts.append(
                            {
                                "row_id": row_id,
                                "row_version": row_version,
                                "deleted": deleted,
                                "content_digest": content_digest,
                            }
                        )
                    connection.execute(
                        "UPDATE base_tables SET current_version = ? WHERE name = ?",
                        (next_version, target),
                    )
                    snapshot = self._snapshot_payload(connection, target)
                    table_now = load_base_table_row(connection, target)
                    connection.execute(
                        "INSERT INTO base_snapshots(table_name, version, generated_at, "
                        "columns_json, null_bitmap_json, content_digest, row_count, "
                        "live_count, commit_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            target,
                            next_version,
                            generated_at,
                            canonical_json(table_now["columns"]),
                            canonical_json(snapshot["null_bitmap"]),
                            snapshot["digest"],
                            snapshot["row_count"],
                            snapshot["live_count"],
                            commit_seq,
                        ),
                    )
                    self._invalidate_views(
                        connection,
                        graph=graph,
                        graph_version=graph_now["graph_version"],
                        views=affected,
                        depths=depths,
                        commit_seq=commit_seq,
                        trigger_type="BASE_BATCH",
                        trigger_id=batch_id,
                        reason=f"Base table {target} advanced to version {next_version}.",
                        gap_code="STALE_INPUT",
                    )
                    connection.execute(
                        "UPDATE intents SET status = 'COMMITTED', commit_seq = ?, updated_at = ? "
                        "WHERE intent_id = ?",
                        (commit_seq, utc_now(), intent_id),
                    )
                    connection.execute(
                        "UPDATE intents SET details_json = ?, updated_at = ? WHERE intent_id = ?",
                        (
                            canonical_json(
                                {
                                    "batch_id": batch_id,
                                    "table": target,
                                    "base_version": base_version,
                                    "next_version": next_version,
                                    "selected_views": sorted(affected),
                                    "change_facts": change_facts,
                                }
                            ),
                            utc_now(),
                            intent_id,
                        ),
                    )
            except BaseException:
                with self.db.write_transaction() as connection:
                    connection.execute(
                        "UPDATE intents SET status = 'ABORTED', updated_at = ? "
                        "WHERE intent_id = ? AND status = 'RUNNING'",
                        (utc_now(), intent_id),
                    )
                raise
            state_rows = self.list_views()
        state_map = {row["view"]: row for row in state_rows}
        return {
            "batch_id": batch_id,
            "table": target,
            "base_version": base_version,
            "committed_version": next_version,
            "generated_at": generated_at,
            "commit_seq": commit_seq,
            "changed_rows": len(normalized),
            "change_facts": change_facts,
            "affected_views": [
                {
                    "view": view_name,
                    "depth": depths.get(view_name, 0),
                    "state": state_map[view_name]["state"] if view_name in state_map else "UNKNOWN",
                }
                for view_name in sorted(affected, key=lambda name: (depths.get(name, 0), name))
            ],
        }

    # ------------------------------------------------------------------
    # Definition replacement
    # ------------------------------------------------------------------
    def replace_definitions(self, definitions: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(definitions, dict):
            fail("E_STALE_BASE", "Definitions must be a JSON object.", status_code=409)
        with self.writer_lock:
            with self.db.read_transaction() as connection:
                before_version = current_graph_version(connection)
                before_row = load_graph_row(connection)
                base_names = table_names(connection)
                if before_version is None or before_row is None:
                    fail("E_NOT_FOUND", "No view graph is committed.", status_code=404)
            new_graph = build_graph(definitions, base_names)
            before_graph = build_graph(before_row["definitions"], base_names)
            before_defs = before_graph.views
            changed = sorted(
                name
                for name, view in new_graph.views.items()
                if name not in before_defs
                or canonical_json(view) != canonical_json(before_defs[name])
            )
            removed = sorted(set(before_defs) - set(new_graph.views))
            roots = set(changed)
            affected = new_graph.downstream(roots) if roots else set()
            depths = new_graph.depth_from(roots)
            next_graph_version = before_version + 1
            intent_id = f"definitions:{next_graph_version}:{uuid.uuid4().hex[:10]}"

            with self.db.write_transaction() as connection:
                connection.execute(
                    "INSERT INTO intents(intent_id, kind, status, payload_digest, "
                    "details_json, created_at, updated_at, commit_seq) "
                    "VALUES (?, 'REPLACE_DEFINITIONS', 'RUNNING', ?, ?, ?, ?, NULL)",
                    (
                        intent_id,
                        digest_json(definitions),
                        canonical_json(
                            {
                                "previous_graph_version": before_version,
                                "next_graph_version": next_graph_version,
                                "changed_views": changed,
                                "removed_views": removed,
                                "selected_views": sorted(affected),
                            }
                        ),
                        utc_now(),
                        utc_now(),
                    ),
                )
            self._maybe_crash("after_definitions_intent")

            try:
                with self.db.write_transaction() as connection:
                    commit_seq = insert_commit(
                        connection,
                        "REPLACE_DEFINITIONS",
                        {
                            "previous_graph_version": before_version,
                            "graph_version": next_graph_version,
                            "changed_views": changed,
                            "removed_views": removed,
                            "affected_views": sorted(affected),
                        },
                    )
                    connection.execute(
                        "INSERT INTO graph_versions(graph_version, definitions_json, edges_json, "
                        "layers_json, digest, commit_seq, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            next_graph_version,
                            canonical_json(definitions),
                            canonical_json(new_graph.edges),
                            canonical_json(new_graph.layers),
                            new_graph.digest,
                            commit_seq,
                            utc_now(),
                        ),
                    )
                    for view_name in new_graph.order:
                        state = load_view_state(connection, view_name)
                        if state is None:
                            gaps = [{"code": "NOT_MATERIALIZED", "graph_version": next_graph_version}]
                            connection.execute(
                                "INSERT INTO view_states(view_name, graph_version, state, "
                                "materialized_version, state_commit_seq, gaps_json, updated_at) "
                                "VALUES (?, ?, ?, NULL, ?, ?, ?)",
                                (
                                    view_name,
                                    next_graph_version,
                                    STALE,
                                    commit_seq,
                                    canonical_json(gaps),
                                    utc_now(),
                                ),
                            )
                            insert_state_transition(
                                connection,
                                view_name=view_name,
                                from_state="INITIAL",
                                to_state=STALE,
                                reason="New view introduced by definition replacement.",
                                gaps=gaps,
                                commit_seq=commit_seq,
                            )
                        elif view_name in affected:
                            materialized_version = state["materialized_version"]
                            gaps = [
                                {
                                    "code": "STALE_DEFINITION",
                                    "graph_version": next_graph_version,
                                    "changed_roots": changed,
                                }
                            ]
                            assert_transition(state["state"], STALE, view_name)
                            update_view_state(
                                connection,
                                view_name=view_name,
                                graph_version=next_graph_version,
                                state=STALE,
                                materialized_version=materialized_version,
                                gaps=gaps,
                                commit_seq=commit_seq,
                                reason="View definition or an upstream definition changed.",
                                old_state=state["state"],
                            )
                        else:
                            connection.execute(
                                "UPDATE view_states SET graph_version = ?, updated_at = ? "
                                "WHERE view_name = ?",
                                (next_graph_version, utc_now(), view_name),
                            )
                        if view_name in affected:
                            insert_invalidation_event(
                                connection,
                                commit_seq=commit_seq,
                                trigger_type="VIEW_DEFINITIONS",
                                trigger_id=str(next_graph_version),
                                view_name=view_name,
                                depth=depths.get(view_name, 0),
                                action="STALE",
                                affected_groups=[],
                                recomputed_groups=None,
                                reason="Definition replacement cascaded to this view.",
                            )
                    for view_name in removed:
                        insert_invalidation_event(
                            connection,
                            commit_seq=commit_seq,
                            trigger_type="VIEW_DEFINITIONS",
                            trigger_id=str(next_graph_version),
                            view_name=view_name,
                            depth=0,
                            action="REMOVED",
                            affected_groups=[],
                            recomputed_groups=None,
                            reason="View was removed by a complete definition replacement.",
                        )
                        connection.execute(
                            "DELETE FROM view_states WHERE view_name = ?", (view_name,)
                        )
                    connection.execute(
                        "UPDATE intents SET status = 'COMMITTED', commit_seq = ?, updated_at = ? "
                        "WHERE intent_id = ?",
                        (commit_seq, utc_now(), intent_id),
                    )
            except BaseException:
                with self.db.write_transaction() as connection:
                    connection.execute(
                        "UPDATE intents SET status = 'ABORTED', updated_at = ? "
                        "WHERE intent_id = ? AND status = 'RUNNING'",
                        (utc_now(), intent_id),
                    )
                raise
            state_map = {row["view"]: row for row in self.list_views()}
        return {
            "previous_graph_version": before_version,
            "graph_version": next_graph_version,
            "digest": new_graph.digest,
            "changed_views": changed,
            "new_views": [name for name in changed if name not in before_defs],
            "removed_views": removed,
            "commit_seq": commit_seq,
            "affected_views": [
                {
                    "view": view_name,
                    "depth": depths.get(view_name, 0),
                    "state": state_map[view_name]["state"] if view_name in state_map else "REMOVED",
                }
                for view_name in sorted(affected, key=lambda name: (depths.get(name, 0), name))
            ],
            "layers": new_graph.layers,
            "edges": new_graph.edges,
        }

    def get_view_definitions(self, version: int | None = None) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            row = load_graph_row(connection, version)
            if row is None:
                fail(
                    "E_NOT_FOUND",
                    "View definition graph version does not exist.",
                    context={"graph_version": version},
                    status_code=404,
                )
            return {
                "graph_version": row["graph_version"],
                "definitions": row["definitions"],
                "digest": row["digest"],
                "commit_seq": row["commit_seq"],
                "created_at": row["created_at"],
            }

    def get_graph(self, version: int | None = None) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            row = load_graph_row(connection, version)
            if row is None:
                fail(
                    "E_NOT_FOUND",
                    "Dependency graph version does not exist.",
                    context={"graph_version": version},
                    status_code=404,
                )
            graph = build_graph(row["definitions"], table_names(connection))
            return {
                "graph_version": row["graph_version"],
                "digest": row["digest"],
                "commit_seq": row["commit_seq"],
                "created_at": row["created_at"],
                **graph_payload(graph, graph_version=row["graph_version"]),
                "history": load_graph_history(connection),
            }

    # ------------------------------------------------------------------
    # Refresh and snapshots
    # ------------------------------------------------------------------
    def refresh(
        self,
        request_id: str,
        *,
        mode: str = "all_stale",
        views: list[str] | None = None,
        include_downstream: bool = True,
        delay_ms: int = 0,
        fail_views: list[str] | None = None,
    ) -> dict[str, Any]:
        request_id = require_str(request_id, field="request_id")
        if mode not in {"all_stale", "all", "views"}:
            fail(
                "E_INVALID_REQUEST",
                "Refresh mode must be all_stale, all, or views.",
                context={"mode": mode},
            )
        if delay_ms < 0 or delay_ms > 10_000:
            fail("E_INVALID_REQUEST", "delay_ms must be between 0 and 10000.", context={"delay_ms": delay_ms})
        requested_views = sorted(set(views or []))
        failed_views = sorted(set(fail_views or []))
        normalized = {
            "request_id": request_id,
            "mode": mode,
            "views": requested_views,
            "include_downstream": bool(include_downstream),
            "fail_views": failed_views,
        }
        request_digest = digest_json(normalized)

        with self.writer_lock:
            existing = self._load_request(request_id)
            if existing is not None:
                if existing["digest"] != request_digest:
                    fail(
                        "E_CONFLICTING_REQUEST",
                        "The refresh request id was already used with different content.",
                        context={
                            "request_id": request_id,
                            "existing_digest": existing["digest"],
                            "submitted_digest": request_digest,
                            "existing_status": existing["status"],
                        },
                        status_code=409,
                    )
                if existing["status"] == "COMPLETED":
                    return json.loads(existing["response_json"])
                if existing["status"] == "IN_PROGRESS":
                    fail(
                        "E_REFRESH_IN_PROGRESS",
                        "The refresh request is still in progress.",
                        context={"request_id": request_id},
                        status_code=409,
                    )
                fail(
                    "E_REQUEST_ABORTED",
                    "The refresh request was not committed before process termination.",
                    context={
                        "request_id": request_id,
                        "status": existing["status"],
                        "response": json.loads(existing["response_json"] or "{}"),
                    },
                    status_code=409,
                )

            with self.db.read_transaction() as connection:
                graph_row = load_graph_row(connection)
                if graph_row is None:
                    fail("E_NOT_FOUND", "No view graph is committed.", status_code=404)
                graph = build_graph(graph_row["definitions"], table_names(connection))
                states = {row["view"]: row for row in load_all_view_states(connection)}
                selected = self._select_refresh_views(
                    graph,
                    states,
                    mode=mode,
                    requested=requested_views,
                    include_downstream=include_downstream,
                )
                for name in failed_views:
                    if name not in graph.views:
                        fail(
                            "E_NOT_FOUND",
                            "fail_views contains an unknown view.",
                            context={"view": name},
                            status_code=404,
                        )

            if not selected:
                response = {
                    "request_id": request_id,
                    "status": "COMPLETED",
                    "no_work": True,
                    "graph_version": graph_row["graph_version"],
                    "commit_seq": latest_commit_seq_from_db(self.db),
                    "views": [],
                    "summary": {"fresh": 0, "partial": 0, "selected": 0},
                }
                with self.db.write_transaction() as connection:
                    now = utc_now()
                    connection.execute(
                        "INSERT INTO refresh_requests(request_id, digest, request_json, "
                        "status, response_json, created_at, updated_at) "
                        "VALUES (?, ?, ?, 'COMPLETED', ?, ?, ?) "
                        "ON CONFLICT(request_id) DO UPDATE SET status = excluded.status, "
                        "response_json = excluded.response_json, updated_at = excluded.updated_at",
                        (
                            request_id,
                            request_digest,
                            canonical_json(normalized),
                            canonical_json(response),
                            now,
                            now,
                        ),
                    )
                return response

            states_before = states
            intent_id = f"refresh:{request_id}"
            with self.db.write_transaction() as connection:
                now = utc_now()
                connection.execute(
                    "INSERT INTO refresh_requests(request_id, digest, request_json, status, "
                    "response_json, created_at, updated_at) VALUES (?, ?, ?, 'IN_PROGRESS', "
                    "NULL, ?, ?)",
                    (request_id, request_digest, canonical_json(normalized), now, now),
                )
                connection.execute(
                    "INSERT INTO intents(intent_id, kind, status, payload_digest, "
                    "details_json, created_at, updated_at, commit_seq) "
                    "VALUES (?, 'REFRESH', 'RUNNING', ?, ?, ?, ?, NULL)",
                    (
                        intent_id,
                        request_digest,
                        canonical_json(
                            {
                                "request_id": request_id,
                                "graph_version": graph_row["graph_version"],
                                "selected_views": selected,
                                "mode": mode,
                            }
                        ),
                        now,
                        now,
                    ),
                )
                accepted_commit = insert_commit(
                    connection,
                    "REFRESH_ACCEPTED",
                    {"request_id": request_id, "selected_views": selected},
                )
                for view_name in selected:
                    state = states_before.get(view_name)
                    if state is None:
                        fail(
                            "E_NOT_FOUND",
                            "Selected view has no persisted state.",
                            context={"view": view_name},
                            status_code=404,
                        )
                    assert_transition(state["state"], REFRESHING, view_name)
                    update_view_state(
                        connection,
                        view_name=view_name,
                        graph_version=graph_row["graph_version"],
                        state=REFRESHING,
                        materialized_version=state["materialized_version"],
                        gaps=state["gaps"],
                        commit_seq=accepted_commit,
                        reason=f"Refresh request {request_id} acquired the single writer.",
                        old_state=state["state"],
                    )
            self._maybe_crash("after_refresh_intent")
            if delay_ms:
                time.sleep(delay_ms / 1000.0)

            try:
                computed = self._compute_refresh(
                    graph=graph,
                    graph_version=graph_row["graph_version"],
                    selected=selected,
                    states_before=states_before,
                    fail_views=set(failed_views),
                )
                with self.db.write_transaction() as connection:
                    commit_seq = insert_commit(
                        connection,
                        "REFRESH",
                        {
                            "request_id": request_id,
                            "graph_version": graph_row["graph_version"],
                            "selected_views": selected,
                        },
                    )
                    view_results: list[dict[str, Any]] = []
                    for name in selected:
                        result = computed[name]
                        state_now = load_view_state(connection, name)
                        if state_now is None:
                            fail("E_NOT_FOUND", "View state disappeared during refresh.", context={"view": name}, status_code=404)
                        if result["state"] == FRESH:
                            self._publish_view_version(
                                connection,
                                name=name,
                                version=result["new_version"],
                                graph_version=graph_row["graph_version"],
                                commit_seq=commit_seq,
                                plan=result["plan"],
                            )
                            update_view_state(
                                connection,
                                view_name=name,
                                graph_version=graph_row["graph_version"],
                                state=FRESH,
                                materialized_version=result["new_version"],
                                gaps=[],
                                commit_seq=commit_seq,
                                reason="Refresh completed and published atomically.",
                                old_state=state_now["state"],
                            )
                            action = "REBUILT"
                        else:
                            update_view_state(
                                connection,
                                view_name=name,
                                graph_version=graph_row["graph_version"],
                                state=PARTIAL,
                                materialized_version=state_now["materialized_version"],
                                gaps=result["gaps"],
                                commit_seq=commit_seq,
                                reason="Refresh could not obtain a complete dependency picture.",
                                old_state=state_now["state"],
                            )
                            action = "PARTIAL"
                        insert_invalidation_event(
                            connection,
                            commit_seq=commit_seq,
                            trigger_type="REFRESH",
                            trigger_id=request_id,
                            view_name=name,
                            depth=0,
                            action=action,
                            affected_groups=result.get("affected_groups", []),
                            recomputed_groups=result.get("recomputed_groups"),
                            reason=result["reason"],
                        )
                        view_results.append(
                            self._public_refresh_result(result, states_before[name])
                        )
                    response = {
                        "request_id": request_id,
                        "status": "COMPLETED",
                        "no_work": False,
                        "graph_version": graph_row["graph_version"],
                        "commit_seq": commit_seq,
                        "views": view_results,
                        "summary": {
                            "selected": len(view_results),
                            "fresh": sum(1 for item in view_results if item["state"] == FRESH),
                            "partial": sum(1 for item in view_results if item["state"] == PARTIAL),
                        },
                    }
                    connection.execute(
                        "UPDATE intents SET status = 'COMMITTED', commit_seq = ?, "
                        "updated_at = ? WHERE intent_id = ?",
                        (commit_seq, utc_now(), intent_id),
                    )
                    connection.execute(
                        "UPDATE refresh_requests SET status = 'COMPLETED', "
                        "response_json = ?, updated_at = ? WHERE request_id = ?",
                        (canonical_json(response), utc_now(), request_id),
                    )
                return response
            except BaseException as exc:
                self._mark_refresh_failed(request_id, intent_id, selected, exc)
                if isinstance(exc, WorkbenchError):
                    raise
                fail(
                    "E_REFRESH_FAILED",
                    "Refresh failed; all selected views were atomically marked PARTIAL.",
                    context={"request_id": request_id, "detail": str(exc)},
                    status_code=500,
                )

    def _select_refresh_views(
        self,
        graph: ViewGraph,
        states: dict[str, dict[str, Any]],
        *,
        mode: str,
        requested: list[str],
        include_downstream: bool,
    ) -> list[str]:
        if mode == "all_stale":
            roots = {
                name
                for name, state in states.items()
                if state["state"] != FRESH and name in graph.views
            }
        elif mode == "all":
            roots = set(graph.views)
        else:
            unknown = [name for name in requested if name not in graph.views]
            if unknown:
                fail(
                    "E_NOT_FOUND",
                    "Refresh references an unknown view.",
                    context={"views": unknown},
                    status_code=404,
                )
            roots = set(requested)
        chosen = graph.downstream(roots) if include_downstream else set(roots)
        return [name for name in graph.order if name in chosen]

    def _compute_refresh(
        self,
        *,
        graph: ViewGraph,
        graph_version: int,
        selected: list[str],
        states_before: dict[str, dict[str, Any]],
        fail_views: set[str],
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        computed_rows: dict[str, list[dict[str, Any]]] = {}
        table_cache: dict[str, list[dict[str, Any]]] = {}

        with self.db.read_transaction() as connection:
            def resolve_ref(kind: str, name: str) -> list[dict[str, Any]]:
                if kind == "table":
                    if name not in table_cache:
                        table_cache[name] = [
                            row["data"] for row in load_base_rows(connection, name)
                        ]
                    return [dict(row) for row in table_cache[name]]
                if name in computed_rows:
                    return [dict(row) for row in computed_rows[name]]
                state = load_view_state(connection, name)
                if state is None or state["state"] != FRESH:
                    fail(
                        "E_DEPENDENCY_NOT_READY",
                        "A view dependency has no complete materialization.",
                        context={"view": name, "state": state["state"] if state else "MISSING"},
                        status_code=409,
                    )
                _, rows = load_latest_view_rows(connection, name)
                return rows

            for view_name in selected:
                definition = graph.views[view_name]
                gaps: list[dict[str, Any]] = []
                for dependency in sorted(graph.dependencies[view_name]):
                    if dependency in results:
                        if results[dependency]["state"] != FRESH:
                            gaps.append(
                                {
                                    "code": "DEPENDENCY_NOT_READY",
                                    "view": dependency,
                                    "state": results[dependency]["state"],
                                }
                            )
                    else:
                        dependency_state = states_before.get(dependency)
                        if dependency_state is None or dependency_state["state"] != FRESH:
                            gaps.append(
                                {
                                    "code": "DEPENDENCY_NOT_READY",
                                    "view": dependency,
                                    "state": dependency_state["state"] if dependency_state else "MISSING",
                                    "detail": "not selected in this refresh",
                                }
                            )
                if view_name in fail_views:
                    gaps.append(
                        {
                            "code": "SIMULATED_REFRESH_FAILURE",
                            "view": view_name,
                            "detail": "fail_views requested a diagnostic failure.",
                        }
                    )
                if gaps:
                    results[view_name] = self._partial_result(
                        view_name,
                        gaps,
                        "Dependency or refresh failure blocked a complete materialization.",
                    )
                    continue
                try:
                    output_rows = execute_view(definition, resolve_ref)
                    old_state = load_view_state(connection, view_name)
                    old_version = load_view_version(
                        connection,
                        view_name,
                        old_state["materialized_version"] if old_state else None,
                    )
                    old_rows: list[dict[str, Any]] | None = None
                    if old_version is not None:
                        old_rows = flatten_groups(
                            load_view_groups(connection, view_name, old_version["version"])
                        )
                    plan = plan_materialization(
                        definition,
                        output_rows,
                        old_rows=old_rows,
                        old_graph_version=old_version["graph_version"] if old_version else None,
                        graph_version=graph_version,
                    )
                    new_version = max_view_version(connection, view_name) + 1
                    results[view_name] = {
                        "view": view_name,
                        "state": FRESH,
                        "plan": plan,
                        "new_version": new_version,
                        "previous_version": old_version["version"] if old_version else None,
                        "affected_groups": plan["affected_groups"],
                        "recomputed_groups": plan["recomputed_groups"],
                        "preserved_groups": plan["preserved_groups"],
                        "mode": plan["mode"],
                        "reason": plan["reason"],
                        "aggregate_functions": plan["aggregate_functions"],
                        "non_incremental_functions": plan["non_incremental_functions"],
                        "gaps": [],
                    }
                    computed_rows[view_name] = output_rows
                except Exception as exc:
                    results[view_name] = self._partial_result(
                        view_name,
                        [
                            {
                                "code": "VIEW_COMPUTE_FAILED",
                                "view": view_name,
                                "detail": str(exc),
                            }
                        ],
                        "The view execution failed; no new materialization was published.",
                    )
        return results

    def _partial_result(
        self, view_name: str, gaps: list[dict[str, Any]], reason: str
    ) -> dict[str, Any]:
        return {
            "view": view_name,
            "state": PARTIAL,
            "plan": None,
            "new_version": None,
            "previous_version": None,
            "affected_groups": [],
            "recomputed_groups": 0,
            "preserved_groups": 0,
            "mode": "not_recomputed",
            "reason": reason,
            "aggregate_functions": [],
            "non_incremental_functions": [],
            "gaps": gaps,
        }

    def _publish_view_version(
        self,
        connection: sqlite3.Connection,
        *,
        name: str,
        version: int,
        graph_version: int,
        commit_seq: int,
        plan: dict[str, Any],
    ) -> None:
        groups = plan["groups"]
        row_count = sum(len(rows) for rows in groups.values())
        version_digest = digest_json(
            {
                "view": name,
                "version": version,
                "groups": [
                    {"key": key, "rows": sorted(rows, key=canonical_json)}
                    for key, rows in sorted(groups.items())
                ],
            }
        )
        connection.execute(
            "INSERT INTO view_versions(view_name, version, commit_seq, graph_version, mode, "
            "affected_groups_json, recomputed_groups, preserved_groups, reason, digest, "
            "row_count, group_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                name,
                version,
                commit_seq,
                graph_version,
                plan["mode"],
                canonical_json(plan["affected_groups"]),
                plan["recomputed_groups"],
                plan["preserved_groups"],
                plan["reason"],
                version_digest,
                row_count,
                len(groups),
                utc_now(),
            ),
        )
        for group_key, rows in sorted(groups.items()):
            sorted_rows = sorted(rows, key=canonical_json)
            group_digest = digest_json({"rows": sorted_rows})
            connection.execute(
                "INSERT INTO view_group_index(view_name, version, group_key_json, row_count, "
                "digest) VALUES (?, ?, ?, ?, ?)",
                (name, version, group_key, len(sorted_rows), group_digest),
            )
            for index, row in enumerate(sorted_rows):
                connection.execute(
                    "INSERT INTO view_rows(view_name, version, group_key_json, row_index, "
                    "row_json) VALUES (?, ?, ?, ?, ?)",
                    (name, version, group_key, index, canonical_json(row)),
                )

    def _public_refresh_result(
        self, result: dict[str, Any], prior_state: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "view": result["view"],
            "state": result["state"],
            "previous_state": prior_state["state"],
            "version": result.get("new_version"),
            "previous_version": (
                result.get("previous_version")
                if result.get("previous_version") is not None
                else prior_state["materialized_version"]
            ),
            "mode": result.get("mode", "not_recomputed"),
            "affected_groups": result.get("affected_groups", []),
            "recomputed_groups": result.get("recomputed_groups", 0),
            "preserved_groups": result.get("preserved_groups", 0),
            "aggregate_functions": result.get("aggregate_functions", []),
            "non_incremental_functions": result.get("non_incremental_functions", []),
            "reason": result.get("reason", ""),
            "gaps": result.get("gaps", []),
        }

    def _mark_refresh_failed(
        self,
        request_id: str,
        intent_id: str,
        selected: list[str],
        exception: BaseException,
    ) -> None:
        try:
            with self.db.write_transaction() as connection:
                commit_seq = insert_commit(
                    connection,
                    "REFRESH_ABORTED",
                    {"request_id": request_id, "selected_views": selected, "error": str(exception)},
                )
                for view_name in selected:
                    state = load_view_state(connection, view_name)
                    if state is not None and state["state"] == REFRESHING:
                        gaps = [{"code": "REFRESH_ABORTED", "request_id": request_id}]
                        update_view_state(
                            connection,
                            view_name=view_name,
                            graph_version=state["graph_version"],
                            state=PARTIAL,
                            materialized_version=state["materialized_version"],
                            gaps=gaps,
                            commit_seq=commit_seq,
                            reason="Refresh raised an exception before publishing view versions.",
                            old_state=REFRESHING,
                        )
                        insert_invalidation_event(
                            connection,
                            commit_seq=commit_seq,
                            trigger_type="REFRESH",
                            trigger_id=request_id,
                            view_name=view_name,
                            depth=0,
                            action="PARTIAL",
                            affected_groups=[],
                            recomputed_groups=0,
                            reason="Refresh aborted before atomic publication.",
                        )
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', commit_seq = ?, updated_at = ? "
                    "WHERE intent_id = ? AND status = 'RUNNING'",
                    (commit_seq, utc_now(), intent_id),
                )
                error_response = {
                    "ok": False,
                    "error": {
                        "code": "E_REFRESH_FAILED",
                        "message": "Refresh aborted before publication.",
                        "context": {"request_id": request_id, "detail": str(exception)},
                    },
                }
                connection.execute(
                    "UPDATE refresh_requests SET status = 'FAILED', response_json = ?, "
                    "updated_at = ? WHERE request_id = ?",
                    (canonical_json(error_response), utc_now(), request_id),
                )
        except Exception:
            # Recovery on the next start will find the RUNNING intent and REFRESHING
            # states. Never hide the original exception behind cleanup failure.
            pass

    # ------------------------------------------------------------------
    # Query, freshness, and diagnostics
    # ------------------------------------------------------------------
    def list_views(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as connection:
            graph_row = load_graph_row(connection)
            if graph_row is None:
                return []
            graph = build_graph(graph_row["definitions"], table_names(connection))
            states = {row["view"]: row for row in load_all_view_states(connection)}
            output = []
            for name in graph.order:
                state = states.get(name)
                if state is None:
                    continue
                version = load_view_version(connection, name, state["materialized_version"])
                output.append(
                    {
                        **state,
                        "view": name,
                        "definition": graph.views[name],
                        "view_version": version,
                        "fresh": state["state"] == FRESH,
                        "graph_version": graph_row["graph_version"],
                    }
                )
            return output

    def get_freshness(self, view_name: str) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            state = load_view_state(connection, view_name)
            if state is None:
                missing_view(view_name)
            graph_row = load_graph_row(connection)
            version = load_view_version(connection, view_name, state["materialized_version"])
            return {
                **state,
                "fresh": state["state"] == FRESH,
                "graph_definition_version": graph_row["graph_version"] if graph_row else None,
                "view_version": version,
            }

    def query_view(
        self,
        view_name: str,
        *,
        version: int | None = None,
        delay_ms: int = 0,
    ) -> dict[str, Any]:
        if delay_ms < 0 or delay_ms > 10_000:
            fail("E_INVALID_REQUEST", "delay_ms must be between 0 and 10000.", context={"delay_ms": delay_ms})
        with self.db.read_transaction() as connection:
            state = load_view_state(connection, view_name)
            if state is None:
                missing_view(view_name)
            if version is None:
                if state["materialized_version"] is None:
                    fail(
                        "E_NOT_MATERIALIZED",
                        "The view has no complete materialized version yet.",
                        context={"view": view_name, "state": state["state"]},
                        status_code=409,
                    )
                bound_version = int(state["materialized_version"])
            else:
                bound_version = int(version)
            materialization = load_view_version(connection, view_name, bound_version)
            if materialization is None:
                fail(
                    "E_VERSION_RETIRED",
                    "The requested materialized-view version is retired or was never committed.",
                    context={
                        "view": view_name,
                        "requested_version": bound_version,
                        "current_version": state["materialized_version"],
                    },
                    status_code=410,
                )
            if delay_ms:
                time.sleep(delay_ms / 1000.0)
            groups = load_view_groups(connection, view_name, bound_version)
            rows = flatten_groups(groups)
            return {
                "view": view_name,
                "requested_version": version,
                "bound_version": bound_version,
                "graph_version": materialization["graph_version"],
                "commit_seq": materialization["commit_seq"],
                "row_count": materialization["row_count"],
                "group_count": materialization["group_count"],
                "state": state["state"],
                "fresh": state["state"] == FRESH,
                "freshness": state,
                "materialization": materialization,
                "groups": groups,
                "rows": rows,
            }

    def diagnostics(self, limit: int = 100) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        with self.db.read_transaction() as connection:
            graph_row = load_graph_row(connection)
            if graph_row is None:
                return {"graph": None, "views": [], "recent_events": []}
            graph = build_graph(graph_row["definitions"], table_names(connection))
            state_rows = load_all_view_states(connection)
            views = []
            for state in state_rows:
                version = load_view_version(connection, state["view"], state["materialized_version"])
                views.append({**state, "fresh": state["state"] == FRESH, "view_version": version})
            event_rows = connection.execute(
                "SELECT * FROM invalidation_events ORDER BY commit_seq DESC, event_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            events = [
                {
                    "event_id": int(row["event_id"]),
                    "commit_seq": int(row["commit_seq"]),
                    "trigger_type": row["trigger_type"],
                    "trigger_id": row["trigger_id"],
                    "view": row["view_name"],
                    "depth": int(row["depth"]),
                    "action": row["action"],
                    "affected_groups": json.loads(row["affected_groups_json"]),
                    "recomputed_groups": row["recomputed_groups"],
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                }
                for row in event_rows
            ]
            latest_trigger = None
            if events:
                latest_trigger = {
                    "trigger_type": events[0]["trigger_type"],
                    "trigger_id": events[0]["trigger_id"],
                    "commit_seq": events[0]["commit_seq"],
                    "events": [
                        event
                        for event in events
                        if event["commit_seq"] == events[0]["commit_seq"]
                    ],
                }
            return {
                "graph": {
                    "graph_version": graph_row["graph_version"],
                    **graph_payload(graph, graph_version=graph_row["graph_version"]),
                },
                "views": views,
                "recent_events": events,
                "latest_propagation": latest_trigger,
                "commit_seq": latest_commit_seq(connection),
            }

    # ------------------------------------------------------------------
    # Recovery and admin state-machine surface
    # ------------------------------------------------------------------
    def recovery_report(self) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT 20"
            ).fetchall()
            reports = [
                {
                    "report_id": int(row["report_id"]),
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "status": row["status"],
                    "facts": json.loads(row["facts_json"]),
                }
                for row in rows
            ]
            return {"latest": reports[0] if reports else None, "reports": reports}

    def _recover_locked(self) -> dict[str, Any]:
        started_at = utc_now()
        with self.db.write_transaction() as connection:
            last_committed = latest_commit_seq(connection)
            complete_row = connection.execute(
                "SELECT MAX(commit_seq) AS value FROM commits "
                "WHERE kind NOT IN ('REFRESH_ACCEPTED', 'RECOVERY')"
            ).fetchone()
            last_complete = (
                int(complete_row["value"])
                if complete_row and complete_row["value"] is not None
                else 0
            )
            intents = connection.execute(
                "SELECT * FROM intents WHERE status = 'RUNNING' ORDER BY created_at"
            ).fetchall()
            facts: list[dict[str, Any]] = []
            partial_views: list[str] = []
            if intents:
                recovery_commit = insert_commit(
                    connection, "RECOVERY", {"running_intents": [row["intent_id"] for row in intents]}
                )
                for intent in intents:
                    details = json.loads(intent["details_json"])
                    fact: dict[str, Any] = {
                        "intent_id": intent["intent_id"],
                        "kind": intent["kind"],
                        "persisted_status": intent["status"],
                        "last_complete_commit_seq": last_complete,
                        "last_committed_seq_before_recovery": last_committed,
                    }
                    if intent["kind"] == "REFRESH":
                        request_id = details.get("request_id")
                        selected = details.get("selected_views", [])
                        marked: list[dict[str, Any]] = []
                        for view_name in selected:
                            state = load_view_state(connection, view_name)
                            if state is not None and state["state"] == REFRESHING:
                                gaps = [
                                    {
                                        "code": "INCOMPLETE_REFRESH",
                                        "intent_id": intent["intent_id"],
                                        "request_id": request_id,
                                    }
                                ]
                                update_view_state(
                                    connection,
                                    view_name=view_name,
                                    graph_version=state["graph_version"],
                                    state=PARTIAL,
                                    materialized_version=state["materialized_version"],
                                    gaps=gaps,
                                    commit_seq=recovery_commit,
                                    reason="Process terminated before atomic refresh publication.",
                                    old_state=REFRESHING,
                                )
                                insert_invalidation_event(
                                    connection,
                                    commit_seq=recovery_commit,
                                    trigger_type="RECOVERY",
                                    trigger_id=intent["intent_id"],
                                    view_name=view_name,
                                    depth=0,
                                    action="RECOVERED_PARTIAL",
                                    affected_groups=[],
                                    recomputed_groups=0,
                                    reason="Only the persisted REFRESHING intent was found at restart.",
                                )
                                marked.append(
                                    {
                                        "view": view_name,
                                        "state": PARTIAL,
                                        "materialized_version": state["materialized_version"],
                                    }
                                )
                                partial_views.append(view_name)
                        fact["views_marked_partial"] = marked
                        if request_id:
                            response = {
                                "ok": False,
                                "recovered": True,
                                "error": {
                                    "code": "E_INCOMPLETE_REFRESH",
                                    "message": "Refresh was interrupted; persisted views are PARTIAL.",
                                    "context": {
                                        "request_id": request_id,
                                        "intent_id": intent["intent_id"],
                                    },
                                },
                            }
                            connection.execute(
                                "UPDATE refresh_requests SET status = 'FAILED', response_json = ?, "
                                "updated_at = ? WHERE request_id = ?",
                                (canonical_json(response), utc_now(), request_id),
                            )
                            fact["request_id"] = request_id
                    else:
                        fact["domain_change"] = "not_committed"
                    connection.execute(
                        "UPDATE intents SET status = 'ABORTED', commit_seq = ?, updated_at = ? "
                        "WHERE intent_id = ?",
                        (recovery_commit, utc_now(), intent["intent_id"]),
                    )
                    facts.append(fact)
                status = "RECOVERED"
                facts_payload = {
                    "mode": "RECOVERY_ON_RESTART",
                    "last_complete_commit_seq": last_complete,
                    "last_committed_seq_before_recovery": last_committed,
                    "unfinished_intents": facts,
                    "views_marked_partial": sorted(set(partial_views)),
                    "persisted_fact_only": True,
                    "note": (
                        "A forced termination does not produce a graceful exit report; "
                        "this report was written after restart from persisted intent rows."
                    ),
                }
            else:
                status = "CLEAN"
                facts_payload = {
                    "mode": "CLEAN_START",
                    "last_complete_commit_seq": last_complete,
                    "last_committed_seq_before_recovery": last_committed,
                    "unfinished_intents": [],
                    "views_marked_partial": [],
                    "persisted_fact_only": True,
                    "note": "No RUNNING intent was present in the persisted database.",
                }
            finished_at = utc_now()
            cursor = connection.execute(
                "INSERT INTO recovery_reports(started_at, finished_at, status, facts_json) "
                "VALUES (?, ?, ?, ?)",
                (started_at, finished_at, status, canonical_json(facts_payload)),
            )
            report_id = int(cursor.lastrowid)
            report = {
                "report_id": report_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "facts": facts_payload,
            }
        return report

    def transition_state(
        self,
        view_name: str,
        target_state: str,
        *,
        reason: str = "admin transition",
        gaps: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if target_state not in STATES:
            fail(
                "E_INVALID_REQUEST",
                "Unknown target state.",
                context={"state": target_state, "supported": sorted(STATES)},
            )
        gaps = gaps or []
        with self.writer_lock:
            with self.db.write_transaction() as connection:
                state = load_view_state(connection, view_name)
                if state is None:
                    missing_view(view_name)
                assert_transition(state["state"], target_state, view_name)
                commit_seq = insert_commit(
                    connection,
                    "ADMIN_STATE_TRANSITION",
                    {"view": view_name, "from": state["state"], "to": target_state},
                )
                update_view_state(
                    connection,
                    view_name=view_name,
                    graph_version=state["graph_version"],
                    state=target_state,
                    materialized_version=state["materialized_version"],
                    gaps=gaps if target_state in {PARTIAL, INVALID} else state["gaps"],
                    commit_seq=commit_seq,
                    reason=reason,
                    old_state=state["state"],
                )
            return self.get_freshness(view_name)

    def get_refresh_request(self, request_id: str) -> dict[str, Any]:
        request = self._load_request(request_id)
        if request is None:
            fail(
                "E_NOT_FOUND",
                "Refresh request does not exist.",
                context={"request_id": request_id},
                status_code=404,
            )
        return {
            **request,
            "request": json.loads(request["request_json"]),
            "response": json.loads(request["response_json"]) if request["response_json"] else None,
        }

    def _load_request(self, request_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM refresh_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                return None
            return {
                "request_id": row["request_id"],
                "digest": row["digest"],
                "request_json": row["request_json"],
                "status": row["status"],
                "response_json": row["response_json"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }

    def _maybe_crash(self, point: str) -> None:
        if self.crash_point == point:
            os._exit(97)


def latest_commit_seq_from_db(db: Database) -> int:
    with db.read_transaction() as connection:
        return latest_commit_seq(connection)
