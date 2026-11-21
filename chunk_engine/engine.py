from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

from .codec import decode_block_slice
from .errors import (
    BusinessError,
    E_BATCH_FAILED,
    E_BLOCK_CORRUPT,
    E_INVALID_REQUEST,
    E_MEMORY_BUDGET_EXCEEDED,
    E_PAGE_CONSUMED,
    E_SNAPSHOT_INVALID,
)
from .persistence import Persistence
from .planning import ExecutionPlan, plan_query
from .snapshot import Snapshot, SnapshotManager, load_snapshot
from .utils import (
    atomic_text_writer,
    atomic_write_json,
    canonical_json_bytes,
    ceil_div,
    exact_value_key,
    file_sha256,
    result_value_for_json,
    sha256_hex,
    strict_json_loads,
)


@dataclass(frozen=True)
class EngineConfig:
    total_memory_budget_bytes: int = 8 * 1024 * 1024
    single_task_budget_bytes: int = 64 * 1024
    max_parallel_blocks: int = 8
    max_keys: int = 20_000
    max_page_size: int = 500
    default_page_size: int = 100
    float_decimal_places: int = 6
    float_abs_tolerance: float = 1e-9
    float_rel_tolerance: float = 1e-12


BLOCK_RESULT_FORMAT = "chunk-block-result-v1"
QUERY_RESULT_FORMAT = "chunk-query-result-v1"
PAGE_FORMAT = "chunk-page-v1"


class Engine:
    """Single-writer materializer with concurrent page readers."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        config: EngineConfig | None = None,
        snapshot_manager: SnapshotManager | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or EngineConfig()
        self.snapshots = snapshot_manager or SnapshotManager()
        self.db = Persistence(db_path or self.data_dir / "engine.sqlite3")
        self._materialize_lock = threading.RLock()
        self._recovery_lock = threading.Lock()
        self.last_recovery_report: dict[str, Any] | None = None

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"

    def load_snapshot(self, path: str | Path) -> Snapshot:
        return self.snapshots.load(path)

    def active_snapshot_info(self) -> dict[str, Any]:
        return self.snapshots.active().info()

    def recover(self) -> dict[str, Any]:
        with self._recovery_lock:
            with self._materialize_lock:
                report = self.db.recover_incomplete()
            self.last_recovery_report = report
            return report

    def _effective_page_size(self, requested: Any, client_max: Any) -> int:
        if requested is None:
            requested = min(self.config.default_page_size, self.config.max_page_size)
        if client_max is None:
            client_max = self.config.max_page_size
        if (
            isinstance(requested, bool)
            or not isinstance(requested, int)
            or requested <= 0
            or isinstance(client_max, bool)
            or not isinstance(client_max, int)
            or client_max <= 0
        ):
            raise BusinessError(
                E_INVALID_REQUEST,
                "page_size and max_page_size must be positive integers",
                {"page_size": requested, "max_page_size": client_max},
            )
        return min(requested, client_max, self.config.max_page_size)

    def create_batch(
        self,
        raw_keys: Any,
        raw_columns: Any,
        *,
        page_size: Any = None,
        client_max_page_size: Any = None,
        execution_mode: str = "auto",
        batch_id: str | None = None,
        consumer_id: str | None = None,
        crash_after_committed_blocks: int | None = None,
        crash_callback: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        snapshot = self.snapshots.active()
        effective_page_size = self._effective_page_size(page_size, client_max_page_size)
        if crash_after_committed_blocks is not None and (
            isinstance(crash_after_committed_blocks, bool)
            or not isinstance(crash_after_committed_blocks, int)
            or crash_after_committed_blocks <= 0
        ):
            raise BusinessError(E_INVALID_REQUEST, "crash_after_committed_blocks must be positive")
        generated_batch_id, generated_handle = Persistence.new_identifiers()
        actual_batch_id = batch_id or generated_batch_id
        if not isinstance(actual_batch_id, str) or not actual_batch_id or len(actual_batch_id) > 128:
            raise BusinessError(E_INVALID_REQUEST, "batch_id must be a non-empty string of at most 128 characters")
        result_handle = generated_handle
        initial_consumer = consumer_id or f"submit:{actual_batch_id}"

        with self._materialize_lock:
            remaining = self.db.remaining_memory_budget(self.config.total_memory_budget_bytes)
            plan = plan_query(
                snapshot,
                raw_keys,
                raw_columns,
                max_keys=self.config.max_keys,
                total_memory_budget_bytes=self.config.total_memory_budget_bytes,
                remaining_memory_budget_bytes=remaining,
                single_task_budget_bytes=self.config.single_task_budget_bytes,
                max_parallel_blocks=self.config.max_parallel_blocks,
                execution_mode=execution_mode,
            )
            task_rows = [
                (
                    block_no,
                    snapshot.primary_column.blocks[block_no].key_min,
                    snapshot.primary_column.blocks[block_no].key_max,
                )
                for block_no in plan.hit_blocks
            ]
            self.db.insert_batch(
                batch_id=actual_batch_id,
                result_handle=result_handle,
                snapshot_version=snapshot.version,
                snapshot_root=str(snapshot.root),
                snapshot_digest=snapshot.digest,
                keys=plan.keys,
                columns=plan.columns,
                page_size=effective_page_size,
                max_page_size=min(self.config.max_page_size, client_max_page_size or self.config.max_page_size),
                requested_mode=execution_mode,
                plan=plan.to_dict(),
                task_rows=task_rows,
                reservation_bytes=plan.peak_memory_estimate_bytes,
            )
            try:
                self._materialize_batch(
                    actual_batch_id,
                    snapshot=snapshot,
                    plan=plan,
                    crash_after_committed_blocks=crash_after_committed_blocks,
                    crash_callback=crash_callback,
                )
            except BusinessError as error:
                self.db.fail_batch(actual_batch_id, error)
                raise
            batch = self.db.get_batch(actual_batch_id)
            page_payload = self._build_page_payload(batch, 0)
            consumed = self.db.claim_page(actual_batch_id, 0, initial_consumer)
            page_payload.update(
                {
                    "state": "consumed",
                    "consumer_id": initial_consumer,
                    "consumed_at": consumed["consumed_at"],
                }
            )
            status = self.batch_status(actual_batch_id)
            return {
                "ok": True,
                "batch": status,
                "result_handle": result_handle,
                "page": page_payload,
                "page_size_requested": page_size if page_size is not None else None,
                "page_size_effective": effective_page_size,
            }

    def _verify_batch_snapshot(self, batch: dict[str, Any]) -> Snapshot:
        snapshot = load_snapshot(batch["snapshot_root"])
        if snapshot.version != batch["snapshot_version"] or snapshot.digest != batch["snapshot_digest"]:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "persisted batch snapshot identity does not match its immutable manifest",
                {
                    "batch_id": batch["batch_id"],
                    "expected_version": batch["snapshot_version"],
                    "actual_version": snapshot.version,
                    "expected_digest": batch["snapshot_digest"],
                    "actual_digest": snapshot.digest,
                },
            )
        return snapshot

    def _materialize_batch(
        self,
        batch_id: str,
        *,
        snapshot: Snapshot,
        plan: ExecutionPlan,
        crash_after_committed_blocks: int | None = None,
        crash_callback: Callable[[], None] | None = None,
    ) -> None:
        batch = self.db.get_batch(batch_id)
        if batch["state"] == "materialized":
            return
        if batch["state"] == "failed":
            raise self._restore_batch_error(batch)
        if not self.db.has_reserved_budget(batch_id):
            self.db.reserve_budget_for_existing_batch(
                batch_id=batch_id,
                bytes_required=plan.peak_memory_estimate_bytes,
                total_budget=self.config.total_memory_budget_bytes,
                selected_path=plan.selected_path,
                reason="re-serving the persisted plan after recovery before any decode",
            )
        tasks_before = self.db.get_tasks(batch_id)
        reused_blocks = sum(1 for task in tasks_before if task["state"] == "committed" and task["reusable"])
        pending = [task["block_no"] for task in tasks_before if task["state"] != "committed"]
        group_size = plan.parallel_blocks if plan.selected_path == "batch" else 1
        group_size = max(1, group_size)
        crashed = False
        committed_this_run = 0

        for index in range(0, len(pending), group_size):
            group = pending[index : index + group_size]
            self.db.begin_task_group(batch_id, group)
            if plan.selected_path == "batch" and len(group) > 1:
                with ThreadPoolExecutor(max_workers=len(group), thread_name_prefix="chunk-decode") as executor:
                    futures = [
                        executor.submit(self._decode_block_result, snapshot, plan, batch_id, block_no)
                        for block_no in group
                    ]
                    decoded = [future.result() for future in futures]
            else:
                decoded = [self._decode_block_result(snapshot, plan, batch_id, block_no) for block_no in group]
            committed = []
            for block_result in decoded:
                artifact_path = self._block_artifact_path(batch_id, int(block_result["block_no"]))
                atomic_write_json(artifact_path, block_result)
                committed.append(
                    {
                        "block_no": int(block_result["block_no"]),
                        "artifact_path": str(artifact_path),
                        "artifact_checksum": file_sha256(artifact_path),
                    }
                )
            self.db.commit_task_group(batch_id, committed)
            committed_this_run += len(committed)
            if (
                crash_after_committed_blocks is not None
                and committed_this_run >= crash_after_committed_blocks
            ):
                crashed = True
                if crash_callback is not None:
                    crash_callback()
                break
        if crashed:
            return
        remaining_tasks = self.db.get_pending_blocks(batch_id)
        if remaining_tasks:
            raise BusinessError(
                E_BLOCK_CORRUPT,
                "materialization ended with pending block tasks",
                {"batch_id": batch_id, "pending_blocks": remaining_tasks},
            )
        self.db.begin_finalize(batch_id)
        summary = self._assemble_and_finalize(
            batch_id=batch_id,
            snapshot=snapshot,
            plan=plan,
            reused_blocks=reused_blocks,
            decoded_this_run=len(pending),
        )
        batch = self.db.get_batch(batch_id)
        batch["reuse_json"] = json.dumps(summary, ensure_ascii=False, sort_keys=True)

    def _restore_batch_error(self, batch: dict[str, Any]) -> BusinessError:
        raw = batch.get("error_json")
        if raw:
            try:
                payload = json.loads(raw)
                error = payload.get("error", {})
                return BusinessError(
                    str(error.get("code", E_BATCH_FAILED)),
                    str(error.get("message", "batch failed")),
                    dict(error.get("context") or {}),
                )
            except Exception:
                pass
        return BusinessError(E_BATCH_FAILED, "batch failed", {"batch_id": batch.get("batch_id")})

    def _block_artifact_path(self, batch_id: str, block_no: int) -> Path:
        return self.artifacts_dir / batch_id / "blocks" / f"block_{block_no:08d}.json"

    def _result_meta_path(self, batch_id: str) -> Path:
        return self.artifacts_dir / batch_id / "result_meta.json"

    def _result_rows_path(self, batch_id: str) -> Path:
        return self.artifacts_dir / batch_id / "result_rows.jsonl"

    def _page_path(self, batch_id: str, page_no: int) -> Path:
        return self.artifacts_dir / batch_id / "pages" / f"page_{page_no:08d}.json"

    def _chunk_rows_for(self, plan: ExecutionPlan, block_no: int, column: str, row_count: int, estimate: int) -> int | None:
        if estimate <= plan.effective_task_budget_bytes:
            return None
        for record in plan.split_records:
            if record.block_no == block_no and record.column == column:
                return max(1, record.chunk_rows)
        bytes_per_row = max(1, ceil_div(estimate, row_count))
        if bytes_per_row > plan.effective_task_budget_bytes:
            raise BusinessError(
                E_MEMORY_BUDGET_EXCEEDED,
                "decoded row cannot fit the effective task budget",
                {"block_no": block_no, "column": column, "bytes_per_row": bytes_per_row},
                507,
            )
        return max(1, min(row_count - 1, plan.effective_task_budget_bytes // bytes_per_row))

    def _decode_key_positions(
        self,
        snapshot: Snapshot,
        plan: ExecutionPlan,
        block_no: int,
        target_keys: Sequence[int],
    ) -> list[tuple[int, int]]:
        meta = snapshot.column_block(snapshot.primary_key, block_no)
        chunk_rows = self._chunk_rows_for(plan, block_no, snapshot.primary_key, meta.row_count, meta.decompressed_bytes)
        target_set = set(target_keys)
        positions: dict[int, int] = {}
        observed_min: int | None = None
        observed_max: int | None = None
        step = chunk_rows or meta.row_count
        for start in range(0, meta.row_count, step):
            end = min(meta.row_count, start + step)
            decoded = decode_block_slice(snapshot.block_path(meta), meta, start, end)
            for index, value in enumerate(decoded.values):
                if decoded.null_mask[index]:
                    raise BusinessError(
                        E_BLOCK_CORRUPT,
                        "primary key block contains a null key",
                        {"block_no": block_no, "column": snapshot.primary_key, "row_index": start + index},
                        400,
                    )
                key = int(value)
                observed_min = key if observed_min is None else min(observed_min, key)
                observed_max = key if observed_max is None else max(observed_max, key)
                if key in target_set:
                    if key in positions:
                        raise BusinessError(
                            E_BLOCK_CORRUPT,
                            "primary key appears more than once in a block",
                            {"block_no": block_no, "key": key},
                            400,
                        )
                    positions[key] = start + index
        if positions and (observed_min != meta.key_min or observed_max != meta.key_max):
            raise BusinessError(
                E_BLOCK_CORRUPT,
                "decoded primary-key range does not match the validated block header",
                {
                    "block_no": block_no,
                    "header_key_min": meta.key_min,
                    "header_key_max": meta.key_max,
                    "decoded_key_min": observed_min,
                    "decoded_key_max": observed_max,
                },
                400,
            )
        return sorted(((key, position) for key, position in positions.items()), key=lambda item: item[0])

    def _decode_projected_column(
        self,
        snapshot: Snapshot,
        plan: ExecutionPlan,
        block_no: int,
        column: str,
        target_positions: Sequence[int],
    ) -> list[tuple[Any, bool]]:
        meta = snapshot.column_block(column, block_no)
        chunk_rows = self._chunk_rows_for(plan, block_no, column, meta.row_count, meta.decompressed_bytes)
        step = chunk_rows or meta.row_count
        output: dict[int, tuple[Any, bool]] = {}
        for start in range(0, meta.row_count, step):
            end = min(meta.row_count, start + step)
            overlapping = [
                position
                for position in target_positions
                if start <= position < end
            ]
            if not overlapping:
                continue
            decoded = decode_block_slice(snapshot.block_path(meta), meta, start, end)
            for position in overlapping:
                index = position - start
                is_null = bool(decoded.null_mask[index])
                value = None if is_null else decoded.values[index]
                output[position] = (value, is_null)
        return [output[position] for position in target_positions]

    def _decode_block_result(
        self,
        snapshot: Snapshot,
        plan: ExecutionPlan,
        batch_id: str,
        block_no: int,
    ) -> dict[str, Any]:
        meta = snapshot.primary_column.blocks[block_no]
        target_keys = [key for key in plan.keys if meta.key_min <= key <= meta.key_max]
        key_positions = self._decode_key_positions(snapshot, plan, block_no, target_keys)
        positions = [position for _, position in key_positions]
        columns = list(plan.columns)
        per_column: dict[str, list[tuple[Any, bool]]] = {}
        for column in columns:
            if column == snapshot.primary_key:
                per_column[column] = [(key, False) for key, _ in key_positions]
            else:
                per_column[column] = self._decode_projected_column(
                    snapshot, plan, block_no, column, positions
                )
        rows: list[dict[str, Any]] = []
        for row_index, (key, _) in enumerate(key_positions):
            values = [per_column[column][row_index][0] for column in columns]
            null_mask = [per_column[column][row_index][1] for column in columns]
            if values[0] != key:
                raise BusinessError(
                    E_BLOCK_CORRUPT,
                    "projected primary key differs from lookup key",
                    {"block_no": block_no, "key": key},
                )
            rows.append({"key": key, "values": values, "null_mask": null_mask})
        return {
            "format": BLOCK_RESULT_FORMAT,
            "batch_id": batch_id,
            "block_no": block_no,
            "columns": columns,
            "rows": rows,
        }

    def _load_artifact_rows(self, task: dict[str, Any], columns: Sequence[str], batch_id: str) -> list[dict[str, Any]]:
        path = Path(task["artifact_path"])
        if not path.is_file():
            raise BusinessError(
                E_BLOCK_CORRUPT,
                "committed block artifact is missing",
                {"batch_id": batch_id, "block_no": task["block_no"]},
            )
        payload = strict_json_loads(path.read_bytes())
        if (
            not isinstance(payload, dict)
            or payload.get("format") != BLOCK_RESULT_FORMAT
            or payload.get("batch_id") != batch_id
            or payload.get("block_no") != task["block_no"]
            or payload.get("columns") != list(columns)
            or not isinstance(payload.get("rows"), list)
        ):
            raise BusinessError(
                E_BLOCK_CORRUPT,
                "committed block artifact metadata is inconsistent",
                {"batch_id": batch_id, "block_no": task["block_no"]},
            )
        return payload["rows"]

    def _format_page_row(self, row: dict[str, Any], columns: Sequence[str], snapshot: Snapshot) -> dict[str, Any]:
        values = row.get("values")
        null_mask = row.get("null_mask")
        if not isinstance(values, list) or not isinstance(null_mask, list):
            raise BusinessError(E_BLOCK_CORRUPT, "materialized row is malformed")
        if len(values) != len(columns) or len(null_mask) != len(columns):
            raise BusinessError(E_BLOCK_CORRUPT, "materialized row width is inconsistent")
        formatted: list[Any] = []
        for column, value, is_null in zip(columns, values, null_mask):
            dtype = snapshot.columns[column].dtype
            if is_null:
                formatted.append(None)
            else:
                formatted.append(result_value_for_json(dtype, value, self.config.float_decimal_places))
        return {"key": int(row["key"]), "values": formatted, "null_mask": [bool(value) for value in null_mask]}

    def _assemble_and_finalize(
        self,
        *,
        batch_id: str,
        snapshot: Snapshot,
        plan: ExecutionPlan,
        reused_blocks: int,
        decoded_this_run: int,
    ) -> dict[str, Any]:
        batch = self.db.get_batch(batch_id)
        columns = list(plan.columns)
        page_size = int(batch["page_size"])
        tasks = self.db.get_tasks(batch_id)
        if any(task["state"] != "committed" for task in tasks):
            raise BusinessError(
                E_BLOCK_CORRUPT,
                "cannot finalize before all block tasks commit",
                {"batch_id": batch_id},
            )
        rows_path = self._result_rows_path(batch_id)
        page_buffer: list[dict[str, Any]] = []
        page_no = 0
        total_rows = 0
        last_key: int | None = None

        def flush_page(force: bool = False) -> None:
            nonlocal page_buffer, page_no
            if not page_buffer and not force:
                return
            rows = page_buffer
            row_start = page_no * page_size
            payload = {
                "format": PAGE_FORMAT,
                "batch_id": batch_id,
                "page_no": page_no,
                "page_size": page_size,
                "row_start": row_start,
                "row_end": row_start + len(rows),
                "columns": [
                    {"name": column, "dtype": snapshot.columns[column].dtype} for column in columns
                ],
                "rows": rows,
            }
            atomic_write_json(self._page_path(batch_id, page_no), payload)
            page_no += 1
            page_buffer = []

        with atomic_text_writer(rows_path) as rows_handle:
            for task in tasks:
                artifact_rows = self._load_artifact_rows(task, columns, batch_id)
                for row in artifact_rows:
                    key = int(row["key"])
                    if last_key is not None and key <= last_key:
                        raise BusinessError(
                            E_BLOCK_CORRUPT,
                            "materialized rows are not in strictly ascending primary-key order",
                            {
                                "batch_id": batch_id,
                                "previous_key": last_key,
                                "key": key,
                                "block_no": task["block_no"],
                            },
                        )
                    last_key = key
                    raw_row = {
                        "key": key,
                        "values": row["values"],
                        "null_mask": [bool(value) for value in row["null_mask"]],
                    }
                    rows_handle.write(json.dumps(raw_row, ensure_ascii=False, separators=(",", ":")) + "\n")
                    page_buffer.append(self._format_page_row(row, columns, snapshot))
                    total_rows += 1
                    if len(page_buffer) >= page_size:
                        flush_page()
        if total_rows == 0 or page_buffer:
            flush_page(force=total_rows == 0)
        total_pages = max(1, page_no)
        rows_checksum = file_sha256(rows_path)
        column_defs = [{"name": column, "dtype": snapshot.columns[column].dtype} for column in columns]
        result_digest = sha256_hex(
            canonical_json_bytes(
                {
                    "columns": column_defs,
                    "row_count": total_rows,
                    "rows_checksum": rows_checksum,
                }
            )
        )
        meta = {
            "format": QUERY_RESULT_FORMAT,
            "batch_id": batch_id,
            "snapshot_version": snapshot.version,
            "snapshot_digest": snapshot.digest,
            "primary_key": snapshot.primary_key,
            "columns": column_defs,
            "row_count": total_rows,
            "rows_file": rows_path.name,
            "rows_checksum": rows_checksum,
            "result_digest": result_digest,
            "page_size": page_size,
            "total_pages": total_pages,
        }
        meta_path = self._result_meta_path(batch_id)
        atomic_write_json(meta_path, meta)
        meta_checksum = file_sha256(meta_path)
        summary = {
            "selected_path": plan.selected_path,
            "parallel_blocks": plan.parallel_blocks,
            "peak_memory_estimate_bytes": plan.peak_memory_estimate_bytes,
            "remaining_budget_bytes_at_plan_time": plan.remaining_budget_bytes,
            "effective_task_budget_bytes": plan.effective_task_budget_bytes,
            "reason": plan.reason,
            "hit_block_count": len(plan.hit_blocks),
            "fetch_task_count": len(plan.fetch_tasks),
            "split_records": [record.to_dict() for record in plan.split_records],
            "blocks_reused": reused_blocks,
            "blocks_decoded_this_run": decoded_this_run,
            "row_count": total_rows,
            "result_digest": result_digest,
            "rows_checksum": rows_checksum,
            "float_rounding": self._rounding_note(columns, snapshot),
        }
        self.db.finalize_batch(
            batch_id=batch_id,
            result_path=str(meta_path),
            result_checksum=meta_checksum,
            result_digest=result_digest,
            total_rows=total_rows,
            total_pages=total_pages,
            page_size=page_size,
            execution_summary=summary,
        )
        return summary

    def _rounding_note(self, columns: Sequence[str], snapshot: Snapshot) -> dict[str, Any]:
        return {
            "decimal_places": self.config.float_decimal_places,
            "mode": "fixed_decimal_round_half_even",
            "float_columns": [column for column in columns if snapshot.columns[column].dtype == "float64"],
            "note": (
                "float64 values are serialized as strings with exactly "
                f"{self.config.float_decimal_places} decimal places; internal exact values and both execution "
                "paths use the same binary values before presentation rounding"
            ),
        }

    def _load_result_meta(self, batch: dict[str, Any], verify_checksum: bool = True) -> dict[str, Any]:
        path = Path(batch["result_path"]) if batch.get("result_path") else None
        if path is None or not path.is_file():
            raise BusinessError(
                E_BATCH_FAILED,
                "materialized result metadata is missing",
                {"batch_id": batch["batch_id"]},
            )
        if verify_checksum:
            actual_checksum = file_sha256(path)
            if actual_checksum != batch.get("result_checksum"):
                raise BusinessError(
                    E_BATCH_FAILED,
                    "materialized result metadata checksum mismatch",
                    {"batch_id": batch["batch_id"]},
                )
        try:
            meta = strict_json_loads(path.read_bytes())
        except Exception as exc:
            raise BusinessError(
                E_BATCH_FAILED,
                "materialized result metadata cannot be parsed",
                {"batch_id": batch["batch_id"]},
            ) from exc
        if (
            not isinstance(meta, dict)
            or meta.get("format") != QUERY_RESULT_FORMAT
            or meta.get("batch_id") != batch["batch_id"]
        ):
            raise BusinessError(
                E_BATCH_FAILED,
                "materialized result metadata is inconsistent",
                {"batch_id": batch["batch_id"]},
            )
        return meta

    def _build_page_payload(self, batch: dict[str, Any], page_no: int) -> dict[str, Any]:
        page_descriptor = self.db.get_page(batch["batch_id"], page_no)
        meta = self._load_result_meta(batch)
        page_path = self._page_path(batch["batch_id"], page_no)
        if not page_path.is_file():
            raise BusinessError(
                E_BATCH_FAILED,
                "materialized page file is missing",
                {"batch_id": batch["batch_id"], "page_no": page_no},
            )
        page = strict_json_loads(page_path.read_bytes())
        if (
            not isinstance(page, dict)
            or page.get("format") != PAGE_FORMAT
            or page.get("batch_id") != batch["batch_id"]
            or page.get("page_no") != page_no
            or page.get("columns") != meta["columns"]
        ):
            raise BusinessError(
                E_BATCH_FAILED,
                "materialized page metadata is inconsistent",
                {"batch_id": batch["batch_id"], "page_no": page_no},
            )
        rows = page.get("rows")
        if not isinstance(rows, list):
            raise BusinessError(E_BATCH_FAILED, "materialized page rows are malformed")
        null_by_column: dict[str, list[bool]] = {}
        for index, column in enumerate(meta["columns"]):
            null_by_column[column["name"]] = [bool(row["null_mask"][index]) for row in rows]
        return {
            "batch_id": batch["batch_id"],
            "result_handle": batch["result_handle"],
            "page_no": page_no,
            "page_size": int(batch["page_size"]),
            "row_start": int(page_descriptor["row_start"]),
            "row_end": int(page_descriptor["row_end"]),
            "total_rows": int(meta["row_count"]),
            "total_pages": int(meta["total_pages"]),
            "primary_key": meta["primary_key"],
            "columns": meta["columns"],
            "rows": rows,
            "null_mask": null_by_column,
            "rounding": self._rounding_for_meta(batch, meta),
        }

    def _rounding_for_meta(self, batch: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
        float_columns = [column["name"] for column in meta["columns"] if column["dtype"] == "float64"]
        return {
            "decimal_places": self.config.float_decimal_places,
            "mode": "fixed_decimal_round_half_even",
            "float_columns": float_columns,
            "note": (
                "float64 result values are strings with a fixed decimal representation; "
                "path and parallelism never change the pre-format binary value or row order"
            ),
        }

    def _ensure_materialized(self, identifier: str) -> dict[str, Any]:
        batch = self.db.get_batch(identifier)
        if batch["state"] == "materialized":
            return batch
        if batch["state"] == "failed":
            raise self._restore_batch_error(batch)
        with self._materialize_lock:
            batch = self.db.get_batch(batch["batch_id"])
            if batch["state"] == "materialized":
                return batch
            if batch["state"] == "failed":
                raise self._restore_batch_error(batch)
            snapshot = self._verify_batch_snapshot(batch)
            plan = ExecutionPlan.from_dict(json.loads(batch["plan_json"]))
            try:
                self._materialize_batch(batch["batch_id"], snapshot=snapshot, plan=plan)
            except BusinessError as error:
                self.db.fail_batch(batch["batch_id"], error)
                raise
            return self.db.get_batch(batch["batch_id"])

    def resume_batch(self, identifier: str) -> dict[str, Any]:
        self._ensure_materialized(identifier)
        return self.batch_status(identifier)

    def get_page(self, identifier: str, page_no: int, consumer_id: str) -> dict[str, Any]:
        if isinstance(page_no, bool) or not isinstance(page_no, int) or page_no < 0:
            raise BusinessError(E_INVALID_REQUEST, "page_no must be a non-negative integer")
        if not isinstance(consumer_id, str) or not consumer_id:
            raise BusinessError(E_INVALID_REQUEST, "consumer_id is required")
        batch = self._ensure_materialized(identifier)
        descriptor = self.db.get_page(batch["batch_id"], page_no)
        if descriptor["state"] != "ready":
            raise BusinessError(
                E_PAGE_CONSUMED,
                "page has already been consumed",
                {
                    "batch_id": batch["batch_id"],
                    "page_no": page_no,
                    "consumer_id": descriptor["consumer_id"],
                    "consumed_at": descriptor["consumed_at"],
                },
                409,
            )
        payload = self._build_page_payload(batch, page_no)
        claimed = self.db.claim_page(batch["batch_id"], page_no, consumer_id)
        payload.update(
            {
                "state": "consumed",
                "consumer_id": consumer_id,
                "consumed_at": claimed["consumed_at"],
            }
        )
        return payload

    def batch_status(self, identifier: str) -> dict[str, Any]:
        batch = self.db.get_batch(identifier)
        plan = ExecutionPlan.from_dict(json.loads(batch["plan_json"]))
        tasks = self.db.get_tasks(batch["batch_id"])
        pages = self.db.list_pages(batch["batch_id"])
        task_counts: dict[str, int] = {}
        for task in tasks:
            task_counts[task["state"]] = task_counts.get(task["state"], 0) + 1
        execution = json.loads(batch.get("reuse_json") or "{}")
        error = json.loads(batch["error_json"]) if batch.get("error_json") else None
        first_ready = next((page["page_no"] for page in pages if page["state"] == "ready"), None)
        return {
            "batch_id": batch["batch_id"],
            "result_handle": batch["result_handle"],
            "snapshot_version": batch["snapshot_version"],
            "snapshot_digest": batch["snapshot_digest"],
            "state": batch["state"],
            "columns": plan.columns,
            "key_count": len(plan.keys),
            "hit_blocks": list(plan.hit_blocks),
            "fetch_tasks": [task.to_dict() for task in plan.fetch_tasks],
            "page_size": batch["page_size"],
            "max_page_size": batch["max_page_size"],
            "total_rows": batch["total_rows"],
            "total_pages": batch["total_pages"],
            "next_ready_page": first_ready,
            "block_task_counts": task_counts,
            "plan": {
                "selected_path": plan.selected_path,
                "parallel_blocks": plan.parallel_blocks,
                "peak_memory_estimate_bytes": plan.peak_memory_estimate_bytes,
                "remaining_budget_bytes": plan.remaining_budget_bytes,
                "effective_task_budget_bytes": plan.effective_task_budget_bytes,
                "reason": plan.reason,
                "split_records": [record.to_dict() for record in plan.split_records],
            },
            "execution": execution,
            "error": error,
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
            "completed_at": batch["completed_at"],
        }

    def read_result_rows(self, identifier: str, verify_checksum: bool = True) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
        batch = self.db.get_batch(identifier)
        if batch["state"] != "materialized":
            raise BusinessError(
                E_BATCH_FAILED,
                "batch is not materialized",
                {"batch_id": batch["batch_id"], "state": batch["state"]},
            )
        meta = self._load_result_meta(batch)
        rows_path = Path(batch["result_path"]).parent / meta["rows_file"]
        if not rows_path.is_file():
            raise BusinessError(E_BATCH_FAILED, "result rows file is missing", {"batch_id": batch["batch_id"]})
        if verify_checksum and file_sha256(rows_path) != meta["rows_checksum"]:
            raise BusinessError(E_BATCH_FAILED, "result rows checksum mismatch", {"batch_id": batch["batch_id"]})

        def iterator() -> Iterator[dict[str, Any]]:
            with rows_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise BusinessError(
                            E_BATCH_FAILED,
                            "result rows file contains invalid JSON",
                            {"batch_id": batch["batch_id"], "line": line_number},
                        ) from exc
                    yield row

        return meta, iterator()

    def verify_pagination(self, identifier: str) -> dict[str, Any]:
        batch = self.db.get_batch(identifier)
        if batch["state"] != "materialized":
            raise BusinessError(
                E_BATCH_FAILED,
                "batch is not materialized",
                {"batch_id": batch["batch_id"], "state": batch["state"]},
            )
        meta, internal_rows = self.read_result_rows(identifier)
        dtypes = [column["dtype"] for column in meta["columns"]]
        pages = self.db.list_pages(batch["batch_id"])
        internal = iter(internal_rows)
        differences: list[dict[str, Any]] = []
        checked_rows = 0
        page_summaries: list[dict[str, Any]] = []
        for expected_page_no, descriptor in enumerate(pages):
            if descriptor["page_no"] != expected_page_no:
                differences.append(
                    {"kind": "page_gap", "expected": expected_page_no, "actual": descriptor["page_no"]}
                )
            page_path = self._page_path(batch["batch_id"], descriptor["page_no"])
            page = strict_json_loads(page_path.read_bytes())
            page_rows = page["rows"]
            first_key = int(page_rows[0]["key"]) if page_rows else None
            last_key = int(page_rows[-1]["key"]) if page_rows else None
            page_summaries.append(
                {
                    "page_no": descriptor["page_no"],
                    "row_count": len(page_rows),
                    "first_key": first_key,
                    "last_key": last_key,
                }
            )
            if len(page_rows) != descriptor["row_end"] - descriptor["row_start"]:
                differences.append(
                    {
                        "kind": "page_row_count",
                        "page_no": descriptor["page_no"],
                        "expected": descriptor["row_end"] - descriptor["row_start"],
                        "actual": len(page_rows),
                    }
                )
            for row in page_rows:
                try:
                    raw = next(internal)
                except StopIteration:
                    differences.append(
                        {"kind": "extra_page_row", "page_no": descriptor["page_no"], "key": row.get("key")}
                    )
                    continue
                checked_rows += 1
                expected_values = [
                    None if raw["null_mask"][index] else result_value_for_json(dtype, raw["values"][index], self.config.float_decimal_places)
                    for index, dtype in enumerate(dtypes)
                ]
                expected_row = {
                    "key": int(raw["key"]),
                    "values": expected_values,
                    "null_mask": [bool(value) for value in raw["null_mask"]],
                }
                if row != expected_row and len(differences) < 20:
                    differences.append(
                        {
                            "kind": "row_mismatch",
                            "page_no": descriptor["page_no"],
                            "key": raw["key"],
                            "expected": expected_row,
                            "actual": row,
                        }
                    )
        remaining = sum(1 for _ in internal)
        if remaining:
            differences.append({"kind": "missing_page_rows", "count": remaining})
        if checked_rows != int(meta["row_count"]):
            differences.append(
                {"kind": "total_row_count", "expected": meta["row_count"], "actual": checked_rows}
            )
        return {
            "ok": True,
            "batch_id": batch["batch_id"],
            "equal": not differences,
            "row_count": checked_rows,
            "page_count": len(pages),
            "pages": page_summaries,
            "differences": differences,
        }

    def compare_paths(
        self,
        raw_keys: Any,
        raw_columns: Any,
        *,
        page_size: int = 100,
    ) -> dict[str, Any]:
        stream = self.create_batch(
            raw_keys,
            raw_columns,
            page_size=page_size,
            execution_mode="stream",
            consumer_id="compare:stream",
        )
        batch = self.create_batch(
            raw_keys,
            raw_columns,
            page_size=page_size,
            execution_mode="batch",
            consumer_id="compare:batch",
        )
        stream_meta, stream_rows = self.read_result_rows(stream["batch"]["batch_id"])
        batch_meta, batch_rows = self.read_result_rows(batch["batch"]["batch_id"])
        columns = [column["name"] for column in stream_meta["columns"]]
        dtypes = {column["name"]: column["dtype"] for column in stream_meta["columns"]}
        row_count = 0
        first_key: int | None = None
        last_key: int | None = None
        bitwise_equal = True
        within_tolerance = True
        max_abs = 0.0
        max_rel = 0.0
        differences: list[dict[str, Any]] = []
        stream_iter = iter(stream_rows)
        batch_iter = iter(batch_rows)
        while True:
            try:
                left = next(stream_iter)
            except StopIteration:
                left = None
            try:
                right = next(batch_iter)
            except StopIteration:
                right = None
            if left is None and right is None:
                break
            row_count += 1
            if left is None or right is None:
                bitwise_equal = False
                within_tolerance = False
                if len(differences) < 20:
                    differences.append(
                        {"row": row_count - 1, "kind": "row_count", "stream_missing": left is None, "batch_missing": right is None}
                    )
                continue
            key = int(left["key"])
            first_key = key if first_key is None else first_key
            last_key = key
            if int(right["key"]) != key:
                bitwise_equal = False
                within_tolerance = False
                if len(differences) < 20:
                    differences.append({"row": row_count - 1, "kind": "key", "stream": key, "batch": right["key"]})
                continue
            if [bool(value) for value in left["null_mask"]] != [bool(value) for value in right["null_mask"]]:
                bitwise_equal = False
                within_tolerance = False
                if len(differences) < 20:
                    differences.append({"row": row_count - 1, "kind": "null_mask", "key": key})
            for index, column in enumerate(columns):
                left_value = left["values"][index]
                right_value = right["values"][index]
                dtype = dtypes[column]
                if left["null_mask"][index] or right["null_mask"][index]:
                    if left_value is not None or right_value is not None:
                        bitwise_equal = False
                        within_tolerance = False
                        if len(differences) < 20:
                            differences.append({"row": row_count - 1, "kind": "null_value", "column": column})
                    continue
                if dtype == "float64":
                    left_float = float(left_value)
                    right_float = float(right_value)
                    if left_float.hex() != right_float.hex():
                        bitwise_equal = False
                    absolute = abs(left_float - right_float)
                    relative = absolute / max(abs(left_float), abs(right_float), 1e-300)
                    max_abs = max(max_abs, absolute)
                    max_rel = max(max_rel, relative)
                    if absolute > self.config.float_abs_tolerance and relative > self.config.float_rel_tolerance:
                        within_tolerance = False
                        if len(differences) < 20:
                            differences.append(
                                {
                                    "row": row_count - 1,
                                    "kind": "float_tolerance",
                                    "column": column,
                                    "absolute_deviation": absolute,
                                    "relative_deviation": relative,
                                }
                            )
                elif exact_value_key(dtype, left_value) != exact_value_key(dtype, right_value):
                    bitwise_equal = False
                    within_tolerance = False
                    if len(differences) < 20:
                        differences.append(
                            {"row": row_count - 1, "kind": "value", "column": column, "stream": left_value, "batch": right_value}
                        )
        if row_count != stream_meta["row_count"] or row_count != batch_meta["row_count"]:
            bitwise_equal = False
            within_tolerance = False
        return {
            "ok": True,
            "keys": {"count": stream["batch"]["key_count"], "first_returned": first_key, "last_returned": last_key},
            "row_count": row_count,
            "columns": stream_meta["columns"],
            "bitwise_equal": bitwise_equal,
            "within_tolerance": within_tolerance,
            "max_absolute_deviation": max_abs,
            "max_relative_deviation": max_rel,
            "tolerance": {
                "absolute": self.config.float_abs_tolerance,
                "relative": self.config.float_rel_tolerance,
            },
            "differences": differences,
            "stream": {
                "batch_id": stream["batch"]["batch_id"],
                "selected_path": stream["batch"]["plan"]["selected_path"],
                "parallel_blocks": stream["batch"]["plan"]["parallel_blocks"],
                "peak_memory_estimate_bytes": stream["batch"]["plan"]["peak_memory_estimate_bytes"],
                "split_record_count": len(stream["batch"]["plan"]["split_records"]),
                "result_digest": stream_meta["result_digest"],
            },
            "batch": {
                "batch_id": batch["batch"]["batch_id"],
                "selected_path": batch["batch"]["plan"]["selected_path"],
                "parallel_blocks": batch["batch"]["plan"]["parallel_blocks"],
                "peak_memory_estimate_bytes": batch["batch"]["plan"]["peak_memory_estimate_bytes"],
                "split_record_count": len(batch["batch"]["plan"]["split_records"]),
                "result_digest": batch_meta["result_digest"],
            },
            "rounding": self._rounding_for_meta(self.db.get_batch(stream["batch"]["batch_id"]), stream_meta),
        }
