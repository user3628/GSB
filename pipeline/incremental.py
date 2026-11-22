from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

from .config import load as load_config
from .checkpoints import CheckpointManager
from .errors import E_INCREMENTAL_MISMATCH, PipelineError
from .runner import FINAL_FILES, run_pipeline
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    first_difference,
    read_json,
    read_jsonl,
    sha256_bytes,
    sha256_file,
)


def _first_result_difference(left_path: Path, right_path: Path) -> dict[str, Any] | None:
    left_rows = read_jsonl(left_path)
    right_rows = read_jsonl(right_path)
    left_by_id = {row["doc_id"]: row for row in left_rows}
    right_by_id = {row["doc_id"]: row for row in right_rows}
    all_ids = sorted(set(left_by_id) | set(right_by_id))
    for doc_id in all_ids:
        left_row = left_by_id.get(doc_id)
        right_row = right_by_id.get(doc_id)
        if left_row is None or right_row is None:
            return {
                "batch_id": (left_row or right_row).get("batch_id") if (left_row or right_row) else None,
                "doc_id": doc_id,
                "field": "results.doc_id_presence",
                "left": left_row,
                "right": right_row,
            }
        difference = first_difference(left_row, right_row)
        if difference is not None:
            field, left_value, right_value = difference
            return {
                "batch_id": left_row.get("batch_id"),
                "doc_id": doc_id,
                "field": f"results.{field}",
                "left": left_value,
                "right": right_value,
            }
    return None


def _context_batch_id(value: Any) -> str | None:
    if isinstance(value, dict):
        batch_id = value.get("batch_id")
        if isinstance(batch_id, str):
            return batch_id
    return None


def _structured_difference(
    left: Any,
    right: Any,
    path: str = "",
    fallback_batch_id: str | None = None,
) -> dict[str, Any] | None:
    if isinstance(left, dict) and isinstance(right, dict):
        all_keys = sorted(set(left) | set(right))
        for key in all_keys:
            child = f"{path}.{key}" if path else key
            if key not in left or key not in right:
                return {
                    "batch_id": _context_batch_id(left) or _context_batch_id(right) or fallback_batch_id,
                    "field": child,
                    "left": left.get(key),
                    "right": right.get(key),
                }
            result = _structured_difference(left[key], right[key], child, fallback_batch_id)
            if result is not None:
                if result.get("batch_id") is None:
                    result["batch_id"] = _context_batch_id(left) or fallback_batch_id
                return result
        return None
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return {
                "batch_id": fallback_batch_id,
                "field": f"{path}.length" if path else "length",
                "left": len(left),
                "right": len(right),
            }
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            child = f"{path}[{index}]"
            result = _structured_difference(left_item, right_item, child, fallback_batch_id)
            if result is not None:
                if result.get("batch_id") is None:
                    result["batch_id"] = _context_batch_id(left_item) or _context_batch_id(right_item) or fallback_batch_id
                return result
        return None
    if left != right:
        return {
            "batch_id": fallback_batch_id,
            "field": path or "$",
            "left": left,
            "right": right,
        }
    return None


def compare_workspaces(
    left_workspace: Path | str,
    right_workspace: Path | str,
    fallback_batch_id: str | None = None,
) -> dict[str, Any]:
    left = Path(left_workspace)
    right = Path(right_workspace)
    for name in FINAL_FILES:
        left_path = left / name
        right_path = right / name
        if not left_path.is_file() or not right_path.is_file():
            return {
                "status": "mismatch",
                "error_code": E_INCREMENTAL_MISMATCH,
                "batch_id": fallback_batch_id,
                "field": name,
                "reason": "missing_final_artifact",
                "left_exists": left_path.is_file(),
                "right_exists": right_path.is_file(),
            }
        if left_path.read_bytes() == right_path.read_bytes():
            continue
        if name == "results.jsonl":
            difference = _first_result_difference(left_path, right_path)
            if difference is not None:
                return {
                    "status": "mismatch",
                    "error_code": E_INCREMENTAL_MISMATCH,
                    **difference,
                }
        try:
            left_value = read_json(left_path)
            right_value = read_json(right_path)
        except Exception:
            return {
                "status": "mismatch",
                "error_code": E_INCREMENTAL_MISMATCH,
                "batch_id": fallback_batch_id,
                "field": name,
                "reason": "non_json_difference",
                "left_digest": sha256_file(left_path),
                "right_digest": sha256_file(right_path),
            }
        difference = _structured_difference(
            left_value, right_value, name, fallback_batch_id
        )
        if difference is not None:
            return {
                "status": "mismatch",
                "error_code": E_INCREMENTAL_MISMATCH,
                **difference,
            }
    return {
        "status": "ok",
        "compared_files": list(FINAL_FILES),
        "left": str(left),
        "right": str(right),
    }


def ensure_workspace_match(comparison: dict[str, Any]) -> None:
    if comparison.get("status") == "ok":
        return
    raise PipelineError(
        E_INCREMENTAL_MISMATCH,
        "incremental rebuild differs from a fresh full run",
        {
            "batch_id": comparison.get("batch_id"),
            "field": comparison.get("field"),
            "left": comparison.get("left"),
            "right": comparison.get("right"),
        },
    )


def replace_batch(
    workspace: Path | str,
    config_path: Path | str,
    dataset_index_path: Path | str,
    batch_id: str,
    source_path: Path | str,
    stop_event: Any = None,
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    config = load_config(config_path)
    index_path = Path(dataset_index_path).resolve()
    index = read_json(index_path)
    target: dict[str, Any] | None = None
    for item in index.get("batches", []):
        if item.get("batch_id") == batch_id:
            target = item
            break
    if target is None:
        raise PipelineError("E_INVALID_CONFIG", "replacement batch is absent from index", {"batch_id": batch_id})
    source = Path(source_path).resolve()
    if not source.is_file():
        raise PipelineError("E_INVALID_CONFIG", "replacement source does not exist", {"path": str(source)})
    source_data = source.read_bytes()
    source_rows = read_jsonl(source)
    if not source_rows:
        raise PipelineError("E_INVALID_CONFIG", "replacement source is empty", {"batch_id": batch_id})
    if len({row.get("doc_id") for row in source_rows}) != len(source_rows):
        raise PipelineError("E_INVALID_CONFIG", "replacement source contains duplicate doc_id values", {"batch_id": batch_id})
    observed_labels = sorted(
        {row.get("label", row.get("category")) for row in source_rows}
    )
    if any(label not in config.labels for label in observed_labels):
        raise PipelineError(
            "E_INVALID_LABEL",
            "replacement source contains labels absent from configuration",
            {"batch_id": batch_id, "labels": [label for label in observed_labels if label not in config.labels]},
        )
    target_path = (index_path.parent / target["path"]).resolve()
    atomic_write_bytes(target_path, source_data)
    target["row_count"] = len(source_rows)
    target["labels"] = observed_labels
    target["languages"] = sorted(
        {
            row.get("lang", row.get("language"))
            for row in source_rows
            if row.get("lang", row.get("language"))
        }
    )
    target["channels"] = sorted(
        {
            row.get("channel", row.get("source_channel", row.get("source")))
            for row in source_rows
            if row.get("channel", row.get("source_channel", row.get("source")))
        }
    )
    target["content_digest"] = sha256_bytes(source_data)
    for position, item in enumerate(index["batches"]):
        if item.get("batch_id") == batch_id:
            index["batches"][position] = target
            break
    atomic_write_json(index_path, index)
    before_sequence = CheckpointManager(workspace_path).state()["sequence"]
    incremental = run_pipeline(workspace_path, config_path, index_path, stop_event=stop_event)
    fresh_root = Path(tempfile.mkdtemp(prefix="fresh-full-"))
    try:
        fresh = run_pipeline(fresh_root, config_path, index_path, stop_event=stop_event)
        comparison = compare_workspaces(workspace_path, fresh_root, fallback_batch_id=batch_id)
    finally:
        shutil.rmtree(fresh_root, ignore_errors=True)
    incremental_report_path = workspace_path / "incremental_report.json"
    invalidations: list[dict[str, Any]] = []
    if incremental_report_path.is_file():
        report = read_json(incremental_report_path)
        invalidations = [
            item
            for item in report.get("invalidations", [])
            if item.get("sequence", 0) > before_sequence
            and item.get("batch_id") == batch_id
        ]
    try:
        ensure_workspace_match(comparison)
    except PipelineError as exc:
        exc.details.update(
            {
                "incremental": incremental,
                "fresh": fresh,
                "new_invalidations": invalidations,
            }
        )
        raise
    return {
        "status": "ok",
        "batch_id": batch_id,
        "incremental": incremental,
        "fresh": fresh,
        "comparison": comparison,
        "new_invalidations": invalidations,
    }
