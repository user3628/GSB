"""Atomic checkpoints, product cache, edits, stop flags and export files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from .errors import EngineError
from .utils import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    canonical_json,
    digest,
    ensure_writable_dir,
    file_lock,
    read_json,
)


CHECKPOINT_SCHEMA = "sumengine.checkpoint.v1"
PRODUCT_SCHEMA = "sumengine.products.v1"
EDIT_SCHEMA = "sumengine.edits.v1"


def _checkpoint_empty() -> dict[str, Any]:
    return {"schema": CHECKPOINT_SCHEMA, "next_sequence": 1, "records": []}


def _doc_file_name(doc_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", doc_id)[:60]
    return f"{safe}-{digest(doc_id)[:12]}.json"


class StateStore:
    def __init__(self, workspace: Path) -> None:
        self.workspace = Path(workspace)
        self.state_dir = self.workspace / "state"
        self.products_dir = self.state_dir / "products"
        self.summaries_dir = self.workspace / "summaries"
        self.exports_dir = self.workspace / "exports"
        self.checkpoints_path = self.state_dir / "checkpoints.json"
        self.checkpoints_lock = self.state_dir / "checkpoints.lock"
        self.products_lock = self.state_dir / "products.lock"
        self.edits_path = self.state_dir / "edits.json"
        self.edits_lock = self.state_dir / "edits.lock"
        self.stop_path = self.state_dir / "stop.request"
        self.last_run_path = self.state_dir / "last_run.json"

    def initialize(self) -> None:
        for directory in (self.workspace, self.state_dir, self.products_dir, self.summaries_dir, self.exports_dir):
            try:
                ensure_writable_dir(directory)
            except OSError as exc:
                raise EngineError(
                    "E_WORKSPACE_NOT_WRITABLE",
                    f"Workspace directory is not writable: {directory}: {exc}",
                ) from exc

    def _product_path(self, doc_id: str) -> Path:
        return self.products_dir / _doc_file_name(doc_id)

    def load_checkpoints(self) -> dict[str, Any]:
        data = read_json(self.checkpoints_path, None)
        if data is None:
            return _checkpoint_empty()
        if not isinstance(data, dict) or data.get("schema") != CHECKPOINT_SCHEMA:
            raise EngineError("E_STATE_CORRUPT", f"Invalid checkpoint journal: {self.checkpoints_path}")
        records = data.get("records")
        if not isinstance(records, list):
            raise EngineError("E_STATE_CORRUPT", "Checkpoint records must be a list")
        return data

    def checkpoint_records(self) -> list[dict[str, Any]]:
        return list(self.load_checkpoints()["records"])

    def latest_checkpoints(self) -> dict[tuple[str, str], dict[str, Any]]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for record in self.load_checkpoints()["records"]:
            key = (str(record.get("doc_id")), str(record.get("stage")))
            previous = latest.get(key)
            if previous is None or int(record.get("sequence", 0)) > int(previous.get("sequence", 0)):
                latest[key] = record
        return latest

    def _validate_record(self, record: dict[str, Any]) -> None:
        required = {
            "stage",
            "doc_id",
            "input_fingerprint",
            "config_version",
            "edit_version",
            "product_digest",
            "stage_fingerprint",
        }
        missing = sorted(required - set(record))
        if missing:
            raise EngineError("E_INVALID_INPUT", f"Checkpoint record missing fields: {', '.join(missing)}")

    def publish_records(
        self,
        records: Iterable[dict[str, Any]],
        *,
        expected_doc_id: str | None = None,
        expected_sequence: int | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically append complete checkpoint records under one file lock.

        Sequence comparison prevents stale/duplicate publications.  Supplying
        expected_doc_id is the cross-document alignment guard; a mismatch is a
        stale checkpoint publication, not a partial write.
        """
        pending = [dict(record) for record in records]
        if not pending:
            return []
        for record in pending:
            self._validate_record(record)
        with file_lock(self.checkpoints_lock):
            journal = self.load_checkpoints()
            existing = journal["records"]
            next_sequence = int(journal.get("next_sequence", 1))
            max_sequence = max((int(item.get("sequence", 0)) for item in existing), default=0)
            if next_sequence <= max_sequence:
                raise EngineError("E_STATE_CORRUPT", "Checkpoint next_sequence is not monotonic")
            if expected_sequence is not None and expected_sequence != next_sequence:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint sequence is stale",
                    doc_id=expected_doc_id,
                    details={"expected_sequence": expected_sequence, "next_sequence": next_sequence},
                )
            latest_by_doc_stage: dict[tuple[str, str], dict[str, Any]] = {}
            for item in existing:
                key = (str(item.get("doc_id")), str(item.get("stage")))
                previous = latest_by_doc_stage.get(key)
                if previous is None or int(item.get("sequence", 0)) > int(previous.get("sequence", 0)):
                    latest_by_doc_stage[key] = item
            batch_keys: set[tuple[str, str, str, str]] = set()
            output: list[dict[str, Any]] = []
            for offset, record in enumerate(pending):
                doc_id = str(record["doc_id"])
                if expected_doc_id is not None and doc_id != expected_doc_id:
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "Checkpoint publication crossed document boundaries",
                        doc_id=doc_id,
                        details={"expected_doc_id": expected_doc_id},
                    )
                key = (
                    str(record["stage"]),
                    doc_id,
                    str(record["stage_fingerprint"]),
                    str(record["product_digest"]),
                )
                latest = latest_by_doc_stage.get((doc_id, str(record["stage"])))
                duplicate_of_latest = bool(
                    latest
                    and str(latest.get("stage_fingerprint")) == str(record["stage_fingerprint"])
                    and str(latest.get("product_digest")) == str(record["product_digest"])
                )
                stale_edit_version = bool(
                    latest and int(record.get("edit_version", 0)) < int(latest.get("edit_version", 0))
                )
                if stale_edit_version:
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "Checkpoint edit version is behind the latest publication",
                        doc_id=doc_id,
                        details={
                            "stage": record["stage"],
                            "incoming_edit_version": int(record.get("edit_version", 0)),
                            "latest_edit_version": int(latest.get("edit_version", 0)),
                        },
                    )
                if duplicate_of_latest or key in batch_keys:
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "Duplicate checkpoint publication",
                        doc_id=doc_id,
                        details={"stage": record["stage"], "reason": "DUPLICATE"},
                    )
                supplied_sequence = record.get("sequence")
                sequence = next_sequence + offset
                if supplied_sequence is not None and int(supplied_sequence) != sequence:
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "Checkpoint sequence is stale or out of order",
                        doc_id=doc_id,
                        details={"supplied_sequence": supplied_sequence, "expected_sequence": sequence},
                    )
                complete = dict(record)
                complete["sequence"] = sequence
                complete["schema"] = CHECKPOINT_SCHEMA
                batch_keys.add(key)
                output.append(complete)
            journal["records"] = existing + output
            journal["next_sequence"] = next_sequence + len(output)
            atomic_write_json(self.checkpoints_path, journal)
            return output

    def publish_checkpoint(
        self,
        record: dict[str, Any],
        *,
        expected_doc_id: str | None = None,
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        return self.publish_records(
            [record],
            expected_doc_id=expected_doc_id,
            expected_sequence=expected_sequence,
        )[0]

    def load_products(self, doc_id: str) -> dict[str, Any] | None:
        data = read_json(self._product_path(doc_id), None)
        if data is None:
            return None
        if not isinstance(data, dict) or data.get("schema") != PRODUCT_SCHEMA:
            raise EngineError("E_STATE_CORRUPT", f"Invalid product cache for {doc_id}")
        return data

    def save_products(
        self,
        doc_id: str,
        *,
        input_fingerprint: str,
        config_version: str,
        schedule_fingerprint: str,
        stages: dict[str, dict[str, Any]],
    ) -> None:
        clean_stages: dict[str, Any] = {}
        for name, entry in stages.items():
            product = entry["product"]
            clean_stages[name] = {
                "fingerprint": str(entry["fingerprint"]),
                "edit_version": int(entry.get("edit_version", 0)),
                "product_digest": digest(product),
                "product": product,
            }
        payload = {
            "schema": PRODUCT_SCHEMA,
            "doc_id": doc_id,
            "input_fingerprint": input_fingerprint,
            "config_version": config_version,
            "schedule_fingerprint": schedule_fingerprint,
            "stages": clean_stages,
        }
        with file_lock(self.products_lock):
            atomic_write_json(self._product_path(doc_id), payload)

    def load_edits(self) -> dict[str, Any]:
        data = read_json(self.edits_path, None)
        if data is None:
            return {"schema": EDIT_SCHEMA, "documents": {}}
        if not isinstance(data, dict) or data.get("schema") != EDIT_SCHEMA:
            raise EngineError("E_STATE_CORRUPT", f"Invalid edits journal: {self.edits_path}")
        return data

    def get_edit(self, doc_id: str) -> dict[str, Any] | None:
        document = self.load_edits()["documents"].get(doc_id)
        return dict(document) if document else None

    def append_edit(
        self,
        doc_id: str,
        text: str,
        *,
        base_fusion_fingerprint: str,
    ) -> dict[str, Any]:
        if not text.strip():
            raise EngineError("E_INVALID_INPUT", "Edited summary must not be empty", doc_id=doc_id)
        with file_lock(self.edits_lock):
            journal = self.load_edits()
            current = journal["documents"].get(doc_id)
            version = int(current.get("version", 0)) + 1 if current else 1
            history = list(current.get("history", [])) if current else []
            entry = {
                "version": version,
                "text": text,
                "text_digest": digest(text),
                "base_fusion_fingerprint": base_fusion_fingerprint,
            }
            history.append(entry)
            document = {
                **entry,
                "doc_id": doc_id,
                "history": history,
            }
            journal["documents"][doc_id] = document
            atomic_write_json(self.edits_path, journal)
            return document

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def request_stop(self, reason: str = "requested") -> None:
        atomic_write_json(self.stop_path, {"stop": True, "reason": reason})

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def save_last_run(self, value: dict[str, Any]) -> None:
        atomic_write_json(self.last_run_path, value)

    def load_last_run(self) -> dict[str, Any] | None:
        return read_json(self.last_run_path, None)

    def write_export(self, value: dict[str, Any]) -> dict[str, str]:
        payload = canonical_bytes(value) + b"\n"
        export_path = self.exports_dir / "final_summaries.json"
        mirror_path = self.summaries_dir / "final_summaries.json"
        atomic_write_bytes(export_path, payload)
        atomic_write_bytes(mirror_path, payload)
        return {
            "export": str(export_path),
            "mirror": str(mirror_path),
            "digest": digest(value),
        }

    def export_text(self, value: dict[str, Any]) -> str:
        return canonical_json(value, indent=2) + "\n"
