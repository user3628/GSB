"""Crash-safe, monotonic stage and fold checkpoints."""

from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .canonical import atomic_write_json, digest, sha256_file
from .errors import InvalidStage, StaleCheckpoint

STAGES = ("preprocess", "folds", "solve", "aggregate")
_REQUIRED = {
    "schema_version",
    "sequence",
    "run_id",
    "stage",
    "fold",
    "task_index",
    "role",
    "input_fingerprint",
    "config_version",
    "coefficient_summary",
    "artifact_path",
    "artifact_sha256",
    "reused_from",
    "checksum",
}


class CheckpointStore:
    def __init__(self, run_dir: str | Path, run_id: str) -> None:
        self.run_dir = Path(run_dir)
        self.run_id = run_id
        self.directory = self.run_dir / "checkpoints"
        self.quarantine_dir = self.directory / "quarantine"
        self.lock_path = self.run_dir / ".checkpoint.lock"
        self.directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _record_checksum(self, payload: dict[str, Any]) -> str:
        unsigned = {key: value for key, value in payload.items() if key != "checksum"}
        return digest(unsigned)

    def _validate_record(self, path: Path) -> tuple[dict[str, Any] | None, str | None]:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            return None, f"unreadable: {exc}"
        try:
            record = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            return None, f"invalid JSON: {exc}"
        if not isinstance(record, dict):
            return None, "record is not an object"
        missing = _REQUIRED - set(record)
        if missing:
            return None, f"missing fields: {sorted(missing)}"
        if record.get("schema_version") != 1:
            return None, "unsupported schema_version"
        sequence = record.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            return None, "invalid sequence"
        try:
            file_sequence = int(path.stem)
        except ValueError:
            return None, "filename is not a sequence"
        if file_sequence != sequence:
            return None, "filename and embedded sequence differ"
        if record.get("run_id") != self.run_id:
            return None, "run_id mismatch"
        if record.get("stage") not in STAGES:
            return None, "unknown stage"
        if record.get("role") not in {"preprocess", "folds", "solve", "final", "aggregate"}:
            return None, "unknown role"
        fold = record.get("fold")
        if fold is not None and (isinstance(fold, bool) or not isinstance(fold, int)):
            return None, "invalid fold"
        task_index = record.get("task_index")
        if task_index is not None and (
            isinstance(task_index, bool) or not isinstance(task_index, int) or task_index < 1
        ):
            return None, "invalid task_index"
        if not isinstance(record.get("input_fingerprint"), str):
            return None, "invalid input_fingerprint"
        if not isinstance(record.get("config_version"), str):
            return None, "invalid config_version"
        if not (record.get("checksum") == digest({k: v for k, v in record.items() if k != "checksum"})):
            return None, "checksum mismatch"
        return record, None

    def _read_candidates(self) -> tuple[dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]], list[dict[str, Any]]]:
        candidates: dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]] = {}
        ignored: list[dict[str, Any]] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                sequence = int(path.stem)
            except ValueError:
                ignored.append({"file": path.name, "reason": "non-sequence filename"})
                continue
            record, error = self._validate_record(path)
            candidates.setdefault(sequence, []).append((path, record, error))
            if error:
                ignored.append({"file": path.name, "sequence": sequence, "reason": error})
        return candidates, ignored

    def _stage_sequence_valid(self, records: list[dict[str, Any]]) -> tuple[bool, str | None]:
        if not records:
            return True, None
        stage_seen = [record["stage"] for record in records]
        if stage_seen[0] != "preprocess":
            return False, "first checkpoint must be preprocess"
        if len(records) >= 2 and records[1]["stage"] != "folds":
            return False, "second checkpoint must be folds"
        solve_seen = 0
        final_seen = False
        aggregate_seen = False
        for index, record in enumerate(records):
            stage = record["stage"]
            expected_sequence = index + 1
            if record["sequence"] != expected_sequence:
                return False, "non-contiguous sequence"
            if stage == "preprocess":
                if index != 0 or record["fold"] is not None or record["role"] != "preprocess":
                    return False, "invalid preprocess checkpoint position"
            elif stage == "folds":
                if index != 1 or record["fold"] is not None or record["role"] != "folds":
                    return False, "invalid folds checkpoint position"
            elif stage == "solve":
                if aggregate_seen:
                    return False, "solve checkpoint after aggregate"
                if final_seen:
                    return False, "solve checkpoint after final solve"
                if record["fold"] is None or record["task_index"] is None:
                    return False, "solve checkpoint requires fold and task_index"
                solve_seen += 1
                if record["task_index"] != solve_seen:
                    return False, "cross-fold/task checkpoint position mismatch"
                if record["role"] == "final":
                    if record["fold"] != 0:
                        return False, "final solve must use fold 0"
                    final_seen = True
                elif record["role"] != "solve":
                    return False, "invalid solve role"
            elif stage == "aggregate":
                if aggregate_seen or index != len(records) - 1:
                    return False, "aggregate must be final checkpoint"
                if not final_seen:
                    return False, "aggregate requires a completed final solve"
                if record["fold"] is not None or record["role"] != "aggregate":
                    return False, "invalid aggregate checkpoint"
                aggregate_seen = True
            else:
                return False, "unknown stage"
        return True, None

    def inspect(self) -> dict[str, Any]:
        """Return the longest valid contiguous prefix; never mutate files."""

        candidates, ignored = self._read_candidates()
        valid: list[dict[str, Any]] = []
        expected = 1
        gap_reason: str | None = None
        while True:
            entries = candidates.get(expected, [])
            if not entries:
                gap_reason = f"missing sequence {expected}" if entries == [] else None
                break
            valid_entries = [entry for entry in entries if entry[1] is not None]
            if len(entries) > 1 or len(valid_entries) != 1:
                gap_reason = f"duplicate or invalid records at sequence {expected}"
                for path, _, error in entries:
                    if error:
                        ignored.append(
                            {"file": path.name, "sequence": expected, "reason": error}
                        )
                break
            candidate = valid_entries[0][1]
            proposed = valid + [candidate]
            stage_ok, stage_error = self._stage_sequence_valid(proposed)
            if not stage_ok:
                ignored.append(
                    {
                        "file": valid_entries[0][0].name,
                        "sequence": expected,
                        "reason": stage_error,
                    }
                )
                gap_reason = stage_error
                break
            valid.append(candidate)
            expected += 1
        if gap_reason:
            first_missing = expected
            for sequence, entries in sorted(candidates.items()):
                if sequence < first_missing:
                    continue
                if sequence == first_missing and len(entries) == 1 and entries[0][1] is not None:
                    # The entry at the gap was already reported as invalid/rejected.
                    continue
                for path, _, error in entries:
                    ignored.append(
                        {
                            "file": path.name,
                            "sequence": sequence,
                            "reason": error or f"ignored after gap at sequence {first_missing}",
                        }
                    )
        return {
            "records": valid,
            "ignored": ignored,
            "next_sequence": (valid[-1]["sequence"] + 1) if valid else 1,
            "last_sequence": valid[-1]["sequence"] if valid else 0,
            "corrupt": bool(ignored),
        }

    def repair_from_gap(self) -> dict[str, Any]:
        """Quarantine invalid records and any records after a broken prefix."""

        state = self.inspect()
        keep = {record["sequence"] for record in state["records"]}
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        moved: list[str] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                sequence = int(path.stem)
            except ValueError:
                sequence = None
            if sequence is not None and sequence in keep:
                # A duplicate with the same sequence but not the accepted record
                # is not distinguishable by name alone; keep the exact accepted
                # filename and quarantine duplicates represented by `ignored`.
                accepted_name = f"{sequence:08d}.json"
                if path.name == accepted_name:
                    continue
            target = self.quarantine_dir / (
                f"{path.stem}.{digest({'file': path.name, 'bytes': path.read_bytes().hex()})[:12]}.bad.json"
            )
            os.replace(path, target)
            moved.append(f"{path.name}->{target.name}")
        if moved:
            state = self.inspect()
            state["quarantined"] = moved
        else:
            state["quarantined"] = []
        return state

    def publish(
        self,
        *,
        stage: str,
        fold: int | None,
        task_index: int | None,
        role: str,
        input_fingerprint: str,
        config_version: str,
        coefficient_summary: dict[str, Any] | None,
        artifact_path: str | None,
        reused_from: str | None = None,
        sequence: int | None = None,
        expected_task: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise InvalidStage(
                "Unknown checkpoint stage", details={"stage": stage, "allowed": list(STAGES)}
            )
        with self.locked():
            state = self.inspect()
            if state["ignored"]:
                state = self.repair_from_gap()
            next_sequence = state["next_sequence"]
            if sequence is not None and sequence != next_sequence:
                raise StaleCheckpoint(
                    "Checkpoint sequence is stale or non-contiguous",
                    details={
                        "supplied": sequence,
                        "expected": next_sequence,
                        "last_sequence": state["last_sequence"],
                    },
                )
            records = state["records"]
            stages_seen = [record["stage"] for record in records]
            if stage == "preprocess":
                if records:
                    raise StaleCheckpoint(
                        "Duplicate preprocess checkpoint publication",
                        details={"last_sequence": state["last_sequence"]},
                    )
            elif stage == "folds":
                if "folds" in stages_seen:
                    raise StaleCheckpoint(
                        "Duplicate folds checkpoint publication",
                        details={"last_sequence": state["last_sequence"]},
                    )
                if stages_seen != ["preprocess"]:
                    raise InvalidStage(
                        "Folds checkpoint requires completed preprocessing",
                        details={"stages_seen": stages_seen},
                    )
            elif stage == "solve":
                if not stages_seen or stages_seen[:2] != ["preprocess", "folds"]:
                    raise InvalidStage(
                        "Solve checkpoint requires preprocess and folds checkpoints",
                        details={"stages_seen": stages_seen},
                    )
                if "aggregate" in stages_seen:
                    raise InvalidStage("Solve checkpoint cannot follow aggregate")
                solve_records = [record for record in records if record["stage"] == "solve"]
                if any(record["role"] == "final" for record in solve_records):
                    raise StaleCheckpoint(
                        "Final solve already completed; no further fold publication expected"
                    )
                expected_index = len(solve_records) + 1
                if task_index != expected_index:
                    raise StaleCheckpoint(
                        "Cross-fold or out-of-order solve publication",
                        details={"task_index": task_index, "expected": expected_index},
                    )
                if role not in {"solve", "final"}:
                    raise InvalidStage("Solve role must be solve or final")
                if role == "final" and fold != 0:
                    raise InvalidStage("Final solve must use fold 0")
                if role == "solve" and (fold is None or fold < 1):
                    raise InvalidStage("CV solve requires a positive fold number")
                if role == "solve":
                    prior_cv = [record for record in solve_records if record["role"] == "solve"]
                    if prior_cv:
                        previous_fold = prior_cv[-1]["fold"]
                        if fold < previous_fold:
                            raise StaleCheckpoint(
                                "Cross-fold checkpoint moved backwards",
                                details={"fold": fold, "previous_fold": previous_fold},
                            )
                        if fold > previous_fold + 1:
                            raise StaleCheckpoint(
                                "Cross-fold checkpoint skipped a fold",
                                details={"fold": fold, "expected_max": previous_fold + 1},
                            )
                    elif fold != 1:
                        raise StaleCheckpoint(
                            "First CV solve checkpoint must use fold 1",
                            details={"fold": fold},
                        )
            elif stage == "aggregate":
                if not records or records[-1]["stage"] != "solve" or records[-1]["role"] != "final":
                    raise InvalidStage(
                        "Aggregate checkpoint requires a completed final solve",
                        details={"stages_seen": stages_seen},
                    )
                if "aggregate" in stages_seen:
                    raise StaleCheckpoint("Aggregate checkpoint already published")
            if expected_task is not None:
                for key, value in expected_task.items():
                    actual = {
                        "stage": stage,
                        "fold": fold,
                        "task_index": task_index,
                        "role": role,
                        "input_fingerprint": input_fingerprint,
                    }.get(key)
                    if actual != value:
                        raise StaleCheckpoint(
                            "Checkpoint does not match the expected fold/task",
                            details={"field": key, "expected": value, "actual": actual},
                        )

            artifact_hash = None
            artifact_relative = None
            if artifact_path is not None:
                artifact = (self.run_dir / artifact_path).resolve()
                try:
                    artifact.relative_to(self.run_dir.resolve())
                except ValueError as exc:
                    raise InvalidStage(
                        "Artifact path escapes the run directory",
                        details={"artifact_path": artifact_path},
                    ) from exc
                if not artifact.exists():
                    raise InvalidStage(
                        "Checkpoint artifact is missing",
                        details={"artifact_path": artifact_path},
                    )
                artifact_hash = sha256_file(artifact)
                artifact_relative = str(artifact.relative_to(self.run_dir.resolve()))

            record = {
                "schema_version": 1,
                "sequence": next_sequence,
                "run_id": self.run_id,
                "stage": stage,
                "fold": fold,
                "task_index": task_index,
                "role": role,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "coefficient_summary": coefficient_summary,
                "artifact_path": artifact_relative,
                "artifact_sha256": artifact_hash,
                "reused_from": reused_from,
            }
            record["checksum"] = self._record_checksum(record)
            atomic_write_json(self.directory / f"{next_sequence:08d}.json", record)
            return record

    def artifact_for(self, record: dict[str, Any]) -> tuple[Path, Any]:
        if not record.get("artifact_path"):
            raise InvalidStage("Checkpoint has no artifact", details={"sequence": record["sequence"]})
        path = self.run_dir / record["artifact_path"]
        if not path.exists():
            raise InvalidStage(
                "Checkpoint artifact is missing",
                details={"sequence": record["sequence"], "path": str(path)},
            )
        if sha256_file(path) != record.get("artifact_sha256"):
            raise InvalidStage(
                "Checkpoint artifact checksum mismatch",
                details={"sequence": record["sequence"], "path": str(path)},
            )
        with open(path, "r", encoding="utf-8") as handle:
            return path, json.load(handle)

    def truncate_at(self, sequence: int) -> list[str]:
        """Quarantine sequence N and every later checkpoint after an artifact fault."""

        with self.locked():
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            moved: list[str] = []
            for path in sorted(self.directory.glob("*.json")):
                try:
                    item_sequence = int(path.stem)
                except ValueError:
                    item_sequence = sequence
                if item_sequence < sequence:
                    continue
                target = self.quarantine_dir / (
                    f"{path.stem}.{digest({'file': path.name, 'bytes': path.read_bytes().hex()})[:12]}.bad.json"
                )
                os.replace(path, target)
                moved.append(f"{path.name}->{target.name}")
            return moved

    def append_stop_marker(self, payload: dict[str, Any] | None = None) -> None:
        # Stop markers are deliberately outside the checkpoint sequence: they
        # are control messages, not completed work records.
        atomic_write_json(self.run_dir / "STOP", payload or {"requested": True})
