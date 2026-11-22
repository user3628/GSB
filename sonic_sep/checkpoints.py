"""Atomic, file-locked checkpoint journal with duplicate/corruption recovery."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
from pathlib import Path
from typing import Any, Iterator, Mapping

from .errors import stale_checkpoint
from .util import atomic_write_json, digest_json, read_json


STAGES = ("stft", "features", "mask_init", "mask_refine", "reconstruct")
CHECKPOINT_FILENAME = re.compile(r"^(?P<sequence>\d{12})(?:[-_.].*)?\.json$")


def _record_without_checksum(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "checksum"}


def _validate_record(record: Any, *, filename_sequence: int) -> tuple[bool, str | None]:
    if not isinstance(record, Mapping):
        return False, "record is not an object"
    required = (
        "schema_version",
        "sequence",
        "block_sequence",
        "slice_id",
        "stage",
        "block_index",
        "frame_start",
        "frame_end",
        "bands",
        "input_fingerprint",
        "config_version",
        "product_digest",
        "round",
        "stage_complete",
        "checksum",
    )
    missing = [key for key in required if key not in record]
    if missing:
        return False, f"missing fields: {','.join(missing)}"
    if record["schema_version"] != 1:
        return False, "unsupported schema_version"
    if not isinstance(record["sequence"], int) or record["sequence"] != filename_sequence:
        return False, "sequence does not match filename"
    if not isinstance(record["block_sequence"], int) or record["block_sequence"] < 1:
        return False, "invalid block_sequence"
    if record["stage"] not in STAGES:
        return False, "unknown stage"
    if not isinstance(record["slice_id"], str) or not record["slice_id"]:
        return False, "invalid slice_id"
    if not isinstance(record["block_index"], int) or record["block_index"] < 0:
        return False, "invalid block_index"
    for key in ("frame_start", "frame_end"):
        if not isinstance(record[key], int) or record[key] < 0:
            return False, f"invalid {key}"
    bands = record["bands"]
    if (
        not isinstance(bands, list)
        or len(bands) != 2
        or not all(isinstance(value, int) for value in bands)
        or bands[0] < 0
        or bands[1] < bands[0]
    ):
        return False, "invalid bands"
    for key in ("input_fingerprint", "config_version", "product_digest"):
        if not isinstance(record[key], str) or len(record[key]) != 64:
            return False, f"invalid {key}"
    if record["round"] is not None and (not isinstance(record["round"], int) or record["round"] < 0):
        return False, "invalid round"
    if not isinstance(record["stage_complete"], bool):
        return False, "invalid stage_complete"
    checksum = record["checksum"]
    if not isinstance(checksum, str):
        return False, "invalid checksum"
    if digest_json(_record_without_checksum(record)) != checksum:
        return False, "checksum mismatch"
    return True, None


class CheckpointStore:
    """A real advisory-lock protected, append-only checkpoint directory."""

    def __init__(self, state_root: str | Path):
        self.state_root = Path(state_root)
        self.directory = self.state_root / "checkpoints"
        self.lock_path = self.state_root / "checkpoints.lock"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.touch(exist_ok=True)

    @contextlib.contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        with open(self.lock_path, "a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _record_candidates(self) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
        by_sequence: dict[int, list[tuple[Path, dict[str, Any]]]] = {}
        diagnostics: list[dict[str, Any]] = []
        for path in sorted(self.directory.glob("*.json")):
            match = CHECKPOINT_FILENAME.match(path.name)
            if not match:
                diagnostics.append({"file": path.name, "reason": "non-numeric filename"})
                continue
            sequence = int(match.group("sequence"))
            try:
                record = read_json(path)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                diagnostics.append({"file": path.name, "sequence": sequence, "reason": f"unreadable: {exc}"})
                continue
            valid, reason = _validate_record(record, filename_sequence=sequence)
            if not valid:
                diagnostics.append({"file": path.name, "sequence": sequence, "reason": reason})
                continue
            by_sequence.setdefault(sequence, []).append((path, dict(record)))
        unique: dict[int, dict[str, Any]] = {}
        for sequence, records in by_sequence.items():
            if len(records) == 1:
                unique[sequence] = records[0][1]
            else:
                canonical = self.directory / f"{sequence:012d}.json"
                canonical_records = [record for path, record in records if path.name == canonical.name]
                first_payload = records[0][1]
                identical = all(record.get("checksum") == first_payload.get("checksum") for _, record in records)
                if len(canonical_records) == 1 and identical:
                    unique[sequence] = canonical_records[0]
                    diagnostics.append(
                        {
                            "sequence": sequence,
                            "reason": "duplicate copies ignored; canonical record retained",
                            "count": len(records),
                        }
                    )
                else:
                    diagnostics.append(
                        {
                            "sequence": sequence,
                            "reason": "conflicting duplicate sequence; all copies ignored",
                            "count": len(records),
                        }
                    )
        return unique, diagnostics

    def scan(self) -> dict[str, Any]:
        """Return unique valid records, diagnostics, and the contiguous recoverable prefix."""
        with self._exclusive_lock():
            unique, diagnostics = self._record_candidates()
        records = [unique[sequence] for sequence in sorted(unique)]
        prefix: list[dict[str, Any]] = []
        expected = 1
        for record in records:
            if record["sequence"] != expected:
                break
            prefix.append(record)
            expected += 1
        return {
            "records": records,
            "recoverable_prefix": prefix,
            "diagnostics": diagnostics,
            "latest_sequence": max(unique) if unique else 0,
            "recoverable_sequence": len(prefix),
        }

    def list_records(self, *, slice_id: str | None = None, stage: str | None = None) -> list[dict[str, Any]]:
        records = self.scan()["records"]
        if slice_id is not None:
            records = [record for record in records if record["slice_id"] == slice_id]
        if stage is not None:
            records = [record for record in records if record["stage"] == stage]
        return records

    @staticmethod
    def _record_key(record: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            record["slice_id"],
            record["stage"],
            record["block_index"],
            record["round"],
        )

    def publish(
        self,
        record: Mapping[str, Any],
        *,
        sequence: int | None = None,
        block_sequence: int | None = None,
        reject_duplicate_product: bool = True,
    ) -> dict[str, Any]:
        """Publish exactly the next sequence under an advisory file lock.

        A caller-supplied stale sequence, stale per-block sequence, or exact
        duplicate product for the same block/round raises E_STALE_CHECKPOINT.
        """
        base = {key: value for key, value in record.items() if key not in {"sequence", "block_sequence", "checksum"}}
        for required in (
            "slice_id",
            "stage",
            "block_index",
            "frame_start",
            "frame_end",
            "bands",
            "input_fingerprint",
            "config_version",
            "product_digest",
        ):
            if required not in base:
                raise ValueError(f"checkpoint record missing {required}")
        base.setdefault("schema_version", 1)
        base.setdefault("round", None)
        base.setdefault("stage_complete", True)
        if base["stage"] not in STAGES:
            raise ValueError(f"unknown checkpoint stage: {base['stage']}")
        with self._exclusive_lock():
            unique, _ = self._record_candidates()
            latest = max(unique) if unique else 0
            expected_sequence = 1
            while expected_sequence in unique:
                expected_sequence += 1
            if sequence is not None and int(sequence) != expected_sequence:
                raise stale_checkpoint(
                    "checkpoint sequence must advance the recoverable prefix by exactly one",
                    expected_sequence=expected_sequence,
                    provided_sequence=int(sequence),
                    latest_sequence=latest,
                )
            provisional = dict(base)
            provisional["sequence"] = expected_sequence
            provisional["block_sequence"] = 0
            key = self._record_key(provisional)
            same_key = [item for item in unique.values() if self._record_key(item) == key]
            expected_block_sequence = 1 + max((int(item["block_sequence"]) for item in same_key), default=0)
            if block_sequence is not None and int(block_sequence) != expected_block_sequence:
                raise stale_checkpoint(
                    "block checkpoint sequence is stale or duplicated",
                    expected_block_sequence=expected_block_sequence,
                    provided_block_sequence=int(block_sequence),
                )
            if reject_duplicate_product and same_key:
                latest_same = max(same_key, key=lambda item: int(item["sequence"]))
                if (
                    latest_same["product_digest"] == provisional["product_digest"]
                    and latest_same["input_fingerprint"] == provisional["input_fingerprint"]
                ):
                    raise stale_checkpoint(
                        "duplicate checkpoint product for the same block and stage",
                        block_index=provisional["block_index"],
                        stage=provisional["stage"],
                        product_digest=provisional["product_digest"],
                    )
            provisional["block_sequence"] = expected_block_sequence
            provisional["checksum"] = digest_json(provisional)
            filename = f"{expected_sequence:012d}.json"
            target = self.directory / filename
            if target.exists() and expected_sequence in unique:
                raise stale_checkpoint(
                    "checkpoint file already exists",
                    sequence=expected_sequence,
                    file=str(target),
                )
            atomic_write_json(target, provisional)
            return provisional

    def publish_unit(
        self,
        *,
        slice_id: str,
        stage: str,
        block_index: int,
        frame_start: int,
        frame_end: int,
        bands: tuple[int, int],
        input_fingerprint: str,
        config_version: str,
        product_digest: str,
        round_index: int | None = None,
        stage_complete: bool = True,
        sequence: int | None = None,
        block_sequence: int | None = None,
        reject_duplicate_product: bool = True,
    ) -> dict[str, Any]:
        return self.publish(
            {
                "schema_version": 1,
                "slice_id": slice_id,
                "stage": stage,
                "block_index": int(block_index),
                "frame_start": int(frame_start),
                "frame_end": int(frame_end),
                "bands": [int(bands[0]), int(bands[1])],
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "product_digest": product_digest,
                "round": round_index,
                "stage_complete": bool(stage_complete),
            },
            sequence=sequence,
            block_sequence=block_sequence,
            reject_duplicate_product=reject_duplicate_product,
        )

    def clear(self) -> None:
        with self._exclusive_lock():
            for path in self.directory.glob("*.json"):
                path.unlink()


def checkpoint_status(store: CheckpointStore, *, slice_id: str | None = None) -> dict[str, Any]:
    scan = store.scan()
    records = scan["records"]
    if slice_id is not None:
        records = [record for record in records if record["slice_id"] == slice_id]
    return {
        "count": len(records),
        "latest_sequence": scan["latest_sequence"],
        "recoverable_sequence": scan["recoverable_sequence"],
        "diagnostics": scan["diagnostics"],
        "records": records,
    }

