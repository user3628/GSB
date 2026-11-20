"""Atomic stage checkpoints, monotonic sequencing and stale-publish guards."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from .errors import invalid_stage, stale_checkpoint
from .utils import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    file_lock,
    read_json,
    relpath_str,
    sha256_bytes,
    sha256_text,
)


STAGE_ORDER = {"frontend": 0, "acoustic": 1, "waveform": 2, "validation": 3}
_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe(value: str) -> str:
    cleaned = _SAFE_RE.sub("_", value).strip("._")
    if not cleaned:
        cleaned = "x"
    if len(cleaned) > 80:
        cleaned = cleaned[:64] + "_" + sha256_text(value)[:12]
    return cleaned


def record_key(utt_id: str, stage: str, chunk_index: int) -> str:
    return f"{utt_id}:{stage}:{chunk_index:06d}"


class StageStore:
    def __init__(self, *, workspace: Path, run_id: str, config_version: str | None = None):
        self.workspace = workspace
        self.run_id = run_id
        self.config_version = config_version
        self.root = workspace / "runtime" / run_id
        self.checkpoint_dir = self.root / "checkpoints"
        self.records_dir = self.checkpoint_dir / "records"
        self.products_dir = self.checkpoint_dir / "products"
        self.ledger_path = self.checkpoint_dir / "ledger.json"
        self.lock_path = self.checkpoint_dir / ".publish.lock"
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.products_dir.mkdir(parents=True, exist_ok=True)

    def _empty_ledger(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "next_seq": 0,
            "records": {},
            "last_key": None,
            "utt_order": [],
            "invalidations": [],
        }

    def _load_ledger(self) -> dict[str, Any]:
        ledger = read_json(self.ledger_path)
        if ledger is None:
            return self._empty_ledger()
        if ledger.get("schema_version") != 1:
            raise invalid_stage("unsupported checkpoint ledger schema", run_id=self.run_id)
        return ledger

    def ensure_utt_order(self, utt_ids: Iterable[str]) -> None:
        order = list(dict.fromkeys(utt_ids))
        with file_lock(self.lock_path):
            ledger = self._load_ledger()
            if not ledger["records"]:
                ledger["utt_order"] = order
            else:
                existing = list(ledger.get("utt_order", []))
                if existing != order:
                    # Preserve already recorded utterance ordering; append genuinely new ids.
                    ledger["utt_order"] = existing + [item for item in order if item not in existing]
            atomic_write_json(self.ledger_path, ledger)

    def _record_path(self, key: str) -> Path:
        return self.records_dir / f"{sha256_text(key)[:24]}.json"

    def _product_path(self, utt_id: str, stage: str, chunk_index: int, suffix: str) -> Path:
        return (
            self.products_dir
            / _safe(utt_id)
            / stage
            / f"{chunk_index:06d}.{suffix.lstrip('.')}"
        )

    def _record_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "schema_version": 1,
            "run_id": self.run_id,
            "key": record["key"],
            "seq": int(record["seq"]),
            "monotonic_sequence": int(record["seq"]),
            "utt_id": record["utt_id"],
            "stage": record["stage"],
            "chunk_index": int(record["chunk_index"]),
            "stage_total_chunks": int(record["stage_total_chunks"]),
            "fingerprint": record["fingerprint"],
            "product_path": record["product_path"],
            "product_digest": record["product_digest"],
            "prerequisites": list(record.get("prerequisites", [])),
        }
        for key, value in record.items():
            if key not in payload:
                payload[key] = value
        return payload

    def _validate_publish(
        self,
        *,
        ledger: dict[str, Any],
        record: dict[str, Any],
        prerequisites: list[str],
        expected_seq: int | None,
    ) -> None:
        key = record["key"]
        key_to_seq = ledger["records"]
        next_seq = int(ledger["next_seq"])
        if expected_seq is not None and int(expected_seq) != next_seq:
            raise stale_checkpoint(
                "checkpoint sequence is behind or ahead of the ledger",
                expected_seq=int(expected_seq),
                ledger_next_seq=next_seq,
                key=key,
            )
        if int(record["seq"]) != next_seq:
            raise stale_checkpoint(
                "checkpoint sequence is not continuous",
                supplied_seq=int(record["seq"]),
                ledger_next_seq=next_seq,
                key=key,
            )
        if key in key_to_seq:
            raise stale_checkpoint("duplicate checkpoint publication", key=key)
        missing = [item for item in prerequisites if item not in key_to_seq]
        if missing:
            raise invalid_stage(
                "required upstream checkpoint product is missing",
                key=key,
                missing_prerequisites=missing,
            )
        for prerequisite in prerequisites:
            raw = read_json(self._record_path(prerequisite))
            if raw is None:
                raise invalid_stage(
                    "required upstream checkpoint record is unreadable",
                    key=key,
                    missing_prerequisite=prerequisite,
                )
            product_path = self.workspace / raw["product_path"]
            if not product_path.exists():
                raise invalid_stage(
                    "required upstream checkpoint product is missing",
                    key=key,
                    missing_prerequisite=prerequisite,
                    product_path=raw["product_path"],
                )
            if sha256_bytes(product_path.read_bytes()) != raw["product_digest"]:
                raise invalid_stage(
                    "required upstream checkpoint product digest mismatch",
                    key=key,
                    invalid_prerequisite=prerequisite,
                )

        utt_id = record["utt_id"]
        stage = record["stage"]
        chunk_index = int(record["chunk_index"])
        if stage not in STAGE_ORDER:
            raise invalid_stage("unknown checkpoint stage", stage=stage, key=key)

        if stage != "frontend":
            prior_stage = {1: "frontend", 2: "acoustic", 3: "waveform"}[STAGE_ORDER[stage]]
            prior_prefix = f"{utt_id}:{prior_stage}:"
            if not any(item.startswith(prior_prefix) for item in key_to_seq):
                raise invalid_stage(
                    "checkpoint cannot be published before its upstream stage",
                    key=key,
                    missing_stage=prior_stage,
                )

        if stage == "frontend" and chunk_index > 0:
            previous = record_key(utt_id, "frontend", chunk_index - 1)
            if previous not in key_to_seq:
                raise invalid_stage(
                    "frontend chunks must be published in order",
                    key=key,
                    missing_prerequisite=previous,
                )

        if stage == "validation" and chunk_index != 0:
            raise invalid_stage("validation records must use chunk_index 0", key=key)

        if stage == "frontend" and chunk_index == 0:
            utt_order = list(ledger.get("utt_order", []))
            if utt_id in utt_order:
                position = utt_order.index(utt_id)
                for earlier in utt_order[:position]:
                    if not any(item.startswith(f"{earlier}:validation:") for item in key_to_seq):
                        raise stale_checkpoint(
                            "cross-utterance publication order is stale",
                            key=key,
                            earlier_utterance=earlier,
                            reason="earlier utterance has no validation checkpoint",
                        )

    def publish_bytes(
        self,
        *,
        utt_id: str,
        stage: str,
        chunk_index: int,
        stage_total_chunks: int,
        fingerprint: str,
        product: bytes,
        suffix: str,
        prerequisites: Iterable[str] = (),
        expected_seq: int | None = None,
        extra_record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_ORDER:
            raise invalid_stage("unknown checkpoint stage", stage=stage)
        key = record_key(utt_id, stage, chunk_index)
        prereq = list(prerequisites)
        with file_lock(self.lock_path):
            ledger = self._load_ledger()
            seq = int(ledger["next_seq"])
            product_path = self._product_path(utt_id, stage, chunk_index, suffix)
            product_rel = relpath_str(product_path, self.workspace)
            record = {
                "key": key,
                "seq": seq,
                "utt_id": utt_id,
                "stage": stage,
                "chunk_index": int(chunk_index),
                "stage_total_chunks": int(stage_total_chunks),
                "fingerprint": fingerprint,
                "product_path": product_rel,
                "product_digest": sha256_bytes(product),
                "prerequisites": prereq,
            }
            if self.config_version:
                record["config_version"] = self.config_version
            if extra_record:
                record.update(extra_record)
            self._validate_publish(
                ledger=ledger, record=record, prerequisites=prereq, expected_seq=expected_seq
            )
            atomic_write_bytes(product_path, product)
            payload = self._record_payload(record)
            atomic_write_json(self._record_path(key), payload)
            ledger["records"][key] = seq
            ledger["next_seq"] = seq + 1
            ledger["last_key"] = key
            atomic_write_json(self.ledger_path, ledger)
            return payload

    def publish_json(
        self,
        *,
        utt_id: str,
        stage: str,
        chunk_index: int,
        stage_total_chunks: int,
        fingerprint: str,
        value: Any,
        prerequisites: Iterable[str] = (),
        expected_seq: int | None = None,
        extra_record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.publish_bytes(
            utt_id=utt_id,
            stage=stage,
            chunk_index=chunk_index,
            stage_total_chunks=stage_total_chunks,
            fingerprint=fingerprint,
            product=canonical_json(value).encode("utf-8"),
            suffix="json",
            prerequisites=prerequisites,
            expected_seq=expected_seq,
            extra_record=extra_record,
        )

    def get_record(self, utt_id: str, stage: str, chunk_index: int) -> dict[str, Any] | None:
        ledger = self._load_ledger()
        key = record_key(utt_id, stage, chunk_index)
        if key not in ledger["records"]:
            return None
        return read_json(self._record_path(key))

    def get_records(self, utt_id: str, stage: str) -> list[dict[str, Any]]:
        ledger = self._load_ledger()
        records = []
        prefix = f"{utt_id}:{stage}:"
        for key in ledger["records"]:
            if key.startswith(prefix):
                raw = read_json(self._record_path(key))
                if raw is not None:
                    records.append(raw)
        records.sort(key=lambda item: int(item["chunk_index"]))
        return records

    def read_product(self, record: dict[str, Any]) -> bytes:
        path = self.workspace / record["product_path"]
        if not path.exists():
            raise invalid_stage(
                "checkpoint product file is missing",
                key=record.get("key"),
                product_path=record.get("product_path"),
            )
        data = path.read_bytes()
        digest = sha256_bytes(data)
        if digest != record["product_digest"]:
            raise invalid_stage(
                "checkpoint product digest mismatch",
                key=record.get("key"),
                expected=record["product_digest"],
                actual=digest,
            )
        return data

    def all_records_for_utt(self, utt_id: str) -> list[dict[str, Any]]:
        ledger = self._load_ledger()
        result = []
        for key in ledger["records"]:
            if key.startswith(f"{utt_id}:"):
                raw = read_json(self._record_path(key))
                if raw is not None:
                    result.append(raw)
        return result

    def invalidate_from(self, *, utt_id: str, stage: str, reason: str) -> list[str]:
        rank = STAGE_ORDER[stage]
        removed: list[str] = []
        with file_lock(self.lock_path):
            ledger = self._load_ledger()
            for key in list(ledger["records"].keys()):
                if not key.startswith(f"{utt_id}:"):
                    continue
                raw = read_json(self._record_path(key))
                if raw is None:
                    continue
                if STAGE_ORDER[raw["stage"]] < rank:
                    continue
                product = self.workspace / raw["product_path"]
                product.unlink(missing_ok=True)
                self._record_path(key).unlink(missing_ok=True)
                del ledger["records"][key]
                removed.append(key)
            ledger["invalidations"].append(
                {
                    "utt_id": utt_id,
                    "from_stage": stage,
                    "reason": reason,
                    "removed_keys": removed,
                }
            )
            atomic_write_json(self.ledger_path, ledger)
        return removed

    def ledger(self) -> dict[str, Any]:
        return self._load_ledger()


def product_key(utt_id: str, stage: str, chunk_index: int) -> str:
    return record_key(utt_id, stage, chunk_index)
