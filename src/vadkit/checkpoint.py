"""Atomic checkpoints, generation metadata, cache, and process-safe locking."""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .errors import VadError
from .util import atomic_write_bytes, atomic_write_json, canonical_json, sha256_canonical


CHECKPOINT_SCHEMA = "vadkit.checkpoint.v1"
META_SCHEMA = "vadkit.workspace-meta.v1"
_RECORD_PATTERN = re.compile(r"^(\d{8})\.json$")


def version_fingerprint(input_sha256: str, vad_config_fingerprint: str) -> str:
    return sha256_canonical(
        {
            "schema": "vadkit.chunk-version.v1",
            "input_sha256": input_sha256,
            "vad_config_fingerprint": vad_config_fingerprint,
        }
    )


@dataclass(frozen=True)
class GenerationMeta:
    generation: int
    dataset_fingerprint: str
    vad_config_fingerprint: str
    chunk_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": META_SCHEMA,
            "generation": self.generation,
            "dataset_fingerprint": self.dataset_fingerprint,
            "vad_config_fingerprint": self.vad_config_fingerprint,
            "chunk_count": self.chunk_count,
        }


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.root = Path(workspace)
        self.root.mkdir(parents=True, exist_ok=True)
        self.generations_dir = self.root / "generations"
        self.generations_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.root / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.stop_path = self.root / "STOP"
        self.meta_path = self.root / "meta.json"
        self.lock_path = self.root / ".lock"

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _read_meta_unlocked(self) -> GenerationMeta | None:
        if not self.meta_path.exists():
            return None
        try:
            import json

            data = json.loads(self.meta_path.read_text(encoding="utf-8"))
            return GenerationMeta(
                generation=int(data["generation"]),
                dataset_fingerprint=str(data["dataset_fingerprint"]),
                vad_config_fingerprint=str(data["vad_config_fingerprint"]),
                chunk_count=int(data["chunk_count"]),
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise VadError(
                "E_INVALID_STAGE",
                f"workspace metadata is unreadable: {exc}",
                sample_position=0,
            ) from exc

    def current_meta(self) -> GenerationMeta | None:
        with self.locked():
            return self._read_meta_unlocked()

    def ensure_generation(
        self,
        dataset_fingerprint: str,
        vad_config_fingerprint: str,
        chunk_count: int,
    ) -> GenerationMeta:
        with self.locked():
            meta = self._read_meta_unlocked()
            if (
                meta is not None
                and meta.dataset_fingerprint == dataset_fingerprint
                and meta.vad_config_fingerprint == vad_config_fingerprint
                and meta.chunk_count == chunk_count
            ):
                return meta
            generations = [
                int(match.group(1))
                for path in self.generations_dir.iterdir()
                if path.is_dir() and (match := _RECORD_PATTERN.match(path.name + ".json"))
            ]
            next_generation = max([meta.generation if meta else 0, *generations], default=0) + 1
            meta = GenerationMeta(
                generation=next_generation,
                dataset_fingerprint=dataset_fingerprint,
                vad_config_fingerprint=vad_config_fingerprint,
                chunk_count=chunk_count,
            )
            self._generation_dir(next_generation).mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.meta_path, meta.as_dict())
            return meta

    def _generation_dir(self, generation: int) -> Path:
        return self.generations_dir / f"{generation:08d}"

    def _records_dir(self, generation: int) -> Path:
        return self._generation_dir(generation) / "records"

    def _record_path(self, generation: int, sequence: int) -> Path:
        return self._records_dir(generation) / f"{sequence:08d}.json"

    def _validate_record_payload(self, payload: dict[str, Any], expected_generation: int) -> None:
        stored_hash = str(payload.get("record_sha256", ""))
        body = dict(payload)
        body.pop("record_sha256", None)
        if stored_hash != sha256_canonical(body):
            raise VadError(
                "E_INVALID_STAGE",
                "checkpoint record hash is invalid",
                chunk_id=payload.get("chunk_id"),
                sample_position=0,
            )
        if int(payload.get("generation", -1)) != expected_generation:
            raise VadError(
                "E_INVALID_STAGE",
                "checkpoint generation does not match workspace metadata",
                chunk_id=payload.get("chunk_id"),
                sample_position=0,
            )
        if payload.get("schema") != CHECKPOINT_SCHEMA:
            raise VadError(
                "E_INVALID_STAGE",
                "unsupported checkpoint schema",
                chunk_id=payload.get("chunk_id"),
                sample_position=0,
            )

    def load_records(self, generation: int) -> list[dict[str, Any]]:
        records_dir = self._records_dir(generation)
        if not records_dir.exists():
            return []
        sequence_paths: dict[int, Path] = {}
        for path in records_dir.iterdir():
            match = _RECORD_PATTERN.match(path.name)
            if match:
                sequence_paths[int(match.group(1))] = path
        records: list[dict[str, Any]] = []
        for index, sequence in enumerate(sorted(sequence_paths), start=1):
            if sequence != index:
                raise VadError(
                    "E_INVALID_STAGE",
                    "checkpoint sequence has a missing predecessor",
                    sample_position=0,
                    details={"expected_sequence": index, "found_sequence": sequence},
                )
            path = sequence_paths[sequence]
            try:
                import json

                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise VadError(
                    "E_INVALID_STAGE",
                    f"checkpoint record is unreadable: {exc}",
                    sample_position=0,
                    details={"sequence": sequence},
                ) from exc
            self._validate_record_payload(payload, generation)
            if int(payload.get("sequence", -1)) != sequence:
                raise VadError(
                    "E_INVALID_STAGE",
                    "checkpoint sequence field does not match its filename",
                    sample_position=0,
                    details={"sequence": sequence},
                )
            records.append(payload)
        for expected_order, payload in enumerate(records):
            if int(payload.get("order", -1)) != expected_order:
                raise VadError(
                    "E_INVALID_STAGE",
                    "checkpoint stage order is not contiguous",
                    chunk_id=payload.get("chunk_id"),
                    sample_position=0,
                    details={"expected_order": expected_order, "actual_order": payload.get("order")},
                )
        return records

    def publish(
        self,
        *,
        generation: int,
        sequence: int,
        order: int,
        chunk_id: str,
        input_sha256: str,
        version_fingerprint: str,
        config_fingerprint: str,
        endpoint_summary: dict[str, Any],
    ) -> dict[str, Any]:
        with self.locked():
            meta = self._read_meta_unlocked()
            if meta is None or meta.generation != generation:
                raise VadError(
                    "E_STALE_CHECKPOINT",
                    "workspace generation changed before publication",
                    chunk_id=chunk_id,
                    details={"expected_generation": generation},
                )
            records = self.load_records(generation)
            expected_sequence = len(records) + 1
            if sequence < expected_sequence:
                raise VadError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint publication is stale or duplicated",
                    chunk_id=chunk_id,
                    details={
                        "sequence": sequence,
                        "expected_sequence": expected_sequence,
                    },
                )
            if sequence > expected_sequence:
                raise VadError(
                    "E_INVALID_STAGE",
                    "checkpoint publication skipped a predecessor",
                    chunk_id=chunk_id,
                    details={
                        "sequence": sequence,
                        "expected_sequence": expected_sequence,
                    },
                )
            if order != len(records):
                raise VadError(
                    "E_INVALID_STAGE",
                    "checkpoint stage order is not the next contiguous stage",
                    chunk_id=chunk_id,
                    details={"order": order, "expected_order": len(records)},
                )
            body: dict[str, Any] = {
                "schema": CHECKPOINT_SCHEMA,
                "generation": generation,
                "sequence": sequence,
                "order": order,
                "chunk_id": chunk_id,
                "input_sha256": input_sha256,
                "version_fingerprint": version_fingerprint,
                "config_fingerprint": config_fingerprint,
                "endpoint_summary": endpoint_summary,
            }
            body["record_sha256"] = sha256_canonical(body)
            atomic_write_json(self._record_path(generation, sequence), body)
            return body

    def write_cache(self, version: str, value: dict[str, Any]) -> None:
        body = dict(value)
        body["version_fingerprint"] = version
        body["cache_sha256"] = sha256_canonical(body)
        atomic_write_json(self.cache_dir / f"{version}.json", body)

    def load_cache(self, version: str) -> dict[str, Any] | None:
        path = self.cache_dir / f"{version}.json"
        if not path.exists():
            return None
        try:
            import json

            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        stored_hash = str(body.pop("cache_sha256", ""))
        if stored_hash != sha256_canonical(body):
            return None
        if body.get("version_fingerprint") != version:
            return None
        return body

    def request_stop(self) -> None:
        atomic_write_bytes(self.stop_path, b"stop\n")

    def clear_stop(self) -> None:
        with self.locked():
            try:
                self.stop_path.unlink()
            except FileNotFoundError:
                pass

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def list_generations(self) -> list[int]:
        generations: list[int] = []
        for path in self.generations_dir.iterdir():
            if path.is_dir() and path.name.isdigit():
                generations.append(int(path.name))
        return sorted(generations)

