from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .canonical import (
    PipelineError,
    atomic_write_json,
    exclusive_file_lock,
    now_iso,
    read_json,
)


_CHECKPOINT_RE = re.compile(r"^(?P<sequence>\d{20})\.json$")


class CheckpointStore:
    """Atomic per-stage checkpoints guarded by a real fcntl flock."""

    def __init__(self, run_dir: Path | str) -> None:
        self.run_dir = Path(run_dir)
        self.directory = self.run_dir / "checkpoints"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.directory / ".publish.lock"

    def _scan(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(self.directory.iterdir()):
            match = _CHECKPOINT_RE.match(path.name)
            if not match:
                continue
            record = read_json(path)
            if isinstance(record, dict):
                records.append(record)
        records.sort(key=lambda item: int(item["sequence"]))
        return records

    def list(self) -> list[dict[str, Any]]:
        return self._scan()

    def last_sequence(self) -> int:
        records = self._scan()
        return int(records[-1]["sequence"]) if records else 0

    def publish(
        self,
        *,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        sequence: int | None = None,
    ) -> dict[str, Any]:
        if not stage or not input_fingerprint or not config_version or not artifact_digest:
            raise PipelineError(
                "E_INVALID_CHECKPOINT",
                "阶段检查点缺少必要字段。",
                {
                    "stage": stage,
                    "input_fingerprint": input_fingerprint,
                    "config_version": config_version,
                    "artifact_digest": artifact_digest,
                },
            )
        with exclusive_file_lock(self.lock_path, blocking=True):
            last = self.last_sequence()
            if sequence is None:
                next_sequence = last + 1
            else:
                if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
                    raise PipelineError(
                        "E_INVALID_CHECKPOINT",
                        "检查点序号必须是正整数。",
                        {"sequence": sequence},
                    )
                if sequence <= last:
                    raise PipelineError(
                        "E_STALE_CHECKPOINT",
                        "检查点序号落后或重复，拒绝发布。",
                        {"last_sequence": last, "provided_sequence": sequence},
                    )
                next_sequence = sequence

            record = {
                "sequence": next_sequence,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "published_at": now_iso(),
            }
            target = self.directory / f"{next_sequence:020d}.json"
            atomic_write_json(target, record)
            return record
