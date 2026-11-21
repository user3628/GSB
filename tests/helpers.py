from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from mpc.samples import generate_sample_workspace
from mpc.service import CheckpointService


class ServiceCase:
    """Small composition helper to keep unittest classes independent."""

    def __init__(self) -> None:
        self._temp = TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.samples = self.root / "samples"
        generate_sample_workspace(self.samples, overwrite=True)
        self.db = self.root / "checkpoints.sqlite3"
        self.service = CheckpointService(self.db, self.samples)

    def close(self) -> None:
        self._temp.cleanup()

    def full_policy(
        self,
        *,
        version: int = 1,
        commit_id: str | None = None,
        loss_scale: float = 64.0,
        assignments: dict[str, str] | None = None,
    ) -> dict:
        return self.service.submit_policy_snapshot(
            {
                "commit_id": commit_id or f"policy-{version}",
                "version": version,
                "format_version": 1,
                "loss_scale": loss_scale,
                "loss_scale_initial": loss_scale,
                "growth_factor": 2.0,
                "shards": assignments
                or {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
            }
        )

    def checkpoint(
        self,
        *,
        commit_id: str = "checkpoint-1",
        round_number: int = 1,
        shards: list[str] | None = None,
        **extra,
    ) -> dict:
        body = {
            "commit_id": commit_id,
            "round": round_number,
            "scheduler_position": {"epoch": 0, "step": round_number * 10},
            "data_cursor": {"epoch": 0, "batch": round_number, "sample_offset": round_number * 32},
            "shard_ids": shards or ["s0", "s1", "s2", "s3"],
        }
        body.update(extra)
        return self.service.submit_checkpoint(body)


def write_snapshot(
    path: Path,
    *,
    snapshot_seq: int,
    version: int,
    assignments: dict[str, str] | None = None,
    format_version: int = 1,
    loss_scale: float = 64.0,
) -> Path:
    payload = {
        "format_version": format_version,
        "snapshot_seq": snapshot_seq,
        "generated_at": "2026-11-22T00:00:00+00:00",
        "policy": {
            "version": version,
            "loss_scale": loss_scale,
            "loss_scale_initial": loss_scale,
            "growth_factor": 2.0,
            "shards": assignments
            or {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def digest_map(derived: dict) -> dict[str, str]:
    return {shard["shard_id"]: shard["derived_summary_digest"] for shard in derived["shards"]}
