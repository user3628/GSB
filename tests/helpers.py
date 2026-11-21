from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from prep_pipeline.config import Settings
from prep_pipeline.models import BatchSubmitRequest, InjectionOptions
from prep_pipeline.policy import PolicySnapshot, parse_policy_snapshot


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


def make_settings(tmp_path: Path, *, stage_delay: float = 0.0) -> Settings:
    artifact_dir = tmp_path / "artifacts"
    return Settings(
        root=tmp_path,
        data_dir=DATA_DIR,
        artifact_dir=artifact_dir,
        db_path=artifact_dir / "pipeline.sqlite3",
        staging_dir=artifact_dir / "staging",
        published_dir=artifact_dir / "batches",
        verification_dir=artifact_dir / "verification",
        test_stage_delay_seconds=stage_delay,
        poll_interval_seconds=0.01,
        batch_wait_timeout_seconds=30.0,
        policy_lock_timeout_seconds=30.0,
    )


def load_policy_dict(name: str = "policy_v1.json") -> dict[str, Any]:
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


def load_policy(name: str = "policy_v1.json") -> PolicySnapshot:
    return parse_policy_snapshot(load_policy_dict(name))


def policy_variant(
    *,
    version: int = 2,
    base_version: int = 1,
    generated_at: str = "2026-03-01T00:00:00Z",
    seed: int = 2001,
) -> PolicySnapshot:
    value = load_policy_dict("policy_v1.json")
    value.update(
        {
            "version": version,
            "base_version": base_version,
            "generated_at": generated_at,
            "seed": seed,
            "checksum": "",
        }
    )
    unsigned = PolicySnapshot.model_validate(value)
    value["checksum"] = unsigned.expected_checksum()
    return parse_policy_snapshot(value)


def batch_request(
    batch_id: str,
    *,
    policy_version: int = 1,
    input_path: str = "records_valid.tsv",
    injections: InjectionOptions | None = None,
) -> BatchSubmitRequest:
    return BatchSubmitRequest(
        batch_id=batch_id,
        policy_version=policy_version,
        input_path=input_path,
        injections=injections or InjectionOptions(),
    )
