"""One-command deterministic demonstration and acceptance report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from .artifacts import ArtifactRef
from .attention import compute_scores_and_weights
from .checkpoints import CheckpointStore
from .config import RopeConfig
from .demo_data import DEMO_SEED, generate_demo_assets
from .digest import atomic_write_json, sha256_bytes
from .errors import RopeError
from .memory import plan_scoring
from .session import DiagnosticSession


def run_demo(base_dir: str | Path = ".", *, reset: bool = True) -> dict[str, Any]:
    base = Path(base_dir).resolve()
    generated = generate_demo_assets(base)
    manifest_path = Path(generated["manifest"])
    config_path = base / "configs" / "rope.yaml"
    workspace = base / "artifacts" / "demo_workspace"
    resume_workspace = base / "artifacts" / "demo_resume_workspace"

    session = DiagnosticSession.create(
        workspace, manifest_path, config_path, reset=reset
    )
    initial_status = session.run()
    initial_fingerprints = session.fingerprints()
    initial_verify = session.verify()
    initial_digests = _stage_digests(initial_fingerprints)

    query_records: dict[str, Any] = {}
    for query_type, options in (
        ("angles", {"position": 4, "channel": [0, 1, 6, 7, 8]}),
        ("score_summary", {"batch": 0, "head": 0, "query_position": 6, "top_k": 5}),
        ("weight_stats", {"batch": 0, "head": 0, "query_position": 6, "top_k": 5}),
    ):
        first = session.query(
            shard_id="odd", query_type=query_type, options=options
        )
        second = session.query(
            shard_id="odd", query_type=query_type, options=options
        )
        query_records[query_type] = {
            "first_cache_hit": first["cache_hit"],
            "second_cache_hit": second["cache_hit"],
            "payload_digest": first["payload_digest"],
            "payloads_byte_identical": first["payload_digest"] == second["payload_digest"],
        }

    latency_error = _capture_error(
        lambda: session.query(
            shard_id="long",
            query_type="score_summary",
            options={"batch": 0, "head": 0, "query_position": 10},
            latency_budget_ms=0.0,
            bypass_cache=True,
        )
    )
    invalid_stage_error = _capture_error(
        lambda: session.run(stop_after_stage="illegal_stage")
    )

    incremental_result = session.update_config(
        {"rotation_block_size": 4}, run=True, verify=True
    )
    incremental_fingerprints = session.fingerprints()
    incremental_report = session.recompute_report

    invalid_config_error = _capture_error(
        lambda: RopeConfig.from_yaml(base / "configs" / "invalid_zero_block.yaml")
    )

    def stale_publication() -> None:
        shard = session.shard_by_id["short"]
        ref_dict = session.stage_refs["short"]["scoring_normalization"].to_dict()
        store = CheckpointStore(session.workspace)
        store.publish(
            shard_id="short",
            shard_fingerprint=shard.content_fingerprint,
            config_version=session.config.version,
            stage="frequency_generation",
            sequence=1,
            artifact=ArtifactRef.from_dict(ref_dict),
        )

    stale_checkpoint_error = _capture_error(stale_publication)
    memory_budget_error = _capture_error(
        lambda: plan_scoring(
            session.shard_by_id["long"], RopeConfig(memory_budget_bytes=1)
        )
    )

    numeric_error = _capture_error(
        lambda: _numeric_error_probe()
    )

    resume_session = DiagnosticSession.create(
        resume_workspace, manifest_path, config_path, reset=reset
    )
    stopped_status = resume_session.run(stop_after_stage="frequency_generation")
    stopped_fingerprints = resume_session.fingerprints()
    resumed_status = resume_session.resume()
    resumed_verify = resume_session.verify()
    resumed_fingerprints = resume_session.fingerprints()
    resume_match = _stage_digests(resumed_fingerprints) == initial_digests

    export_result = session.export()
    export_bytes = Path(export_result["path"]).read_bytes()
    export_result["verified_sha256"] = sha256_bytes(export_bytes) == export_result["sha256"]

    checks = {
        "initial_full_run_completed": initial_status["status"] == "completed",
        "initial_full_verify_byte_identical": bool(initial_verify["all_byte_identical"]),
        "odd_head_dim_last_channel_unrotated": _odd_channel_unrotated(session),
        "query_cache_hits_are_byte_identical": all(
            item["payloads_byte_identical"] for item in query_records.values()
        ),
        "incremental_reused_frequency_only": (
            incremental_report.get("earliest_affected_stage") == "block_rotation"
            and incremental_report.get("stage_summary", {})
            .get("frequency_generation", {})
            .get("statuses", {})
            == {"short": "reused", "long": "reused", "odd": "reused"}
        ),
        "incremental_matches_full_recompute": bool(
            incremental_result.get("consistency", {}).get("all_byte_identical")
        ),
        "stop_resume_matches_fresh_full_run": resume_match,
        "stop_was_observable": stopped_status["status"] == "stopped",
        "resume_completed": resumed_status["status"] == "completed",
        "resume_verify_byte_identical": bool(resumed_verify["all_byte_identical"]),
        "export_sha256_verified": export_result["verified_sha256"],
        "latency_budget_error": latency_error["code"] == "E_LATENCY_BUDGET",
        "numeric_error_has_location": (
            numeric_error["code"] == "E_NUMERIC"
            and {"shard_id", "row_block_index", "step_index"}
            <= set(numeric_error.get("details", {}))
        ),
        "invalid_config_error": invalid_config_error["code"] == "E_INVALID_CONFIG",
        "invalid_stage_error": invalid_stage_error["code"] == "E_INVALID_STAGE",
        "stale_checkpoint_error": stale_checkpoint_error["code"] == "E_STALE_CHECKPOINT",
        "memory_budget_error": memory_budget_error["code"] == "E_MEMORY_BUDGET",
    }
    summary = {
        "schema_version": 1,
        "demo": "rope-diag",
        "seed": DEMO_SEED,
        "workspace": str(workspace),
        "manifest": str(manifest_path),
        "initial_full_run": {
            "status": initial_status["status"],
            "config_version": initial_fingerprints["config_version"],
            "elapsed_ms": initial_status["recompute_report"].get("run_elapsed_ms"),
            "stage_artifact_fingerprints": initial_digests,
            "verification": initial_verify,
        },
        "queries": query_records,
        "latency_error": latency_error,
        "incremental": {
            "patch": {"rotation_block_size": 4},
            "earliest_affected_stage": incremental_report.get("earliest_affected_stage"),
            "stage_summary": incremental_report.get("stage_summary"),
            "config_version": incremental_fingerprints["config_version"],
            "stage_artifact_fingerprints": _stage_digests(incremental_fingerprints),
            "consistency": incremental_result.get("consistency"),
        },
        "stop_resume": {
            "stopped_status": stopped_status["status"],
            "stopped_stage_artifact_fingerprints": _stage_digests(stopped_fingerprints),
            "resumed_status": resumed_status["status"],
            "resumed_stage_artifact_fingerprints": _stage_digests(resumed_fingerprints),
            "fresh_run_stage_artifact_fingerprints": initial_digests,
            "matches_fresh_run": resume_match,
            "verification": resumed_verify,
        },
        "error_injections": {
            "invalid_config": invalid_config_error,
            "invalid_stage": invalid_stage_error,
            "stale_checkpoint": stale_checkpoint_error,
            "memory_budget": memory_budget_error,
            "numeric": numeric_error,
            "latency": latency_error,
        },
        "export": export_result,
        "checks": checks,
        "all_checks_passed": all(checks.values()),
    }
    summary_path = base / "artifacts" / "demo_summary.json"
    atomic_write_json(summary_path, summary)
    summary["summary_path"] = str(summary_path)
    return summary


def _stage_digests(fingerprints: dict[str, Any]) -> dict[str, dict[str, str]]:
    return {
        shard_id: dict(shard["stage_artifact_fingerprints"])
        for shard_id, shard in fingerprints["shards"].items()
    }


def _capture_error(callable_: Any) -> dict[str, Any]:
    try:
        callable_()
    except RopeError as exc:
        return exc.to_dict()["error"]
    raise AssertionError("expected a RopeError")


def _odd_channel_unrotated(session: DiagnosticSession) -> bool:
    shard = session.shard_by_id["odd"]
    ref = session.stage_refs["odd"]["frequency_generation"]
    metadata = session.artifacts.metadata(ref) if hasattr(session, "artifacts") else None
    if metadata is None:
        from .artifacts import ArtifactStore

        metadata = ArtifactStore(session.workspace).metadata(ref)
    return metadata.get("unrotated_channels") == [shard.head_dim - 1]


def _numeric_error_probe() -> None:
    config = RopeConfig()
    query = np.zeros((1, 1, 2, 2), dtype=np.float64)
    key = np.zeros_like(query)
    query[0, 0, 0, 0] = np.nan
    compute_scores_and_weights(
        query,
        key,
        config,
        shard_id="numeric_probe",
        row_chunk_size=1,
    )
