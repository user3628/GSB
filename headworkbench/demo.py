from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest_object
from .config import load_config, current_config_path
from .data import load_dataset
from .errors import WorkbenchError
from .generator import generate_demo
from .pipeline import Pipeline, compare_runs
from .service import WorkbenchService
from .versioning import VersionStore


def reset_versions(root: Path) -> None:
    workbench = root / ".workbench"
    if (workbench / "versions").exists():
        shutil.rmtree(workbench / "versions")
    (workbench / "HEAD.json").unlink(missing_ok=True)
    (workbench / "version.lock").unlink(missing_ok=True)


def reset_stage_cache(root: Path) -> None:
    stage_cache = root / ".workbench" / "stage_cache"
    if stage_cache.exists():
        shutil.rmtree(stage_cache)


def _run_summary(result: dict[str, Any]) -> dict[str, Any]:
    payload = result["payload"]
    allocation = payload["allocation"]
    return {
        "mode": result["mode"],
        "window_name": result["window_name"],
        "result_digest": result["result_digest"],
        "pipeline_fingerprint": payload["pipeline_fingerprint"],
        "stage_digests": {name: entry["digest"] for name, entry in payload["stages"].items()},
        "recomputed_stages": result["recomputed_stages"],
        "cache_hits": result["cache_hits"],
        "ranking": payload["adjudication"]["ranking"],
        "error_upper_bounds": {
            str(head["head_id"]): head["error_upper_bound"]
            for head in payload["adjudication"]["heads_by_id"]
        },
        "sampling_intervals": {
            str(head["head_id"]): head["sample_interval"]
            for head in payload["adjudication"]["heads_by_id"]
        },
        "suggested_head_widths": allocation["suggested_head_widths"],
        "total_error_estimate": allocation["total_error_estimate"],
        "global_error_budget": allocation["global_error_budget"],
        "budget_remaining": allocation["budget_remaining"],
        "budget_limited_head_ids": allocation["budget_limited_head_ids"],
        "downgraded_head_ids": allocation["downgraded_head_ids"],
        "allocation_diagnostics": allocation["diagnostics"],
    }


def create_mismatch_fixture(root: Path) -> tuple[Path, dict[str, Any]]:
    """Create a deterministic shard whose query projection is one dimension short."""

    import numpy as np

    dataset = load_dataset(root)
    manifest = dataset.manifest
    fixture_dir = root / ".workbench" / "invalid_mismatch"
    shard_dir = fixture_dir / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    source_manifest_path = dataset.manifest_path
    for entry in manifest["shards"]:
        source = source_manifest_path.parent / entry["shard_file"]
        target = shard_dir / Path(entry["shard_file"]).name
        if source.resolve() != target.resolve():
            shutil.copy2(source, target)
    selected_shard = manifest["shards"][0]["shard_file"]
    source_shard = source_manifest_path.parent / selected_shard
    with np.load(source_shard, allow_pickle=False) as npz:
        arrays = {name: np.asarray(npz[name]) for name in npz.files}
    bad_query = arrays["query_proj"]
    if bad_query.ndim != 3 or bad_query.shape[1] < 2:
        raise RuntimeError("fixture source projection cannot be shortened")
    arrays["query_proj"] = bad_query[:, :-1, :]
    bad_name = "shard_mismatch.npz"
    bad_path = shard_dir / bad_name
    np.savez_compressed(bad_path, **arrays)
    from .canonical import digest_file

    bad_digest = digest_file(bad_path)
    copied = json.loads(json.dumps(manifest))
    copied["shards"][0]["shard_file"] = f"shards/{bad_name}"
    copied["shards"][0]["sha256"] = bad_digest
    copied["shards"][0]["query_proj_shape"] = list(arrays["query_proj"].shape)
    copied["heads"][0]["shard_file"] = f"shards/{bad_name}"
    copied.pop("manifest_digest", None)
    from .canonical import atomic_write_json as write_json

    copied["manifest_digest"] = digest_object(copied)
    bad_manifest = fixture_dir / "manifest.json"
    write_json(bad_manifest, copied)
    expected_error: BaseException | None = None
    try:
        load_dataset(root, bad_manifest)
    except WorkbenchError as exc:
        expected_error = exc
    if expected_error is None or expected_error.code != "E_HEAD_MISMATCH":
        raise RuntimeError("mismatch fixture did not trigger E_HEAD_MISMATCH")
    return bad_manifest, expected_error.as_dict()


def run_demo(root: Path, *, force_generate: bool = True) -> dict[str, Any]:
    root = root.resolve()
    generation = generate_demo(root, force=force_generate)
    store = VersionStore(root)
    layout = store.ensure_layout()
    reset_versions(root)
    reset_stage_cache(root)
    service = WorkbenchService(root)
    config = load_config(current_config_path(root))
    pipeline = Pipeline(root, config)

    compact_incremental = pipeline.run("compact", mode="incremental")
    compact_full = pipeline.run("compact", mode="full")
    compact_consistency = compare_runs(compact_incremental, compact_full)

    window_expansion = pipeline.run("full", mode="incremental")
    full_from_scratch = pipeline.run("full", mode="full")
    full_consistency = compare_runs(window_expansion, full_from_scratch)

    compact_allocation = compact_incremental["payload"]["allocation"]
    first_commit = store.commit(
        pipeline,
        compact_allocation["suggested_head_widths"],
        expected_base_version=0,
        window_name="compact",
        adjudication_digest=compact_incremental["payload"]["stages"]["adjudication"]["digest"],
        note="demo active compact-window composition",
        verify_full=True,
    )

    minimum_bits = str(min(config.candidate_bitwidths))
    over_budget_widths = {str(head.head_id): int(minimum_bits) for head in pipeline.dataset.heads}
    rollback_commit = store.commit(
        pipeline,
        over_budget_widths,
        expected_base_version=1,
        window_name="compact",
        note="intentional over-budget composition for atomic rollback",
        verify_full=True,
    )
    widths_after_rollback = service.width_query("compact", query_id="demo-width-query")
    history_after_rollback = store.history()
    rollback_records = store.rollback_records()

    stale_error: dict[str, Any] | None = None
    try:
        store.commit(
            pipeline,
            compact_allocation["suggested_head_widths"],
            expected_base_version=1,
            window_name="compact",
            verify_full=False,
        )
    except WorkbenchError as exc:
        stale_error = exc.as_dict()

    full_allocation = window_expansion["payload"]["allocation"]
    full_commit = store.commit(
        pipeline,
        full_allocation["suggested_head_widths"],
        expected_base_version=2,
        window_name="full",
        adjudication_digest=window_expansion["payload"]["stages"]["adjudication"]["digest"],
        note="demo full-window expansion using incremental reference cache",
        verify_full=True,
    )
    widths_after_full = service.width_query("full", query_id="demo-full-width-query")
    final_history = store.history()

    mismatch_manifest, mismatch_error = create_mismatch_fixture(root)
    try:
        # The HTTP import endpoint performs this same validation before publishing a pointer.
        from fastapi.testclient import TestClient
        from .service import create_app

        client = TestClient(create_app(root))
        response = client.post(
            "/api/import",
            json={"manifest_path": str(mismatch_manifest.relative_to(root))},
        )
        mismatch_http = {"status_code": response.status_code, "body": response.json()}
    except Exception as exc:
        mismatch_http = {"status_code": None, "body": {"unexpected_exception": repr(exc)}}

    report: dict[str, Any] = {
        "schema": "headworkbench.demo-report/v1",
        "python": __import__("sys").version.split()[0],
        "root": str(root),
        "generation": {
            "manifest_digest": generation["manifest_digest"],
            "config_digest": generation["config_digest"],
            "config_version": generation["config_version"],
            "shard_count": generation["shard_count"],
            "head_count": generation["head_count"],
            "sampling_windows": generation["sampling_windows"],
        },
        "workspace_layout": layout,
        "compact": _run_summary(compact_incremental),
        "compact_incremental_vs_full": compact_consistency,
        "window_expansion": _run_summary(window_expansion),
        "full_incremental_vs_full_from_scratch": full_consistency,
        "commits": {
            "initial_active": first_commit,
            "over_budget_attempt": rollback_commit,
            "full_window_active": full_commit,
        },
        "rollback_query": {
            "version": widths_after_rollback["version"],
            "status": widths_after_rollback["version_status"],
            "head_widths": widths_after_rollback["head_widths"],
            "attempted_head_widths": widths_after_rollback["attempted_head_widths"],
            "consistency": widths_after_rollback["consistency"],
            "query": widths_after_rollback["query"],
        },
        "full_window_query": {
            "version": widths_after_full["version"],
            "status": widths_after_full["version_status"],
            "head_widths": widths_after_full["head_widths"],
            "consistency": widths_after_full["consistency"],
            "query": widths_after_full["query"],
        },
        "history_after_rollback": history_after_rollback,
        "rollback_records": rollback_records,
        "stale_commit_error": stale_error,
        "head_mismatch": {
            "fixture_manifest": str(mismatch_manifest.relative_to(root)),
            "direct_error": mismatch_error,
            "http_response": mismatch_http,
        },
        "final_history": final_history,
        "fixed_summary_digest": digest_object(
            {
                "manifest_digest": generation["manifest_digest"],
                "config_digest": generation["config_digest"],
                "compact_result_digest": compact_incremental["result_digest"],
                "full_result_digest": window_expansion["result_digest"],
                "active_widths": full_commit["effective_head_widths"],
                "final_version": final_history["current_version"],
                "rollback_version": rollback_commit["version"],
            }
        ),
    }
    atomic_write_json(root / "artifacts" / "demo_report.json", report)
    return report

