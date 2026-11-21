"""Byte-level full-vs-incremental and stop-vs-resume verification."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import config_fingerprint, load_config
from .errors import E_INCREMENTAL_MISMATCH, E_NUMERIC, error
from .pipeline import Pipeline
from .search import corpus_diagnostics, run_search
from .util import canonical_bytes, digest_obj, sha256_bytes, write_json_atomic
from .workspace import Workspace


SEARCH_CASES: tuple[dict[str, Any], ...] = (
    {"label": "text-red-blue", "direction": "text_to_image", "query_text": "红色方块 blue gradient"},
    {
        "label": "text-near-duplicate",
        "direction": "text_to_image",
        "query_text": "近似重复 red block blue gradient",
    },
    {"label": "image-item-01", "direction": "image_to_text", "image_item_id": "item-01"},
    {"label": "image-item-04", "direction": "image_to_text", "image_item_id": "item-04"},
)


def _report_payloads(workspace: Workspace, top_k: int) -> dict[str, bytes]:
    payloads: dict[str, bytes] = {}
    for case in SEARCH_CASES:
        kwargs = {key: value for key, value in case.items() if key != "label"}
        report = run_search(workspace, top_k=top_k, **kwargs)
        payloads[case["label"]] = canonical_bytes(report)
    payloads["corpus-diagnostics"] = canonical_bytes(corpus_diagnostics(workspace))
    return payloads


def _first_item_difference(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> str | None:
    left_map = {entry["item_id"]: digest_obj(entry) for entry in left}
    right_map = {entry["item_id"]: digest_obj(entry) for entry in right}
    for item_id in sorted(set(left_map) | set(right_map)):
        if left_map.get(item_id) != right_map.get(item_id):
            return item_id
    return None


def _compare_or_raise(
    *,
    inc_workspace: Workspace,
    full_workspace: Workspace,
    inc_build: dict[str, Any],
    full_build: dict[str, Any],
) -> dict[str, Any]:
    rep_inc = (inc_workspace.representation_summary_path).read_bytes()
    rep_full = (full_workspace.representation_summary_path).read_bytes()
    idx_inc = (inc_workspace.index_summary_path).read_bytes()
    idx_full = (full_workspace.index_summary_path).read_bytes()
    top_k = min(5, int(full_build["item_count"]))
    comparisons = {
        "representation_summary_bytes_equal": rep_inc == rep_full,
        "index_summary_bytes_equal": idx_inc == idx_full,
        "representation_summary_sha256_incremental": sha256_bytes(rep_inc),
        "representation_summary_sha256_full": sha256_bytes(rep_full),
        "index_summary_sha256_incremental": sha256_bytes(idx_inc),
        "index_summary_sha256_full": sha256_bytes(idx_full),
    }
    if rep_inc != rep_full:
        stage = "representation"
        from .util import read_json

        first_item = _first_item_difference(
            read_json(inc_workspace.representation_summary_path).get("items", []),
            read_json(full_workspace.representation_summary_path).get("items", []),
        )
        raise error(
            E_INCREMENTAL_MISMATCH,
            "Incremental representation summary differs from a from-scratch build",
            stage=stage,
            item_id=first_item,
            details={"first_difference_item_id": first_item, **comparisons},
        )
    reports_inc = _report_payloads(inc_workspace, top_k)
    reports_full = _report_payloads(full_workspace, top_k)
    comparisons["search_reports_bytes_equal"] = reports_inc == reports_full
    comparisons["search_report_sha256_incremental"] = {
        key: sha256_bytes(value) for key, value in sorted(reports_inc.items())
    }
    comparisons["search_report_sha256_full"] = {
        key: sha256_bytes(value) for key, value in sorted(reports_full.items())
    }
    if idx_inc != idx_full:
        from .util import read_json

        first_item = _first_item_difference(
            read_json(inc_workspace.index_summary_path).get("dense", []),
            read_json(full_workspace.index_summary_path).get("dense", []),
        )
        raise error(
            E_INCREMENTAL_MISMATCH,
            "Incremental index summary differs from a from-scratch build",
            stage="index",
            item_id=first_item,
            details={"first_difference_item_id": first_item, **comparisons},
        )
    if reports_inc != reports_full:
        first_label = next(
            (label for label in sorted(reports_inc) if reports_inc[label] != reports_full.get(label)),
            None,
        )
        raise error(
            E_INCREMENTAL_MISMATCH,
            "Incremental retrieval report differs from a from-scratch build",
            stage="retrieval",
            item_id=first_label,
            details={"first_difference_item_id": first_label, **comparisons},
        )
    return comparisons


def verify_incremental(
    *,
    parent_root: str | Path,
    manifest_path: str | Path,
    project_root: str | Path = ".",
    baseline_config: str = "baseline_8bit",
    changed_config: str = "compact_4bit",
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    parent = Path(parent_root)
    inc_root = parent / "incremental"
    full_root = parent / "full"
    inc = Workspace(inc_root, project_root=project_root)
    full = Workspace(full_root, project_root=project_root)
    inc.clean()
    full.clean()
    inc.import_manifest(manifest_path)
    full.import_manifest(manifest_path)
    inc.set_config(baseline_config)
    baseline_build = Pipeline(inc).build(mode="full")
    inc.set_config(changed_config)
    incremental_build = Pipeline(inc).build(mode="incremental")
    full.set_config(changed_config)
    full_build = Pipeline(full).build(mode="full")
    expected_reuse = "feature_extract" not in incremental_build.get("reused_stages", [])
    if incremental_build.get("earliest_stage") != "project_quantize":
        raise error(
            E_INCREMENTAL_MISMATCH,
            "A quantization-only change did not invalidate exactly the projection/quantization stage",
            stage="incremental_detection",
            details={
                "earliest_stage": incremental_build.get("earliest_stage"),
                "expected": "project_quantize",
                "reused_stages": incremental_build.get("reused_stages"),
            },
        )
    comparisons = _compare_or_raise(
        inc_workspace=inc,
        full_workspace=full,
        inc_build=incremental_build,
        full_build=full_build,
    )
    baseline_cfg = load_config(baseline_config, project_root)
    changed_cfg = load_config(changed_config, project_root)
    result = {
        "format": "shardseek-incremental-verification-v1",
        "all_equal": True,
        "baseline_config": {
            "name": baseline_cfg.get("name"),
            "fingerprint": config_fingerprint(baseline_cfg),
            "bits": int(baseline_cfg["quantization"]["bits"]),
        },
        "changed_config": {
            "name": changed_cfg.get("name"),
            "fingerprint": config_fingerprint(changed_cfg),
            "bits": int(changed_cfg["quantization"]["bits"]),
        },
        "baseline_build": {
            "earliest_stage": baseline_build.get("earliest_stage"),
            "representation_fingerprint": baseline_build.get("representation_fingerprint"),
            "index_fingerprint": baseline_build.get("index_fingerprint"),
        },
        "incremental_build": {
            "earliest_stage": incremental_build.get("earliest_stage"),
            "reused_stages": incremental_build.get("reused_stages"),
            "feature_cache_reused": expected_reuse,
            "representation_fingerprint": incremental_build.get("representation_fingerprint"),
            "index_fingerprint": incremental_build.get("index_fingerprint"),
        },
        "full_build": {
            "representation_fingerprint": full_build.get("representation_fingerprint"),
            "index_fingerprint": full_build.get("index_fingerprint"),
        },
        "comparisons": comparisons,
    }
    result["verification_sha256"] = digest_obj(result)
    if output_path is not None:
        write_json_atomic(output_path, result, pretty=True)
    return result


def verify_stop_resume(
    *,
    parent_root: str | Path,
    manifest_path: str | Path,
    project_root: str | Path = ".",
    config_name: str = "baseline_8bit",
    stop_after_shards: int = 2,
) -> dict[str, Any]:
    parent = Path(parent_root)
    interrupted = Workspace(parent / "interrupted", project_root=project_root)
    full = Workspace(parent / "full", project_root=project_root)
    interrupted.clean()
    full.clean()
    interrupted.import_manifest(manifest_path)
    full.import_manifest(manifest_path)
    interrupted.set_config(config_name)
    full.set_config(config_name)
    stopped = Pipeline(interrupted).build(mode="full", stop_after_shards=stop_after_shards)
    if not stopped.get("stopped"):
        raise error(
            E_NUMERIC,
            "Stop demonstration did not stop before completion",
            stage="stop_resume",
            details={"result": stopped},
        )
    resumed = Pipeline(interrupted).build(mode="resume")
    completed = Pipeline(full).build(mode="full")
    top_k = min(5, int(completed["item_count"]))
    reports_interrupted = _report_payloads(interrupted, top_k)
    reports_full = _report_payloads(full, top_k)
    rep_inc = interrupted.representation_summary_path.read_bytes()
    rep_full = full.representation_summary_path.read_bytes()
    idx_inc = interrupted.index_summary_path.read_bytes()
    idx_full = full.index_summary_path.read_bytes()
    checkpoint_records = interrupted.checkpoint_store().read_records()
    sequences = [int(record["seq"]) for record in checkpoint_records]
    expected_count = 4 * int(resumed["item_count"])
    checks = {
        "stopped_early": bool(stopped.get("stopped")),
        "stopped_completed_shards": int(stopped.get("completed_shards", 0)),
        "representation_summary_bytes_equal": rep_inc == rep_full,
        "index_summary_bytes_equal": idx_inc == idx_full,
        "search_reports_bytes_equal": reports_interrupted == reports_full,
        "corpus_diagnostics_bytes_equal": reports_interrupted["corpus-diagnostics"]
        == reports_full["corpus-diagnostics"],
        "checkpoint_sequence_continuous": sequences == list(range(1, len(sequences) + 1)),
        "checkpoint_count_expected": len(sequences) == expected_count,
        "representation_summary_sha256": sha256_bytes(rep_inc),
        "index_summary_sha256": sha256_bytes(idx_inc),
        "search_report_sha256": {
            key: sha256_bytes(value) for key, value in sorted(reports_interrupted.items())
        },
    }
    if not all(
        checks[key]
        for key in (
            "representation_summary_bytes_equal",
            "index_summary_bytes_equal",
            "search_reports_bytes_equal",
            "corpus_diagnostics_bytes_equal",
            "checkpoint_sequence_continuous",
            "checkpoint_count_expected",
        )
    ):
        raise error(
            E_INCREMENTAL_MISMATCH,
            "Stop/resume artifacts differ from a one-shot full build",
            stage="resume",
            details=checks,
        )
    result = {
        "format": "shardseek-stop-resume-verification-v1",
        "all_equal": True,
        "stop_after_shards": stop_after_shards,
        "checks": checks,
        "resumed_build": {
            "representation_fingerprint": resumed.get("representation_fingerprint"),
            "index_fingerprint": resumed.get("index_fingerprint"),
        },
        "full_build": {
            "representation_fingerprint": completed.get("representation_fingerprint"),
            "index_fingerprint": completed.get("index_fingerprint"),
        },
    }
    result["verification_sha256"] = digest_obj(result)
    return result

