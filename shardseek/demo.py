"""One-command deterministic demonstration and validation artifact generator."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from .checkpoint import CheckpointStore
from .config import config_fingerprint, load_config
from .errors import BusinessError
from .features import normalize_caption
from .generator import DEFAULT_SEED, generate_samples
from .pipeline import Pipeline
from .quantization import project_and_normalize
from .search import corpus_diagnostics, run_search
from .util import canonical_bytes, digest_obj, sha256_bytes, write_json_atomic
from .verification import verify_incremental, verify_stop_resume
from .workspace import Workspace


def _capture_error(callable_) -> dict[str, Any]:
    try:
        callable_()
    except BusinessError as exc:
        return exc.to_dict()["error"]
    raise RuntimeError("expected a BusinessError for the demonstration")


def _top_results(report: dict[str, Any], count: int = 3) -> list[dict[str, Any]]:
    return [
        {
            "rank": result["rank"],
            "item_id": result["item_id"],
            "source_shard": result["source_shard"],
            "combined_score": result["combined_score"],
            "dense_cosine": result["dense_cosine"],
            "reference_cosine_decimal": result["reference_cosine_decimal"],
            "cosine_error_bound": result["cosine_error_bound"],
            "effective_digits": result["effective_digits"],
            "inverted_matches": result["inverted"]["matched_tokens"],
            "score_components": result["score_components"],
            "log_contributions": result["log_contributions"],
        }
        for result in report["results"][:count]
    ]


def run_demo(
    *,
    project_root: str | Path = ".",
    root: str | Path = "runtime",
    artifacts_dir: str | Path = "artifacts",
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    project = Path(project_root).resolve()
    workspace = Workspace(root, project_root=project)
    artifacts = Path(artifacts_dir)
    samples_dir = project / "samples"
    sample_summary = generate_samples(samples_dir, seed=seed)
    manifest = samples_dir / "manifest.jsonl"

    workspace.clean()
    imported = workspace.import_manifest(manifest)
    workspace.set_config("baseline_8bit")
    stopped = Pipeline(workspace).build(mode="full", stop_after_shards=2)
    if not stopped.get("stopped"):
        raise RuntimeError("demo stop request did not stop at a shard boundary")
    stopped_snapshot = {
        "completed_shards": stopped["completed_shards"],
        "total_shards": stopped["total_shards"],
        "checkpoint_last_seq": stopped["checkpoints"]["last_seq"],
        "next_expected": stopped["checkpoints"]["next_expected"],
    }
    resumed = Pipeline(workspace).build(mode="resume")
    baseline_8bit_build = resumed
    baseline_8bit_search_text = run_search(
        workspace,
        direction="text_to_image",
        query_text="红色方块 blue gradient",
        top_k=5,
    )
    baseline_8bit_search_image = run_search(
        workspace,
        direction="image_to_text",
        image_item_id="item-01",
        top_k=5,
    )
    baseline_8bit_search_text_repeat = run_search(
        workspace,
        direction="text_to_image",
        query_text="红色方块 blue gradient",
        top_k=5,
    )
    baseline_diagnostics = corpus_diagnostics(workspace)
    baseline_cfg = load_config("baseline_8bit", project)

    resumed_checkpoints = workspace.checkpoint_store().read_records()
    checkpoint_sequences = [record["seq"] for record in resumed_checkpoints]
    stop_resume = verify_stop_resume(
        parent_root=workspace.root / "verification" / "stop_resume",
        manifest_path=manifest,
        project_root=project,
        config_name="baseline_8bit",
        stop_after_shards=2,
    )

    workspace.set_config("compact_4bit")
    incremental = Pipeline(workspace).build(mode="incremental")
    compact_4bit_search_text = run_search(
        workspace,
        direction="text_to_image",
        query_text="红色方块 blue gradient",
        top_k=5,
    )
    compact_4bit_search_image = run_search(
        workspace,
        direction="image_to_text",
        image_item_id="item-01",
        top_k=5,
    )
    compact_diagnostics = corpus_diagnostics(workspace)
    changed_cfg = load_config("compact_4bit", project)
    incremental_verification = verify_incremental(
        parent_root=workspace.root / "verification" / "incremental",
        manifest_path=manifest,
        project_root=project,
        baseline_config="baseline_8bit",
        changed_config="compact_4bit",
        output_path=workspace.root / "verification" / "incremental_verification.json",
    )

    empty_workspace = Workspace(workspace.root / "error_demos" / "index_not_ready", project_root=project)
    empty_workspace.clean()
    empty_workspace.import_manifest(manifest)
    errors = {
        "empty_caption": _capture_error(lambda: normalize_caption("  \n\t ", item_id="item-08")),
        "invalid_k": _capture_error(
            lambda: run_search(
                workspace,
                direction="text_to_image",
                query_text="red block",
                top_k=999,
            )
        ),
        "empty_query": _capture_error(
            lambda: run_search(workspace, direction="text_to_image", query_text="  ", top_k=2)
        ),
        "index_not_ready": _capture_error(
            lambda: run_search(
                empty_workspace,
                direction="text_to_image",
                query_text="red block",
                top_k=2,
            )
        ),
        "stale_checkpoint": _capture_error(
            lambda: workspace.checkpoint_store().publish(
                {
                    "shard_id": "item-01",
                    "stage": "recall_verify",
                    "input_fingerprint": "demo",
                    "config_version": "demo",
                    "quantization_fingerprint": "demo",
                    "artifact_digest": "demo",
                }
            )
        ),
        "dimension_mismatch": _capture_error(
            lambda: project_and_normalize(
                np.zeros(10, dtype=np.float64),
                load_config("baseline_8bit", project)["projection"]["visual"],
                route="visual",
                item_id="demo-dim-mismatch",
            )
        ),
    }

    baseline_bound = float(baseline_diagnostics["series"][-1]["max_similarity_error_bound"])
    compact_bound = float(compact_diagnostics["series"][-1]["max_similarity_error_bound"])
    if not compact_bound > baseline_bound:
        raise RuntimeError("4-bit corpus error bound did not exceed the 8-bit bound")
    report: dict[str, Any] = {
        "format": "shardseek-demo-report-v1",
        "demo_status": "passed",
        "environment": {
            "python": "3.11.18",
            "numpy": "2.4.6",
            "pillow": "13.0.0",
            "file_locking": "fcntl.flock",
            "external_services": False,
            "gpu": False,
        },
        "samples": {
            "seed": sample_summary["seed"],
            "image_count": sample_summary["image_count"],
            "enabled_count": sample_summary["enabled_count"],
            "quarantined_count": sample_summary["quarantined_count"],
            "manifest_sha256": sample_summary["manifest_sha256"],
            "manifest_bytes_sha256": sample_summary["manifest_bytes_sha256"],
            "images": sample_summary["images"],
        },
        "import": {
            "item_count": imported["item_count"],
            "enabled_count": imported["enabled_count"],
            "rejected": imported["rejected"],
        },
        "baseline_8bit": {
            "config_fingerprint": config_fingerprint(baseline_cfg),
            "earliest_stage": baseline_8bit_build["earliest_stage"],
            "representation_fingerprint": baseline_8bit_build["representation_fingerprint"],
            "index_fingerprint": baseline_8bit_build["index_fingerprint"],
            "checkpoint_last_seq": resumed_checkpoints[-1]["seq"],
            "checkpoint_sequence_continuous": checkpoint_sequences == list(range(1, len(checkpoint_sequences) + 1)),
            "checkpoint_count": len(checkpoint_sequences),
            "search_text_report_sha256": baseline_8bit_search_text["report_sha256"],
            "search_image_report_sha256": baseline_8bit_search_image["report_sha256"],
            "repeat_search_bytes_equal": canonical_bytes(baseline_8bit_search_text)
            == canonical_bytes(baseline_8bit_search_text_repeat),
            "text_to_image_top3": _top_results(baseline_8bit_search_text),
            "image_to_text_top3": _top_results(baseline_8bit_search_image),
            "corpus_error_bound_by_shard_size": [
                {
                    "shard_count": entry["shard_count"],
                    "max_similarity_error_bound": entry["max_similarity_error_bound"],
                    "min_effective_digits": entry["min_effective_digits"],
                }
                for entry in baseline_diagnostics["series"]
            ],
            "corpus_diagnostics_sha256": baseline_diagnostics["diagnostics_sha256"],
        },
        "stop_then_resume": {
            "stopped": stopped_snapshot,
            "all_equal_to_full_build": stop_resume["all_equal"],
            "verification": stop_resume,
        },
        "compact_4bit": {
            "config_fingerprint": config_fingerprint(changed_cfg),
            "earliest_stage": incremental["earliest_stage"],
            "reused_stages": incremental["reused_stages"],
            "representation_fingerprint": incremental["representation_fingerprint"],
            "index_fingerprint": incremental["index_fingerprint"],
            "search_text_report_sha256": compact_4bit_search_text["report_sha256"],
            "search_image_report_sha256": compact_4bit_search_image["report_sha256"],
            "text_to_image_top3": _top_results(compact_4bit_search_text),
            "image_to_text_top3": _top_results(compact_4bit_search_image),
            "corpus_error_bound_by_shard_size": [
                {
                    "shard_count": entry["shard_count"],
                    "max_similarity_error_bound": entry["max_similarity_error_bound"],
                    "min_effective_digits": entry["min_effective_digits"],
                }
                for entry in compact_diagnostics["series"]
            ],
            "max_error_bound_ratio_vs_8bit": compact_bound / baseline_bound,
            "corpus_diagnostics_sha256": compact_diagnostics["diagnostics_sha256"],
        },
        "incremental_vs_full": {
            "all_equal": incremental_verification["all_equal"],
            "earliest_stage": incremental_verification["incremental_build"]["earliest_stage"],
            "reused_stages": incremental_verification["incremental_build"]["reused_stages"],
            "representation_summary_sha256": incremental_verification["comparisons"][
                "representation_summary_sha256_incremental"
            ],
            "index_summary_sha256": incremental_verification["comparisons"][
                "index_summary_sha256_incremental"
            ],
            "search_reports_bytes_equal": incremental_verification["comparisons"][
                "search_reports_bytes_equal"
            ],
            "verification_sha256": incremental_verification["verification_sha256"],
        },
        "business_error_demonstrations": errors,
    }
    report["demo_sha256"] = digest_obj(report)
    write_json_atomic(artifacts / "demo_report.json", report, pretty=True)
    write_json_atomic(artifacts / "incremental_verification.json", incremental_verification, pretty=True)
    (artifacts / "DEMO_SUMMARY.sha256").write_text(report["demo_sha256"] + "\n", encoding="utf-8")
    return report

