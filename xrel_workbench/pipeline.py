from __future__ import annotations

import copy
import tempfile
from pathlib import Path
from typing import Any, Callable

from .bridge import load_bridge, relevant_bridge_view
from .checkpoints import CheckpointStore
from .config import config_version, load_config, stage_config_slices
from .diagnostics import aggregate_diagnostics, diagnose_shard
from .errors import (
    BusinessError,
    E_INCREMENTAL_MISMATCH,
    E_INPUT,
)
from .mapping import map_and_quantize_shard
from .normalize import normalize_pair
from .scoring import score_shard
from .util import (
    atomic_write_json,
    atomic_write_text,
    canonical_bytes,
    digest_bytes,
    digest_file,
    digest_value,
    jsonl_bytes,
    read_json,
    read_jsonl,
    relative_path,
    write_jsonl,
)
from .workspace import (
    WorkspacePaths,
    clear_stop,
    copy_inputs,
    input_digests,
    list_shards,
    load_all_pairs,
    read_manifest,
    stop_requested,
    write_manifest,
)


STAGE_ORDER = ["stage1", "stage2", "stage3", "stage4", "stage4_aggregate"]


def stage_artifact_path(paths: WorkspacePaths, stage: str, shard: str) -> Path:
    if stage == "stage4_aggregate":
        return paths.reports / "summary.json"
    return paths.artifacts / stage / f"{shard}.jsonl"


def _stage1_fingerprint(shard_digest: str, config: dict[str, Any]) -> str:
    return digest_value(
        {
            "format": "stage1_v1",
            "input": shard_digest,
            "normalization": config["normalization"],
        }
    )


def _stage2_fingerprint(
    stage1_fingerprint: str,
    relevant_bridge: dict[str, Any],
    config: dict[str, Any],
) -> str:
    return digest_value(
        {
            "format": "stage2_v1",
            "stage1": stage1_fingerprint,
            "relevant_bridge": relevant_bridge,
            "mapping": config["mapping"],
        }
    )


def _stage3_fingerprint(stage2_fingerprint: str, config: dict[str, Any]) -> str:
    return digest_value(
        {
            "format": "stage3_v1",
            "stage2": stage2_fingerprint,
            "scoring": config["scoring"],
        }
    )


def _stage4_fingerprint(stage3_fingerprint: str, config: dict[str, Any]) -> str:
    return digest_value(
        {
            "format": "stage4_v1",
            "stage3": stage3_fingerprint,
            "grading": config["grading"],
            "diagnostics": config["diagnostics"],
        }
    )


def _aggregate_fingerprint(
    stage4_fingerprints: dict[str, str],
    input_digest_map: dict[str, Any],
    config_version_value: str,
) -> str:
    return digest_value(
        {
            "format": "stage4_aggregate_v1",
            "stage4": stage4_fingerprints,
            "inputs": input_digest_map,
            "config_version": config_version_value,
        }
    )


def _normalized_token_terms(
    pairs: list[dict[str, Any]],
    config: dict[str, Any],
    shard: str,
) -> set[str]:
    terms: set[str] = set()
    for pair in pairs:
        normalized = normalize_pair(pair, config, shard=shard)
        terms.update(token["term"] for token in normalized["query_tokens"])
        terms.update(token["term"] for token in normalized["doc_tokens"])
    return terms


def compute_fingerprints(
    paths: WorkspacePaths,
    config: dict[str, Any],
    bridge: dict[str, Any],
    shards: list[str],
) -> dict[str, Any]:
    all_digests = input_digests(paths)
    shard_digests = all_digests["shards"]
    pairs_by_shard = load_all_pairs(paths)
    fingerprints: dict[str, Any] = {
        "stage1": {},
        "stage2": {},
        "stage3": {},
        "stage4": {},
        "stage4_aggregate": None,
    }
    for shard in shards:
        stage1_fp = _stage1_fingerprint(shard_digests[shard], config)
        bridge_view = relevant_bridge_view(
            bridge,
            _normalized_token_terms(pairs_by_shard[shard], config, shard),
        )
        stage2_fp = _stage2_fingerprint(stage1_fp, bridge_view, config)
        stage3_fp = _stage3_fingerprint(stage2_fp, config)
        stage4_fp = _stage4_fingerprint(stage3_fp, config)
        fingerprints["stage1"][shard] = stage1_fp
        fingerprints["stage2"][shard] = stage2_fp
        fingerprints["stage3"][shard] = stage3_fp
        fingerprints["stage4"][shard] = stage4_fp
    fingerprints["stage4_aggregate"] = _aggregate_fingerprint(
        fingerprints["stage4"],
        all_digests,
        config_version(config),
    )
    return fingerprints


def invalidation_plan(
    previous: dict[str, Any],
    current: dict[str, Any],
    shards: list[str],
) -> dict[str, Any]:
    old_fingerprints = previous.get("fingerprints", {}) if previous else {}
    per_shard: dict[str, Any] = {}
    for shard in shards:
        earliest = None
        comparisons = []
        for index, stage in enumerate(STAGE_ORDER[:-1], 1):
            old_value = old_fingerprints.get(stage, {}).get(shard)
            new_value = current[stage][shard]
            changed = old_value != new_value
            comparisons.append({"stage": stage, "changed": changed})
            if changed and earliest is None:
                earliest = stage
        if earliest is None and not old_fingerprints:
            earliest = "stage1"
        per_shard[shard] = {
            "earliest_affected_stage": earliest,
            "comparisons": comparisons,
        }
    aggregate_changed = (
        old_fingerprints.get("stage4_aggregate") != current["stage4_aggregate"]
        if old_fingerprints
        else True
    )
    return {
        "per_shard": per_shard,
        "aggregate_changed": aggregate_changed,
        "summary": [
            {
                "shard": shard,
                "earliest_affected_stage": value["earliest_affected_stage"],
            }
            for shard, value in per_shard.items()
        ],
    }


def _empty_manifest() -> dict[str, Any]:
    return {
        "format_version": 1,
        "config_version": None,
        "input_digests": {},
        "fingerprints": {},
        "artifacts": {stage: {} for stage in STAGE_ORDER},
    }


def _manifest_artifact(
    manifest: dict[str, Any],
    paths: WorkspacePaths,
    stage: str,
    shard: str,
    fingerprint: str,
    artifact_path: Path,
    checkpoint_seq: int,
) -> dict[str, Any]:
    entry = {
        "path": relative_path(artifact_path, paths.root),
        "digest": digest_file(artifact_path),
        "fingerprint": fingerprint,
        "checkpoint_seq": checkpoint_seq,
    }
    manifest.setdefault("artifacts", {}).setdefault(stage, {})[shard] = entry
    if stage == "stage4_aggregate":
        manifest.setdefault("fingerprints", {})[stage] = fingerprint
    else:
        manifest.setdefault("fingerprints", {}).setdefault(stage, {})[shard] = fingerprint
    return entry


def _load_stage_records(
    paths: WorkspacePaths,
    manifest: dict[str, Any],
    stage: str,
    shard: str,
) -> list[dict[str, Any]]:
    entry = manifest.get("artifacts", {}).get(stage, {}).get(shard)
    if entry is None:
        raise BusinessError(E_INPUT, f"manifest has no artifact for {stage}/{shard}")
    path = paths.root / entry["path"]
    if not path.exists():
        raise BusinessError(E_INPUT, f"artifact is missing: {entry['path']}")
    return read_jsonl(path)


def _write_run_status(paths: WorkspacePaths, value: dict[str, Any]) -> None:
    atomic_write_json(paths.run_status, value, indent=2)


def _build_stage4_aggregate(
    paths: WorkspacePaths,
    config: dict[str, Any],
    shards: list[str],
    manifest: dict[str, Any],
    fingerprints: dict[str, Any],
) -> dict[str, Any]:
    stage4_by_shard = {
        shard: _load_stage_records(paths, manifest, "stage4", shard) for shard in shards
    }
    all_pairs = [record for shard in shards for record in stage4_by_shard[shard]]
    aggregate = aggregate_diagnostics(all_pairs, config)
    error_report = {
        "format_version": 1,
        "method": "quantized_score_minus_decimal_reference_v1",
        "pair_count": len(all_pairs),
        "corpus_error_propagation": aggregate["error_propagation"],
        "crosscheck_review_pairs": aggregate["error_crosscheck_review_pairs"],
        "pairs": [
            {
                "pair_id": item["pair_id"],
                "shard": item["shard"],
                "score": item["score"],
                "score_unclipped": item["score_unclipped"],
                "grade": item["grade"],
                "human_relevance": item["human_relevance"],
                "source_log_contributions": item["source_log_contributions"],
                "error_report": item["error_report"],
                "review_required": item["review_required"],
                "review_reasons": item["review_reasons"],
            }
            for item in all_pairs
        ],
    }
    exports = [
        {
            "pair_id": item["pair_id"],
            "shard": item["shard"],
            "query": item["query"],
            "doc": item["doc"],
            "lang_query": item["lang_query"],
            "lang_doc": item["lang_doc"],
            "score": item["score"],
            "score_unclipped": item["score_unclipped"],
            "grade": item["grade"],
            "grade_label": item["grade_label"],
            "human_relevance": item["human_relevance"],
            "candidate_count": item["candidate_count"],
            "source_log_contributions": item["source_log_contributions"],
            "quantization_error_bound": item["error_report"]["analytical_error_bound"],
            "score_effective_digits": item["error_report"]["effective_digits"],
            "review_required": item["review_required"],
        }
        for item in all_pairs
    ]
    report_paths = {
        "reports/error_report.json": canonical_bytes(error_report, indent=2),
        "reports/validation.json": canonical_bytes(aggregate, indent=2),
        "exports/scores.jsonl": jsonl_bytes(exports),
    }
    for relative, data in report_paths.items():
        atomic_write_text(paths.root / relative, data.decode("utf-8"))

    artifact_digests: dict[str, str] = {}
    for shard in shards:
        stage4_path = stage_artifact_path(paths, "stage4", shard)
        artifact_digests[relative_path(stage4_path, paths.root)] = digest_file(stage4_path)
    for relative, data in report_paths.items():
        artifact_digests[relative] = digest_bytes(data)
    build_id = digest_value({"format": "build_v1", "artifacts": artifact_digests})
    summary = {
        "format_version": 1,
        "build_id": build_id,
        "config_version": config_version(config),
        "input_digests": input_digests(paths),
        "fingerprints": {
            "stage1": fingerprints["stage1"],
            "stage2": fingerprints["stage2"],
            "stage3": fingerprints["stage3"],
            "stage4": fingerprints["stage4"],
            "stage4_aggregate": fingerprints["stage4_aggregate"],
        },
        "counts": {
            "shards": len(shards),
            "pairs": len(all_pairs),
            "graded_pairs": len(all_pairs),
            "labeled_pairs": aggregate["labeled_pair_count"],
            "review_pairs": len(aggregate["out_of_bounds_samples"]),
        },
        "grades": {
            "labels": aggregate["labels"],
            "thresholds": aggregate["thresholds"],
            "counts": aggregate["grade_counts"],
            "observed_distribution": aggregate["observed_distribution"],
            "distribution_review_required": aggregate["distribution_review_required"],
            "boundary_review_required": aggregate["boundary_review_required"],
        },
        "error_propagation": {
            "method": aggregate["error_propagation"]["method"],
            "slope_per_pair": aggregate["error_propagation"]["slope_per_pair"],
            "intercept": aggregate["error_propagation"]["intercept"],
            "mean_slope_per_pair": aggregate["error_propagation"]["mean_slope_per_pair"],
            "mean_intercept": aggregate["error_propagation"]["mean_intercept"],
            "predictions": aggregate["error_propagation"]["predictions"],
        },
        "review_required": aggregate["review_required"],
        "artifact_digests": dict(sorted(artifact_digests.items())),
    }
    atomic_write_json(paths.reports / "summary.json", summary, indent=2)
    return summary


def _output_paths(paths: WorkspacePaths) -> list[str]:
    result: list[str] = []
    for path in sorted((paths.artifacts / "stage4").glob("*.jsonl")):
        result.append(relative_path(path, paths.root))
    result.extend(
        [
            "reports/error_report.json",
            "reports/validation.json",
            "reports/summary.json",
            "exports/scores.jsonl",
        ]
    )
    return result


def compare_outputs(left: WorkspacePaths, right: WorkspacePaths) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    left_paths = set(_output_paths(left))
    right_paths = set(_output_paths(right))
    for relative in sorted(left_paths | right_paths):
        left_file = left.root / relative
        right_file = right.root / relative
        if not left_file.exists() or not right_file.exists():
            differences.append(
                {
                    "path": relative,
                    "kind": "missing_artifact",
                    "present_incremental": left_file.exists(),
                    "present_full": right_file.exists(),
                }
            )
            continue
        left_bytes = left_file.read_bytes()
        right_bytes = right_file.read_bytes()
        if left_bytes == right_bytes:
            continue
        first_diff = next(
            (index for index, (a, b) in enumerate(zip(left_bytes, right_bytes)) if a != b),
            min(len(left_bytes), len(right_bytes)),
        )
        differences.append(
            {
                "path": relative,
                "kind": "byte_mismatch",
                "incremental_digest": digest_bytes(left_bytes),
                "full_digest": digest_bytes(right_bytes),
                "first_diff_offset": first_diff,
            }
        )
    return differences


def run_pipeline(
    paths: WorkspacePaths,
    *,
    full: bool = False,
    verify_incremental: bool = True,
    stop_after_units: int | None = None,
) -> dict[str, Any]:
    from .locking import FileLock

    shards = list_shards(paths)
    if len(shards) < 3:
        raise BusinessError(E_INPUT, "at least three shards are required")
    config = load_config(paths.scoring)
    bridge = load_bridge(paths.bridge)
    current_version = config_version(config)

    with FileLock(paths.build_lock):
        previous_manifest = read_manifest(paths)
        had_previous_build = bool(previous_manifest)
        fingerprints = compute_fingerprints(paths, config, bridge, shards)
        plan = invalidation_plan(previous_manifest, fingerprints, shards)
        manifest = copy.deepcopy(previous_manifest) if previous_manifest else _empty_manifest()
        manifest["format_version"] = 1
        manifest["config_version"] = current_version
        manifest["input_digests"] = input_digests(paths)
        manifest["fingerprints"] = copy.deepcopy(fingerprints)
        write_manifest(paths, manifest)
        store = CheckpointStore(paths.root)
        completed_units = 0
        rebuilt_units = 0
        reused_units = 0
        units: list[dict[str, Any]] = []
        stopped = False
        next_unit: dict[str, Any] | None = None

        def run_unit(
            stage: str,
            shard: str,
            input_digest: str,
            fingerprint: str,
            artifact_path: Path,
            builder: Callable[[], Any],
            expected_shards: list[str] | None = None,
        ) -> dict[str, Any]:
            nonlocal rebuilt_units, reused_units
            checkpoint = store.verify_artifact(stage, shard, fingerprint)
            if checkpoint is not None and not full:
                reused_units += 1
                entry = _manifest_artifact(
                    manifest,
                    paths,
                    stage,
                    shard,
                    fingerprint,
                    artifact_path,
                    int(checkpoint["seq"]),
                )
                write_manifest(paths, manifest)
                return {"stage": stage, "shard": shard, "mode": "reused", "artifact": entry}
            builder()
            record = store.publish(
                stage=stage,
                shard=shard,
                input_digest=input_digest,
                config_version=current_version,
                artifact_path=relative_path(artifact_path, paths.root),
                fingerprint=fingerprint,
                expected_shards=expected_shards,
            )
            rebuilt_units += 1
            entry = _manifest_artifact(
                manifest,
                paths,
                stage,
                shard,
                fingerprint,
                artifact_path,
                int(record["seq"]),
            )
            write_manifest(paths, manifest)
            return {"stage": stage, "shard": shard, "mode": "rebuilt", "artifact": entry}

        def stop_now(reason: str) -> bool:
            nonlocal stopped, next_unit
            if stop_requested(paths) or (
                stop_after_units is not None and completed_units >= int(stop_after_units)
            ):
                stopped = True
                _write_run_status(
                    paths,
                    {
                        "status": "stopped",
                        "reason": reason,
                        "completed_units_this_run": completed_units,
                        "rebuilt_units_this_run": rebuilt_units,
                        "reused_units_this_run": reused_units,
                        "next_unit": next_unit,
                        "invalidation_plan": plan,
                    },
                )
                return True
            return False

        _write_run_status(
            paths,
            {
                "status": "running",
                "mode": "full" if full else "incremental",
                "invalidation_plan": plan,
            },
        )
        try:
            for shard in shards:
                next_unit = {"stage": "stage1", "shard": shard}
                if stop_now("stopped before next shard unit"):
                    break
                fp = fingerprints["stage1"][shard]
                input_digest = manifest["input_digests"]["shards"][shard]
                artifact = stage_artifact_path(paths, "stage1", shard)

                def build_stage1(shard=shard, artifact=artifact):
                    pairs = read_jsonl(paths.shards / f"{shard}.jsonl")
                    write_jsonl(
                        artifact,
                        [normalize_pair(pair, config, shard=shard) for pair in pairs],
                    )

                units.append(
                    run_unit("stage1", shard, input_digest, fp, artifact, build_stage1)
                )
                completed_units += 1
            if not stopped:
                for shard in shards:
                    next_unit = {"stage": "stage2", "shard": shard}
                    if stop_now("stopped before next shard unit"):
                        break
                    stage1_entry = manifest["artifacts"]["stage1"][shard]
                    fp = fingerprints["stage2"][shard]
                    artifact = stage_artifact_path(paths, "stage2", shard)

                    def build_stage2(shard=shard, artifact=artifact):
                        stage1_records = _load_stage_records(paths, manifest, "stage1", shard)
                        write_jsonl(artifact, map_and_quantize_shard(stage1_records, bridge, config))

                    units.append(
                        run_unit(
                            "stage2",
                            shard,
                            stage1_entry["digest"],
                            fp,
                            artifact,
                            build_stage2,
                        )
                    )
                    completed_units += 1
            if not stopped:
                for shard in shards:
                    next_unit = {"stage": "stage3", "shard": shard}
                    if stop_now("stopped before next shard unit"):
                        break
                    stage2_entry = manifest["artifacts"]["stage2"][shard]
                    fp = fingerprints["stage3"][shard]
                    artifact = stage_artifact_path(paths, "stage3", shard)

                    def build_stage3(shard=shard, artifact=artifact):
                        stage2_records = _load_stage_records(paths, manifest, "stage2", shard)
                        write_jsonl(artifact, score_shard(stage2_records, config))

                    units.append(
                        run_unit(
                            "stage3",
                            shard,
                            stage2_entry["digest"],
                            fp,
                            artifact,
                            build_stage3,
                        )
                    )
                    completed_units += 1
            if not stopped:
                for shard in shards:
                    next_unit = {"stage": "stage4", "shard": shard}
                    if stop_now("stopped before next shard unit"):
                        break
                    stage3_entry = manifest["artifacts"]["stage3"][shard]
                    fp = fingerprints["stage4"][shard]
                    artifact = stage_artifact_path(paths, "stage4", shard)

                    def build_stage4(shard=shard, artifact=artifact):
                        stage1_records = _load_stage_records(paths, manifest, "stage1", shard)
                        stage2_records = _load_stage_records(paths, manifest, "stage2", shard)
                        stage3_records = _load_stage_records(paths, manifest, "stage3", shard)
                        write_jsonl(
                            artifact,
                            diagnose_shard(stage1_records, stage2_records, stage3_records, config),
                        )

                    units.append(
                        run_unit(
                            "stage4",
                            shard,
                            stage3_entry["digest"],
                            fp,
                            artifact,
                            build_stage4,
                        )
                    )
                    completed_units += 1
            aggregate_summary = None
            if not stopped:
                next_unit = {"stage": "stage4_aggregate", "shard": "ALL"}
                if stop_now("stopped before aggregate publication"):
                    stopped = True
                else:
                    stage4_digests = {
                        shard: manifest["artifacts"]["stage4"][shard]["digest"] for shard in shards
                    }
                    aggregate_input_digest = digest_value(stage4_digests)
                    aggregate_fp = fingerprints["stage4_aggregate"]
                    aggregate_path = stage_artifact_path(paths, "stage4_aggregate", "ALL")

                    def build_aggregate():
                        nonlocal aggregate_summary
                        aggregate_summary = _build_stage4_aggregate(
                            paths, config, shards, manifest, fingerprints
                        )

                    units.append(
                        run_unit(
                            "stage4_aggregate",
                            "ALL",
                            aggregate_input_digest,
                            aggregate_fp,
                            aggregate_path,
                            build_aggregate,
                            expected_shards=shards,
                        )
                    )
                    completed_units += 1
        except BusinessError as exc:
            _write_run_status(
                paths,
                {
                    "status": "error",
                    "error": exc.to_dict()["error"],
                    "completed_units_this_run": completed_units,
                    "invalidation_plan": plan,
                },
            )
            raise

        if stopped:
            write_manifest(paths, manifest)
            return {
                "ok": True,
                "stopped": True,
                "rebuilt_units": rebuilt_units,
                "reused_units": reused_units,
                "invalidation_plan": plan,
                "units": units,
            }

        summary_path = paths.reports / "summary.json"
        if aggregate_summary is None:
            aggregate_summary = read_json(summary_path)
        write_manifest(paths, manifest)
        if full:
            atomic_write_json(paths.state / "full_baseline_summary.json", aggregate_summary, indent=2)
        verification = None
        if verify_incremental and had_previous_build and not full:
            try:
                verification = verify_against_full(paths)
            except BusinessError as exc:
                _write_run_status(
                    paths,
                    {
                        "status": "error",
                        "error": exc.to_dict()["error"],
                        "completed_units_this_run": completed_units,
                        "invalidation_plan": plan,
                    },
                )
                raise
        _write_run_status(
            paths,
            {
                "status": "complete",
                "build_id": aggregate_summary["build_id"],
                "completed_units_this_run": completed_units,
                "rebuilt_units_this_run": rebuilt_units,
                "reused_units_this_run": reused_units,
                "verification": verification,
                "invalidation_plan": plan,
            },
        )
        return {
            "ok": True,
            "stopped": False,
            "build_id": aggregate_summary["build_id"],
            "summary": aggregate_summary,
            "rebuilt_units": rebuilt_units,
            "reused_units": reused_units,
            "invalidation_plan": plan,
            "verification": verification,
            "units": units,
        }


def verify_against_full(paths: WorkspacePaths) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="xrel-full-baseline-") as temporary:
        shadow_root = Path(temporary) / "workspace"
        shadow = WorkspacePaths(shadow_root)
        for directory in (
            shadow.shards,
            shadow.artifacts,
            shadow.checkpoints,
            shadow.reports,
            shadow.exports,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        copy_inputs(paths, shadow)
        run_pipeline(shadow, full=True, verify_incremental=False)
        differences = compare_outputs(paths, shadow)
        if differences:
            raise BusinessError(
                E_INCREMENTAL_MISMATCH,
                "incremental outputs differ from a from-scratch full build",
                {"difference_count": len(differences), "differences": differences},
            )
        return {
            "match": True,
            "compared_paths": _output_paths(paths),
            "difference_count": 0,
        }


def workspace_status(paths: WorkspacePaths) -> dict[str, Any]:
    manifest = read_manifest(paths)
    store = CheckpointStore(paths.root)
    status = read_json(paths.run_status) if paths.run_status.exists() else {"status": "never_run"}
    return {
        "workspace": str(paths.root),
        "run": status,
        "config_version": manifest.get("config_version"),
        "input_digests": manifest.get("input_digests", {}),
        "fingerprints": manifest.get("fingerprints", {}),
        "artifacts": manifest.get("artifacts", {}),
        "checkpoints": store.status(),
        "reports": {
            "summary": (paths.reports / "summary.json").exists(),
            "validation": (paths.reports / "validation.json").exists(),
            "error_report": (paths.reports / "error_report.json").exists(),
            "export": (paths.exports / "scores.jsonl").exists(),
        },
    }


def export_scores(paths: WorkspacePaths) -> bytes:
    path = paths.exports / "scores.jsonl"
    if not path.exists():
        raise BusinessError(E_INPUT, "scores export is not available; run the build first")
    return path.read_bytes()
