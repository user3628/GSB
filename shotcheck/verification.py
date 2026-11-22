from __future__ import annotations

import copy
import multiprocessing
import shutil
from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .errors import BusinessError, E_STALE_CHECKPOINT
from .generator import generate_all
from .pipeline import (
    GracefulStop,
    Pipeline,
    StopController,
    dependencies,
    load_inputs,
)
from .text import normalize_and_split
from .util import (
    atomic_write_json,
    canonical_bytes,
    ensure_dir,
    read_json,
    read_jsonl,
    sha256_bytes,
    write_jsonl,
)
from .version import PIPELINE_VERSION, SCHEMA_VERSION


def _fresh_from_inputs(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    ensure_dir(destination)
    shutil.copytree(source / "inputs", destination / "inputs")
    for directory in ("checkpoints", "objects", "exports", "locks"):
        ensure_dir(destination / directory)


def _clone_workspace(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def _check(passed: bool, details: Any) -> dict[str, Any]:
    return {"passed": bool(passed), "details": details}


def _error_codes_by_shot(errors: list[dict[str, Any]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for error in errors:
        shot_id = error.get("shot_id")
        if shot_id is None:
            continue
        result.setdefault(shot_id, []).append(error["code"])
    return {shot_id: sorted(set(codes)) for shot_id, codes in result.items()}


def _reference_unit_distribution(reference: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for shot in reference.get("shots", {}).values():
        for unit in shot.get("units", []):
            counts[unit["type"]] = counts.get(unit["type"], 0) + 1
    return dict(sorted(counts.items()))


def _reference_emitted_unit_distribution(reference: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for shot in reference.get("shots", {}).values():
        if "E_EMPTY_PROMPT" in shot.get("expected_error_codes", []):
            continue
        for unit in shot.get("units", []):
            counts[unit["type"]] = counts.get(unit["type"], 0) + 1
    return dict(sorted(counts.items()))


def _validate_tier(
    workspace: Path,
    result: dict[str, Any],
    run_meta: dict[str, Any],
) -> dict[str, Any]:
    bundle = load_inputs(workspace)
    reference = bundle.reference
    scripts = {shot["shot_id"]: shot for shot in bundle.scripts}
    normalize_artifact = result["outputs"]["normalize"]
    duration_artifact = result["outputs"]["duration"]
    decisions = result["shots"]
    actual_error_codes = _error_codes_by_shot(result["errors"])

    unit_mismatches: list[dict[str, Any]] = []
    emitted_unit_mismatches: list[dict[str, Any]] = []
    units_checked = 0
    for shot_id, reference_shot in reference["shots"].items():
        shot = scripts[shot_id]
        entry = bundle.subjects_by_shot.get(shot_id, {})
        normalized = normalize_and_split(
            shot.get("prompt", ""),
            subject_terms=[
                *entry.get("required_subjects", []),
                *entry.get("forbidden_subjects", []),
            ],
            scene_terms=shot.get("must_scenes", []),
            negative_terms=shot.get("negative_terms", []),
        )
        units_checked += 1
        expected_units = [
            (unit["text"], unit["start"], unit["end"], unit["type"])
            for unit in reference_shot["units"]
        ]
        actual_units = [
            (unit["text"], unit["start"], unit["end"], unit["type"])
            for unit in normalized["units"]
        ]
        if (
            normalized["normalized_prompt"] != reference_shot["normalized_prompt"]
            or actual_units != expected_units
        ):
            unit_mismatches.append(
                {
                    "shot_id": shot_id,
                    "expected_prompt": reference_shot["normalized_prompt"],
                    "actual_prompt": normalized["normalized_prompt"],
                    "expected_units": expected_units,
                    "actual_units": actual_units,
                }
            )
        if shot_id in normalize_artifact["shots"]:
            emitted = normalize_artifact["shots"][shot_id]
            emitted_units = [
                (unit["text"], unit["start"], unit["end"], unit["type"])
                for unit in emitted["units"]
            ]
            if (
                emitted["normalized_prompt"] != reference_shot["normalized_prompt"]
                or emitted_units != expected_units
            ):
                emitted_unit_mismatches.append({"shot_id": shot_id})

    duration_mismatches: list[dict[str, Any]] = []
    duration_checked = 0
    for shot_id, reference_shot in reference["shots"].items():
        expected_duration = reference_shot.get("duration")
        actual_duration = duration_artifact["shots"].get(shot_id)
        if expected_duration is None:
            if actual_duration is not None:
                duration_mismatches.append(
                    {
                        "shot_id": shot_id,
                        "reason": "unexpected_parse_success",
                        "actual": actual_duration,
                    }
                )
            elif "E_DURATION_PARSE" not in actual_error_codes.get(shot_id, []):
                duration_mismatches.append(
                    {
                        "shot_id": shot_id,
                        "reason": "missing_duration_error",
                    }
                )
            continue
        duration_checked += 1
        if actual_duration is None:
            duration_mismatches.append(
                {"shot_id": shot_id, "reason": "parse_expected_but_failed"}
            )
            continue
        fields = ("target_ms", "source", "precision_digits")
        actual_fields = (
            actual_duration["duration_ms"],
            actual_duration["source"],
            actual_duration["precision_digits"],
        )
        expected_fields = tuple(expected_duration[field] for field in fields)
        if actual_fields != expected_fields:
            duration_mismatches.append(
                {
                    "shot_id": shot_id,
                    "expected": expected_fields,
                    "actual": actual_fields,
                }
            )

    coverage_mismatches: list[dict[str, Any]] = []
    verdict_mismatches: list[dict[str, Any]] = []
    error_code_mismatches: list[dict[str, Any]] = []
    coverage_checked = 0
    expected_na_counts: dict[str, int] = {}
    for shot_id, reference_shot in reference["shots"].items():
        expected_codes = sorted(reference_shot.get("expected_error_codes", []))
        actual_codes = actual_error_codes.get(shot_id, [])
        if actual_codes != expected_codes:
            error_code_mismatches.append(
                {
                    "shot_id": shot_id,
                    "expected": expected_codes,
                    "actual": actual_codes,
                }
            )
        outcome = result["outcomes"][shot_id]
        if outcome["verdict"] != reference_shot["expected_verdict"]:
            verdict_mismatches.append(
                {
                    "shot_id": shot_id,
                    "expected": reference_shot["expected_verdict"],
                    "actual": outcome["verdict"],
                    "reasons": outcome["reasons"],
                }
            )
        expected_coverage = reference_shot.get("expected_coverage")
        if expected_coverage is None:
            continue
        coverage_checked += 1
        decision = decisions.get(shot_id)
        if decision is None:
            coverage_mismatches.append(
                {"shot_id": shot_id, "reason": "decision_missing"}
            )
            continue
        if canonical_bytes(decision["coverage"]) != canonical_bytes(
            expected_coverage["coverage"]
        ):
            coverage_mismatches.append(
                {
                    "shot_id": shot_id,
                    "reason": "coverage_mismatch",
                    "expected": expected_coverage["coverage"],
                    "actual": decision["coverage"],
                }
            )
        if decision["score"] != expected_coverage["score"]:
            coverage_mismatches.append(
                {
                    "shot_id": shot_id,
                    "reason": "score_mismatch",
                    "expected": expected_coverage["score"],
                    "actual": decision["score"],
                }
            )
        if decision["na_components"] != expected_coverage["na_components"]:
            coverage_mismatches.append(
                {
                    "shot_id": shot_id,
                    "reason": "na_component_mismatch",
                    "expected": expected_coverage["na_components"],
                    "actual": decision["na_components"],
                }
            )
        for component in expected_coverage["na_components"]:
            expected_na_counts[component] = expected_na_counts.get(component, 0) + 1

    offset_errors: list[dict[str, Any]] = []
    for error in result["errors"]:
        code = error["code"]
        if code == "E_EMPTY_PROMPT":
            if not (
                error.get("shot_id")
                and isinstance(error.get("split_offset"), int)
                and error["split_offset"] >= 0
            ):
                offset_errors.append(error)
        elif code == "E_DURATION_PARSE":
            if not (
                error.get("shot_id")
                and isinstance(error.get("phrase_offset"), int)
                and error["phrase_offset"] >= 0
                and isinstance(error.get("phrase_offsets"), list)
            ):
                offset_errors.append(error)
        elif code == "E_SEMANTIC_CONFLICT":
            required_span = error.get("required_span")
            negative_span = error.get("negative_span")
            if not (
                error.get("shot_id")
                and isinstance(required_span, dict)
                and isinstance(required_span.get("start"), int)
                and isinstance(required_span.get("end"), int)
                and isinstance(negative_span, dict)
                and isinstance(negative_span.get("start"), int)
                and isinstance(negative_span.get("end"), int)
            ):
                offset_errors.append(error)
        elif code == "E_FORBIDDEN_SUBJECT":
            span = error.get("span")
            if not (
                error.get("shot_id")
                and isinstance(span, dict)
                and isinstance(span.get("start"), int)
                and isinstance(span.get("end"), int)
            ):
                offset_errors.append(error)
        if not error.get("shot_id") and not error.get("stage"):
            offset_errors.append(error)

    expected_error_distribution: dict[str, int] = {}
    for reference_shot in reference["shots"].values():
        for code in set(reference_shot.get("expected_error_codes", [])):
            expected_error_distribution[code] = (
                expected_error_distribution.get(code, 0) + 1
            )
    expected_error_distribution = dict(sorted(expected_error_distribution.items()))
    actual_error_distribution = result["summary"]["error_distribution"]
    expected_na_counts = dict(sorted(expected_na_counts.items()))
    actual_na_counts = result["summary"]["na_component_distribution"]

    reference_emitted_distribution = _reference_emitted_unit_distribution(reference)
    unit_distribution_pass = (
        result["summary"]["unit_type_distribution"] == reference_emitted_distribution
    )
    strategy = result["strategy_selection"]
    truncated = next(
        item
        for item in strategy["candidates"]
        if item["strategy"] == "keyword_priority_truncated"
    )
    small_scale = bundle.tier == "small"

    checks = {
        "units_exact": _check(
            not unit_mismatches,
            {
                "shots_checked": units_checked,
                "mismatch_count": len(unit_mismatches),
                "mismatches": unit_mismatches,
            },
        ),
        "stage1_emitted_units_exact": _check(
            not emitted_unit_mismatches,
            {
                "emitted_shots_checked": len(normalize_artifact["shots"]),
                "mismatch_count": len(emitted_unit_mismatches),
                "mismatches": emitted_unit_mismatches,
            },
        ),
        "duration_values_exact": _check(
            not duration_mismatches,
            {
                "parsed_shots_checked": duration_checked,
                "mismatch_count": len(duration_mismatches),
                "mismatches": duration_mismatches,
            },
        ),
        "coverage_exact": _check(
            not coverage_mismatches,
            {
                "shots_checked": coverage_checked,
                "mismatch_count": len(coverage_mismatches),
                "mismatches": coverage_mismatches,
            },
        ),
        "verdicts_exact": _check(
            not verdict_mismatches,
            {
                "shots_checked": len(reference["shots"]),
                "mismatch_count": len(verdict_mismatches),
                "mismatches": verdict_mismatches,
            },
        ),
        "error_codes_exact": _check(
            not error_code_mismatches,
            {
                "mismatch_count": len(error_code_mismatches),
                "mismatches": error_code_mismatches,
            },
        ),
        "error_locations_present": _check(
            not offset_errors,
            {"mismatch_count": len(offset_errors), "mismatches": offset_errors},
        ),
        "error_distribution_exact": _check(
            actual_error_distribution == expected_error_distribution,
            {
                "expected": expected_error_distribution,
                "actual": actual_error_distribution,
            },
        ),
        "na_distribution_exact": _check(
            actual_na_counts == expected_na_counts,
            {"expected": expected_na_counts, "actual": actual_na_counts},
        ),
        "unit_distribution_consistent": _check(
            unit_distribution_pass,
            {
                "stage1": normalize_artifact["summary"]["unit_type_distribution"],
                "reference_emitted": reference_emitted_distribution,
                "reference_all": _reference_unit_distribution(reference),
            },
        ),
        "strategy_parity": _check(
            result["strategy_parity"]["all_equal"],
            {
                "digests": result["strategy_parity"]["digests"],
                "selected": strategy["strategy"],
            },
        ),
        "small_scale_not_truncated": _check(
            (not small_scale) or strategy["strategy"] != "keyword_priority_truncated",
            {
                "tier": bundle.tier,
                "selected": strategy["strategy"],
                "truncated_candidate_status": truncated["status"],
                "truncated_rejection_reason": truncated["rejection_reason"],
            },
        ),
    }
    return {
        "tier": bundle.tier,
        "shot_count": len(bundle.scripts),
        "workspace": str(workspace),
        "checks": checks,
        "all_passed": all(item["passed"] for item in checks.values()),
        "summary": result["summary"],
        "reference_unit_type_distribution": _reference_unit_distribution(reference),
        "reference_emitted_unit_type_distribution": reference_emitted_distribution,
        "strategy": {
            "selected": strategy["strategy"],
            "selected_reason": strategy["selected_reason"],
            "metrics": strategy["metrics"],
            "candidates": strategy["candidates"],
        },
        "version_fingerprint": result["version_fingerprint"],
        "result_digest": sha256_bytes((workspace / "exports/result.json").read_bytes()),
        "invalidation": run_meta["invalidation"],
    }


def _mutate_duration(workspace: Path) -> dict[str, Any]:
    scripts_path = workspace / "inputs/scripts.jsonl"
    reference_path = workspace / "inputs/reference.json"
    scripts = read_jsonl(scripts_path)
    reference = read_json(reference_path)
    chosen: tuple[dict[str, Any], dict[str, Any]] | None = None
    for script in scripts:
        reference_shot = reference["shots"][script["shot_id"]]
        if (
            not reference_shot.get("expected_error_codes")
            and reference_shot.get("expected_verdict") == "pass"
            and reference_shot.get("duration")
        ):
            chosen = (script, reference_shot)
            break
    if chosen is None:
        raise AssertionError("no mutable successful duration shot found")
    script, reference_shot = chosen
    old_ms = int(reference_shot["duration"]["target_ms"])
    new_ms = 3200 if old_ms != 3200 else 3600
    new_phrase = f"{new_ms / 1000:g}s"
    script["duration_phrase"] = new_phrase
    reference_shot["duration"] = {
        "target_ms": new_ms,
        "source": "arabic_unit",
        "precision_digits": 1 if "." in new_phrase else 0,
    }
    write_jsonl(scripts_path, scripts)
    atomic_write_json(reference_path, reference)
    return {
        "shot_id": script["shot_id"],
        "old_target_ms": old_ms,
        "new_target_ms": new_ms,
        "new_phrase": new_phrase,
    }


def _publish_worker(
    workspace: str,
    barrier: Any,
    queue: Any,
    payload_marker: str,
) -> None:
    store = CheckpointStore(workspace)
    artifact = {
        "stage": "normalize",
        "marker": payload_marker,
        "shots": {},
        "errors": [],
    }
    barrier.wait()
    try:
        record = store.publish(
            "normalize",
            artifact,
            input_fingerprint=f"fp-{payload_marker}",
            config_version="test",
            config_fingerprint="cfg",
            expected_prev_sequence=0,
        )
        queue.put(
            {
                "ok": True,
                "sequence": record["sequence"],
                "marker": payload_marker,
            }
        )
    except BusinessError as exc:
        queue.put({"ok": False, "error": exc.to_dict()})


def _concurrency_checks(root: Path) -> dict[str, Any]:
    workspace = root / "checkpoint-concurrency"
    if workspace.exists():
        shutil.rmtree(workspace)
    store = CheckpointStore(workspace)
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_publish_worker,
            args=(str(workspace), barrier, queue, marker),
        )
        for marker in ("worker-a", "worker-b")
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    results = sorted(
        results,
        key=lambda item: (
            not item.get("ok"),
            str(item.get("error", {}).get("reason", "")),
            str(item.get("error", {}).get("code", "")),
        ),
    )
    successes = [item for item in results if item.get("ok")]
    failures = [item for item in results if not item.get("ok")]

    duplicate_rejected = False
    duplicate_reason = None
    duplicate_checkpoint = store.read_checkpoint("normalize")
    assert duplicate_checkpoint is not None
    duplicate_artifact = store.verify_artifact(duplicate_checkpoint)
    try:
        store.publish(
            "normalize",
            duplicate_artifact,
            input_fingerprint=duplicate_checkpoint["input_fingerprint"],
            config_version="test",
            config_fingerprint=duplicate_checkpoint["config_fingerprint"],
            expected_prev_sequence=1,
        )
    except BusinessError as exc:
        duplicate_rejected = exc.code == E_STALE_CHECKPOINT
        duplicate_reason = exc.details.get("reason")

    jump_rejected = False
    jump_code = None
    try:
        store.publish(
            "coverage",
            {"stage": "coverage", "shots": {}, "errors": []},
            input_fingerprint="jump-fp",
            config_version="test",
            config_fingerprint="cfg",
            expected_prev_sequence=0,
        )
    except BusinessError as exc:
        jump_code = exc.code
        jump_rejected = True

    upstream_rejected = False
    upstream_reason = None
    try:
        store.publish(
            "duration",
            {"stage": "duration", "shots": {}, "errors": []},
            input_fingerprint="duration-fp",
            config_version="test",
            config_fingerprint="cfg",
            expected_prev_sequence=0,
            upstream={
                "stage": "normalize",
                "sequence": 99,
                "input_fingerprint": "wrong",
                "artifact_digest": "wrong",
            },
        )
    except BusinessError as exc:
        upstream_rejected = exc.code == E_STALE_CHECKPOINT
        upstream_reason = exc.details.get("reason")

    checks = {
        "two_writers_one_winner": _check(
            len(successes) == 1
            and len(failures) == 1
            and failures[0]["error"]["code"] == E_STALE_CHECKPOINT,
            {
                "results": [
                    (
                        {"ok": True, "sequence": item["sequence"]}
                        if item.get("ok")
                        else {
                            "ok": False,
                            "code": item["error"]["code"],
                            "reason": item["error"]["reason"],
                        }
                    )
                    for item in results
                ]
            },
        ),
        "duplicate_publish_rejected": _check(
            duplicate_rejected and duplicate_reason == "duplicate_publish",
            {
                "rejected": duplicate_rejected,
                "reason": duplicate_reason,
            },
        ),
        "invalid_stage_jump_rejected": _check(
            jump_rejected and jump_code == "E_INVALID_STAGE",
            {"rejected": jump_rejected, "code": jump_code},
        ),
        "upstream_lineage_mismatch_rejected": _check(
            upstream_rejected and upstream_reason == "upstream_lineage_mismatch",
            {"rejected": upstream_rejected, "reason": upstream_reason},
        ),
    }
    return {
        "workspace": str(workspace),
        "checks": checks,
        "all_passed": all(item["passed"] for item in checks.values()),
    }


def _fingerprint_matrix(bundle: Any) -> dict[str, Any]:
    base = dependencies(bundle)

    def changed_stages(mutated: Any) -> list[str]:
        mutated_deps = dependencies(mutated)
        return [stage for stage in ("normalize", "duration", "coverage", "strategy") if base[stage] != mutated_deps[stage]]

    cases: list[dict[str, Any]] = []

    prompt_bundle = copy.deepcopy(bundle)
    prompt_bundle.scripts[0]["prompt"] += " extra scene detail"
    cases.append(
        {
            "case": "prompt_change",
            "changed_stages": changed_stages(prompt_bundle),
            "expected_earliest": "normalize",
        }
    )

    duration_bundle = copy.deepcopy(bundle)
    duration_bundle.scripts[0]["duration_phrase"] = "3.3s"
    cases.append(
        {
            "case": "duration_phrase_change",
            "changed_stages": changed_stages(duration_bundle),
            "expected_earliest": "duration",
        }
    )

    subject_bundle = copy.deepcopy(bundle)
    first_subject = next(iter(subject_bundle.subjects["shots"].values()))
    first_subject.setdefault("forbidden_subjects", []).append("shadow")
    cases.append(
        {
            "case": "subject_table_change",
            "changed_stages": changed_stages(subject_bundle),
            "expected_earliest": "normalize",
        }
    )

    negative_bundle = copy.deepcopy(bundle)
    negative_bundle.scripts[0].setdefault("negative_terms", []).append("avoid")
    cases.append(
        {
            "case": "negative_terms_change",
            "changed_stages": changed_stages(negative_bundle),
            "expected_earliest": "normalize",
        }
    )

    weights_bundle = copy.deepcopy(bundle)
    weights_bundle.config["coverage"]["weights"]["must_subject_coverage"] = 0.5
    cases.append(
        {
            "case": "weights_change",
            "changed_stages": changed_stages(weights_bundle),
            "expected_earliest": "coverage",
        }
    )

    budget_bundle = copy.deepcopy(bundle)
    budget_bundle.config["strategy"]["check_depth_budget"] = 123456
    cases.append(
        {
            "case": "budget_change",
            "changed_stages": changed_stages(budget_bundle),
            "expected_earliest": "strategy",
        }
    )

    split_bundle = copy.deepcopy(bundle)
    split_bundle.config["split"]["test_variant"] = "changed"
    cases.append(
        {
            "case": "split_config_change",
            "changed_stages": changed_stages(split_bundle),
            "expected_earliest": "normalize",
        }
    )

    stage_order = {"normalize": 1, "duration": 2, "coverage": 3, "strategy": 4}
    for case in cases:
        case["passed"] = bool(case["changed_stages"]) and min(
            case["changed_stages"], key=lambda stage: stage_order[stage]
        ) == case["expected_earliest"]
    return {
        "cases": cases,
        "all_passed": all(case["passed"] for case in cases),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Deterministic Storyboard Pipeline Verification",
        "",
        f"- Pipeline version: `{report['pipeline_version']}`",
        f"- Seed: `{report['seed']}`",
        f"- Overall: **{'PASS' if report['all_passed'] else 'FAIL'}**",
        "",
        "## Tier Results",
        "",
    ]
    for tier, item in report["tiers"].items():
        status = "PASS" if item["all_passed"] else "FAIL"
        lines.extend(
            [
                f"### {tier} ({status})",
                "",
                f"- Shots: `{item['shot_count']}`",
                f"- Verdicts: `{item['summary']['verdict_distribution']}`",
                f"- Errors: `{item['summary']['error_distribution']}`",
                f"- Unit types: `{item['summary']['unit_type_distribution']}`",
                f"- Reference emitted unit types: `{item['reference_emitted_unit_type_distribution']}`",
                f"- Reference unit types: `{item['reference_unit_type_distribution']}`",
                f"- Duration sources: `{item['summary']['duration_source_distribution']}`",
                f"- N/A components: `{item['summary']['na_component_distribution']}`",
                f"- Strategy: `{item['strategy']['selected']}` ({item['strategy']['selected_reason']})",
                f"- Result digest: `{item['result_digest']}`",
                "",
            ]
        )
        for name, check in item["checks"].items():
            lines.append(f"- `{name}`: {'PASS' if check['passed'] else 'FAIL'}")
        lines.append("")

    lines.extend(["## Checkpoint Concurrency", ""])
    for name, check in report["checkpoint_concurrency"]["checks"].items():
        lines.append(f"- `{name}`: {'PASS' if check['passed'] else 'FAIL'}")
    lines.append("")

    lines.extend(["## Incremental Recompute", ""])
    for name, check in report["incremental"]["checks"].items():
        lines.append(f"- `{name}`: {'PASS' if check['passed'] else 'FAIL'}")
    lines.extend(
        [
            f"- Changed shot: `{report['incremental']['mutation']['shot_id']}`",
            f"- Duration: `{report['incremental']['mutation']['old_target_ms']}` -> "
            f"`{report['incremental']['mutation']['new_target_ms']}` ms",
            "",
        ]
    )

    lines.extend(["## Graceful Resume", ""])
    for name, check in report["resume"]["checks"].items():
        lines.append(f"- `{name}`: {'PASS' if check['passed'] else 'FAIL'}")
    lines.append("")

    lines.extend(["## Version Fingerprint Matrix", ""])
    for case in report["fingerprint_matrix"]["cases"]:
        lines.append(
            f"- `{case['case']}`: {'PASS' if case['passed'] else 'FAIL'} "
            f"(changed: `{case['changed_stages']}`, earliest: `{case['expected_earliest']}`)"
        )
    lines.append("")
    return "\n".join(lines)


def verify_all(root: str | Path, *, seed: int = 20261123) -> dict[str, Any]:
    root_path = Path(root)
    ensure_dir(root_path)
    generate_all(root_path, seed=seed)
    verification_root = ensure_dir(root_path / "verification")
    if verification_root.exists():
        for item in verification_root.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

    baselines: dict[str, Any] = {}
    tier_reports: dict[str, Any] = {}
    baseline_bytes: dict[str, bytes] = {}
    for tier in ("small", "large"):
        workspace = verification_root / "baseline" / tier
        _fresh_from_inputs(root_path / tier, workspace)
        run = Pipeline(workspace).run()
        baseline_bytes[tier] = (workspace / "exports/result.json").read_bytes()
        baselines[tier] = run
        tier_reports[tier] = _validate_tier(workspace, run["result"], run["run_meta"])

    incremental_workspace = verification_root / "incremental"
    _clone_workspace(verification_root / "baseline/large", incremental_workspace)
    mutation = _mutate_duration(incremental_workspace)
    incremental_run = Pipeline(incremental_workspace).run()
    incremental_result = incremental_run["result"]
    baseline_result = baselines["large"]["result"]
    unaffected_diff = [
        shot_id
        for shot_id, outcome in baseline_result["outcomes"].items()
        if shot_id != mutation["shot_id"]
        and canonical_bytes(outcome)
        != canonical_bytes(incremental_result["outcomes"][shot_id])
    ]

    fresh_mutated = verification_root / "fresh-mutated"
    _fresh_from_inputs(incremental_workspace, fresh_mutated)
    fresh_mutated_run = Pipeline(fresh_mutated).run()
    incremental_bytes = (incremental_workspace / "exports/result.json").read_bytes()
    fresh_mutated_bytes = (fresh_mutated / "exports/result.json").read_bytes()
    baseline_normalize = (
        verification_root / "baseline/large/objects"
    )
    incremental_checkpoints = {
        stage: CheckpointStore(incremental_workspace).read_checkpoint(stage)
        for stage in ("normalize", "duration", "coverage", "strategy")
    }
    incremental_checks = {
        "earliest_stage_is_duration": _check(
            incremental_run["run_meta"]["invalidation"]["earliest_stage"]
            == "duration",
            incremental_run["run_meta"]["invalidation"],
        ),
        "normalize_stage_reused": _check(
            "normalize"
            in incremental_run["run_meta"]["invalidation"]["reused_stages"]
            and "normalize"
            not in incremental_run["run_meta"]["invalidation"]["rebuilt_stages"],
            incremental_run["run_meta"]["invalidation"],
        ),
        "unaffected_shots_byte_identical": _check(
            not unaffected_diff,
            {
                "unaffected_shot_count": len(incremental_result["outcomes"]) - 1,
                "different_shots": unaffected_diff,
            },
        ),
        "incremental_equals_fresh_full": _check(
            incremental_bytes == fresh_mutated_bytes,
            {
                "incremental_digest": sha256_bytes(incremental_bytes),
                "fresh_digest": sha256_bytes(fresh_mutated_bytes),
            },
        ),
        "sequence_advanced_only_downstream": _check(
            incremental_checkpoints["normalize"]["sequence"] == 1
            and incremental_checkpoints["duration"]["sequence"] == 2
            and incremental_checkpoints["coverage"]["sequence"] == 2
            and incremental_checkpoints["strategy"]["sequence"] == 2,
            {
                stage: checkpoint["sequence"]
                for stage, checkpoint in incremental_checkpoints.items()
            },
        ),
    }

    resume_workspace = verification_root / "resume"
    _fresh_from_inputs(root_path / "large", resume_workspace)
    stop_controller = StopController()
    stop_controller.configure_target("coverage", 2)
    stopped: dict[str, Any] = {}
    try:
        Pipeline(resume_workspace, stop_controller=stop_controller).run()
    except GracefulStop as exc:
        stopped = {
            "stage": exc.stage,
            "completed_shots": exc.completed_shots,
            "shot_id": exc.shot_id,
        }
    resume_store = CheckpointStore(resume_workspace)
    checkpoints_after_stop = {
        stage: resume_store.read_checkpoint(stage)
        for stage in ("normalize", "duration", "coverage", "strategy")
    }
    result_exists_after_stop = (resume_workspace / "exports/result.json").exists()
    resumed = Pipeline(resume_workspace).run()
    resumed_bytes = (resume_workspace / "exports/result.json").read_bytes()
    resume_checks = {
        "stopped_after_current_shot": _check(
            stopped.get("stage") == "coverage"
            and stopped.get("completed_shots") == 2,
            stopped,
        ),
        "no_partial_stage_publish": _check(
            checkpoints_after_stop["normalize"] is not None
            and checkpoints_after_stop["duration"] is not None
            and checkpoints_after_stop["coverage"] is None
            and checkpoints_after_stop["strategy"] is None
            and not result_exists_after_stop,
            {
                "checkpoints": {
                    stage: checkpoint["sequence"] if checkpoint else None
                    for stage, checkpoint in checkpoints_after_stop.items()
                },
                "result_exists": result_exists_after_stop,
            },
        ),
        "resume_reuses_upstream": _check(
            resumed["run_meta"]["invalidation"]["earliest_stage"] == "coverage"
            and resumed["run_meta"]["invalidation"]["reused_stages"]
            == ["normalize", "duration"],
            resumed["run_meta"]["invalidation"],
        ),
        "resume_equals_fresh_full": _check(
            resumed_bytes == baseline_bytes["large"],
            {
                "resumed_digest": sha256_bytes(resumed_bytes),
                "baseline_digest": sha256_bytes(baseline_bytes["large"]),
            },
        ),
    }

    concurrency = _concurrency_checks(verification_root)
    fingerprint_matrix = _fingerprint_matrix(load_inputs(root_path / "large"))

    all_passed = (
        all(item["all_passed"] for item in tier_reports.values())
        and all(item["passed"] for item in incremental_checks.values())
        and all(item["passed"] for item in resume_checks.values())
        and concurrency["all_passed"]
        and fingerprint_matrix["all_passed"]
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "seed": seed,
        "all_passed": all_passed,
        "tiers": tier_reports,
        "incremental": {
            "workspace": str(incremental_workspace),
            "fresh_workspace": str(fresh_mutated),
            "mutation": mutation,
            "invalidation": incremental_run["run_meta"]["invalidation"],
            "checks": incremental_checks,
        },
        "resume": {
            "workspace": str(resume_workspace),
            "stopped": stopped,
            "invalidation": resumed["run_meta"]["invalidation"],
            "checks": resume_checks,
        },
        "checkpoint_concurrency": concurrency,
        "fingerprint_matrix": fingerprint_matrix,
    }
    atomic_write_json(verification_root / "report.json", report)
    from .util import atomic_write_bytes

    atomic_write_bytes(
        verification_root / "report.md", _render_markdown(report).encode("utf-8")
    )
    return report
