from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .checkpoints import STAGES, CheckpointStore
from .config import default_config
from .duration import parse_duration_phrase
from .errors import BusinessError, E_STALE_CHECKPOINT, sort_errors
from .generator import generate_all
from .matching import METRIC_SCORE
from .strategies import STRATEGIES, choose_strategy, run_strategy
from .text import empty_prompt_error, normalize_and_split
from .util import (
    atomic_write_json,
    canonical_bytes,
    ensure_dir,
    read_json,
    read_jsonl,
    sha256_obj,
)
from .version import PIPELINE_VERSION, SCHEMA_VERSION


class GracefulStop(Exception):
    def __init__(self, stage: str, completed_shots: int, shot_id: str | None = None):
        super().__init__(
            f"graceful stop requested after shot {shot_id or '-'} "
            f"({completed_shots} completed in {stage})"
        )
        self.stage = stage
        self.completed_shots = completed_shots
        self.shot_id = shot_id


class StopController:
    """Thread-safe stop request checked only at indivisible shot boundaries."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self.target_stage: str | None = None
        self.stop_after_shots: int | None = None
        self._current_stage: str | None = None
        self._completed = 0

    def request(self) -> None:
        self._event.set()

    def configure_target(self, stage: str | None, after_shots: int | None = None) -> None:
        with self._lock:
            self.target_stage = stage
            self.stop_after_shots = after_shots

    def is_requested(self) -> bool:
        return self._event.is_set()

    def before_stage(self, stage: str) -> None:
        with self._lock:
            self._current_stage = stage
            self._completed = 0
            should_stop = self._event.is_set()
            target_matches = self.target_stage in (None, stage)
        if should_stop and target_matches:
            raise GracefulStop(stage, 0)

    def after_shot(self, shot_id: str) -> None:
        with self._lock:
            self._completed += 1
            stage = self._current_stage
            target_matches = self.target_stage in (None, stage)
            threshold_reached = (
                self.target_stage in (None, stage)
                and self.stop_after_shots is not None
                and self._completed >= self.stop_after_shots
            )
            requested = self._event.is_set()
            completed = self._completed
        if target_matches and (requested or threshold_reached):
            self._event.set()
            raise GracefulStop(stage or "unknown", completed, shot_id)


@dataclass
class InputBundle:
    workspace: Path
    scripts: list[dict[str, Any]]
    subjects: dict[str, Any]
    reference: dict[str, Any]
    config: dict[str, Any]

    @property
    def scripts_by_id(self) -> dict[str, dict[str, Any]]:
        return {shot["shot_id"]: shot for shot in self.scripts}

    @property
    def subjects_by_shot(self) -> dict[str, dict[str, Any]]:
        return self.subjects.get("shots", {})

    @property
    def reference_by_shot(self) -> dict[str, Any]:
        return self.reference.get("shots", {})

    @property
    def tier(self) -> str:
        return str(self.reference.get("tier", self.workspace.name))


def load_inputs(workspace: str | Path) -> InputBundle:
    workspace_path = Path(workspace)
    inputs_dir = workspace_path / "inputs"
    scripts = read_jsonl(inputs_dir / "scripts.jsonl")
    subjects = read_json(inputs_dir / "subjects.json")
    reference = read_json(inputs_dir / "reference.json")
    config_path = inputs_dir / "config.json"
    config = read_json(config_path) if config_path.exists() else default_config()
    if not isinstance(subjects, dict) or not isinstance(reference, dict):
        raise ValueError("subjects and reference inputs must be JSON objects")
    ids = [shot.get("shot_id") for shot in scripts]
    if any(not shot_id for shot_id in ids):
        raise ValueError("every script shot must have a shot_id")
    if len(ids) != len(set(ids)):
        raise ValueError("script shot_id values must be unique")
    return InputBundle(
        workspace=workspace_path,
        scripts=scripts,
        subjects=subjects,
        reference=reference,
        config=config,
    )


def _stage1_dependency(bundle: InputBundle) -> dict[str, Any]:
    table = bundle.subjects_by_shot
    rows = []
    for shot in bundle.scripts:
        entry = table.get(shot["shot_id"], {})
        rows.append(
            {
                "shot_id": shot["shot_id"],
                "prompt": shot.get("prompt", ""),
                "required_subjects": entry.get("required_subjects", []),
                "forbidden_subjects": entry.get("forbidden_subjects", []),
                "must_scenes": shot.get("must_scenes", []),
                "negative_terms": shot.get("negative_terms", []),
            }
        )
    return {
        "pipeline_version": PIPELINE_VERSION,
        "prompts_and_lexicons": rows,
        "split_config": bundle.config.get("split", {}),
    }


def _stage2_dependency(bundle: InputBundle) -> dict[str, Any]:
    return {
        "pipeline_version": PIPELINE_VERSION,
        "duration_phrases": [
            {
                "shot_id": shot["shot_id"],
                "duration_phrase": shot.get("duration_phrase", ""),
            }
            for shot in bundle.scripts
        ],
        "duration_config": bundle.config.get("duration", {}),
    }


def _stage3_dependency(bundle: InputBundle) -> dict[str, Any]:
    return {
        "pipeline_version": PIPELINE_VERSION,
        "subject_table": bundle.subjects,
        "scripts": [
            {
                "shot_id": shot["shot_id"],
                "manual_subjects": shot.get("must_subjects", []),
                "must_scenes": shot.get("must_scenes", []),
                "negative_terms": shot.get("negative_terms", []),
            }
            for shot in bundle.scripts
        ],
        "reference": bundle.reference,
        "coverage_config": bundle.config.get("coverage", {}),
    }


def _scale_metrics(bundle: InputBundle) -> dict[str, Any]:
    from .text import normalize_text

    table = bundle.subjects_by_shot
    keyword_count = 0
    for shot in bundle.scripts:
        entry = table.get(shot["shot_id"], {})
        keyword_count += len(entry.get("required_subjects", []))
        keyword_count += len(entry.get("forbidden_subjects", []))
        keyword_count += len(shot.get("must_scenes", []))
        keyword_count += len(shot.get("negative_terms", []))
    return {
        "shot_count": len(bundle.scripts),
        "total_prompt_chars": sum(
            len(normalize_text(str(shot.get("prompt", ""))))
            for shot in bundle.scripts
        ),
        "negative_constraint_count": sum(
            len(shot.get("negative_terms", [])) for shot in bundle.scripts
        ),
        "keyword_count": keyword_count,
    }


def _stage4_dependency(bundle: InputBundle) -> dict[str, Any]:
    return {
        "pipeline_version": PIPELINE_VERSION,
        "strategy_config": bundle.config.get("strategy", {}),
        "scale_metrics": _scale_metrics(bundle),
    }


def dependencies(bundle: InputBundle) -> dict[str, str]:
    payloads = {
        "normalize": _stage1_dependency(bundle),
        "duration": _stage2_dependency(bundle),
        "coverage": _stage3_dependency(bundle),
        "strategy": _stage4_dependency(bundle),
    }
    return {stage: sha256_obj(payload) for stage, payload in payloads.items()}


def pipeline_version_fingerprint(bundle: InputBundle) -> str:
    return sha256_obj(
        {
            "pipeline_version": PIPELINE_VERSION,
            "dependencies": dependencies(bundle),
            "reference_digest": sha256_obj(bundle.reference),
        }
    )


def _stage_config(bundle: InputBundle, stage: str) -> dict[str, Any]:
    config = bundle.config
    if stage == "normalize":
        return config.get("split", {})
    if stage == "duration":
        return config.get("duration", {})
    if stage == "coverage":
        return config.get("coverage", {})
    if stage == "strategy":
        return config.get("strategy", {})
    raise ValueError(stage)


def _config_version(bundle: InputBundle) -> str:
    return f"{PIPELINE_VERSION}:{bundle.config.get('config_version', 'unversioned')}"


def _error_counts(errors: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for error in errors:
        counts[error["code"]] = counts.get(error["code"], 0) + 1
    return dict(sorted(counts.items()))


def _deduplicate_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_bytes: dict[bytes, dict[str, Any]] = {}
    for error in errors:
        by_bytes[canonical_bytes(error)] = error
    return sort_errors(list(by_bytes.values()))


def _run_normalize(
    bundle: InputBundle, stop: StopController
) -> dict[str, Any]:
    shots: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    unit_type_counts: dict[str, int] = {}
    for shot in bundle.scripts:
        shot_id = shot["shot_id"]
        entry = bundle.subjects_by_shot.get(shot_id, {})
        subject_terms = [
            *entry.get("required_subjects", []),
            *entry.get("forbidden_subjects", []),
        ]
        normalized = normalize_and_split(
            shot.get("prompt", ""),
            subject_terms=subject_terms,
            scene_terms=shot.get("must_scenes", []),
            negative_terms=shot.get("negative_terms", []),
        )
        if not normalized["normalized_prompt"] or not any(
            unit["type"] == "subject_description" for unit in normalized["units"]
        ):
            errors.append(
                empty_prompt_error(
                    shot_id,
                    normalized_prompt=normalized["normalized_prompt"],
                    units=normalized["units"],
                )
            )
        else:
            shots[shot_id] = normalized
            for unit in normalized["units"]:
                unit_type_counts[unit["type"]] = (
                    unit_type_counts.get(unit["type"], 0) + 1
                )
        stop.after_shot(shot_id)
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "normalize",
        "shots": shots,
        "errors": errors,
        "summary": {
            "shot_count": len(bundle.scripts),
            "emitted_shot_count": len(shots),
            "error_count": len(errors),
            "unit_count": sum(unit_type_counts.values()),
            "unit_type_distribution": dict(sorted(unit_type_counts.items())),
        },
    }


def _run_duration(bundle: InputBundle, stop: StopController) -> dict[str, Any]:
    fps = bundle.config.get("duration", {}).get("fps", 25)
    shots: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}
    for shot in bundle.scripts:
        shot_id = shot["shot_id"]
        parsed = parse_duration_phrase(
            shot_id, shot.get("duration_phrase", ""), fps=fps
        )
        if parsed["ok"]:
            result = parsed["result"]
            shots[shot_id] = result
            source = result["source"]
            source_counts[source] = source_counts.get(source, 0) + 1
        else:
            errors.append(parsed["error"])
        stop.after_shot(shot_id)
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "duration",
        "shots": shots,
        "errors": errors,
        "summary": {
            "shot_count": len(bundle.scripts),
            "parsed_shot_count": len(shots),
            "error_count": len(errors),
            "source_distribution": dict(sorted(source_counts.items())),
            "fps": fps,
        },
    }


def _strategy_jobs(
    bundle: InputBundle,
    normalize_artifact: dict[str, Any],
    duration_artifact: dict[str, Any],
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for shot in bundle.scripts:
        shot_id = shot["shot_id"]
        if (
            shot_id not in normalize_artifact["shots"]
            or shot_id not in duration_artifact["shots"]
        ):
            continue
        jobs.append(
            {
                "shot": shot,
                "subject_entry": bundle.subjects_by_shot.get(shot_id, {}),
                "reference_shot": bundle.reference_by_shot.get(shot_id),
                "normalize_result": normalize_artifact["shots"][shot_id],
                "duration_result": duration_artifact["shots"][shot_id],
            }
        )
    return jobs


def _run_coverage(
    bundle: InputBundle,
    normalize_artifact: dict[str, Any],
    duration_artifact: dict[str, Any],
    stop: StopController,
) -> dict[str, Any]:
    selection = choose_strategy(bundle.scripts, bundle.subjects, bundle.config)
    jobs = _strategy_jobs(bundle, normalize_artifact, duration_artifact)
    executed = run_strategy(
        selection["strategy"],
        jobs,
        bundle.config,
        after_shot=lambda _count, shot_id: stop.after_shot(shot_id),
    )
    decisions = executed["decisions"]
    errors = _deduplicate_errors(
        [*normalize_artifact.get("errors", []), *duration_artifact.get("errors", [])]
    )
    for decision in decisions.values():
        errors.extend(decision.get("errors", []))
    errors = _deduplicate_errors(errors)

    verdict_counts: dict[str, int] = {}
    na_counts: dict[str, int] = {}
    for decision in decisions.values():
        verdict = decision["verdict"]
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        for component in decision["na_components"]:
            na_counts[component] = na_counts.get(component, 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "coverage",
        "shots": decisions,
        "errors": errors,
        "summary": {
            "analyzable_shot_count": len(jobs),
            "error_count": len(errors),
            "error_distribution": _error_counts(errors),
            "verdict_distribution": dict(sorted(verdict_counts.items())),
            "na_component_distribution": dict(sorted(na_counts.items())),
        },
    }


def _run_strategy_stage(
    bundle: InputBundle,
    normalize_artifact: dict[str, Any],
    duration_artifact: dict[str, Any],
    stop: StopController,
) -> dict[str, Any]:
    selection = choose_strategy(bundle.scripts, bundle.subjects, bundle.config)
    jobs = _strategy_jobs(bundle, normalize_artifact, duration_artifact)
    parity: dict[str, Any] = {"digests": {}, "all_equal": True}
    stats: dict[str, Any] = {}
    reference_bytes: bytes | None = None
    for strategy in STRATEGIES:
        executed = run_strategy(
            strategy,
            jobs,
            bundle.config,
            after_shot=lambda _count, shot_id: stop.after_shot(shot_id),
        )
        decision_bytes = canonical_bytes(executed["decisions"])
        digest = sha256_obj(executed["decisions"])
        parity["digests"][strategy] = digest
        stats[strategy] = executed["stats"]
        if reference_bytes is None:
            reference_bytes = decision_bytes
        elif decision_bytes != reference_bytes:
            parity["all_equal"] = False
    parity["decision_digest"] = parity["digests"].get(selection["strategy"])
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": "strategy",
        "selection": selection,
        "parity": parity,
        "execution": stats,
        "summary": {
            "selected_strategy": selection["strategy"],
            "selected_reason": selection["selected_reason"],
            "all_strategies_equal": parity["all_equal"],
            "analyzed_shot_count": len(jobs),
        },
    }


def _detect_invalidation(
    store: CheckpointStore, expected_dependencies: dict[str, str]
) -> tuple[int | None, str | None]:
    checkpoints: dict[str, dict[str, Any] | None] = {}
    for stage, index, _ in STAGES:
        try:
            checkpoint = store.read_checkpoint(stage)
        except BusinessError:
            raise
        checkpoints[stage] = checkpoint
        if checkpoint is None:
            return index, "missing_checkpoint"
        if checkpoint.get("input_fingerprint") != expected_dependencies[stage]:
            return index, "input_fingerprint_changed"
        try:
            store.verify_artifact(checkpoint)
        except BusinessError as exc:
            if exc.code == E_STALE_CHECKPOINT and exc.details.get("reason") in {
                "artifact_missing",
                "artifact_digest_mismatch",
                "artifact_corrupt",
            }:
                return index, str(exc.details.get("reason"))
            raise
        if index > 1:
            previous_stage = STAGES[index - 2][0]
            previous = checkpoints[previous_stage]
            if previous is None:
                return index - 1, "missing_checkpoint"
            expected_upstream = {
                "stage": previous_stage,
                "sequence": previous["sequence"],
                "input_fingerprint": previous["input_fingerprint"],
                "artifact_digest": previous["artifact_digest"],
            }
            if checkpoint.get("upstream") != [expected_upstream]:
                return index, "upstream_lineage_mismatch"
    return None, None


def _build_result(
    bundle: InputBundle,
    artifacts: dict[str, dict[str, Any]],
    version_fingerprint: str,
) -> dict[str, Any]:
    normalize_artifact = artifacts["normalize"]
    duration_artifact = artifacts["duration"]
    coverage_artifact = artifacts["coverage"]
    strategy_artifact = artifacts["strategy"]
    errors = _deduplicate_errors(coverage_artifact.get("errors", []))
    decisions: dict[str, Any] = coverage_artifact.get("shots", {})
    outcomes: dict[str, Any] = {}
    for shot in bundle.scripts:
        shot_id = shot["shot_id"]
        decision = decisions.get(shot_id)
        if decision is not None:
            outcomes[shot_id] = {
                "verdict": decision["verdict"],
                "score": decision["score"],
                "coverage": decision["coverage"],
                "reasons": decision["reasons"],
                "error_codes": sorted(
                    {error["code"] for error in decision.get("errors", [])}
                ),
                "na_components": decision["na_components"],
            }
        else:
            shot_errors = [error for error in errors if error.get("shot_id") == shot_id]
            outcomes[shot_id] = {
                "verdict": "rejected",
                "score": None,
                "coverage": None,
                "reasons": ["stage_error"],
                "error_codes": sorted({error["code"] for error in shot_errors}),
                "na_components": [],
            }

    verdict_counts: dict[str, int] = {}
    na_counts: dict[str, int] = {}
    for outcome in outcomes.values():
        verdict = outcome["verdict"]
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        for component in outcome["na_components"]:
            na_counts[component] = na_counts.get(component, 0) + 1

    return {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "tier": bundle.tier,
        "version_fingerprint": version_fingerprint,
        "reference_digest": sha256_obj(bundle.reference),
        "inputs_digest": sha256_obj(
            {
                "scripts": bundle.scripts,
                "subjects": bundle.subjects,
                "reference": bundle.reference,
                "config": bundle.config,
            }
        ),
        "stage_artifact_digests": {
            stage: sha256_obj(artifact) for stage, artifact in artifacts.items()
        },
        "outputs": {
            "normalize": normalize_artifact,
            "duration": duration_artifact,
            "coverage": coverage_artifact,
            "strategy": strategy_artifact,
        },
        "shots": decisions,
        "outcomes": outcomes,
        "errors": errors,
        "summary": {
            "shot_count": len(bundle.scripts),
            "verdict_distribution": dict(sorted(verdict_counts.items())),
            "error_distribution": _error_counts(errors),
            "unit_type_distribution": normalize_artifact["summary"][
                "unit_type_distribution"
            ],
            "duration_source_distribution": duration_artifact["summary"][
                "source_distribution"
            ],
            "na_component_distribution": dict(sorted(na_counts.items())),
            "all_strategies_equal": strategy_artifact["parity"]["all_equal"],
        },
        "strategy_selection": strategy_artifact["selection"],
        "strategy_parity": strategy_artifact["parity"],
    }


class Pipeline:
    def __init__(
        self,
        workspace: str | Path,
        *,
        stop_controller: StopController | None = None,
    ):
        self.workspace = Path(workspace)
        self.store = CheckpointStore(self.workspace)
        self.stop = stop_controller or StopController()

    def run(self, *, force: bool = False) -> dict[str, Any]:
        bundle = load_inputs(self.workspace)
        expected_dependencies = dependencies(bundle)
        version_fingerprint = pipeline_version_fingerprint(bundle)
        earliest_index, invalidation_reason = (
            (1, "forced_rebuild")
            if force
            else _detect_invalidation(self.store, expected_dependencies)
        )

        artifacts: dict[str, dict[str, Any]] = {}
        stage_status: list[dict[str, Any]] = []
        config_version = _config_version(bundle)

        for stage, index, _ in STAGES:
            must_rebuild = earliest_index is not None and index >= earliest_index
            if must_rebuild:
                before = self.store.read_checkpoint(stage)
                expected_prev = int(before["sequence"]) if before else 0
                upstream = self.store.upstream_descriptor(stage)
                self.stop.before_stage(stage)
                if stage == "normalize":
                    artifact = _run_normalize(bundle, self.stop)
                elif stage == "duration":
                    artifact = _run_duration(bundle, self.stop)
                elif stage == "coverage":
                    artifact = _run_coverage(
                        bundle, artifacts["normalize"], artifacts["duration"], self.stop
                    )
                elif stage == "strategy":
                    artifact = _run_strategy_stage(
                        bundle, artifacts["normalize"], artifacts["duration"], self.stop
                    )
                else:
                    raise AssertionError(stage)
                config_payload = _stage_config(bundle, stage)
                config_fingerprint = sha256_obj(config_payload)
                artifact_digest = sha256_obj(artifact)
                identical_recompute = bool(
                    before
                    and before.get("input_fingerprint")
                    == expected_dependencies[stage]
                    and before.get("config_fingerprint") == config_fingerprint
                    and before.get("artifact_digest") == artifact_digest
                    and before.get("upstream")
                    == ([] if stage == "normalize" else ([upstream] if upstream else []))
                )
                if identical_recompute:
                    checkpoint = before
                    status = "recomputed_unchanged"
                else:
                    checkpoint = self.store.publish(
                        stage,
                        artifact,
                        input_fingerprint=expected_dependencies[stage],
                        config_version=config_version,
                        config_fingerprint=config_fingerprint,
                        expected_prev_sequence=expected_prev,
                        upstream=upstream if stage != "normalize" else None,
                    )
                    status = "rebuilt"
                artifacts[stage] = artifact
                stage_status.append(
                    {
                        "stage": stage,
                        "status": status,
                        "sequence": checkpoint["sequence"],
                        "artifact_digest": checkpoint["artifact_digest"],
                    }
                )
            else:
                artifact = self.store.load_artifact(stage)
                if artifact is None:
                    raise AssertionError(f"reusable stage {stage} has no artifact")
                artifacts[stage] = artifact
                checkpoint = self.store.read_checkpoint(stage)
                assert checkpoint is not None
                stage_status.append(
                    {
                        "stage": stage,
                        "status": "reused",
                        "sequence": checkpoint["sequence"],
                        "artifact_digest": checkpoint["artifact_digest"],
                    }
                )

        invalidation = {
            "earliest_stage": (
                STAGES[earliest_index - 1][0] if earliest_index is not None else None
            ),
            "reason": invalidation_reason,
            "rebuilt_stages": [
                entry["stage"]
                for entry in stage_status
                if entry["status"] in {"rebuilt", "recomputed_unchanged"}
            ],
            "reused_stages": [
                entry["stage"] for entry in stage_status if entry["status"] == "reused"
            ],
        }
        result = _build_result(bundle, artifacts, version_fingerprint)
        exports_dir = ensure_dir(self.workspace / "exports")
        result_path = exports_dir / "result.json"
        atomic_write_json(result_path, result)
        run_meta = {
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "tier": bundle.tier,
            "status": "complete",
            "version_fingerprint": version_fingerprint,
            "invalidation": invalidation,
            "stages": stage_status,
            "result_digest": sha256_obj(result),
        }
        atomic_write_json(exports_dir / "run-meta.json", run_meta)
        return {"result": result, "run_meta": run_meta, "artifacts": artifacts}


def prepare_and_run(
    root: str | Path,
    *,
    seed: int = 20261123,
    tiers: tuple[str, ...] = ("small", "large"),
    force: bool = False,
) -> dict[str, Any]:
    root_path = Path(root)
    generate_all(root_path, seed=seed)
    results: dict[str, Any] = {}
    for tier in tiers:
        results[tier] = Pipeline(root_path / tier).run(force=force)
    return results
