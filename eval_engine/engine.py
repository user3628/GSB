"""Four-stage incremental evaluation engine orchestration."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .aggregation import run_stage2
from .errors import EngineError, StopRequested
from .inputs import replace_sample_batch, update_benchmark_answers
from .locking import file_lock
from .profiling import run_stage3
from .ranking import run_stage4
from .scoring import run_stage1
from .store import CheckpointStore, STAGE_NAMES, WorkspaceContext
from .util import read_json, sha256_bytes


class StopController:
    """Deterministic graceful-stop signal checked only between aggregate units."""

    def __init__(
        self,
        *,
        stop_file: Path | None = None,
        after_units: int | None = None,
        after_stage: int | None = None,
    ) -> None:
        self.stop_file = Path(stop_file) if stop_file is not None else None
        self.after_units = after_units
        self.after_stage = after_stage
        self.completed_units = 0
        self.current_stage: int | None = None

    def _check_file(self, stage: int | None = None) -> None:
        if self.stop_file is not None and self.stop_file.exists():
            raise StopRequested(self.completed_units, stage=stage)

    def maybe_stop_before_stage(self, stage: int) -> None:
        self.current_stage = stage
        self._check_file(stage)
        if self.after_stage is not None and stage > self.after_stage:
            raise StopRequested(self.completed_units, stage=stage)

    def unit_completed(self) -> None:
        self.completed_units += 1
        self._check_file(self.current_stage)
        if self.after_units is not None and self.completed_units >= self.after_units:
            raise StopRequested(self.completed_units, stage=self.current_stage)


@dataclass
class BuildResult:
    status: str
    inspection: dict[str, Any]
    ranking: dict[str, Any] | None
    checkpoints_published: list[dict[str, Any]] = field(default_factory=list)
    stopped_stage: int | None = None
    verification: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": self.status != "stopped",
            "status": self.status,
            "checkpoints_published": self.checkpoints_published,
            "inspection": self.inspection,
            "ranking": self.ranking,
        }
        if self.status == "stopped":
            result["code"] = "E_STOPPED"
            result["stopped_stage"] = self.stopped_stage
        if self.verification is not None:
            result["verification"] = self.verification
        return result


def parse_stage(value: Any) -> int:
    if isinstance(value, int) and value in STAGE_NAMES:
        return value
    aliases = {
        "1": 1,
        "stage1": 1,
        "score": 1,
        "scoring": 1,
        "normalize_and_score": 1,
        "2": 2,
        "stage2": 2,
        "aggregate": 2,
        "aggregation": 2,
        "aggregate_and_exclude": 2,
        "3": 3,
        "stage3": 3,
        "profile": 3,
        "profiling": 3,
        "build_capability_profiles": 3,
        "4": 4,
        "stage4": 4,
        "ranking": 4,
        "rank": 4,
        "rank_and_select": 4,
    }
    if isinstance(value, str) and value.lower() in aliases:
        return aliases[value.lower()]
    raise EngineError(
        "E_INVALID_STAGE",
        "unknown evaluation stage",
        details={"stage": value, "allowed_stages": list(STAGE_NAMES)},
    )


class Engine:
    def __init__(
        self,
        workspace: str | Path,
        *,
        samples_dir: str | Path | None = None,
        config_path: str | Path | None = None,
        answers_path: str | Path | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.samples_dir = Path(samples_dir).resolve() if samples_dir else self.workspace / "samples"
        self.config_path = Path(config_path).resolve() if config_path else self.workspace / "configs" / "engine.json"
        self.answers_path = Path(answers_path).resolve() if answers_path else self.workspace / "configs" / "answers.json"
        self.store = CheckpointStore(
            self.workspace,
            samples_dir=self.samples_dir,
            config_path=self.config_path,
            answers_path=self.answers_path,
        )
        self.stop_file = self.workspace / "work" / "STOP"

    def ensure_workspace(self) -> None:
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.workspace.joinpath("work").mkdir(parents=True, exist_ok=True)
        self.store.ensure_directories()

    def status(self) -> dict[str, Any]:
        return {"ok": True, "data": self.store.inspect()}

    def _load_current_ranking(self) -> dict[str, Any]:
        inspection, artifact, _ = self.store.inspect_with_stage(4)
        if inspection["invalid_stages"] or artifact is None:
            raise EngineError(
                "E_INVALID_STAGE",
                "the current ranking artifact is not valid",
                details={
                    "invalid_stages": inspection["invalid_stages"],
                    "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                },
            )
        return artifact

    def _run_stage(
        self,
        stage: int,
        upstream: Any,
        context: WorkspaceContext,
        controller: StopController,
    ) -> dict[str, Any]:
        if stage == 1:
            return run_stage1(
                context.bundle,
                context.config,
                context.stage_dependencies[1],
                on_unit=controller.unit_completed,
            )
        if stage == 2:
            return run_stage2(
                upstream,
                context.config,
                context.stage_dependencies[2],
                on_unit=controller.unit_completed,
            )
        if stage == 3:
            return run_stage3(
                upstream,
                context.config,
                context.stage_dependencies[3],
                on_unit=controller.unit_completed,
            )
        return run_stage4(
            upstream,
            context.config,
            context.stage_dependencies[4],
            on_unit=controller.unit_completed,
        )

    def build(
        self,
        *,
        stop_controller: StopController | None = None,
        verify: bool = False,
    ) -> BuildResult:
        self.ensure_workspace()
        controller = stop_controller or StopController(stop_file=self.stop_file)
        context, inspection = self.store.snapshot()
        earliest = inspection["earliest_invalid_stage"]
        if earliest is None:
            result = BuildResult(
                status="reused",
                inspection=inspection,
                ranking=self._load_current_ranking(),
            )
            if verify:
                result.verification = self.verify_consistency()
            return result

        artifacts: dict[int, dict[str, Any]] = {}
        for stage in range(1, earliest):
            artifacts[stage] = self.store.load_artifact(stage)

        seq = int(inspection["next_checkpoint_seq"])
        published: list[dict[str, Any]] = []
        try:
            for stage in range(earliest, 5):
                controller.maybe_stop_before_stage(stage)
                upstream = context.bundle if stage == 1 else artifacts[stage - 1]
                artifact = self._run_stage(stage, upstream, context, controller)
                checkpoint = self.store.publish_stage(
                    stage, seq, artifact, expected_context=context
                )
                published.append(
                    {
                        "checkpoint_seq": checkpoint["checkpoint_seq"],
                        "stage": checkpoint["stage"],
                        "stage_name": checkpoint["stage_name"],
                        "artifact_digest": checkpoint["artifact_digest"],
                        "dependency_fingerprint": checkpoint["dependency_fingerprint"],
                    }
                )
                artifacts[stage] = artifact
                seq += 1
        except StopRequested as exc:
            _, stopped_inspection = self.store.snapshot()
            return BuildResult(
                status="stopped",
                inspection=stopped_inspection,
                ranking=None,
                checkpoints_published=published,
                stopped_stage=exc.stage,
            )

        final_inspection = self.store.inspect()
        if final_inspection["invalid_stages"]:
            raise EngineError(
                "E_STALE_CHECKPOINT",
                "publication completed but the pipeline still has invalid stages",
                details={
                    "invalid_stages": final_inspection["invalid_stages"],
                    "stage_status": final_inspection["stage_status"],
                },
            )
        result = BuildResult(
            status="completed",
            inspection=final_inspection,
            ranking=self._load_current_ranking(),
            checkpoints_published=published,
        )
        if verify:
            result.verification = self.verify_consistency()
        return result

    def query_stage(self, stage: int | str, *, allow_stale: bool = False) -> dict[str, Any]:
        stage_number = parse_stage(stage)
        inspection, artifact, checkpoint = self.store.inspect_with_stage(stage_number)
        stale = stage_number in inspection["invalid_stages"]
        if stale and not allow_stale:
            raise EngineError(
                "E_INVALID_STAGE",
                "requested stage artifact is stale and cannot be presented as current",
                details={
                    "stage": stage_number,
                    "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                    "invalid_stages": inspection["invalid_stages"],
                    "invalidation_reasons": inspection["invalidation_reasons"],
                    "affected_models": inspection["affected_models"],
                },
            )
        if artifact is None:
            raise EngineError(
                "E_INVALID_STAGE",
                "requested stage artifact or prerequisite artifact is missing",
                details={
                    "stage": stage_number,
                    "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                    "invalid_stages": inspection["invalid_stages"],
                },
            )
        return {
            "ok": True,
            "data": {
                "stage": stage_number,
                "stage_name": STAGE_NAMES[stage_number],
                "stale": stale,
                "checkpoint": checkpoint,
                "artifact": artifact,
                "inspection": inspection,
            },
        }

    def query_ranking(self, *, allow_stale: bool = False) -> dict[str, Any]:
        return self.query_stage(4, allow_stale=allow_stale)

    def update_answers(
        self,
        benchmark_id: str,
        answers: dict[str, str],
        *,
        replace: bool = False,
    ) -> dict[str, Any]:
        self.ensure_workspace()
        with file_lock(self.store.lock_path):
            update = update_benchmark_answers(
                self.samples_dir,
                self.answers_path,
                benchmark_id,
                answers,
                replace=replace,
            )
        inspection = self.store.inspect()
        return {
            "ok": True,
            "data": {
                "update": update,
                "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                "invalidation_reasons": inspection["invalidation_reasons"],
                "affected_models": inspection["affected_models"],
            },
        }

    def replace_batch(
        self, batch_id: str, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self.ensure_workspace()
        with file_lock(self.store.lock_path):
            update = replace_sample_batch(
                self.samples_dir,
                self.answers_path,
                batch_id,
                records,
            )
        inspection = self.store.inspect()
        return {
            "ok": True,
            "data": {
                "update": update,
                "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                "invalidation_reasons": inspection["invalidation_reasons"],
                "affected_models": inspection["affected_models"],
            },
        }

    def verify_consistency(self) -> dict[str, Any]:
        inspection = self.store.inspect()
        if inspection["invalid_stages"]:
            raise EngineError(
                "E_INVALID_STAGE",
                "consistency verification requires a fully valid incremental build",
                details={
                    "invalid_stages": inspection["invalid_stages"],
                    "earliest_invalid_stage": inspection["earliest_invalid_stage"],
                },
            )
        with tempfile.TemporaryDirectory(
            prefix="full-verify-", dir=str(self.workspace / "work")
        ) as temp_name:
            temp_root = Path(temp_name)
            temp_samples = temp_root / "samples"
            temp_configs = temp_root / "configs"
            shutil.copytree(self.samples_dir, temp_samples)
            temp_configs.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.config_path, temp_configs / "engine.json")
            if self.answers_path.exists():
                shutil.copy2(self.answers_path, temp_configs / "answers.json")
            full_engine = Engine(
                temp_root,
                samples_dir=temp_samples,
                config_path=temp_configs / "engine.json",
                answers_path=temp_configs / "answers.json",
            )
            full_result = full_engine.build(verify=False)
            if full_result.status != "completed":
                raise EngineError(
                    "E_INCREMENTAL_MISMATCH",
                    "the full-from-scratch verification build did not complete",
                    details={"status": full_result.status},
                )
            comparisons: dict[str, Any] = {}
            first_difference_stage: int | None = None
            first_difference_model: str | None = None
            for stage in (2, 3, 4):
                incremental_bytes = self.store.artifact_path(stage).read_bytes()
                full_bytes = full_engine.store.artifact_path(stage).read_bytes()
                incremental_digest = sha256_bytes(incremental_bytes)
                full_digest = sha256_bytes(full_bytes)
                equal = incremental_bytes == full_bytes
                comparisons[f"stage{stage}"] = {
                    "equal": equal,
                    "incremental_digest": incremental_digest,
                    "full_digest": full_digest,
                }
                if not equal and first_difference_stage is None:
                    first_difference_stage = stage
                    incremental_artifact = read_json(self.store.artifact_path(stage))
                    full_artifact = read_json(full_engine.store.artifact_path(stage))
                    first_difference_model = _first_different_model(
                        stage, incremental_artifact, full_artifact
                    )
            if first_difference_stage is not None:
                raise EngineError(
                    "E_INCREMENTAL_MISMATCH",
                    "incremental artifacts differ from a full rebuild",
                    details={
                        "first_difference_stage": first_difference_stage,
                        "stage": first_difference_stage,
                        "model_id": first_difference_model,
                        "comparisons": comparisons,
                    },
                )
            return {
                "ok": True,
                "status": "consistent",
                "comparisons": comparisons,
            }


def _model_group_key(stage: int, item: dict[str, Any]) -> str:
    if stage == 2:
        return f"{item.get('model_id')}|{item.get('benchmark_id')}"
    return str(item.get("model_id"))


def _first_different_model(
    stage: int, incremental: dict[str, Any], full: dict[str, Any]
) -> str | None:
    if stage == 2:
        incremental_items = incremental.get("groups", [])
        full_items = full.get("groups", [])
    elif stage == 3:
        incremental_items = incremental.get("models", [])
        full_items = full.get("models", [])
    else:
        incremental_items = list(incremental.get("ranking", [])) + list(
            incremental.get("rejected", [])
        )
        full_items = list(full.get("ranking", [])) + list(full.get("rejected", []))
    incremental_by_key = {
        _model_group_key(stage, item): item for item in incremental_items
    }
    full_by_key = {_model_group_key(stage, item): item for item in full_items}
    for key in sorted(set(incremental_by_key) | set(full_by_key)):
        if incremental_by_key.get(key) != full_by_key.get(key):
            model_id = key.split("|", 1)[0]
            return model_id or None
    return None
