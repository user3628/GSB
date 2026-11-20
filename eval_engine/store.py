"""Atomic checkpoints, dependency fingerprints, and invalidation inspection."""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import config_component_digests, config_digest, load_config
from .errors import EngineError
from .inputs import InputBundle, load_inputs
from .locking import file_lock
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    digest_file,
    digest_json,
    read_json,
    sha256_bytes,
)

STAGE_NAMES = {
    1: "normalize_and_score",
    2: "aggregate_and_exclude",
    3: "build_capability_profiles",
    4: "rank_and_select",
}

_CHECKPOINT_RE = re.compile(r"^(\d{8})\.json$")


@dataclass(frozen=True)
class WorkspaceContext:
    bundle: InputBundle
    config: dict[str, Any]
    config_digest: str
    component_digests: dict[str, str]
    stage_components: dict[int, dict[str, str]]
    stage_dependencies: dict[int, str]


def _components(context: WorkspaceContext) -> dict[int, dict[str, str]]:
    component_digests = context.component_digests
    stage1 = {
        "samples": context.bundle.samples_digest,
        "answers": context.bundle.answers_digest,
        "scoring": component_digests["scoring"],
    }
    stage2 = {
        "upstream": digest_json(stage1),
        "exclusions": component_digests["exclusions"],
    }
    stage3 = {
        "upstream": digest_json(stage2),
        "profiling": component_digests["profiling"],
    }
    stage4 = {
        "upstream": digest_json(stage3),
        "ranking": component_digests["ranking"],
    }
    return {1: stage1, 2: stage2, 3: stage3, 4: stage4}


def build_context(bundle: InputBundle, config: dict[str, Any]) -> WorkspaceContext:
    component_digests = config_component_digests(config)
    context_without_deps = WorkspaceContext(
        bundle=bundle,
        config=config,
        config_digest=config_digest(config),
        component_digests=component_digests,
        stage_components={},
        stage_dependencies={},
    )
    stage_components = _components(context_without_deps)
    return WorkspaceContext(
        bundle=bundle,
        config=config,
        config_digest=context_without_deps.config_digest,
        component_digests=component_digests,
        stage_components=stage_components,
        stage_dependencies={
            stage: digest_json(components)
            for stage, components in stage_components.items()
        },
    )


def _changed_component_names(
    stage: int, previous: dict[str, str], current: dict[str, str]
) -> list[str]:
    names = sorted(
        key
        for key in set(previous) | set(current)
        if previous.get(key) != current.get(key)
    )
    reason_by_key = {
        (1, "samples"): "samples_changed",
        (1, "answers"): "answers_changed",
        (1, "scoring"): "scoring_rules_changed",
        (2, "exclusions"): "exclusion_rules_changed",
        (3, "profiling"): "profile_weights_changed",
        (4, "ranking"): "ranking_rules_changed",
    }
    reasons = [
        reason_by_key.get((stage, name), f"upstream_fingerprint_changed:{name}")
        for name in names
    ]
    return reasons


class CheckpointStore:
    def __init__(
        self,
        workspace: Path,
        *,
        samples_dir: Path | None = None,
        config_path: Path | None = None,
        answers_path: Path | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.samples_dir = Path(samples_dir or (self.workspace / "samples"))
        self.config_path = Path(config_path or (self.workspace / "configs" / "engine.json"))
        self.answers_path = Path(
            answers_path or (self.workspace / "configs" / "answers.json")
        )
        self.work_dir = self.workspace / "work"
        self.artifact_dir = self.work_dir / "artifacts"
        self.checkpoint_dir = self.work_dir / "checkpoints"
        self.lock_path = self.work_dir / ".lock"

    def ensure_directories(self) -> None:
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def load_context(self) -> WorkspaceContext:
        bundle = load_inputs(self.samples_dir, self.answers_path)
        config = load_config(self.config_path)
        return build_context(bundle, config)

    def artifact_path(self, stage: int) -> Path:
        if stage not in STAGE_NAMES:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage must be an integer from 1 through 4",
                details={"stage": stage},
            )
        return self.artifact_dir / f"stage{stage}.json"

    def checkpoint_path(self, seq: int) -> Path:
        return self.checkpoint_dir / f"{seq:08d}.json"

    def _load_checkpoints_locked(self) -> list[dict[str, Any]]:
        if not self.checkpoint_dir.exists():
            return []
        entries: list[tuple[int, Path]] = []
        for path in self.checkpoint_dir.iterdir():
            if not path.is_file():
                continue
            match = _CHECKPOINT_RE.match(path.name)
            if match is None:
                continue
            entries.append((int(match.group(1)), path))
        entries.sort()
        records: list[dict[str, Any]] = []
        for expected, (seq, path) in enumerate(entries, 1):
            if seq != expected:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence is not contiguous",
                    details={
                        "expected_seq": expected,
                        "found_seq": seq,
                        "path": str(path),
                    },
                )
            try:
                record = read_json(path)
            except (OSError, ValueError) as exc:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint file cannot be read",
                    details={"path": str(path), "error": str(exc)},
                ) from exc
            if (
                not isinstance(record, dict)
                or record.get("checkpoint_seq") != seq
                or record.get("stage") not in STAGE_NAMES
            ):
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint record is malformed or out of sequence",
                    details={"path": str(path), "checkpoint_seq": seq},
                )
            records.append(record)
        return records

    def _latest_by_stage(
        self, checkpoints: list[dict[str, Any]]
    ) -> dict[int, dict[str, Any]]:
        latest: dict[int, dict[str, Any]] = {}
        for checkpoint in checkpoints:
            latest[int(checkpoint["stage"])] = checkpoint
        return latest

    def _artifact_digest_on_disk(self, stage: int) -> str | None:
        path = self.artifact_path(stage)
        if not path.exists():
            return None
        return digest_file(path)

    def _inspect_locked(
        self, context: WorkspaceContext
    ) -> dict[str, Any]:
        checkpoints = self._load_checkpoints_locked()
        latest = self._latest_by_stage(checkpoints)
        valid_stages: list[int] = []
        invalid_stages: list[int] = []
        stage_status: dict[str, dict[str, Any]] = {}
        earliest_invalid: int | None = None
        upstream_invalidated = False
        for stage in range(1, 5):
            checkpoint = latest.get(stage)
            reasons: list[str] = []
            if upstream_invalidated:
                reasons.append(f"downstream_of_invalid_stage_{earliest_invalid}")
            if checkpoint is None:
                reasons.append(f"stage{stage}_checkpoint_missing")
            else:
                previous_components = checkpoint.get("components", {})
                current_components = context.stage_components[stage]
                if previous_components != current_components:
                    reasons.extend(
                        _changed_component_names(
                            stage, previous_components, current_components
                        )
                    )
                if (
                    previous_components == current_components
                    and checkpoint.get("dependency_fingerprint")
                    != context.stage_dependencies[stage]
                ):
                    reasons.append(f"stage{stage}_dependency_fingerprint_mismatch")
                artifact_digest = self._artifact_digest_on_disk(stage)
                if artifact_digest is None:
                    reasons.append(f"stage{stage}_artifact_missing")
                elif artifact_digest != checkpoint.get("artifact_digest"):
                    reasons.append(f"stage{stage}_artifact_digest_mismatch")
            reasons = sorted(set(reasons))
            valid = not reasons
            if valid:
                valid_stages.append(stage)
                stage_status[str(stage)] = {
                    "valid": True,
                    "checkpoint_seq": checkpoint["checkpoint_seq"],
                    "dependency_fingerprint": checkpoint["dependency_fingerprint"],
                    "reasons": [],
                }
            else:
                invalid_stages.append(stage)
                if earliest_invalid is None:
                    earliest_invalid = stage
                upstream_invalidated = True
                stage_status[str(stage)] = {
                    "valid": False,
                    "checkpoint_seq": checkpoint["checkpoint_seq"] if checkpoint else None,
                    "dependency_fingerprint": (
                        checkpoint.get("dependency_fingerprint") if checkpoint else None
                    ),
                    "current_dependency_fingerprint": context.stage_dependencies[stage],
                    "reasons": reasons,
                }

        affected_models = self._affected_models_locked(
            context, latest, earliest_invalid
        )
        earliest_reasons: list[str] = []
        if earliest_invalid is not None:
            earliest_reasons = list(
                stage_status[str(earliest_invalid)].get("reasons", [])
            )
            earliest_reasons = [
                reason
                for reason in earliest_reasons
                if not reason.startswith("downstream_of_invalid_stage_")
            ]
        last_seq = int(checkpoints[-1]["checkpoint_seq"]) if checkpoints else 0
        return {
            "checkpoint_count": len(checkpoints),
            "last_checkpoint_seq": last_seq,
            "next_checkpoint_seq": last_seq + 1,
            "last_checkpoint_stage": checkpoints[-1]["stage"] if checkpoints else None,
            "valid_stages": valid_stages,
            "invalid_stages": invalid_stages,
            "earliest_invalid_stage": earliest_invalid,
            "expected_stage": earliest_invalid,
            "invalidation_reasons": sorted(set(earliest_reasons)),
            "affected_models": affected_models,
            "stage_status": stage_status,
            "current_fingerprints": {
                f"stage{stage}": context.stage_dependencies[stage]
                for stage in range(1, 5)
            },
            "current_stage_components": {
                f"stage{stage}": context.stage_components[stage]
                for stage in range(1, 5)
            },
            "config_version": context.config["version"],
            "config_digest": context.config_digest,
            "shard_content_digest": context.bundle.shards_digest,
            "effective_samples_digest": context.bundle.samples_digest,
            "answers_digest": context.bundle.answers_digest,
            "models": list(context.bundle.models),
            "benchmarks": list(context.bundle.benchmarks),
            "batches": list(context.bundle.batches),
        }

    def _affected_models_locked(
        self,
        context: WorkspaceContext,
        latest: dict[int, dict[str, Any]],
        earliest_invalid: int | None,
    ) -> list[str]:
        all_models = set(context.bundle.models)
        if earliest_invalid is None:
            return []
        if earliest_invalid != 1:
            return sorted(all_models)
        stage1_checkpoint = latest.get(1)
        if stage1_checkpoint is None:
            return sorted(all_models)
        previous = stage1_checkpoint.get("components", {})
        current = context.stage_components[1]
        changed = {
            key
            for key in set(previous) | set(current)
            if previous.get(key) != current.get(key)
        }
        if "scoring" in changed or not changed:
            return sorted(all_models)
        if not changed.intersection({"samples", "answers"}):
            return sorted(all_models)
        try:
            old_artifact = read_json(self.artifact_path(1))
            old_snapshot = old_artifact.get("source_snapshot", [])
        except (OSError, ValueError):
            return sorted(all_models)
        if not isinstance(old_snapshot, list):
            return sorted(all_models)
        old_by_key = {
            item.get("record_key"): item
            for item in old_snapshot
            if isinstance(item, dict) and item.get("record_key")
        }
        new_by_key = {
            item["record_key"]: item for item in context.bundle.source_snapshot
        }
        affected: set[str] = set()
        for key in set(old_by_key) | set(new_by_key):
            if old_by_key.get(key) != new_by_key.get(key):
                old_item = old_by_key.get(key, {})
                new_item = new_by_key.get(key, {})
                if old_item.get("model_id"):
                    affected.add(old_item["model_id"])
                if new_item.get("model_id"):
                    affected.add(new_item["model_id"])
        return sorted(affected)

    def inspect(self) -> dict[str, Any]:
        self.ensure_directories()
        with file_lock(self.lock_path):
            context = self.load_context()
            return self._inspect_locked(context)

    def snapshot(self) -> tuple[WorkspaceContext, dict[str, Any]]:
        self.ensure_directories()
        with file_lock(self.lock_path):
            context = self.load_context()
            return context, self._inspect_locked(context)

    def publish_stage(
        self,
        stage: int,
        seq: int,
        artifact: dict[str, Any],
        expected_context: WorkspaceContext,
    ) -> dict[str, Any]:
        if stage not in STAGE_NAMES:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage must be an integer from 1 through 4",
                details={"stage": stage},
            )
        if artifact.get("stage") != stage:
            raise EngineError(
                "E_INVALID_STAGE",
                "artifact stage does not match the requested stage",
                details={"stage": stage, "artifact_stage": artifact.get("stage")},
            )
        if (
            artifact.get("dependency_fingerprint")
            != expected_context.stage_dependencies[stage]
        ):
            raise EngineError(
                "E_STALE_CHECKPOINT",
                "artifact was built from a different dependency fingerprint",
                details={
                    "stage": stage,
                    "expected_fingerprint": expected_context.stage_dependencies[stage],
                    "artifact_fingerprint": artifact.get("dependency_fingerprint"),
                },
            )
        self.ensure_directories()
        with file_lock(self.lock_path):
            live_context = self.load_context()
            if (
                live_context.stage_components != expected_context.stage_components
                or live_context.config_digest != expected_context.config_digest
            ):
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "inputs or configuration changed while the stage was being built",
                    details={
                        "stage": stage,
                        "requested_seq": seq,
                        "expected_fingerprint": expected_context.stage_dependencies[stage],
                        "live_fingerprint": live_context.stage_dependencies[stage],
                    },
                )
            checkpoints = self._load_checkpoints_locked()
            last_seq = int(checkpoints[-1]["checkpoint_seq"]) if checkpoints else 0
            if not isinstance(seq, int) or seq != last_seq + 1:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence must advance by exactly one",
                    details={
                        "stage": stage,
                        "requested_seq": seq,
                        "last_seq": last_seq,
                        "expected_seq": last_seq + 1,
                    },
                )
            inspection = self._inspect_locked(live_context)
            if inspection["expected_stage"] != stage:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "stage publication is out of order",
                    details={
                        "stage": stage,
                        "requested_seq": seq,
                        "expected_stage": inspection["expected_stage"],
                        "valid_stages": inspection["valid_stages"],
                    },
                )
            artifact_data = canonical_bytes(artifact)
            artifact_digest = sha256_bytes(artifact_data)
            artifact_path = self.artifact_path(stage)
            atomic_write_bytes(artifact_path, artifact_data)

            if stage == 1:
                input_digest = live_context.bundle.stage1_input_digest
            else:
                upstream_checkpoint = self._latest_by_stage(checkpoints)[stage - 1]
                input_digest = upstream_checkpoint["artifact_digest"]
            shard_content_digest = live_context.bundle.shards_digest
            checkpoint = {
                "schema_version": 1,
                "checkpoint_seq": seq,
                "stage": stage,
                "stage_name": STAGE_NAMES[stage],
                "input_digest": input_digest,
                "shard_content_digest": shard_content_digest,
                "config_version": live_context.config["version"],
                "config_digest": live_context.config_digest,
                "dependency_fingerprint": live_context.stage_dependencies[stage],
                "components": live_context.stage_components[stage],
                "artifact_digest": artifact_digest,
                "artifact_path": f"artifacts/stage{stage}.json",
            }
            checkpoint_path = self.checkpoint_path(seq)
            if checkpoint_path.exists():
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence already exists",
                    details={"stage": stage, "requested_seq": seq},
                )
            atomic_write_json(checkpoint_path, checkpoint)
            return checkpoint

    def load_artifact(self, stage: int) -> dict[str, Any]:
        self.ensure_directories()
        with file_lock(self.lock_path):
            return self._load_artifact_unlocked(stage)

    def _load_artifact_unlocked(self, stage: int) -> dict[str, Any]:
        if stage not in STAGE_NAMES:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage must be an integer from 1 through 4",
                details={"stage": stage},
            )
        path = self.artifact_path(stage)
        if not path.exists():
            raise EngineError(
                "E_INVALID_STAGE",
                "stage artifact is missing",
                details={"stage": stage, "path": str(path)},
            )
        try:
            value = read_json(path)
        except (OSError, ValueError) as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage artifact cannot be read",
                details={"stage": stage, "path": str(path), "error": str(exc)},
            ) from exc
        if not isinstance(value, dict) or value.get("stage") != stage:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage artifact is malformed",
                details={"stage": stage, "path": str(path)},
            )
        return value

    def inspect_with_stage(
        self, stage: int
    ) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
        """Return inspection, artifact, and latest checkpoint in one lock scope."""

        if stage not in STAGE_NAMES:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage must be an integer from 1 through 4",
                details={"stage": stage},
            )
        self.ensure_directories()
        with file_lock(self.lock_path):
            context = self.load_context()
            inspection = self._inspect_locked(context)
            artifact: dict[str, Any] | None = None
            try:
                artifact = self._load_artifact_unlocked(stage)
            except EngineError:
                artifact = None
            checkpoint: dict[str, Any] | None = None
            for record in reversed(self._load_checkpoints_locked()):
                if record["stage"] == stage:
                    checkpoint = record
                    break
            return inspection, artifact, checkpoint

    def checkpoints(self) -> list[dict[str, Any]]:
        self.ensure_directories()
        with file_lock(self.lock_path):
            return copy.deepcopy(self._load_checkpoints_locked())

    def artifact_digests(self) -> dict[str, str | None]:
        self.ensure_directories()
        with file_lock(self.lock_path):
            return {
                f"stage{stage}": self._artifact_digest_on_disk(stage)
                for stage in range(1, 5)
            }
