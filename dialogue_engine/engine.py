"""End-to-end staged engine with checkpointing, recovery, and query APIs."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .behavior import recognize_behavior
from .checkpoints import CheckpointStore
from .config import (
    STAGES,
    BehaviorConfig,
    ClassifierConfig,
    EngineConfig,
    FeatureConfig,
    MetricsConfig,
    StateConfig,
)
from .emotion import score_features
from .errors import E_INPUT_LENGTH, E_INVALID_STAGE, E_STALE_CHECKPOINT, EngineError, as_error
from .features import extract_turn_features
from .manifest import Dataset, ShardData, TurnAnnotation, load_dataset
from .metrics import build_understanding_summary
from .serialization import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_json,
    read_json,
    round_floats,
)
from .state import evolve_state, summarize_shard


def _canonical(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value).decode("utf-8"))


class DialogueEngine:
    """Local deterministic dialogue emotion and understanding engine."""

    def __init__(
        self,
        workspace: str | Path,
        *,
        config: EngineConfig | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.config = config or EngineConfig()
        try:
            self.config.validate()
        except ValueError as exc:
            raise as_error(
                E_INVALID_STAGE,
                f"invalid engine configuration: {exc}",
                stage="config",
            ) from exc
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.store = CheckpointStore(self.workspace)
        self.run_dir = self.workspace / "run"
        self.exports_dir = self.workspace / "exports"
        self.diagnostics_dir = self.workspace / "diagnostics"
        self.stop_path = self.run_dir / "stop.request"
        self.run_state_path = self.run_dir / "run_state.json"
        self.dataset: Dataset | None = None
        self._rebuild_started = False
        self._stage_timings: dict[str, dict[str, float | int]] = {
            stage: {"count": 0, "seconds": 0.0} for stage in STAGES
        }
        self._reused_counts: dict[str, int] = {stage: 0 for stage in STAGES}
        self._computed_counts: dict[str, int] = {stage: 0 for stage in STAGES}

    # ------------------------------------------------------------------
    # Dataset and version fingerprints
    # ------------------------------------------------------------------

    def import_dataset(self, data_dir: str | Path) -> Dataset:
        dataset = load_dataset(data_dir)
        if self.config.features.sample_rate != 16_000:
            raise as_error(
                E_INPUT_LENGTH,
                "the engine accepts 16000 Hz shards only",
                stage="import",
                configured_sample_rate=self.config.features.sample_rate,
            )
        self.dataset = dataset
        return dataset

    def _require_dataset(self) -> Dataset:
        if self.dataset is None:
            raise as_error(
                E_INVALID_STAGE,
                "dataset is not imported; call import_dataset() first",
                stage="import",
            )
        return self.dataset

    def _feature_fingerprint(self, shard: ShardData, turn: TurnAnnotation) -> str:
        return digest_json(
            {
                "schema_version": 1,
                "stage": "features",
                "shard": {
                    "shard_id": shard.shard_id,
                    "sample_rate": shard.sample_rate,
                    "sample_count": shard.sample_count,
                    "samples_digest": shard.samples_digest,
                },
                "turn": {
                    "turn_index": turn.index,
                    "start_sample": turn.start_sample,
                    "end_sample": turn.end_sample,
                },
                "config": self.config.features.to_dict(),
            }
        )

    def _emotion_fingerprint(
        self, feature_artifact: dict[str, Any]
    ) -> str:
        return digest_json(
            {
                "schema_version": 1,
                "stage": "emotion",
                "feature_digest": digest_json(feature_artifact),
                "config": self.config.classifier.to_dict(),
            }
        )

    def _behavior_fingerprint(
        self,
        shard: ShardData,
        turn: TurnAnnotation,
        feature_artifact: dict[str, Any],
        emotion_artifact: dict[str, Any],
        previous_turn: TurnAnnotation | None,
        previous_emotion: dict[str, Any] | None,
        previous_behavior: dict[str, Any] | None,
    ) -> str:
        return digest_json(
            {
                "schema_version": 1,
                "stage": "behavior",
                "shard_id": shard.shard_id,
                "turn": {
                    "turn_index": turn.index,
                    "speaker_id": turn.speaker_id,
                    "start_ms": turn.start_ms,
                    "end_ms": turn.end_ms,
                },
                "feature_digest": digest_json(feature_artifact),
                "emotion_digest": digest_json(emotion_artifact),
                "previous": None
                if previous_turn is None
                else {
                    "turn_index": previous_turn.index,
                    "speaker_id": previous_turn.speaker_id,
                    "end_ms": previous_turn.end_ms,
                    "emotion_digest": digest_json(previous_emotion)
                    if previous_emotion is not None
                    else None,
                    "behavior_digest": digest_json(previous_behavior)
                    if previous_behavior is not None
                    else None,
                },
                "config": self.config.behavior.to_dict(),
            }
        )

    def _state_fingerprint(
        self,
        shard: ShardData,
        turn: TurnAnnotation,
        emotion_artifact: dict[str, Any],
        behavior_artifact: dict[str, Any],
        previous_state_artifact: dict[str, Any] | None,
    ) -> str:
        return digest_json(
            {
                "schema_version": 1,
                "stage": "state",
                "shard_id": shard.shard_id,
                "truth": turn.truth_dict(),
                "emotion_digest": digest_json(emotion_artifact),
                "behavior_digest": digest_json(behavior_artifact),
                "previous_state_digest": "initial"
                if previous_state_artifact is None
                else digest_json(previous_state_artifact),
                "config": {
                    "state": self.config.state.to_dict(),
                    "metrics": self.config.metrics.to_dict(),
                },
            }
        )

    def _work_order(self, shard_order: int, turn_index: int, stage: str) -> int:
        dataset = self._require_dataset()
        base = sum(len(shard.turns) * len(STAGES) for shard in dataset.shards[:shard_order])
        return base + turn_index * len(STAGES) + STAGES.index(stage)

    # ------------------------------------------------------------------
    # Stage execution
    # ------------------------------------------------------------------

    def _artifact_summary(self, stage: str, artifact: dict[str, Any]) -> dict[str, Any]:
        if stage == "features":
            return {
                "feature_dim": artifact["feature_dim"],
                "frame_count": artifact["frame_count"],
            }
        if stage == "emotion":
            return {"label": artifact["label"], "confidence": artifact["confidence"]}
        if stage == "behavior":
            return {"label": artifact["label"], "confidence": artifact["confidence"]}
        state = artifact["state"]
        return {
            "step": state["step"],
            "dominant_emotion": state["dominant_emotion"],
            "interruption_count": state["interruption_count"],
        }

    def _start_rebuild_if_needed(
        self,
        *,
        stage: str,
        shard: ShardData,
        shard_order: int,
        turn: TurnAnnotation,
        existing: dict[str, Any],
        new_fingerprint: str,
        new_config_version: str,
    ) -> None:
        status = self.store.status()
        if status.get("rebuild"):
            self._rebuild_started = True
            return
        if self._rebuild_started:
            return
        reason = (
            f"{stage} input fingerprint or configuration changed; "
            "this stage and all downstream state will be rebuilt"
        )
        fingerprints = {
            "stage": stage,
            "shard_id": shard.shard_id,
            "turn_index": turn.index,
            "old_input_fingerprint": existing.get("input_fingerprint"),
            "new_input_fingerprint": new_fingerprint,
            "old_config_version": existing.get("config_version"),
            "new_config_version": new_config_version,
            "engine_config_version": self.config.version,
        }
        self.store.begin_rebuild(
            earliest_stage=stage,
            earliest_shard_id=shard.shard_id,
            earliest_shard_order=shard_order,
            reason=reason,
            fingerprints=fingerprints,
        )
        self._rebuild_started = True

    def _ensure_stage(
        self,
        *,
        stage: str,
        shard: ShardData,
        shard_order: int,
        turn: TurnAnnotation,
        input_fingerprint: str,
        compute: Any,
        replacement_hint: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        config_version = self.config.stage_version(stage)
        existing = self.store.latest_checkpoint(shard.shard_id, turn.index, stage)
        if existing is not None:
            if (
                existing.get("input_fingerprint") == input_fingerprint
                and existing.get("config_version") == config_version
            ):
                artifact = self.store.read_artifact(existing)
                self._reused_counts[stage] += 1
                return artifact, existing, True
            self._start_rebuild_if_needed(
                stage=stage,
                shard=shard,
                shard_order=shard_order,
                turn=turn,
                existing=existing,
                new_fingerprint=input_fingerprint,
                new_config_version=config_version,
            )
            replacement_hint = True

        started = time.perf_counter()
        artifact = _canonical(compute())
        elapsed = time.perf_counter() - started
        self._stage_timings[stage]["count"] = int(
            self._stage_timings[stage]["count"]
        ) + 1
        self._stage_timings[stage]["seconds"] = float(
            self._stage_timings[stage]["seconds"]
        ) + elapsed
        self._computed_counts[stage] += 1
        artifact_digest, _ = self.store.write_artifact(
            shard.shard_id,
            turn.index,
            stage,
            artifact,
            input_fingerprint=input_fingerprint,
        )
        expected_sequence = self.store.scan().sequence + 1
        shard_complete = stage == "state" and turn.index == shard.turn_count - 1
        try:
            record = self.store.publish_checkpoint(
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage=stage,
                work_order=self._work_order(shard_order, turn.index, stage),
                shard_order=shard_order,
                input_fingerprint=input_fingerprint,
                config_version=config_version,
                artifact_digest=artifact_digest,
                artifact_summary=self._artifact_summary(stage, artifact),
                shard_complete=shard_complete,
                expected_sequence=expected_sequence,
                replacement=replacement_hint,
            )
            return artifact, record, False
        except EngineError as exc:
            if exc.code != E_STALE_CHECKPOINT:
                raise
            raced = self.store.latest_checkpoint(shard.shard_id, turn.index, stage)
            if (
                raced is not None
                and raced.get("input_fingerprint") == input_fingerprint
                and raced.get("config_version") == config_version
            ):
                self._reused_counts[stage] += 1
                return self.store.read_artifact(raced), raced, True
            raise

    # ------------------------------------------------------------------
    # Run, stop, and resume
    # ------------------------------------------------------------------

    def request_stop(self, reason: str = "external stop request") -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(self.stop_path, (reason.strip() + "\n").encode("utf-8"))

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def _write_timings(self, status: str) -> dict[str, Any]:
        self.diagnostics_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": status,
            "stages": {},
        }
        for stage, timing in self._stage_timings.items():
            count = int(timing["count"])
            seconds = float(timing["seconds"])
            payload["stages"][stage] = {
                "executed_count": count,
                "total_seconds": seconds,
                "average_ms": (seconds * 1000.0 / count) if count else 0.0,
                "reused_count": self._reused_counts[stage],
            }
        atomic_write_json(self.diagnostics_dir / "timings.json", payload)
        return payload

    def _completed_state_count(self) -> int:
        effective = self.store.effective_records()
        return sum(1 for key in effective if key[2] == "state")

    def run(
        self,
        data_dir: str | Path | None = None,
        *,
        stop_after_turns: int | None = None,
        resume: bool = False,
    ) -> dict[str, Any]:
        """Process all turns, checking stop requests only at turn boundaries."""

        dataset = self.import_dataset(data_dir) if data_dir is not None else self._require_dataset()
        if stop_after_turns is not None and stop_after_turns < 0:
            raise ValueError("stop_after_turns must be non-negative")
        if resume:
            self.clear_stop()
        recovery = self.store.recover()
        self._rebuild_started = False
        self._stage_timings = {
            stage: {"count": 0, "seconds": 0.0} for stage in STAGES
        }
        self._reused_counts = {stage: 0 for stage in STAGES}
        self._computed_counts = {stage: 0 for stage in STAGES}
        newly_completed_turns = 0
        last_processed_shard: str | None = None
        last_processed_turn: int | None = None
        stop_reason: str | None = None

        initial_status = self.store.status()
        if (
            initial_status["sequence"] > 0
            and initial_status.get("active_shard_complete")
            and not initial_status.get("rebuild")
        ):
            first_shard = dataset.shards[0]
            first_turn = first_shard.turns[0]
            if (
                self.store.latest_checkpoint(
                    first_shard.shard_id, first_turn.index, "features"
                )
                is None
            ):
                self.store.begin_rebuild(
                    earliest_stage="features",
                    earliest_shard_id=first_shard.shard_id,
                    earliest_shard_order=0,
                    reason="dataset layout begins with unpublished shard content",
                    fingerprints={
                        "stage": "features",
                        "shard_id": first_shard.shard_id,
                        "turn_index": first_turn.index,
                        "reason": "missing_first_shard_checkpoint",
                    },
                )
                self._rebuild_started = True

        for shard_order, shard in enumerate(dataset.shards):
            previous_turn: TurnAnnotation | None = None
            previous_emotion: dict[str, Any] | None = None
            previous_behavior: dict[str, Any] | None = None
            previous_state: dict[str, Any] | None = None
            for turn in shard.turns:
                feature_fp = self._feature_fingerprint(shard, turn)
                feature_artifact, feature_record, feature_reused = self._ensure_stage(
                    stage="features",
                    shard=shard,
                    shard_order=shard_order,
                    turn=turn,
                    input_fingerprint=feature_fp,
                    compute=lambda shard=shard, turn=turn: extract_turn_features(
                        shard, turn, self.config.features
                    ),
                )
                emotion_fp = self._emotion_fingerprint(feature_artifact)
                emotion_artifact, emotion_record, emotion_reused = self._ensure_stage(
                    stage="emotion",
                    shard=shard,
                    shard_order=shard_order,
                    turn=turn,
                    input_fingerprint=emotion_fp,
                    compute=lambda feature_artifact=feature_artifact, turn=turn: score_features(
                        feature_artifact,
                        feature_config=self.config.features,
                        classifier_config=self.config.classifier,
                        shard_id=shard.shard_id,
                        turn_index=turn.index,
                    ),
                )
                behavior_fp = self._behavior_fingerprint(
                    shard,
                    turn,
                    feature_artifact,
                    emotion_artifact,
                    previous_turn,
                    previous_emotion,
                    previous_behavior,
                )
                behavior_artifact, behavior_record, behavior_reused = self._ensure_stage(
                    stage="behavior",
                    shard=shard,
                    shard_order=shard_order,
                    turn=turn,
                    input_fingerprint=behavior_fp,
                    compute=lambda shard=shard, turn=turn, feature_artifact=feature_artifact, emotion_artifact=emotion_artifact, previous_turn=previous_turn, previous_emotion=previous_emotion, previous_behavior=previous_behavior: recognize_behavior(
                        shard,
                        turn,
                        feature_artifact,
                        emotion_artifact,
                        previous_turn=previous_turn,
                        previous_emotion_artifact=previous_emotion,
                        previous_behavior_artifact=previous_behavior,
                        config=self.config.behavior,
                        expected_feature_dim=self.config.features.dimension,
                    ),
                )
                state_fp = self._state_fingerprint(
                    shard,
                    turn,
                    emotion_artifact,
                    behavior_artifact,
                    previous_state,
                )
                state_artifact, state_record, state_reused = self._ensure_stage(
                    stage="state",
                    shard=shard,
                    shard_order=shard_order,
                    turn=turn,
                    input_fingerprint=state_fp,
                    compute=lambda shard=shard, turn=turn, emotion_artifact=emotion_artifact, behavior_artifact=behavior_artifact, previous_state=previous_state: evolve_state(
                        shard,
                        turn,
                        emotion_artifact,
                        behavior_artifact,
                        previous_state_artifact=previous_state,
                        config=self.config.state,
                    ),
                )
                if not state_reused:
                    newly_completed_turns += 1
                previous_turn = turn
                previous_emotion = emotion_artifact
                previous_behavior = behavior_artifact
                previous_state = state_artifact
                last_processed_shard = shard.shard_id
                last_processed_turn = turn.index

                if stop_after_turns is not None and newly_completed_turns >= stop_after_turns:
                    stop_reason = f"stop_after_turns={stop_after_turns}"
                    break
                if self.stop_requested():
                    try:
                        stop_reason = self.stop_path.read_text(encoding="utf-8").strip()
                    except OSError:
                        stop_reason = "stop request"
                    break
            if stop_reason is not None:
                break

        if stop_reason is not None:
            status_payload = {
                "status": "stopped",
                "reason": stop_reason,
                "sequence": self.store.scan().sequence,
                "completed_state_count": self._completed_state_count(),
                "newly_completed_turns_in_call": newly_completed_turns,
                "last_shard_id": last_processed_shard,
                "last_turn_index": last_processed_turn,
                "recovered_records": recovery,
                "reused_stages": dict(self._reused_counts),
                "computed_stages": dict(self._computed_counts),
            }
            self.run_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.run_state_path, status_payload)
            status_payload["timings"] = self._write_timings("stopped")
            return status_payload

        head_status = self.store.status()
        if head_status.get("rebuild"):
            last_shard = dataset.shards[-1]
            self.store.finish_rebuild(
                active_shard_id=last_shard.shard_id,
                active_shard_order=len(dataset.shards) - 1,
            )
        summary = self.export_summary()
        status_payload = {
            "status": "completed",
            "sequence": self.store.scan().sequence,
            "completed_state_count": self._completed_state_count(),
            "newly_completed_turns_in_call": newly_completed_turns,
            "recovered_records": recovery,
            "reused_stages": dict(self._reused_counts),
            "computed_stages": dict(self._computed_counts),
            "summary_digest": summary["summary_digest"],
            "summary_path": str(self.exports_dir / "summary.json"),
            "metrics": summary["metrics"],
            "invalidation": self.store.status().get("last_invalidation"),
        }
        self.run_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.run_state_path, status_payload)
        status_payload["timings"] = self._write_timings("completed")
        return status_payload

    def resume(self, data_dir: str | Path | None = None) -> dict[str, Any]:
        return self.run(data_dir, resume=True)

    # ------------------------------------------------------------------
    # Query and export APIs
    # ------------------------------------------------------------------

    def query_stage(
        self, shard_id: str, turn_index: int, stage: str
    ) -> dict[str, Any]:
        dataset = self._require_dataset()
        shard = dataset.shard(shard_id)
        if turn_index < 0 or turn_index >= shard.turn_count:
            raise as_error(
                E_INVALID_STAGE,
                f"turn {turn_index} is outside shard {shard_id}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        if stage not in STAGES:
            raise as_error(
                E_INVALID_STAGE,
                f"illegal stage jump/request: {stage!r}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        stage_index = STAGES.index(stage)
        if stage_index > 0:
            prerequisite = STAGES[stage_index - 1]
            if self.store.latest_checkpoint(shard_id, turn_index, prerequisite) is None:
                raise as_error(
                    E_INVALID_STAGE,
                    f"stage {stage!r} is missing prerequisite {prerequisite!r}",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    missing_stage=prerequisite,
                )
        record = self.store.latest_checkpoint(shard_id, turn_index, stage)
        if record is None:
            raise as_error(
                E_INVALID_STAGE,
                f"stage {stage!r} has no published artifact",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        artifact = self.store.read_artifact(record)
        return {
            "artifact": artifact,
            "checkpoint": {
                "sequence": record["sequence"],
                "shard_id": record["shard_id"],
                "turn_index": record["turn_index"],
                "stage": record["stage"],
                "input_fingerprint": record["input_fingerprint"],
                "config_version": record["config_version"],
                "artifact_digest": record["artifact_digest"],
                "artifact_summary": record["artifact_summary"],
            },
        }

    def query_turn(self, shard_id: str, turn_index: int) -> dict[str, Any]:
        dataset = self._require_dataset()
        shard = dataset.shard(shard_id)
        turn = shard.turn(turn_index)
        stage_payloads = {
            stage: self.query_stage(shard_id, turn_index, stage) for stage in STAGES
        }
        return {
            "shard_id": shard_id,
            "turn_index": turn_index,
            "reference": turn.truth_dict(),
            "features": stage_payloads["features"]["artifact"],
            "emotion": stage_payloads["emotion"]["artifact"],
            "behavior": stage_payloads["behavior"]["artifact"],
            "state": stage_payloads["state"]["artifact"],
            "checkpoints": {
                stage: stage_payloads[stage]["checkpoint"] for stage in STAGES
            },
        }

    def query_results(self) -> dict[str, list[dict[str, Any]]]:
        dataset = self._require_dataset()
        return {
            shard.shard_id: [
                self.query_turn(shard.shard_id, turn.index) for turn in shard.turns
            ]
            for shard in dataset.shards
        }

    def version_fingerprints(
        self, shard_id: str, turn_index: int
    ) -> dict[str, Any]:
        dataset = self._require_dataset()
        shard = dataset.shard(shard_id)
        turn = shard.turn(turn_index)
        previous_turn = shard.turns[turn_index - 1] if turn_index > 0 else None
        feature_record = self.store.latest_checkpoint(shard_id, turn_index, "features")
        feature_artifact = (
            self.store.read_artifact(feature_record)
            if feature_record is not None
            else None
        )
        emotion_record = self.store.latest_checkpoint(shard_id, turn_index, "emotion")
        emotion_artifact = (
            self.store.read_artifact(emotion_record)
            if emotion_record is not None
            else None
        )
        previous_emotion_record = (
            self.store.latest_checkpoint(shard_id, turn_index - 1, "emotion")
            if turn_index > 0
            else None
        )
        previous_behavior_record = (
            self.store.latest_checkpoint(shard_id, turn_index - 1, "behavior")
            if turn_index > 0
            else None
        )
        behavior_record = self.store.latest_checkpoint(shard_id, turn_index, "behavior")
        behavior_artifact = (
            self.store.read_artifact(behavior_record)
            if behavior_record is not None
            else None
        )
        previous_state_record = (
            self.store.latest_checkpoint(shard_id, turn_index - 1, "state")
            if turn_index > 0
            else None
        )

        expected: dict[str, Any] = {}
        expected["features"] = {
            "available": True,
            "input_fingerprint": self._feature_fingerprint(shard, turn),
        }
        if feature_artifact is not None:
            expected["emotion"] = {
                "available": True,
                "input_fingerprint": self._emotion_fingerprint(feature_artifact),
            }
        else:
            expected["emotion"] = {"available": False, "reason": "features missing"}
        if emotion_artifact is not None:
            previous_emotion = (
                self.store.read_artifact(previous_emotion_record)
                if previous_emotion_record is not None
                else None
            )
            previous_behavior = (
                self.store.read_artifact(previous_behavior_record)
                if previous_behavior_record is not None
                else None
            )
            expected["behavior"] = {
                "available": True,
                "input_fingerprint": self._behavior_fingerprint(
                    shard,
                    turn,
                    feature_artifact,
                    emotion_artifact,
                    previous_turn,
                    previous_emotion,
                    previous_behavior,
                ),
            }
        else:
            expected["behavior"] = {"available": False, "reason": "emotion missing"}
        if behavior_artifact is not None:
            expected["state"] = {
                "available": True,
                "input_fingerprint": self._state_fingerprint(
                    shard,
                    turn,
                    emotion_artifact,
                    behavior_artifact,
                    self.store.read_artifact(previous_state_record)
                    if previous_state_record is not None
                    else None,
                ),
            }
        else:
            expected["state"] = {"available": False, "reason": "behavior missing"}

        result: dict[str, Any] = {
            "shard_id": shard_id,
            "turn_index": turn_index,
            "config_version": self.config.version,
            "stages": {},
        }
        for stage in STAGES:
            record = self.store.latest_checkpoint(shard_id, turn_index, stage)
            payload = dict(expected[stage])
            payload["config_version"] = self.config.stage_version(stage)
            payload["published"] = record is not None
            if record is not None:
                payload["published_sequence"] = record["sequence"]
                payload["published_input_fingerprint"] = record["input_fingerprint"]
                payload["published_config_version"] = record["config_version"]
                payload["matches"] = (
                    record["input_fingerprint"] == expected[stage].get("input_fingerprint")
                    and record["config_version"] == self.config.stage_version(stage)
                )
                if not payload["matches"]:
                    payload["reason"] = "fingerprint_or_config_changed"
            else:
                payload["matches"] = False
                payload.setdefault("reason", "not_published")
            result["stages"][stage] = payload
        return result

    def version_fingerprints_all(self) -> dict[str, Any]:
        dataset = self._require_dataset()
        return {
            shard.shard_id: [
                self.version_fingerprints(shard.shard_id, turn.index)
                for turn in shard.turns
            ]
            for shard in dataset.shards
        }

    def status(self) -> dict[str, Any]:
        store_status = self.store.status()
        payload: dict[str, Any] = {
            "workspace": str(self.workspace),
            "engine_config_version": self.config.version,
            "checkpoints": store_status,
            "stop_requested": self.stop_requested(),
        }
        if self.dataset is not None:
            payload["dataset"] = {
                "manifest_path": str(self.dataset.manifest_path),
                "shard_count": len(self.dataset.shards),
                "turn_count": sum(shard.turn_count for shard in self.dataset.shards),
                "shards": [
                    {
                        "shard_id": shard.shard_id,
                        "sample_count": shard.sample_count,
                        "turn_count": shard.turn_count,
                        "samples_digest": shard.samples_digest,
                        "manifest_digest": shard.manifest_digest,
                    }
                    for shard in self.dataset.shards
                ],
            }
            mismatches = []
            predicted_invalidation = None
            for shard in self.dataset.shards:
                for turn in shard.turns:
                    for stage, value in self.version_fingerprints(
                        shard.shard_id, turn.index
                    )["stages"].items():
                        if not value["matches"] and value["published"]:
                            mismatch = {
                                "shard_id": shard.shard_id,
                                "turn_index": turn.index,
                                "stage": stage,
                                "reason": value.get("reason"),
                                "published_input_fingerprint": value.get(
                                    "published_input_fingerprint"
                                ),
                                "expected_input_fingerprint": value.get(
                                    "input_fingerprint"
                                ),
                                "published_config_version": value.get(
                                    "published_config_version"
                                ),
                                "expected_config_version": value.get("config_version"),
                            }
                            mismatches.append(mismatch)
                            if predicted_invalidation is None:
                                predicted_invalidation = dict(mismatch)
            payload["pending_invalidations"] = mismatches
            payload["predicted_invalidation"] = predicted_invalidation
            payload["invalidation"] = (
                store_status.get("last_invalidation") or predicted_invalidation
            )
        timings_path = self.diagnostics_dir / "timings.json"
        if timings_path.is_file():
            try:
                payload["timings"] = read_json(timings_path)
            except Exception:
                payload["timings"] = None
        return payload

    def export_summary(self) -> dict[str, Any]:
        dataset = self._require_dataset()
        results = self.query_results()
        shard_summaries: dict[str, dict[str, Any]] = {}
        for shard in dataset.shards:
            artifacts = [
                results[shard.shard_id][turn.index]["state"] for turn in shard.turns
            ]
            shard_summaries[shard.shard_id] = summarize_shard(shard, artifacts)
        summary = build_understanding_summary(
            dataset,
            results,
            shard_summaries,
            self.config.metrics,
            decay=self.config.state.decay,
            engine_config_version=self.config.version,
        )
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.exports_dir / "summary.json", summary)
        return summary

    def export_dialogue_summary(self, output: str | Path | None = None) -> dict[str, Any]:
        summary = self.export_summary()
        if output is not None:
            atomic_write_json(Path(output).expanduser().resolve(), summary)
        return summary



