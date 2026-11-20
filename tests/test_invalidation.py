from __future__ import annotations

from pathlib import Path

from dialogue_engine.config import (
    BehaviorConfig,
    ClassifierConfig,
    EngineConfig,
    FeatureConfig,
    StateConfig,
)
from dialogue_engine.engine import DialogueEngine

from helpers import clone_data, mutate_manifest


def test_classifier_change_invalidates_emotion_and_downstream(
    demo_data: Path, tmp_path: Path
) -> None:
    workspace = tmp_path / "classifier_change"
    first = DialogueEngine(workspace).run(demo_data)
    changed = DialogueEngine(
        workspace,
        config=EngineConfig(classifier=ClassifierConfig(projection_seed=777)),
    ).run(demo_data)
    assert first["summary_digest"] != changed["summary_digest"]
    assert changed["reused_stages"] == {
        "features": 15,
        "emotion": 0,
        "behavior": 0,
        "state": 0,
    }
    assert changed["computed_stages"] == {
        "features": 0,
        "emotion": 15,
        "behavior": 15,
        "state": 15,
    }
    invalidation = changed["invalidation"]
    assert invalidation["earliest_stage"] == "emotion"
    assert invalidation["earliest_shard_id"] == "shard_01"
    assert invalidation["fingerprints"]["stage"] == "emotion"
    assert (
        invalidation["fingerprints"]["old_input_fingerprint"]
        != invalidation["fingerprints"]["new_input_fingerprint"]
    )
    status = DialogueEngine(
        workspace,
        config=EngineConfig(classifier=ClassifierConfig(projection_seed=777)),
    )
    status.import_dataset(demo_data)
    assert status.status()["pending_invalidations"] == []


def test_behavior_and_state_threshold_changes_have_distinct_earliest_stage(
    demo_data: Path, tmp_path: Path
) -> None:
    workspace = tmp_path / "threshold_changes"
    DialogueEngine(workspace).run(demo_data)
    behavior_config = BehaviorConfig(interruption_max_gap_ms=40.0)
    behavior_run = DialogueEngine(
        workspace, config=EngineConfig(behavior=behavior_config)
    ).run(demo_data)
    assert behavior_run["invalidation"]["earliest_stage"] == "behavior"
    assert behavior_run["reused_stages"]["features"] == 15
    assert behavior_run["reused_stages"]["emotion"] == 15
    assert behavior_run["computed_stages"]["behavior"] == 15

    state_run = DialogueEngine(
        workspace,
        config=EngineConfig(
            behavior=behavior_config,
            state=StateConfig(decay=0.5),
        ),
    ).run(demo_data)
    assert state_run["invalidation"]["earliest_stage"] == "state"
    assert state_run["reused_stages"] == {
        "features": 15,
        "emotion": 15,
        "behavior": 15,
        "state": 0,
    }
    assert state_run["computed_stages"]["state"] == 15


def test_annotation_change_only_invalidates_state(demo_data: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "annotation_change"
    DialogueEngine(workspace).run(demo_data)
    changed_data = clone_data(demo_data, tmp_path / "changed_annotation")

    def mutate(manifest: object) -> None:
        assert isinstance(manifest, dict)
        manifest["shards"][0]["turns"][0]["ref_emotion"] = "悲伤"

    mutate_manifest(changed_data, mutate)
    changed = DialogueEngine(workspace).run(changed_data)
    assert changed["invalidation"]["earliest_stage"] == "state"
    assert changed["reused_stages"] == {
        "features": 15,
        "emotion": 15,
        "behavior": 15,
        "state": 10,
    }
    assert changed["computed_stages"]["state"] == 5


def test_feature_configuration_change_invalidates_from_features(
    demo_data: Path, tmp_path: Path
) -> None:
    workspace = tmp_path / "feature_change"
    DialogueEngine(workspace).run(demo_data)
    changed = DialogueEngine(
        workspace, config=EngineConfig(features=FeatureConfig(frame_ms=20.0))
    ).run(demo_data)
    assert changed["invalidation"]["earliest_stage"] == "features"
    assert changed["reused_stages"] == {
        "features": 0,
        "emotion": 0,
        "behavior": 0,
        "state": 0,
    }
    assert changed["computed_stages"]["features"] == 15

