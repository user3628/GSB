from __future__ import annotations

from pathlib import Path

from dialogue_engine.config import ClassifierConfig, FeatureConfig
from dialogue_engine.emotion import score_features
from dialogue_engine.features import extract_turn_features
from dialogue_engine.generator import generate_demo_dataset
from dialogue_engine.manifest import load_dataset
from dialogue_engine.serialization import canonical_json_bytes

from helpers import tree_digests


def test_generator_is_seeded_and_covers_demo_requirements(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_demo_dataset(first, seed=20_261_121, force=True)
    generate_demo_dataset(second, seed=20_261_121, force=True)
    assert tree_digests(first) == tree_digests(second)

    dataset = load_dataset(first)
    assert len(dataset.shards) == 3
    assert sum(shard.turn_count for shard in dataset.shards) == 15
    speakers = {turn.speaker_id for shard in dataset.shards for turn in shard.turns}
    emotions = {turn.ref_emotion for shard in dataset.shards for turn in shard.turns}
    acts = {turn.ref_dialogue_act for shard in dataset.shards for turn in shard.turns}
    texts = " ".join(turn.text for shard in dataset.shards for turn in shard.turns)
    assert speakers == {"S1", "S2", "S3"}
    assert emotions == {"平静", "高兴", "愤怒", "悲伤"}
    assert acts == {"陈述", "提问", "确认", "打断"}
    assert any("我们" in turn.text for shard in dataset.shards for turn in shard.turns)
    assert any("resume" in turn.text.lower() for shard in dataset.shards for turn in shard.turns)
    assert "Export the summary" in texts


def test_turn_features_and_emotion_scores_are_byte_deterministic(
    demo_data: Path,
) -> None:
    dataset = load_dataset(demo_data)
    shard = dataset.shards[0]
    turn = shard.turns[1]
    feature_config = FeatureConfig()
    classifier_config = ClassifierConfig()
    feature_a = extract_turn_features(shard, turn, feature_config)
    feature_b = extract_turn_features(shard, turn, feature_config)
    assert canonical_json_bytes(feature_a) == canonical_json_bytes(feature_b)
    score_a = score_features(
        feature_a,
        feature_config=feature_config,
        classifier_config=classifier_config,
        shard_id=shard.shard_id,
        turn_index=turn.index,
    )
    score_b = score_features(
        feature_b,
        feature_config=feature_config,
        classifier_config=classifier_config,
        shard_id=shard.shard_id,
        turn_index=turn.index,
    )
    assert canonical_json_bytes(score_a) == canonical_json_bytes(score_b)
    assert abs(sum(score_a["scores"].values()) - 1.0) < 1e-9
    assert score_a["label"] == turn.ref_emotion

