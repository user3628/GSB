"""Reference-label metrics and deterministic dialogue understanding exports."""

from __future__ import annotations

from typing import Any

from .config import DIALOGUE_ACTS, EMOTIONS, MetricsConfig
from .manifest import Dataset, ShardData
from .serialization import digest_json, round_floats


def _empty_confusion(labels: tuple[str, ...]) -> dict[str, dict[str, int]]:
    return {reference: {predicted: 0 for predicted in labels} for reference in labels}


def _reference_distribution(
    shard: ShardData, decay: float
) -> list[dict[str, float]]:
    cumulative = {label: 0.0 for label in EMOTIONS}
    result: list[dict[str, float]] = []
    for turn in shard.turns:
        cumulative = {
            label: decay * cumulative[label] + (1.0 if label == turn.ref_emotion else 0.0)
            for label in EMOTIONS
        }
        total = sum(cumulative.values())
        result.append({label: cumulative[label] / total for label in EMOTIONS})
    return result


def evaluate_results(
    dataset: Dataset,
    results_by_shard: dict[str, list[dict[str, Any]]],
    config: MetricsConfig,
    *,
    decay: float,
) -> dict[str, Any]:
    emotion_confusion = _empty_confusion(EMOTIONS)
    behavior_confusion = _empty_confusion(DIALOGUE_ACTS)
    emotion_correct = 0
    emotion_tolerant_correct = 0
    behavior_correct = 0
    interruption_true_positive = 0
    interruption_false_positive = 0
    interruption_false_negative = 0
    state_distance_total = 0.0
    state_tolerance_hits = 0
    state_dominant_hits = 0
    turn_count = 0
    per_shard: dict[str, Any] = {}

    for shard in dataset.shards:
        shard_results = results_by_shard.get(shard.shard_id, [])
        reference_distribution = _reference_distribution(shard, decay)
        shard_emotion_correct = 0
        shard_behavior_correct = 0
        shard_state_distance = 0.0
        for index, result in enumerate(shard_results):
            reference = result["reference"]
            emotion = result["emotion"]
            behavior = result["behavior"]
            state = result["state"]["state"]
            predicted_emotion = str(emotion["label"])
            reference_emotion = str(reference["ref_emotion"])
            predicted_behavior = str(behavior["label"])
            reference_behavior = str(reference["ref_dialogue_act"])

            emotion_confusion[reference_emotion][predicted_emotion] += 1
            behavior_confusion[reference_behavior][predicted_behavior] += 1
            exact = predicted_emotion == reference_emotion
            top_score = max(float(value) for value in emotion["scores"].values())
            reference_score = float(emotion["scores"][reference_emotion])
            tolerant = exact or (top_score - reference_score) <= config.label_score_tolerance
            emotion_correct += int(exact)
            emotion_tolerant_correct += int(tolerant)
            shard_emotion_correct += int(exact)
            behavior_hit = predicted_behavior == reference_behavior
            behavior_correct += int(behavior_hit)
            shard_behavior_correct += int(behavior_hit)

            if reference_behavior == "打断" and predicted_behavior == "打断":
                interruption_true_positive += 1
            elif reference_behavior != "打断" and predicted_behavior == "打断":
                interruption_false_positive += 1
            elif reference_behavior == "打断" and predicted_behavior != "打断":
                interruption_false_negative += 1

            predicted_distribution = state["normalized_emotions"]
            distance = 0.5 * sum(
                abs(
                    float(predicted_distribution[label])
                    - reference_distribution[index][label]
                )
                for label in EMOTIONS
            )
            state_distance_total += distance
            shard_state_distance += distance
            state_tolerance_hits += int(distance <= config.trace_tolerance)
            reference_dominant = max(
                EMOTIONS,
                key=lambda label: (
                    reference_distribution[index][label],
                    -EMOTIONS.index(label),
                ),
            )
            state_dominant_hits += int(state["dominant_emotion"] == reference_dominant)
            turn_count += 1

        shard_count = len(shard_results)
        per_shard[shard.shard_id] = {
            "turn_count": shard_count,
            "emotion_accuracy": (
                shard_emotion_correct / shard_count if shard_count else 0.0
            ),
            "behavior_accuracy": (
                shard_behavior_correct / shard_count if shard_count else 0.0
            ),
            "state_trace_deviation": (
                shard_state_distance / shard_count if shard_count else 0.0
            ),
        }

    interruption_precision = (
        interruption_true_positive
        / (interruption_true_positive + interruption_false_positive)
        if interruption_true_positive + interruption_false_positive
        else 0.0
    )
    interruption_recall = (
        interruption_true_positive
        / (interruption_true_positive + interruption_false_negative)
        if interruption_true_positive + interruption_false_negative
        else 0.0
    )
    metrics = {
        "turn_count": turn_count,
        "emotion_accuracy": emotion_correct / turn_count if turn_count else 0.0,
        "emotion_tolerant_accuracy": (
            emotion_tolerant_correct / turn_count if turn_count else 0.0
        ),
        "emotion_confusion_matrix": emotion_confusion,
        "behavior_accuracy": behavior_correct / turn_count if turn_count else 0.0,
        "behavior_confusion_matrix": behavior_confusion,
        "interruption_recall": interruption_recall,
        "interruption_precision": interruption_precision,
        "state_trace_deviation": (
            state_distance_total / turn_count if turn_count else 0.0
        ),
        "state_trace_tolerance_agreement": (
            state_tolerance_hits / turn_count if turn_count else 0.0
        ),
        "state_dominant_accuracy": (
            state_dominant_hits / turn_count if turn_count else 0.0
        ),
        "tolerances": {
            "label_score_tolerance": config.label_score_tolerance,
            "trace_tolerance": config.trace_tolerance,
        },
        "per_shard": per_shard,
    }
    rounded_metrics = round_floats(metrics, config.decimals)
    rounded_metrics["metrics_digest"] = digest_json(rounded_metrics)
    return rounded_metrics


def build_understanding_summary(
    dataset: Dataset,
    results_by_shard: dict[str, list[dict[str, Any]]],
    shard_summaries: dict[str, dict[str, Any]],
    config: MetricsConfig,
    *,
    decay: float,
    engine_config_version: str,
) -> dict[str, Any]:
    metrics = evaluate_results(dataset, results_by_shard, config, decay=decay)
    shards_payload = []
    for shard in dataset.shards:
        reference_distribution = _reference_distribution(shard, decay)
        turns_payload = []
        for index, result in enumerate(results_by_shard.get(shard.shard_id, [])):
            reference = result["reference"]
            emotion = result["emotion"]
            behavior = result["behavior"]
            state = result["state"]["state"]
            emotion_scores = emotion["scores"]
            top_score = max(float(value) for value in emotion_scores.values())
            reference_emotion_score = float(emotion_scores[reference["ref_emotion"]])
            emotion_exact = emotion["label"] == reference["ref_emotion"]
            emotion_tolerant = (
                emotion_exact
                or top_score - reference_emotion_score <= config.label_score_tolerance
            )
            trace_distance = 0.5 * sum(
                abs(
                    float(state["normalized_emotions"][label])
                    - reference_distribution[index][label]
                )
                for label in EMOTIONS
            )
            reference_dominant = max(
                EMOTIONS,
                key=lambda label: (
                    reference_distribution[index][label],
                    -EMOTIONS.index(label),
                ),
            )
            turns_payload.append(
                {
                    "turn_index": result["turn_index"],
                    "reference": reference,
                    "emotion": emotion,
                    "behavior": behavior,
                    "state": state,
                    "evaluation": {
                        "emotion_exact": emotion_exact,
                        "emotion_tolerant": emotion_tolerant,
                        "behavior_exact": behavior["label"]
                        == reference["ref_dialogue_act"],
                        "interruption_correct": behavior["label"]
                        == reference["ref_dialogue_act"]
                        if reference["ref_dialogue_act"] == "打断"
                        or behavior["label"] == "打断"
                        else True,
                        "state_trace_distance": trace_distance,
                        "state_trace_within_tolerance": trace_distance
                        <= config.trace_tolerance,
                        "state_dominant_correct": state["dominant_emotion"]
                        == reference_dominant,
                    },
                }
            )
        shards_payload.append(
            {
                "shard_id": shard.shard_id,
                "summary": shard_summaries.get(shard.shard_id),
                "turns": turns_payload,
            }
        )
    summary = round_floats({
        "schema_version": 1,
        "engine_config_version": engine_config_version,
        "metrics": metrics,
        "shards": shards_payload,
    }, config.decimals)
    summary["summary_digest"] = digest_json(summary)
    return summary

