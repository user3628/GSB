from __future__ import annotations

from typing import Any

from .util import stable_round


def aggregate_metrics(stage3_records: dict[str, dict[str, Any]], classes: list[str]) -> dict[str, Any]:
    records = [record for _, record in sorted(stage3_records.items())]
    per_label: dict[str, Any] = {}
    total_tp = total_fp = total_fn = 0

    for label in classes:
        tp = fp = fn = tn = 0
        ranking: list[tuple[float, str, bool]] = []
        for record in records:
            artifact = record["artifact"]
            label_fact = artifact["labels"].get(label)
            if label_fact is None:
                continue
            truth = bool(label_fact["truth"])
            score = float(label_fact["score"])
            outcome = label_fact["outcome"]
            if outcome == "hit":
                tp += 1
            elif outcome == "false_positive":
                fp += 1
            elif outcome == "miss":
                fn += 1
            else:
                tn += 1
            ranking.append((score, record["sample_key"], truth))

        precision = tp / (tp + fp) if tp + fp else None
        recall = tp / (tp + fn) if tp + fn else None
        f1 = _f1(precision, recall)
        positives = sum(1 for _, _, truth in ranking if truth)
        if positives == 0:
            average_precision = None
        else:
            ranking.sort(key=lambda item: (-item[0], item[1]))
            hits = 0
            precision_sum = 0.0
            for rank, (_, _, truth) in enumerate(ranking, start=1):
                if truth:
                    hits += 1
                    precision_sum += hits / rank
            average_precision = precision_sum / positives
        per_label[label] = {
            "support": tp + fn,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": stable_round(precision),
            "recall": stable_round(recall),
            "f1": stable_round(f1),
            "average_precision": stable_round(average_precision),
        }
        total_tp += tp
        total_fp += fp
        total_fn += fn

    macro = {
        "precision": _mean_defined(per_label, "precision"),
        "recall": _mean_defined(per_label, "recall"),
        "f1": _mean_defined(per_label, "f1"),
        "average_precision": _mean_defined(per_label, "average_precision"),
    }
    micro_precision = total_tp / (total_tp + total_fp) if total_tp + total_fp else None
    micro_recall = total_tp / (total_tp + total_fn) if total_tp + total_fn else None
    micro = {
        "tp": total_tp,
        "fp": total_fp,
        "fn": total_fn,
        "precision": stable_round(micro_precision),
        "recall": stable_round(micro_recall),
        "f1": stable_round(_f1(micro_precision, micro_recall)),
    }
    return {
        "sample_count": len(records),
        "labels": classes,
        "per_label": per_label,
        "macro": macro,
        "micro": micro,
        "cooccurrence": cooccurrence(records, classes),
        "definitions": {
            "precision": "hits / (hits + false_positives); null when denominator is zero",
            "recall": "hits / (hits + misses); null when denominator is zero",
            "average_precision": "sum(precision@rank at each truth positive) / truth positives; ties broken by sample_key",
            "macro": "arithmetic mean over labels with a defined metric",
            "micro": "aggregate TP/FP/FN across all labels",
            "cooccurrence_consistency": "1 - sum(abs(truth_matrix-predicted_matrix)) / max(sum(truth+predicted), 1)",
        },
    }


def cooccurrence(records: list[dict[str, Any]], classes: list[str]) -> dict[str, Any]:
    size = len(classes)
    index = {label: position for position, label in enumerate(classes)}
    truth_matrix = [[0 for _ in range(size)] for _ in range(size)]
    predicted_matrix = [[0 for _ in range(size)] for _ in range(size)]
    for record in records:
        artifact = record["artifact"]
        truth_labels = [label for label in artifact["truth_positive_labels"] if label in index]
        predicted_labels = [label for label in artifact["predicted_positive_labels"] if label in index]
        for labels, matrix in ((truth_labels, truth_matrix), (predicted_labels, predicted_matrix)):
            for left in labels:
                i = index[left]
                matrix[i][i] += 1
                for right in labels:
                    j = index[right]
                    if i < j:
                        matrix[i][j] += 1
                        matrix[j][i] += 1
    delta = [
        [abs(truth_matrix[i][j] - predicted_matrix[i][j]) for j in range(size)]
        for i in range(size)
    ]
    numerator = 0
    denominator = 0
    for i in range(size):
        for j in range(i, size):
            numerator += delta[i][j]
            denominator += truth_matrix[i][j] + predicted_matrix[i][j]
    consistency = 1.0 - numerator / max(denominator, 1)
    return {
        "matrix_labels": classes,
        "truth": truth_matrix,
        "predicted": predicted_matrix,
        "absolute_delta": delta,
        "consistency": stable_round(consistency),
    }


def _mean_defined(per_label: dict[str, Any], key: str) -> float | None:
    values = [item[key] for item in per_label.values() if item[key] is not None]
    if not values:
        return None
    return stable_round(sum(values) / len(values))


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None or precision + recall == 0:
        return None
    return 2.0 * precision * recall / (precision + recall)


__all__ = ["aggregate_metrics", "cooccurrence"]
