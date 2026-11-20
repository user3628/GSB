"""Deterministic endpoint matching metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from .errors import invalid_config
from .util import sha256_canonical


@dataclass(frozen=True)
class EndpointMetrics:
    tolerance_ms: float
    reference_count: int
    prediction_count: int
    hits: int
    false_positives: int
    misses: int
    hit_rate: float
    precision: float
    mean_deviation_ms: float
    max_deviation_ms: float
    matches: tuple[tuple[int, int, float, float], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "tolerance_ms": self.tolerance_ms,
            "reference_count": self.reference_count,
            "prediction_count": self.prediction_count,
            "hits": self.hits,
            "false_positives": self.false_positives,
            "misses": self.misses,
            "hit_rate": self.hit_rate,
            "precision": self.precision,
            "mean_deviation_ms": self.mean_deviation_ms,
            "max_deviation_ms": self.max_deviation_ms,
        }


def evaluation_fingerprint(vad_config_fingerprint: str, tolerance_ms: float) -> str:
    if tolerance_ms < 0:
        raise invalid_config("tolerance_ms must be non-negative", tolerance_ms=tolerance_ms)
    return sha256_canonical(
        {
            "schema": "vadkit.evaluation-config.v1",
            "vad_config_fingerprint": vad_config_fingerprint,
            "tolerance_ms": tolerance_ms,
        }
    )


def match_endpoints(
    references: Sequence[dict[str, float]],
    predictions: Sequence[dict[str, float]],
    tolerance_ms: float,
) -> EndpointMetrics:
    if tolerance_ms < 0:
        raise invalid_config("tolerance_ms must be non-negative", tolerance_ms=tolerance_ms)

    possible: list[tuple[float, float, int, int, float, float]] = []
    for ref_index, reference in enumerate(references):
        ref_start = float(reference["start_ms"])
        ref_end = float(reference["end_ms"])
        for pred_index, prediction in enumerate(predictions):
            pred_start = float(prediction["start_ms"])
            pred_end = float(prediction["end_ms"])
            start_deviation = abs(pred_start - ref_start)
            end_deviation = abs(pred_end - ref_end)
            if start_deviation <= tolerance_ms and end_deviation <= tolerance_ms:
                possible.append(
                    (
                        max(start_deviation, end_deviation),
                        start_deviation + end_deviation,
                        ref_index,
                        pred_index,
                        start_deviation,
                        end_deviation,
                    )
                )
    possible.sort(key=lambda item: (item[0], item[1], item[2], item[3]))

    used_references: set[int] = set()
    used_predictions: set[int] = set()
    matches: list[tuple[int, int, float, float]] = []
    for _, _, ref_index, pred_index, start_deviation, end_deviation in possible:
        if ref_index in used_references or pred_index in used_predictions:
            continue
        used_references.add(ref_index)
        used_predictions.add(pred_index)
        matches.append((ref_index, pred_index, start_deviation, end_deviation))
    matches.sort(key=lambda item: (item[0], item[1]))

    hits = len(matches)
    misses = len(references) - hits
    false_positives = len(predictions) - hits
    deviations = [value for match in matches for value in match[2:]]
    mean_deviation = sum(deviations) / len(deviations) if deviations else 0.0
    max_deviation = max(deviations) if deviations else 0.0
    hit_rate = hits / len(references) if references else (1.0 if not predictions else 0.0)
    precision = hits / len(predictions) if predictions else (1.0 if not references else 0.0)
    return EndpointMetrics(
        tolerance_ms=round(float(tolerance_ms), 6),
        reference_count=len(references),
        prediction_count=len(predictions),
        hits=hits,
        false_positives=false_positives,
        misses=misses,
        hit_rate=round(hit_rate, 9),
        precision=round(precision, 9),
        mean_deviation_ms=round(mean_deviation, 6),
        max_deviation_ms=round(max_deviation, 6),
        matches=tuple(matches),
    )


def aggregate_metrics(metrics: Iterable[EndpointMetrics], tolerance_ms: float) -> dict[str, Any]:
    values = list(metrics)
    references = sum(value.reference_count for value in values)
    predictions = sum(value.prediction_count for value in values)
    hits = sum(value.hits for value in values)
    false_positives = sum(value.false_positives for value in values)
    misses = sum(value.misses for value in values)
    deviation_sum = sum(value.mean_deviation_ms * 2 * value.hits for value in values)
    deviation_count = sum(2 * value.hits for value in values)
    max_deviation = max((value.max_deviation_ms for value in values), default=0.0)
    return {
        "tolerance_ms": round(float(tolerance_ms), 6),
        "reference_count": references,
        "prediction_count": predictions,
        "hits": hits,
        "false_positives": false_positives,
        "misses": misses,
        "hit_rate": round(hits / references, 9) if references else 1.0,
        "precision": round(hits / predictions, 9) if predictions else 1.0,
        "mean_deviation_ms": round(deviation_sum / deviation_count, 6)
        if deviation_count
        else 0.0,
        "max_deviation_ms": round(max_deviation, 6),
    }

