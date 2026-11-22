from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable

from .errors import (
    E_FORBIDDEN_SUBJECT,
    E_REFERENCE_MISSING,
    E_SEMANTIC_CONFLICT,
    error_record,
)
from .text import (
    CONSTRAINT,
    SCENE_DESCRIPTION,
    SUBJECT_DESCRIPTION,
    normalize_terms,
)


METRIC_SUBJECT = "must_subject_coverage"
METRIC_SCENE = "scene_coverage"
METRIC_DURATION = "duration_consistency"
METRIC_SCORE = "score"


def _term_key(term: str) -> tuple[int, str]:
    return (-len(term), term)


def longest_term_matches(
    units: Iterable[dict[str, Any]], terms: Iterable[str]
) -> list[dict[str, Any]]:
    """Return non-overlapping, leftmost-longest term matches in half-open spans."""
    ordered_terms = sorted(normalize_terms(terms), key=_term_key)
    if not ordered_terms:
        return []
    matches: list[dict[str, Any]] = []
    for unit in units:
        text = unit["text"]
        folded = text.casefold()
        position = 0
        while position < len(text):
            chosen: str | None = None
            for term in ordered_terms:
                end = position + len(term)
                if end <= len(text) and folded[position:end] == term.casefold():
                    chosen = term
                    break
            if chosen is None:
                position += 1
                continue
            end = position + len(chosen)
            matches.append(
                {
                    "term": chosen,
                    "start": unit["start"] + position,
                    "end": unit["start"] + end,
                    "unit_id": unit["unit_id"],
                    "unit_type": unit["type"],
                }
            )
            position = end
    return matches


def _reference_unit_type(
    reference_shot: dict[str, Any] | None, start: int, end: int
) -> str | None:
    if not reference_shot:
        return None
    for unit in reference_shot.get("units", []):
        if unit["start"] <= start and end <= unit["end"]:
            return unit["type"]
    return None


def _match_is_in_type(
    match: dict[str, Any],
    expected_type: str,
    reference_shot: dict[str, Any] | None,
) -> bool:
    truth_type = _reference_unit_type(reference_shot, match["start"], match["end"])
    return truth_type == expected_type


def _window_overlaps(
    match: dict[str, Any], window_start: int, window_end: int
) -> bool:
    return match["start"] < window_end and window_start < match["end"]


def _component(numerator: int, denominator: int) -> dict[str, Any]:
    if denominator <= 0:
        return {
            "numerator": numerator,
            "denominator": denominator,
            "value": None,
            "applicable": False,
        }
    value = (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.000001"), rounding=ROUND_HALF_UP
    )
    return {
        "numerator": numerator,
        "denominator": denominator,
        "value": float(value),
        "applicable": True,
    }


def _coverage_config(config: dict[str, Any]) -> dict[str, Any]:
    coverage = config.get("coverage", {})
    return {
        "negative_window_chars": int(coverage.get("negative_window_chars", 18)),
        "weights": {
            METRIC_SUBJECT: coverage.get("weights", {}).get(METRIC_SUBJECT, 0.45),
            METRIC_SCENE: coverage.get("weights", {}).get(METRIC_SCENE, 0.25),
            METRIC_DURATION: coverage.get(
                "weights", {}
            ).get(METRIC_DURATION, 0.30),
        },
        "review_min_score": coverage.get("review_min_score", 0.70),
    }


def _score(
    coverage: dict[str, dict[str, Any]], weights: dict[str, Decimal]
) -> float | None:
    total_weight = Decimal(0)
    weighted = Decimal(0)
    for metric in (METRIC_SUBJECT, METRIC_SCENE, METRIC_DURATION):
        component = coverage[metric]
        weight = Decimal(str(weights[metric]))
        if not component["applicable"] or weight <= 0:
            continue
        total_weight += weight
        weighted += weight * Decimal(str(component["value"]))
    if total_weight == 0:
        return None
    return float(
        (weighted / total_weight).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP
        )
    )


def _within(value: float, interval: Any) -> bool | None:
    if not isinstance(interval, dict) or "min" not in interval or "max" not in interval:
        return None
    return float(interval["min"]) <= value <= float(interval["max"])


def _check_targets(
    *,
    shot_id: str,
    coverage: dict[str, dict[str, Any]],
    score: float | None,
    reference_shot: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    checks: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    missing = False
    targets = reference_shot.get("targets", {}) if reference_shot else {}

    for metric in (METRIC_SUBJECT, METRIC_SCENE, METRIC_DURATION):
        component = coverage[metric]
        if not component["applicable"]:
            checks.append({"metric": metric, "status": "not_applicable"})
            continue
        interval = targets.get(metric)
        result = _within(float(component["value"]), interval)
        if result is None:
            missing = True
            checks.append({"metric": metric, "status": "reference_missing"})
            errors.append(
                error_record(
                    E_REFERENCE_MISSING,
                    "coverage",
                    "reference target interval is missing for an applicable metric",
                    shot_id=shot_id,
                    metric=metric,
                )
            )
            continue
        checks.append(
            {
                "metric": metric,
                "status": "within" if result else "outside",
                "value": component["value"],
                "target": interval,
            }
        )

    if score is None:
        checks.append({"metric": METRIC_SCORE, "status": "not_applicable"})
    else:
        interval = targets.get(METRIC_SCORE)
        result = _within(score, interval)
        if result is None:
            missing = True
            checks.append({"metric": METRIC_SCORE, "status": "reference_missing"})
            errors.append(
                error_record(
                    E_REFERENCE_MISSING,
                    "coverage",
                    "reference target interval is missing for the composite score",
                    shot_id=shot_id,
                    metric=METRIC_SCORE,
                )
            )
        else:
            checks.append(
                {
                    "metric": METRIC_SCORE,
                    "status": "within" if result else "outside",
                    "value": score,
                    "target": interval,
                }
            )
    return checks, errors, missing


def analyze_shot(
    *,
    shot: dict[str, Any],
    subject_entry: dict[str, Any],
    reference_shot: dict[str, Any] | None,
    normalize_result: dict[str, Any],
    duration_result: dict[str, Any] | None,
    config: dict[str, Any],
    candidate_units: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute interval matches, conflicts, coverage, score, and verdict."""
    shot_id = shot["shot_id"]
    units = candidate_units if candidate_units is not None else normalize_result["units"]
    reference_units = reference_shot.get("units", []) if reference_shot else []

    required_subjects = normalize_terms(subject_entry.get("required_subjects", []))
    forbidden_subjects = normalize_terms(subject_entry.get("forbidden_subjects", []))
    scene_terms = normalize_terms(shot.get("must_scenes", []))
    negative_terms = normalize_terms(shot.get("negative_terms", []))

    subject_matches = longest_term_matches(units, required_subjects)
    scene_matches = longest_term_matches(units, scene_terms)
    negative_matches = longest_term_matches(units, negative_terms)
    forbidden_matches = longest_term_matches(units, forbidden_subjects)

    coverage_config = _coverage_config(config)
    window_chars = coverage_config["negative_window_chars"]
    prompt_length = len(normalize_result["normalized_prompt"])
    negative_windows = [
        {
            "term": match["term"],
            "term_start": match["start"],
            "term_end": match["end"],
            "start": match["end"],
            "end": min(prompt_length, match["end"] + window_chars),
        }
        for match in negative_matches
    ]

    conflicts: list[dict[str, Any]] = []
    conflicted_subjects: set[str] = set()
    for subject in required_subjects:
        subject_hits = [match for match in subject_matches if match["term"] == subject]
        for subject_hit in subject_hits:
            matching_window = next(
                (
                    window
                    for window in negative_windows
                    if _window_overlaps(subject_hit, window["start"], window["end"])
                ),
                None,
            )
            if matching_window is not None and subject not in conflicted_subjects:
                conflicted_subjects.add(subject)
                conflicts.append(
                    {
                        "code": E_SEMANTIC_CONFLICT,
                        "type": "required_subject_in_negative_window",
                        "subject": subject,
                        "required_span": {
                            "start": subject_hit["start"],
                            "end": subject_hit["end"],
                            "unit_id": subject_hit["unit_id"],
                        },
                        "negative_span": {
                            "start": matching_window["term_start"],
                            "end": matching_window["term_end"],
                        },
                        "window": {
                            "start": matching_window["start"],
                            "end": matching_window["end"],
                        },
                        "reason": (
                            "required subject appears inside the configurable window "
                            "after a negative constraint"
                        ),
                    }
                )
                break

    forbidden_hits = [
        {
            "subject": match["term"],
            "start": match["start"],
            "end": match["end"],
            "unit_id": match["unit_id"],
            "reason": "subject is declared forbidden by the subject table",
        }
        for match in forbidden_matches
    ]

    for subject in forbidden_subjects:
        if subject not in required_subjects or subject in conflicted_subjects:
            continue
        subject_hit = next(
            (match for match in subject_matches if match["term"] == subject), None
        )
        if subject_hit is None:
            continue
        conflicted_subjects.add(subject)
        conflicts.append(
            {
                "code": E_SEMANTIC_CONFLICT,
                "type": "required_subject_also_forbidden_by_table",
                "subject": subject,
                "required_span": {
                    "start": subject_hit["start"],
                    "end": subject_hit["end"],
                    "unit_id": subject_hit["unit_id"],
                },
                "negative_span": None,
                "window": None,
                "reason": (
                    "subject table declares the same subject as required and "
                    "forbidden; no truth-preserving precedence may be inferred"
                ),
            }
        )

    subject_matches_with_status: list[dict[str, Any]] = []
    covered_subjects: set[str] = set()
    for match in subject_matches:
        eligible = _match_is_in_type(match, SUBJECT_DESCRIPTION, reference_shot)
        status = "eligible" if eligible else "outside_subject_truth"
        if match["term"] in conflicted_subjects:
            status = "conflicted"
        elif eligible:
            covered_subjects.add(match["term"])
        subject_matches_with_status.append({**match, "eligible": eligible, "status": status})

    covered_scenes: set[str] = set()
    scene_matches_with_status: list[dict[str, Any]] = []
    for match in scene_matches:
        eligible = _match_is_in_type(match, SCENE_DESCRIPTION, reference_shot)
        if eligible:
            covered_scenes.add(match["term"])
        scene_matches_with_status.append(
            {**match, "eligible": eligible, "status": "eligible" if eligible else "outside_scene_truth"}
        )

    subject_component = _component(len(covered_subjects), len(required_subjects))
    scene_component = _component(len(covered_scenes), len(scene_terms))

    duration_component: dict[str, Any]
    reference_duration = (reference_shot or {}).get("duration")
    if duration_result is None or not reference_duration or "target_ms" not in reference_duration:
        duration_component = {
            "numerator": 0,
            "denominator": 0,
            "value": None,
            "applicable": False,
        }
    else:
        consistent = int(duration_result["duration_ms"] == reference_duration["target_ms"])
        duration_component = _component(consistent, 1)

    coverage = {
        METRIC_SUBJECT: subject_component,
        METRIC_SCENE: scene_component,
        METRIC_DURATION: duration_component,
    }
    weights_decimal = {
        metric: Decimal(str(value))
        for metric, value in coverage_config["weights"].items()
    }
    score = _score(coverage, weights_decimal)
    target_checks, target_errors, missing_reference = _check_targets(
        shot_id=shot_id,
        coverage=coverage,
        score=score,
        reference_shot=reference_shot,
    )

    reasons: list[str] = []
    decision_errors: list[dict[str, Any]] = []
    for conflict in conflicts:
        decision_errors.append(
            error_record(
                E_SEMANTIC_CONFLICT,
                "coverage",
                conflict["reason"],
                shot_id=shot_id,
                conflict_type=conflict["type"],
                subject=conflict["subject"],
                required_span=conflict["required_span"],
                negative_span=conflict["negative_span"],
                window=conflict["window"],
            )
        )
    for hit in forbidden_hits:
        decision_errors.append(
            error_record(
                E_FORBIDDEN_SUBJECT,
                "coverage",
                "a subject declared forbidden by the subject table appears in the prompt",
                shot_id=shot_id,
                subject=hit["subject"],
                span={"start": hit["start"], "end": hit["end"]},
            )
        )
    decision_errors.extend(target_errors)

    if conflicts:
        reasons.append("semantic_conflict")
    if forbidden_hits:
        reasons.append("forbidden_subject_present")
    if missing_reference:
        reasons.append("reference_missing")
    for check in target_checks:
        if check["status"] == "outside":
            reasons.append(f"target_outside:{check['metric']}")

    forced_reject = bool(conflicts or forbidden_hits)
    all_not_applicable = not any(component["applicable"] for component in coverage.values())
    review_min_score = float(coverage_config["review_min_score"])
    if forced_reject:
        verdict = "rejected"
        reasons.append("forced_reject_by_violation")
    elif missing_reference:
        verdict = "review"
        reasons.append("cannot_pass_with_missing_reference")
    elif all_not_applicable or score is None:
        verdict = "review"
        reasons.append("insufficient_applicable_metrics")
    elif any(check["status"] == "outside" for check in target_checks):
        if score < review_min_score:
            verdict = "rejected"
            reasons.append("score_below_review_threshold")
        else:
            verdict = "review"
            reasons.append("truth_target_interval_not_met")
    else:
        verdict = "pass"

    na_components = [
        metric for metric, component in coverage.items() if not component["applicable"]
    ]

    return {
        "shot_id": shot_id,
        "normalized_prompt": normalize_result["normalized_prompt"],
        "required_subjects": required_subjects,
        "forbidden_subjects": forbidden_subjects,
        "scene_terms": scene_terms,
        "negative_terms": negative_terms,
        "subject_matches": subject_matches_with_status,
        "scene_matches": scene_matches_with_status,
        "negative_matches": negative_matches,
        "negative_windows": negative_windows,
        "conflicts": conflicts,
        "forbidden_hits": forbidden_hits,
        "conflicted_subjects": sorted(conflicted_subjects),
        "coverage": coverage,
        "weights": coverage_config["weights"],
        "score": score,
        "target_checks": target_checks,
        "na_components": na_components,
        "verdict": verdict,
        "reasons": sorted(set(reasons)),
        "errors": decision_errors,
        "reference_unit_count": len(reference_units),
    }
