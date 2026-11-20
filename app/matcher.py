from __future__ import annotations

from typing import Any

from app.models import LabelSelector, SelectorRequirement, Taint, Toleration


def evaluate_requirement(labels: dict[str, str], requirement: SelectorRequirement) -> tuple[bool, dict[str, Any]]:
    label_value = labels.get(requirement.key)
    exists = requirement.key in labels
    operator = requirement.operator
    passed = False
    reason = ""

    if operator == "In":
        passed = exists and label_value in requirement.values
        reason = "label value is in the required set" if passed else "label is missing or its value is not in the required set"
    elif operator == "NotIn":
        passed = (not exists) or label_value not in requirement.values
        reason = "label is absent or outside the forbidden set" if passed else "label value is in the forbidden set"
    elif operator == "Exists":
        passed = exists
        reason = "label exists" if passed else "label does not exist"
    elif operator == "DoesNotExist":
        passed = not exists
        reason = "label does not exist" if passed else "label exists"
    elif operator in ("Gt", "Lt"):
        if not exists or label_value is None:
            reason = "label does not exist"
        else:
            try:
                actual = int(label_value)
                expected = int(requirement.values[0])
                passed = actual > expected if operator == "Gt" else actual < expected
                reason = f"numeric label comparison {actual} {operator} {expected} is {passed}"
            except (TypeError, ValueError):
                reason = "label value is not an integer"
    else:  # pragma: no cover - protects callers constructing domain objects directly
        reason = f"unsupported operator {operator}"

    return passed, {
        "key": requirement.key,
        "operator": operator,
        "expectedValues": list(requirement.values),
        "labelValue": label_value,
        "labelExists": exists,
        "passed": passed,
        "reason": reason,
    }


def evaluate_term(
    labels: dict[str, str], requirements: tuple[SelectorRequirement, ...]
) -> tuple[bool, list[dict[str, Any]], dict[str, Any] | None]:
    results: list[dict[str, Any]] = []
    first_failure: dict[str, Any] | None = None
    for requirement in requirements:
        passed, result = evaluate_requirement(labels, requirement)
        results.append(result)
        if not passed and first_failure is None:
            first_failure = result
    return first_failure is None, results, first_failure


def matches_label_selector(labels: dict[str, str], selector: LabelSelector) -> bool:
    for key, expected in selector.match_labels:
        if labels.get(key) != expected:
            return False
    for requirement in selector.match_expressions:
        passed, _ = evaluate_requirement(labels, requirement)
        if not passed:
            return False
    return True


def tolerance_matches(taint: Taint, toleration: Toleration, *, effect: str) -> bool:
    if toleration.effect is not None and toleration.effect != effect:
        return False
    if toleration.operator == "Exists":
        return toleration.key == "" or toleration.key == taint.key
    return toleration.key == taint.key and toleration.value == taint.value


def topology_value(labels: dict[str, str], topology_key: str) -> str | None:
    return labels.get(topology_key)
