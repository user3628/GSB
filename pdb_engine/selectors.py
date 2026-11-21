from __future__ import annotations

from typing import Any

from .errors import spec_invalid
from .models import Node, Pod, PDB, Taint, Toleration


def selector_matches(selector: dict[str, Any], labels: dict[str, str]) -> bool:
    """Match either a plain label map or a Kubernetes-style selector object."""

    if not selector:
        return True

    if "matchLabels" in selector or "matchExpressions" in selector:
        match_labels = selector.get("matchLabels", {})
        if not isinstance(match_labels, dict):
            raise spec_invalid("matchLabels must be an object")
        for key, expected in match_labels.items():
            if labels.get(str(key)) != str(expected):
                return False

        expressions = selector.get("matchExpressions", [])
        if not isinstance(expressions, list):
            raise spec_invalid("matchExpressions must be an array")
        for expression in expressions:
            if not isinstance(expression, dict):
                raise spec_invalid("selector expression must be an object")
            key = str(expression.get("key", ""))
            operator = str(expression.get("operator", "In"))
            values = expression.get("values", [])
            if not isinstance(values, list):
                raise spec_invalid("selector expression values must be an array")
            string_values = {str(value) for value in values}
            present = key in labels
            if operator == "In" and (not present or labels[key] not in string_values):
                return False
            if operator == "NotIn" and present and labels[key] in string_values:
                return False
            if operator == "Exists" and not present:
                return False
            if operator == "DoesNotExist" and present:
                return False
            if operator not in {"In", "NotIn", "Exists", "DoesNotExist"}:
                raise spec_invalid("unsupported selector operator", operator=operator)
        return True

    return all(labels.get(str(key)) == str(value) for key, value in selector.items())


def controller_matches(pod: Pod, pdb: PDB) -> bool:
    if pod.controller_type != pdb.controller_type:
        return False
    return pdb.controller_name is None or pod.workload_name == pdb.controller_name


def pod_matches_pdb(pod: Pod, pdb: PDB) -> bool:
    return controller_matches(pod, pdb) and selector_matches(pdb.selector, pod.labels)


def node_selector_matches(pod: Pod, node: Node) -> bool:
    return all(node.labels.get(key) == value for key, value in pod.node_selector.items())


def toleration_matches(toleration: Toleration, taint: Taint) -> bool:
    if toleration.effect and toleration.effect != taint.effect:
        return False
    if toleration.key != taint.key:
        return False
    if toleration.operator == "Exists":
        return True
    if toleration.operator == "Equal":
        return toleration.value == taint.value
    return False


def tolerates_taint(pod: Pod, taint: Taint) -> bool:
    return any(toleration_matches(toleration, taint) for toleration in pod.tolerations)


def untolerated_taints(pod: Pod, node: Node) -> list[Taint]:
    return [
        taint
        for taint in node.taints
        if taint.effect in {"NoSchedule", "NoExecute"} and not tolerates_taint(pod, taint)
    ]

