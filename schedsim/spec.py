"""Deterministic validation and normalization of batch, node, and Pod specs."""

from __future__ import annotations

import json
from typing import Any

from .errors import E_SPEC_INVALID, E_TOPOLOGY_KEY_MISSING, SchedError
from .models import (
    BatchSpec,
    BudgetConfig,
    LabelRequirement,
    LabelSelector,
    NodeAffinityRules,
    NodeSelectorTerm,
    NodeSpec,
    PodAffinityRules,
    PodAffinityTerm,
    PodSpec,
    ResourceVector,
    Taint,
    Toleration,
    WeightedNodeTerm,
    WeightedPodTerm,
)
from .quantities import QuantityError, parse_cpu_millicores, parse_memory_bytes

NODE_AFFINITY_OPERATORS = {"In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"}
POD_SELECTOR_OPERATORS = {"In", "NotIn", "Exists", "DoesNotExist"}
TAINT_EFFECTS = {"NoSchedule", "NoExecute", "PreferNoSchedule"}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return repr(value)


def _field(obj: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in obj:
            return obj[name]
    return default


class _Validation:
    def __init__(self) -> None:
        self.spec_failures: list[dict[str, Any]] = []
        self.topology_failures: list[dict[str, Any]] = []

    def fail(self, path: str, reason: str, value: Any = None) -> None:
        failure: dict[str, Any] = {"path": path, "reason": reason}
        if value is not None:
            failure["value"] = _json_safe(value)
        self.spec_failures.append(failure)

    def topology(self, path: str, reason: str, value: Any = None) -> None:
        failure: dict[str, Any] = {"path": path, "reason": reason}
        if value is not None:
            failure["value"] = _json_safe(value)
        self.topology_failures.append(failure)

    def raise_if_any(self) -> None:
        if self.spec_failures:
            raise SchedError(
                E_SPEC_INVALID,
                "batch specification is invalid; no batch was persisted or scheduled",
                {"failures": self.spec_failures},
                http_status=422,
            )
        if self.topology_failures:
            raise SchedError(
                E_TOPOLOGY_KEY_MISSING,
                "a Pod affinity topology key is not declared by any node",
                {"failures": self.topology_failures},
                http_status=422,
            )


def _read_object(value: Any, path: str, validation: _Validation) -> dict[str, Any]:
    if not isinstance(value, dict):
        validation.fail(path, "expected an object", value)
        return {}
    return value


def _read_list(value: Any, path: str, validation: _Validation) -> list[Any]:
    if not isinstance(value, list):
        validation.fail(path, "expected a list", value)
        return []
    return value


def _read_string(value: Any, path: str, validation: _Validation, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        validation.fail(path, "expected a string", value)
        return ""
    if not allow_empty and not value:
        validation.fail(path, "must not be empty", value)
    return value


def _read_int(
    value: Any,
    path: str,
    validation: _Validation,
    *,
    minimum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        validation.fail(path, "expected an integer", value)
        return 0
    if minimum is not None and value < minimum:
        validation.fail(path, f"must be greater than or equal to {minimum}", value)
    return value


def _read_bool(value: Any, path: str, validation: _Validation) -> bool:
    if not isinstance(value, bool):
        validation.fail(path, "expected a boolean", value)
        return False
    return value


def _read_string_map(value: Any, path: str, validation: _Validation) -> dict[str, str]:
    obj = _read_object(value, path, validation)
    result: dict[str, str] = {}
    for key, item in obj.items():
        if not isinstance(key, str) or not key:
            validation.fail(f"{path}.<key>", "label/selector key must be a non-empty string", key)
            continue
        if not isinstance(item, str):
            validation.fail(f"{path}.{key}", "expected a string", item)
            continue
        result[key] = item
    return result


def _read_resource_vector(
    value: Any,
    path: str,
    validation: _Validation,
    *,
    require_fields: bool = False,
) -> tuple[ResourceVector, dict[str, str]]:
    obj = _read_object(value, path, validation)
    raw: dict[str, str] = {}
    parsed: dict[str, int] = {}
    for field_name, parser, target in (
        ("cpu", parse_cpu_millicores, "cpu_millicores"),
        ("memory", parse_memory_bytes, "memory_bytes"),
    ):
        if field_name not in obj:
            if require_fields:
                validation.fail(f"{path}.{field_name}", "resource quantity is required", None)
            parsed[target] = 0
            continue
        raw_value = obj[field_name]
        raw[field_name] = raw_value if isinstance(raw_value, str) else repr(raw_value)
        try:
            parsed[target] = parser(raw_value)
        except QuantityError as exc:
            validation.fail(f"{path}.{field_name}", str(exc), raw_value)
            parsed[target] = 0
    return (
        ResourceVector(parsed["cpu_millicores"], parsed["memory_bytes"]),
        raw,
    )


def _parse_budget(value: Any, validation: _Validation) -> BudgetConfig:
    if value is None:
        return BudgetConfig()
    obj = _read_object(value, "budget", validation)

    def pick(default: int, minimum: int, *names: str) -> int:
        for name in names:
            if name in obj:
                return _read_int(obj[name], f"budget.{name}", validation, minimum=minimum)
        return default

    return BudgetConfig(
        concurrency_limit=pick(2, 1, "concurrency_limit", "global_concurrency_limit", "concurrency"),
        time_budget_ms=pick(1000, 0, "time_budget_ms", "round_time_budget_ms"),
        per_node_round_limit=pick(2, 1, "per_node_round_limit", "per_node_limit", "single_node_limit"),
        queue_capacity=pick(256, 0, "queue_capacity", "waiting_queue_capacity", "max_queue"),
        node_check_cost_ms=pick(1, 1, "node_check_cost_ms"),
        score_tolerance=pick(5, 0, "score_tolerance"),
    )


def _parse_taints(value: Any, path: str, validation: _Validation) -> tuple[Taint, ...]:
    items = _read_list(value, path, validation)
    result: list[Taint] = []
    for index, item in enumerate(items):
        item_path = f"{path}[{index}]"
        obj = _read_object(item, item_path, validation)
        key = _read_string(obj.get("key"), f"{item_path}.key", validation)
        taint_value = obj.get("value", "")
        if not isinstance(taint_value, str):
            validation.fail(f"{item_path}.value", "expected a string", taint_value)
            taint_value = ""
        effect = _read_string(obj.get("effect"), f"{item_path}.effect", validation)
        if not effect:
            validation.fail(f"{item_path}.effect", "taint effect is required", obj.get("effect"))
        elif effect not in TAINT_EFFECTS:
            validation.fail(
                f"{item_path}.effect",
                "effect must be NoSchedule, NoExecute, or PreferNoSchedule",
                effect,
            )
        result.append(Taint(key, taint_value, effect, canonical_json(item)))
    return tuple(result)


def _parse_node(item: Any, index: int, validation: _Validation) -> NodeSpec | None:
    path = f"nodes[{index}]"
    obj = _read_object(item, path, validation)
    name = _read_string(obj.get("name"), f"{path}.name", validation)
    allocatable_raw = _field(obj, "allocatable", "capacity", default=None)
    allocatable_path = f"{path}.allocatable" if "allocatable" in obj else f"{path}.capacity"
    allocatable, raw_allocatable = _read_resource_vector(
        allocatable_raw, allocatable_path, validation, require_fields=True
    )
    labels = _read_string_map(obj.get("labels", {}), f"{path}.labels", validation)

    healthy_value = obj.get("healthy")
    if healthy_value is not None:
        healthy = _read_bool(healthy_value, f"{path}.healthy", validation)
    elif "status" in obj:
        status = _read_string(obj["status"], f"{path}.status", validation)
        healthy = status in {"Ready", "ready", "Healthy", "healthy", "True", "true"}
    else:
        healthy = True

    failure_domain_value = _field(obj, "failure_domain", "failureDomain", default=None)
    failure_domain: str | None = None
    if failure_domain_value is not None:
        failure_domain = _read_string(failure_domain_value, f"{path}.failure_domain", validation)

    taints = _parse_taints(obj.get("taints", []), f"{path}.taints", validation)
    if not name:
        return None
    return NodeSpec(
        name=name,
        allocatable=allocatable,
        labels=labels,
        taints=taints,
        failure_domain=failure_domain,
        healthy=healthy,
        raw_allocatable=raw_allocatable,
    )


def _parse_tolerations(value: Any, path: str, validation: _Validation) -> tuple[Toleration, ...]:
    items = _read_list(value, path, validation)
    result: list[Toleration] = []
    for index, item in enumerate(items):
        item_path = f"{path}[{index}]"
        obj = _read_object(item, item_path, validation)
        key = obj.get("key", "")
        if not isinstance(key, str):
            validation.fail(f"{item_path}.key", "expected a string", key)
            key = ""
        operator = obj.get("operator", "Equal")
        if operator not in {"Equal", "Exists"}:
            validation.fail(f"{item_path}.operator", "operator must be Equal or Exists", operator)
        value_text = obj.get("value", "")
        if not isinstance(value_text, str):
            validation.fail(f"{item_path}.value", "expected a string", value_text)
            value_text = ""
        effect = obj.get("effect")
        if effect is not None and effect not in TAINT_EFFECTS:
            validation.fail(f"{item_path}.effect", "invalid toleration effect", effect)
        if operator == "Exists" and value_text:
            validation.fail(f"{item_path}.value", "value is not valid when operator is Exists", value_text)
        if operator == "Equal" and not key:
            validation.fail(f"{item_path}.key", "key must not be empty for Equal tolerations", key)
        result.append(Toleration(key, operator, value_text, effect, canonical_json(item)))
    return tuple(result)


def _parse_expression(
    item: Any,
    path: str,
    validation: _Validation,
    *,
    allowed_operators: set[str],
) -> LabelRequirement | None:
    obj = _read_object(item, path, validation)
    key = _read_string(obj.get("key"), f"{path}.key", validation)
    operator = _read_string(obj.get("operator"), f"{path}.operator", validation)
    if operator and operator not in allowed_operators:
        validation.fail(
            f"{path}.operator",
            f"unsupported operator {operator!r}; expected one of {sorted(allowed_operators)}",
            operator,
        )
    values_raw = obj.get("values", [])
    values_list = _read_list(values_raw, f"{path}.values", validation)
    values: list[str] = []
    for value_index, value in enumerate(values_list):
        if not isinstance(value, str):
            validation.fail(f"{path}.values[{value_index}]", "expected a string", value)
        else:
            values.append(value)
    if operator in {"In", "NotIn"} and not values:
        validation.fail(f"{path}.values", f"operator {operator} requires at least one value", values_raw)
    if operator in {"Exists", "DoesNotExist"} and values:
        validation.fail(f"{path}.values", f"operator {operator} must not include values", values_raw)
    if operator in {"Gt", "Lt"}:
        if len(values) != 1:
            validation.fail(f"{path}.values", f"operator {operator} requires exactly one integer value", values_raw)
        elif not _is_integer_text(values[0]):
            validation.fail(f"{path}.values[0]", f"operator {operator} requires an integer value", values[0])
    return LabelRequirement(key, operator, tuple(values), canonical_json(obj))


def _is_integer_text(value: str) -> bool:
    try:
        int(value, 10)
    except (TypeError, ValueError):
        return False
    return True


def _parse_label_selector(
    value: Any,
    path: str,
    validation: _Validation,
    *,
    allowed_operators: set[str] = POD_SELECTOR_OPERATORS,
) -> LabelSelector:
    obj = _read_object(value, path, validation)
    match_labels = _read_string_map(obj.get("matchLabels", obj.get("match_labels", {})), f"{path}.matchLabels", validation)
    expressions_raw = obj.get("matchExpressions", obj.get("match_expressions", []))
    expressions_list = _read_list(expressions_raw, f"{path}.matchExpressions", validation)
    expressions: list[LabelRequirement] = []
    for index, item in enumerate(expressions_list):
        expression = _parse_expression(
            item,
            f"{path}.matchExpressions[{index}]",
            validation,
            allowed_operators=allowed_operators,
        )
        if expression is not None:
            expressions.append(expression)
    return LabelSelector(match_labels, tuple(expressions), canonical_json(obj))


def _parse_node_term(
    value: Any,
    path: str,
    validation: _Validation,
    *,
    check_node_keys: bool,
    declared_label_keys: set[str],
) -> NodeSelectorTerm:
    obj = _read_object(value, path, validation)
    expressions_raw = obj.get("matchExpressions", obj.get("match_expressions", []))
    expressions_list = _read_list(expressions_raw, f"{path}.matchExpressions", validation)
    expressions: list[LabelRequirement] = []
    for index, item in enumerate(expressions_list):
        expression_path = f"{path}.matchExpressions[{index}]"
        expression = _parse_expression(
            item,
            expression_path,
            validation,
            allowed_operators=NODE_AFFINITY_OPERATORS,
        )
        if expression is None:
            continue
        if check_node_keys and expression.key and expression.key not in declared_label_keys:
            validation.fail(
                f"{expression_path}.key",
                "node label key is not declared by any node",
                expression.key,
            )
        expressions.append(expression)
    if "matchFields" in obj or "match_fields" in obj:
        validation.fail(f"{path}.matchFields", "matchFields is not supported by this simulator", obj.get("matchFields", obj.get("match_fields")))
    return NodeSelectorTerm(tuple(expressions), canonical_json(obj))


def _parse_node_affinity(
    value: Any,
    path: str,
    validation: _Validation,
    declared_label_keys: set[str],
) -> NodeAffinityRules:
    if value is None:
        return NodeAffinityRules()
    obj = _read_object(value, path, validation)
    required = _field(
        obj,
        "requiredDuringSchedulingIgnoredDuringExecution",
        "required_during_scheduling_ignored_during_execution",
        default=None,
    )
    required_terms: list[NodeSelectorTerm] = []
    if required is not None:
        required_obj = _read_object(required, f"{path}.requiredDuringSchedulingIgnoredDuringExecution", validation)
        terms_raw = _field(required_obj, "nodeSelectorTerms", "node_selector_terms", default=[])
        terms_list = _read_list(terms_raw, f"{path}.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms", validation)
        for index, term in enumerate(terms_list):
            required_terms.append(
                _parse_node_term(
                    term,
                    f"{path}.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[{index}]",
                    validation,
                    check_node_keys=True,
                    declared_label_keys=declared_label_keys,
                )
            )

    preferred_raw = _field(
        obj,
        "preferredDuringSchedulingIgnoredDuringExecution",
        "preferred_during_scheduling_ignored_during_execution",
        default=[],
    )
    preferred_list = _read_list(preferred_raw, f"{path}.preferredDuringSchedulingIgnoredDuringExecution", validation)
    preferred_terms: list[WeightedNodeTerm] = []
    for index, item in enumerate(preferred_list):
        item_path = f"{path}.preferredDuringSchedulingIgnoredDuringExecution[{index}]"
        item_obj = _read_object(item, item_path, validation)
        weight = _read_int(item_obj.get("weight"), f"{item_path}.weight", validation, minimum=0)
        preference = _field(item_obj, "preference", "nodeSelectorTerm", "node_selector_term", default={})
        term = _parse_node_term(
            preference,
            f"{item_path}.preference",
            validation,
            check_node_keys=True,
            declared_label_keys=declared_label_keys,
        )
        preferred_terms.append(WeightedNodeTerm(weight, term))
    return NodeAffinityRules(tuple(required_terms), tuple(preferred_terms))


def _parse_pod_affinity_rules(
    value: Any,
    path: str,
    validation: _Validation,
    *,
    is_anti: bool,
) -> PodAffinityRules:
    if value is None:
        return PodAffinityRules()
    obj = _read_object(value, path, validation)
    required_raw = _field(
        obj,
        "requiredDuringSchedulingIgnoredDuringExecution",
        "required_during_scheduling_ignored_during_execution",
        default=[],
    )
    required_list = _read_list(required_raw, f"{path}.requiredDuringSchedulingIgnoredDuringExecution", validation)
    required_terms: list[PodAffinityTerm] = []
    for index, item in enumerate(required_list):
        term_path = f"{path}.requiredDuringSchedulingIgnoredDuringExecution[{index}]"
        term = _parse_pod_affinity_term(item, term_path, validation)
        if term is not None:
            required_terms.append(term)

    preferred_raw = _field(
        obj,
        "preferredDuringSchedulingIgnoredDuringExecution",
        "preferred_during_scheduling_ignored_during_execution",
        default=[],
    )
    preferred_list = _read_list(preferred_raw, f"{path}.preferredDuringSchedulingIgnoredDuringExecution", validation)
    preferred_terms: list[WeightedPodTerm] = []
    for index, item in enumerate(preferred_list):
        item_path = f"{path}.preferredDuringSchedulingIgnoredDuringExecution[{index}]"
        item_obj = _read_object(item, item_path, validation)
        weight = _read_int(item_obj.get("weight"), f"{item_path}.weight", validation, minimum=0)
        term_raw = _field(item_obj, "podAffinityTerm", "pod_affinity_term", "term", default=item_obj)
        term = _parse_pod_affinity_term(term_raw, f"{item_path}.podAffinityTerm", validation)
        if term is not None:
            preferred_terms.append(WeightedPodTerm(weight, term))
    return PodAffinityRules(tuple(required_terms), tuple(preferred_terms))


def _parse_pod_affinity_term(
    value: Any, path: str, validation: _Validation
) -> PodAffinityTerm | None:
    obj = _read_object(value, path, validation)
    selector_raw = _field(obj, "labelSelector", "label_selector", default={})
    selector = _parse_label_selector(selector_raw, f"{path}.labelSelector", validation)
    topology_key = _read_string(
        _field(obj, "topologyKey", "topology_key"),
        f"{path}.topologyKey",
        validation,
    )
    namespaces_raw = obj.get("namespaces", [])
    namespaces_list = _read_list(namespaces_raw, f"{path}.namespaces", validation)
    namespaces: list[str] = []
    for index, namespace in enumerate(namespaces_list):
        if not isinstance(namespace, str) or not namespace:
            validation.fail(f"{path}.namespaces[{index}]", "expected a non-empty string", namespace)
        else:
            namespaces.append(namespace)
    return PodAffinityTerm(selector, topology_key, tuple(namespaces), canonical_json(obj))


def _parse_pod(
    item: Any,
    index: int,
    validation: _Validation,
    declared_label_keys: set[str],
    topology_references: list[tuple[str, str]],
) -> PodSpec | None:
    path = f"pods[{index}]"
    obj = _read_object(item, path, validation)
    name = _read_string(obj.get("name"), f"{path}.name", validation)
    namespace = obj.get("namespace", "default")
    if not isinstance(namespace, str) or not namespace:
        validation.fail(f"{path}.namespace", "expected a non-empty string", namespace)
        namespace = "default"
    priority = _read_int(obj.get("priority", 0), f"{path}.priority", validation)
    arrival_seq = _read_int(
        _field(obj, "arrival_seq", "arrival_sequence", "arrival_order", default=index),
        f"{path}.arrival_seq",
        validation,
        minimum=0,
    )
    labels = _read_string_map(obj.get("labels", {}), f"{path}.labels", validation)

    requests_obj = _read_object(obj.get("requests", {}), f"{path}.requests", validation)
    requests, raw_requests = _read_resource_vector(
        requests_obj, f"{path}.requests", validation, require_fields=True
    )
    limits: ResourceVector | None = None
    if "limits" in obj and obj["limits"] is not None:
        limits_obj = _read_object(obj["limits"], f"{path}.limits", validation)
        limit_cpu_value = limits_obj.get("cpu", raw_requests.get("cpu"))
        limit_memory_value = limits_obj.get("memory", raw_requests.get("memory"))
        limit_cpu = 0
        limit_memory = 0
        if limit_cpu_value is not None:
            try:
                limit_cpu = parse_cpu_millicores(limit_cpu_value)
            except QuantityError as exc:
                validation.fail(f"{path}.limits.cpu", str(exc), limit_cpu_value)
        if limit_memory_value is not None:
            try:
                limit_memory = parse_memory_bytes(limit_memory_value)
            except QuantityError as exc:
                validation.fail(f"{path}.limits.memory", str(exc), limit_memory_value)
        if limit_cpu < requests.cpu_millicores and "cpu" in limits_obj:
            validation.fail(f"{path}.limits.cpu", "limit must not be smaller than request", limit_cpu_value)
        if limit_memory < requests.memory_bytes and "memory" in limits_obj:
            validation.fail(f"{path}.limits.memory", "limit must not be smaller than request", limit_memory_value)
        limits = ResourceVector(limit_cpu, limit_memory)

    node_selector_raw = _field(obj, "nodeSelector", "node_selector", default={})
    node_selector = _read_string_map(node_selector_raw, f"{path}.nodeSelector", validation)
    for key in node_selector:
        if key not in declared_label_keys:
            validation.fail(
                f"{path}.nodeSelector.{key}",
                "node label key is not declared by any node",
                key,
            )

    affinity_raw = obj.get("affinity", {})
    affinity_obj = _read_object(affinity_raw, f"{path}.affinity", validation)
    node_affinity = _parse_node_affinity(
        _field(
            affinity_obj,
            "nodeAffinity",
            "node_affinity",
            default=_field(obj, "nodeAffinity", "node_affinity", default={}),
        ),
        f"{path}.affinity.nodeAffinity",
        validation,
        declared_label_keys,
    )
    pod_affinity = _parse_pod_affinity_rules(
        _field(
            affinity_obj,
            "podAffinity",
            "pod_affinity",
            default=_field(obj, "podAffinity", "pod_affinity", default={}),
        ),
        f"{path}.affinity.podAffinity",
        validation,
        is_anti=False,
    )
    pod_anti_affinity = _parse_pod_affinity_rules(
        _field(
            affinity_obj,
            "podAntiAffinity",
            "pod_anti_affinity",
            default=_field(obj, "podAntiAffinity", "pod_anti_affinity", default={}),
        ),
        f"{path}.affinity.podAntiAffinity",
        validation,
        is_anti=True,
    )
    for rules_path, rules in (
        (f"{path}.affinity.podAffinity", pod_affinity),
        (f"{path}.affinity.podAntiAffinity", pod_anti_affinity),
    ):
        for term_index, term in enumerate(rules.required_terms):
            topology_references.append(
                (
                    f"{rules_path}.requiredDuringSchedulingIgnoredDuringExecution[{term_index}].topologyKey",
                    term.topology_key,
                )
            )
        for term_index, weighted in enumerate(rules.preferred_terms):
            topology_references.append(
                (
                    f"{rules_path}.preferredDuringSchedulingIgnoredDuringExecution[{term_index}].podAffinityTerm.topologyKey",
                    weighted.term.topology_key,
                )
            )

    tolerations = _parse_tolerations(obj.get("tolerations", []), f"{path}.tolerations", validation)
    if not name:
        return None
    return PodSpec(
        name=name,
        namespace=namespace,
        priority=priority,
        arrival_seq=arrival_seq,
        labels=labels,
        requests=requests,
        limits=limits,
        raw_requests=raw_requests,
        node_selector=node_selector,
        node_affinity=node_affinity,
        pod_affinity=pod_affinity,
        pod_anti_affinity=pod_anti_affinity,
        tolerations=tolerations,
    )


def parse_batch(payload: Any, *, batch_id_override: str | None = None) -> BatchSpec:
    """Validate a complete batch. Validation never mutates persistence state."""

    validation = _Validation()
    if not isinstance(payload, dict):
        validation.fail("$", "batch request body must be an object", payload)
        validation.raise_if_any()
        raise AssertionError("unreachable")

    supplied_batch_id = batch_id_override if batch_id_override is not None else payload.get("batch_id")
    if supplied_batch_id is None:
        batch_id = ""
        validation.fail("batch_id", "batch_id is required", None)
    else:
        batch_id = _read_string(supplied_batch_id, "batch_id", validation)

    nodes_raw = payload.get("nodes")
    pods_raw = payload.get("pods")
    nodes_list = _read_list(nodes_raw, "nodes", validation)
    pods_list = _read_list(pods_raw, "pods", validation)
    if not nodes_list:
        validation.fail("nodes", "at least one node is required", nodes_raw)
    if not pods_list:
        validation.fail("pods", "at least one Pod is required", pods_raw)

    budget = _parse_budget(payload.get("budget"), validation)

    declared_label_keys: set[str] = set()
    for index, node_item in enumerate(nodes_list):
        if isinstance(node_item, dict) and isinstance(node_item.get("labels"), dict):
            for key in node_item["labels"]:
                if isinstance(key, str) and key:
                    declared_label_keys.add(key)

    nodes: list[NodeSpec] = []
    seen_nodes: set[str] = set()
    for index, item in enumerate(nodes_list):
        node = _parse_node(item, index, validation)
        if node is None:
            continue
        if node.name in seen_nodes:
            validation.fail(f"nodes[{index}].name", "duplicate node name", node.name)
        seen_nodes.add(node.name)
        nodes.append(node)

    topology_references: list[tuple[str, str]] = []
    pods: list[PodSpec] = []
    seen_pods: set[str] = set()
    for index, item in enumerate(pods_list):
        pod = _parse_pod(item, index, validation, declared_label_keys, topology_references)
        if pod is None:
            continue
        if pod.name in seen_pods:
            validation.fail(f"pods[{index}].name", "duplicate Pod name", pod.name)
        seen_pods.add(pod.name)
        pods.append(pod)

    for path, topology_key in topology_references:
        if not topology_key:
            validation.topology(path, "topologyKey must not be empty", topology_key)
        elif topology_key not in declared_label_keys:
            validation.topology(
                path,
                "topologyKey is not declared by any node label; refusing to guess a default",
                topology_key,
            )

    validation.raise_if_any()
    return BatchSpec(batch_id=batch_id, nodes=tuple(nodes), pods=tuple(pods), budget=budget)

