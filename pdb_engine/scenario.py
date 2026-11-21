from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

from .errors import BusinessError, spec_invalid
from .models import Node, Pod, PDB, Scenario, Taint, Toleration, Workload
from .quantities import MAX_INT64, checked_add, parse_cpu_millicores, parse_memory_bytes
from .selectors import pod_matches_pdb, selector_matches


def _as_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise spec_invalid(f"{field} must be an object", field=field)
    return value


def _as_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise spec_invalid(f"{field} must be an array", field=field)
    return value


def _as_text(value: Any, field: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise spec_invalid(f"{field} must be a non-empty string", field=field, value=value)
    return value


def _as_labels(value: Any, field: str) -> dict[str, str]:
    if value is None:
        return {}
    mapping = _as_mapping(value, field)
    return {str(key): str(item) for key, item in mapping.items()}


def _as_bool(value: Any, field: str, default: bool = False) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise spec_invalid(f"{field} must be a boolean", field=field, value=value)
    return value


def _parse_taint(value: Any, index: int) -> Taint:
    item = _as_mapping(value, f"taints[{index}]")
    effect = _as_text(item.get("effect", "NoSchedule"), f"taints[{index}].effect")
    if effect not in {"NoSchedule", "PreferNoSchedule", "NoExecute"}:
        raise spec_invalid("unsupported taint effect", effect=effect)
    return Taint(
        key=_as_text(item.get("key"), f"taints[{index}].key"),
        value=str(item.get("value", "")),
        effect=effect,
    )


def _parse_toleration(value: Any, index: int) -> Toleration:
    item = _as_mapping(value, f"tolerations[{index}]")
    operator = str(item.get("operator", "Equal"))
    if operator not in {"Equal", "Exists"}:
        raise spec_invalid("unsupported toleration operator", operator=operator)
    effect = str(item.get("effect", ""))
    if effect and effect not in {"NoSchedule", "PreferNoSchedule", "NoExecute"}:
        raise spec_invalid("unsupported toleration effect", effect=effect)
    return Toleration(
        key=_as_text(item.get("key"), f"tolerations[{index}].key", allow_empty=operator == "Exists"),
        operator=operator,
        value=str(item.get("value", "")),
        effect=effect,
    )


def _parse_budget(raw: dict[str, Any]) -> tuple[str, Any, bool, Decimal | None]:
    present = [key for key in ("minAvailable", "maxUnavailable") if key in raw]
    if len(present) != 1:
        raise spec_invalid("PDB must define exactly one of minAvailable or maxUnavailable")
    kind = present[0]
    value = raw[kind]
    if isinstance(value, bool):
        raise spec_invalid("PDB budget must not be boolean", field=kind, value=value)
    if isinstance(value, int):
        if value < 0 or value > MAX_INT64:
            raise spec_invalid("PDB integer budget is out of range", field=kind, value=value)
        return kind, value, False, None
    if not isinstance(value, str) or not value.endswith("%"):
        raise spec_invalid("PDB budget must be an integer or percentage string", field=kind, value=value)
    text = value[:-1].strip()
    try:
        percent = Decimal(text)
    except InvalidOperation as exc:
        raise spec_invalid("invalid PDB percentage", field=kind, value=value) from exc
    if not percent.is_finite() or percent < 0 or percent > 100:
        raise spec_invalid("PDB percentage must be between 0% and 100%", field=kind, value=value)
    return kind, value, True, percent


def _require_workloads(
    workloads: dict[str, Workload], controller_type: str, controller_name: str | None
) -> list[Workload]:
    selected = [
        workload
        for workload in workloads.values()
        if workload.controller_type == controller_type
        and (controller_name is None or workload.name == controller_name)
    ]
    if not selected:
        raise spec_invalid(
            "PDB applicable controller does not exist",
            controller_type=controller_type,
            controller_name=controller_name,
        )
    return selected


def _parse_pdb(raw: dict[str, Any], index: int, scenario_parts: dict[str, Any]) -> PDB:
    field = f"pdbs[{index}]"
    name = _as_text(raw.get("name"), f"{field}.name")
    selector = raw.get("selector", {})
    if not isinstance(selector, dict):
        raise spec_invalid("PDB selector must be an object", pdb=name)

    controller = raw.get("controller")
    if isinstance(controller, str):
        controller_type = controller
        controller_name = raw.get("controller_name")
    elif isinstance(controller, dict):
        controller_type = controller.get("type")
        controller_name = controller.get("name")
    else:
        controller_type = raw.get("controller_type")
        controller_name = raw.get("controller_name")
    controller_type = _as_text(controller_type, f"{field}.controller.type")
    if controller_name is not None:
        controller_name = _as_text(controller_name, f"{field}.controller.name")

    workloads: dict[str, Workload] = scenario_parts["workloads"]
    pods: dict[str, Pod] = scenario_parts["pods"]
    applicable_workloads = _require_workloads(workloads, controller_type, controller_name)
    desired_replicas = 0
    for workload in applicable_workloads:
        desired_replicas = checked_add(
            desired_replicas, workload.desired_replicas, f"{field}.desired_replicas"
        )

    kind, budget_input, is_percentage, percent = _parse_budget(raw)
    if is_percentage:
        assert percent is not None
        scaled = Decimal(desired_replicas) * percent / Decimal(100)
        if kind == "minAvailable":
            calculated = int(scaled.to_integral_value(rounding=ROUND_CEILING))
            required_available = calculated
            max_unavailable = None
            rounding = "ceil"
        else:
            calculated = int(scaled.to_integral_value(rounding=ROUND_FLOOR))
            max_unavailable = calculated
            required_available = max(0, desired_replicas - calculated)
            rounding = "floor"
    elif kind == "minAvailable":
        calculated = int(budget_input)
        required_available = calculated
        max_unavailable = None
        rounding = "exact"
    else:
        calculated = int(budget_input)
        max_unavailable = calculated
        required_available = max(0, desired_replicas - calculated)
        rounding = "exact"

    provisional = PDB(
        name=name,
        selector=selector,
        controller_type=controller_type,
        controller_name=controller_name,
        budget_kind=kind,
        budget_input=budget_input,
        is_percentage=is_percentage,
        desired_replicas=desired_replicas,
        rounding=rounding,
        calculated_integer=calculated,
        max_unavailable=max_unavailable,
        required_available=required_available,
        initial_available=0,
        matched_pods=(),
    )
    matched = [
        pod for pod in sorted(pods.values(), key=lambda candidate: candidate.ordinal)
        if pod_matches_pdb(pod, provisional)
    ]
    if not matched:
        raise BusinessError(
            "E_SELECTOR_EMPTY",
            "PDB selector and applicable controller match no pods",
            {
                "pdb": name,
                "selector": selector,
                "controller": {"type": controller_type, "name": controller_name},
            },
        )
    initial_available = sum(1 for pod in matched if pod.healthy)
    return PDB(
        name=name,
        selector=selector,
        controller_type=controller_type,
        controller_name=controller_name,
        budget_kind=kind,
        budget_input=budget_input,
        is_percentage=is_percentage,
        desired_replicas=desired_replicas,
        rounding=rounding,
        calculated_integer=calculated,
        max_unavailable=max_unavailable,
        required_available=required_available,
        initial_available=initial_available,
        matched_pods=tuple(pod.name for pod in matched),
    )


def parse_scenario_data(
    data: Any, source_path: str | None = None, default_name: str = "local-sample"
) -> Scenario:
    root = _as_mapping(data, "scenario")
    name = str(root.get("name", default_name))

    nodes: dict[str, Node] = {}
    for index, value in enumerate(_as_list(root.get("nodes"), "nodes")):
        item = _as_mapping(value, f"nodes[{index}]")
        node_name = _as_text(item.get("name"), f"nodes[{index}].name")
        if node_name in nodes:
            raise spec_invalid("duplicate node name", node=node_name)
        allocatable = _as_mapping(item.get("allocatable", {}), f"nodes[{index}].allocatable")
        cpu_milli_alias = allocatable.get("cpu_milli", item.get("cpu_milli"))
        memory_bytes_alias = allocatable.get("memory_bytes", item.get("memory_bytes"))
        cpu_source = allocatable.get("cpu", item.get("cpu"))
        memory_source = allocatable.get("memory", item.get("memory"))
        if cpu_source is None and cpu_milli_alias is not None:
            if (
                isinstance(cpu_milli_alias, bool)
                or not isinstance(cpu_milli_alias, int)
                or cpu_milli_alias < 0
                or cpu_milli_alias > MAX_INT64
            ):
                raise spec_invalid(
                    "cpu_milli must be a non-negative signed 64-bit integer",
                    node=node_name,
                )
            cpu_milli = cpu_milli_alias
        else:
            cpu_milli = parse_cpu_millicores(
                cpu_source, f"nodes[{index}].allocatable.cpu"
            ) if cpu_source is not None else None
        if memory_source is None and memory_bytes_alias is not None:
            if (
                isinstance(memory_bytes_alias, bool)
                or not isinstance(memory_bytes_alias, int)
                or memory_bytes_alias < 0
                or memory_bytes_alias > MAX_INT64
            ):
                raise spec_invalid(
                    "memory_bytes must be a non-negative signed 64-bit integer",
                    node=node_name,
                )
            memory_bytes = memory_bytes_alias
        else:
            memory_bytes = parse_memory_bytes(
                memory_source, f"nodes[{index}].allocatable.memory"
            ) if memory_source is not None else None
        if cpu_milli is None or memory_bytes is None:
            raise spec_invalid("node allocatable CPU and memory are required", node=node_name)
        taints = tuple(
            _parse_taint(taint, taint_index)
            for taint_index, taint in enumerate(_as_list(item.get("taints", []), "taints"))
        )
        nodes[node_name] = Node(
            name=node_name,
            cpu_milli=cpu_milli,
            memory_bytes=memory_bytes,
            drain_requested=_as_bool(item.get("drain_requested"), "drain_requested"),
            labels=_as_labels(item.get("labels"), f"nodes[{index}].labels"),
            taints=taints,
        )

    workloads: dict[str, Workload] = {}
    pods: dict[str, Pod] = {}
    ordered_pods: list[Pod] = []
    ordinal = 0
    for w_index, value in enumerate(_as_list(root.get("workloads"), "workloads")):
        item = _as_mapping(value, f"workloads[{w_index}]")
        workload_name = _as_text(item.get("name"), f"workloads[{w_index}].name")
        if workload_name in workloads:
            raise spec_invalid("duplicate workload name", workload=workload_name)
        controller_type = _as_text(
            item.get("controller_type"), f"workloads[{w_index}].controller_type"
        )
        desired = item.get("desired_replicas")
        if isinstance(desired, bool) or not isinstance(desired, int) or desired < 0 or desired > MAX_INT64:
            raise spec_invalid(
                "desired_replicas must be a non-negative integer",
                workload=workload_name,
            )
        pod_names: list[str] = []
        for p_index, pod_value in enumerate(
            _as_list(item.get("pods", []), f"workloads[{w_index}].pods")
        ):
            pod_item = _as_mapping(pod_value, f"workloads[{w_index}].pods[{p_index}]")
            pod_name = _as_text(
                pod_item.get("name"), f"workloads[{w_index}].pods[{p_index}].name"
            )
            if pod_name in pods:
                raise spec_invalid("duplicate pod name", pod=pod_name)
            node_name = _as_text(
                pod_item.get("node"), f"workloads[{w_index}].pods[{p_index}].node"
            )
            if node_name not in nodes:
                raise spec_invalid("pod references an unknown node", pod=pod_name, node=node_name)
            requests = _as_mapping(
                pod_item.get("requests", {}), f"workloads[{w_index}].pods[{p_index}].requests"
            )
            cpu = parse_cpu_millicores(requests.get("cpu", 0), f"{pod_name}.requests.cpu")
            memory = parse_memory_bytes(requests.get("memory", 0), f"{pod_name}.requests.memory")
            tolerations = tuple(
                _parse_toleration(toleration, tolerance_index)
                for tolerance_index, toleration in enumerate(
                    _as_list(pod_item.get("tolerations", []), "tolerations")
                )
            )
            phase = str(pod_item.get("phase", pod_item.get("status", "Running")))
            eviction_state = str(pod_item.get("eviction_state", "active"))
            pod = Pod(
                name=pod_name,
                workload_name=workload_name,
                controller_type=controller_type,
                node=node_name,
                phase=phase,
                eviction_state=eviction_state,
                terminating=_as_bool(pod_item.get("terminating"), "terminating"),
                labels=_as_labels(pod_item.get("labels"), f"{pod_name}.labels"),
                cpu_milli=cpu,
                memory_bytes=memory,
                node_selector=_as_labels(pod_item.get("node_selector"), f"{pod_name}.node_selector"),
                tolerations=tolerations,
                ordinal=ordinal,
            )
            pods[pod_name] = pod
            ordered_pods.append(pod)
            pod_names.append(pod_name)
            ordinal += 1
        workloads[workload_name] = Workload(
            name=workload_name,
            controller_type=controller_type,
            desired_replicas=desired,
            pod_names=tuple(pod_names),
        )

    scenario_parts = {"workloads": workloads, "pods": pods}
    pdbs: dict[str, PDB] = {}
    for index, value in enumerate(_as_list(root.get("pdbs"), "pdbs")):
        pdb = _parse_pdb(_as_mapping(value, f"pdbs[{index}]"), index, scenario_parts)
        if pdb.name in pdbs:
            raise spec_invalid("duplicate PDB name", pdb=pdb.name)
        pdbs[pdb.name] = pdb

    return Scenario(
        name=name,
        nodes=nodes,
        workloads=workloads,
        pods=pods,
        ordered_pods=tuple(ordered_pods),
        pdbs=pdbs,
        source_path=source_path,
    )


def parse_scenario_text(text: str, source_path: str | None = None) -> Scenario:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise spec_invalid(
            "scenario JSON is malformed", line=exc.lineno, column=exc.colno, error=exc.msg
        ) from exc
    return parse_scenario_data(data, source_path=source_path)


def load_scenario_file(path: str | Path) -> tuple[Scenario, str]:
    source = Path(path)
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise spec_invalid("unable to read sample file", path=str(source), error=str(exc)) from exc
    return parse_scenario_text(text, source_path=str(source.resolve())), text

