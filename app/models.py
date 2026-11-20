from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from app.errors import BusinessError
from app.quantity import parse_cpu_millicores, parse_memory_bytes


def to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(piece[:1].upper() + piece[1:] for piece in tail)


class InputModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )


Scalar = StrictInt | StrictStr
TaintEffect = Literal["NoSchedule", "PreferNoSchedule", "NoExecute"]
NodeSelectorOperator = Literal["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"]
LabelSelectorOperator = Literal["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"]
TolerationOperator = Literal["Equal", "Exists"]


class QuantityInput(InputModel):
    cpu: Scalar
    memory: Scalar


class PodResourcesInput(InputModel):
    requests: QuantityInput
    limits: QuantityInput | None = None


class TaintInput(InputModel):
    key: StrictStr = Field(min_length=1)
    value: StrictStr | None = None
    effect: TaintEffect
    time_added_seconds: StrictInt | None = Field(default=None, ge=0)


class TolerationInput(InputModel):
    key: StrictStr = ""
    operator: TolerationOperator = "Equal"
    value: StrictStr = ""
    effect: TaintEffect | None = None
    toleration_seconds: StrictInt | None = Field(default=None, ge=0)


class NodeSelectorRequirementInput(InputModel):
    key: StrictStr = Field(min_length=1)
    operator: NodeSelectorOperator
    values: list[StrictStr] = Field(default_factory=list)


class LabelSelectorRequirementInput(InputModel):
    key: StrictStr = Field(min_length=1)
    operator: LabelSelectorOperator
    values: list[StrictStr] = Field(default_factory=list)


class NodeSelectorTermInput(InputModel):
    match_expressions: list[NodeSelectorRequirementInput] = Field(default_factory=list)


class NodeSelectorInput(InputModel):
    node_selector_terms: list[NodeSelectorTermInput] = Field(default_factory=list)


class PreferredNodeTermInput(InputModel):
    weight: StrictInt = Field(ge=1, le=100)
    preference: NodeSelectorTermInput


class NodeAffinityInput(InputModel):
    required_during_scheduling_ignored_during_execution: NodeSelectorInput | None = None
    preferred_during_scheduling_ignored_during_execution: list[PreferredNodeTermInput] = Field(default_factory=list)


class LabelSelectorInput(InputModel):
    match_labels: dict[str, StrictStr] = Field(default_factory=dict)
    match_expressions: list[LabelSelectorRequirementInput] = Field(default_factory=list)


class PodAffinityTermInput(InputModel):
    label_selector: LabelSelectorInput = Field(default_factory=LabelSelectorInput)
    topology_key: StrictStr = Field(min_length=1)


class WeightedPodAffinityTermInput(InputModel):
    weight: StrictInt = Field(ge=1, le=100)
    pod_affinity_term: PodAffinityTermInput


class PodAffinityInput(InputModel):
    required_during_scheduling_ignored_during_execution: list[PodAffinityTermInput] = Field(default_factory=list)
    preferred_during_scheduling_ignored_during_execution: list[WeightedPodAffinityTermInput] = Field(default_factory=list)


class AffinityInput(InputModel):
    node_affinity: NodeAffinityInput | None = None
    pod_affinity: PodAffinityInput | None = None
    pod_anti_affinity: PodAffinityInput | None = None


class AssignedPodInput(InputModel):
    name: StrictStr = Field(min_length=1)
    priority: StrictInt = 0
    labels: dict[str, StrictStr] = Field(default_factory=dict)
    resources: PodResourcesInput
    preemptable: bool = True


class PodInput(InputModel):
    name: StrictStr = Field(min_length=1)
    priority: StrictInt = 0
    labels: dict[str, StrictStr] = Field(default_factory=dict)
    resources: PodResourcesInput
    node_selector: dict[str, StrictStr] = Field(default_factory=dict)
    affinity: AffinityInput = Field(default_factory=AffinityInput)
    tolerations: list[TolerationInput] = Field(default_factory=list)


class NodeInput(InputModel):
    name: StrictStr = Field(min_length=1)
    allocatable: QuantityInput
    labels: dict[str, StrictStr] = Field(default_factory=dict)
    taints: list[TaintInput] = Field(default_factory=list)
    assigned_pods: list[AssignedPodInput] = Field(default_factory=list)


class SchedulerConfigInput(InputModel):
    preemption_enabled: bool = False


class BatchSpecInput(InputModel):
    name: StrictStr = "batch"
    now_seconds: StrictInt = Field(default=0, ge=0)
    config: SchedulerConfigInput = Field(default_factory=SchedulerConfigInput)
    nodes: list[NodeInput]
    pods: list[PodInput]


@dataclass(frozen=True)
class ResourceAmount:
    cpu_millicores: int
    memory_bytes: int


@dataclass(frozen=True)
class SelectorRequirement:
    key: str
    operator: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class LabelSelector:
    match_labels: tuple[tuple[str, str], ...] = ()
    match_expressions: tuple[SelectorRequirement, ...] = ()


@dataclass(frozen=True)
class NodeAffinity:
    required_terms: tuple[tuple[SelectorRequirement, ...], ...] = ()
    has_required_terms: bool = False
    preferred_terms: tuple[tuple[int, tuple[SelectorRequirement, ...]], ...] = ()


@dataclass(frozen=True)
class PodAffinityTerm:
    selector: LabelSelector
    topology_key: str


@dataclass(frozen=True)
class WeightedPodAffinityTerm:
    weight: int
    term: PodAffinityTerm


@dataclass(frozen=True)
class PodAffinity:
    required_terms: tuple[PodAffinityTerm, ...] = ()
    preferred_terms: tuple[WeightedPodAffinityTerm, ...] = ()


@dataclass(frozen=True)
class Affinity:
    node: NodeAffinity = field(default_factory=NodeAffinity)
    pod: PodAffinity = field(default_factory=PodAffinity)
    pod_anti: PodAffinity = field(default_factory=PodAffinity)


@dataclass(frozen=True)
class Taint:
    key: str
    value: str
    effect: str
    time_added_seconds: int | None


@dataclass(frozen=True)
class Toleration:
    key: str
    operator: str
    value: str
    effect: str | None
    toleration_seconds: int | None


@dataclass(frozen=True)
class AssignedPodSpec:
    name: str
    priority: int
    labels: dict[str, str]
    requests: ResourceAmount
    preemptable: bool


@dataclass(frozen=True)
class NodeSpec:
    name: str
    allocatable: ResourceAmount
    labels: dict[str, str]
    taints: tuple[Taint, ...]
    assigned_pods: tuple[AssignedPodSpec, ...]


@dataclass(frozen=True)
class PodSpec:
    name: str
    priority: int
    labels: dict[str, str]
    requests: ResourceAmount
    limits: ResourceAmount | None
    node_selector: dict[str, str]
    affinity: Affinity
    tolerations: tuple[Toleration, ...]


@dataclass(frozen=True)
class BatchSpec:
    name: str
    now_seconds: int
    preemption_enabled: bool
    nodes: tuple[NodeSpec, ...]
    pods: tuple[PodSpec, ...]


def _invalid_spec(message: str, *, path: str) -> BusinessError:
    return BusinessError("E_INVALID_SPEC", message, details={"path": path})


def _normalize_requirement(
    requirement: NodeSelectorRequirementInput | LabelSelectorRequirementInput,
    *,
    path: str,
) -> SelectorRequirement:
    values = tuple(requirement.values)
    if requirement.operator in ("In", "NotIn") and not values:
        raise _invalid_spec(f"{requirement.operator} requires at least one value", path=path)
    if requirement.operator in ("Gt", "Lt"):
        if len(values) != 1:
            raise _invalid_spec(f"{requirement.operator} requires exactly one integer value", path=path)
        try:
            int(values[0])
        except ValueError as exc:
            raise _invalid_spec(f"{requirement.operator} value must be an integer", path=path) from exc
    if requirement.operator in ("Exists", "DoesNotExist") and values:
        raise _invalid_spec(f"{requirement.operator} does not accept values", path=path)
    return SelectorRequirement(requirement.key, requirement.operator, values)


def _normalize_node_term(term: NodeSelectorTermInput, *, path: str) -> tuple[SelectorRequirement, ...]:
    if not term.match_expressions:
        raise _invalid_spec("node selector term must contain at least one match expression", path=path)
    return tuple(
        _normalize_requirement(requirement, path=f"{path}.matchExpressions[{index}]")
        for index, requirement in enumerate(term.match_expressions)
    )


def _normalize_label_selector(selector: LabelSelectorInput, *, path: str) -> LabelSelector:
    requirements = tuple(
        _normalize_requirement(requirement, path=f"{path}.matchExpressions[{index}]")
        for index, requirement in enumerate(selector.match_expressions)
    )
    return LabelSelector(tuple(sorted(selector.match_labels.items())), requirements)


def _normalize_affinity(affinity: AffinityInput, *, path: str) -> Affinity:
    node_affinity = NodeAffinity()
    if affinity.node_affinity is not None:
        required = affinity.node_affinity.required_during_scheduling_ignored_during_execution
        required_terms: tuple[tuple[SelectorRequirement, ...], ...] = ()
        has_required = required is not None
        if required is not None:
            if not required.node_selector_terms:
                raise _invalid_spec("required node affinity must contain at least one nodeSelectorTerm", path=f"{path}.nodeAffinity")
            required_terms = tuple(
                _normalize_node_term(term, path=f"{path}.nodeAffinity.nodeSelectorTerms[{index}]")
                for index, term in enumerate(required.node_selector_terms)
            )
        preferred_terms = tuple(
            (
                preferred.weight,
                _normalize_node_term(preferred.preference, path=f"{path}.nodeAffinity.preferred[{index}]"),
            )
            for index, preferred in enumerate(
                affinity.node_affinity.preferred_during_scheduling_ignored_during_execution
            )
        )
        node_affinity = NodeAffinity(required_terms, has_required, preferred_terms)

    def normalize_pod_affinity(value: PodAffinityInput | None, prefix: str) -> PodAffinity:
        if value is None:
            return PodAffinity()
        required_terms = tuple(
            PodAffinityTerm(
                _normalize_label_selector(term.label_selector, path=f"{prefix}.required[{index}].labelSelector"),
                term.topology_key,
            )
            for index, term in enumerate(value.required_during_scheduling_ignored_during_execution)
        )
        preferred_terms = tuple(
            WeightedPodAffinityTerm(
                preferred.weight,
                PodAffinityTerm(
                    _normalize_label_selector(
                        preferred.pod_affinity_term.label_selector,
                        path=f"{prefix}.preferred[{index}].podAffinityTerm.labelSelector",
                    ),
                    preferred.pod_affinity_term.topology_key,
                ),
            )
            for index, preferred in enumerate(value.preferred_during_scheduling_ignored_during_execution)
        )
        return PodAffinity(required_terms, preferred_terms)

    return Affinity(
        node=node_affinity,
        pod=normalize_pod_affinity(affinity.pod_affinity, f"{path}.podAffinity"),
        pod_anti=normalize_pod_affinity(affinity.pod_anti_affinity, f"{path}.podAntiAffinity"),
    )


def _normalize_resources(resources: PodResourcesInput, *, path: str) -> tuple[ResourceAmount, ResourceAmount | None]:
    requests = ResourceAmount(
        parse_cpu_millicores(resources.requests.cpu, path=f"{path}.requests.cpu"),
        parse_memory_bytes(resources.requests.memory, path=f"{path}.requests.memory"),
    )
    limits = None
    if resources.limits is not None:
        limits = ResourceAmount(
            parse_cpu_millicores(resources.limits.cpu, path=f"{path}.limits.cpu"),
            parse_memory_bytes(resources.limits.memory, path=f"{path}.limits.memory"),
        )
    return requests, limits


def normalize_spec(spec: BatchSpecInput) -> BatchSpec:
    if not spec.nodes:
        raise _invalid_spec("at least one node is required", path="nodes")

    node_names: set[str] = set()
    nodes: list[NodeSpec] = []
    pod_names: set[str] = set()

    for node_index, node in enumerate(spec.nodes):
        if node.name in node_names:
            raise BusinessError(
                "E_DUPLICATE_NAME",
                f"duplicate node name: {node.name}",
                details={"path": f"nodes[{node_index}].name", "name": node.name},
            )
        node_names.add(node.name)
        allocatable = ResourceAmount(
            parse_cpu_millicores(node.allocatable.cpu, path=f"nodes[{node_index}].allocatable.cpu"),
            parse_memory_bytes(node.allocatable.memory, path=f"nodes[{node_index}].allocatable.memory"),
        )
        assigned: list[AssignedPodSpec] = []
        for assigned_index, assigned_pod in enumerate(node.assigned_pods):
            if assigned_pod.name in pod_names:
                raise BusinessError(
                    "E_DUPLICATE_NAME",
                    f"duplicate pod name: {assigned_pod.name}",
                    details={"path": f"nodes[{node_index}].assignedPods[{assigned_index}].name", "name": assigned_pod.name},
                )
            pod_names.add(assigned_pod.name)
            requests, _ = _normalize_resources(
                assigned_pod.resources,
                path=f"nodes[{node_index}].assignedPods[{assigned_index}].resources",
            )
            assigned.append(
                AssignedPodSpec(
                    name=assigned_pod.name,
                    priority=assigned_pod.priority,
                    labels=dict(assigned_pod.labels),
                    requests=requests,
                    preemptable=assigned_pod.preemptable,
                )
            )
        taints = tuple(
            Taint(taint.key, taint.value or "", taint.effect, taint.time_added_seconds)
            for taint in node.taints
        )
        nodes.append(NodeSpec(node.name, allocatable, dict(node.labels), taints, tuple(assigned)))

    pods: list[PodSpec] = []
    for pod_index, pod in enumerate(spec.pods):
        if pod.name in pod_names:
            raise BusinessError(
                "E_DUPLICATE_NAME",
                f"duplicate pod name: {pod.name}",
                details={"path": f"pods[{pod_index}].name", "name": pod.name},
            )
        pod_names.add(pod.name)
        requests, limits = _normalize_resources(pod.resources, path=f"pods[{pod_index}].resources")
        tolerations = tuple(
            Toleration(
                toleration.key,
                toleration.operator,
                toleration.value,
                toleration.effect,
                toleration.toleration_seconds,
            )
            for toleration in pod.tolerations
        )
        pods.append(
            PodSpec(
                name=pod.name,
                priority=pod.priority,
                labels=dict(pod.labels),
                requests=requests,
                limits=limits,
                node_selector=dict(pod.node_selector),
                affinity=_normalize_affinity(pod.affinity, path=f"pods[{pod_index}].affinity"),
                tolerations=tolerations,
            )
        )

    return BatchSpec(
        name=spec.name,
        now_seconds=spec.now_seconds,
        preemption_enabled=spec.config.preemption_enabled,
        nodes=tuple(nodes),
        pods=tuple(pods),
    )


def spec_to_jsonable(spec: BatchSpecInput) -> dict[str, Any]:
    return spec.model_dump(by_alias=True, mode="json")
