"""Logical-plan node model and deterministic serialization."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Iterator

from .business import SemanticError
from .expressions import Expr, canonical_json, columns_in, expr_key


ALLOWED_NODE_TYPES = {"Scan", "Filter", "Project", "Aggregate", "Sort", "Limit", "Join"}


class IdAllocator:
    def __init__(self, next_number: int = 1, prefix: str = "n") -> None:
        self.next_number = next_number
        self.prefix = prefix

    def new(self) -> str:
        value = f"{self.prefix}{self.next_number:04d}"
        self.next_number += 1
        return value

    @classmethod
    def after(cls, plan: "PlanNode", prefix: str = "n") -> "IdAllocator":
        maximum = 0
        for node, _, _ in iter_nodes(plan):
            suffix = node.node_id[len(prefix) :] if node.node_id.startswith(prefix) else ""
            if suffix.isdigit():
                maximum = max(maximum, int(suffix))
        return cls(maximum + 1, prefix)


@dataclass
class PlanNode:
    node_type: str
    node_id: str
    attributes: dict[str, Any] = field(default_factory=dict)
    children: list["PlanNode"] = field(default_factory=list)
    origin_ids: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.node_type not in ALLOWED_NODE_TYPES:
            raise SemanticError(
                "unknown logical plan node type", node_type=self.node_type, node_id=self.node_id
            )
        if not self.origin_ids:
            self.origin_ids = [self.node_id]

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.node_type,
            "id": self.node_id,
            "children": [child.to_dict() for child in self.children],
            "attributes": _canonical_object(self.attributes),
            "origin_ids": sorted(set(self.origin_ids)),
        }

    def clone(self) -> "PlanNode":
        return PlanNode(
            node_type=self.node_type,
            node_id=self.node_id,
            attributes=deepcopy(self.attributes),
            children=[child.clone() for child in self.children],
            origin_ids=list(self.origin_ids),
        )


def _canonical_object(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonical_object(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical_object(item) for item in value]
    if isinstance(value, tuple):
        return [_canonical_object(item) for item in value]
    return value


def serialize_plan(plan: PlanNode) -> str:
    return canonical_json(plan.to_dict())


def deserialize_plan(text: str) -> PlanNode:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SemanticError("serialized plan is not valid JSON", json_error=str(exc)) from exc
    return plan_from_dict(payload)


def plan_from_dict(payload: dict[str, Any]) -> PlanNode:
    if not isinstance(payload, dict):
        raise SemanticError("serialized plan node must be an object")
    required = {"type", "id", "children", "attributes"}
    missing = required - set(payload)
    if missing:
        raise SemanticError("serialized plan node is missing fields", missing=sorted(missing))
    if not isinstance(payload["children"], list) or not isinstance(payload["attributes"], dict):
        raise SemanticError("serialized plan has invalid children or attributes")
    node = PlanNode(
        node_type=payload["type"],
        node_id=payload["id"],
        attributes=deepcopy(payload["attributes"]),
        children=[plan_from_dict(child) for child in payload["children"]],
        origin_ids=list(payload.get("origin_ids", [payload["id"]])),
    )
    return node


def iter_nodes(plan: PlanNode, depth: int = 0, parent_id: str | None = None) -> Iterator[tuple[PlanNode, int, str | None]]:
    yield plan, depth, parent_id
    for child in plan.children:
        yield from iter_nodes(child, depth + 1, plan.node_id)


def node_count(plan: PlanNode) -> int:
    return sum(1 for _ in iter_nodes(plan))


def plan_fingerprint(plan: PlanNode) -> str:
    return hashlib.sha256(serialize_plan(plan).encode("utf-8")).hexdigest()


def node_index(plan: PlanNode) -> dict[str, PlanNode]:
    return {node.node_id: node for node, _, _ in iter_nodes(plan)}


def node_summary(plan: PlanNode) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for node, depth, parent_id in iter_nodes(plan):
        summaries.append(
            {
                "id": node.node_id,
                "type": node.node_type,
                "depth": depth,
                "parent_id": parent_id,
                "attribute_keys": sorted(node.attributes),
                "attribute_digest": hashlib.sha256(
                    canonical_json(node.attributes).encode("utf-8")
                ).hexdigest()[:16],
                "origin_ids": sorted(set(node.origin_ids)),
                "summary": _summary_for(node),
            }
        )
    return summaries


def _summary_for(node: PlanNode) -> str:
    attrs = node.attributes
    if node.node_type == "Scan":
        return (
            f"Scan {attrs.get('table')} AS {attrs.get('alias')} "
            f"columns={','.join(attrs.get('columns', [])) or '<none>'}"
        )
    if node.node_type == "Filter":
        return f"Filter source={attrs.get('source')}"
    if node.node_type == "Project":
        return (
            "Project outputs="
            + ",".join(str(item.get("output_name", item["name"])) for item in attrs.get("items", []))
        )
    if node.node_type == "Aggregate":
        return (
            f"Aggregate groups={len(attrs.get('group_by', []))} "
            f"aggregates={len(attrs.get('aggregates', []))}"
        )
    if node.node_type == "Sort":
        return f"Sort keys={len(attrs.get('keys', []))}"
    if node.node_type == "Limit":
        return f"Limit {attrs.get('limit')}"
    if node.node_type == "Join":
        return f"{attrs.get('join_type')} Join"
    return node.node_type


def format_plan_text(plan: PlanNode) -> str:
    lines: list[str] = []
    for node, depth, _ in iter_nodes(plan):
        attrs = canonical_json(node.attributes)
        lines.append(
            f"{'  ' * depth}{node.node_type}(id={node.node_id}, origin={','.join(sorted(set(node.origin_ids)))}) {attrs}"
        )
    return "\n".join(lines)


def output_columns(plan: PlanNode) -> list[str]:
    """Infer the ordered output names of a logical plan."""

    if plan.node_type == "Scan":
        return list(plan.attributes["columns"])
    if plan.node_type in {"Filter", "Sort", "Limit"}:
        return output_columns(plan.children[0])
    if plan.node_type == "Join":
        return output_columns(plan.children[0]) + output_columns(plan.children[1])
    if plan.node_type == "Aggregate":
        return [entry["name"] for entry in plan.attributes["group_by"]] + [
            entry["name"] for entry in plan.attributes["aggregates"]
        ]
    if plan.node_type == "Project":
        return [item["name"] for item in plan.attributes["items"]]
    raise SemanticError("cannot infer output columns", node_type=plan.node_type, node_id=plan.node_id)


def expression_locations(plan: PlanNode) -> list[tuple[PlanNode, str, Expr]]:
    locations: list[tuple[PlanNode, str, Expr]] = []
    for node, _, _ in iter_nodes(plan):
        attrs = node.attributes
        if node.node_type == "Filter":
            locations.append((node, "predicate", attrs["predicate"]))
        elif node.node_type == "Join" and attrs.get("condition") is not None:
            locations.append((node, "condition", attrs["condition"]))
        elif node.node_type == "Project":
            for index, item in enumerate(attrs.get("items", [])):
                locations.append((node, f"items[{index}].expr", item["expr"]))
        elif node.node_type == "Aggregate":
            for index, entry in enumerate(attrs.get("group_by", [])):
                locations.append((node, f"group_by[{index}].expr", entry["expr"]))
            for index, entry in enumerate(attrs.get("aggregates", [])):
                locations.append((node, f"aggregates[{index}].expr", entry["expr"]))
        elif node.node_type == "Sort":
            for index, entry in enumerate(attrs.get("keys", [])):
                locations.append((node, f"keys[{index}].expr", entry["expr"]))
    return locations


def output_schema_check(plan: PlanNode) -> None:
    """Validate that every stored expression references available child columns."""

    def visit(node: PlanNode) -> list[str]:
        if node.node_type == "Scan":
            return list(node.attributes["columns"])
        if node.node_type in {"Filter", "Sort", "Limit"}:
            child_columns = visit(node.children[0])
            if node.node_type == "Filter":
                _require_columns(node, node.attributes["predicate"], set(child_columns))
            elif node.node_type == "Sort":
                for key in node.attributes["keys"]:
                    _require_columns(node, key["expr"], set(child_columns))
            return child_columns
        if node.node_type == "Join":
            left_columns = visit(node.children[0])
            right_columns = visit(node.children[1])
            available = left_columns + right_columns
            if node.attributes.get("condition") is not None:
                _require_columns(node, node.attributes["condition"], set(available))
            return available
        if node.node_type == "Project":
            child_columns = visit(node.children[0])
            for item in node.attributes["items"]:
                _require_columns(node, item["expr"], set(child_columns))
            return [item["name"] for item in node.attributes["items"]]
        if node.node_type == "Aggregate":
            child_columns = visit(node.children[0])
            for entry in node.attributes["group_by"]:
                _require_columns(node, entry["expr"], set(child_columns))
            for entry in node.attributes["aggregates"]:
                _require_columns(node, entry["expr"], set(child_columns))
            return [entry["name"] for entry in node.attributes["group_by"]] + [
                entry["name"] for entry in node.attributes["aggregates"]
            ]
        raise SemanticError("unknown node type during schema check", node_id=node.node_id)

    visit(plan)


def _require_columns(node: PlanNode, expr: Expr, available: set[str]) -> None:
    missing = columns_in(expr) - available
    if missing:
        raise SemanticError(
            "plan expression references columns not produced by its child",
            node_id=node.node_id,
            node_type=node.node_type,
            missing=sorted(missing),
            expression_digest=hashlib.sha256(expr_key(expr).encode("utf-8")).hexdigest()[:16],
        )

