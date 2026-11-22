from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from .errors import fail
from .util import digest_json, require_dict, require_list, require_str


SUPPORTED_FILTER_OPS = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "is_null",
    "not_null",
}
SUPPORTED_AGGREGATES = {"sum", "count", "min", "max", "avg", "quantile", "median"}


def _source_refs(source: Any, *, view_name: str) -> tuple[list[dict[str, str]], str]:
    source = require_dict(source, field=f"view {view_name}.source")
    kind = source.get("kind")
    if kind in {"table", "view"}:
        name = require_str(source.get("name"), field=f"view {view_name}.source.name")
        ref = {"kind": str(kind), "name": name}
        return [ref], "single"
    if kind == "join":
        join_type = source.get("type", "inner")
        if join_type not in {"inner", "left"}:
            fail(
                "E_INVALID_REQUEST",
                "Unsupported join type.",
                context={"view": view_name, "type": join_type, "supported": ["inner", "left"]},
            )
        refs: list[dict[str, str]] = []
        for side in ("left", "right"):
            side_source = require_dict(
                source.get(side), field=f"view {view_name}.source.{side}"
            )
            side_kind = side_source.get("kind")
            if side_kind not in {"table", "view"}:
                fail(
                    "E_INVALID_REQUEST",
                    "Join inputs must reference a table or view.",
                    context={"view": view_name, "side": side, "kind": side_kind},
                )
            name = require_str(
                side_source.get("name"), field=f"view {view_name}.source.{side}.name"
            )
            refs.append({"kind": str(side_kind), "name": name})
        on = require_list(source.get("on"), field=f"view {view_name}.source.on")
        if not on:
            fail(
                "E_INVALID_REQUEST",
                "A join needs at least one equality predicate.",
                context={"view": view_name},
            )
        for predicate in on:
            predicate = require_dict(
                predicate, field=f"view {view_name}.source.on[]"
            )
            for key in ("left", "right"):
                require_str(
                    predicate.get(key), field=f"view {view_name}.source.on[].{key}"
                )
        select = require_dict(
            source.get("select"), field=f"view {view_name}.source.select"
        )
        if not select:
            fail(
                "E_INVALID_REQUEST",
                "A join needs a non-empty select mapping.",
                context={"view": view_name},
            )
        for output, expression in select.items():
            require_str(output, field=f"view {view_name}.source.select key")
            require_str(expression, field=f"view {view_name}.source.select[{output}]")
        return refs, "join"
    fail(
        "E_INVALID_REQUEST",
        "Unsupported source kind.",
        context={
            "view": view_name,
            "kind": kind,
            "supported": ["table", "view", "join"],
        },
    )


def validate_view(view: Any) -> dict[str, Any]:
    view = require_dict(view, field="view definition")
    name = require_str(view.get("name"), field="view.name")
    operators = require_list(view.get("operators", []), field=f"view {name}.operators")
    grouped = False
    for index, operator in enumerate(operators):
        operator = require_dict(operator, field=f"view {name}.operators[{index}]")
        op = operator.get("op")
        if op == "project":
            columns = require_dict(
                operator.get("columns"), field=f"view {name}.project.columns"
            )
            if not columns:
                fail(
                    "E_INVALID_REQUEST",
                    "A projection needs columns.",
                    context={"view": name},
                )
            for output, expression in columns.items():
                require_str(output, field=f"view {name}.project output")
                require_str(expression, field=f"view {name}.project[{output}]")
        elif op == "filter":
            where = require_list(
                operator.get("where"), field=f"view {name}.filter.where"
            )
            if not where:
                fail(
                    "E_INVALID_REQUEST",
                    "A filter needs at least one predicate.",
                    context={"view": name},
                )
            for predicate in where:
                predicate = require_dict(
                    predicate, field=f"view {name}.filter.where[]"
                )
                require_str(
                    predicate.get("column"), field=f"view {name}.filter.column"
                )
                filter_op = predicate.get("operator")
                if filter_op not in SUPPORTED_FILTER_OPS:
                    fail(
                        "E_INVALID_REQUEST",
                        "Unsupported filter operator.",
                        context={
                            "view": name,
                            "operator": filter_op,
                            "supported": sorted(SUPPORTED_FILTER_OPS),
                        },
                    )
        elif op == "group_by":
            if grouped or index != len(operators) - 1:
                fail(
                    "E_INVALID_REQUEST",
                    "group_by must be the final operator and may appear only once.",
                    context={"view": name, "index": index},
                )
            grouped = True
            keys = require_list(operator.get("keys", []), field=f"view {name}.keys")
            for key in keys:
                require_str(key, field=f"view {name}.group_by.keys[]")
            aggregates = require_list(
                operator.get("aggregates", []), field=f"view {name}.aggregates"
            )
            if not aggregates:
                fail(
                    "E_INVALID_REQUEST",
                    "A group_by needs aggregates.",
                    context={"view": name},
                )
            for aggregate in aggregates:
                aggregate = require_dict(
                    aggregate, field=f"view {name}.aggregates[]"
                )
                require_str(
                    aggregate.get("name"), field=f"view {name}.aggregates[].name"
                )
                function = aggregate.get("func")
                if function not in SUPPORTED_AGGREGATES:
                    fail(
                        "E_INVALID_REQUEST",
                        "Unsupported aggregate function.",
                        context={
                            "view": name,
                            "function": function,
                            "supported": sorted(SUPPORTED_AGGREGATES),
                        },
                    )
                if function != "count" or aggregate.get("column") is not None:
                    require_str(
                        aggregate.get("column"),
                        field=f"view {name}.aggregates[].column",
                    )
                if function in {"quantile", "median"}:
                    if function == "quantile":
                        q = aggregate.get("q")
                        if not isinstance(q, (int, float)) or not 0 <= float(q) <= 1:
                            fail(
                                "E_INVALID_REQUEST",
                                "quantile.q must be a number in [0, 1].",
                                context={"view": name, "q": q},
                            )
        else:
            fail(
                "E_INVALID_REQUEST",
                "Unsupported operator.",
                context={
                    "view": name,
                    "operator": op,
                    "supported": ["project", "filter", "group_by"],
                },
            )
    refs, _ = _source_refs(view.get("source"), view_name=name)
    return {
        "name": name,
        "source": view.get("source"),
        "operators": operators,
        "dependency_refs": refs,
        "description": str(view.get("description", "")),
    }


@dataclass(frozen=True)
class ViewGraph:
    definitions: dict[str, Any]
    views: dict[str, dict[str, Any]]
    edges: list[dict[str, str]]
    dependencies: dict[str, set[str]]
    base_dependencies: dict[str, set[str]]
    reverse_dependencies: dict[str, set[str]]
    layers: list[dict[str, Any]]
    order: list[str]
    digest: str

    def downstream(self, names: set[str] | list[str]) -> set[str]:
        found = set(names)
        queue = deque(names)
        while queue:
            current = queue.popleft()
            for child in sorted(self.reverse_dependencies.get(current, set())):
                if child not in found:
                    found.add(child)
                    queue.append(child)
        return found

    def depth_from(self, roots: set[str]) -> dict[str, int]:
        depths: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque((root, 0) for root in sorted(roots))
        while queue:
            view, depth = queue.popleft()
            old = depths.get(view)
            if old is None or depth < old:
                depths[view] = depth
                for child in sorted(self.reverse_dependencies.get(view, set())):
                    queue.append((child, depth + 1))
        return depths


def build_graph(definitions: Any, base_tables: set[str]) -> ViewGraph:
    definitions = require_dict(definitions, field="view definitions")
    raw_views = require_list(definitions.get("views"), field="definitions.views")
    if not raw_views:
        fail(
            "E_INVALID_REQUEST",
            "At least one view definition is required.",
            context={"field": "definitions.views"},
        )

    views: dict[str, dict[str, Any]] = {}
    for raw_view in raw_views:
        view = validate_view(raw_view)
        name = view["name"]
        if name in views:
            fail(
                "E_INVALID_REQUEST",
                "Duplicate view name.",
                context={"view": name},
            )
        views[name] = view

    unknown: list[dict[str, str]] = []
    edges: list[dict[str, str]] = []
    dependencies: dict[str, set[str]] = {name: set() for name in views}
    base_dependencies: dict[str, set[str]] = {name: set() for name in views}
    for name, view in views.items():
        for ref in view["dependency_refs"]:
            ref_name = ref["name"]
            if ref["kind"] == "table":
                if ref_name not in base_tables:
                    unknown.append({"from": ref_name, "to": name, "kind": "table"})
                else:
                    base_dependencies[name].add(ref_name)
                edges.append({"from": ref_name, "to": name, "kind": "table"})
            else:
                if ref_name not in views:
                    unknown.append({"from": ref_name, "to": name, "kind": "view"})
                dependencies[name].add(ref_name)
                edges.append({"from": ref_name, "to": name, "kind": "view"})
    if unknown:
        fail(
            "E_REFERENCE_UNKNOWN",
            "A view references an undeclared base table or view.",
            context={
                "references": sorted(unknown, key=lambda item: (item["to"], item["from"])),
                "declared_tables": sorted(base_tables),
                "declared_views": sorted(views),
            },
            status_code=422,
        )

    # Deterministic cycle detection with the view stack attached to the error.
    color: dict[str, int] = {name: 0 for name in views}
    stack: list[str] = []

    def visit(name: str) -> None:
        color[name] = 1
        stack.append(name)
        for dependency in sorted(dependencies[name]):
            if color[dependency] == 0:
                visit(dependency)
            elif color[dependency] == 1:
                start = stack.index(dependency)
                cycle = stack[start:] + [dependency]
                fail(
                    "E_DEPENDENCY_CYCLE",
                    "The view dependency graph contains a cycle.",
                    context={"views": cycle[:-1], "cycle": cycle},
                    status_code=422,
                )
        stack.pop()
        color[name] = 2

    for name in sorted(views):
        if color[name] == 0:
            visit(name)

    indegree = {name: len(dependencies[name]) for name in views}
    reverse: dict[str, set[str]] = {name: set() for name in views}
    for child, parents in dependencies.items():
        for parent in parents:
            reverse[parent].add(child)
    ready = sorted(name for name, degree in indegree.items() if degree == 0)
    layers: list[dict[str, Any]] = []
    order: list[str] = []
    layer_index = 0
    while ready:
        current_layer = ready
        layers.append({"layer": layer_index, "views": current_layer})
        next_layer: list[str] = []
        for name in current_layer:
            order.append(name)
            for child in sorted(reverse[name]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    next_layer.append(child)
        ready = sorted(next_layer)
        layer_index += 1
    if len(order) != len(views):
        fail(
            "E_DEPENDENCY_CYCLE",
            "The view dependency graph could not be topologically ordered.",
            context={"views": sorted(name for name, degree in indegree.items() if degree)},
            status_code=422,
        )

    return ViewGraph(
        definitions=definitions,
        views=views,
        edges=sorted(edges, key=lambda edge: (edge["from"], edge["to"], edge["kind"])),
        dependencies=dependencies,
        base_dependencies=base_dependencies,
        reverse_dependencies=reverse,
        layers=layers,
        order=order,
        digest=digest_json(definitions),
    )


def graph_payload(graph: ViewGraph, *, graph_version: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "layers": graph.layers,
        "edges": graph.edges,
        "topological_order": graph.order,
        "digest": graph.digest,
    }
    if graph_version is not None:
        payload["graph_version"] = graph_version
    return payload
