"""Semantics-preserving normalization with explicit safety vetoes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .business import RewriteAbortedError, SemanticError
from .expressions import (
    Expr,
    binary,
    boolean,
    canonical_json,
    clone_expr,
    columns_in,
    expr_key,
    fold_expr,
    walk,
)
from .plan import (
    IdAllocator,
    PlanNode,
    expression_locations,
    iter_nodes,
    node_count,
    output_schema_check,
    plan_fingerprint,
    serialize_plan,
)


@dataclass
class RewriteResult:
    plan: PlanNode
    log: list[dict[str, Any]]
    rejections: list[dict[str, Any]]
    before_fingerprint: str
    after_fingerprint: str
    rounds: int
    rule_attempts: int
    node_mapping: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_dict(),
            "normalized_plan": serialize_plan(self.plan),
            "rewrite_log": self.log,
            "rejections": self.rejections,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "rounds": self.rounds,
            "rule_attempts": self.rule_attempts,
            "node_mapping": self.node_mapping,
        }


class PlanRewriter:
    MAX_FOLD_ROUNDS = 8

    def __init__(self, *, max_steps: int = 500, abort_after: int | None = None) -> None:
        self.max_steps = max_steps
        self.abort_after = abort_after
        self._actions = 0
        self._inspection_steps = 0
        self._log: list[dict[str, Any]] = []
        self._allocator: IdAllocator | None = None

    def normalize(self, plan: PlanNode) -> RewriteResult:
        original = plan.clone()
        before_fingerprint = plan_fingerprint(original)
        self._log = []
        self._actions = 0
        self._inspection_steps = 0
        self._allocator = IdAllocator.after(plan)
        working = plan.clone()
        rounds = 0

        working = self._fold_constants(working, phase="constant_fold_initial")
        working = self._push_predicates(working)
        working = self._prune_columns(working)
        for index in range(self.MAX_FOLD_ROUNDS):
            rounds += 1
            before = serialize_plan(working)
            working = self._fold_constants(working, phase=f"constant_fold_{index + 1}")
            if serialize_plan(working) == before:
                break

        if self.abort_after is not None and self._actions < self.abort_after:
            self._raise_abort(
                "normalization completed fewer actions than the injected abort marker",
                node_id=working.node_id,
                rule="abort_marker",
                actions_completed=self._actions,
            )

        output_schema_check(working)
        self._assert_no_aggregate_above_aggregate(working)
        _assert_unique_ids(working)
        mapping = _node_mapping(original, working)
        rejections = [entry for entry in self._log if entry["status"] == "rejected"]
        return RewriteResult(
            plan=working,
            log=list(self._log),
            rejections=rejections,
            before_fingerprint=before_fingerprint,
            after_fingerprint=plan_fingerprint(working),
            rounds=max(rounds, 1),
            rule_attempts=len(self._log),
            node_mapping=mapping,
        )

    def _fold_constants(self, plan: PlanNode, *, phase: str) -> PlanNode:
        result = self._fold_node(plan, phase)
        return result

    def _fold_node(self, node: PlanNode, phase: str) -> PlanNode:
        self._inspect("constant_fold", node.node_id)
        folded_children = [self._fold_node(child, phase) for child in node.children]
        node = PlanNode(
            node_type=node.node_type,
            node_id=node.node_id,
            attributes=node.attributes,
            children=folded_children,
            origin_ids=list(node.origin_ids),
        )
        before = node.to_dict()
        changed = False
        attrs = node.attributes

        if node.node_type == "Filter":
            original_predicate = attrs["predicate"]
            folded, did_change = fold_expr(original_predicate)
            attrs["predicate"] = folded
            changed = changed or did_change
            if folded.get("kind") == "literal" and folded.get("value_type") == "boolean":
                if folded.get("value") is True:
                    after = node.children[0].to_dict()
                    self._record(
                        "constant_fold",
                        "applied",
                        node.node_id,
                        before,
                        after,
                        reason="predicate folded to TRUE; filter removed",
                        conflict_node=None,
                        phase=phase,
                    )
                    return node.children[0]
        elif node.node_type == "Join" and attrs.get("condition") is not None:
            attrs["condition"], did_change = fold_expr(attrs["condition"])
            changed = changed or did_change
        elif node.node_type == "Project":
            for item in attrs.get("items", []):
                item["expr"], did_change = fold_expr(item["expr"])
                changed = changed or did_change
        elif node.node_type == "Aggregate":
            for entry in attrs.get("group_by", []):
                entry["expr"], did_change = fold_expr(entry["expr"])
                changed = changed or did_change
            for entry in attrs.get("aggregates", []):
                entry["expr"], did_change = fold_expr(entry["expr"])
                changed = changed or did_change
        elif node.node_type == "Sort":
            for entry in attrs.get("keys", []):
                entry["expr"], did_change = fold_expr(entry["expr"])
                changed = changed or did_change

        if changed:
            self._record(
                "constant_fold",
                "applied",
                node.node_id,
                before,
                node.to_dict(),
                reason="constant subexpressions folded",
                conflict_node=None,
                phase=phase,
            )
        return node

    def _push_predicates(self, plan: PlanNode) -> PlanNode:
        return self._push_node(plan)

    def _push_node(self, node: PlanNode) -> PlanNode:
        node = PlanNode(
            node_type=node.node_type,
            node_id=node.node_id,
            attributes=node.attributes,
            children=[self._push_node(child) for child in node.children],
            origin_ids=list(node.origin_ids),
        )
        if node.node_type != "Filter":
            return node
        source = node.attributes.get("source", "WHERE")
        predicate = node.attributes["predicate"]
        conjuncts = _split_and(predicate)
        if source == "HAVING":
            for conjunct in conjuncts:
                self._inspect("predicate_pushdown", node.node_id)
                self._record(
                    "predicate_pushdown",
                    "rejected",
                    node.node_id,
                    node.to_dict(),
                    node.to_dict(),
                    reason="HAVING crosses an aggregation boundary; pushdown would change aggregate semantics",
                    conflict_node=node.children[0].node_id,
                    phase="predicate_pushdown",
                )
            return node

        child = node.children[0]
        residuals: list[Expr] = []
        for conjunct in conjuncts:
            self._inspect("predicate_pushdown", node.node_id)
            before = PlanNode(
                node_type=node.node_type,
                node_id=node.node_id,
                attributes={**node.attributes, "predicate": clone_expr(conjunct)},
                children=[child.clone()],
                origin_ids=list(node.origin_ids),
            ).to_dict()
            new_child, pushed, conflict_node, reason = self._try_push(child, conjunct, node.origin_ids)
            if pushed:
                child = new_child
                self._record(
                    "predicate_pushdown",
                    "applied",
                    node.node_id,
                    before,
                    child.to_dict(),
                    reason="predicate pushed toward Scan while preserving join semantics",
                    conflict_node=None,
                    phase="predicate_pushdown",
                )
            else:
                residuals.append(conjunct)
                self._record(
                    "predicate_pushdown",
                    "rejected",
                    node.node_id,
                    before,
                    node.to_dict(),
                    reason=reason,
                    conflict_node=conflict_node,
                    phase="predicate_pushdown",
                )
        if not residuals:
            return child
        residual = residuals[0]
        for item in residuals[1:]:
            residual = binary("AND", residual, item)
        return PlanNode(
            node_type="Filter",
            node_id=node.node_id,
            attributes={**node.attributes, "predicate": residual},
            children=[child],
            origin_ids=list(node.origin_ids),
        )

    def _try_push(
        self, subtree: PlanNode, predicate: Expr, source_origin_ids: list[str]
    ) -> tuple[PlanNode, bool, str | None, str]:
        if subtree.node_type == "Scan":
            assert self._allocator is not None
            node_id = self._allocator.new()
            filter_node = PlanNode(
                node_type="Filter",
                node_id=node_id,
                attributes={"source": "WHERE", "predicate": predicate},
                children=[subtree],
                origin_ids=sorted(set([node_id, *source_origin_ids])),
            )
            return filter_node, True, None, ""
        if subtree.node_type == "Filter":
            merged = binary("AND", subtree.attributes["predicate"], predicate)
            return (
                PlanNode(
                    node_type="Filter",
                    node_id=subtree.node_id,
                    attributes={**subtree.attributes, "predicate": merged},
                    children=subtree.children,
                    origin_ids=list(subtree.origin_ids),
                ),
                True,
                None,
                "",
            )
        if subtree.node_type != "Join":
            return (
                subtree,
                False,
                subtree.node_id,
                f"predicate cannot cross a {subtree.node_type} boundary safely",
            )

        refs = columns_in(predicate)
        left_aliases = _scan_aliases(subtree.children[0])
        right_aliases = _scan_aliases(subtree.children[1])
        left_refs = {ref for ref in refs if _ref_alias(ref) in left_aliases}
        right_refs = {ref for ref in refs if _ref_alias(ref) in right_aliases}
        cross_or_unknown = len(left_refs) + len(right_refs) != len(refs)
        join_type = str(subtree.attributes.get("join_type", "INNER")).upper()
        side: int | None = None
        if cross_or_unknown:
            side = None
        elif not refs:
            if join_type in {"INNER", "CROSS"}:
                side = 0
        elif join_type in {"INNER", "CROSS"}:
            if left_refs and not right_refs:
                side = 0
            elif right_refs and not left_refs:
                side = 1
        elif join_type == "LEFT":
            if left_refs and not right_refs:
                side = 0
        elif join_type == "RIGHT":
            if right_refs and not left_refs:
                side = 1

        if side is None:
            reason = (
                f"predicate references both sides or a non-preserved side of {join_type} JOIN"
                if refs
                else f"constant predicate is retained above {join_type} JOIN"
            )
            return subtree, False, subtree.node_id, reason

        child, pushed, conflict, reason = self._try_push(
            subtree.children[side], predicate, source_origin_ids
        )
        if not pushed:
            return subtree, False, conflict, reason
        children = list(subtree.children)
        children[side] = child
        return (
            PlanNode(
                node_type="Join",
                node_id=subtree.node_id,
                attributes=subtree.attributes,
                children=children,
                origin_ids=list(subtree.origin_ids),
            ),
            True,
            None,
            "",
        )

    def _prune_columns(self, plan: PlanNode) -> PlanNode:
        cloned = plan.clone()
        all_needed = _all_referenced_columns(cloned)
        visible_project_refs = _visible_project_columns(cloned)
        protected = _protected_columns(cloned)
        must_keep = all_needed | set(protected)
        scans = [node for node, _, _ in iter_nodes(cloned) if node.node_type == "Scan"]
        for scan in scans:
            alias = scan.attributes["alias"]
            current = list(scan.attributes["columns"])
            kept = [name for name in current if name in must_keep]
            candidate_removed = [
                name
                for name in current
                if name not in visible_project_refs
                and _ref_alias(name) == alias
            ]
            conflicts = [
                name
                for name in candidate_removed
                if name in protected
            ]
            for name in conflicts:
                for reason, node_id in protected[name]:
                    self._inspect("projection_pruning", scan.node_id)
                    self._record(
                        "projection_pruning",
                        "rejected",
                        scan.node_id,
                        scan.to_dict(),
                        scan.to_dict(),
                        reason=f"refusing to prune {name}: {reason}",
                        conflict_node=node_id,
                        phase="projection_pruning",
                        column=name,
                    )
            if kept == current:
                continue
            self._inspect("projection_pruning", scan.node_id)
            before = scan.to_dict()
            scan.attributes["columns"] = kept
            try:
                output_schema_check(cloned)
            except SemanticError as exc:
                scan.attributes["columns"] = current
                self._record(
                    "projection_pruning",
                    "rejected",
                    scan.node_id,
                    before,
                    scan.to_dict(),
                    reason=f"pruning would invalidate a downstream reference: {exc.message}",
                    conflict_node=scan.node_id,
                    phase="projection_pruning",
                )
                continue
            self._record(
                "projection_pruning",
                "applied",
                scan.node_id,
                before,
                scan.to_dict(),
                reason="removed columns not referenced by any surviving expression",
                conflict_node=None,
                phase="projection_pruning",
                removed=current,
                kept=kept,
            )
        return cloned

    def _assert_no_aggregate_above_aggregate(self, plan: PlanNode) -> None:
        for node, _, _ in iter_nodes(plan):
            if node.node_type == "Aggregate":
                continue
            attrs = node.attributes
            expressions: list[Expr] = []
            if node.node_type == "Filter":
                expressions.append(attrs["predicate"])
            elif node.node_type == "Join" and attrs.get("condition") is not None:
                expressions.append(attrs["condition"])
            elif node.node_type == "Project":
                expressions.extend(item["expr"] for item in attrs.get("items", []))
            elif node.node_type == "Sort":
                expressions.extend(item["expr"] for item in attrs.get("keys", []))
            for expr in expressions:
                if any(
                    item.get("kind") == "func" and item.get("name") in {"COUNT", "SUM", "MIN", "MAX"}
                    for item in walk(expr)
                ):
                    raise SemanticError(
                        "normalization left an aggregate function outside Aggregate",
                        node_id=node.node_id,
                        node_type=node.node_type,
                    )

    def _inspect(self, rule: str, node_id: str) -> None:
        self._inspection_steps += 1
        if self.abort_after is not None and self._actions >= self.abort_after:
            self._raise_abort(
                "rewrite abort marker reached",
                node_id=node_id,
                rule=rule,
                actions_completed=self._actions,
            )
        if self._inspection_steps > self.max_steps:
            self._raise_abort(
                "rewrite inspection budget exhausted; stopping to guarantee termination",
                node_id=node_id,
                rule=rule,
                actions_completed=self._actions,
                max_steps=self.max_steps,
            )

    def _raise_abort(self, message: str, **context: Any) -> None:
        raise RewriteAbortedError(message, **context)

    def _record(
        self,
        rule: str,
        status: str,
        node_id: str,
        before: dict[str, Any],
        after: dict[str, Any],
        *,
        reason: str,
        conflict_node: str | None,
        phase: str,
        **extra: Any,
    ) -> None:
        entry = {
            "step": len(self._log) + 1,
            "rule": rule,
            "status": status,
            "phase": phase,
            "node_id": node_id,
            "conflict_node": conflict_node,
            "reason": reason,
            "before": before,
            "after": after,
        }
        entry.update(extra)
        self._log.append(entry)
        if status == "applied":
            self._actions += 1
            if self.abort_after is not None and self._actions >= self.abort_after:
                self._raise_abort(
                    "rewrite abort marker reached after a completed action",
                    node_id=node_id,
                    rule=rule,
                    actions_completed=self._actions,
                )


def _split_and(expr: Expr) -> list[Expr]:
    if expr.get("kind") == "binary" and expr.get("op") == "AND":
        return _split_and(expr["left"]) + _split_and(expr["right"])
    return [expr]


def _scan_aliases(plan: PlanNode) -> set[str]:
    aliases: set[str] = set()
    for node, _, _ in iter_nodes(plan):
        if node.node_type == "Scan":
            aliases.add(str(node.attributes["alias"]))
    return aliases


def _ref_alias(reference: str) -> str:
    return reference.split(".", 1)[0]


def _all_referenced_columns(plan: PlanNode) -> set[str]:
    referenced: set[str] = set()
    for _, _, expr in expression_locations(plan):
        referenced.update(columns_in(expr))
    return referenced


def _visible_project_columns(plan: PlanNode) -> set[str]:
    referenced: set[str] = set()
    for node, _, _ in iter_nodes(plan):
        if node.node_type != "Project":
            continue
        for item in node.attributes.get("items", []):
            if item.get("visible", True):
                referenced.update(columns_in(item["expr"]))
    return referenced


def _protected_columns(plan: PlanNode) -> dict[str, list[tuple[str, str]]]:
    protected: dict[str, list[tuple[str, str]]] = {}

    def protect(reference: str, reason: str, node_id: str) -> None:
        protected.setdefault(reference, []).append((reason, node_id))

    project_items: dict[str, Expr] = {}
    aggregate_outputs: dict[str, Expr] = {}
    for node, _, _ in iter_nodes(plan):
        if node.node_type == "Project":
            for item in node.attributes.get("items", []):
                project_items[str(item["name"])] = item["expr"]
        elif node.node_type == "Aggregate":
            for entry in node.attributes.get("group_by", []):
                aggregate_outputs[str(entry["name"])] = entry["expr"]
            for entry in node.attributes.get("aggregates", []):
                aggregate_outputs[str(entry["name"])] = entry["expr"]

    having_refs: set[str] = set()
    for node, _, _ in iter_nodes(plan):
        if node.node_type == "Filter" and node.attributes.get("source") == "HAVING":
            direct = columns_in(node.attributes["predicate"])
            expanded: set[str] = set()
            for ref in direct:
                expr = aggregate_outputs.get(ref)
                if expr is None:
                    expr = project_items.get(ref)
                if expr is None:
                    expanded.add(ref)
                else:
                    expanded.update(columns_in(expr))
            for ref in expanded:
                protect(ref, "HAVING references this column", node.node_id)
            having_refs.update(direct)

    for node, _, _ in iter_nodes(plan):
        if node.node_type == "Filter" and node.attributes.get("source") != "HAVING":
            direct = columns_in(node.attributes["predicate"])
            for ref in direct:
                protect(ref, "filter predicate would be lost", node.node_id)
        elif node.node_type == "Join" and node.attributes.get("condition") is not None:
            for ref in columns_in(node.attributes["condition"]):
                protect(ref, "join condition or outer-join semantics would change", node.node_id)
        elif node.node_type == "Aggregate":
            for entry in node.attributes.get("group_by", []):
                reason = (
                    "HAVING depends on this grouping expression"
                    if entry["name"] in having_refs
                    else "aggregate grouping would change"
                )
                for ref in columns_in(entry["expr"]):
                    protect(ref, reason, node.node_id)
            for entry in node.attributes.get("aggregates", []):
                reason = (
                    "HAVING depends on this aggregate input"
                    if entry["name"] in having_refs
                    else "aggregate result would change"
                )
                for ref in columns_in(entry["expr"]):
                    protect(ref, reason, node.node_id)
        elif node.node_type == "Sort":
            for key in node.attributes.get("keys", []):
                refs = columns_in(key["expr"])
                for ref in refs:
                    target = project_items.get(ref)
                    if target is not None:
                        for column_ref in columns_in(target):
                            protect(column_ref, "sort key would be lost", node.node_id)
                    else:
                        protect(ref, "sort key would be lost", node.node_id)
        elif node.node_type == "Project":
            for item in node.attributes.get("items", []):
                if not item.get("visible", True):
                    for ref in columns_in(item["expr"]):
                        protect(ref, "hidden ORDER BY projection would be lost", node.node_id)
    return protected


def _assert_unique_ids(plan: PlanNode) -> None:
    seen: set[str] = set()
    for node, _, _ in iter_nodes(plan):
        if node.node_id in seen:
            raise SemanticError("duplicate node id after rewrite", node_id=node.node_id)
        seen.add(node.node_id)


def _node_mapping(before: PlanNode, after: PlanNode) -> dict[str, list[str]]:
    after_ids = {node.node_id for node, _, _ in iter_nodes(after)}
    mapping: dict[str, list[str]] = {}
    for node, _, _ in iter_nodes(before):
        mapped = sorted(after_ids.intersection(node.origin_ids))
        if node.node_id in after_ids:
            mapped = sorted(set(mapped + [node.node_id]))
        mapping[node.node_id] = mapped
    for node, _, _ in iter_nodes(after):
        for origin in node.origin_ids:
            if origin in mapping and node.node_id not in mapping[origin]:
                mapping[origin].append(node.node_id)
                mapping[origin].sort()
    return mapping


def normalize_plan(
    plan: PlanNode, *, max_steps: int = 500, abort_after: int | None = None
) -> RewriteResult:
    return PlanRewriter(max_steps=max_steps, abort_after=abort_after).normalize(plan)

