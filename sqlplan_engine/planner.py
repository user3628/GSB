"""Translate the parsed SELECT subset into a logical plan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from .ast import (
    BinaryExpr,
    FunctionCall,
    Identifier,
    InListExpr,
    IsNullExpr,
    JoinRef,
    Literal,
    Node,
    SelectItem,
    SelectStatement,
    Star,
    TableRef,
    UnaryExpr,
)
from .business import InvalidAggregateError, ParseError, SemanticError
from .expressions import (
    Expr,
    aggregate_functions,
    binary,
    canonical_json,
    column,
    contains_aggregate,
    expr_key,
    literal,
    unary,
    walk,
)
from .plan import IdAllocator, PlanNode, output_schema_check
from .sample import Catalog


AGGREGATE_NAMES = {"COUNT", "SUM", "MIN", "MAX"}


@dataclass(frozen=True)
class Source:
    table: str
    alias: str


class LogicalPlanner:
    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog
        self.allocator = IdAllocator()
        self.sources: list[Source] = []
        self._by_alias: dict[str, Source] = {}

    def plan(
        self, statement: SelectStatement, *, force_invalid_aggregate: bool = False
    ) -> PlanNode:
        self.allocator = IdAllocator()
        self.sources = []
        self._by_alias = {}
        plan, sources = self._build_from(statement.from_clause)
        self.sources = sources
        self._by_alias = {source.alias: source for source in sources}

        where_expr = None
        if statement.where is not None:
            where_expr = self._resolve_expr(statement.where)
            if contains_aggregate(where_expr):
                raise InvalidAggregateError(
                    "aggregate functions are not allowed in WHERE",
                    clause="WHERE",
                    position=self._position_context(statement.where),
                )
            if any(node.get("kind") == "star" for node in walk(where_expr)):
                raise SemanticError(
                    "wildcard is not allowed in WHERE",
                    position=self._position_context(statement.where),
                )
            plan = self._node("Filter", {"source": "WHERE", "predicate": where_expr}, [plan])

        aggregate_context = self._build_aggregate_context(statement)
        use_aggregate = aggregate_context is not None or bool(statement.group_by) or (
            statement.having is not None
        )

        if statement.having is not None and not use_aggregate:
            raise SemanticError(
                "HAVING requires GROUP BY or an aggregate expression",
                position=self._position_context(statement.having),
            )

        group_map: dict[str, tuple[str, Expr]] = {}
        agg_map: dict[str, tuple[str, Expr]] = {}
        aggregate_node: PlanNode | None = None
        if use_aggregate:
            aggregate_context = aggregate_context or self._build_aggregate_context(statement)
            assert aggregate_context is not None
            group_map, group_entries = self._make_group_map(statement.group_by)
            agg_map, agg_entries = self._make_aggregate_map(
                aggregate_context, statement, group_map=group_map
            )
            aggregate_node = self._node(
                "Aggregate",
                {"group_by": group_entries, "aggregates": agg_entries},
                [plan],
            )
            plan = aggregate_node
            if force_invalid_aggregate:
                raise InvalidAggregateError(
                    "injected aggregate validation conflict: a non-grouped column was referenced",
                    clause="INJECTION",
                    node_id=aggregate_node.node_id,
                    rule="aggregate_validation",
                    injected=True,
                    group_by=[entry["name"] for entry in group_entries],
                    conflicting_column=self._find_injected_aggregate_column(
                        statement, group_map, agg_map
                    ),
                )

        project_items: list[dict[str, object]] = []
        used_names: set[str] = set()
        for index, item in enumerate(statement.items):
            if isinstance(item.expr, Star):
                for source in self.sources:
                    for table_column in self.catalog.columns(source.table):
                        ref = f"{source.alias}.{table_column}"
                        name = self._unique_name(table_column, used_names)
                        expr = column(ref)
                        if use_aggregate:
                            expr = self._translate_aggregate_expr(
                                expr, group_map, agg_map, "SELECT"
                            )
                        project_items.append(
                            {
                                "name": name,
                                "output_name": table_column,
                                "expr": expr,
                                "visible": True,
                                "source_index": index,
                            }
                        )
                continue
            resolved = self._resolve_expr(item.expr)
            translated = resolved
            if use_aggregate:
                translated = self._translate_aggregate_expr(resolved, group_map, agg_map, "SELECT")
            default_name = self._default_output_name(item.expr, resolved, index)
            output_name = item.alias or default_name
            name = self._unique_name(output_name, used_names)
            project_items.append(
                {
                    "name": name,
                    "output_name": output_name,
                    "expr": translated,
                    "visible": True,
                    "source_index": index,
                }
            )
        if not project_items:
            raise SemanticError("SELECT list is empty")

        if statement.having is not None:
            having_resolved = self._resolve_expr(statement.having)
            if not use_aggregate:
                raise SemanticError("HAVING requires aggregation")
            having_translated = self._translate_aggregate_expr(
                having_resolved, group_map, agg_map, "HAVING"
            )
            plan = self._node(
                "Filter", {"source": "HAVING", "predicate": having_translated}, [plan]
            )

        sort_keys, extra_hidden = self._build_sort_keys(
            statement,
            project_items,
            group_map,
            agg_map,
            use_aggregate,
        )

        project_node = self._node("Project", {"items": project_items}, [plan])
        plan = project_node
        if extra_hidden:
            visible_items = [
                {
                    "name": item["name"],
                    "output_name": item["output_name"],
                    "expr": column(str(item["name"])),
                    "visible": True,
                }
                for item in project_items
                if item.get("visible", True)
            ]
            plan = self._node("Sort", {"keys": sort_keys}, [plan])
            plan = self._node("Project", {"items": visible_items}, [plan])
        elif sort_keys:
            plan = self._node("Sort", {"keys": sort_keys}, [plan])

        if statement.limit is not None:
            plan = self._node("Limit", {"limit": statement.limit}, [plan])

        output_schema_check(plan)
        return plan

    def _build_from(self, node: Node) -> tuple[PlanNode, list[Source]]:
        if isinstance(node, TableRef):
            table = node.name
            alias = node.alias or table.split(".")[-1]
            columns = self.catalog.columns(table)
            scan = self._node(
                "Scan",
                {
                    "table": table,
                    "alias": alias,
                    "columns": [f"{alias}.{name}" for name in columns],
                },
                [],
            )
            return scan, [Source(table=table, alias=alias)]
        if isinstance(node, JoinRef):
            left, left_sources = self._build_from(node.left)
            right, right_sources = self._build_from(node.right)
            alias_seen: set[str] = set()
            for source in left_sources + right_sources:
                if source.alias in alias_seen:
                    raise SemanticError(
                        "duplicate table alias", alias=source.alias, position=self._position_context(node)
                    )
                alias_seen.add(source.alias)
            self.sources = left_sources + right_sources
            self._by_alias = {source.alias: source for source in self.sources}
            condition = None
            if node.condition is not None:
                condition = self._resolve_expr(node.condition)
                if contains_aggregate(condition):
                    raise InvalidAggregateError(
                        "aggregate functions are not allowed in JOIN conditions",
                        clause="JOIN",
                        position=self._position_context(node.condition),
                    )
            join = self._node(
                "Join",
                {"join_type": node.join_type, "condition": condition},
                [left, right],
            )
            return join, left_sources + right_sources
        raise SemanticError("unsupported FROM clause", node_type=type(node).__name__)

    def _make_group_map(
        self, group_by: tuple[Node, ...]
    ) -> tuple[dict[str, tuple[str, Expr]], list[dict[str, object]]]:
        mapping: dict[str, tuple[str, Expr]] = {}
        entries: list[dict[str, object]] = []
        for ast_expr in group_by:
            resolved = self._resolve_expr(ast_expr)
            if contains_aggregate(resolved):
                raise InvalidAggregateError(
                    "GROUP BY cannot contain aggregate functions",
                    clause="GROUP BY",
                    position=self._position_context(ast_expr),
                )
            key = expr_key(resolved)
            if key not in mapping:
                name = f"__group_{len(entries) + 1}"
                mapping[key] = (name, resolved)
                entries.append({"name": name, "expr": resolved})
        return mapping, entries

    def _build_aggregate_context(
        self, statement: SelectStatement
    ) -> list[Expr] | None:
        aliases = {
            item.alias: item.expr
            for item in statement.items
            if item.alias is not None and not isinstance(item.expr, Star)
        }
        expressions: list[Expr] = []
        for item in statement.items:
            if not isinstance(item.expr, Star):
                expressions.extend(self._find_aggregate_exprs(self._resolve_expr(item.expr)))
        if statement.having is not None:
            having_expr = statement.having
            if isinstance(having_expr, Identifier) and having_expr.qualifier is None:
                have_alias = aliases.get(having_expr.name)
                if have_alias is not None:
                    having_expr = have_alias
            expressions.extend(self._find_aggregate_exprs(self._resolve_expr(having_expr)))
        for item in statement.order_by:
            order_expr = item.expr
            if isinstance(order_expr, Identifier) and order_expr.qualifier is None:
                have_alias = aliases.get(order_expr.name)
                if have_alias is not None:
                    order_expr = have_alias
            expressions.extend(self._find_aggregate_exprs(self._resolve_expr(order_expr)))
        if statement.group_by or statement.having is not None or expressions:
            return expressions
        return None

    def _find_aggregate_exprs(self, expr: Expr) -> list[Expr]:
        found: list[Expr] = []

        def visit(current: Expr) -> None:
            if current.get("kind") == "func" and current.get("name") in AGGREGATE_NAMES:
                found.append(current)
                for arg in current.get("args", []):
                    if any(
                        node.get("kind") == "func" and node.get("name") in AGGREGATE_NAMES
                        for node in walk(arg)
                    ):
                        raise InvalidAggregateError(
                            "aggregate functions cannot be nested",
                            function=current.get("name"),
                        )
                return
            for child in self._expr_children(current):
                visit(child)

        visit(expr)
        return found

    def _make_aggregate_map(
        self,
        aggregate_exprs: list[Expr],
        statement: SelectStatement,
        *,
        group_map: dict[str, tuple[str, Expr]],
    ) -> tuple[dict[str, tuple[str, Expr]], list[dict[str, object]]]:
        del statement, group_map
        mapping: dict[str, tuple[str, Expr]] = {}
        entries: list[dict[str, object]] = []
        for expr in aggregate_exprs:
            key = expr_key(expr)
            if key not in mapping:
                name = f"__agg_{len(entries) + 1}"
                mapping[key] = (name, expr)
                entries.append({"name": name, "expr": expr})
        return mapping, entries

    def _translate_aggregate_expr(
        self,
        expr: Expr,
        group_map: dict[str, tuple[str, Expr]],
        agg_map: dict[str, tuple[str, Expr]],
        clause: str,
    ) -> Expr:
        key = expr_key(expr)
        if key in agg_map:
            return column(agg_map[key][0])
        if key in group_map:
            return column(group_map[key][0])
        kind = expr.get("kind")
        if kind == "column":
            raise InvalidAggregateError(
                "column is neither grouped nor inside an aggregate function",
                clause=clause,
                column=expr["name"],
                group_expressions=[entry["name"] for _, entry in group_map.values()],
                rule="aggregate_validation",
            )
        if kind == "literal":
            return dict(expr)
        if kind == "unary":
            return unary(
                expr["op"], self._translate_aggregate_expr(expr["operand"], group_map, agg_map, clause)
            )
        if kind == "binary":
            return binary(
                expr["op"],
                self._translate_aggregate_expr(expr["left"], group_map, agg_map, clause),
                self._translate_aggregate_expr(expr["right"], group_map, agg_map, clause),
            )
        if kind == "is_null":
            return {
                **expr,
                "operand": self._translate_aggregate_expr(
                    expr["operand"], group_map, agg_map, clause
                ),
            }
        if kind == "in":
            return {
                **expr,
                "operand": self._translate_aggregate_expr(
                    expr["operand"], group_map, agg_map, clause
                ),
                "items": [
                    self._translate_aggregate_expr(item, group_map, agg_map, clause)
                    for item in expr["items"]
                ],
            }
        if kind == "func":
            if expr.get("name") in AGGREGATE_NAMES:
                raise InvalidAggregateError(
                    "aggregate function is not registered in the Aggregate node",
                    clause=clause,
                    function=expr.get("name"),
                )
            raise SemanticError("unsupported scalar function", function=expr.get("name"))
        if kind == "star":
            raise SemanticError("wildcard is not valid in this aggregate expression")
        raise SemanticError("unknown expression kind during aggregation", kind=kind)

    def _build_sort_keys(
        self,
        statement: SelectStatement,
        project_items: list[dict[str, object]],
        group_map: dict[str, tuple[str, Expr]],
        agg_map: dict[str, tuple[str, Expr]],
        use_aggregate: bool,
    ) -> tuple[list[dict[str, object]], bool]:
        if not statement.order_by:
            return [], False
        item_by_expr = {expr_key(item["expr"]): item["name"] for item in project_items}
        output_names = {str(item["name"]) for item in project_items}
        keys: list[dict[str, object]] = []
        hidden_added = False
        for order_item in statement.order_by:
            key_expr: Expr | None = None
            if isinstance(order_item.expr, Identifier) and order_item.expr.qualifier is None:
                if order_item.expr.name in output_names:
                    key_expr = column(order_item.expr.name)
            if key_expr is None:
                resolved = self._resolve_expr(order_item.expr)
                translated = (
                    self._translate_aggregate_expr(resolved, group_map, agg_map, "ORDER BY")
                    if use_aggregate
                    else resolved
                )
                key = expr_key(translated)
                if key in item_by_expr:
                    key_expr = column(item_by_expr[key])
                else:
                    hidden_name = f"__sort_{sum(1 for item in project_items if str(item['name']).startswith('__sort_')) + 1}"
                    project_items.append(
                        {
                            "name": hidden_name,
                            "output_name": hidden_name,
                            "expr": translated,
                            "visible": False,
                            "source_index": None,
                        }
                    )
                    hidden_added = True
                    key_expr = column(hidden_name)
            keys.append(
                {
                    "expr": key_expr,
                    "direction": order_item.direction,
                    "nulls": order_item.nulls
                    or ("FIRST" if order_item.direction == "ASC" else "LAST"),
                }
            )
        return keys, hidden_added

    def _resolve_expr(self, node: Node) -> Expr:
        if isinstance(node, Literal):
            return literal(node.value, node.value_type)
        if isinstance(node, Identifier):
            return column(self._resolve_column(node))
        if isinstance(node, Star):
            return {"kind": "star"}
        if isinstance(node, UnaryExpr):
            if node.op.upper() not in {"NOT", "+", "-"}:
                raise SemanticError(
                    "unsupported unary operator",
                    operator=node.op,
                    position=self._position_context(node),
                )
            return unary(node.op, self._resolve_expr(node.operand))
        if isinstance(node, BinaryExpr):
            if node.op.upper() not in {"AND", "OR", "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||"}:
                raise SemanticError(
                    "unsupported binary operator",
                    operator=node.op,
                    position=self._position_context(node),
                )
            return binary(node.op, self._resolve_expr(node.left), self._resolve_expr(node.right))
        if isinstance(node, IsNullExpr):
            return {
                "kind": "is_null",
                "operand": self._resolve_expr(node.operand),
                "negated": node.negated,
            }
        if isinstance(node, InListExpr):
            return {
                "kind": "in",
                "operand": self._resolve_expr(node.operand),
                "items": [self._resolve_expr(item) for item in node.items],
                "negated": node.negated,
            }
        if isinstance(node, FunctionCall):
            return self._resolve_function(node)
        raise SemanticError(
            "unsupported expression", expression_type=type(node).__name__, position=self._position_context(node)
        )

    def _resolve_function(self, node: FunctionCall) -> Expr:
        name = node.name.upper()
        if name not in AGGREGATE_NAMES:
            raise SemanticError(
                "unsupported function",
                function=name,
                supported=sorted(AGGREGATE_NAMES),
                position=self._position_context(node),
            )
        if node.star and name != "COUNT":
            raise InvalidAggregateError(
                "only COUNT supports '*': use COUNT(*)",
                clause="FUNCTION",
                function=name,
                position=self._position_context(node),
            )
        if not node.star and len(node.args) != 1:
            raise InvalidAggregateError(
                "aggregate function requires exactly one argument or COUNT(*)",
                clause="FUNCTION",
                function=name,
                argument_count=len(node.args),
                position=self._position_context(node),
            )
        args = [self._resolve_expr(arg) for arg in node.args]
        if any(contains_aggregate(arg) for arg in args):
            raise InvalidAggregateError(
                "aggregate functions cannot be nested",
                clause="FUNCTION",
                function=name,
                position=self._position_context(node),
            )
        return {
            "kind": "func",
            "name": name,
            "args": args,
            "star": node.star,
            "distinct": node.distinct,
        }

    def _resolve_column(self, node: Identifier) -> str:
        if node.qualifier is not None:
            source = self._by_alias.get(node.qualifier)
            if source is None:
                raise SemanticError(
                    "unknown table alias or qualifier in column reference",
                    qualifier=node.qualifier,
                    column=node.name,
                    available_aliases=sorted(self._by_alias),
                    position=self._position_context(node),
                )
            if node.name not in self.catalog.columns(source.table):
                raise SemanticError(
                    "unknown column",
                    table=source.table,
                    column=node.name,
                    position=self._position_context(node),
                )
            return f"{source.alias}.{node.name}"
        matches = [
            source
            for source in self.sources
            if node.name in self.catalog.columns(source.table)
        ]
        if not matches:
            raise SemanticError(
                "unknown column",
                column=node.name,
                position=self._position_context(node),
            )
        if len(matches) > 1:
            raise SemanticError(
                "ambiguous column reference",
                column=node.name,
                matching_aliases=sorted(source.alias for source in matches),
                position=self._position_context(node),
            )
        return f"{matches[0].alias}.{node.name}"

    def _default_output_name(self, ast_expr: Node, resolved: Expr, index: int) -> str:
        if isinstance(ast_expr, Identifier):
            return ast_expr.name
        if isinstance(ast_expr, FunctionCall):
            return ast_expr.name.lower()
        if resolved.get("kind") == "column":
            return str(resolved["name"]).split(".", 1)[-1]
        return f"expr_{index + 1}"

    def _unique_name(self, preferred: str, used: set[str]) -> str:
        candidate = preferred or "column"
        counter = 2
        while candidate in used:
            candidate = f"{preferred}_{counter}"
            counter += 1
        used.add(candidate)
        return candidate

    def _find_injected_aggregate_column(
        self,
        statement: SelectStatement,
        group_map: dict[str, tuple[str, Expr]],
        agg_map: dict[str, tuple[str, Expr]],
    ) -> str:
        grouped_columns: set[str] = set()
        for _, expr in group_map.values():
            for node in walk(expr):
                if node.get("kind") == "column":
                    grouped_columns.add(node["name"])
        aggregate_arguments: set[str] = set()
        for _, expr in agg_map.values():
            for arg in expr.get("args", []):
                for node in walk(arg):
                    if node.get("kind") == "column":
                        aggregate_arguments.add(node["name"])
        for node in walk(self._resolve_expr(statement.items[0].expr) if statement.items else literal(0)):
            if node.get("kind") == "column" and node["name"] not in grouped_columns:
                return node["name"]
        for source in self.sources:
            for table_column in self.catalog.columns(source.table):
                qualified = f"{source.alias}.{table_column}"
                if qualified not in grouped_columns and qualified not in aggregate_arguments:
                    return qualified
        return "<synthetic-ungrouped-column>"

    def _node(
        self, node_type: str, attributes: dict[str, object], children: list[PlanNode]
    ) -> PlanNode:
        node_id = self.allocator.new()
        return PlanNode(node_type=node_type, node_id=node_id, attributes=attributes, children=children)

    def _expr_children(self, expr: Expr) -> list[Expr]:
        kind = expr.get("kind")
        if kind == "unary":
            return [expr["operand"]]
        if kind == "binary":
            return [expr["left"], expr["right"]]
        if kind == "is_null":
            return [expr["operand"]]
        if kind == "in":
            return [expr["operand"], *expr["items"]]
        if kind == "func":
            return list(expr.get("args", []))
        return []

    def _position_context(self, node: Node | None) -> dict[str, object] | None:
        if node is None or getattr(node, "pos", None) is None:
            return None
        return {"line": node.pos.line, "column": node.pos.column, "offset": node.pos.offset}


def build_logical_plan(
    statement: SelectStatement,
    catalog: Catalog,
    *,
    force_invalid_aggregate: bool = False,
) -> PlanNode:
    return LogicalPlanner(catalog).plan(
        statement, force_invalid_aggregate=force_invalid_aggregate
    )

