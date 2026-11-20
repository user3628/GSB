"""Small, position-aware AST for the supported SELECT subset."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Position:
    line: int
    column: int
    offset: int


@dataclass(frozen=True)
class Node:
    pos: Position


@dataclass(frozen=True)
class Identifier(Node):
    name: str
    qualifier: str | None = None


@dataclass(frozen=True)
class Literal(Node):
    value: Any
    value_type: str


@dataclass(frozen=True)
class Star(Node):
    pass


@dataclass(frozen=True)
class UnaryExpr(Node):
    op: str
    operand: Node


@dataclass(frozen=True)
class BinaryExpr(Node):
    op: str
    left: Node
    right: Node


@dataclass(frozen=True)
class IsNullExpr(Node):
    operand: Node
    negated: bool = False


@dataclass(frozen=True)
class InListExpr(Node):
    operand: Node
    items: tuple[Node, ...]
    negated: bool = False


@dataclass(frozen=True)
class FunctionCall(Node):
    name: str
    args: tuple[Node, ...]
    star: bool = False
    distinct: bool = False


@dataclass(frozen=True)
class SelectItem(Node):
    expr: Node
    alias: str | None = None


@dataclass(frozen=True)
class TableRef(Node):
    name: str
    alias: str | None = None


@dataclass(frozen=True)
class JoinRef(Node):
    left: Node
    right: Node
    join_type: str
    condition: Node | None = None


@dataclass(frozen=True)
class OrderItem(Node):
    expr: Node
    direction: str = "ASC"
    nulls: str | None = None


@dataclass(frozen=True)
class SelectStatement:
    items: tuple[SelectItem, ...]
    from_clause: Node
    where: Node | None = None
    group_by: tuple[Node, ...] = field(default_factory=tuple)
    having: Node | None = None
    order_by: tuple[OrderItem, ...] = field(default_factory=tuple)
    limit: int | None = None
    pos: Position | None = None

