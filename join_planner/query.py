from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .errors import invalid_query


@dataclass(frozen=True)
class RelationSpec:
    alias: str
    table: str

    def as_dict(self) -> dict[str, str]:
        return {"alias": self.alias, "table": self.table}


@dataclass(frozen=True)
class ColumnRef:
    relation: str
    column: str

    def as_dict(self) -> dict[str, str]:
        return {"relation": self.relation, "column": self.column}


@dataclass(frozen=True)
class Predicate:
    predicate_id: str
    predicate_type: str
    left: ColumnRef
    right: ColumnRef | None = None
    literal: Any = None
    lower: float | None = None
    upper: float | None = None

    @property
    def relation_aliases(self) -> frozenset[str]:
        aliases = {self.left.relation}
        if self.right is not None:
            aliases.add(self.right.relation)
        return frozenset(aliases)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.predicate_id,
            "type": self.predicate_type,
            "left": self.left.as_dict(),
        }
        if self.right is not None:
            payload["right"] = self.right.as_dict()
        if self.literal is not None:
            payload["literal"] = self.literal
        if self.lower is not None:
            payload["lower"] = self.lower
        if self.upper is not None:
            payload["upper"] = self.upper
        return payload


@dataclass(frozen=True)
class QuerySpec:
    query_id: str
    relations: tuple[RelationSpec, ...]
    predicates: tuple[Predicate, ...]

    @property
    def aliases(self) -> tuple[str, ...]:
        return tuple(relation.alias for relation in self.relations)

    def relation_map(self) -> dict[str, RelationSpec]:
        return {relation.alias: relation for relation in self.relations}

    def as_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "relations": [relation.as_dict() for relation in self.relations],
            "predicates": [predicate.as_dict() for predicate in self.predicates],
        }


def _column_ref(raw: Any, context: str) -> ColumnRef:
    if not isinstance(raw, dict):
        raise invalid_query(f"{context} must be a column reference object")
    relation = raw.get("relation")
    column = raw.get("column")
    if not isinstance(relation, str) or not relation:
        raise invalid_query(f"{context}.relation must be a non-empty string")
    if not isinstance(column, str) or not column:
        raise invalid_query(f"{context}.column must be a non-empty string")
    return ColumnRef(relation=relation, column=column)


def parse_query(raw: Mapping[str, Any], expected_query_id: str | None = None) -> QuerySpec:
    query_id = raw.get("query_id")
    if not isinstance(query_id, str) or not query_id:
        raise invalid_query("query_id must be a non-empty string")
    if expected_query_id is not None and query_id != expected_query_id:
        raise invalid_query(
            "catalog query_id does not match the requested query_id",
            requested_query_id=expected_query_id,
            catalog_query_id=query_id,
        )

    raw_relations = raw.get("relations")
    if not isinstance(raw_relations, list) or not raw_relations:
        raise invalid_query("query must contain a non-empty relations array")
    relations: list[RelationSpec] = []
    for index, item in enumerate(raw_relations):
        if isinstance(item, str):
            if not item:
                raise invalid_query("relation names cannot be empty", index=index)
            relation = RelationSpec(alias=item, table=item)
        elif isinstance(item, dict):
            alias = item.get("alias", item.get("table"))
            table = item.get("table")
            if not isinstance(alias, str) or not alias:
                raise invalid_query("relation alias must be a non-empty string", index=index)
            if not isinstance(table, str) or not table:
                raise invalid_query("relation table must be a non-empty string", index=index)
            relation = RelationSpec(alias=alias, table=table)
        else:
            raise invalid_query("relation must be a string or object", index=index)
        relations.append(relation)
    aliases = [relation.alias for relation in relations]
    if len(set(aliases)) != len(aliases):
        raise invalid_query("relation aliases must be unique", aliases=aliases)
    alias_set = set(aliases)

    raw_predicates = raw.get("predicates")
    if not isinstance(raw_predicates, list):
        raise invalid_query("query must contain a predicates array")
    predicates: list[Predicate] = []
    predicate_ids: set[str] = set()
    for index, item in enumerate(raw_predicates):
        if not isinstance(item, dict):
            raise invalid_query("predicate must be an object", index=index)
        predicate_id = item.get("id")
        if not isinstance(predicate_id, str) or not predicate_id:
            raise invalid_query("predicate id must be a non-empty string", index=index)
        if predicate_id in predicate_ids:
            raise invalid_query("predicate ids must be unique", predicate_id=predicate_id)
        predicate_ids.add(predicate_id)
        predicate_type = item.get("type")
        if predicate_type not in {"equality", "range", "self_join"}:
            raise invalid_query(
                "predicate type must be equality, range, or self_join",
                predicate_id=predicate_id,
            )
        left = _column_ref(item.get("left"), f"predicates[{index}].left")
        right_raw = item.get("right")
        right = _column_ref(right_raw, f"predicates[{index}].right") if right_raw is not None else None
        literal = item.get("literal")
        lower = item.get("lower")
        upper = item.get("upper")

        for bound_name, bound in (("lower", lower), ("upper", upper)):
            if bound is not None and (
                isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(float(bound))
            ):
                raise invalid_query(
                    f"range {bound_name} must be a finite number",
                    predicate_id=predicate_id,
                )
        if lower is not None:
            lower = float(lower)
        if upper is not None:
            upper = float(upper)
        if lower is not None and upper is not None and lower > upper:
            raise invalid_query("range lower cannot exceed upper", predicate_id=predicate_id)

        if predicate_type in {"equality", "self_join"}:
            if right is None and literal is None:
                raise invalid_query(
                    "equality predicate requires right column or literal",
                    predicate_id=predicate_id,
                )
            if right is not None and literal is not None:
                raise invalid_query(
                    "equality predicate cannot specify both right and literal",
                    predicate_id=predicate_id,
                )
        elif predicate_type == "range":
            if lower is None and upper is None:
                raise invalid_query(
                    "range predicate requires lower or upper bound",
                    predicate_id=predicate_id,
                )
            if right is not None and literal is not None:
                raise invalid_query(
                    "range predicate cannot specify both right and literal",
                    predicate_id=predicate_id,
                )

        referenced = {left.relation}
        if right is not None:
            referenced.add(right.relation)
        missing_aliases = sorted(referenced - alias_set)
        if missing_aliases:
            raise invalid_query(
                "predicate references an unknown relation alias",
                predicate_id=predicate_id,
                missing_aliases=missing_aliases,
            )
        if predicate_type == "self_join" and (right is None or left.relation == right.relation):
            raise invalid_query(
                "self_join requires two distinct relation aliases",
                predicate_id=predicate_id,
            )
        predicates.append(
            Predicate(
                predicate_id=predicate_id,
                predicate_type=predicate_type,
                left=left,
                right=right,
                literal=literal,
                lower=lower,
                upper=upper,
            )
        )

    return QuerySpec(query_id=query_id, relations=tuple(relations), predicates=tuple(predicates))


class QueryCatalog:
    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self._queries = self._load()

    def _load(self) -> dict[str, QuerySpec]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise invalid_query("query catalog cannot be loaded", path=str(self.path), error=str(exc)) from exc
        raw_queries = payload.get("queries") if isinstance(payload, dict) else None
        if not isinstance(raw_queries, list) or not raw_queries:
            raise invalid_query("query catalog must contain a non-empty queries array")
        queries: dict[str, QuerySpec] = {}
        for raw in raw_queries:
            if not isinstance(raw, dict):
                raise invalid_query("catalog query must be an object")
            query = parse_query(raw)
            if query.query_id in queries:
                raise invalid_query("query catalog contains duplicate query_id", query_id=query.query_id)
            queries[query.query_id] = query
        return queries

    def get(self, query_id: str) -> QuerySpec:
        try:
            return self._queries[query_id]
        except KeyError as exc:
            raise invalid_query(
                "unknown query_id",
                query_id=query_id,
                available_query_ids=sorted(self._queries),
            ) from exc

    def list_public(self) -> list[dict[str, Any]]:
        return [
            {
                "query_id": query.query_id,
                "relations": [relation.as_dict() for relation in query.relations],
                "predicate_count": len(query.predicates),
            }
            for query in sorted(self._queries.values(), key=lambda item: item.query_id)
        ]

