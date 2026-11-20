from __future__ import annotations

from typing import Any

from .errors import error
from .models import PhysicalTable, TableSpec
from .util import stable_value_key


def _primary_value(row: dict[str, Any], target: PhysicalTable) -> Any:
    if target.primary_key not in row:
        raise error(
            "E_EXECUTION_FAILED",
            "result row is missing its declared primary key column",
            logical_db=target.logical_db,
            physical_table=target.table,
            primary_key=target.primary_key,
        )
    return row[target.primary_key]


def _preferred(
    existing: tuple[dict[str, Any], int, PhysicalTable],
    candidate: tuple[dict[str, Any], int, PhysicalTable],
    conflict_rule: str,
) -> tuple[dict[str, Any], int, PhysicalTable]:
    if conflict_rule == "physical_order_first":
        return existing if existing[1] <= candidate[1] else candidate
    if conflict_rule == "physical_order_last":
        return existing if existing[1] >= candidate[1] else candidate
    if conflict_rule == "max_updated_at_physical_order":
        existing_updated = stable_value_key(existing[0].get("updated_at"))
        candidate_updated = stable_value_key(candidate[0].get("updated_at"))
        if candidate_updated > existing_updated:
            return candidate
        if candidate_updated < existing_updated:
            return existing
        return existing if existing[1] <= candidate[1] else candidate
    raise error("E_INTERNAL", "unknown conflict rule", conflict_rule=conflict_rule)


def merge_rows(
    table: TableSpec,
    planned_targets: list[dict[str, Any]],
    rows_by_target: list[tuple[PhysicalTable, list[dict[str, Any]]]],
    *,
    offset: int,
    limit: int | None,
) -> dict[str, Any]:
    selected: dict[tuple, tuple[dict[str, Any], int, PhysicalTable]] = {}
    conflicts: list[dict[str, Any]] = []
    for target, rows in rows_by_target:
        order = next(
            item["order"]
            for item in planned_targets
            if item["logical_db"] == target.logical_db
            and item["table"] == target.table
            and item["shard"] == target.shard
        )
        for row in rows:
            primary = _primary_value(row, target)
            key = (table.logical_table, stable_value_key(primary))
            candidate = (row, order, target)
            existing = selected.get(key)
            if existing is None:
                selected[key] = candidate
                continue
            winner = _preferred(existing, candidate, table.conflict_rule)
            loser = candidate if winner is existing else existing
            conflicts.append(
                {
                    "primary_key": primary,
                    "winner": {
                        "logical_db": winner[2].logical_db,
                        "physical_table": winner[2].table,
                        "shard": winner[2].shard,
                    },
                    "discarded": {
                        "logical_db": loser[2].logical_db,
                        "physical_table": loser[2].table,
                        "shard": loser[2].shard,
                    },
                    "conflict_rule": table.conflict_rule,
                }
            )
            selected[key] = winner

    ordered = sorted(
        selected.values(),
        key=lambda item: (
            stable_value_key(item[0].get(table.shard_key)),
            stable_value_key(_primary_value(item[0], item[2])),
            item[1],
        ),
    )
    total = len(ordered)
    window = ordered[offset:] if limit is None else ordered[offset : offset + limit]
    rows = [item[0] for item in window]
    row_sources = [
        {
            "primary_key": _primary_value(item[0], item[2]),
            "logical_db": item[2].logical_db,
            "physical_table": item[2].table,
            "shard": item[2].shard,
            "physical_order": item[1],
        }
        for item in window
    ]
    return {
        "rows": rows,
        "row_sources": row_sources,
        "conflicts": conflicts,
        "page": {
            "offset": offset,
            "limit": limit,
            "returned": len(rows),
            "total_after_conflict": total,
            "has_more": offset + len(rows) < total,
        },
    }

