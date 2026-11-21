from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import WorkbenchConfig
from .errors import BusinessError
from .sql_analysis import validate_readonly_sql


class VerificationExecutionError(Exception):
    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def as_business_error(self) -> BusinessError:
        return BusinessError(self.code, self.message, self.details)


@dataclass(slots=True)
class QueryResult:
    columns: list[str]
    row_count: int
    digest: str
    preview: list[list[Any]]


def _canonical_cell(value: Any) -> Any:
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["int", int(value)]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        if math.isnan(value):
            return ["float", "nan"]
        if math.isinf(value):
            return ["float", "inf" if value > 0 else "-inf"]
        return ["float", format(value, ".17g")]
    if isinstance(value, bytes):
        return ["blob", base64.b64encode(value).decode("ascii")]
    return ["text", str(value)]


def _row_digest(columns: list[str], rows: list[list[Any]]) -> str:
    canonical_rows = sorted(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
    payload = json.dumps(
        {"columns": columns, "rows": canonical_rows},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def execute_query(conn: sqlite3.Connection, sql: str, max_rows: int) -> QueryResult:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [item[0] for item in (cursor.description or [])]
        rows: list[list[Any]] = []
        while True:
            batch = cursor.fetchmany(512)
            if not batch:
                break
            for raw_row in batch:
                rows.append([_canonical_cell(value) for value in raw_row])
                if len(rows) > max_rows:
                    raise VerificationExecutionError(
                        "E_VERIFY_ERROR",
                        "Query result exceeds the configured verification row limit.",
                        {"max_result_rows": max_rows},
                    )
        preview = [[cell[1] if len(cell) > 1 else cell[0] for cell in row] for row in rows[:3]]
        return QueryResult(
            columns=columns,
            row_count=len(rows),
            digest=_row_digest(columns, rows),
            preview=preview,
        )
    finally:
        cursor.close()


_PLAN_INDEX_PATTERN = re.compile(
    r"USING\s+(?:COVERING\s+)?INDEX\s+([^\s(]+)", re.IGNORECASE
)
_PLAN_EQUALITY_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*=\s*\?")
_PLAN_RANGE_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*(?:<|>|<=|>=|BETWEEN)\s*", re.IGNORECASE)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "`"}:
        return value[1:-1].replace(value[0] * 2, value[0])
    if len(value) >= 2 and value[0] == "[" and value[-1] == "]":
        return value[1:-1]
    return value


def _table_entry(snapshot: dict[str, Any], table: str) -> dict[str, Any] | None:
    table = _strip_quotes(table).lower()
    return snapshot.get("tables", {}).get(table)


def _index_entry(table_entry: dict[str, Any] | None, index_name: str) -> dict[str, Any] | None:
    if not table_entry:
        return None
    cleaned = _strip_quotes(index_name).lower()
    for index in table_entry.get("indexes", []):
        if str(index.get("name", "")).lower() == cleaned:
            return index
    return None


def _ndv(table_entry: dict[str, Any] | None, column: str) -> int:
    if not table_entry:
        return 1
    columns = table_entry.get("columns", {})
    entry = columns.get(column.lower())
    if not isinstance(entry, dict):
        return 1
    value = entry.get("ndv", 1)
    return max(int(value), 1)


def estimate_plan_cost(
    conn: sqlite3.Connection, sql: str, snapshot: dict[str, Any]
) -> dict[str, Any]:
    cursor = conn.cursor()
    try:
        rows = cursor.execute("EXPLAIN QUERY PLAN " + sql).fetchall()
    finally:
        cursor.close()
    plan = [str(row[3]) for row in rows]
    estimated_rows = 0.0
    temp_btree = False
    for detail in plan:
        upper = detail.upper()
        if "TEMP B-TREE" in upper or "TEMP B-TREE" in upper:
            temp_btree = True
        if "SUBQUERY" in upper or upper.startswith(("CO-ROUTINE", "MATERIALIZE", "MULTI-INDEX OR")):
            continue
        match = re.match(r"^(SCAN|SEARCH)\s+([^\s]+)", detail, re.IGNORECASE)
        if not match:
            continue
        action, table_token = match.group(1).upper(), match.group(2)
        table = _strip_quotes(table_token).lower()
        table_entry = _table_entry(snapshot, table)
        if not table_entry:
            estimated_rows += 1.0
            continue
        row_count = float(table_entry.get("row_count", 1))
        index_match = _PLAN_INDEX_PATTERN.search(detail)
        if not index_match and action == "SCAN":
            estimated_rows += row_count
            continue
        if not index_match and "INTEGER PRIMARY KEY" in upper:
            estimated_rows += 1.0
            continue
        if not index_match:
            estimated_rows += max(1.0, row_count / 10.0)
            continue
        index_name = index_match.group(1)
        index_entry = _index_entry(table_entry, index_name)
        equality_columns = [name.lower() for name in _PLAN_EQUALITY_PATTERN.findall(detail)]
        if not equality_columns and not index_entry:
            estimated_rows += max(1.0, row_count / 10.0)
            continue
        divisor = 1.0
        for column in equality_columns:
            divisor *= _ndv(table_entry, column)
        estimate = max(1.0, row_count / max(divisor, 1.0))
        range_columns = [name.lower() for name in _PLAN_RANGE_PATTERN.findall(detail)]
        for column in range_columns:
            if column not in equality_columns:
                estimate *= 0.25
        estimated_rows += max(1.0, estimate)
    estimated_rows = max(1.0, estimated_rows)
    penalty = 100.0 if temp_btree else 0.0
    return {
        "score": int(round(estimated_rows + penalty)),
        "estimated_rows_scanned": int(round(estimated_rows)),
        "temp_btree": temp_btree,
        "plan": plan,
    }


def _validate_setup_sql(statements: list[str]) -> None:
    for statement in statements:
        normalized = statement.strip().rstrip(";").strip()
        if not normalized.upper().startswith("CREATE INDEX "):
            raise VerificationExecutionError(
                "E_VERIFY_ERROR",
                "Only generated CREATE INDEX setup statements may be executed during verification.",
                {"statement": statement},
            )
        if ";" in normalized:
            raise VerificationExecutionError(
                "E_VERIFY_ERROR",
                "A setup statement must contain exactly one SQLite statement.",
                {"statement": statement},
            )


def _copy_database(source_path: Path, destination_path: Path) -> tuple[bool, bool]:
    source_closed = False
    destination_closed = False
    source = None
    destination = None
    try:
        uri = f"{source_path.as_uri()}?mode=ro"
        source = sqlite3.connect(uri, uri=True, timeout=5.0)
        source.execute("PRAGMA query_only=ON")
        destination = sqlite3.connect(str(destination_path), timeout=5.0)
        source.backup(destination)
        destination.commit()
    finally:
        if source is not None:
            source.close()
            source_closed = True
        if destination is not None:
            destination.close()
            destination_closed = True
    return source_closed, destination_closed


def verify_rewrite(
    *,
    sample_database_path: Path,
    original_sql: str,
    suggestion: dict[str, Any],
    snapshot: dict[str, Any],
    config: WorkbenchConfig,
) -> dict[str, Any]:
    validate_readonly_sql(original_sql)
    validate_readonly_sql(suggestion["rewritten_sql"])
    setup_sql = list(suggestion.get("setup_sql") or [])
    _validate_setup_sql(setup_sql)
    if not sample_database_path.exists():
        raise VerificationExecutionError(
            "E_VERIFY_ERROR",
            "The local sample SQLite database does not exist.",
            {"sample_database_path": str(sample_database_path)},
        )
    temp_dir = Path(
        tempfile.mkdtemp(prefix="slowquery-verify-", dir=str(config.verification_temp_dir or tempfile.gettempdir()))
    )
    temp_db = temp_dir / "verification.sqlite3"
    connection: sqlite3.Connection | None = None
    connection_closed = False
    temp_removed = False
    source_closed = False
    copy_destination_closed = False
    result_holder: dict[str, dict[str, Any]] = {}
    try:
        source_closed, copy_destination_closed = _copy_database(sample_database_path, temp_db)
        connection = sqlite3.connect(str(temp_db), timeout=5.0)
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        original_cost = estimate_plan_cost(connection, original_sql, snapshot)
        original_result = execute_query(connection, original_sql, config.max_result_rows)

        connection.execute("PRAGMA query_only=OFF")
        for statement in setup_sql:
            connection.execute(statement)
        connection.commit()

        rewritten_cost = estimate_plan_cost(
            connection, suggestion["rewritten_sql"], snapshot
        )
        rewritten_result = execute_query(
            connection, suggestion["rewritten_sql"], config.max_result_rows
        )

        consistent = (
            original_result.row_count == rewritten_result.row_count
            and original_result.digest == rewritten_result.digest
        )
        if not consistent:
            result = {
                "outcome": "FAILED",
                "error_code": "E_VERIFY_MISMATCH",
                "message": "Rewrite result set differs from the original query result set.",
                "original_cost": original_cost,
                "rewritten_cost": rewritten_cost,
                "original_result_digest": original_result.digest,
                "rewritten_result_digest": rewritten_result.digest,
                "original_row_count": original_result.row_count,
                "rewritten_row_count": rewritten_result.row_count,
                "original_columns": original_result.columns,
                "rewritten_columns": rewritten_result.columns,
                "original_preview": original_result.preview,
                "rewritten_preview": rewritten_result.preview,
                "cost_improved": rewritten_cost["score"] < original_cost["score"],
                "details": {
                    "comparison": "full sorted row multiset digest including column names",
                    "sample_database_path": str(sample_database_path),
                },
                "resources": {},
            }
            result_holder["result"] = result
            return result
        result = {
            "outcome": "VERIFIED",
            "error_code": None,
            "message": "Rewrite is result-set consistent with the original query.",
            "original_cost": original_cost,
            "rewritten_cost": rewritten_cost,
            "original_result_digest": original_result.digest,
            "rewritten_result_digest": rewritten_result.digest,
            "original_row_count": original_result.row_count,
            "rewritten_row_count": rewritten_result.row_count,
            "original_columns": original_result.columns,
            "rewritten_columns": rewritten_result.columns,
            "original_preview": original_result.preview,
            "rewritten_preview": rewritten_result.preview,
            "cost_improved": rewritten_cost["score"] < original_cost["score"],
            "details": {
                "comparison": "full sorted row multiset digest including column names",
                "sample_database_path": str(sample_database_path),
            },
            "resources": {},
        }
        result_holder["result"] = result
        return result
    except VerificationExecutionError:
        raise
    except sqlite3.Error as exc:
        raise VerificationExecutionError(
            "E_VERIFY_ERROR",
            f"SQLite rejected a query or rewrite during verification: {exc}",
            {"sqlite_error": str(exc)},
        ) from exc
    finally:
        if connection is not None:
            connection.close()
            connection_closed = True
        try:
            shutil.rmtree(temp_dir)
            temp_removed = True
        except OSError:
            temp_removed = False
        if "result" in result_holder:
            result_holder["result"]["resources"] = verification_resource_report(
                source_closed=source_closed,
                copy_destination_closed=copy_destination_closed,
                connection_closed=connection_closed,
                temp_removed=temp_removed,
            )


def verification_resource_report(
    *,
    source_closed: bool,
    copy_destination_closed: bool,
    connection_closed: bool,
    temp_removed: bool,
) -> dict[str, bool]:
    return {
        "source_connection_closed": source_closed,
        "copy_destination_connection_closed": copy_destination_closed,
        "verification_connection_closed": connection_closed,
        "temp_database_removed": temp_removed,
    }
