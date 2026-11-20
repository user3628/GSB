from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any

from .errors import error
from .models import PhysicalTable, TableSpec
from .util import SAFE_IDENTIFIER


def _quote_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise error("E_SNAPSHOT_INVALID", "unsafe SQL identifier", identifier=value)
    return f'"{value}"'


def _sql_type(values: list[Any]) -> str:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return "TEXT"
    if all(isinstance(value, bool) or isinstance(value, int) for value in non_null):
        return "INTEGER"
    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in non_null
    ):
        return "REAL"
    if all(isinstance(value, (bytes, bytearray)) for value in non_null):
        return "BLOB"
    return "TEXT"


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return f"0x{value.hex()}"
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return str(value)
    return value


class SQLiteDataStore:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def database_path(self, logical_db: str) -> Path:
        if not SAFE_IDENTIFIER.fullmatch(logical_db):
            raise error("E_SNAPSHOT_INVALID", "unsafe logical database name", logical_db=logical_db)
        return self.data_dir / f"{logical_db}.sqlite3"

    def initialize(self, seed: dict[str, Any], *, replace: bool = True) -> dict[str, Any]:
        databases = seed.get("databases")
        if not isinstance(databases, dict):
            raise error("E_BAD_REQUEST", "seed must contain a databases object")
        initialized: dict[str, Any] = {"databases": {}}
        for logical_db, tables in sorted(databases.items()):
            if not isinstance(tables, dict):
                raise error(
                    "E_BAD_REQUEST", "seed database entry must be an object", logical_db=logical_db
                )
            database_path = self.database_path(logical_db)
            if replace and database_path.exists():
                for suffix in ("", "-wal", "-shm"):
                    Path(str(database_path) + suffix).unlink(missing_ok=True)
            connection = sqlite3.connect(database_path)
            database_report: dict[str, int] = {}
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                for physical_table, rows in sorted(tables.items()):
                    if not SAFE_IDENTIFIER.fullmatch(physical_table):
                        raise error(
                            "E_BAD_REQUEST",
                            "unsafe physical table name in seed",
                            logical_db=logical_db,
                            physical_table=physical_table,
                        )
                    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                        raise error(
                            "E_BAD_REQUEST",
                            "seed table rows must be a list of objects",
                            logical_db=logical_db,
                            physical_table=physical_table,
                        )
                    columns: list[str] = []
                    for row in rows:
                        for column in row:
                            if not isinstance(column, str) or not SAFE_IDENTIFIER.fullmatch(column):
                                raise error(
                                    "E_BAD_REQUEST",
                                    "unsafe column name in seed",
                                    column=column,
                                )
                            if column not in columns:
                                columns.append(column)
                    if not columns:
                        columns = ["id"]
                    connection.execute(f"DROP TABLE IF EXISTS {_quote_identifier(physical_table)}")
                    definitions = ", ".join(
                        f"{_quote_identifier(column)} {_sql_type([row.get(column) for row in rows])}"
                        for column in columns
                    )
                    connection.execute(
                        f"CREATE TABLE {_quote_identifier(physical_table)} ({definitions})"
                    )
                    placeholders = ", ".join("?" for _ in columns)
                    insert_sql = (
                        f"INSERT INTO {_quote_identifier(physical_table)} "
                        f"({', '.join(_quote_identifier(column) for column in columns)}) "
                        f"VALUES ({placeholders})"
                    )
                    connection.executemany(
                        insert_sql,
                        [[row.get(column) for column in columns] for row in rows],
                    )
                    database_report[physical_table] = len(rows)
                connection.commit()
            finally:
                connection.close()
            initialized["databases"][logical_db] = {
                "path": str(database_path),
                "tables": database_report,
            }
        return initialized

    def fetch_rows(
        self,
        target: PhysicalTable,
        table: TableSpec,
        predicate: dict[str, Any],
    ) -> list[dict[str, Any]]:
        database_path = self.database_path(target.logical_db)
        if not database_path.exists():
            raise error(
                "E_EXECUTION_FAILED",
                "physical SQLite database does not exist",
                logical_db=target.logical_db,
                database_path=str(database_path),
            )
        uri = database_path.resolve().as_uri() + "?mode=ro"
        where_sql = ""
        parameters: list[Any] = []
        if "normalized_shard_key" in predicate:
            where_sql = f" WHERE {_quote_identifier(table.shard_key)} = ?"
            parameters.append(predicate["normalized_shard_key"])
        elif "normalized_shard_keys" in predicate:
            values = predicate["normalized_shard_keys"]
            if not values:
                return []
            placeholders = ", ".join("?" for _ in values)
            where_sql = f" WHERE {_quote_identifier(table.shard_key)} IN ({placeholders})"
            parameters.extend(values)
        elif "query_range" in predicate:
            query_range = predicate["query_range"]
            clauses: list[str] = []
            lower = query_range.get("lower")
            upper = query_range.get("upper")
            if lower is not None:
                operator = ">=" if query_range.get("lower_inclusive", True) else ">"
                clauses.append(f"{_quote_identifier(table.shard_key)} {operator} ?")
                parameters.append(lower)
            if upper is not None:
                operator = "<=" if query_range.get("upper_inclusive", False) else "<"
                clauses.append(f"{_quote_identifier(table.shard_key)} {operator} ?")
                parameters.append(upper)
            if clauses:
                where_sql = " WHERE " + " AND ".join(clauses)
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=15)
            try:
                connection.execute("PRAGMA query_only=ON")
                connection.row_factory = sqlite3.Row
                cursor = connection.execute(
                    f"SELECT * FROM {_quote_identifier(target.table)}{where_sql}",
                    parameters,
                )
                return [
                    {key: _json_safe(row[key]) for key in row.keys()}
                    for row in cursor.fetchall()
                ]
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise error(
                "E_EXECUTION_FAILED",
                "SQLite query failed",
                logical_db=target.logical_db,
                physical_table=target.table,
                sqlite_error=str(exc),
            ) from exc

    def row_count(self, logical_db: str, physical_table: str) -> int:
        database_path = self.database_path(logical_db)
        connection = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            row = connection.execute(
                f"SELECT COUNT(*) FROM {_quote_identifier(physical_table)}"
            ).fetchone()
            return int(row[0])
        finally:
            connection.close()

