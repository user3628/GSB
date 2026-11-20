"""Deterministic local SQLite sample database and statement manifest."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .business import EngineError, SemanticError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SAMPLE_DB = PROJECT_ROOT / "data" / "sample.sqlite3"
DEFAULT_RUNTIME_DB = PROJECT_ROOT / "runtime" / "plans.sqlite3"
DEFAULT_MANIFEST = PROJECT_ROOT / "samples" / "statements.json"


SCHEMA_SQL = """
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    budget REAL NOT NULL
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    dept_id INTEGER NOT NULL,
    salary REAL NOT NULL,
    active INTEGER NOT NULL,
    manager_id INTEGER,
    FOREIGN KEY (dept_id) REFERENCES departments(id)
);
CREATE TABLE projects (
    id INTEGER PRIMARY KEY,
    dept_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    cost REAL NOT NULL,
    FOREIGN KEY (dept_id) REFERENCES departments(id)
);
"""


ROWS: dict[str, list[tuple[Any, ...]]] = {
    "departments": [
        (1, "Engineering", "East", 900000.0),
        (2, "Sales", "East", 450000.0),
        (3, "Operations", "West", 600000.0),
        (4, "Research", "North", 725000.0),
    ],
    "employees": [
        (1, "Alice", 1, 120000.0, 1, None),
        (2, "Bob", 1, 98000.0, 1, 1),
        (3, "Carol", 2, 87000.0, 1, 4),
        (4, "Dave", 2, 76000.0, 0, 4),
        (5, "Eve", 3, 91000.0, 1, 6),
        (6, "Frank", 3, 105000.0, 1, None),
        (7, "Grace", 4, 115000.0, 1, 6),
        (8, "Heidi", 1, 72000.0, 0, 1),
    ],
    "projects": [
        (1, 1, "Compiler", 210000.0),
        (2, 1, "Telemetry", 135000.0),
        (3, 2, "CRM Refresh", 90000.0),
        (4, 3, "Warehouse Move", 170000.0),
        (5, 4, "Materials Lab", 250000.0),
        (6, 4, "Sensor Study", 80000.0),
    ],
}


def generate_sample_database(path: str | Path = DEFAULT_SAMPLE_DB) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    if temp.exists():
        temp.unlink()
    connection = sqlite3.connect(temp)
    try:
        connection.executescript(SCHEMA_SQL)
        for table, rows in ROWS.items():
            placeholders = ",".join("?" for _ in rows[0])
            connection.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()
    if target.exists():
        target.unlink()
    temp.replace(target)
    return target


def load_manifest(path: str | Path = DEFAULT_MANIFEST) -> list[dict[str, Any]]:
    target = Path(path)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SemanticError("statement manifest is missing", path=str(target)) from exc
    except json.JSONDecodeError as exc:
        raise SemanticError("statement manifest is invalid JSON", path=str(target)) from exc
    if not isinstance(payload, list):
        raise SemanticError("statement manifest must contain a list", path=str(target))
    return payload


class Catalog:
    """Read-only table metadata and schema lookup for planning."""

    def __init__(self, db_path: str | Path = DEFAULT_SAMPLE_DB) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise EngineError(
                "E_SAMPLE_NOT_READY",
                "sample database does not exist; run the deterministic generator first",
                {"path": str(self.db_path)},
            )
        self._tables = self._load_tables()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path.resolve().as_uri() + "?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _load_tables(self) -> dict[str, list[str]]:
        connection = self._connect()
        try:
            names = [
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
            tables: dict[str, list[str]] = {}
            for name in names:
                quoted = '"' + name.replace('"', '""') + '"'
                tables[name] = [
                    row["name"] for row in connection.execute(f"PRAGMA table_info({quoted})")
                ]
            return tables
        finally:
            connection.close()

    @property
    def tables(self) -> dict[str, list[str]]:
        return {name: list(columns) for name, columns in self._tables.items()}

    def columns(self, table: str) -> list[str]:
        if table not in self._tables:
            raise SemanticError("unknown table", table=table, available=sorted(self._tables))
        return list(self._tables[table])

