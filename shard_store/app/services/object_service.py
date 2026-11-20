"""Read-side object metadata queries (current pointer and history)."""
from __future__ import annotations

from app.config import Settings
from app.db.connection import connect
from app.errors import BusinessError
from app.repositories import ObjectRepository


class ObjectService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def get_current_object(self, object_key: str) -> dict:
        with connect(self.settings) as conn:
            row = ObjectRepository(conn).get_current(object_key)
            if row is None:
                raise BusinessError(
                    "E_OBJECT_NOT_FOUND",
                    f"no committed object for key '{object_key}'",
                    404,
                )
            result = {k: row[k] for k in row.keys()}
            result["visibility"] = "current"
            return result

    def list_versions(self, object_key: str) -> dict:
        with connect(self.settings) as conn:
            repo = ObjectRepository(conn)
            current = repo.get_current(object_key)
            versions = repo.list_versions(object_key)
            if not versions:
                raise BusinessError(
                    "E_OBJECT_NOT_FOUND",
                    f"no committed object for key '{object_key}'",
                    404,
                )
            return {
                "object_key": object_key,
                "current_version": current["version"] if current else None,
                "versions": [
                    {
                        **{k: r[k] for k in r.keys()},
                        "readable_as_current": bool(current and r["version"] == current["version"]),
                    }
                    for r in versions
                ],
            }

    def get_version(self, object_key: str, version: int) -> dict:
        with connect(self.settings) as conn:
            current = ObjectRepository(conn).get_current(object_key)
            row = ObjectRepository(conn).get_version(object_key, version)
            if row is None:
                raise BusinessError("E_OBJECT_NOT_FOUND",
                                    f"version {version} of '{object_key}' not found", 404)
            result = {k: row[k] for k in row.keys()}
            result["readable_as_current"] = bool(current and current["version"] == version)
            return result
