"""Load only local deterministic JSON samples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import E_SPEC_INVALID, SchedError

_DIRECT_SAMPLES = {
    "boundary_valid": "spec_boundary_valid.json",
    "boundary_invalid": "spec_boundary_invalid.json",
}


class SampleRepository:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()

    def _read_json(self, name: str) -> Any:
        path = self.root / name
        if not path.is_file():
            raise SchedError(
                E_SPEC_INVALID,
                "local sample file was not found",
                {"sample": name, "path": str(path)},
                http_status=422,
            )
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except json.JSONDecodeError as exc:
            raise SchedError(
                E_SPEC_INVALID,
                "local sample file is not valid JSON",
                {"sample": name, "path": str(path), "line": exc.lineno, "column": exc.colno},
                http_status=422,
            ) from exc

    def load(self, name: str = "default", *, batch_id: str | None = None) -> dict[str, Any]:
        normalized = name.strip().lower().replace("-", "_")
        if normalized in _DIRECT_SAMPLES:
            payload = self._read_json(_DIRECT_SAMPLES[normalized])
            if not isinstance(payload, dict):
                raise SchedError(
                    E_SPEC_INVALID,
                    "sample payload must be a JSON object",
                    {"sample": name},
                    http_status=422,
                )
        elif normalized == "default":
            nodes = self._read_json("nodes.json")
            pods = self._read_json("pods.json")
            budget = self._read_json("budget.json")
            payload = {
                "batch_id": "sample-default",
                "nodes": nodes,
                "pods": pods,
                "budget": budget,
            }
        else:
            raise SchedError(
                E_SPEC_INVALID,
                "unknown local sample",
                {
                    "sample": name,
                    "available": ["default", "boundary_valid", "boundary_invalid"],
                },
                http_status=422,
            )
        result = dict(payload)
        if batch_id is not None:
            result["batch_id"] = batch_id
        return result

