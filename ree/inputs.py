from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .errors import REEError
from .models import canonical_bytes
from .storage import Workspace, resolve_workspace_path

INPUT_KINDS = ("documents", "seeds", "legacy")
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise REEError(
                "E_INVALID_JSONL",
                "import",
                f"invalid JSON at {path.as_posix()}:{line_number}: {exc}",
            ) from exc
        if not isinstance(value, dict):
            raise REEError(
                "E_INVALID_JSONL",
                "import",
                f"JSONL record is not an object at {path.as_posix()}:{line_number}",
            )
        records.append(value)
    return records


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    payload = b"".join(canonical_bytes(record) for record in records)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def validate_records(kind: str, records: list[dict[str, Any]], *, source: str) -> None:
    if kind not in INPUT_KINDS:
        raise REEError("E_INVALID_INPUT_KIND", "import", f"unknown input kind: {kind}")
    for index, record in enumerate(records, start=1):
        doc_id = record.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            raise REEError(
                "E_INVALID_JSONL",
                "import",
                f"{kind} record {index} in {source} has no doc_id",
            )
        if kind in {"documents", "legacy"}:
            tokens = record.get("tokens")
            if not isinstance(tokens, list) or not tokens or not all(
                isinstance(token, str) for token in tokens
            ):
                raise REEError(
                    "E_INVALID_JSONL",
                    "import",
                    f"document {doc_id} in {source} has invalid tokens",
                )
            if not isinstance(record.get("text"), str):
                raise REEError(
                    "E_INVALID_JSONL",
                    "import",
                    f"document {doc_id} in {source} has no text",
                )
        if kind == "seeds" and "schema_version" not in record and "annotation" not in record:
            raise REEError(
                "E_INVALID_JSONL",
                "import",
                f"seed annotation {doc_id} in {source} has no schema_version",
            )


class InputCatalog:
    def __init__(self, workspace: Workspace, manifest_path: str = "data/manifest.json") -> None:
        self.workspace = workspace
        self.path = resolve_workspace_path(workspace.root, manifest_path)

    def discover(self) -> dict[str, list[str]]:
        discovered = {
            "documents": sorted(
                path.relative_to(self.workspace.root).as_posix()
                for path in (self.workspace.root / "data" / "documents").glob("*.jsonl")
            ),
            "seeds": sorted(
                path.relative_to(self.workspace.root).as_posix()
                for path in (self.workspace.root / "data" / "seeds").glob("*.jsonl")
            ),
            "legacy": sorted(
                path.relative_to(self.workspace.root).as_posix()
                for path in (self.workspace.root / "data" / "legacy").glob("*.jsonl")
            ),
        }
        legacy_seed_paths = sorted(
            path.relative_to(self.workspace.root).as_posix()
            for path in (self.workspace.root / "data" / "legacy_seeds").glob("*.jsonl")
        )
        discovered["seeds"].extend(legacy_seed_paths)
        return discovered

    def load(self, *, create: bool = False) -> dict[str, Any]:
        if self.path.exists():
            payload = self.workspace.read_json(self.path)
            for kind in INPUT_KINDS:
                payload.setdefault(kind, [])
                payload[kind] = sorted(dict.fromkeys(payload[kind]))
            return payload
        payload = {"schema_version": 1, **self.discover()}
        if create:
            self.workspace.atomic_write_json(self.path, payload)
        return payload

    def save(self, payload: dict[str, Any]) -> None:
        normalized = {"schema_version": 1}
        for kind in INPUT_KINDS:
            normalized[kind] = sorted(dict.fromkeys(payload.get(kind, [])))
        self.workspace.atomic_write_json(self.path, normalized)

    def import_files(self, kind: str, paths: list[str]) -> dict[str, Any]:
        if kind not in INPUT_KINDS:
            raise REEError(
                "E_INVALID_INPUT_KIND",
                "import",
                f"unknown input kind: {kind}",
                details={"allowed_kinds": list(INPUT_KINDS)},
            )
        catalog = self.load(create=True)
        added: list[str] = []
        for supplied in paths:
            source = resolve_workspace_path(self.workspace.root, supplied)
            if not source.exists() or not source.is_file():
                raise REEError(
                    "E_MISSING_INPUT",
                    "import",
                    f"input file does not exist: {supplied}",
                )
            records = read_jsonl(source)
            validate_records(kind, records, source=supplied)
            raw = source.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            safe_name = SAFE_NAME_RE.sub("_", source.name)
            target = (
                self.workspace.root
                / "data"
                / "imported"
                / kind
                / f"{digest[:12]}-{safe_name}"
            )
            self.workspace.atomic_write_bytes(target, raw)
            relative = target.relative_to(self.workspace.root).as_posix()
            if relative not in catalog[kind]:
                catalog[kind].append(relative)
            added.append(relative)
        self.save(catalog)
        return {
            "kind": kind,
            "added": sorted(dict.fromkeys(added)),
            "catalog": self.load(),
        }

    def all_paths(self) -> list[str]:
        catalog = self.load()
        return sorted(
            {
                path
                for kind in INPUT_KINDS
                for path in catalog.get(kind, [])
            }
        )

    def document_paths(self) -> list[str]:
        catalog = self.load()
        return sorted(set(catalog["documents"] + catalog["legacy"]))
