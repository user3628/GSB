from __future__ import annotations

import copy
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION
from .errors import E_VALIDATION, WorkbenchError
from .model import FragmentModel, default_metadata

PROJECT_FORMAT = "image-workbench-project"


def build_project_document(
    fragments: Iterable[FragmentModel],
    *,
    project_metadata: dict[str, Any] | None = None,
    project_name: str = "untitled",
) -> dict[str, Any]:
    metadata = copy.deepcopy(project_metadata) if project_metadata is not None else default_metadata("internal-project")
    metadata.setdefault("schema_version", SCHEMA_VERSION)
    metadata.setdefault("codec_version", "2.0.0")
    metadata.setdefault("workbench_version", "2.0.0")
    return {
        "format": PROJECT_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "project_name": project_name,
        "metadata": metadata,
        "fragments": [fragment.to_internal_dict() for fragment in fragments],
    }


def load_project_document(document: dict[str, Any]) -> tuple[list[FragmentModel], dict[str, Any], str]:
    if not isinstance(document, dict) or document.get("format") != PROJECT_FORMAT:
        raise WorkbenchError(E_VALIDATION, "Internal project format marker is missing or unsupported")
    if int(document.get("schema_version", -1)) != SCHEMA_VERSION:
        raise WorkbenchError(
            E_VALIDATION,
            "Internal project schema version is not supported",
            details={"expected": SCHEMA_VERSION, "actual": document.get("schema_version")},
        )
    raw_fragments = document.get("fragments")
    if not isinstance(raw_fragments, list):
        raise WorkbenchError(E_VALIDATION, "Internal project fragments must be a list")
    fragments = [FragmentModel.from_internal_dict(item) for item in raw_fragments]
    ids = [fragment.fragment_id for fragment in fragments]
    if len(ids) != len(set(ids)):
        raise WorkbenchError(E_VALIDATION, "Internal project contains duplicate fragment identifiers")
    metadata = copy.deepcopy(document.get("metadata", {}))
    if not isinstance(metadata, dict) or not isinstance(metadata.get("schema_version"), int):
        raise WorkbenchError(E_VALIDATION, "Internal project metadata is missing schema_version")
    return fragments, metadata, str(document.get("project_name", "untitled"))


def dump_project_json(fragments: Iterable[FragmentModel], **kwargs: Any) -> str:
    return json.dumps(build_project_document(fragments, **kwargs), ensure_ascii=False, indent=2, allow_nan=False) + "\n"


def save_project(path: str | Path, fragments: Iterable[FragmentModel], **kwargs: Any) -> dict[str, Any]:
    from .atomic import atomic_write_text

    document = build_project_document(fragments, **kwargs)
    atomic_write_text(Path(path), json.dumps(document, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    return document


def load_project(path: str | Path) -> tuple[list[FragmentModel], dict[str, Any], str]:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkbenchError(E_VALIDATION, f"Project file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise WorkbenchError(E_VALIDATION, f"Project file is not valid JSON: {exc}") from exc
    return load_project_document(document)

