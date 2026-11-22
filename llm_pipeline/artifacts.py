"""Content-addressed artifact envelopes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import E_IO, PipelineError
from .util import canonical_json_line, sha256_obj


ARTIFACT_FORMAT = "llm-pipeline-artifact-v1"


def make_artifact(stage: str, payload: dict[str, Any]) -> tuple[str, bytes]:
    digest = sha256_obj(payload)
    envelope = {
        "format": ARTIFACT_FORMAT,
        "stage": stage,
        "artifact_digest": digest,
        "payload": payload,
    }
    return digest, canonical_json_line(envelope)


def read_artifact(path: Path, *, stage: str | None = None) -> dict[str, Any]:
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(
            E_IO,
            "artifact is missing or invalid JSON",
            {"path": str(path), "error": str(exc)},
            http_status=500,
        ) from exc
    if not isinstance(envelope, dict) or envelope.get("format") != ARTIFACT_FORMAT:
        raise PipelineError(
            E_IO,
            "artifact envelope has an unsupported format",
            {"path": str(path)},
            http_status=500,
        )
    if stage is not None and envelope.get("stage") != stage:
        raise PipelineError(
            E_IO,
            "artifact stage does not match checkpoint stage",
            {"path": str(path), "expected": stage, "actual": envelope.get("stage")},
            http_status=500,
        )
    payload = envelope.get("payload")
    digest = sha256_obj(payload)
    if digest != envelope.get("artifact_digest"):
        raise PipelineError(
            E_IO,
            "artifact payload digest mismatch",
            {"path": str(path), "expected": envelope.get("artifact_digest"), "actual": digest},
            http_status=500,
        )
    return payload


def artifact_digest(payload: dict[str, Any]) -> str:
    return sha256_obj(payload)
