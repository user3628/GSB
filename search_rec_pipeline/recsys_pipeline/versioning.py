"""Per-stage dependency fingerprints used for reuse and invalidation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import STAGES, Config
from .util import hash_json


@dataclass(frozen=True)
class StageVersion:
    stage: str
    input_fingerprint: str
    config_fingerprint: str
    stage_fingerprint: str


def declaration_fingerprint_view(declaration: dict[str, Any]) -> dict[str, Any]:
    return {
        "scale": declaration.get("scale"),
        "sample_count": declaration.get("sample_count"),
        "feature_cardinality": declaration.get("feature_cardinality"),
        "cross_limit": declaration.get("cross_limit"),
        "budget_units": declaration.get("budget_units"),
    }


def expected_stage_versions(
    config: Config,
    scale: str,
    declaration: dict[str, Any],
    sample_input_fingerprint: str,
) -> dict[str, StageVersion]:
    """Return fingerprints in stage order, chaining each stage to its upstream stage."""

    versions: dict[str, StageVersion] = {}
    upstream_input = sample_input_fingerprint
    for stage in STAGES:
        slice_data = config.stage_config_slice(stage)
        if stage == "decision":
            slice_data = {
                "stage_config": slice_data,
                "declaration": declaration_fingerprint_view(declaration),
            }
        config_fingerprint = hash_json(slice_data)
        stage_fingerprint = hash_json(
            {
                "stage": stage,
                "scale": scale,
                "input_fingerprint": upstream_input,
                "config_fingerprint": config_fingerprint,
            }
        )
        versions[stage] = StageVersion(
            stage=stage,
            input_fingerprint=upstream_input,
            config_fingerprint=config_fingerprint,
            stage_fingerprint=stage_fingerprint,
        )
        upstream_input = stage_fingerprint
    return versions


def stage_change_reason(stage: str, input_changed: bool, config_changed: bool) -> str:
    if input_changed:
        if stage == "normalize":
            return "sample_content_changed"
        return "upstream_stage_changed"
    if config_changed:
        return f"{stage}_config_changed"
    return "stage_changed"
