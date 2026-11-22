"""Deterministic configuration loading and validation."""
from __future__ import annotations

import os
from typing import Any, Dict

from .errors import E_CONFIG_INVALID, WorkbenchError
from .yaml_lite import load as yaml_load

_REQUIRED_PATHS = (
    ("retrieval", "depth"),
    ("retrieval", "evidence_limit"),
    ("retrieval", "alignment_tolerance"),
    ("retrieval", "channel_prior"),
    ("retrieval", "weights"),
    ("answer", "template"),
    ("answer", "required_slots"),
    ("scale_tiers",),
    ("budget", "global_evidence_evaluations"),
)


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    missing = []
    for path in _REQUIRED_PATHS:
        node: Any = config
        for key in path:
            if not isinstance(node, dict) or key not in node:
                missing.append(".".join(path))
                break
            node = node[key]
    if missing:
        raise WorkbenchError(
            E_CONFIG_INVALID,
            "configuration is missing required sections",
            stage="configuration",
            context={"missing": missing})
    budget = config["budget"]["global_evidence_evaluations"]
    if not isinstance(budget, int) or budget <= 0:
        raise WorkbenchError(
            E_CONFIG_INVALID,
            "global_evidence_evaluations must be a positive integer",
            stage="configuration",
            context={"global_evidence_evaluations": budget})
    return config


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise WorkbenchError(
            E_CONFIG_INVALID, f"config file not found: {path}",
            stage="configuration")
    return validate_config(yaml_load(path))
