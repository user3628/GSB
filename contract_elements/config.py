from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from .canonical import read_json, sha256_value


DEFAULT_CONFIG_PATH = Path("config/default.json")


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config = read_json(path)
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "normalization",
        "extraction",
        "arbitration",
        "interop",
        "experiment",
        "runtime",
        "service",
    }
    missing = sorted(required.difference(config))
    if missing:
        raise ValueError(f"config missing keys: {', '.join(missing)}")
    if config["schema_version"] != 2:
        raise ValueError("only config schema_version 2 is supported")
    if int(config["normalization"]["max_text_chars"]) < 1:
        raise ValueError("normalization.max_text_chars must be positive")
    for key in ("amount_scale", "ratio_scale"):
        value = int(config["extraction"][key])
        if value < 0 or value > 18:
            raise ValueError(f"extraction.{key} must be in [0, 18]")
    order = config["arbitration"]["order"]
    allowed = {"effective_date", "clause_priority", "clause_no", "unit_index", "source_start", "candidate_id"}
    if not order or not set(order).issubset(allowed):
        raise ValueError("arbitration.order contains unsupported fields")


def normalized_config(config: dict[str, Any]) -> dict[str, Any]:
    return deepcopy(config["normalization"])


def extraction_config(config: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(config["extraction"])
    result["dictionary"] = deepcopy(config.get("dictionary", {}))
    return result


def arbitration_config(config: dict[str, Any]) -> dict[str, Any]:
    return deepcopy(config["arbitration"])


def config_fingerprint(config: dict[str, Any]) -> str:
    return sha256_value(config)


def stage_config_fingerprints(config: dict[str, Any]) -> dict[str, str]:
    return {
        "normalize": sha256_value(normalized_config(config)),
        "extract": sha256_value(extraction_config(config)),
        "arbitrate": sha256_value(arbitration_config(config)),
    }

