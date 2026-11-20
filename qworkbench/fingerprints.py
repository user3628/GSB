from __future__ import annotations

from typing import Any

from . import ALGORITHM_VERSION
from .config import WorkbenchConfig
from .samples import SampleDataset
from .util import sha256_json


STAGES = ("normalize", "score", "allocate", "mix")


def compute_stage_fingerprints(
    dataset: SampleDataset,
    config: WorkbenchConfig,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    upstream = dataset.sample_fingerprint
    for stage in STAGES:
        config_fingerprint = config.stage_config_fingerprint(stage)
        input_fingerprint = upstream
        if stage == "mix":
            input_fingerprint = sha256_json(
                {
                    "upstream_stage_fingerprint": upstream,
                    "annotation_fingerprint": dataset.annotation_fingerprint,
                }
            )
        fingerprint = sha256_json(
            {
                "algorithm_version": ALGORITHM_VERSION,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
            }
        )
        result[stage] = {
            "stage": stage,
            "fingerprint": fingerprint,
            "input_fingerprint": input_fingerprint,
            "config_fingerprint": config_fingerprint,
            "upstream_fingerprint": upstream,
            "config_version": config.version,
            "algorithm_version": ALGORITHM_VERSION,
        }
        upstream = fingerprint
    return result


def stage_fingerprint_only(dataset: SampleDataset, config: WorkbenchConfig) -> dict[str, str]:
    return {
        stage: details["fingerprint"]
        for stage, details in compute_stage_fingerprints(dataset, config).items()
    }
