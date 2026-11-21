"""Deterministic in-repo sample generation.

A fixed seed and fixed base timestamps make every generated run, metric value
and artifact digest reproducible. Samples deliberately cover:

* an omitted/optional parameter block (for jsonl downgrade back-fill);
* mixed integer/float metric precision;
* high, equal and pre-release toolchain versions;
* a known CRC tamper point for corruption tests.
"""

from __future__ import annotations

import hashlib
import os
import zlib
from typing import Dict, List

import numpy as np

from .models import Artifact, MetricPoint, Run
from .timeutil import retention_to_expiry

SEED = 20261122
BASE_TIME_US = 1_700_000_000_000_000  # fixed deterministic anchor (us)
SAMPLE_VERSIONS = ["2.1.0-rc3", "2.1.0", "2.2.0", "1.9.4"]


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def build_sample_runs() -> List[Run]:
    rng = np.random.default_rng(SEED)
    runs: List[Run] = []
    experiments = ["vision", "tabular"]
    for index in range(6):
        run_id = f"run-{index:03d}"
        experiment = experiments[index % 2]
        version = SAMPLE_VERSIONS[index % len(SAMPLE_VERSIONS)]
        steps = 5
        metrics: List[MetricPoint] = []
        for step in range(steps):
            # Mixed precision: integer-valued points plus fractional values,
            # and timestamps that straddle the millisecond grid.
            value = float(rng.integers(0, 100)) + round(float(rng.random()), 4)
            if step % 2 == 0:
                value = float(int(value))
            time_us = BASE_TIME_US + index * 1_000_000 + step * 250_000 + (step * 333)
            metrics.append(MetricPoint(step=step, value=value, time_us=time_us))
        params: Dict[str, object] = {
            "learning_rate": round(float(rng.random()) * 0.1 + 0.001, 5),
            "batch_size": int(16 * (index + 1)),
            "max_epochs": 10,
        }
        if index == 3:
            # This run is exported without params to exercise default back-fill.
            params = {}
        artifacts = [
            Artifact(
                name=f"model-{index}.pt",
                size_bytes=1000 + index * 137,
                digest=_digest(f"artifact-{index}"),
            )
        ]
        base_time_us = BASE_TIME_US + index * 1_000_000
        retention_seconds = 86_400.0 + index * 60.0
        run = Run(
            run_id=run_id,
            experiment=experiment,
            params=params,
            metrics=metrics,
            toolchain_version=version,
            last_write_us=BASE_TIME_US + index * 10_000,
            base_time_us=base_time_us,
            retention_seconds=retention_seconds,
            expires_at_us=retention_to_expiry(base_time_us, retention_seconds),
            artifacts=artifacts,
        )
        runs.append(run)
    return runs


def generate_sample_files(directory: str) -> Dict[str, str]:
    """Write deterministic binary + jsonl shards and return their paths."""
    from . import binary, jsonl_fmt

    os.makedirs(directory, exist_ok=True)
    runs = build_sample_runs()
    paths = {
        "binary_v2": os.path.join(directory, "samples_v2.etrk"),
        "binary_v1": os.path.join(directory, "samples_v1.etrk"),
        "jsonl_v2": os.path.join(directory, "samples_v2.jsonl"),
        "jsonl_v1_omit_params": os.path.join(directory, "samples_v1.jsonl"),
    }
    binary.encode_file(runs, paths["binary_v2"], version=binary.BIN_V2, endian="little")
    binary.encode_file(runs, paths["binary_v1"], version=binary.BIN_V1, endian="big")
    jsonl_fmt.encode_file(runs, paths["jsonl_v2"], version=jsonl_fmt.JSON_V2)
    jsonl_fmt.encode_file(runs, paths["jsonl_v1_omit_params"],
                          version=jsonl_fmt.JSON_V1, omit_params=True)
    return paths


def tamper_binary_crc(path: str, target_record: int = 1, output_path: str | None = None) -> str:
    """Flip one payload byte of a chosen record to break its CRC32.

    The length prefix stays valid, isolating the failure to the CRC check.
    """
    output_path = output_path or path
    with open(path, "rb") as handle:
        blob = bytearray(handle.read())
    marker = blob[5]
    char = "<" if marker == 1 else ">"
    import struct

    pos = 6
    record = 0
    while pos < len(blob):
        length = struct.unpack(char + "I", blob[pos:pos + 4])[0]
        if length == 0:
            break
        payload_start = pos + 4
        if record == target_record:
            blob[payload_start] = blob[payload_start] ^ 0xFF
            break
        pos = payload_start + length + 4
        record += 1
    else:
        raise IndexError(f"record {target_record} not found in {path}")
    with open(output_path, "wb") as handle:
        handle.write(blob)
    return output_path
