"""Reusable demonstrations for every structured workbench error code."""

from __future__ import annotations

import tempfile
from typing import Any, Callable

import numpy as np

from .batch import BatchAccumulator
from .checkpoints import CheckpointStore
from .data import Dataset, compare_column_schemas
from .errors import WorkbenchError
from .workbench import Workbench


def _capture(label: str, action: Callable[[], None]) -> dict[str, Any]:
    try:
        action()
    except WorkbenchError as exc:
        return {"scenario": label, "raised": exc.code, "error": exc.to_dict()["error"]}
    return {"scenario": label, "raised": None, "unexpected": "no error raised"}


def collect_error_demonstrations(dataset: Dataset) -> list[dict[str, Any]]:
    demonstrations: list[dict[str, Any]] = []

    def numeric_case() -> None:
        workbench = Workbench(
            dataset,
            {"bandwidth": {"rule": "fixed", "value": 0}},
            tempfile.mkdtemp(prefix="kde-error-numeric-"),
        )
        workbench.run()

    demonstrations.append(_capture("zero fixed bandwidth", numeric_case))

    def mismatch_case() -> None:
        compare_column_schemas(
            ["a", "b", "c"],
            ["a", "b", "d"],
            (10, 3),
            (10, 3),
        )

    demonstrations.append(_capture("column schema mismatch", mismatch_case))

    def batch_case() -> None:
        accumulator = BatchAccumulator(expected_columns=3, column_names=["a", "b", "c"])
        accumulator.add_batch(np.empty((0, 3), dtype=np.float64))

    demonstrations.append(_capture("empty batch", batch_case))

    def dimension_case() -> None:
        accumulator = BatchAccumulator(expected_columns=3, column_names=["a", "b", "c"])
        accumulator.add_batch(np.ones((4, 2), dtype=np.float64))

    demonstrations.append(_capture("batch feature-dimension mismatch", dimension_case))

    workdir = tempfile.mkdtemp(prefix="kde-error-checkpoint-")
    store = CheckpointStore(workdir)

    def publish_bandwidth(revision: int) -> None:
        store.publish(
            stage="bandwidth",
            payload={"stage": "bandwidth", "columns": [], "diagnostics": {}},
            expected_previous_revision=revision,
            config_version="demo-config",
            stage_input_fingerprint="demo-input",
            dataset_fingerprint="demo-dataset",
            stage_config={},
            upstream={},
            accumulator={"kind": "demo"},
        )

    publish_bandwidth(0)
    demonstrations.append(_capture("duplicate checkpoint publication", lambda: publish_bandwidth(0)))

    def invalid_stage_case() -> None:
        fresh = CheckpointStore(tempfile.mkdtemp(prefix="kde-error-stage-"))
        fresh.publish(
            stage="density",
            payload={"stage": "density", "columns": []},
            expected_previous_revision=0,
            config_version="demo-config",
            stage_input_fingerprint="demo-input",
            dataset_fingerprint="demo-dataset",
            stage_config={},
            upstream={"bandwidth": "missing-upstream"},
            accumulator={"kind": "demo"},
        )

    demonstrations.append(_capture("missing prerequisite stage", invalid_stage_case))
    return demonstrations
