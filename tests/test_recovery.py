from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.errors import SimulatedForcedTermination
from app.service import TieringService

from .conftest import SAMPLES


def _layer_map(occupancy):
    return {
        layer["layer_id"]: (layer["used_bytes"], layer["shard_count"])
        for layer in occupancy["layers"]
    }


@pytest.mark.parametrize("point", ["after_intent", "after_transfer_before_commit"])
def test_forced_termination_rolls_back_uncommitted_batch(tmp_path: Path, point: str):
    db = tmp_path / "tiering.db"
    first = TieringService(db, samples_dir=SAMPLES)
    first.startup()
    before_occupancy = first.get_occupancy()
    before_layers = _layer_map(before_occupancy)
    first.arm_fault(point=point, mode="raise", batch_id="crash-batch")
    with pytest.raises(SimulatedForcedTermination):
        first.execute_batch("crash-batch", 1)

    live_batch = first.get_batch("crash-batch")
    assert live_batch["state"] == "MIGRATING"

    recovered = TieringService(db, samples_dir=SAMPLES, auto_seed=False)
    recovered.startup()
    batch = recovered.get_batch("crash-batch")
    assert batch["state"] == "ABORTED"
    assert all(item["state"] == "ABORTED" for item in batch["items"])
    assert batch["conclusion_matches_items"] is True
    assert _layer_map(recovered.get_occupancy()) == before_layers

    with recovered.db.read() as conn:
        intents = conn.execute(
            "SELECT state FROM migration_intents WHERE batch_id='crash-batch'"
        ).fetchall()
    assert intents and all(row["state"] == "ROLLED_BACK" for row in intents)

    report = recovered.get_recovery_report()
    assert report["forced_recovery_available"] is True
    recovery = report["latest_startup_recovery"]
    assert recovery["forced_termination_detected"] is True
    assert recovery["safe_exit_summary_available"] is False
    assert recovery["persisted_only"] is True
    assert recovery["unfinished_batch_count"] == 1
    assert recovery["recovered_batches"][0]["batch_id"] == "crash-batch"
    assert recovery["occupancy_difference"]
    assert all(item["used_bytes_delta"] == 0 for item in recovery["occupancy_difference"])
    recovered.safe_shutdown("pytest_teardown")


def test_committed_batch_survives_post_commit_termination(tmp_path: Path):
    db = tmp_path / "tiering.db"
    first = TieringService(db, samples_dir=SAMPLES)
    first.startup()
    first.arm_fault(point="after_commit_before_response", mode="raise", batch_id="committed")
    with pytest.raises(SimulatedForcedTermination):
        first.execute_batch("committed", 1)

    recovered = TieringService(db, samples_dir=SAMPLES, auto_seed=False)
    recovered.startup()
    batch = recovered.get_batch("committed")
    assert batch["state"] == "COMMITTED"
    assert batch["result"]["moved_count"] == 9
    report = recovered.get_recovery_report()
    assert report["latest_startup_recovery"]["unfinished_batch_count"] == 0
    replay = recovered.execute_batch("committed", 1)
    assert replay["replayed"] is True
    assert replay["result"]["moved_count"] == 9
    recovered.safe_shutdown("pytest_teardown")
