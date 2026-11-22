from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from diaglab.errors import BusinessError
from diaglab.formats import decode_bundle
from diaglab.models import NegotiationReport
from diaglab.normalize import normalize_bundle
from diaglab.store import WorkbenchStore, make_batch_fingerprint


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def prepared_import():
    bundle = decode_bundle((SAMPLES / "binary_v2.bin").read_bytes())
    normalized = normalize_bundle(bundle)
    return bundle, normalized, make_batch_fingerprint("import", {"sample": "binary_v2"})


def initialize_store(path: Path) -> WorkbenchStore:
    store = WorkbenchStore(path)
    store.initialize()
    return store


def test_import_is_idempotent_under_concurrent_duplicate_submissions(tmp_path: Path):
    store = initialize_store(tmp_path / "idempotent.sqlite3")
    bundle, normalized, fingerprint = prepared_import()

    def submit() -> dict:
        return store.import_batch(
            batch_id="same-batch",
            fingerprint=fingerprint,
            records=normalized.records,
            negotiation=bundle.report,
            normalization_report=normalized.report,
            source={"name": "binary_v2.bin"},
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: submit(), range(4)))
    assert len({result["commit"]["version"] for result in results}) == 1
    assert sum(1 for result in results if not result["reused"]) == 1
    assert store.overview()["record_count"] == 21
    batch = store.get_batch("same-batch")
    assert batch["status"] == "complete"


def test_same_batch_with_different_content_conflicts(tmp_path: Path):
    store = initialize_store(tmp_path / "conflict.sqlite3")
    bundle, normalized, fingerprint = prepared_import()
    store.import_batch(
        batch_id="conflict-batch",
        fingerprint=fingerprint,
        records=normalized.records,
        negotiation=bundle.report,
        normalization_report=normalized.report,
        source={"name": "binary_v2.bin"},
    )
    with pytest.raises(BusinessError) as caught:
        store.import_batch(
            batch_id="conflict-batch",
            fingerprint="different-content",
            records=normalized.records[:1],
            negotiation=bundle.report,
            normalization_report=normalized.report,
            source={"name": "other"},
        )
    assert caught.value.code == "E_CONFLICTING_BATCH"
    assert store.overview()["record_count"] == 21


def test_snapshot_reads_and_materialized_stats_share_complete_version(tmp_path: Path):
    store = initialize_store(tmp_path / "snapshot.sqlite3")
    bundle, normalized, fingerprint = prepared_import()
    result = store.import_batch(
        batch_id="snapshot-batch",
        fingerprint=fingerprint,
        records=normalized.records,
        negotiation=bundle.report,
        normalization_report=normalized.report,
        source={"name": "binary_v2.bin"},
    )
    version = result["commit"]["version"]
    overview = store.overview()
    materialized = store.stats_snapshot()
    stats = store.stats("local-edge-a")
    assert overview["snapshot_version"] == version
    assert overview["record_count"] == 21
    assert materialized["snapshot"]["record_count"] == 21
    assert stats["snapshot_version"] == version
    assert stats["loss"]["percent"] == "12.50"


def test_insufficient_samples_through_store_is_business_error(tmp_path: Path):
    store = initialize_store(tmp_path / "sparse.sqlite3")
    bundle, normalized, fingerprint = prepared_import()
    store.import_batch(
        batch_id="sparse",
        fingerprint=fingerprint,
        records=normalized.records,
        negotiation=bundle.report,
        normalization_report=normalized.report,
        source={"name": "binary_v2.bin"},
    )
    with pytest.raises(BusinessError) as caught:
        store.stats("local-edge-a", min_samples=8)
    assert caught.value.code == "E_INSUFFICIENT_SAMPLES"
    assert caught.value.details["required_samples"] == 8
    assert caught.value.details["actual_samples"] == 7


def test_force_kill_recovers_to_last_complete_version(tmp_path: Path):
    db_path = tmp_path / "crash.sqlite3"
    marker = tmp_path / "crash.marker"
    store = initialize_store(db_path)
    bundle, normalized, fingerprint = prepared_import()
    first = store.import_batch(
        batch_id="before-crash",
        fingerprint=fingerprint,
        records=normalized.records,
        negotiation=bundle.report,
        normalization_report=normalized.report,
        source={"name": "binary_v2.bin"},
    )
    version_before = first["commit"]["version"]
    records_before = store.overview()["record_count"]

    process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "crash_worker.py"),
            "--db",
            str(db_path),
            "--batch-id",
            "crash-batch",
            "--marker",
            str(marker),
            "--records",
            "3",
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    try:
        deadline = time.time() + 15
        while not marker.exists() and time.time() < deadline:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(f"crash worker exited early: {stdout}\n{stderr}")
            time.sleep(0.05)
        assert marker.exists(), "crash worker did not publish marker"
        # A concurrent reader must stay on the last complete commit while the
        # crash worker holds uncommitted record frames in WAL.
        during_crash = store.overview()
        assert during_crash["snapshot_version"] == version_before
        assert during_crash["record_count"] == records_before
        assert during_crash["recovery_pending_intents"] == 1
        assert "crash-path" not in {row["path_node"] for row in during_crash["paths"]}
        process.kill()
        process.wait(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    recovered = initialize_store(db_path)
    assert recovered.startup_recovery_count == 1
    assert recovered.overview()["record_count"] == records_before
    assert recovered.overview()["snapshot_version"] == version_before
    report = recovered.recovery_report()
    assert report["latest_complete_version"] == version_before
    assert report["pending_intents_now"] == 0
    assert report["difference_report"][0]["planned_records"] == 3
    assert report["difference_report"][0]["committed_records"] == 0
    assert report["difference_report"][0]["difference_records"] == -3
    assert report["batch_status_counts"]["aborted"] == 1
    aborted = recovered.get_batch("crash-batch")
    assert aborted["status"] == "aborted"
    assert aborted["result"]["code"] == "E_BATCH_ABORTED"
    aborted_fingerprint = aborted["fingerprint"]
    with pytest.raises(BusinessError) as caught:
        store.import_batch(
            batch_id="crash-batch",
            fingerprint=aborted_fingerprint,
            records=normalized.records,
            negotiation=bundle.report,
            normalization_report=normalized.report,
            source={"name": "retry"},
        )
    assert caught.value.code == "E_BATCH_ABORTED"

