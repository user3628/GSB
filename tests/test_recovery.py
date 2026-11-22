from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

from rca_pipeline.config import get_settings
from rca_pipeline.service import Pipeline
from tests.conftest import submit_topology_v1

ROOT = Path(__file__).resolve().parent.parent

WORKER = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, %(root)r)
from rca_pipeline.config import get_settings
from rca_pipeline.service import Pipeline

pipeline = Pipeline(get_settings(%(db)r))
pipeline.inject_fault("crash_after_intent")
payload = json.loads(Path(%(payload)r).read_text())
try:
    pipeline.submit_samples(payload)
except SystemExit:
    pass
# If we ever get here the fault did not fire; signal failure explicitly.
os._exit(1)
"""


def _run_crash_worker(db_path: str, payload_file: Path) -> None:
    """Spawn an isolated process; its os._exit must not touch the test runner."""
    code = WORKER % {"root": str(ROOT), "db": db_path, "payload": str(payload_file)}
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        start_new_session=True,
    )
    # SIGKILL surfaces as -9 in Python's subprocess API (137 in shell terms).
    assert completed.returncode in (-9, 137), completed.stderr


def test_recovery_after_hard_kill(tmp_path, topo_v1, dataset):
    db_path = tmp_path / "crash.db"
    pipeline = Pipeline(get_settings(db_path))
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples(dataset[0])
    version_before = pipeline.diagnostic_report()["versions"]["sample_version"]
    assert version_before == 1

    payload_file = tmp_path / "crash_payload.json"
    payload_file.write_text(json.dumps(dataset[1]))
    _run_crash_worker(str(db_path), payload_file)

    restarted = Pipeline(get_settings(db_path))
    report = restarted.run_startup_recovery()
    assert report["pending_intents_found"] >= 1
    assert report["actions"][0]["action"] == "aborted_unfinished_intent"

    versions = restarted.diagnostic_report()["versions"]
    assert versions["sample_version"] == version_before

    quantiles = restarted.query_quantiles(service="payment")
    assert quantiles["entries"], "derived index rebuilt after recovery"
    entry = quantiles["entries"][0]
    assert entry["version_range"]["hi"] == version_before

    recovery = restarted.recovery_report()
    assert recovery["pending_intents"] == 0
    assert any(event["action"] == "derived_rebuild" for event in recovery["events"])

    again = restarted.submit_samples(dataset[1])
    assert again["new_version"] == 2
