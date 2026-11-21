from __future__ import annotations

from pathlib import Path

import pytest

from app.errors import WorkbenchError
from app.store import Store


def test_snapshot_seq_must_be_monotonic(service, samples_dir):
    service.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())
    with pytest.raises(WorkbenchError) as exc:
        service.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())
    assert exc.value.code == "E_SNAPSHOT_INVALID"


def test_recovery_after_hard_termination(tmp_path, samples_dir):
    db_path = tmp_path / "crash.sqlite3"
    marker = tmp_path / "crash.marker"
    store = Store(db_path)
    service = type("S", (), {})()  # placeholder, real service built below
    from app.service import OrchestrationService

    svc = OrchestrationService(store)
    svc.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())

    pid = Path(__file__).resolve().parent
    import os, sys

    child = os.fork()
    if child == 0:
        try:
            child_store = Store(db_path)
            child_svc = OrchestrationService(child_store)
            child_svc.crash_during_import(
                (samples_dir / "snapshot_v2.jsonl").read_bytes(), str(marker)
            )
        finally:
            os._exit(0)
    _, status = os.waitpid(child, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 99

    restarted = Store(db_path)
    report = restarted.recover()
    assert report.active_snapshot_seq == 1
    assert report.aborted_intents and report.aborted_intents[0].status == "aborted"
    stats, _, _ = restarted.load_active()
    assert stats.snapshot_seq == 1
    restarted.close()


def test_concurrent_increment_only_one_wins(service, samples_dir):
    import threading

    service.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())
    errors = []

    def worker(submit_id: str, resource_id: str):
        from app.models import IncrementFragment, RawRecord

        fragment = IncrementFragment(
            base_snapshot_seq=1,
            submit_id=submit_id,
            changes=[RawRecord(resource_id=resource_id, parents=["svc-gateway"], budget=1, weight_float=0.2)],
        )
        try:
            service.apply_increment(fragment)
        except WorkbenchError as exc:
            errors.append(exc.code)

    threads = [threading.Thread(target=worker, args=(f"s-{i}", f"svc-{i}")) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors.count("E_STALE_BASE") == 1
