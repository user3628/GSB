from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from slowquery.config import WorkbenchConfig
from slowquery.service import SlowQueryService
from slowquery.verification import verify_rewrite


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def test_duplicate_diagnosis_executes_once_and_reuses_result(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.create_session(session_id="duplicate", sample_id="sq-missing-index")
    barrier = threading.Barrier(8)

    def run():
        barrier.wait(timeout=5)
        return seeded_service.diagnose("duplicate")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: run(), range(8)))

    assert all(result["session"]["status"] == "SUGGESTED" for result in results)
    assert all(len(result["session"]["suggestions"]) == 1 for result in results)
    assert sum(1 for result in results if result["reused"] is False) == 1
    assert sum(1 for result in results if result["reused"] is True) == 7
    assert seeded_service.repo.count_completed_intents("ANALYZE", "duplicate") == 1


def test_concurrent_readers_never_observe_suggested_without_suggestions(
    config: WorkbenchConfig,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def blocking_verifier(**kwargs):
        entered.set()
        if not release.wait(timeout=10):
            raise RuntimeError("test verifier was not released")
        return verify_rewrite(**kwargs)

    service = SlowQueryService(config, verifier=blocking_verifier)
    service.submit_samples(json.loads((DATA_DIR / "samples.json").read_text()))
    service.load_snapshot(json.loads((DATA_DIR / "snapshot_v1.json").read_text()))
    service.create_session(session_id="visibility", sample_id="sq-missing-index")
    service.diagnose("visibility")

    errors: list[str] = []

    def verify_worker():
        return service.verify("visibility")

    def reader_worker():
        for _ in range(100):
            session = service.get_session("visibility")
            if session["status"] == "SUGGESTED" and session["is_slow"]:
                if not session["suggestions"]:
                    errors.append("SUGGESTED visible without suggestions")
            if session["status"] == "CREATED" and session["suggestions"]:
                errors.append("CREATED visible with suggestions")

    with ThreadPoolExecutor(max_workers=9) as executor:
        verify_future = executor.submit(verify_worker)
        assert entered.wait(timeout=5)
        read_futures = [executor.submit(reader_worker) for _ in range(8)]
        for future in read_futures:
            future.result(timeout=5)
        release.set()
        result = verify_future.result(timeout=10)

    assert result["session"]["status"] == "VERIFIED"
    assert errors == []
    trace = service.get_session("visibility")["trace"]
    assert [(event["from_status"], event["to_status"]) for event in trace] == [
        (None, "CREATED"),
        ("CREATED", "ANALYZING"),
        ("ANALYZING", "SUGGESTED"),
        ("SUGGESTED", "ANALYZING"),
        ("ANALYZING", "VERIFIED"),
    ]
