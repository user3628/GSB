import json
import multiprocessing as mp
import os

import pytest

from layout_order.config import DEFAULT_CONFIG
from layout_order.engine import PipelineEngine
from layout_order.errors import LayoutError
from layout_order.generator import generate_documents
from layout_order.hashing import canonical_bytes
from layout_order.stages import normalize as normalize_stage


def _full_run_worker(workspace: str, queue: mp.Queue) -> None:
    try:
        engine = PipelineEngine(workspace)
        engine.run(generate_documents()[:2])
        queue.put(("ok", engine.store.list_documents()))
    except LayoutError as exc:
        queue.put(("error", exc.code))


def _publish_worker(workspace: str, doc_id: str, stage: str, fingerprint: str,
                    queue: mp.Queue) -> None:
    try:
        engine = PipelineEngine(workspace)
        document = engine.store.load_input(doc_id)
        product = normalize_stage.run(document, DEFAULT_CONFIG)
        engine.store.publish(
            doc_id, stage, fingerprint, DEFAULT_CONFIG["config_version"],
            fingerprint, product,
        )
        queue.put(("ok", fingerprint))
    except LayoutError as exc:
        queue.put(("error", exc.code))
    except Exception as exc:  # pragma: no cover
        queue.put(("error", f"{type(exc).__name__}:{exc}"))


def test_concurrent_publishers_only_one_accepted(tmp_path):
    documents = generate_documents()
    target = documents[0]
    engine = PipelineEngine(str(tmp_path))
    engine.store.save_input(target)

    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    processes = [
        ctx.Process(target=_publish_worker,
                    args=(str(tmp_path), target["doc_id"], "normalize",
                          f"{idx:016d}", queue))
        for idx in range(4)
    ]
    for proc in processes:
        proc.start()
    outcomes = [queue.get() for _ in processes]
    for proc in processes:
        proc.join()

    accepted = [item for item in outcomes if item[0] == "ok"]
    rejected = [item for item in outcomes if item[0] == "error"]
    assert len(accepted) == 1
    assert all(code == "E_STALE_CHECKPOINT" for _, code in rejected)
    record = engine.store.load_stage(target["doc_id"], "normalize")
    assert record is not None and record["seq"] == 1
    assert record["stage_fingerprint"] == accepted[0][1]


def test_concurrent_full_runs_converge(tmp_path):
    documents = generate_documents()[:2]
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    processes = [
        ctx.Process(target=_full_run_worker, args=(str(tmp_path), queue))
        for _ in range(2)
    ]
    for proc in processes:
        proc.start()
    outcomes = [queue.get() for _ in processes]
    for proc in processes:
        proc.join(timeout=60)
        assert proc.exitcode == 0
    assert all(outcome[0] == "ok" for outcome in outcomes)

    engine = PipelineEngine(str(tmp_path))
    for doc in documents:
        record = engine.store.load_stage(doc["doc_id"], "summary")
        assert record is not None
