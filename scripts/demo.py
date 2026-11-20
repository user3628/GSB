#!/usr/bin/env python3
"""End-to-end deterministic demonstration.

Sections:
  1. generate deterministic samples and run a full parallel pipeline
  2. prove parallel output equals single-thread output byte for byte
  3. trigger a graceful stop between shards, then resume and compare bytes
  4. inject an illegal budget and out-of-range thresholds -> E_INVALID_CONFIG
  5. exercise E_STALE_CHECKPOINT / E_INVALID_STAGE publication guards
  6. mutate one image, rebase (partial reuse) and rebuild identical stats

The script only uses local files and in-process signals; no network or
external service is required.
"""
from __future__ import annotations

import copy
import json
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detpipe.checkpoint import CheckpointStore
from detpipe.config import PipelineConfig
from detpipe.errors import PipeError
from detpipe.generate import write_dataset
from detpipe.pipeline import Pipeline
from detpipe.validator import report_bytes, validate_run


def heading(text: str) -> None:
    print("\n" + "=" * 72)
    print(text)
    print("=" * 72)


def main() -> int:
    work = Path("run/demo")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    samples_path = work / "samples.json"
    dataset = write_dataset(samples_path)
    print(f"samples: {len(dataset['images'])} images, sources={dataset['sources']}")

    heading("1. full parallel run")
    full_ws = work / "full"
    full = Pipeline(
        dataset, PipelineConfig(workers=3, shard_size=2), full_ws
    ).run()
    full_report = validate_run(full_ws)
    print(json.dumps(full_report["global_statistics"]["totals"], indent=2))
    expected_bytes = report_bytes(full_report)

    heading("2. parallel vs single-thread byte equality")
    seq_ws = work / "sequential"
    Pipeline(dataset, PipelineConfig(workers=1, shard_size=2), seq_ws).run()
    seq_bytes = report_bytes(validate_run(seq_ws))
    print(f"identical={seq_bytes == expected_bytes} bytes={len(expected_bytes)}")
    assert seq_bytes == expected_bytes

    heading("3. stop between shards and resume")
    stop_ws = work / "stop"
    store = CheckpointStore(stop_ws)
    original = Pipeline.process_shard

    def slow(self, plan):
        time.sleep(0.08)
        return original(self, plan)

    Pipeline.process_shard = slow

    def trigger_stop():
        time.sleep(0.06)
        store.request_stop()

    thread = threading.Thread(target=trigger_stop)
    thread.start()
    first = Pipeline(dataset, PipelineConfig(workers=2, shard_size=2), stop_ws).run()
    thread.join()
    Pipeline.process_shard = original
    print(f"phase1 status={first['status']} published={first['shards_published']}/{first['shards_total']}")
    assert first["status"] == "stopped"

    store.clear_stop()
    second = Pipeline(dataset, PipelineConfig(workers=2, shard_size=2), stop_ws).run()
    print(f"phase2 status={second['status']} resumed_from={second['start_index']} published={second['shards_published']}")
    resumed_bytes = report_bytes(validate_run(stop_ws))
    print(f"resumed stats identical to full run={resumed_bytes == expected_bytes}")
    assert resumed_bytes == expected_bytes

    heading("4. invalid budget and out-of-range thresholds")
    for bad in (
        PipelineConfig(max_evaluations=-1),
        PipelineConfig(worker_quota=-5),
        PipelineConfig(iou_threshold=1.7),
        PipelineConfig(score_threshold=-0.1),
    ):
        try:
            bad.validate()
        except PipeError as exc:
            print(f"{exc.code:18s} field={exc.field}")

    heading("5. concurrent publication protection")
    guard_ws = work / "guard"
    pipe = Pipeline(dataset, PipelineConfig(workers=1, shard_size=2), guard_ws)
    pipe.store.write_manifest(pipe.build_manifest())
    r0 = pipe.build_record(pipe.plans[0], pipe.process_shard(pipe.plans[0]), "processed", worker_id=0)
    pipe.store.publish(r0)
    try:
        pipe.store.publish(copy.deepcopy(r0))
    except PipeError as exc:
        print(f"re-publish shard 0 -> {exc.code} (shard_index={exc.shard_index})")
    r2 = pipe.build_record(pipe.plans[2], pipe.process_shard(pipe.plans[2]), "processed", worker_id=0)
    try:
        pipe.store.publish(r2)
    except PipeError as exc:
        print(f"publish shard 2 before 1 -> {exc.code} (shard_index={exc.shard_index})")

    heading("6. sample mutation: invalidate affected shard, reuse the rest")
    rebase_ws = work / "rebase"
    cfg = PipelineConfig(workers=2, shard_size=2)
    Pipeline(dataset, cfg, rebase_ws).run()
    mutated = copy.deepcopy(dataset)
    mutated["images"][4]["candidates"][0]["score"] = 0.246810
    rebase_pipe = Pipeline(mutated, cfg, rebase_ws)
    rebase_info = rebase_pipe.rebase()
    print(json.dumps(rebase_info, indent=2))
    rebase_pipe.run()
    fresh_ws = work / "fresh_mutated"
    Pipeline(mutated, cfg, fresh_ws).run()
    same = report_bytes(validate_run(rebase_ws)) == report_bytes(validate_run(fresh_ws))
    print(f"rebased stats identical to a fresh run={same}")
    assert same

    heading("ALL DEMO CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
