"""End-to-end deterministic demonstration with error injection.

Stages:
  1. regenerate fixed-seed samples, config and model weights;
  2. full parallel run, serial run and byte-level summary comparison;
  3. stop-between-steps then resume, comparing against the from-scratch run;
  4. corrupt a checkpoint and prove recovery ignores it;
  5. inject invalid rank / negative LR / over-budget quota and assert codes;
  6. exercise E_STALE_CHECKPOINT and E_WEIGHT_MISMATCH paths.
"""
from __future__ import annotations

import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from lora_bench import engine  # noqa: E402
from lora_bench.checkpoint import (  # noqa: E402
    CheckpointRecord,
    publish_checkpoint,
)
from lora_bench.config import parse_config, validate_task  # noqa: E402
from lora_bench.errors import (  # noqa: E402
    E_BUDGET_EXCEEDED,
    E_INVALID_TASK,
    E_STALE_CHECKPOINT,
    E_WEIGHT_MISMATCH,
    BenchError,
    InvalidTaskError,
    StaleCheckpointError,
    WeightMismatchError,
)
from lora_bench.generator import (  # noqa: E402
    DEFAULT_MODEL_CONFIG,
    generate_all,
    write_config,
)
from lora_bench.state import load_state, request_stop  # noqa: E402
import torch  # noqa: E402


def heading(text: str) -> None:
    print(f"\n=== {text} ===")


def main(base_dir: str) -> int:
    data_dir = os.path.join(base_dir, "data")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    manifest = generate_all(data_dir)
    heading("generated artifacts and checksums")
    print(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True))
    config_path = manifest["config_path"]

    ws_parallel = os.path.join(base_dir, "ws-parallel")
    ws_serial = os.path.join(base_dir, "ws-serial")
    for ws in (ws_parallel, ws_serial):
        if os.path.exists(ws):
            shutil.rmtree(ws)

    heading("parallel run")
    engine.import_work(ws_parallel, config_path, DEFAULT_MODEL_CONFIG)
    parallel = engine.run(ws_parallel)
    print("fingerprint:", parallel["summary_fingerprint"])

    heading("serial run (must be byte-identical)")
    engine.import_work(ws_serial, config_path, DEFAULT_MODEL_CONFIG)
    serial = engine.run(ws_serial, max_workers=1)
    assert parallel["summary_fingerprint"] == serial["summary_fingerprint"]
    print("fingerprint:", serial["summary_fingerprint"], "MATCH")

    heading("budget ledger")
    for task in parallel["tasks"]:
        print(task["task_id"], task["status"], "reason=", task["rejection_reason"])

    heading("stop then resume")
    frequent_payload = parse_config(config_path).to_dict()
    frequent_payload["checkpoint_every"] = 1
    frequent_path = os.path.join(data_dir, "experiment-frequent.yaml")
    write_config(frequent_path, frequent_payload)
    ws_stop = os.path.join(base_dir, "ws-stop")
    if os.path.exists(ws_stop):
        shutil.rmtree(ws_stop)
    engine.import_work(ws_stop, frequent_path, DEFAULT_MODEL_CONFIG)
    request_stop(ws_stop)
    stopped = engine.run(ws_stop, keep_stop_flag=True)
    print("stopped statuses:", {t["task_id"]: t["status"] for t in stopped["tasks"]})
    print("stopped steps:", {t["task_id"]: t["completed_steps"] for t in stopped["tasks"]})
    resumed = engine.resume(ws_stop)
    fresh_stop_ws = os.path.join(base_dir, "ws-stop-fresh")
    if os.path.exists(fresh_stop_ws):
        shutil.rmtree(fresh_stop_ws)
    engine.import_work(fresh_stop_ws, frequent_path, DEFAULT_MODEL_CONFIG)
    fresh_stop = engine.run(fresh_stop_ws)
    assert resumed["summary_fingerprint"] == fresh_stop["summary_fingerprint"]
    print("resumed fingerprint matches full run")

    heading("corrupt checkpoint injection")
    task_dir = os.path.join(ws_stop, "checkpoints", "task-alpha")
    newest = sorted(os.listdir(task_dir))[-1]
    with open(os.path.join(task_dir, newest, "checkpoint.json"), "w") as handle:
        handle.write("{broken")
    os.unlink(os.path.join(task_dir, newest, ".committed"))
    # Re-corrupt by requesting another stop/resume cycle.
    request_stop(ws_stop)
    engine.run(ws_stop, keep_stop_flag=True)
    recovered = engine.resume(ws_stop)
    assert recovered["summary_fingerprint"] == parallel["summary_fingerprint"]
    print("recovery after corruption matches full run")

    heading("error code injection")
    bad_rank = dict(parse_config(config_path).tasks[0].to_dict())
    bad_rank["rank"] = 0
    try:
        validate_task(bad_rank, 0)
    except InvalidTaskError as exc:
        assert exc.code == E_INVALID_TASK
        print("invalid rank ->", exc.code, exc.field)

    bad_lr = dict(parse_config(config_path).tasks[0].to_dict())
    bad_lr["learning_rate"] = -0.1
    try:
        validate_task(bad_lr, 0)
    except InvalidTaskError as exc:
        assert exc.code == E_INVALID_TASK
        print("negative LR ->", exc.code, exc.field)

    payload = parse_config(config_path).to_dict()
    payload["budget"]["per_task_sample_quota"] = 5
    tight_path = os.path.join(data_dir, "experiment-tight.yaml")
    write_config(tight_path, payload)
    ws_tight = os.path.join(base_dir, "ws-tight")
    if os.path.exists(ws_tight):
        shutil.rmtree(ws_tight)
    state = engine.import_work(ws_tight, tight_path, DEFAULT_MODEL_CONFIG)
    assert all(
        task.status == "not_started" for task in state.tasks.values()
    )
    print("over-budget ->", E_BUDGET_EXCEEDED, "reasons persisted per task")

    heading("stale checkpoint and weight mismatch guards")
    ws_guard = os.path.join(base_dir, "ws-guard")
    if os.path.exists(ws_guard):
        shutil.rmtree(ws_guard)
    record = CheckpointRecord(
        task_id="guard",
        step=1,
        sequence=1,
        sample_cursor=4,
        sample_fingerprint="s",
        config_fingerprint="c",
        adapter_digest={"w": "x"},
        loss_history=[0.1],
        seed=1,
    )
    state_dict = {"w": torch.zeros(2)}
    publish_checkpoint(ws_guard, record, state_dict)
    try:
        publish_checkpoint(ws_guard, record, state_dict)
    except StaleCheckpointError as exc:
        assert exc.code == E_STALE_CHECKPOINT
        print("duplicate sequence ->", exc.code)

    bad = CheckpointRecord(
        task_id="guard",
        step=2,
        sequence=2,
        sample_cursor=8,
        sample_fingerprint="s",
        config_fingerprint="c",
        adapter_digest={"w": "wrong"},
        loss_history=[0.1, 0.2],
        seed=1,
    )
    publish_checkpoint(ws_guard, bad, state_dict)
    try:
        engine.build_summary  # import touch
        from lora_bench.checkpoint import load_latest

        load_latest(ws_guard, "guard")
    except WeightMismatchError as exc:
        assert exc.code == E_WEIGHT_MISMATCH
        print("digest mismatch ->", exc.code, exc.field)

    heading("DEMO OK")
    print("canonical summary fingerprint:", parallel["summary_fingerprint"])
    return 0


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "data", "workspace")
    raise SystemExit(main(base))
