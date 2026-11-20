import json
import os
import time

import pytest

from lora_bench import engine
from lora_bench.config import parse_config, task_config_fingerprint
from lora_bench.generator import DEFAULT_MODEL_CONFIG, generate_all, write_config
from lora_bench.state import (
    PHASE_COMPLETED,
    PHASE_NOT_STARTED,
    PHASE_STOPPED,
    load_state,
    request_stop,
)


@pytest.fixture()
def prepared(tmp_path):
    data_dir = str(tmp_path / "data")
    ws = str(tmp_path / "ws")
    manifest = generate_all(data_dir)
    return data_dir, ws, manifest


def test_full_parallel_run_matches_serial(prepared):
    data_dir, ws, manifest = prepared
    config_path = manifest["config_path"]

    engine.import_work(ws, config_path, DEFAULT_MODEL_CONFIG)
    parallel_summary = engine.run(ws)

    serial_ws = os.path.join(os.path.dirname(ws), "ws-serial")
    engine.import_work(serial_ws, config_path, DEFAULT_MODEL_CONFIG)
    serial_summary = engine.run(serial_ws, max_workers=1)

    key = lambda summary: [
        (
            entry["task_id"],
            entry["status"],
            entry["loss_history"],
            entry["adapter_digest"],
        )
        for entry in summary["tasks"]
    ]
    assert key(parallel_summary) == key(serial_summary)
    assert parallel_summary["summary_fingerprint"] == serial_summary["summary_fingerprint"]

    state = load_state(ws)
    not_started = [tid for tid, task in state.tasks.items() if task.status == PHASE_NOT_STARTED]
    assert not_started == ["task-gamma-overbudget"]
    assert state.tasks["task-gamma-overbudget"].rejection_reason is not None
    assert state.global_consumed <= state.global_cap
    assert state.tasks["task-alpha"].consumed_samples == 48
    assert state.tasks["task-beta"].consumed_samples == 20
    assert state.tasks["task-gamma-overbudget"].consumed_samples == 0
    assert state.global_consumed == 68


def test_stop_and_resume_byte_identical(prepared):
    data_dir, ws, manifest = prepared
    config_path = manifest["config_path"]
    engine.import_work(ws, config_path, DEFAULT_MODEL_CONFIG)

    # Stop flag placed before the run: workers finish the current step then exit.
    request_stop(ws)
    partial = engine.run(ws, keep_stop_flag=True)
    assert load_state(ws).run_phase == PHASE_STOPPED
    assert all(entry["completed_steps"] < entry["max_steps"] for entry in partial["tasks"] if entry["status"] == PHASE_STOPPED)

    resumed = engine.resume(ws)
    fresh_ws = os.path.join(os.path.dirname(ws), "ws-fresh")
    engine.import_work(fresh_ws, config_path, DEFAULT_MODEL_CONFIG)
    fresh = engine.run(fresh_ws)
    assert resumed["summary_fingerprint"] == fresh["summary_fingerprint"]
    assert load_state(ws).run_phase == PHASE_COMPLETED


def test_corrupt_checkpoint_ignored_on_resume(prepared):
    data_dir, ws, manifest = prepared
    config_path = manifest["config_path"]
    payload = parse_config(config_path).to_dict()
    payload["checkpoint_every"] = 1
    frequent_path = os.path.join(data_dir, "experiment-frequent.yaml")
    write_config(frequent_path, payload)
    engine.import_work(ws, frequent_path, DEFAULT_MODEL_CONFIG)
    request_stop(ws)
    engine.run(ws, keep_stop_flag=True)

    target_task = "task-alpha"
    ckpt_root = os.path.join(ws, "checkpoints", target_task)
    entries = sorted(os.listdir(ckpt_root))
    # Corrupt the newest committed checkpoint's metadata; it must be ignored
    # and resume must continue from the preceding consistent checkpoint.
    newest = entries[-1]
    with open(os.path.join(ckpt_root, newest, "checkpoint.json"), "w") as handle:
        handle.write("{not valid json")
    os.unlink(os.path.join(ckpt_root, newest, ".committed"))

    resumed = engine.resume(ws)
    fresh_ws = os.path.join(os.path.dirname(ws), "ws-fresh2")
    engine.import_work(fresh_ws, frequent_path, DEFAULT_MODEL_CONFIG)
    fresh = engine.run(fresh_ws)
    assert resumed["summary_fingerprint"] == fresh["summary_fingerprint"]


def test_config_change_invalidates_affected_task_only(prepared):
    data_dir, ws, manifest = prepared
    config_path = manifest["config_path"]
    engine.import_work(ws, config_path, DEFAULT_MODEL_CONFIG)
    engine.run(ws)

    config = parse_config(config_path)
    before = {tid: task_config_fingerprint(task) for tid, task in
              ((t.task_id, t) for t in config.tasks)}
    payload = config.to_dict()
    for task in payload["tasks"]:
        if task["task_id"] == "task-beta":
            task["rank"] = 6
    changed_path = os.path.join(data_dir, "experiment-v2.yaml")
    write_config(changed_path, payload)

    state = engine.import_work(ws, changed_path, DEFAULT_MODEL_CONFIG, reimport=True)
    beta = state.tasks["task-beta"]
    alpha = state.tasks["task-alpha"]
    assert beta.invalidation["reason"] == "config_or_samples_changed"
    assert beta.status == "pending" and beta.completed_steps == 0
    assert alpha.status == PHASE_COMPLETED and alpha.completed_steps == alpha.max_steps

    rebuilt = engine.run(ws)
    fresh_ws = os.path.join(os.path.dirname(ws), "ws-v2-fresh")
    engine.import_work(fresh_ws, changed_path, DEFAULT_MODEL_CONFIG)
    fresh = engine.run(fresh_ws)
    assert rebuilt["summary_fingerprint"] == fresh["summary_fingerprint"]


def test_budget_change_reschedules_without_reusing_rejections(prepared):
    data_dir, ws, manifest = prepared
    engine.import_work(ws, manifest["config_path"], DEFAULT_MODEL_CONFIG)
    engine.run(ws)
    rejected = load_state(ws).tasks["task-gamma-overbudget"]
    assert rejected.status == PHASE_NOT_STARTED

    payload = parse_config(manifest["config_path"]).to_dict()
    payload["budget"]["per_task_sample_quota"] = 400
    payload["budget"]["global_sample_cap"] = 900
    relaxed_path = os.path.join(data_dir, "experiment-relaxed.yaml")
    write_config(relaxed_path, payload)
    state = engine.import_work(ws, relaxed_path, DEFAULT_MODEL_CONFIG, reimport=True)
    assert state.tasks["task-gamma-overbudget"].status != PHASE_NOT_STARTED
    summary = engine.run(ws)
    assert all(item["status"] == PHASE_COMPLETED for item in summary["tasks"])
