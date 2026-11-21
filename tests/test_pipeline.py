import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from evalbench.config import load_config
from evalbench.pipeline import Pipeline, run_pipeline


def test_full_run_then_all_reused(workspace, shards_dir, config_v2):
    rep1 = run_pipeline(workspace, shards_dir, config_v2)
    assert rep1["status"] == "completed"
    assert all(s == "rebuilt" for sh in rep1["shards"].values() for s in sh["stages"].values())
    rep2 = run_pipeline(workspace, shards_dir, config_v2)
    assert all(s == "reused" for sh in rep2["shards"].values() for s in sh["stages"].values())


def test_export_exists_and_deterministic(workspace, shards_dir, config_v2, tmp_path):
    run_pipeline(workspace, shards_dir, config_v2)
    first = Path(workspace, "exports/reliability_report.json").read_bytes()
    ws2 = str(tmp_path / "ws2")
    run_pipeline(ws2, shards_dir, config_v2)
    second = Path(ws2, "exports/reliability_report.json").read_bytes()
    assert first == second


def test_invalidation_propagation_min_samples(workspace, shards_dir, cfg_dict, cfg_path):
    run_pipeline(workspace, shards_dir, cfg_path)
    cfg_dict["reliability_min_samples"] = 30
    Path(cfg_path).write_text(json.dumps(cfg_dict), encoding="utf-8")
    rep = run_pipeline(workspace, shards_dir, cfg_path)
    shard = rep["shards"]["shard_v1_a"]
    assert shard["stages"] == {"parse": "reused", "calibrate": "reused", "diagnose": "rebuilt"}
    assert shard["invalidations"][0]["earliest_stage"] == "diagnose"
    assert any("reliability_min_samples" in r for r in shard["invalidations"][0]["reasons"])


def test_invalidation_propagation_method(workspace, shards_dir, cfg_dict, cfg_path):
    run_pipeline(workspace, shards_dir, cfg_path)
    cfg_dict["calibration"] = {"method": "isotonic"}
    Path(cfg_path).write_text(json.dumps(cfg_dict), encoding="utf-8")
    rep = run_pipeline(workspace, shards_dir, cfg_path)
    shard = rep["shards"]["shard_v1_a"]
    assert shard["stages"]["parse"] == "reused"
    assert shard["stages"]["calibrate"] == "rebuilt"
    assert shard["stages"]["diagnose"] == "rebuilt"
    assert shard["invalidations"][0]["earliest_stage"] == "calibrate"


def test_invalidation_propagation_shard_edit(workspace, shards_dir, cfg_path, tmp_path):
    run_pipeline(workspace, shards_dir, cfg_path)
    mod = tmp_path / "mod"
    shutil.copytree(shards_dir, mod)
    target = mod / "shard_v2_a.jsonl"
    lines = target.read_text(encoding="utf-8").splitlines()
    obj = json.loads(lines[0])
    obj["score"] = 0.03
    obj["bucket"] = 0
    lines[0] = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rep = run_pipeline(workspace, str(mod), cfg_path)
    assert rep["shards"]["shard_v2_a"]["stages"]["parse"] == "rebuilt"
    for other in ("shard_v1_a", "shard_v1_b", "shard_v2_b"):
        assert rep["shards"][other]["stages"]["parse"] == "reused"


def test_status_exposes_fingerprints_and_reasons(workspace, shards_dir, cfg_dict, cfg_path):
    run_pipeline(workspace, shards_dir, cfg_path)
    cfg_dict["reliability_min_samples"] = 25
    Path(cfg_path).write_text(json.dumps(cfg_dict), encoding="utf-8")
    pipe = Pipeline(workspace, load_config(cfg_path))
    status = pipe.status(data_dir=shards_dir)
    diag = status["shards"]["shard_v1_a"]["diagnose"]
    assert diag["state"] == "invalid"
    assert diag["invalidation_reasons"]
    assert diag["stored_config_fingerprint"] != diag["expected_config_fingerprint"]
    assert status["shards"]["shard_v1_a"]["parse"]["state"] == "valid"


def test_upgraded_v1_config_matches_native_v2(workspace, shards_dir, config_v1,
                                              config_v2, tmp_path):
    ws2 = str(tmp_path / "ws_native")
    run_pipeline(workspace, shards_dir, config_v1)
    run_pipeline(ws2, shards_dir, config_v2)
    r1 = json.loads(Path(workspace, "exports/reliability_report.json").read_text())
    r2 = json.loads(Path(ws2, "exports/reliability_report.json").read_text())
    r1.pop("config_upgrade_notes")
    r2.pop("config_upgrade_notes")
    assert r1 == r2


def test_graceful_stop_resume_byte_identical(shards_dir, config_v2, tmp_path):
    ws_full = str(tmp_path / "full")
    ws_stop = str(tmp_path / "stop")
    env = dict(os.environ)
    subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", shards_dir,
                    "--config", config_v2, "--workspace", ws_full],
                   check=True, capture_output=True, env=env)
    proc = subprocess.Popen([sys.executable, "-m", "evalbench", "run", "--data", shards_dir,
                             "--config", config_v2, "--workspace", ws_stop,
                             "--unit-delay-ms", "200"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.0)
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert json.loads(out)["status"] == "stopped"
    subprocess.run([sys.executable, "-m", "evalbench", "run", "--data", shards_dir,
                    "--config", config_v2, "--workspace", ws_stop],
                   check=True, capture_output=True, env=env)
    for rel in ("exports/reliability_report.json",):
        a = Path(ws_full, rel).read_bytes()
        b = Path(ws_stop, rel).read_bytes()
        assert a == b, f"{rel} differs after resume"
    full_ckpts = sorted(Path(ws_full, "checkpoints").rglob("*.json"))
    for path in full_ckpts:
        twin = Path(str(path).replace(ws_full, ws_stop))
        assert twin.exists()
        assert path.read_bytes() == twin.read_bytes()


def test_stop_event_checked_between_units(workspace, shards_dir, config_v2):
    cfg = load_config(config_v2)
    pipe = Pipeline(workspace, cfg)
    event = threading.Event()
    event.set()  # already stopped: nothing should be rebuilt
    rep = pipe.run(shards_dir, stop_event=event)
    assert rep["status"] == "stopped"
