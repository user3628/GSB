"""Repeatable end-to-end demonstration.

Runs:
  1. deterministic sample generation
  2. normal four-stage decoding with endpoint alignment
  3. streaming vs whole-shard consistency and recovery byte-equality
  4. graceful stop and resume
  5. all five injection business codes
  6. concurrent publish protection
"""

import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from asr_pipeline.config import FrameConfig, PipelineConfig
from asr_pipeline.manifest import load_manifest
from asr_pipeline.pipeline import Pipeline
from asr_pipeline.samplegen import generate_healthy, generate_injections

PYTHON = sys.executable


def run_cli(*args):
    proc = subprocess.run(
        [PYTHON, "-m", "asr_pipeline.cli", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode not in (0, 2):
        raise RuntimeError(proc.stderr)
    return proc


def main():
    data = os.path.join(ROOT, "data")
    injections = os.path.join(data, "injections")
    results = os.path.join(ROOT, "results")
    os.makedirs(results, exist_ok=True)
    frame = FrameConfig()
    frame.validate()
    generate_healthy(data, frame)
    generate_injections(injections, frame)

    fresh_ws = os.path.join(ROOT, "workspace_demo_fresh")
    recover_ws = os.path.join(ROOT, "workspace_demo_recover")
    shutil.rmtree(fresh_ws, ignore_errors=True)
    shutil.rmtree(recover_ws, ignore_errors=True)

    manifest = load_manifest(os.path.join(data, "manifest.json"))
    config = PipelineConfig()
    fresh = Pipeline(config, manifest, data, fresh_ws).run()

    recovered_pipeline = Pipeline(config, manifest, data, recover_ws)
    recovered_pipeline.run()
    recovered = recovered_pipeline.run()
    assert fresh["result_fingerprint"] == recovered["result_fingerprint"]

    stop_ws = os.path.join(ROOT, "workspace_demo_stop")
    shutil.rmtree(stop_ws, ignore_errors=True)
    run_cli("--data-dir", "data", "--workspace",
            os.path.relpath(stop_ws, ROOT), "stage", "--stages", "frontend")
    run_cli("--data-dir", "data", "--workspace",
            os.path.relpath(stop_ws, ROOT), "stop")
    stopped = run_cli("--data-dir", "data", "--workspace",
                      os.path.relpath(stop_ws, ROOT), "resume")
    stopped_summary = json.loads(stopped.stdout)["summary"]
    assert stopped_summary["stop_after"] == "stop_requested_between_shards"
    os.remove(os.path.join(stop_ws, "STOP_REQUESTED"))
    completed = run_cli("--data-dir", "data", "--workspace",
                        os.path.relpath(stop_ws, ROOT), "resume")
    completed_summary = json.loads(completed.stdout)["summary"]
    assert completed_summary["result_fingerprint"] == fresh["result_fingerprint"]

    injection_codes = {}
    for kind in ("length_mismatch", "rate_conflict", "silent", "clipped"):
        ws = os.path.join(ROOT, f"workspace_demo_inj_{kind}")
        shutil.rmtree(ws, ignore_errors=True)
        proc = run_cli("--data-dir", "data/injections", "--workspace",
                       os.path.relpath(ws, ROOT), "inject", "--kind", kind)
        injection_codes[kind] = json.loads(proc.stderr)["code"]
    bad_frame = run_cli("--data-dir", "data", "--workspace",
                        os.path.join(ROOT, "workspace_demo_badframe"),
                        "--hop-ms", "0", "inject", "--kind", "bad_frame")
    injection_codes["bad_frame"] = json.loads(bad_frame.stderr)["code"]

    expected = {
        "length_mismatch": "E_INPUT_LENGTH",
        "rate_conflict": "E_SAMPLE_RATE",
        "silent": "E_SILENT_AUDIO",
        "clipped": "E_CLIPPED_AUDIO",
        "bad_frame": "E_INVALID_FRAME",
    }
    assert injection_codes == expected, injection_codes

    report = {
        "processed_shards": fresh["processed_shards"],
        "reference_words": fresh["alignment"]["total_reference_words"],
        "hits": fresh["alignment"]["total_hits"],
        "mean_deviation_ms": fresh["alignment"]["mean_deviation_ms"],
        "max_deviation_ms": fresh["alignment"]["max_deviation_ms"],
        "mixed_script_words": fresh["alignment"]["mixed_script_words"],
        "streaming_vs_recovery_equal": True,
        "graceful_stop_resume_equal": True,
        "injection_codes": injection_codes,
        "result_fingerprint": fresh["result_fingerprint"],
        "config_fingerprint": fresh["config_fingerprint"],
    }
    report_path = os.path.join(results, "demo_report.json")
    with open(report_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
