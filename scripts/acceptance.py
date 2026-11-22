#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mtlab.checkpoints import CheckpointStore
from mtlab.errors import MtlError
from mtlab.util import sha256_json, sha256_tree, write_json_atomic

DATA = ROOT / "data"
REPORT_PATH = ROOT / "artifacts" / "acceptance_report.json"


def run_cli(args: list[str], *, expect: int | None = 0) -> tuple[int, dict[str, Any]]:
    command = [sys.executable, "-m", "mtlab", *args]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if expect is not None and completed.returncode != expect:
        raise AssertionError(
            f"command failed: {' '.join(command)}\n"
            f"expected={expect} actual={completed.returncode}\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )
    text = completed.stdout.strip()
    payload = json.loads(text) if text else {}
    return completed.returncode, payload


def make_probe_stage(
    store: CheckpointStore,
    *,
    tier: str,
    stage: str,
    descriptor: dict[str, Any],
) -> tuple[Path, Path]:
    staged = store.make_staging_dir(tier, stage)
    write_json_atomic(
        staged / "_meta.json",
        {
            "tier": tier,
            "stage": stage,
            "input_fingerprint": sha256_json(descriptor),
        },
    )
    write_json_atomic(staged / "payload.json", {"probe": True})
    descriptor_path = staged.parent / f"{staged.name}-descriptor.json"
    write_json_atomic(descriptor_path, descriptor)
    return staged, descriptor_path


def verify_checkpoint_errors(work_root: Path, report: dict[str, Any]) -> None:
    ws = work_root / "checkpoint-errors"
    store = CheckpointStore(ws)
    observed: list[str] = []

    staged, _ = make_probe_stage(
        store,
        tier="small",
        stage="align",
        descriptor={"probe": "illegal-jump"},
    )
    try:
        store.publish_stage(
            tier="small",
            stage="align",
            staged_dir=staged,
            input_descriptor={"probe": "illegal-jump"},
            decision_fingerprint="probe",
        )
    except MtlError as exc:
        assert exc.code == "E_INVALID_STAGE", exc.as_dict()
        observed.append(exc.code)
    else:
        raise AssertionError("illegal jump unexpectedly published")
    store.discard_staging_dir(staged)

    split_descriptor = {"probe": "split"}
    staged, _ = make_probe_stage(store, tier="small", stage="split", descriptor=split_descriptor)
    split_record = store.publish_stage(
        tier="small",
        stage="split",
        staged_dir=staged,
        input_descriptor=split_descriptor,
        decision_fingerprint="probe",
    )
    assert split_record["sequence"] == 1

    staged, _ = make_probe_stage(store, tier="small", stage="split", descriptor=split_descriptor)
    try:
        store.publish_stage(
            tier="small",
            stage="split",
            staged_dir=staged,
            input_descriptor=split_descriptor,
            decision_fingerprint="probe",
        )
    except MtlError as exc:
        assert exc.code == "E_STALE_CHECKPOINT", exc.as_dict()
        observed.append(exc.code)
    else:
        raise AssertionError("duplicate checkpoint unexpectedly published")
    store.discard_staging_dir(staged)

    staged, _ = make_probe_stage(
        store,
        tier="small",
        stage="decode",
        descriptor={"probe": "jump-decode"},
    )
    try:
        store.publish_stage(
            tier="small",
            stage="decode",
            staged_dir=staged,
            input_descriptor={"probe": "jump-decode"},
            decision_fingerprint="probe",
        )
    except MtlError as exc:
        assert exc.code == "E_INVALID_STAGE", exc.as_dict()
        observed.append(exc.code)
    else:
        raise AssertionError("decode jump unexpectedly published")
    store.discard_staging_dir(staged)

    staged, _ = make_probe_stage(
        store,
        tier="small",
        stage="align",
        descriptor={"probe": "bad-sequence"},
    )
    try:
        store.publish_stage(
            tier="small",
            stage="align",
            staged_dir=staged,
            input_descriptor={"probe": "bad-sequence"},
            decision_fingerprint="probe",
            expected_sequence=9,
        )
    except MtlError as exc:
        assert exc.code == "E_STALE_CHECKPOINT", exc.as_dict()
        observed.append(exc.code)
    else:
        raise AssertionError("bad sequence unexpectedly published")
    store.discard_staging_dir(staged)

    staged = store.make_staging_dir("small", "align")
    cross_descriptor = {"probe": "cross-tier"}
    write_json_atomic(
        staged / "_meta.json",
        {
            "tier": "medium",
            "stage": "align",
            "input_fingerprint": sha256_json(cross_descriptor),
        },
    )
    write_json_atomic(staged / "payload.json", {"probe": True})
    try:
        store.publish_stage(
            tier="small",
            stage="align",
            staged_dir=staged,
            input_descriptor=cross_descriptor,
            decision_fingerprint="probe",
        )
    except MtlError as exc:
        assert exc.code == "E_STALE_CHECKPOINT", exc.as_dict()
        observed.append(exc.code)
    else:
        raise AssertionError("cross-tier checkpoint unexpectedly published")
    store.discard_staging_dir(staged)

    report["checkpoint_errors"] = {
        "observed_codes": sorted(set(observed)),
        "final_sequence": store.records("small")[0]["sequence"],
    }


def verify_concurrent_publish(work_root: Path, report: dict[str, Any]) -> None:
    ws = work_root / "concurrent-publish"
    store = CheckpointStore(ws)
    descriptor = {"probe": "concurrent-split"}
    staged_dirs: list[Path] = []
    descriptor_files: list[Path] = []
    for index in range(2):
        staged, descriptor_path = make_probe_stage(
            store,
            tier="small",
            stage="split",
            descriptor=descriptor,
        )
        staged_dirs.append(staged)
        descriptor_files.append(descriptor_path)
    start_file = ws / "start-flag"
    processes: list[subprocess.Popen[str]] = []
    for index in range(2):
        command = [
            sys.executable,
            str(ROOT / "scripts" / "publish_probe.py"),
            "--workspace",
            str(ws),
            "--tier",
            "small",
            "--stage",
            "split",
            "--staged-dir",
            str(staged_dirs[index]),
            "--descriptor-file",
            str(descriptor_files[index]),
            "--decision-fingerprint",
            f"probe-{index}",
            "--start-file",
            str(start_file),
        ]
        processes.append(
            subprocess.Popen(
                command,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    start_file.write_text("go\n", encoding="utf-8")
    outputs: list[dict[str, Any]] = []
    exit_codes: list[int] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=30)
        exit_codes.append(int(process.returncode or 0))
        if stdout.strip():
            outputs.append(json.loads(stdout))
        else:
            outputs.append({"stderr": stderr})
    successes = [item for item in outputs if item.get("status") == "published"]
    stale = [
        item
        for item in outputs
        if item.get("error", {}).get("code") == "E_STALE_CHECKPOINT"
    ]
    records = store.records("small")
    assert len(successes) == 1, (exit_codes, outputs)
    assert len(stale) == 1, (exit_codes, outputs)
    assert len(records) == 1 and records[0]["sequence"] == 1, records
    report["concurrent_publish"] = {
        "worker_exit_codes": sorted(exit_codes),
        "winner_count": len(successes),
        "stale_count": len(stale),
        "checkpoint_count": len(records),
    }


def verify_sigterm(work_root: Path, report: dict[str, Any]) -> None:
    ws = work_root / "signal-stop"
    command = [
        sys.executable,
        "-m",
        "mtlab",
        "run",
        "--workspace",
        str(ws),
        "--data-dir",
        str(DATA),
        "--tiers",
        "large",
    ]
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 10.0

    while not (ws / ".mtlab" / "workspace.lock").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(1.0)
    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=20)
    assert process.returncode == 75, (process.returncode, stdout, stderr)
    payload = json.loads(stdout)
    assert payload["status"] == "stopped"
    assert payload["tier"] == "large"
    report["signal_stop"] = {
        "exit_code": process.returncode,
        "status": payload["status"],
        "tier": payload["tier"],
        "stage": payload["stage"],
        "resume_instruction_present": bool(payload.get("restart")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full reproducibility acceptance suite")
    parser.add_argument("--keep-work", action="store_true")
    args = parser.parse_args()
    if not (DATA / "manifest.json").exists():
        raise SystemExit("dataset missing; run `python -m mtlab generate` first")

    work = Path(tempfile.mkdtemp(prefix="mtlab-acceptance-"))
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "environment": {
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "numpy_version": np.__version__,
            "system": platform.system(),
            "machine": platform.machine(),
        },
    }
    try:
        manifest = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))
        anomaly_summary: dict[str, Any] = {}
        for tier in ("small", "medium", "large"):
            tier_manifest = manifest["tiers"][tier]
            anomaly_summary[tier] = {
                "pair_count": tier_manifest["pair_count"],
                "lang_pair_counts": tier_manifest["lang_pair_counts"],
                "noise_counts": tier_manifest["noise_counts"],
            }
            assert len(tier_manifest["lang_pair_counts"]) == 2
            assert len(tier_manifest["noise_counts"]) >= 3
        report["dataset"] = anomaly_summary

        clean_ws = work / "clean"
        _, clean_result = run_cli(
            ["run", "--workspace", str(clean_ws), "--data-dir", str(DATA)],
            expect=0,
        )
        clean_summary_path = clean_ws / "artifacts" / "run_summary.json"
        clean_summary_bytes = clean_summary_path.read_bytes()
        clean_summary = json.loads(clean_summary_bytes)
        assert clean_result["summary_digest"] == clean_summary["summary_digest"]
        assert [clean_summary["tiers"][tier]["policy"]["alignment_mode"] for tier in ("small", "medium", "large")] == [
            "subword_approx",
            "word_exact",
            "word_exact",
        ]
        assert clean_summary["tiers"]["small"]["policy"]["phrase_limit"] <= 256
        assert clean_summary["tiers"]["small"]["policy"]["beam_width"] <= 4

        _, reuse_result = run_cli(
            ["run", "--workspace", str(clean_ws), "--data-dir", str(DATA)],
            expect=0,
        )
        assert reuse_result["summary_digest"] == clean_summary["summary_digest"]
        assert clean_summary_path.read_bytes() == clean_summary_bytes
        reuse_report = json.loads((clean_ws / "artifacts" / "last_run_report.json").read_text())
        assert all(
            reuse_report["reused_stages"][tier] == ["split", "align", "policy", "decode", "evaluate"]
            for tier in ("small", "medium", "large")
        )

        resume_ws = work / "resume"
        stop_code, stop_payload = run_cli(
            [
                "run",
                "--workspace",
                str(resume_ws),
                "--data-dir",
                str(DATA),
                "--stop-after-tier",
                "small",
            ],
            expect=75,
        )
        assert stop_payload["status"] == "stopped"
        _, resume_result = run_cli(
            ["run", "--workspace", str(resume_ws), "--data-dir", str(DATA)],
            expect=0,
        )
        resume_summary_path = resume_ws / "artifacts" / "run_summary.json"
        assert resume_summary_path.read_bytes() == clean_summary_bytes
        assert resume_result["summary_digest"] == clean_summary["summary_digest"]
        stage_tree_equal = True
        for tier in ("small", "medium", "large"):
            for stage in ("split", "align", "policy", "decode", "evaluate"):
                stage_tree_equal &= (
                    sha256_tree(clean_ws / "artifacts" / "stages" / tier / stage)
                    == sha256_tree(resume_ws / "artifacts" / "stages" / tier / stage)
                )
        ledger_equal = (
            (clean_ws / "artifacts" / "checkpoints" / "ledger.json").read_bytes()
            == (resume_ws / "artifacts" / "checkpoints" / "ledger.json").read_bytes()
        )
        assert stage_tree_equal
        assert ledger_equal

        _, verify_payload = run_cli(
            ["verify-policy", "--workspace", str(clean_ws), "--data-dir", str(DATA)],
            expect=0,
        )
        assert verify_payload["valid"] is True

        version_ws = work / "version"
        run_cli(
            ["run", "--workspace", str(version_ws), "--data-dir", str(DATA), "--tiers", "small"],
            expect=0,
        )
        split_checkpoint_path = version_ws / "artifacts" / "checkpoints" / "small" / "0001-split.json"
        original_split = split_checkpoint_path.read_bytes()
        status_cases = {
            "smoothing": ("align", "smoothing_changed", ["--smoothing", "0.5"]),
            "bpe": ("split", "bpe_rules_config_changed", ["--bpe-merges", "41"]),
            "budget": ("policy", "policy_budget_changed", ["--budget", "100000"]),
            "decode_width": ("policy", "decode_width_changed", ["--beam-width", "6"]),
        }
        invalidation_status: dict[str, Any] = {}
        for name, (earliest, reason, extra_args) in status_cases.items():
            _, status = run_cli(
                [
                    "status",
                    "--workspace",
                    str(version_ws),
                    "--data-dir",
                    str(DATA),
                    "--tiers",
                    "small",
                    *extra_args,
                ],
                expect=0,
            )
            would = status["tiers"]["small"]["would_invalidate"]
            assert would["earliest_stage"] == earliest, (name, would)
            assert reason in would["reasons"], (name, would)
            invalidation_status[name] = would

        altered_data = work / "altered-data"
        shutil.copytree(DATA, altered_data)
        with (altered_data / "corpora" / "small.jsonl").open("ab") as handle:
            handle.write(b"\n")
        _, content_status = run_cli(
            [
                "status",
                "--workspace",
                str(version_ws),
                "--data-dir",
                str(altered_data),
                "--tiers",
                "small",
            ],
            expect=0,
        )
        content_would = content_status["tiers"]["small"]["would_invalidate"]
        assert content_would["earliest_stage"] == "split", content_would
        assert "corpus_content_changed" in content_would["reasons"], content_would
        invalidation_status["corpus_content"] = content_would

        _, changed_result = run_cli(
            [
                "run",
                "--workspace",
                str(version_ws),
                "--data-dir",
                str(DATA),
                "--tiers",
                "small",
                "--smoothing",
                "0.5",
            ],
            expect=0,
        )
        assert split_checkpoint_path.read_bytes() == original_split
        changed_align = json.loads(
            (version_ws / "artifacts" / "checkpoints" / "small" / "0002-align.json").read_text()
        )
        assert changed_align["sequence"] == 2
        changed_summary = json.loads((version_ws / "artifacts" / "run_summary.json").read_text())
        assert changed_summary["summary_digest"] == changed_result["summary_digest"]
        changed_report = json.loads((version_ws / "artifacts" / "last_run_report.json").read_text())
        assert changed_report["reused_stages"]["small"] == ["split"]
        assert changed_report["invalidations"][0]["earliest_stage"] == "align"

        numeric_ws = work / "numeric-error"
        _, numeric_payload = run_cli(
            [
                "run",
                "--workspace",
                str(numeric_ws),
                "--data-dir",
                str(DATA),
                "--tiers",
                "small",
                "--smoothing",
                "0",
            ],
            expect=2,
        )
        assert numeric_payload["error"]["code"] == "E_NUMERIC"
        assert numeric_payload["error"]["details"]["lang_pair"] in {"en-zh", "zh-en"}
        assert numeric_payload["error"]["details"]["positions"]

        _, decode_payload = run_cli(
            [
                "run",
                "--workspace",
                str(numeric_ws),
                "--data-dir",
                str(DATA),
                "--tiers",
                "small",
                "--beam-width",
                "0",
            ],
            expect=2,
        )
        assert decode_payload["error"]["code"] == "E_DECODE_FAILED"
        assert "pair_id" in decode_payload["error"]["details"]
        assert "step_index" in decode_payload["error"]["details"]

        verify_checkpoint_errors(work, report)
        verify_concurrent_publish(work, report)
        verify_sigterm(work, report)

        report.update(
            {
                "status": "ok",
                "summary_digest": clean_summary["summary_digest"],
                "summary_file_sha256": hashlib.sha256(clean_summary_bytes).hexdigest(),
                "run_summary_byte_identical_after_reuse": True,
                "run_summary_byte_identical_after_stop_resume": True,
                "stage_trees_identical_after_stop_resume": stage_tree_equal,
                "checkpoint_ledger_identical": ledger_equal,
                "stop_resume": {
                    "stop_exit_code": stop_code,
                    "resumed_summary_digest": resume_result["summary_digest"],
                },
                "policy_verification": verify_payload,
                "policy_choices": {
                    tier: {
                        "alignment_mode": clean_summary["tiers"][tier]["policy"]["alignment_mode"],
                        "phrase_limit": clean_summary["tiers"][tier]["policy"]["phrase_limit"],
                        "beam_width": clean_summary["tiers"][tier]["policy"]["beam_width"],
                    }
                    for tier in ("small", "medium", "large")
                },
                "metrics": {
                    tier: {
                        "effective_pair_count": clean_summary["tiers"][tier]["metrics"]["effective_pair_count"],
                        "skipped_by_reason": clean_summary["tiers"][tier]["metrics"]["skipped_by_reason"],
                        "sentence_bleu": clean_summary["tiers"][tier]["metrics"]["sentence_bleu"],
                        "word_precision": clean_summary["tiers"][tier]["metrics"]["word_precision"],
                    }
                    for tier in ("small", "medium", "large")
                },
                "version_invalidation_status": invalidation_status,
                "error_injection": {
                    "E_NUMERIC": numeric_payload["error"],
                    "E_DECODE_FAILED": decode_payload["error"],
                },
            }
        )
        write_json_atomic(REPORT_PATH, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    except Exception as exc:
        report["status"] = "failed"
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        write_json_atomic(REPORT_PATH, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), file=sys.stderr)
        raise
    finally:
        if not args.keep_work:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"acceptance work kept at {work}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

