#!/usr/bin/env python
from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import fastapi
import numpy as np

from prep_pipeline.config import Settings
from prep_pipeline.errors import PipelineError
from prep_pipeline.models import BatchSubmitRequest, InjectionOptions
from prep_pipeline.service import PipelineService
from prep_pipeline.storage import utc_now
from prep_pipeline.verifier import ResultVerifier


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
ARTIFACT_ROOT = REPO_ROOT / "artifacts" / "acceptance"
REPORT_PATH = REPO_ROOT / "artifacts" / "acceptance-report.json"


def request(
    batch_id: str,
    policy_version: int = 1,
    injections: InjectionOptions | None = None,
) -> BatchSubmitRequest:
    return BatchSubmitRequest(
        batch_id=batch_id,
        policy_version=policy_version,
        input_path="records_valid.tsv",
        injections=injections or InjectionOptions(),
    )


def main() -> int:
    if ARTIFACT_ROOT.exists():
        shutil.rmtree(ARTIFACT_ROOT)
    settings = Settings(
        root=REPO_ROOT,
        data_dir=DATA_DIR,
        artifact_dir=ARTIFACT_ROOT,
        db_path=ARTIFACT_ROOT / "pipeline.sqlite3",
        staging_dir=ARTIFACT_ROOT / "staging",
        published_dir=ARTIFACT_ROOT / "batches",
        verification_dir=ARTIFACT_ROOT / "verification",
        poll_interval_seconds=0.01,
    )
    service = PipelineService(settings)
    service.initialize(recover=False)
    service.bootstrap_policies([DATA_DIR / "policy_v1.json", DATA_DIR / "policy_v2.json"])

    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, details: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "details": details})

    try:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 8767))
        port_available = True
    except OSError:
        port_available = False
    check("loopback_port_8767_available", port_available, {"host": "127.0.0.1", "port": 8767})

    first = service.submit_batch(request("accept-repro-a"))
    second = service.submit_batch(request("accept-repro-b"))
    first_digests = {stage["stage"]: stage["output_digest"] for stage in first["stages"]}
    second_digests = {stage["stage"]: stage["output_digest"] for stage in second["stages"]}
    check("same_policy_stage_digests_equal", first_digests == second_digests, first_digests)
    check(
        "same_policy_shard_manifests_equal",
        [item["manifest_sha256"] for item in first["shards"]]
        == [item["manifest_sha256"] for item in second["shards"]],
        [item["manifest_sha256"] for item in first["shards"]],
    )
    byte_equal = True
    compared_files = 0
    for left_shard, right_shard in zip(first["shards"], second["shards"], strict=True):
        for filename in (
            "features.npy",
            "timestamps.npy",
            "text_lengths.npy",
            "label_ids.npy",
            "shard_ids.npy",
            "null_mask.npy",
            "manifest.json",
        ):
            left = settings.artifact_dir / left_shard["relative_path"] / filename
            right = settings.artifact_dir / right_shard["relative_path"] / filename
            compared_files += 1
            byte_equal = byte_equal and left.read_bytes() == right.read_bytes()
    check("same_policy_tensor_files_byte_equal", byte_equal, {"files": compared_files})

    parse_stats = next(stage for stage in first["stages"] if stage["stage"] == "parse_clean")["stats"]
    check(
        "duplicate_first_kept_and_counted",
        parse_stats["duplicate_records_removed"] == 1 and parse_stats["duplicate_ids"] == ["s004"],
        parse_stats,
    )

    verifier = ResultVerifier(settings, service.db)
    verification = verifier.verify("accept-repro-a")
    check(
        "verifier_self_recompute_matches",
        verification["status"] == "match" and verification["first_divergence_stage"] is None,
        {"verification_id": verification["verification_id"]},
    )

    v2_batch = service.submit_batch(request("accept-skew-v2", policy_version=2))
    shard_stage = next(stage for stage in v2_batch["stages"] if stage["stage"] == "shard")
    warning_codes = sorted({item["code"] for item in shard_stage["stats"]["warnings"]})
    check(
        "skew_warning_without_reassignment",
        warning_codes == ["W_SHARD_BYTE_SKEW", "W_SHARD_SAMPLE_SKEW"]
        and sum(item["sample_count"] for item in v2_batch["shards"])
        == v2_batch["summary"]["samples_total"],
        {"warnings": warning_codes, "shards": v2_batch["shards"]},
    )

    injection_cases = {
        "accept-inject-label": (InjectionOptions(invalid_label=True), "E_LABEL_INVALID"),
        "accept-inject-shard": (InjectionOptions(missing_shard_key=True), "E_SHARD_KEY_MISSING"),
        "accept-inject-ratio": (InjectionOptions(sample_ratio=1.25), "E_SAMPLE_RATIO_INVALID"),
        "accept-inject-conflict": (InjectionOptions(duplicate_conflict=True), "E_CONFLICTING_RECORD"),
        "accept-inject-interrupt": (
            InjectionOptions(interrupt_at_stage="shard", interrupt_after_records=2),
            "E_INJECTED_INTERRUPT",
        ),
    }
    observed_codes: dict[str, str] = {}
    for batch_id, (injection, expected_code) in injection_cases.items():
        try:
            service.submit_batch(request(batch_id, injections=injection))
            observed_codes[batch_id] = "NO_ERROR"
        except PipelineError as exc:
            observed_codes[batch_id] = exc.code
        check(
            f"injection_{batch_id}",
            observed_codes[batch_id] == expected_code,
            {"expected": expected_code, "actual": observed_codes[batch_id]},
        )
    check(
        "five_injection_codes_distinct",
        len(set(observed_codes.values())) == 5,
        observed_codes,
    )
    check(
        "injection_failures_do_not_change_committed_batch",
        {stage["stage"]: stage["output_digest"] for stage in service.get_batch("accept-repro-a")["stages"]}
        == first_digests,
        {"batch_id": "accept-repro-a"},
    )

    env = os.environ.copy()
    env.update(
        {
            "PREP_ROOT": str(REPO_ROOT),
            "PREP_DATA_DIR": str(DATA_DIR),
            "PREP_ARTIFACT_DIR": str(ARTIFACT_ROOT),
            "PREP_DB_PATH": str(settings.db_path),
        }
    )
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "prep_pipeline.cli",
            "run-batch",
            "--batch-id",
            "accept-crash",
            "--policy-version",
            "1",
            "--input",
            "records_valid.tsv",
            "--hard-crash-after-stage",
            "filter",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    check("forced_termination_exit_71", crash.returncode == 71, {"returncode": crash.returncode})
    recovered = service.recover()
    recovery = next((item for item in recovered if item["batch_id"] == "accept-crash"), None)
    check(
        "recovery_uses_last_complete_stage",
        recovery is not None
        and recovery["last_complete_stage"]["stage"] == "filter"
        and recovery["committed_shards"] == []
        and recovery["exit_report_available"] is False
        and recovery["uncommitted_artifacts_recoverable"] is False,
        recovery,
    )
    check(
        "recovery_batch_aborted",
        service.db.get_batch("accept-crash")["status"] == "aborted",
        {"status": service.db.get_batch("accept-crash")["status"]},
    )

    service.submit_batch(request("accept-verify-corrupt"))
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE stage_facts SET output_digest = ? "
            "WHERE batch_id = 'accept-verify-corrupt' AND stage_name = 'sample'",
            ("sha256:" + "f" * 64,),
        )
        conn.commit()
    divergence: dict[str, Any] | None = None
    try:
        verifier.verify("accept-verify-corrupt")
    except PipelineError as exc:
        divergence = {
            "code": exc.code,
            "first_divergence_stage": exc.context.get("first_divergence_stage"),
            "stored_digest": exc.context.get("stored_digest"),
            "recomputed_digest": exc.context.get("recomputed_digest"),
        }
    check(
        "verifier_reports_first_divergence",
        divergence is not None
        and divergence["code"] == "E_REPRO_MISMATCH"
        and divergence["first_divergence_stage"] == "sample",
        divergence,
    )

    report = {
        "generated_at": utc_now(),
        "environment": {
            "python": sys.version.split()[0],
            "numpy": np.__version__,
            "fastapi": fastapi.__version__,
            "sqlite": sqlite3.sqlite_version,
        },
        "database": str(settings.db_path),
        "artifact_root": str(settings.artifact_dir),
        "checks": checks,
        "passed": all(item["passed"] for item in checks),
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"report": str(REPORT_PATH), "passed": report["passed"], "checks": len(checks)}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
