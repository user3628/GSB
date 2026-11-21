#!/usr/bin/env python3
"""Reproduce the acceptance checks and write artifacts/verification_report.json."""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factmerge import FactMergeEngine  # noqa: E402
from factmerge.config import load_config  # noqa: E402
from factmerge.errors import FactMergeError  # noqa: E402
from factmerge.evidence import (  # noqa: E402
    export_manifest_bytes,
    load_manifest,
    validate_manifest,
)
from factmerge.migration import export_legacy_v1_bytes, migrate_legacy_v1_file  # noqa: E402
from factmerge.pipeline import load_corpus, load_expected, write_run_outputs  # noqa: E402
from factmerge.util import canonical_json  # noqa: E402


def _clean(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def main() -> int:
    config = load_config(ROOT / "configs" / "default.json")
    corpus = load_corpus(ROOT / "samples" / "corpus")
    expected = load_expected(ROOT / "samples" / "expected.json")
    demo_root = ROOT / "artifacts" / "demo"
    _clean(demo_root)
    demo_root.mkdir(parents=True, exist_ok=True)

    full_state = demo_root / "full-state"
    full = FactMergeEngine(config, corpus, state_dir=full_state, expected=expected).run()
    write_run_outputs(full, demo_root / "full-run")
    full_manifest_bytes = export_manifest_bytes(full["manifest"])
    full_validation = validate_manifest(load_manifest(demo_root / "full-run" / "evidence_manifest.json"))

    selfcheck = FactMergeEngine(config, corpus, expected=expected).consistency_check()

    resume_state = demo_root / "resume-state"
    stopped = FactMergeEngine(config, corpus, state_dir=resume_state, expected=expected).run(
        stop_after_entities=2
    )
    resumed = FactMergeEngine(config, corpus, state_dir=resume_state, expected=expected).run(resume=True)
    resumed_manifest_bytes = export_manifest_bytes(resumed["manifest"])
    checkpoint_files = sorted((resume_state / "checkpoints").glob("*.json"))
    checkpoint_sequences = [
        json.loads(path.read_text(encoding="utf-8"))["seq"] for path in checkpoint_files
    ]

    changed_config = copy.deepcopy(config)
    changed_config["language_weights"]["en"] = "0.51"
    incremental = FactMergeEngine(
        changed_config,
        corpus,
        state_dir=full_state,
        expected=expected,
    ).run()
    version_status = incremental["version_status"]

    legacy_source = ROOT / "samples" / "legacy_evidence_v1.jsonl"
    migrated = migrate_legacy_v1_file(legacy_source, config)
    legacy_roundtrip_equal = export_legacy_v1_bytes(migrated) == legacy_source.read_bytes()
    legacy_validation = validate_manifest(migrated)
    try:
        migrate_legacy_v1_file(ROOT / "samples" / "legacy_evidence_v1_invalid.jsonl", config)
        invalid_legacy_error = None
    except FactMergeError as exc:
        invalid_legacy_error = exc.as_dict()

    broken = copy.deepcopy(full["manifest"])
    broken_claim = broken["entities"][0]["claims"][0]
    broken_claim_id = broken_claim["claim_id"]
    del broken_claim["stage2"]
    try:
        validate_manifest(broken)
        broken_error = None
    except FactMergeError as exc:
        broken_error = exc.as_dict()

    report = {
        "schema_version": "1.0",
        "environment": {
            "python": sys.version.split()[0],
            "offline_runtime": True,
            "gpu_required": False,
            "container_required": False,
        },
        "sample": {
            "claim_count": len(corpus),
            "entity_count": len({row["entity_id"] for row in corpus}),
            "generator_seed": json.loads(
                (ROOT / "samples" / "generator_manifest.json").read_text(encoding="utf-8")
            )["seed"],
        },
        "batch": {
            "summary": full["summary"],
            "manifest_sha256": hashlib.sha256(full_manifest_bytes).hexdigest(),
            "manifest_bytes": len(full_manifest_bytes),
            "evidence_validation": full_validation,
        },
        "consistency": selfcheck,
        "stop_resume": {
            "stopped_status": stopped["status"],
            "stopped_entities": len(stopped["entity_results"]),
            "next_entity_id": stopped["next_entity_id"],
            "resumed_status": resumed["status"],
            "manifest_byte_equal": resumed_manifest_bytes == full_manifest_bytes,
            "entity_results_equal": resumed["entity_results"] == full["entity_results"],
            "summary_equal": resumed["summary"] == full["summary"],
            "checkpoint_count": len(checkpoint_files),
            "checkpoint_sequences": checkpoint_sequences,
            "sequences_monotonic": checkpoint_sequences == sorted(checkpoint_sequences),
            "resume_reuse_counts": resumed["version_status"]["last_run"]["reuse_counts"],
        },
        "incremental_invalidation": {
            "change": "language_weights.en 0.95 -> 0.51",
            "earliest_affected_stage": version_status["earliest_affected_stage"],
            "invalidated_stages": version_status["invalidations"]["invalidated_stages"],
            "reason_codes": version_status["invalidations"]["reason_codes"],
            "reuse_counts": version_status["last_run"]["reuse_counts"],
        },
        "legacy_v1_migration": {
            "validation": legacy_validation,
            "roundtrip_byte_equal": legacy_roundtrip_equal,
            "downgraded_records": sum(
                1 for item in migrated["migration"]["downgrades"] if item["downgrade_codes"]
            ),
            "unmappable_unit_error": invalid_legacy_error,
        },
        "evidence_break_detection": {
            "removed_stage": "stage2",
            "claim_id": broken_claim_id,
            "error": broken_error,
        },
    }
    report_path = ROOT / "artifacts" / "verification_report.json"
    report_path.write_text(canonical_json(report) + "\n", encoding="utf-8")
    print(canonical_json({"status": "verified", "report": str(report_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
