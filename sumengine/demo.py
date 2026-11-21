"""Reproducible end-to-end demonstration and verification report."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .config import config_from_file
from .engine import SummaryEngine
from .errors import EngineError
from .samples import write_samples
from .utils import atomic_write_bytes, canonical_json


def _prepare_workspace(source_samples: Path, target: Path) -> SummaryEngine:
    if target.exists():
        shutil.rmtree(target)
    (target / "samples").mkdir(parents=True)
    for name in ("documents.jsonl", "config.yaml"):
        shutil.copy2(source_samples / name, target / "samples" / name)
    return SummaryEngine(target)


def _export_bytes(engine: SummaryEngine) -> bytes:
    engine.export()
    return (engine.store.exports_dir / "final_summaries.json").read_bytes()


def run_demo(base: str | Path = ".") -> dict[str, Any]:
    base_path = Path(base).resolve()
    demo_root = base_path / "artifacts" / "demo"
    if demo_root.exists():
        shutil.rmtree(demo_root)
    samples_dir = demo_root / "samples"
    write_samples(samples_dir)
    sample_documents = samples_dir / "documents.jsonl"
    sample_config = samples_dir / "config.yaml"

    parallel = _prepare_workspace(samples_dir, demo_root / "parallel")
    serial = _prepare_workspace(samples_dir, demo_root / "serial")
    stopped = _prepare_workspace(samples_dir, demo_root / "stop_resume")

    parallel_run = parallel.run(mode="parallel")
    parallel_export = _export_bytes(parallel)
    parallel_digest = parallel.export()["digest"]

    serial_run = serial.run(mode="serial")
    serial_export = _export_bytes(serial)
    serial_digest = serial.export()["digest"]

    stop_run = stopped.run(mode="serial", stop_after_docs=2)
    stop_processed = [doc["doc_id"] for doc in stop_run["documents"]]
    stop_deferred = list(stop_run["deferred"])
    resume_run = stopped.resume(mode="serial")
    resume_export = _export_bytes(stopped)
    resume_digest = stopped.export()["digest"]

    incremental = _prepare_workspace(samples_dir, demo_root / "incremental")
    incremental.run(mode="serial")
    edit = incremental.edit_summary(
        "news-zh-002",
        "【人工复核稿】农业数字化试点扩大，平台接入超过三百家合作社；后续将公开节水与服务质量数据。",
    )
    modified_config = config_from_file(sample_config)
    modified_config["compression"]["synonym_replacements"]["迅速"] = "较快"
    incremental.save_config(modified_config)
    incremental_run = incremental.run(mode="serial")
    incremental_export = _export_bytes(incremental)
    incremental_digest = incremental.export()["digest"]
    incremental_docs = {doc["doc_id"]: doc for doc in incremental_run["documents"]}
    edited_doc = incremental_docs.get("news-zh-002", {})

    error_engine = _prepare_workspace(samples_dir, demo_root / "errors")
    error_engine.import_documents(
        [
            {
                "doc_id": "error-too-short",
                "title": "短讯",
                "body": "短讯。",
                "language": "zh",
                "source": "desk",
                "target_length": "short",
            }
        ]
    )
    short_run = error_engine.run(mode="serial")
    short_code = None
    if short_run.get("documents_with_errors"):
        short_code = short_run["documents_with_errors"][0].get("code")
    low_latency = error_engine.get_config()
    low_latency["budget"]["latency_budget_ms"] = 1
    latency_run = error_engine.run(mode="serial", config=low_latency)
    latency_code = (latency_run.get("error") or {}).get("code")
    if latency_code is None:
        latency_codes = [item.get("code") for item in latency_run.get("schedule", {}).get("unprocessed", [])]
        latency_code = "E_LATENCY_BUDGET" if "E_LATENCY_BUDGET" in latency_codes else None

    stale_code = None
    try:
        records = stopped.store.checkpoint_records()
        if records:
            stolen = dict(records[-1])
            stolen.pop("sequence", None)
            stopped.store.publish_records([stolen], expected_doc_id="another-document")
    except EngineError as exc:
        stale_code = exc.code

    checks = {
        "parallel_serial_byte_identical": parallel_export == serial_export,
        "stop_resume_byte_identical": parallel_export == resume_export,
        "stop_was_stopped": stop_run.get("status") == "stopped" and bool(stop_deferred),
        "resume_completed": resume_run.get("status") == "complete" and not resume_run.get("deferred"),
        "budget_unprocessed_recorded": any(
            item.get("code") == "E_GLOBAL_BUDGET" for item in parallel_run["schedule"]["unprocessed"]
        ),
        "incremental_extract_reused": bool(edited_doc) and "extract" in edited_doc.get("reused_stages", []),
        "incremental_compression_rebuilt": "compress" in edited_doc.get("changed_stages", []),
        "edited_summary_preserved": edited_doc.get("summary", "").startswith("【人工复核稿】"),
        "edited_summary_needs_review": bool(edited_doc.get("final", {}).get("needs_review")),
        "input_too_short_code": short_code == "E_INPUT_TOO_SHORT",
        "latency_budget_code": latency_code == "E_LATENCY_BUDGET",
        "stale_checkpoint_code": stale_code == "E_STALE_CHECKPOINT",
    }
    report = {
        "ok": all(checks.values()),
        "seed": 20261122,
        "sample_document_count": len(sample_documents.read_text(encoding="utf-8").splitlines()),
        "checks": checks,
        "full_run": {
            "status": parallel_run.get("status"),
            "accepted_doc_ids": parallel_run["schedule"]["accepted_doc_ids"],
            "unprocessed": parallel_run["schedule"]["unprocessed"],
            "budget_usage": parallel_run["budget_usage"],
            "parallel_export_digest": parallel_digest,
            "serial_export_digest": serial_digest,
            "summaries": [
                {
                    "doc_id": doc["doc_id"],
                    "route": doc["final"]["candidate_route"],
                    "fusion_score": doc["final"]["fusion_score"],
                    "summary": doc["summary"],
                }
                for doc in parallel_run["documents"]
            ],
        },
        "stop_resume": {
            "stop_status": stop_run.get("status"),
            "processed_before_stop": stop_processed,
            "deferred_after_stop": stop_deferred,
            "resume_status": resume_run.get("status"),
            "resume_export_digest": resume_digest,
        },
        "incremental": {
            "edited_doc": "news-zh-002",
            "edit_version": edit["edit_version"],
            "reused_stages_after_synonym_change": edited_doc.get("reused_stages"),
            "changed_stages_after_synonym_change": edited_doc.get("changed_stages"),
            "needs_review": edited_doc.get("final", {}).get("needs_review"),
            "export_digest": incremental_digest,
        },
        "error_checks": {
            "E_INPUT_TOO_SHORT": short_code,
            "E_LATENCY_BUDGET": latency_code,
            "E_STALE_CHECKPOINT": stale_code,
        },
        "artifacts": {
            "parallel_export": "artifacts/demo/parallel/exports/final_summaries.json",
            "serial_export": "artifacts/demo/serial/exports/final_summaries.json",
            "resume_export": "artifacts/demo/stop_resume/exports/final_summaries.json",
            "incremental_export": "artifacts/demo/incremental/exports/final_summaries.json",
        },
    }
    report_bytes = canonical_json(report, indent=2).encode("utf-8") + b"\n"
    atomic_write_bytes(base_path / "artifacts" / "demo_report.json", report_bytes)
    return report
