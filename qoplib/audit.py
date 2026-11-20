"""Audit report export and run-state query helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .checkpoint import PIPELINE_STAGES, CheckpointStore
from .fingerprints import fingerprint_json
from .io_util import atomic_write_json


def export_audit_report(run_dir: str | Path) -> Path:
    run_dir = Path(run_dir)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if \
        summary_path.exists() else {}

    stage_states: List[Dict[str, Any]] = []
    for shard_dir in sorted((run_dir / "shards").iterdir()) \
            if (run_dir / "shards").exists() else []:
        if not shard_dir.is_dir():
            continue
        for stage in PIPELINE_STAGES:
            record_path = shard_dir / stage / "record.json"
            entry = {"shard_id": shard_dir.name, "stage": stage,
                     "present": record_path.exists()}
            if record_path.exists():
                record = json.loads(record_path.read_text(encoding="utf-8"))
                entry.update({
                    "sequence": record["sequence"],
                    "stage_fingerprint": record["stage_fingerprint"],
                    "input_fingerprint": record["input_fingerprint"],
                    "config_version": record["config_version"],
                })
            stage_states.append(entry)

    report = {
        "summary": summary,
        "stage_states": stage_states,
        "counter": _read_optional_json(run_dir / "counter.json"),
        "report_fingerprint": "",
    }
    report["report_fingerprint"] = fingerprint_json(
        {"summary": summary, "stage_states": stage_states,
         "counter": report["counter"]})
    target = run_dir / "audit_report.json"
    atomic_write_json(target, report)
    return target


def query_status(run_dir: str | Path) -> Dict[str, Any]:
    run_dir = Path(run_dir)
    store = CheckpointStore(run_dir)
    records = store.all_records()
    return {
        "run_dir": str(run_dir),
        "next_sequence": store.next_sequence(),
        "latest_sequence": store.latest_sequence(),
        "completed_shards": sorted(store.completed_shards()),
        "records": records,
        "stop_requested": (run_dir / "STOP").exists(),
    }


def query_fingerprints(run_dir: str | Path) -> List[Dict[str, Any]]:
    run_dir = Path(run_dir)
    store = CheckpointStore(run_dir)
    rows: List[Dict[str, Any]] = []
    for record in store.all_records():
        rows.append({
            "shard_id": record["shard_id"],
            "stage": record["stage"],
            "sequence": record["sequence"],
            "stage_fingerprint": record["stage_fingerprint"],
            "input_fingerprint": record["input_fingerprint"],
            "config_version": record["config_version"],
        })
    return rows


def _read_optional_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
