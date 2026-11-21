"""Deterministic end-to-end demo, including bitwidth-triggered incremental rebuild."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest_obj
from .configs import default_config
from .datagen import generate_dataset
from .dataset import Dataset
from .engine import RetrievalEngine


def run_demo(
    *,
    workspace: str | Path = "runtime/demo-workspace",
    data_dir: str | Path = "data/sample",
    report_path: str | Path = "artifacts/demo-report.json",
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    data_path = Path(data_dir).resolve()
    report_file = Path(report_path).resolve()
    if workspace_path.exists():
        shutil.rmtree(workspace_path)
    generated = generate_dataset(data_path, shards=3, items_per_shard=6, seed=20261122)
    dataset = Dataset.load(data_path)
    dataset.assert_valid()
    engine = RetrievalEngine(workspace_path, dataset_dir=data_path)
    imported = engine.import_dataset(data_path)
    engine.set_config(default_config(8, "per_dim"))

    stop_run = engine.build(stop_after_units=4)
    resume_run = engine.resume()
    if resume_run.get("status") != "complete":
        raise RuntimeError("demo resume did not complete")
    eight_status = engine.status()
    eight_audit = engine.checkpoints.audit()
    first_item = sorted(dataset.items, key=lambda item: item.item_id)[0]
    text_query = first_item.caption
    q8 = engine.search(mode="text2image", query=text_query, k=min(5, dataset.item_count))
    i8 = engine.search(
        mode="image2text", query_item_id=first_item.item_id, k=min(5, dataset.item_count)
    )
    export8 = engine.export_report(workspace_path / "reports" / "quant8")
    run8 = resume_run["run_digest"]
    q8_digest = digest_obj({"text2image": q8["top_k"], "image2text": i8["top_k"]})

    engine.set_config(patch={"bits": 4})
    rebuild4 = engine.build()
    q4 = engine.search(mode="text2image", query=text_query, k=min(5, dataset.item_count))
    i4 = engine.search(
        mode="image2text", query_item_id=first_item.item_id, k=min(5, dataset.item_count)
    )
    export4 = engine.export_report(workspace_path / "reports" / "quant4")
    verification = engine.verify_full_consistency()
    q4_digest = digest_obj({"text2image": q4["top_k"], "image2text": i4["top_k"]})
    four_status = engine.status()
    final_audit = engine.checkpoints.audit()

    records = engine.checkpoints.recover().records
    summary = {
        "schema": "cmir.demo-report.v1",
        "generated_dataset": {
            "digest": generated["digest"],
            "dataset_digest": imported["dataset_digest"],
            "item_count": generated["item_count"],
            "shards": generated["shards"],
        },
        "quant8": {
            "run_digest": run8,
            "stopped_after_units": stop_run.get("published_units", 0),
            "resume_status": resume_run.get("status"),
            "checkpoint_status": eight_audit["status"],
            "checkpoint_last_seq": eight_status["state"]["checkpoint_last_seq"],
            "text2image_top1": q8["top_k"][0],
            "image2text_top1": i8["top_k"][0],
            "topk_digest": q8_digest,
            "export_query_results_digest": export8["query_results_digest"],
        },
        "quant4_incremental": {
            "run_digest": rebuild4["run_digest"],
            "checkpoint_last_seq": four_status["state"]["checkpoint_last_seq"],
            "earliest_stage": rebuild4["invalidations"][-1]["earliest_stage"],
            "reason": rebuild4["invalidations"][-1]["reason"],
            "reused_stages": rebuild4["invalidations"][-1]["reused_stages"],
            "text2image_top1": q4["top_k"][0],
            "image2text_top1": i4["top_k"][0],
            "topk_digest": q4_digest,
            "export_query_results_digest": export4["query_results_digest"],
        },
        "full_consistency": {
            "consistent": verification["consistent"],
            "full_run_digest": verification["full_run_digest"],
            "incremental_run_digest": verification["incremental_run_digest"],
            "probe_result_digests": verification["probe_result_digests"],
        },
        "checkpoints": {
            "audit_status": final_audit["status"],
            "record_count": len(records),
            "sequences": [record["seq"] for record in records],
            "sequence_contiguous": [record["seq"] for record in records]
            == list(range(1, len(records) + 1)),
            "stages": [
                {
                    "seq": record["seq"],
                    "shard_id": record["shard_id"],
                    "stage": record["stage"],
                    "shard_seq": record["shard_seq"],
                    "cycle": record["cycle"],
                    "reused": record.get("reused", False),
                }
                for record in records
            ],
        },
        "determinism_note": "All digests exclude timestamps and absolute workspace paths.",
    }
    summary["demo_summary_digest"] = digest_obj(
        {
            "dataset": summary["generated_dataset"],
            "quant8_run": run8,
            "quant4_run": rebuild4["run_digest"],
            "quant8_topk_digest": q8_digest,
            "quant4_topk_digest": q4_digest,
            "full_consistency_probe_digests": verification["probe_result_digests"],
        }
    )
    report_file.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(report_file, summary)
    summary["report_path"] = str(report_file)
    return summary
