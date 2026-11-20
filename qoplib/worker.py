"""Top-level worker entry point (picklable for the spawn start method)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .checkpoint import CheckpointStore
from .errors import StructuredError
from .manifest import load_manifest
from .pipeline import run_shard_pipeline


def process_shard_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one shard inside a worker process.

    ``task`` only holds primitive types / paths so it survives the spawn
    start method on Windows and POSIX identically.
    """

    shard_id = task["shard_id"]
    stage = "dispatch"
    try:
        manifest = load_manifest(task["manifest_path"])
        shard = manifest.shard_by_id(shard_id)
        store = CheckpointStore(task["run_dir"])
        stage = "pipeline"
        result = run_shard_pipeline(
            shard=shard,
            data_dir=Path(task["data_dir"]),
            manifest=manifest,
            store=store,
            run_fingerprint=task["run_fingerprint"],
            check_tolerance=bool(task.get("check_tolerance", True)),
            auto_commit=False,
        )
        return result.to_dict()
    except BaseException as exc:  # structured, never a timeout kill report
        error = StructuredError.from_exception(
            exc, shard_id=shard_id, stage=stage)
        return {
            "shard_id": shard_id,
            "shard_index": task.get("shard_index"),
            "status": "worker_error",
            "stages": [],
            "invalidations": [],
            "final_summary": {},
            "error": error.to_dict(),
            "sequence": None,
        }
