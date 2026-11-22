"""Environment capability checks and checkpoint fault-injection probes."""

from __future__ import annotations

import multiprocessing
import os
import platform
import socket
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, digest_obj
from .checkpoints import CheckpointStore
from .config import DEFAULT_CONFIG, validate_config
from .decode import decode_shard
from .demo_generator import generate_shard
from .errors import WorkbenchError
from .shards import import_shard, load_shard_file, shard_path
from .workspace import create_workspace


def doctor() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    checks["python"] = {
        "version": sys.version.split()[0],
        "ok": sys.version_info >= (3, 10),
        "minimum": "3.10",
    }
    try:
        import numpy as np

        checks["numpy"] = {"version": np.__version__, "ok": True}
    except Exception as exc:
        checks["numpy"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    checks["platform"] = {
        "system": platform.system(),
        "release": platform.release(),
        "python_implementation": platform.python_implementation(),
    }
    checks["file_lock"] = {
        "backend": "msvcrt" if os.name == "nt" else "fcntl.flock",
        "ok": True,
    }
    checks["process_pool"] = {
        "start_methods": multiprocessing.get_all_start_methods(),
        "ok": "spawn" in multiprocessing.get_all_start_methods(),
    }
    with tempfile.TemporaryDirectory(prefix="segval-doctor-") as temp_dir:
        path = Path(temp_dir) / "atomic-target.bin"
        try:
            atomic_write_bytes(path, b"segval-atomic-check")
            checks["atomic_replace"] = {"ok": path.read_bytes() == b"segval-atomic-check"}
        except Exception as exc:
            checks["atomic_replace"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        checks["loopback_bind"] = {"ok": True, "tested_port": port}
    except Exception as exc:
        checks["loopback_bind"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {
        "ok": all(
            bool(checks[key].get("ok"))
            for key in ("python", "numpy", "file_lock", "process_pool", "atomic_replace", "loopback_bind")
        ),
        "checks": checks,
    }


def _publish_minimal_decode(workspace_path: str, shard_index: int) -> dict[str, Any]:
    workspace = create_workspace(Path(workspace_path))
    shard = load_shard_file(shard_path(workspace.shards_dir, shard_index))
    config = validate_config(DEFAULT_CONFIG)
    artifact = decode_shard(shard, config)
    store = CheckpointStore(workspace.root)
    from .engine import _publish_decode

    record = _publish_decode(store, workspace, shard, config, artifact)
    return {
        "shard_index": shard_index,
        "sequence": record["sequence"],
        "artifact_digest": record["artifact_digest"],
    }


def checkpoint_fault_probe(root: Path | None = None) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="segval-checkpoint-probe-") as temp_dir:
        workspace = create_workspace(Path(temp_dir))
        for shard_index in (1, 2, 3, 4):
            import_shard(workspace.root, generate_shard(shard_index))
        store = CheckpointStore(workspace.root)

        missing_prior = None
        try:
            store.publish(
                shard_id="shard-0002",
                shard_index=2,
                stage="match",
                input_fingerprint="probe-missing-prior",
                config_fingerprint="probe",
                config_version="1",
                artifact={"artifact_digest": digest_obj({"probe": 1})},
                upstream_digest=digest_obj({"probe": 0}),
            )
        except WorkbenchError as exc:
            missing_prior = exc.to_dict()

        first = _publish_minimal_decode(str(workspace.root), 1)
        duplicate = None
        shard = load_shard_file(shard_path(workspace.shards_dir, 1))
        config = validate_config(DEFAULT_CONFIG)
        artifact = decode_shard(shard, config)
        try:
            from .engine import _publish_decode

            _publish_decode(store, workspace, shard, config, artifact)
        except WorkbenchError as exc:
            duplicate = exc.to_dict()

        concurrent_root = Path(temp_dir) / "concurrent"
        concurrent_workspace = create_workspace(concurrent_root)
        for shard_index in (1, 2, 3, 4):
            import_shard(concurrent_workspace.root, generate_shard(shard_index))
        concurrent_store = CheckpointStore(concurrent_workspace.root)
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=4, mp_context=context) as executor:
            concurrent_results = list(
                executor.map(
                    _publish_minimal_decode,
                    [str(concurrent_workspace.root)] * 4,
                    [1, 2, 3, 4],
                )
            )
        sequences = sorted(item["sequence"] for item in concurrent_results)
        journal = concurrent_store.history()
        journal_contiguous = [item["sequence"] for item in journal] == list(range(1, len(journal) + 1))
        return {
            "ok": (
                missing_prior is not None
                and missing_prior.get("code") == "E_INVALID_STAGE"
                and duplicate is not None
                and duplicate.get("code") == "E_STALE_CHECKPOINT"
                and len(set(sequences)) == 4
                and sequences == [1, 2, 3, 4]
                and journal_contiguous
            ),
            "missing_prior": missing_prior,
            "duplicate_publication": duplicate,
            "first_publication": first,
            "concurrent_publications": sorted(concurrent_results, key=lambda item: item["shard_index"]),
            "concurrent_sequences_unique": len(set(sequences)) == 4,
            "concurrent_sequences": sequences,
            "journal_count": len(journal),
            "journal_sequences_contiguous": journal_contiguous,
        }

