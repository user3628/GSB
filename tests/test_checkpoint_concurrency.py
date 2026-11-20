from __future__ import annotations

import json
import multiprocessing
import os
import subprocess
import sys
from pathlib import Path

import pytest
from scipy.io import wavfile

from vadkit.checkpoint import CheckpointStore
from vadkit.errors import VadError
from vadkit.pipeline import evaluate_dataset, workspace_status


def _publish_worker(
    workspace: str,
    generation: int,
    sequence: int,
    order: int,
    chunk_id: str,
    connection,
) -> None:
    store = CheckpointStore(workspace)
    try:
        record = store.publish(
            generation=generation,
            sequence=sequence,
            order=order,
            chunk_id=chunk_id,
            input_sha256=f"input-{order}",
            version_fingerprint=f"version-{order}",
            config_fingerprint="config",
            endpoint_summary={"batch_endpoints": [], "stream_endpoints": []},
        )
        connection.send(("ok", record["sequence"]))
    except VadError as exc:
        connection.send(("error", exc.code))
    finally:
        connection.close()


def test_checkpoint_fields_and_graceful_resume(dataset_dir: Path, tmp_path: Path) -> None:
    fresh = evaluate_dataset(dataset_dir / "manifest.json", tmp_path / "fresh", tolerance_ms=80.0)
    workspace = tmp_path / "resume"
    stopped = evaluate_dataset(
        dataset_dir / "manifest.json",
        workspace,
        tolerance_ms=80.0,
        stop_after_chunks=2,
    )
    assert stopped.status == "stopped"
    status = workspace_status(dataset_dir / "manifest.json", workspace)
    assert status["checkpoint_count"] == 2
    assert status["stop_requested"]
    records_dir = workspace / "generations" / f"{stopped.generation:08d}" / "records"
    records = [json.loads(path.read_text()) for path in sorted(records_dir.glob("*.json"))]
    assert [record["sequence"] for record in records] == [1, 2]
    for record in records:
        assert {
            "chunk_id",
            "input_sha256",
            "config_fingerprint",
            "endpoint_summary",
            "sequence",
            "version_fingerprint",
            "record_sha256",
        } <= set(record)

    resumed = evaluate_dataset(
        dataset_dir / "manifest.json",
        workspace,
        tolerance_ms=80.0,
        clear_stop=True,
    )
    assert resumed.status == "complete"
    assert resumed.checkpoint_reused_chunks == ("clear_01", "low_snr_02")
    assert fresh.result_path.read_bytes() == resumed.result_path.read_bytes()


def test_forced_process_termination_recovers_from_records_only(
    dataset_dir: Path, tmp_path: Path
) -> None:
    fresh = evaluate_dataset(dataset_dir / "manifest.json", tmp_path / "crash-fresh", tolerance_ms=80.0)
    workspace = tmp_path / "crash"
    command = [
        sys.executable,
        "-m",
        "vadkit",
        "evaluate",
        "--manifest",
        str(dataset_dir / "manifest.json"),
        "--workspace",
        str(workspace),
        "--crash-after-chunks",
        "2",
    ]
    crashed = subprocess.run(command, cwd=Path.cwd(), capture_output=True, text=True, timeout=60)
    assert crashed.returncode == 70
    assert not (workspace / "result.json").exists()
    status = workspace_status(dataset_dir / "manifest.json", workspace)
    assert status["checkpoint_count"] == 2

    records_dir = workspace / "generations" / f"{status['generation']:08d}" / "records"
    (records_dir / ".00000003.json.partial.tmp").write_text('{"half":', encoding="utf-8")
    resumed = evaluate_dataset(
        dataset_dir / "manifest.json",
        workspace,
        tolerance_ms=80.0,
        clear_stop=True,
    )
    assert resumed.status == "complete"
    resume_bytes = resumed.result_path.read_bytes()
    assert resume_bytes == fresh.result_path.read_bytes()


def test_concurrent_publishers_only_advance_contiguously(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "concurrent")
    meta = store.ensure_generation("dataset", "config", 4)
    store.publish(
        generation=meta.generation,
        sequence=1,
        order=0,
        chunk_id="chunk-0",
        input_sha256="input-0",
        version_fingerprint="version-0",
        config_fingerprint="config",
        endpoint_summary={"batch_endpoints": [], "stream_endpoints": []},
    )

    context = multiprocessing.get_context("fork")
    parent_a, child_a = context.Pipe(False)
    parent_b, child_b = context.Pipe(False)
    process_a = context.Process(
        target=_publish_worker,
        args=(str(store.root), meta.generation, 2, 1, "chunk-1", child_a),
    )
    process_b = context.Process(
        target=_publish_worker,
        args=(str(store.root), meta.generation, 2, 1, "chunk-1", child_b),
    )
    process_a.start()
    process_b.start()
    results = [parent_a.recv(), parent_b.recv()]
    process_a.join(timeout=20)
    process_b.join(timeout=20)
    assert sorted(item[0] for item in results) == ["error", "ok"]
    assert [item[1] for item in results if item[0] == "error"] == ["E_STALE_CHECKPOINT"]

    with pytest.raises(VadError) as duplicate:
        store.publish(
            generation=meta.generation,
            sequence=2,
            order=1,
            chunk_id="chunk-1",
            input_sha256="input-1",
            version_fingerprint="version-1",
            config_fingerprint="config",
            endpoint_summary={"batch_endpoints": [], "stream_endpoints": []},
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(VadError) as skipped:
        store.publish(
            generation=meta.generation,
            sequence=4,
            order=3,
            chunk_id="chunk-3",
            input_sha256="input-3",
            version_fingerprint="version-3",
            config_fingerprint="config",
            endpoint_summary={"batch_endpoints": [], "stream_endpoints": []},
        )
    assert skipped.value.code == "E_INVALID_STAGE"


def test_one_changed_chunk_invalidates_only_itself(dataset_copy: Path, tmp_path: Path) -> None:
    manifest = dataset_copy / "manifest.json"
    workspace = tmp_path / "versioned"
    initial = evaluate_dataset(manifest, workspace, tolerance_ms=80.0)
    assert initial.status == "complete"

    wav_path = dataset_copy / "audio" / "01_clear.wav"
    sample_rate, samples = wavfile.read(wav_path)
    modified = samples.copy()
    modified[::17] = (modified[::17].astype("int32") + 1).astype("int16")
    wavfile.write(wav_path, sample_rate, modified)

    status = workspace_status(manifest, workspace)
    states = {stage["chunk_id"]: stage["state"] for stage in status["stages"]}
    assert states["clear_01"] == "invalidated"
    assert states["low_snr_02"] == "reusable"
    assert states["breath_pause_03"] == "reusable"
    assert states["impulse_04"] == "reusable"

    rebuilt = evaluate_dataset(manifest, workspace, tolerance_ms=80.0)
    assert rebuilt.generation == initial.generation + 1
    assert rebuilt.computed_chunks == ("clear_01",)
    assert set(rebuilt.cache_reused_chunks) == {
        "low_snr_02",
        "breath_pause_03",
        "impulse_04",
    }

