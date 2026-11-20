from __future__ import annotations

import json
import shutil
import wave
from pathlib import Path
from typing import Any

import numpy as np

from .audio import StubTranscriptionAdapter, load_manifest
from .config import WorkbenchConfig, digest_value
from .errors import WorkbenchError
from .workspace import Workspace


DEMO_SEED = 20_261_120
DEMO_CHUNKS = [
    {
        "order": 1,
        "chunk_id": "chunk-01",
        "file": "audio/chunk-01.wav",
        "reference_text": "大家好，欢迎使用语音识别分片转写工作台。",
        "language": "zh-CN",
        "duration_ms": 2200,
        "frequency_hz": 220.0,
    },
    {
        "order": 2,
        "chunk_id": "chunk-02",
        "file": "audio/chunk-02.wav",
        "reference_text": "This local deterministic demo mixes English and 中文 seamlessly.",
        "language": "en-US",
        "duration_ms": 2600,
        "frequency_hz": 330.0,
    },
    {
        "order": 3,
        "chunk_id": "chunk-03",
        "file": "audio/chunk-03.wav",
        "reference_text": "最终检查 JSON、SRT 与 WebVTT 的往返一致性和版本恢复。",
        "language": "zh-CN",
        "duration_ms": 3200,
        "frequency_hz": 440.0,
    },
]


def _write_wav(path: Path, *, seed: int, chunk_index: int, duration_ms: int, frequency_hz: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame_count = round(duration_ms * 16_000 / 1000)
    rng = np.random.default_rng(seed + chunk_index)
    times = np.arange(frame_count, dtype=np.float64) / 16_000.0
    tone = 0.32 * np.sin(2 * np.pi * frequency_hz * times)
    overtone = 0.12 * np.sin(2 * np.pi * frequency_hz * 2.0 * times)
    noise = 0.015 * rng.standard_normal(frame_count)
    envelope = np.minimum(1.0, np.minimum(times / 0.05, (duration_ms / 1000.0 - times) / 0.05))
    envelope = np.clip(envelope, 0.0, 1.0)
    signal = np.clip((tone + overtone + noise) * envelope, -1.0, 1.0)
    pcm = np.round(signal * 32767.0).astype("<i2")
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(16_000)
        handle.writeframes(pcm.tobytes())


def generate_demo_data(
    root: str | Path,
    *,
    seed: int = DEMO_SEED,
    clean: bool = True,
) -> dict[str, Any]:
    root = Path(root).resolve()
    demo_dir = root / "demo_data"
    if clean and demo_dir.exists():
        shutil.rmtree(demo_dir)
    audio_dir = demo_dir / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)
    manifest_chunks: list[dict[str, Any]] = []
    for index, specification in enumerate(DEMO_CHUNKS, start=1):
        _write_wav(
            demo_dir / specification["file"],
            seed=seed,
            chunk_index=index,
            duration_ms=int(specification["duration_ms"]),
            frequency_hz=float(specification["frequency_hz"]),
        )
        manifest_chunks.append(
            {
                key: specification[key]
                for key in (
                    "order",
                    "chunk_id",
                    "file",
                    "reference_text",
                    "language",
                    "duration_ms",
                )
            }
        )
    manifest = {
        "schema_version": 1,
        "seed": seed,
        "generator": "numpy-fixed-seed-wav-v1",
        "audio_contract": {
            "sample_rate_hz": 16_000,
            "channels": 1,
            "sample_width_bits": 16,
            "encoding": "pcm_s16le",
        },
        "chunks": manifest_chunks,
    }
    manifest_path = demo_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    _, chunks = load_manifest(manifest_path)
    adapter = StubTranscriptionAdapter(WorkbenchConfig())
    first_chunk = chunks["chunk-01"]
    generated_words = adapter.transcribe(
        chunk_id=first_chunk.chunk_id,
        audio_path=Path(first_chunk.path),
        duration_ms=first_chunk.duration_ms,
        reference_text=first_chunk.reference_text,
        language=first_chunk.language,
        source_version=0,
    )
    legacy_segments = []
    for word in generated_words:
        legacy_segments.append(
            {
                "start": round(word.start_ms / 1000.0, 3),
                "end": round(word.end_ms / 1000.0, 3),
                "word": word.text,
            }
        )
    legacy = {
        "version": "legacy-v1",
        "schema_version": 0,
        "audio": "chunk-01",
        "language": "zh-CN",
        "segments": legacy_segments,
    }
    (demo_dir / "legacy_transcript.json").write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    invalid_words = [
        generated_words[0].to_dict(),
        generated_words[1].to_dict(),
    ]
    invalid_words[0]["start_ms"] = -20
    invalid_words[1]["start_ms"] = 100
    invalid_words[1]["end_ms"] = 60
    invalid_words[1]["entry_id"] = "invalid-overlap-entry"
    invalid_timeline = {
        "format": "workbench-json",
        "schema_version": 2,
        "time_unit": "ms",
        "chunks": [
            {
                "chunk_id": "chunk-01",
                "order": 1,
                "duration_ms": first_chunk.duration_ms,
                "language": "zh-CN",
            }
        ],
        "words": invalid_words,
    }
    (demo_dir / "invalid_timeline.json").write_text(
        json.dumps(invalid_timeline, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    seed_workspace_dir = demo_dir / ".snapshot_seed_workspace"
    if seed_workspace_dir.exists():
        shutil.rmtree(seed_workspace_dir)
    sample_workspace = Workspace(seed_workspace_dir)
    sample_workspace.bootstrap(manifest_path)
    replacement_snapshot = (seed_workspace_dir / "snapshot.json").read_text(
        encoding="utf-8"
    )
    (demo_dir / "replacement_snapshot.json").write_text(
        replacement_snapshot, encoding="utf-8"
    )
    shutil.rmtree(seed_workspace_dir)
    corrupt = json.loads(replacement_snapshot)
    corrupt["words"][0]["text"] = corrupt["words"][0]["text"] + "-corrupted"
    (demo_dir / "corrupt_snapshot.json").write_text(
        json.dumps(corrupt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return {
        "demo_dir": str(demo_dir),
        "manifest_path": str(manifest_path),
        "legacy_path": str(demo_dir / "legacy_transcript.json"),
        "invalid_timeline_path": str(demo_dir / "invalid_timeline.json"),
        "corrupt_snapshot_path": str(demo_dir / "corrupt_snapshot.json"),
        "chunks": manifest_chunks,
    }


def _copy_runtime(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _seal_snapshot(body: dict[str, Any]) -> dict[str, Any]:
    value = dict(body)
    value["snapshot_digest"] = digest_value(value)
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _error_code(exception: WorkbenchError) -> str:
    return exception.code


def run_demo(root: str | Path, *, seed: int = DEMO_SEED) -> dict[str, Any]:
    root = Path(root).resolve()
    errors: dict[str, int] = {}

    def count_error(exception: WorkbenchError) -> None:
        errors[exception.code] = errors.get(exception.code, 0) + 1

    demo = generate_demo_data(root, seed=seed, clean=True)
    runtime_root = root / "artifacts" / "demo_workspace"
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    workspace = Workspace(runtime_root)
    boot = workspace.bootstrap(demo["manifest_path"])
    replacement = runtime_root / "snapshot.json"
    replacement_copy = root / "demo_data" / "replacement_snapshot.json"
    shutil.copy2(replacement, replacement_copy)

    roundtrip = workspace.roundtrip_check(version=boot["version"])
    first_state = workspace.load_version(boot["version"])
    first_chunk_word = min(
        (word for word in first_state.words if word.chunk_id == "chunk-01"),
        key=lambda word: (word.start_ms, word.entry_id),
    )
    edited = workspace.edit(
        [
            {
                "op": "update_word",
                "entry_id": first_chunk_word.entry_id,
                "text": first_chunk_word.text + "，",
                "confidence": 0.977,
            }
        ],
        expected_version=boot["version"],
    )
    forked = workspace.fork_edit(
        [
            {
                "op": "update_word",
                "entry_id": first_chunk_word.entry_id,
                "text": first_chunk_word.text + "!",
                "confidence": 0.866,
            }
        ],
        parent_version=boot["version"],
        expected_version=edited["version"],
    )
    merged = workspace.merge_versions(
        left_version=edited["version"],
        right_version=forked["version"],
        strategy="last-wins",
        expected_version=forked["version"],
    )
    legacy_text = Path(demo["legacy_path"]).read_text(encoding="utf-8")
    migrated = workspace.import_text(
        content=legacy_text,
        fmt="json",
        expected_version=merged["version"],
        chunk_id="chunk-01",
    )
    version_before_invalid = migrated["version"]
    invalid_text = Path(demo["invalid_timeline_path"]).read_text(encoding="utf-8")
    try:
        workspace.import_text(
            content=invalid_text,
            fmt="json",
            expected_version=version_before_invalid,
            chunk_id="chunk-01",
        )
    except WorkbenchError as exc:
        count_error(exc)
        assert exc.code == "E_TIMELINE_ERROR"
    after_invalid = workspace.load_state()
    assert after_invalid.version == version_before_invalid

    stopped = workspace.request_stop()
    try:
        workspace.edit([], expected_version=after_invalid.version)
    except WorkbenchError as exc:
        count_error(exc)
        assert exc.code == "E_STOPPING"
    recovered = workspace.recover()
    assert recovered["version"] == after_invalid.version

    stale_root = root / "artifacts" / "demo_stale_case"
    _copy_runtime(runtime_root, stale_root)
    stale_workspace = Workspace(stale_root)
    shutil.copy2(replacement_copy, stale_root / "snapshot.json")
    replaced_bytes = (stale_root / "snapshot.json").read_bytes()
    try:
        stale_workspace.load_state()
    except WorkbenchError as exc:
        count_error(exc)
        assert exc.code == "E_STALE_SNAPSHOT"
        assert exc.details.get("reason") == "rollback"
    assert (stale_root / "snapshot.json").read_bytes() == replaced_bytes

    tamper_root = root / "artifacts" / "demo_tamper_case"
    _copy_runtime(runtime_root, tamper_root)
    tampered = json.loads((tamper_root / "snapshot.json").read_text(encoding="utf-8"))
    tampered["words"][0]["text"] = tampered["words"][0]["text"] + "-tampered"
    _write_json(tamper_root / "snapshot.json", tampered)
    try:
        Workspace(tamper_root).load_state()
    except WorkbenchError as exc:
        count_error(exc)
        assert exc.code == "E_STALE_SNAPSHOT"
        assert exc.details.get("reason") in {"tamper", "summary_mismatch"}

    corrupt_root = root / "artifacts" / "demo_corrupt_case"
    _copy_runtime(runtime_root, corrupt_root)
    shutil.copy2(demo["corrupt_snapshot_path"], corrupt_root / "snapshot.json")
    try:
        Workspace(corrupt_root).load_state()
    except WorkbenchError as exc:
        count_error(exc)
        assert exc.code == "E_STALE_SNAPSHOT"
        assert exc.details.get("reason") == "tamper"

    advance_root = root / "artifacts" / "demo_advance_case"
    _copy_runtime(runtime_root, advance_root)
    current = json.loads((advance_root / "snapshot.json").read_text(encoding="utf-8"))
    previous_digest = current["snapshot_digest"]
    next_body = {
        key: value for key, value in current.items() if key != "snapshot_digest"
    }
    next_body["version"] = int(current["version"]) + 1
    next_body["parent_version"] = int(current["version"])
    next_body["parent_snapshot_digest"] = previous_digest
    next_body["kind"] = "replacement-normal-advance"
    _write_json(advance_root / "snapshot.json", _seal_snapshot(next_body))
    advanced = Workspace(advance_root).load_state()
    assert advanced.version == int(current["version"]) + 1

    history = workspace.version_history()
    word_counts = {str(item["version"]): int(item["words"]) for item in history}
    final_state = workspace.load_state()
    summary = {
        "summary_schema_version": 1,
        "seed": seed,
        "dataset": {
            "chunks": len(demo["chunks"]),
            "total_duration_ms": sum(int(chunk["duration_ms"]) for chunk in demo["chunks"]),
            "mixed_language": True,
        },
        "versions": {
            "word_counts": word_counts,
            "kinds": {str(item["version"]): item["kind"] for item in history},
            "final_version": final_state.version,
        },
        "format_checks": {
            "checks": len(roundtrip["checks"]),
            "all_exact": roundtrip["all_exact"],
            "loss_entries": sum(
                len(check["loss_report"]) for check in roundtrip["checks"]
            ),
        },
        "merge": {
            "strategy": "last-wins",
            "left_version": merged["merge"]["left_version"],
            "right_version": merged["merge"]["right_version"],
            "conflict_count": merged["merge"]["conflict_count"],
            "result_version": merged["version"],
        },
        "migration": {
            "from_schema_version": 0,
            "to_schema_version": 2,
            "status": "migrated",
            "rules": migrated["migrations"][-1]["rules"],
            "result_version": migrated["version"],
        },
        "recovery": {
            "stop_marker_version": stopped["version"],
            "recovered_version": recovered["version"],
            "replayed_versions": recovered["replayed_versions"],
        },
        "snapshot_checks": {
            "rollback": "E_STALE_SNAPSHOT",
            "tamper": "E_STALE_SNAPSHOT",
            "corrupt_sample": "E_STALE_SNAPSHOT",
            "normal_advance_accepted_version": advanced.version,
        },
        "error_codes": dict(sorted(errors.items())),
    }
    summary_path = root / "artifacts" / "demo_summary.json"
    _write_json(summary_path, summary)
    summary["summary_path"] = str(summary_path.relative_to(root))
    return summary
