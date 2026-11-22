from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_json, atomic_write_jsonl, canonical_bytes, read_json, read_jsonl
from .engine import Engine
from .errors import EngineError
from .png import encode_rgb_png
from .workspace import Workspace


def _image_pattern(kind: str, seed: int) -> np.ndarray:
    rng = random.Random(seed)
    height = width = 24
    pixels = np.zeros((height, width, 3), dtype=np.uint8)
    for row_index in range(height):
        for column_index in range(width):
            noise = rng.randint(-8, 8)
            if kind == "red-blue-checker":
                base = (210, 40, 45) if ((column_index // 6) + (row_index // 6)) % 2 == 0 else (35, 70, 210)
            elif kind == "green-gradient":
                base = (
                    30 + int(180 * column_index / (width - 1)),
                    190,
                    45 + int(80 * row_index / (height - 1)),
                )
            else:
                base = (120, 45, 170) if (column_index + row_index) % 11 < 5 else (225, 190, 55)
            pixels[row_index, column_index] = tuple(max(0, min(255, channel + noise)) for channel in base)
    return pixels


def _layout_config() -> dict[str, Any]:
    return {
        "schema_version": "layout-v1",
        "layout_id": "demo-main-v1",
        "vector": {
            "dimension": 280,
            "fusion": {"image_weight": 0.45, "text_weight": 0.55},
            "visual": {"grid_rows": 4, "grid_cols": 4, "edge_bins": 8},
            "text": {"dimension": 128},
        },
        "layers": [{"layer": 0, "max_vectors": 10, "max_shards": 8}],
        "shards": [
            {"shard_id": "s0", "layer": 0, "capacity": 1, "fallbacks": ["s1"]},
            {"shard_id": "s1", "layer": 0, "capacity": 2, "fallbacks": []},
            {"shard_id": "s2", "layer": 0, "capacity": 2, "fallbacks": []},
        ],
    }


def generate_demo_dataset(directory: Path, seed: int = 42) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    images_dir = directory / "images"
    faults_dir = directory / "faults"
    images_dir.mkdir(parents=True, exist_ok=True)
    faults_dir.mkdir(parents=True, exist_ok=True)

    image_rows = [
        {"image_id": "img-red-blue", "file": "img-red-blue.png", "language": "zh"},
        {"image_id": "img-green-gradient", "file": "img-green-gradient.png", "language": "en"},
        {"image_id": "img-purple-stripes", "file": "img-purple-stripes.png", "language": "zh-en"},
    ]
    text_rows = [
        {
            "text_id": "txt-a",
            "text": "红色与蓝色 checker 样本 / red-blue sample",
            "image_id": "img-red-blue",
            "target_shard": "s0",
        },
        {
            "text_id": "txt-b",
            "text": "红色 检索 red retrieval（一对多）",
            "image_id": "img-red-blue",
            "target_shard": "s0",
        },
        {
            "text_id": "txt-c",
            "text": "绿色 gradient image",
            "image_id": "img-green-gradient",
            "target_shard": "s1",
        },
        {
            "text_id": "txt-d",
            "text": "紫色 striped late sample / 晚到样本",
            "image_id": "img-purple-stripes",
            "target_shard": "s1",
        },
        {
            "text_id": "txt-e",
            "text": "更晚到 very late arrival sample",
            "image_id": "img-purple-stripes",
            "target_shard": "s1",
        },
        {
            "text_id": "txt-unknown",
            "text": "引用不存在的 image_id / dangling reference",
            "image_id": "img-missing",
            "target_shard": "s2",
        },
    ]
    missing_text_rows = [
        {
            "text_id": "txt-missing-png",
            "text": "损失 PNG 的样本 / missing PNG",
            "image_id": "img-missing-png",
            "target_shard": "s0",
        }
    ]
    full_text_rows = [
        {
            "text_id": "txt-full-1",
            "text": "已满分片 first / full one",
            "image_id": "img-red-blue",
            "target_shard": "s-full",
        },
        {
            "text_id": "txt-full-2",
            "text": "已满分片 second / full two",
            "image_id": "img-green-gradient",
            "target_shard": "s-full",
        },
        {
            "text_id": "txt-full-3",
            "text": "无备用分片 third / full three",
            "image_id": "img-purple-stripes",
            "target_shard": "s-full",
        },
    ]

    for image_index, row in enumerate(image_rows):
        kind = {"img-red-blue": "red-blue-checker", "img-green-gradient": "green-gradient", "img-purple-stripes": "purple-stripes"}[
            row["image_id"]
        ]
        pixels = _image_pattern(kind, seed + image_index * 101)
        (images_dir / row["file"]).write_bytes(encode_rgb_png(pixels))

    atomic_write_jsonl(directory / "images.jsonl", image_rows)
    atomic_write_jsonl(directory / "texts.jsonl", text_rows)
    atomic_write_json(directory / "layout.json", _layout_config())

    zero_capacity = _layout_config()
    zero_capacity["layout_id"] = "fault-zero-capacity"
    zero_capacity["shards"][0]["capacity"] = 0
    atomic_write_json(faults_dir / "layout_zero_capacity.json", zero_capacity)

    layer_too_small = _layout_config()
    layer_too_small["layout_id"] = "fault-layer-too-small"
    layer_too_small["layers"][0]["max_vectors"] = 1
    atomic_write_json(faults_dir / "layout_layer_limit_below_capacity.json", layer_too_small)

    dimension_mismatch = _layout_config()
    dimension_mismatch["layout_id"] = "fault-dimension-mismatch"
    dimension_mismatch["vector"]["dimension"] = 279
    atomic_write_json(faults_dir / "layout_dimension_mismatch.json", dimension_mismatch)

    nan_fusion = _layout_config()
    nan_fusion["layout_id"] = "fault-nan-fusion"
    nan_fusion["vector"]["fusion"]["image_weight"] = float("nan")
    (faults_dir / "layout_nan_fusion.json").write_text(
        json.dumps(nan_fusion, ensure_ascii=False, indent=2, allow_nan=True) + "\n",
        encoding="utf-8",
    )

    full_layout = _layout_config()
    full_layout["layout_id"] = "fault-full-no-standby"
    full_layout["layers"] = [{"layer": 0, "max_vectors": 2, "max_shards": 1}]
    full_layout["shards"] = [{"shard_id": "s-full", "layer": 0, "capacity": 2, "fallbacks": []}]
    atomic_write_json(faults_dir / "layout_full_no_standby.json", full_layout)
    atomic_write_jsonl(faults_dir / "texts_full_no_standby.jsonl", full_text_rows)
    atomic_write_jsonl(faults_dir / "texts_missing_png.jsonl", missing_text_rows)
    atomic_write_jsonl(
        faults_dir / "images_missing_png.jsonl",
        [{"image_id": "img-missing-png", "file": "missing.png", "language": "zh"}],
    )
    atomic_write_json(faults_dir / "vectors_dimension_mismatch.json", {"txt-a": [0.01] * 279})
    (faults_dir / "vectors_nan.json").write_text(
        json.dumps({"txt-a": [1.0] * 280}, allow_nan=False).replace("[1.0, 1.0", "[NaN, 1.0", 1) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "seed": seed,
        "image_count": len(image_rows),
        "text_count": len(text_rows),
        "valid_text_ids": ["txt-a", "txt-b", "txt-c", "txt-d", "txt-e"],
        "unknown_ref_text_id": "txt-unknown",
        "faults": sorted(path.name for path in faults_dir.iterdir()),
    }
    atomic_write_json(directory / "manifest.json", manifest)
    return manifest


def _fresh_workspace(root: Path, dataset: Path, layout_path: Path | None = None, texts_path: Path | None = None) -> Engine:
    engine = Engine(root)
    engine.workspace.ensure()
    images = read_jsonl(dataset / "images.jsonl")
    texts = read_jsonl(texts_path or dataset / "texts.jsonl")
    engine.import_inputs(images, texts, image_source_dir=dataset / "images")
    if layout_path is None:
        engine.install_layout(read_json(dataset / "layout.json"))
    else:
        engine.install_layout(read_json(layout_path))
    return engine


def _error_dict(exc: EngineError) -> dict[str, Any]:
    return exc.to_dict()


def run_demo(root: Path, seed: int = 42) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    for name in (
        "dataset",
        "main",
        "fault-full",
        "fault-missing-png",
        "clean-reference",
        "order-arrival",
        "order-id",
        "invalidation",
        "demo-report.json",
    ):
        path = root / name
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    dataset = root / "dataset"
    manifest = generate_demo_dataset(dataset, seed)
    valid_ids = manifest["valid_text_ids"]
    report: dict[str, Any] = {"ok": True, "seed": seed, "dataset": str(dataset), "scenarios": {}}

    main = _fresh_workspace(root / "main", dataset)
    report["scenarios"]["doctor"] = main.workspace.doctor()
    first = main.submit_batch(batch_id="demo-0001", text_ids=["txt-a", "txt-b"], expected_sequence=0, order="id")
    main.workspace.request_stop("demo stop between atomic batches")
    stopped_attempt = None
    try:
        main.submit_batch(batch_id="demo-0002", text_ids=["txt-c"], expected_sequence=1, order="id")
    except EngineError as exc:
        stopped_attempt = _error_dict(exc)
    recovered = main.recover(clear_stop=True)
    second = main.submit_batch(batch_id="demo-0002", text_ids=["txt-c"], expected_sequence=1, order="id")
    third = main.submit_batch(batch_id="demo-0003", text_ids=["txt-d", "txt-e"], expected_sequence=2, order="id")
    report["scenarios"]["graceful_stop_resume"] = {
        "batch1": first["batch_seq"],
        "stopped_attempt": stopped_attempt,
        "recovered_reused_existing_index": recovered["reused_existing_index"],
        "batch2": second["batch_seq"],
        "batch3": third["batch_seq"],
        "split_count": third["index"]["split_history"],
        "routes": third["index"]["routes"],
        "last_sequence": main.status()["last_batch_seq"],
    }

    unknown_error = None
    try:
        main.submit_batch(batch_id="fault-unknown-ref", text_ids=["txt-unknown"], expected_sequence=3, order="id")
    except EngineError as exc:
        unknown_error = _error_dict(exc)
    after_unknown = main.status()
    report["scenarios"]["unknown_reference"] = {
        "error": unknown_error,
        "sequence_unchanged": after_unknown["last_batch_seq"] == 3,
    }

    override_errors: dict[str, Any] = {}
    for label, filename in (
        ("dimension", "vectors_dimension_mismatch.json"),
        ("numeric", "vectors_nan.json"),
    ):
        overrides = read_json(dataset / "faults" / filename)
        try:
            main.submit_batch(
                batch_id=f"fault-{label}",
                text_ids=["txt-a"],
                expected_sequence=3,
                order="id",
                vector_overrides=overrides,
            )
        except EngineError as exc:
            override_errors[label] = _error_dict(exc)
    report["scenarios"]["vector_overrides"] = {
        "errors": override_errors,
        "sequence_unchanged": main.status()["last_batch_seq"] == 3,
    }

    layout_errors: dict[str, Any] = {}
    for filename in (
        "layout_zero_capacity.json",
        "layout_layer_limit_below_capacity.json",
        "layout_dimension_mismatch.json",
        "layout_nan_fusion.json",
    ):
        try:
            main.install_layout(read_json(dataset / "faults" / filename))
        except EngineError as exc:
            layout_errors[filename] = _error_dict(exc)
    report["scenarios"]["layout_faults"] = {
        "errors": layout_errors,
        "config_unchanged": main.status()["config_version"] is not None,
    }

    full_engine = _fresh_workspace(
        root / "fault-full",
        dataset,
        layout_path=dataset / "faults" / "layout_full_no_standby.json",
        texts_path=dataset / "faults" / "texts_full_no_standby.jsonl",
    )
    full_error = None
    try:
        full_engine.submit_batch(
            batch_id="fault-shard-full",
            text_ids=["txt-full-1", "txt-full-2", "txt-full-3"],
            expected_sequence=0,
            order="id",
        )
    except EngineError as exc:
        full_error = _error_dict(exc)
    report["scenarios"]["shard_full"] = {
        "error": full_error,
        "sequence_unchanged": full_engine.status()["last_batch_seq"] == 0,
    }

    missing_engine = _fresh_workspace(root / "fault-missing-png", dataset, texts_path=dataset / "faults" / "texts_missing_png.jsonl")
    missing_images = read_jsonl(dataset / "faults" / "images_missing_png.jsonl")
    missing_engine.import_inputs(
        read_jsonl(dataset / "images.jsonl") + missing_images,
        read_jsonl(dataset / "faults" / "texts_missing_png.jsonl"),
        image_source_dir=dataset / "images",
    )
    missing_error = None
    try:
        missing_engine.submit_batch(
            batch_id="fault-empty-png",
            text_ids=["txt-missing-png"],
            expected_sequence=0,
            order="id",
        )
    except EngineError as exc:
        missing_error = _error_dict(exc)
    report["scenarios"]["empty_input"] = {
        "error": missing_error,
        "sequence_unchanged": missing_engine.status()["last_batch_seq"] == 0,
    }

    clean = _fresh_workspace(root / "clean-reference", dataset)
    clean.submit_batch(batch_id="clean-0001", text_ids=valid_ids, expected_sequence=0, order="id")
    main_index_bytes = main.workspace.index_path.read_bytes()
    clean_index_bytes = clean.workspace.index_path.read_bytes()
    report["scenarios"]["stop_resume_byte_identical"] = {
        "identical_index": main_index_bytes == clean_index_bytes,
        "sha256": hashlib_sha256(main_index_bytes),
        "clean_sha256": hashlib_sha256(clean_index_bytes),
    }

    arrival = _fresh_workspace(root / "order-arrival", dataset)
    id_order = _fresh_workspace(root / "order-id", dataset)
    arrival_result = arrival.backfill_all(text_ids=valid_ids, expected_sequence=0, batch_size=1, batch_prefix="arrival", order="arrival")
    id_result = id_order.backfill_all(text_ids=valid_ids, expected_sequence=0, batch_size=1, batch_prefix="id", order="id")
    arrival_index = arrival.workspace.index_path.read_bytes()
    id_index = id_order.workspace.index_path.read_bytes()
    arrival_vectors = arrival.workspace.vectors_path.read_bytes()
    id_vectors = id_order.workspace.vectors_path.read_bytes()
    report["scenarios"]["order_convergence"] = {
        "arrival_batches": arrival_result["committed_batch_count"],
        "id_batches": id_result["committed_batch_count"],
        "identical_index": arrival_index == id_index,
        "identical_vectors": arrival_vectors == id_vectors,
        "identical_split_history": arrival.index()["split_history"] == id_order.index()["split_history"],
        "identical_ownership": arrival.index()["ownership_summary"] == id_order.index()["ownership_summary"],
        "index_sha256": hashlib_sha256(arrival_index),
    }

    invalidated = _fresh_workspace(root / "invalidation", dataset)
    invalidated.submit_batch(batch_id="invalidation-0001", text_ids=valid_ids, expected_sequence=0, order="id")
    changed_layout = _layout_config()
    changed_layout["layout_id"] = "demo-main-v2-fusion"
    changed_layout["vector"]["fusion"]["image_weight"] = 0.60
    changed_layout["vector"]["fusion"]["text_weight"] = 0.40
    invalidation_result = invalidated.install_layout(changed_layout)
    report["scenarios"]["layout_invalidation"] = {
        "event": invalidation_result["invalidated"],
        "old_vector_fingerprint": invalidation_result["invalidated"]["old_fingerprints"]["vector_build"],
        "new_vector_fingerprint": invalidation_result["invalidated"]["new_fingerprints"]["vector_build"],
        "invalidated_shards": invalidation_result["invalidated"]["invalidated_shards"],
        "reused_shards": invalidation_result["invalidated"]["reused_shards"],
        "current_config_version": invalidated.status()["config_version"],
    }

    report["final_sequences"] = {
        "main": main.status()["last_batch_seq"],
        "fault_full": full_engine.status()["last_batch_seq"],
        "fault_missing_png": missing_engine.status()["last_batch_seq"],
        "order_arrival": arrival.status()["last_batch_seq"],
        "order_id": id_order.status()["last_batch_seq"],
    }
    atomic_write_json(root / "demo-report.json", report)
    return report


def hashlib_sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()
