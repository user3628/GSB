from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eval_engine.config import default_config
from eval_engine.engine import Engine
from eval_engine.util import atomic_write_json, canonical_bytes


def make_test_config() -> dict[str, Any]:
    config = default_config()
    config["version"] = "test-1"
    config["exclusions"].update(
        {
            "max_normalized_chars": 80,
            "min_normalized_chars": 0,
            "benchmark_languages": {
                "bench-a": ["zh", "en"],
                "bench-reasoning": ["zh", "en"],
                "bench-knowledge": ["zh", "en"],
                "bench-instruction": ["zh", "en"],
            },
            "allowed_question_types": [
                "reasoning",
                "knowledge",
                "instruction_following",
            ],
        }
    )
    config["profiling"].update(
        {"min_samples_per_layer": 1, "min_samples_per_dimension": 1}
    )
    config["ranking"].update(
        {
            "score_tolerance": 0.01,
            "min_valid_samples": 0,
            "min_benchmark_coverage": 0,
            "top_k": 1,
        }
    )
    return config


def record(
    *,
    model_id: str,
    sample_id: str,
    benchmark_id: str = "bench-a",
    output: str,
    reference_answer: str | None = "答案 蓝色 7",
    language: str = "zh",
    question_type: str = "reasoning",
    source_batch: str = "batch-a",
    run_id: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "run_id": run_id or f"run-{model_id}",
        "model_id": model_id,
        "benchmark_id": benchmark_id,
        "sample_id": sample_id,
        "output": output,
        "language": language,
        "source_batch": source_batch,
        "question_type": question_type,
    }
    if reference_answer is not None:
        value["reference_answer"] = reference_answer
    return value


def write_workspace(
    root: Path,
    records: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
    *,
    batch_overrides: dict[str, list[dict[str, Any]]] | None = None,
) -> Engine:
    samples_dir = root / "samples"
    configs_dir = root / "configs"
    (root / "work").mkdir(parents=True, exist_ok=True)
    samples_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    sorted_records = sorted(
        records,
        key=lambda item: (
            item["benchmark_id"],
            item["sample_id"],
            item["model_id"],
            item["run_id"],
        ),
    )
    shard_count = max(1, min(3, len(sorted_records)))
    shards: list[list[dict[str, Any]]] = [[] for _ in range(shard_count)]
    for index, item in enumerate(sorted_records):
        shards[index % shard_count].append(item)
    for index, shard in enumerate(shards):
        data = b"".join(canonical_bytes(item) for item in shard)
        (samples_dir / f"shard-{index:03d}.jsonl").write_bytes(data)
    write_config(root, config or make_test_config())
    atomic_write_json(
        configs_dir / "answers.json", {"version": 1, "overrides": {}}
    )
    if batch_overrides:
        atomic_write_json(
            samples_dir / "replacements.json",
            {"version": 1, "batches": batch_overrides},
        )
    return Engine(root)


def write_config(root: Path, config: dict[str, Any]) -> None:
    path = root / "configs" / "engine.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
