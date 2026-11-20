"""Fixed-seed local sample/config generator; no network, GPU, or real model."""

from __future__ import annotations

import hashlib
import random
import shutil
from pathlib import Path
from typing import Any

from .config import default_config
from .errors import EngineError
from .util import atomic_write_bytes, atomic_write_json, canonical_bytes


MODELS = ["model-a", "model-b", "model-c", "model-d", "model-e"]

BENCHMARKS: list[dict[str, str]] = [
    {"benchmark_id": "bench-reasoning", "question_type": "reasoning"},
    {"benchmark_id": "bench-knowledge", "question_type": "knowledge"},
    {
        "benchmark_id": "bench-instruction",
        "question_type": "instruction_following",
    },
]

LANGUAGES = ["zh", "en"]

SKILLS: dict[str, dict[str, float]] = {
    "model-a": {
        "reasoning": 0.82,
        "knowledge": 0.78,
        "instruction_following": 0.86,
    },
    "model-b": {
        "reasoning": 0.70,
        "knowledge": 0.64,
        "instruction_following": 0.71,
    },
    "model-c": {
        "reasoning": 0.92,
        "knowledge": 0.94,
        "instruction_following": 0.93,
    },
    "model-d": {
        "reasoning": 0.74,
        "knowledge": 0.70,
        "instruction_following": 0.72,
    },
    "model-e": {
        "reasoning": 0.61,
        "knowledge": 0.55,
        "instruction_following": 0.59,
    },
}


def generated_config() -> dict[str, Any]:
    config = default_config()
    config["version"] = "2026.11.1"
    config["exclusions"].update(
        {
            "max_normalized_chars": 240,
            "min_normalized_chars": 0,
            "benchmark_languages": {
                "bench-reasoning": ["zh", "en"],
                "bench-knowledge": ["zh", "en"],
                "bench-instruction": ["zh", "en"],
            },
            "allowed_question_types": [
                "reasoning",
                "knowledge",
                "instruction_following",
            ],
            "exclude_empty_output": False,
        }
    )
    config["profiling"].update(
        {
            "min_samples_per_layer": 2,
            "min_samples_per_dimension": 5,
        }
    )
    config["ranking"].update(
        {
            "score_tolerance": 0.01,
            "min_valid_samples": 24,
            "min_benchmark_coverage": 3,
            "top_k": 2,
        }
    )
    return config


def _reference(language: str, benchmark_id: str, item_index: int) -> str:
    subject = ["蓝色圆形", "红色三角", "绿色方块"][item_index % 3]
    number = 7 + item_index
    if language == "zh":
        return f"答案 {subject} {number}"
    subject_en = ["blue circle", "red triangle", "green square"][item_index % 3]
    return f"answer {subject_en} {number}"


def _record_rng(seed: int, *parts: Any) -> random.Random:
    material = "|".join([str(seed), *(str(part) for part in parts)])
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))


def _make_output(
    *,
    seed: int,
    model_id: str,
    benchmark_id: str,
    language: str,
    item_index: int,
    question_type: str,
) -> tuple[str, str | None, str | None]:
    reference = _reference(language, benchmark_id, item_index)
    rng = _record_rng(seed, model_id, benchmark_id, language, item_index)
    skill = SKILLS[model_id][question_type]
    draw = rng.random()

    language_override: str | None = None
    question_type_override: str | None = None
    if (
        language == "zh"
        and benchmark_id == "bench-reasoning"
        and item_index == 4
        and model_id == "model-b"
    ):
        language_override = "fr"
    if (
        language == "en"
        and benchmark_id == "bench-knowledge"
        and item_index == 4
        and model_id == "model-d"
    ):
        question_type_override = "legacy_freeform"

    if language == "zh" and item_index == 5 and model_id == "model-a":
        return "", language_override, question_type_override
    if (
        language == "en"
        and benchmark_id == "bench-instruction"
        and item_index == 5
        and model_id == "model-e"
    ):
        return (reference + " ") + ("额外说明 " * 80), language_override, question_type_override

    adjusted = min(0.98, max(0.05, skill + rng.uniform(-0.08, 0.08)))
    if draw < adjusted:
        if language == "zh" and rng.random() < 0.5:
            return reference.replace(" ", "\u3000").replace("7", "７"), language_override, question_type_override
        if language == "en" and rng.random() < 0.5:
            return ("  " + reference.upper() + "  "), language_override, question_type_override
        return reference, language_override, question_type_override
    if draw < adjusted + 0.22:
        tokens = reference.split(" ")
        if len(tokens) > 2:
            tokens.pop(1)
        return " ".join(tokens) + " extra", language_override, question_type_override
    if draw < adjusted + 0.34:
        tokens = reference.split(" ")
        return (tokens[0] if tokens else "answer") + " unrelated", language_override, question_type_override
    return "zzz unrelated 999", language_override, question_type_override


def generate_records(seed: int = 20261121) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for benchmark in BENCHMARKS:
        benchmark_id = benchmark["benchmark_id"]
        question_type = benchmark["question_type"]
        for language in LANGUAGES:
            batch_id = f"batch-{benchmark_id.removeprefix('bench-')}-{language}"
            for item_index in range(6):
                sample_id = f"{benchmark_id}-{language}-{item_index:03d}"
                reference = _reference(language, benchmark_id, item_index)
                for model_id in MODELS:
                    output, language_override, question_type_override = _make_output(
                        seed=seed,
                        model_id=model_id,
                        benchmark_id=benchmark_id,
                        language=language,
                        item_index=item_index,
                        question_type=question_type,
                    )
                    records.append(
                        {
                            "run_id": f"run-{model_id}-{batch_id}",
                            "model_id": model_id,
                            "benchmark_id": benchmark_id,
                            "sample_id": sample_id,
                            "output": output,
                            "reference_answer": reference,
                            "language": language_override or language,
                            "source_batch": batch_id,
                            "question_type": question_type_override or question_type,
                        }
                    )
    records.sort(
        key=lambda record: (
            record["benchmark_id"],
            record["sample_id"],
            record["model_id"],
            record["run_id"],
        )
    )
    return records


def _write_shards(samples_dir: Path, records: list[dict[str, Any]]) -> list[Path]:
    shard_count = 3
    shards: list[list[dict[str, Any]]] = [[] for _ in range(shard_count)]
    for index, record in enumerate(records):
        shards[index % shard_count].append(record)
    paths: list[Path] = []
    for index, shard_records in enumerate(shards):
        path = samples_dir / f"shard-{index:03d}.jsonl"
        data = b"".join(canonical_bytes(record) for record in shard_records)
        atomic_write_bytes(path, data)
        paths.append(path)
    return paths


def generate_workspace(
    workspace: str | Path,
    *,
    seed: int = 20261121,
    force: bool = False,
) -> dict[str, Any]:
    root = Path(workspace).resolve()
    samples_dir = root / "samples"
    configs_dir = root / "configs"
    work_dir = root / "work"
    for directory in (samples_dir, configs_dir, work_dir):
        directory.mkdir(parents=True, exist_ok=True)
    existing = list(samples_dir.glob("*.jsonl")) + list(samples_dir.glob("replacements.json"))
    if existing and not force:
        raise EngineError(
            "E_INVALID_STAGE",
            "generated sample files already exist; pass force to replace them",
            details={"workspace": str(root)},
        )
    for path in existing:
        path.unlink()
    for name in ("engine.json", "answers.json"):
        path = configs_dir / name
        if path.exists():
            path.unlink()
    for name in ("checkpoints", "artifacts"):
        path = work_dir / name
        if path.exists():
            shutil.rmtree(path)
    stop_file = work_dir / "STOP"
    if stop_file.exists():
        stop_file.unlink()

    records = generate_records(seed)
    shard_paths = _write_shards(samples_dir, records)
    atomic_write_json(configs_dir / "engine.json", generated_config(), pretty=True)
    atomic_write_json(
        configs_dir / "answers.json",
        {"version": 1, "overrides": {}},
        pretty=True,
    )
    unique_samples = len({(record["benchmark_id"], record["sample_id"]) for record in records})
    return {
        "ok": True,
        "workspace": str(root),
        "seed": seed,
        "record_count": len(records),
        "unique_sample_count": unique_samples,
        "models": MODELS,
        "benchmarks": [item["benchmark_id"] for item in BENCHMARKS],
        "shards": [path.name for path in shard_paths],
    }
