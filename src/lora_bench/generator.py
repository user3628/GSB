"""Fixed-seed generators for JSONL samples, YAML config and model weights.

Everything is regenerated from literal seed constants, so deleting the data
directory and rerunning the generator restores byte-identical artifacts.
"""
from __future__ import annotations

import os
from typing import Any

import numpy as np
import torch
import yaml

from .data import samples_fingerprint
from .determinism import set_global_seed, sha256_file, sha256_text, stable_json
from .model import ModelConfig, TinyTransformer, init_model_weights
from .tokenizer import VOCAB_SIZE

SAMPLE_SEED = 20240601
MODEL_SEED = 20240601
DEFAULT_MODEL_CONFIG = ModelConfig(
    vocab_size=VOCAB_SIZE,
    max_seq_len=48,
    d_model=48,
    n_heads=4,
    d_ff=96,
    n_layers=2,
)

ZH_INSTRUCTIONS = [
    "把下面的句子翻译成英文",
    "总结这段文字的要点",
    "为这条指令生成一个礼貌回复",
    "提取句子中的关键词",
    "把陈述句改写为疑问句",
]
EN_INSTRUCTIONS = [
    "Translate the following sentence into Chinese",
    "Summarize the key point of this text",
    "Generate a polite reply to this message",
    "Extract the keywords from this sentence",
    "Rewrite this statement as a question",
]
ZH_WORDS = ["模型", "数据", "训练", "并行", "调度", "预算", "检查点", "恢复"]
EN_WORDS = ["model", "data", "training", "parallel", "schedule", "budget", "checkpoint", "resume"]
SOURCES = ["synth-zh", "synth-en", "synth-mixed"]


def generate_samples(count: int = 24, seed: int = SAMPLE_SEED) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    samples: list[dict[str, Any]] = []
    for index in range(count):
        lang = "zh" if index % 2 == 0 else "en"
        if lang == "zh":
            instruction = str(ZH_INSTRUCTIONS[int(rng.integers(len(ZH_INSTRUCTIONS)))])
            words = rng.choice(ZH_WORDS, size=3, replace=False)
            text = "，".join(words.tolist()) + "。"
            output = f"中文处理结果：{text}"
        else:
            instruction = str(EN_INSTRUCTIONS[int(rng.integers(len(EN_INSTRUCTIONS)))])
            words = rng.choice(EN_WORDS, size=3, replace=False)
            text = " ".join(words.tolist()) + "."
            output = f"English result: {text}"
        samples.append(
            {
                "id": f"sample-{index:04d}",
                "instruction": instruction,
                "output": output,
                "lang": lang,
                "source": SOURCES[int(rng.integers(len(SOURCES)))],
            }
        )
    return samples


def write_jsonl(path: str, samples: list[dict[str, Any]]) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    lines = [stable_json(item) for item in samples]
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    return sha256_file(path)


def default_config_dict(
    experiment: str,
    model_path: str,
    samples_path: str,
    *,
    budget_override: dict[str, int] | None = None,
    task_overrides: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    budget = {
        "max_workers": 3,
        "per_task_sample_quota": 240,
        "global_sample_cap": 480,
    }
    if budget_override:
        budget.update(budget_override)
    tasks = task_overrides or [
        {
            "task_id": "task-alpha",
            "rank": 4,
            "alpha": 8.0,
            "target_modules": ["q_proj", "v_proj"],
            "learning_rate": 0.05,
            "batch_size": 2,
            "grad_accum": 2,
            "max_steps": 12,
        },
        {
            "task_id": "task-beta",
            "rank": 8,
            "alpha": 16.0,
            "target_modules": ["q_proj", "k_proj", "v_proj"],
            "learning_rate": 0.02,
            "batch_size": 2,
            "grad_accum": 1,
            "max_steps": 10,
        },
        {
            # batch 4 * accum 4 * steps 20 = 320 samples -> exceeds the
            # per-task quota (240) and is guaranteed to stay not-started.
            "task_id": "task-gamma-overbudget",
            "rank": 4,
            "alpha": 4.0,
            "target_modules": ["ff1", "ff2"],
            "learning_rate": 0.01,
            "batch_size": 4,
            "grad_accum": 4,
            "max_steps": 20,
        },
    ]
    return {
        "experiment": experiment,
        "model_path": model_path,
        "samples_path": samples_path,
        "seed": 20240601,
        "checkpoint_every": 4,
        "budget": budget,
        "tasks": tasks,
    }


def write_config(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(payload, handle, sort_keys=True, allow_unicode=True)
    return sha256_file(path)


def generate_model_weights(path: str, model_config: ModelConfig = DEFAULT_MODEL_CONFIG) -> str:
    set_global_seed(MODEL_SEED)
    model = TinyTransformer(model_config)
    init_model_weights(model, MODEL_SEED)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    torch.save(model.state_dict(), path)
    return sha256_file(path)


def generate_all(
    data_dir: str,
    *,
    config_name: str = "experiment.yaml",
    budget_override: dict[str, int] | None = None,
    task_overrides: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    samples_path = os.path.join(data_dir, "samples.jsonl")
    model_path = os.path.join(data_dir, "tiny_transformer.pt")
    config_path = os.path.join(data_dir, config_name)
    samples = generate_samples()
    sample_hash = write_jsonl(samples_path, samples)
    model_hash = generate_model_weights(model_path)
    config = default_config_dict(
        "lora-deterministic-demo",
        model_path,
        samples_path,
        budget_override=budget_override,
        task_overrides=task_overrides,
    )
    config_hash = write_config(config_path, config)
    from .data import load_jsonl

    fingerprint = samples_fingerprint(load_jsonl(samples_path))
    manifest = {
        "samples_path": samples_path,
        "model_path": model_path,
        "config_path": config_path,
        "sample_count": len(samples),
        "samples_sha256": sample_hash,
        "model_sha256": model_hash,
        "config_sha256": config_hash,
        "samples_fingerprint": fingerprint,
        "model_config": DEFAULT_MODEL_CONFIG.to_dict(),
    }
    manifest_path = os.path.join(data_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(stable_json(manifest) + "\n")
    return manifest
