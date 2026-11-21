from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from contract_elements.config import load_config
from contract_elements.pipeline import DatasetPaths


ROOT = Path(__file__).resolve().parents[1]


def config() -> dict:
    return load_config(ROOT / "config/default.json")


def paths(output: Path, runtime: Path, root: Path = ROOT) -> DatasetPaths:
    return DatasetPaths(
        input_path=root / "data/contracts.jsonl",
        seed_path=root / "data/seeds.jsonl",
        dictionary_path=root / "data/elements.json",
        v1_path=root / "data/annotations_v1.jsonl",
        gold_path=root / "data/gold_conflicts.json",
        output_dir=output,
        runtime_dir=runtime,
    )

