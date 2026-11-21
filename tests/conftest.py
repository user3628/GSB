from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rope_diag.config import RopeConfig
from rope_diag.demo_data import generate_demo_assets


@pytest.fixture(scope="session")
def assets(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    base = tmp_path_factory.mktemp("rope_assets")
    generated = generate_demo_assets(base)
    config_dir = base / "configs"
    config_dir.mkdir()
    config_path = config_dir / "rope.yaml"
    config_path.write_bytes(RopeConfig().to_yaml_bytes())
    invalid_path = config_dir / "invalid.yaml"
    invalid_path.write_text(
        "\n".join(
            [
                "base_frequency: 10000.0",
                "scaling_mode: linear",
                "scaling_factor: 0.0",
                "rotation_block_size: 0",
                "numeric_tolerance: 1.0e-6",
                "causal_mask: true",
                "latency_budget_ms: 100.0",
                "memory_budget_bytes: 4194304",
                "row_chunk_size: 16",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "base": base,
        "manifest": Path(generated["manifest"]),
        "config": config_path,
        "invalid_config": invalid_path,
    }
