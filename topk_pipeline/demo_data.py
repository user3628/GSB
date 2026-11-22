"""Deterministic local sample and injection corpus generator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


GENERATOR_SEED = 20261123


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _shard(
    shard_id: str,
    path: str,
    *,
    k: int | None,
    temperature: float,
    nucleus_enabled: bool,
    top_p: float,
    seed: int,
    expected_shape: list[int],
    expected_score_range: list[float],
    row_block: int,
    col_block: int,
) -> dict[str, Any]:
    return {
        "shard_id": shard_id,
        "path": path,
        "k": k,
        "temperature": temperature,
        "nucleus_enabled": nucleus_enabled,
        "top_p": top_p,
        "seed": seed,
        "expected_shape": expected_shape,
        "expected_score_range": expected_score_range,
        "row_block": row_block,
        "col_block": col_block,
    }


def _manifest(shards: list[dict[str, Any]], *, global_elements: int, worker_elements: int, peak_memory_bytes: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "name": "deterministic-local-topk-demo",
        "defaults": {"workers": 2, "seed": 20261123},
        "budget": {
            "global_elements": int(global_elements),
            "worker_elements": int(worker_elements),
            "peak_memory_bytes": int(peak_memory_bytes),
        },
        "workers": 2,
        "shards": shards,
    }


def generate_demo_data(output_dir: str | Path) -> dict[str, Any]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    injections = out / "injections"
    injections.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(GENERATOR_SEED)

    # Four valid shards: small vocabulary, large vocabulary, max-score ties,
    # and a probability tie intentionally crossing the nucleus boundary.
    small = rng.normal(0.0, 1.6, size=(12, 16)).astype(np.float64)
    np.save(out / "shard_00_small.npy", small, allow_pickle=False)

    large = rng.normal(0.0, 2.0, size=(32, 4096)).astype(np.float64)
    np.save(out / "shard_01_large.npy", large, allow_pickle=False)

    tie_max = rng.normal(0.0, 0.8, size=(10, 32)).astype(np.float64)
    tied_positions = np.array([3, 11, 19, 27, 30, 31])
    tie_max[:, tied_positions] = 6.25
    np.save(out / "shard_02_tie_max.npy", tie_max, allow_pickle=False)

    nucleus_ties = rng.normal(-0.5, 0.3, size=(16, 64)).astype(np.float64)
    nucleus_ties[:, :4] = 1.5
    np.save(out / "shard_03_nucleus_ties.npy", nucleus_ties, allow_pickle=False)

    # Abnormal inputs.
    nan_values = rng.normal(0.0, 1.0, size=(4, 8)).astype(np.float64)
    nan_values[1, 3] = np.nan
    np.save(out / "injection_nan.npy", nan_values, allow_pickle=False)

    posinf_values = rng.normal(0.0, 1.0, size=(4, 8)).astype(np.float64)
    posinf_values[2, 5] = np.inf
    np.save(out / "injection_posinf.npy", posinf_values, allow_pickle=False)

    all_neg_inf = rng.normal(0.0, 1.0, size=(4, 8)).astype(np.float64)
    all_neg_inf[2, :] = -np.inf
    np.save(out / "injection_all_neg_inf.npy", all_neg_inf, allow_pickle=False)

    all_tied = np.full((5, 16), 2.5, dtype=np.float64)
    np.save(out / "injection_tied_max.npy", all_tied, allow_pickle=False)

    invalid_k_values = rng.normal(0.0, 1.0, size=(3, 8)).astype(np.float64)
    np.save(out / "injection_invalid_k.npy", invalid_k_values, allow_pickle=False)

    valid_manifest = _manifest(
        [
            _shard(
                "shard_00_small",
                "shard_00_small.npy",
                k=3,
                temperature=0.8,
                nucleus_enabled=True,
                top_p=0.85,
                seed=1001,
                expected_shape=[12, 3],
                expected_score_range=[-6.0, 6.0],
                row_block=4,
                col_block=5,
            ),
            _shard(
                "shard_01_large",
                "shard_01_large.npy",
                k=12,
                temperature=1.1,
                nucleus_enabled=True,
                top_p=0.92,
                seed=1002,
                expected_shape=[32, 12],
                expected_score_range=[-10.0, 10.0],
                row_block=8,
                col_block=257,
            ),
            _shard(
                "shard_02_tie_max",
                "shard_02_tie_max.npy",
                k=4,
                temperature=0.7,
                nucleus_enabled=False,
                top_p=1.0,
                seed=1003,
                expected_shape=[10, 4],
                expected_score_range=[-4.0, 7.0],
                row_block=3,
                col_block=7,
            ),
            _shard(
                "shard_03_nucleus_ties",
                "shard_03_nucleus_ties.npy",
                k=12,
                temperature=1.0,
                nucleus_enabled=True,
                top_p=0.4,
                seed=1004,
                expected_shape=[16, 12],
                expected_score_range=[-2.0, 2.0],
                row_block=5,
                col_block=9,
            ),
        ],
        global_elements=300_000,
        worker_elements=200_000,
        peak_memory_bytes=8 * 1024 * 1024,
    )
    _write_json(out / "manifest.json", valid_manifest)

    def write_case(name: str, manifest: dict[str, Any], expected: dict[str, Any]) -> None:
        case_dir = injections / name
        _write_json(case_dir / "manifest.json", manifest)
        expected_payload = {"name": name, "manifest": f"injections/{name}/manifest.json", **expected}
        _write_json(case_dir / "expected.json", expected_payload)

    common_budget = {"global_elements": 100_000, "worker_elements": 100_000, "peak_memory_bytes": 8 * 1024 * 1024}

    write_case(
        "invalid_k_zero",
        _manifest(
            [
                _shard(
                    "invalid_k_zero",
                    "../../injection_invalid_k.npy",
                    k=0,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=1,
                    expected_shape=[3, 1],
                    expected_score_range=[-5.0, 5.0],
                    row_block=2,
                    col_block=4,
                )
            ],
            **common_budget,
        ),
        {"shard_id": "invalid_k_zero", "status": "unprocessed", "code": "E_INVALID_K", "stage": "filter"},
    )
    write_case(
        "invalid_k_too_large",
        _manifest(
            [
                _shard(
                    "invalid_k_too_large",
                    "../../injection_invalid_k.npy",
                    k=99,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=1,
                    expected_shape=[3, 8],
                    expected_score_range=[-5.0, 5.0],
                    row_block=2,
                    col_block=4,
                )
            ],
            **common_budget,
        ),
        {"shard_id": "invalid_k_too_large", "status": "unprocessed", "code": "E_INVALID_K", "stage": "filter"},
    )
    write_case(
        "nan_logits",
        _manifest(
            [
                _shard(
                    "nan_logits",
                    "../../injection_nan.npy",
                    k=2,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=2,
                    expected_shape=[4, 2],
                    expected_score_range=[-5.0, 5.0],
                    row_block=2,
                    col_block=4,
                )
            ],
            **common_budget,
        ),
        {"shard_id": "nan_logits", "status": "error", "code": "E_NUMERIC", "stage": "filter"},
    )
    write_case(
        "posinf_logits",
        _manifest(
            [
                _shard(
                    "posinf_logits",
                    "../../injection_posinf.npy",
                    k=2,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=3,
                    expected_shape=[4, 2],
                    expected_score_range=[-5.0, 5.0],
                    row_block=2,
                    col_block=4,
                )
            ],
            **common_budget,
        ),
        {"shard_id": "posinf_logits", "status": "error", "code": "E_NUMERIC", "stage": "filter"},
    )
    write_case(
        "all_neg_inf_row",
        _manifest(
            [
                _shard(
                    "all_neg_inf_row",
                    "../../injection_all_neg_inf.npy",
                    k=3,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=4,
                    expected_shape=[4, 3],
                    expected_score_range=[-5.0, 5.0],
                    row_block=2,
                    col_block=4,
                )
            ],
            **common_budget,
        ),
        {
            "shard_id": "all_neg_inf_row",
            "status": "error",
            "code": "E_NUMERIC",
            "stage": "normalize",
            "row": 2,
        },
    )
    write_case(
        "all_tied_max",
        _manifest(
            [
                _shard(
                    "all_tied_max",
                    "../../injection_tied_max.npy",
                    k=3,
                    temperature=1.0,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=5,
                    expected_shape=[5, 3],
                    expected_score_range=[2.0, 3.0],
                    row_block=2,
                    col_block=5,
                )
            ],
            **common_budget,
        ),
        {
            "shard_id": "all_tied_max",
            "status": "success",
            "code": None,
            "tie_tokens_per_row": [0, 1, 2],
            "score": 2.5,
        },
    )
    write_case(
        "global_budget_exceeded",
        _manifest(
            [
                _shard(
                    "budget_first_small",
                    "../../shard_00_small.npy",
                    k=3,
                    temperature=0.8,
                    nucleus_enabled=True,
                    top_p=0.85,
                    seed=1001,
                    expected_shape=[12, 3],
                    expected_score_range=[-6.0, 6.0],
                    row_block=4,
                    col_block=5,
                ),
                _shard(
                    "budget_second_small",
                    "../../shard_00_small.npy",
                    k=3,
                    temperature=0.8,
                    nucleus_enabled=True,
                    top_p=0.85,
                    seed=1001,
                    expected_shape=[12, 3],
                    expected_score_range=[-6.0, 6.0],
                    row_block=4,
                    col_block=5,
                ),
            ],
            global_elements=350,
            worker_elements=10_000,
            peak_memory_bytes=8 * 1024 * 1024,
        ),
        {
            "shard_id": "budget_second_small",
            "status": "unprocessed",
            "code": "E_BUDGET_EXCEEDED",
            "stage": "schedule",
        },
    )
    write_case(
        "worker_quota_exceeded",
        _manifest(
            [
                _shard(
                    "worker_quota_large",
                    "../../shard_01_large.npy",
                    k=12,
                    temperature=1.1,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=1002,
                    expected_shape=[32, 12],
                    expected_score_range=[-10.0, 10.0],
                    row_block=8,
                    col_block=257,
                )
            ],
            global_elements=300_000,
            worker_elements=1000,
            peak_memory_bytes=8 * 1024 * 1024,
        ),
        {
            "shard_id": "worker_quota_large",
            "status": "unprocessed",
            "code": "E_BUDGET_EXCEEDED",
            "stage": "schedule",
        },
    )
    write_case(
        "memory_budget_exceeded",
        _manifest(
            [
                _shard(
                    "memory_large",
                    "../../shard_01_large.npy",
                    k=12,
                    temperature=1.1,
                    nucleus_enabled=False,
                    top_p=1.0,
                    seed=1002,
                    expected_shape=[32, 12],
                    expected_score_range=[-10.0, 10.0],
                    row_block=8,
                    col_block=257,
                )
            ],
            global_elements=300_000,
            worker_elements=200_000,
            peak_memory_bytes=100_000,
        ),
        {
            "shard_id": "memory_large",
            "status": "unprocessed",
            "code": "E_MEMORY_BUDGET",
            "stage": "schedule",
        },
    )

    case_paths = sorted(str(path.relative_to(out)) for path in injections.glob("*/expected.json"))
    cases = [
        json.loads((out / relative).read_text(encoding="utf-8"))
        for relative in case_paths
    ]
    _write_json(out / "cases.json", {"generator_seed": GENERATOR_SEED, "cases": cases})
    return {
        "output_dir": str(out),
        "manifest": str(out / "manifest.json"),
        "cases": str(out / "cases.json"),
        "shard_files": sorted(path.name for path in out.glob("*.npy")),
    }
