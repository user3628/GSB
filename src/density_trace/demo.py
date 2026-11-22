from __future__ import annotations

import io
import shutil
import zipfile
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import (
    atomic_write_json,
    canonical_json_bytes,
    read_json,
    sha256_bytes,
    sha256_file,
)
from .config import Config
from .engine import Engine, import_inputs
from .errors import BusinessError
from .manifest import BatchSpec, LoadedBatch, load_manifest
from .numerics import evaluate_batch_density
from .reference import marginal_quantiles_from_fixed


def _npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def write_deterministic_npz(path: str | Path, arrays: dict[str, np.ndarray]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(arrays):
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o600 << 16
            archive.writestr(info, _npy_bytes(np.asarray(arrays[name])))


def demo_config() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "config_version": "demo-v1",
        "grid": {
            "x_min": -8.0,
            "x_max": 10.0,
            "y_min": -6.0,
            "y_max": 8.0,
            "nx": 22,
            "ny": 20,
        },
        "density": {
            "chunk_size": 32,
            "sum_strategy": "neumaier",
            "density_scale": 1_000_000_000_000,
            "bandwidth": {
                "method": "mad_iqr_fallback",
                "mad_factor": 1.4826,
                "iqr_factor": 1.349,
                "floor": 0.18,
            },
            "curve_abs_tolerance": 1e-9,
        },
        "adjudication": {
            "warning_threshold": 0.03,
            "drift_threshold": 0.12,
            "warning_confidence": 0.90,
            "drift_confidence": 0.99,
            "quantile_index_start": 1,
            "quantile_index_end": 17,
        },
    }


def _demo_samples(kind: str, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    if kind == "unimodal":
        return rng.normal(loc=(0.0, 0.0), scale=(0.85, 0.8), size=(160, 2))
    if kind == "bimodal":
        first = rng.normal(loc=(-2.2, 1.0), scale=(0.75, 0.72), size=(80, 2))
        second = rng.normal(loc=(2.4, -1.2), scale=(0.8, 0.76), size=(80, 2))
        samples = np.vstack((first, second))
        return samples[rng.permutation(samples.shape[0])]
    if kind == "outliers":
        normal = rng.normal(loc=(0.3, 0.2), scale=(0.65, 0.7), size=(144, 2))
        outliers = rng.normal(loc=(6.5, 5.5), scale=(0.4, 0.4), size=(16, 2))
        return np.vstack((normal, outliers))
    if kind == "missing":
        first = rng.normal(loc=(1.8, 1.2), scale=(0.9, 0.85), size=(96, 2))
        second = rng.normal(loc=(-1.5, -1.0), scale=(0.8, 0.82), size=(64, 2))
        samples = np.vstack((first, second))
        samples = samples[rng.permutation(samples.shape[0])]
        missing_rows = rng.choice(samples.shape[0], size=24, replace=False)
        samples[missing_rows, 1] = np.nan
        return samples
    raise ValueError(f"unknown demo kind: {kind}")


def _reference_samples(seed: int = 7100) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.normal(loc=(0.0, 0.0), scale=(0.9, 0.86), size=(1600, 2))


def _reference_table(config: Config) -> dict[str, Any]:
    samples = _reference_samples()
    spec = BatchSpec(
        batch_id=0,
        file="reference-baseline.npz",
        rows=int(samples.shape[0]),
        cols=2,
        valid_columns=(0, 1),
        source_label="reference",
    )
    batch = LoadedBatch(
        spec=spec,
        file_path=Path("reference-baseline.npz"),
        input_digest=sha256_bytes(samples.astype("<f8").tobytes()),
        raw_shape=(int(samples.shape[0]), 2),
        effective_samples=samples,
        dropped_rows=0,
        source_labels=("reference",),
    )
    density = evaluate_batch_density(batch, config, batch_id=0)
    probabilities = np.linspace(0.05, 0.95, 19, dtype=np.float64)
    x_quantiles, y_quantiles = marginal_quantiles_from_fixed(
        density.density_units, config, probabilities
    )
    scale = float(config.density.density_scale)
    return {
        "schema_version": 1,
        "name": "demo-reference-v1",
        "grid": config.grid.to_dict(),
        "curve": [
            float(value) for value in density.density_units.astype(np.float64) / scale
        ],
        "quantiles": [
            {
                "index": int(index),
                "probability": float(probability),
                "x_value": float(x_value),
                "y_value": float(y_value),
            }
            for index, (probability, x_value, y_value) in enumerate(
                zip(probabilities, x_quantiles, y_quantiles)
            )
        ],
    }


def generate_demo_data(
    output_dir: str | Path, overwrite: bool = True
) -> dict[str, Any]:
    output = Path(output_dir).resolve()
    if output.exists() and overwrite:
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    config_dict = demo_config()
    config = Config.from_dict(config_dict)
    kinds = ["unimodal", "bimodal", "outliers", "missing"]
    labels = ["unimodal", "bimodal", "outliers-and-normal", "missing-values"]
    manifest_batches: list[dict[str, Any]] = []
    for batch_id, (kind, label) in enumerate(zip(kinds, labels)):
        samples = _demo_samples(kind, 1000 + batch_id)
        file_name = f"batches/batch-{batch_id:03d}.npz"
        source_labels = np.asarray([f"demo-{label}"] * samples.shape[0], dtype="<U32")
        write_deterministic_npz(
            output / file_name,
            {"features": samples.astype("<f8"), "source_batch": source_labels},
        )
        manifest_batches.append(
            {
                "batch_id": batch_id,
                "file": file_name,
                "rows": int(samples.shape[0]),
                "cols": 2,
                "valid_columns": [0, 1],
                "source_label": f"demo-{label}",
                "feature_key": "features",
                "label_key": "source_batch",
                "kind": kind,
            }
        )
    manifest = {"schema_version": 1, "batches": manifest_batches}
    reference = _reference_table(config)
    atomic_write_json(output / "config.json", config_dict)
    atomic_write_json(output / "manifest.json", manifest)
    atomic_write_json(output / "reference_quantiles.json", reference)
    return {
        "schema_version": 1,
        "batch_count": len(manifest_batches),
        "batch_kinds": kinds,
        "manifest_sha256": sha256_file(output / "manifest.json"),
        "config_sha256": sha256_file(output / "config.json"),
        "reference_sha256": sha256_file(output / "reference_quantiles.json"),
        "batch_sha256": [
            sha256_file(output / item["file"]) for item in manifest_batches
        ],
    }


def rewrite_batch_for_invalidation(data_dir: str | Path, batch_id: int = 2) -> None:
    data = Path(data_dir).resolve()
    manifest = load_manifest(data / "manifest.json")
    spec = manifest.by_id(batch_id)
    rng = np.random.default_rng(9902)
    normal = rng.normal(loc=(-1.0, -0.8), scale=(0.55, 0.65), size=(120, 2))
    outliers = rng.normal(loc=(-5.4, 5.8), scale=(0.35, 0.3), size=(40, 2))
    samples = np.vstack((normal, outliers))
    source_labels = np.asarray(
        ["demo-invalidated-content"] * samples.shape[0], dtype="<U32"
    )
    write_deterministic_npz(
        manifest.resolve(spec),
        {"features": samples.astype("<f8"), "source_batch": source_labels},
    )


def _records_fingerprint(workspace) -> list[dict[str, Any]]:
    history = workspace.history()
    return [
        {
            "sequence": int(record["sequence"]),
            "batch_id": int(record["batch_id"]),
            "sha256": sha256_bytes(canonical_json_bytes(record) + b"\n"),
        }
        for record in history.records
    ]


def run_demo(output_dir: str | Path, overwrite: bool = True) -> dict[str, Any]:
    base = Path(output_dir).resolve()
    if base.exists() and overwrite:
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)
    data_dir = base / "data"
    generated = generate_demo_data(data_dir)
    manifest_path = data_dir / "manifest.json"
    config_path = data_dir / "config.json"
    reference_path = data_dir / "reference_quantiles.json"

    continuous_ws_path = base / "continuous"
    import_inputs(continuous_ws_path, manifest_path, config_path, reference_path)
    continuous_engine = Engine.from_workspace(continuous_ws_path)
    continuous_engine.resume()
    continuous_summary = continuous_engine.final_summary()
    continuous_fingerprints = _records_fingerprint(continuous_engine.workspace)

    resumed_ws_path = base / "resume"
    import_inputs(resumed_ws_path, manifest_path, config_path, reference_path)
    resumed_engine = Engine.from_workspace(resumed_ws_path)
    resumed_engine.run(batch_id=0)
    resumed_engine.run(batch_id=1)
    resumed_engine.request_stop({"demo": "between-batches"})
    first_resume = resumed_engine.resume()
    second_resume = resumed_engine.resume()
    resumed_summary = resumed_engine.final_summary()
    resumed_fingerprints = _records_fingerprint(resumed_engine.workspace)
    recovery_identical = (
        continuous_fingerprints == resumed_fingerprints
        and canonical_json_bytes(continuous_summary)
        == canonical_json_bytes(resumed_summary)
        and canonical_json_bytes(continuous_engine.get_verdict(3))
        == canonical_json_bytes(resumed_engine.get_verdict(3))
        and canonical_json_bytes(continuous_engine.get_error_report(3))
        == canonical_json_bytes(resumed_engine.get_error_report(3))
    )

    invalid_data_dir = base / "invalid-reference-data"
    shutil.copytree(data_dir, invalid_data_dir)
    invalid_reference = read_json(invalid_data_dir / "reference_quantiles.json")
    invalid_reference["quantiles"] = [
        point for point in invalid_reference["quantiles"] if int(point["index"]) != 7
    ]
    atomic_write_json(invalid_data_dir / "reference_quantiles.json", invalid_reference)
    invalid_ws_path = base / "invalid-reference"
    import_inputs(
        invalid_ws_path,
        invalid_data_dir / "manifest.json",
        invalid_data_dir / "config.json",
        invalid_data_dir / "reference_quantiles.json",
    )
    invalid_engine = Engine.from_workspace(invalid_ws_path)
    try:
        invalid_engine.run(batch_id=0)
        invalid_reference_error = None
    except BusinessError as exc:
        invalid_reference_error = exc.to_dict()

    changed_data_dir = base / "changed-data"
    shutil.copytree(data_dir, changed_data_dir)
    changed_ws_path = base / "invalidation"
    shutil.copytree(continuous_ws_path, changed_ws_path)
    import_inputs(
        changed_ws_path,
        changed_data_dir / "manifest.json",
        changed_data_dir / "config.json",
        changed_data_dir / "reference_quantiles.json",
    )
    rewrite_batch_for_invalidation(changed_data_dir, batch_id=2)
    changed_engine = Engine.from_workspace(changed_ws_path)
    plan_before = changed_engine.inspect_versions()
    prefix_before = {
        int(record["batch_id"]): sha256_bytes(canonical_json_bytes(record) + b"\n")
        for record in changed_engine.workspace.history().records
    }
    reconciliation = changed_engine.reconcile()
    prefix_after = {
        int(record["batch_id"]): sha256_bytes(canonical_json_bytes(record) + b"\n")
        for record in changed_engine.workspace.history().records
    }
    changed_engine.resume()
    changed_summary = changed_engine.final_summary()
    invalidation_summary = {
        "plan": plan_before,
        "event": reconciliation["event"],
        "reused_prefix_unchanged": {
            str(batch_id): prefix_before.get(batch_id) == prefix_after.get(batch_id)
            for batch_id in sorted(prefix_after)
        },
        "final": changed_summary,
    }

    summary = {
        "schema_version": 1,
        "generated": generated,
        "continuous": continuous_summary,
        "resume": {
            "first_resume_status": first_resume["status"],
            "first_resume_published_batches": first_resume.get("published_batches", []),
            "second_resume_status": second_resume["status"],
            "second_resume_published_batches": second_resume.get(
                "published_batches", []
            ),
            "summary": resumed_summary,
        },
        "recovery_identical": recovery_identical,
        "record_fingerprints": continuous_fingerprints,
        "invalid_reference_error": invalid_reference_error,
        "invalidation": invalidation_summary,
    }
    atomic_write_json(base / "summary.json", summary)
    return summary
