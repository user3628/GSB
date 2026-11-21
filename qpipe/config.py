from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from . import PIPELINE_VERSION, SCHEMA_VERSION
from .errors import InvalidParam, QPipeError, StaleCheckpoint
from .util import (
    FileLock,
    atomic_write_json,
    canonical_json_bytes,
    digest_file,
    digest_obj,
    exclusive_directory_publish,
    json_ready,
    load_json,
    temporary_directory,
)

STAGES = ("calibration", "forward", "attribution", "validation")
STAGE_LABELS = {
    "calibration": "01_calibration",
    "forward": "02_forward",
    "attribution": "03_attribution",
    "validation": "04_validation",
}


def state_root(workspace: Path, state_dir: str | Path | None = None) -> Path:
    if state_dir is None:
        return workspace / ".qpipe"
    path = Path(state_dir)
    return path if path.is_absolute() else workspace / path


def ensure_state_layout(root: Path) -> None:
    for relative in ("configs", "cache", "runs", "state", "locks", "control"):
        (root / relative).mkdir(parents=True, exist_ok=True)


def _manifest_source_dir(manifest_path: Path) -> Path:
    return manifest_path.resolve().parent


def _collect_shard_refs(manifest: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for layer in manifest.get("layers", []):
        for role in ("weight", "activation"):
            ref = layer.get(role, {}).get("shard")
            if not isinstance(ref, str) or not ref:
                raise InvalidParam(
                    f"layer {layer.get('id')} has no {role}.shard",
                    layer=layer.get("id"),
                    parameter=f"{role}.shard",
                )
            if ref not in refs:
                refs.append(ref)
    return refs


def _validate_manifest(manifest: dict[str, Any], shapes: dict[str, Any]) -> None:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise InvalidParam(
            "unsupported manifest schema_version",
            parameter="schema_version",
            details={"expected": SCHEMA_VERSION},
        )
    layers = manifest.get("layers")
    chain = manifest.get("operator_chain")
    if not isinstance(layers, list) or not layers:
        raise InvalidParam("manifest layers must be a non-empty array", parameter="layers")
    if not isinstance(chain, list) or not chain:
        raise InvalidParam(
            "manifest operator_chain must be a non-empty array",
            parameter="operator_chain",
        )
    layer_ids = [layer.get("id") for layer in layers]
    if len(layer_ids) != len(set(layer_ids)):
        raise InvalidParam("layer ids must be unique", parameter="operator_chain")
    if chain != layer_ids:
        raise InvalidParam(
            "operator_chain must list layer ids in execution order",
            parameter="operator_chain",
        )
    for layer in layers:
        layer_id = layer.get("id")
        if layer.get("op") != "linear":
            raise InvalidParam(
                "only the linear operator is supported",
                layer=layer_id,
                parameter="op",
            )
        for role in ("weight", "activation"):
            quant = layer.get(role, {}).get("quantization", {})
            if quant.get("granularity") == "per_channel":
                axis = quant.get("channel_axis")
                if not isinstance(axis, int):
                    raise InvalidParam(
                        "per-channel quantization requires channel_axis",
                        layer=layer_id,
                        parameter=f"{role}.quantization.channel_axis",
                    )
        if not isinstance(layer.get("accumulator_bits"), int):
            raise InvalidParam(
                "accumulator_bits must be an integer",
                layer=layer_id,
                parameter="accumulator_bits",
            )
        tolerance = layer.get("tolerance", {})
        for key in ("max_abs", "max_rel"):
            if not isinstance(tolerance.get(key), (int, float)) or float(tolerance[key]) < 0:
                raise InvalidParam(
                    "layer tolerance must be non-negative",
                    layer=layer_id,
                    parameter=f"tolerance.{key}",
                )
    if shapes.get("schema_version") != SCHEMA_VERSION:
        raise InvalidParam(
            "unsupported shape schema_version", parameter="shapes.schema_version"
        )
    if not isinstance(shapes.get("layers"), dict):
        raise InvalidParam("shape declarations need a layers object", parameter="shapes.layers")
    for layer_id in layer_ids:
        if layer_id not in shapes["layers"]:
            raise InvalidParam(
                "missing shape declaration",
                layer=layer_id,
                parameter="shapes",
            )


def import_configuration(
    workspace: Path,
    *,
    manifest_path: Path,
    shapes_path: Path,
    version: str,
    state_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = state_root(workspace, state_dir)
    ensure_state_layout(root)
    manifest_path = manifest_path.resolve()
    shapes_path = shapes_path.resolve()
    if not manifest_path.is_file():
        raise InvalidParam(f"manifest not found: {manifest_path}", parameter="manifest")
    if not shapes_path.is_file():
        raise InvalidParam(f"shape declarations not found: {shapes_path}", parameter="shapes")
    manifest = load_json(manifest_path)
    shapes = load_json(shapes_path)
    _validate_manifest(manifest, shapes)

    source_dir = _manifest_source_dir(manifest_path)
    shard_refs = _collect_shard_refs(manifest)
    resolved: dict[str, Path] = {}
    input_hashes: dict[str, str] = {}
    for ref in shard_refs:
        source = (source_dir / ref).resolve()
        if not source.is_file():
            raise InvalidParam(
                f"shard not found: {ref}",
                parameter=ref,
                details={"resolved_path": str(source)},
            )
        resolved[ref] = source
        input_hashes[ref] = digest_file(source)

    core = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "manifest": json_ready(manifest),
        "shapes": json_ready(shapes),
        "input_hashes": input_hashes,
    }
    config_id = digest_obj(core)
    config_dir = root / "configs" / config_id
    config_path = config_dir / "config.json"
    shard_files: dict[str, str] = {}

    with FileLock(root / "locks" / "config.lock"):
        if not config_path.exists():
            with temporary_directory(root / "configs", ".tmp-config-") as temp_dir:
                (temp_dir / "inputs").mkdir()
                shard_files = {}
                for index, ref in enumerate(shard_refs):
                    suffix = Path(ref).name
                    relative = f"inputs/{index:02d}_{suffix}"
                    shard_files[ref] = relative
                    (temp_dir / relative).write_bytes(resolved[ref].read_bytes())
                manifest_snapshot = temp_dir / "operator_manifest.json"
                shapes_snapshot = temp_dir / "shape_declarations.json"
                manifest_snapshot.write_bytes(
                    canonical_json_bytes(core["manifest"], pretty=True)
                )
                shapes_snapshot.write_bytes(
                    canonical_json_bytes(core["shapes"], pretty=True)
                )
                snapshot = {
                    **core,
                    "config_id": config_id,
                    "shard_files": shard_files,
                    "operator_manifest_file": "operator_manifest.json",
                    "shape_declarations_file": "shape_declarations.json",
                }
                atomic_write_json(temp_dir / "config.json", snapshot)
                if not exclusive_directory_publish(temp_dir, config_dir):
                    current = load_json(config_path)
                    if current.get("config_id") != config_id:
                        raise StaleCheckpoint(
                            "published config does not match its fingerprint",
                            details={"config_id": config_id},
                        )
        else:
            snapshot = load_json(config_path)
            if snapshot.get("config_id") != config_id:
                raise StaleCheckpoint(
                    "existing config fingerprint mismatch",
                    details={"config_id": config_id},
                )
            shard_files = snapshot.get("shard_files", {})

        registry_path = root / "configs" / "registry.json"
        if registry_path.exists():
            registry = load_json(registry_path)
        else:
            registry = {
                "schema_version": SCHEMA_VERSION,
                "versions": {},
                "current": None,
            }
        registry["versions"][version] = {
            "config_id": config_id,
            "pipeline_version": PIPELINE_VERSION,
        }
        registry["current"] = version
        atomic_write_json(registry_path, registry)

    result = load_json(config_path)
    result["config_version"] = version
    result["config_dir"] = str(config_dir)
    result["registry"] = registry
    return result


def load_registry(root: Path) -> dict[str, Any]:
    path = root / "configs" / "registry.json"
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "versions": {}, "current": None}
    return load_json(path)


def load_current_config(
    root: Path, *, version: str | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    registry = load_registry(root)
    selected = version or registry.get("current")
    if not selected or selected not in registry.get("versions", {}):
        raise InvalidParam(
            "no imported configuration is available",
            parameter="config_version",
            details={"requested": selected},
        )
    config_id = registry["versions"][selected]["config_id"]
    config_path = root / "configs" / config_id / "config.json"
    if not config_path.is_file():
        raise InvalidParam(
            "configuration snapshot is missing",
            parameter="config_version",
            details={"config_id": config_id},
        )
    config = load_json(config_path)
    config["config_version"] = selected
    config["config_dir"] = str(config_path.parent)
    config["registry"] = registry
    return config, registry


def shard_path(config: dict[str, Any], ref: str) -> Path:
    relative = config.get("shard_files", {}).get(ref)
    if not relative:
        raise InvalidParam(f"missing shard mapping for {ref}", parameter=ref)
    return Path(config["config_dir"]) / relative


def load_shard_array(config: dict[str, Any], ref: str, key: str, *, role: str) -> np.ndarray:
    path = shard_path(config, ref)
    try:
        with np.load(path, allow_pickle=False) as shard:
            if key not in shard:
                raise InvalidParam(
                    f"array key {key!r} missing from shard {ref}",
                    parameter=f"{role}.key",
                )
            return np.asarray(shard[key])
    except QPipeError:
        raise
    except (OSError, ValueError) as exc:
        raise InvalidParam(
            f"cannot load shard {ref}: {exc}",
            parameter=f"{role}.shard",
        ) from exc


def calibration_inputs(config: dict[str, Any]) -> dict[str, Any]:
    layers = []
    for layer in config["manifest"]["layers"]:
        layers.append(
            {
                "id": layer["id"],
                "op": layer["op"],
                "weight": {
                    "shard": layer["weight"]["shard"],
                    "key": layer["weight"]["key"],
                    "quantization": layer["weight"]["quantization"],
                },
                "activation": {
                    "shard": layer["activation"]["shard"],
                    "key": layer["activation"]["key"],
                    "quantization": layer["activation"]["quantization"],
                },
            }
        )
    return {
        "pipeline_version": PIPELINE_VERSION,
        "calibration_algorithm": "range_cover_v3",
        "operator_chain": config["manifest"]["operator_chain"],
        "layers": layers,
        "shapes": config["shapes"],
        "input_hashes": config["input_hashes"],
    }


def compute_stage_fingerprints(config: dict[str, Any]) -> dict[str, Any]:
    calibration_fp = digest_obj(calibration_inputs(config))
    accumulator_bits = {
        layer["id"]: int(layer["accumulator_bits"])
        for layer in config["manifest"]["layers"]
    }
    forward_fp = digest_obj(
        {
            "pipeline_version": PIPELINE_VERSION,
            "upstream": calibration_fp,
            "accumulator_bits": accumulator_bits,
            "integer_accumulation": "fixed_width_v1",
        }
    )
    attribution_fp = digest_obj(
        {
            "pipeline_version": PIPELINE_VERSION,
            "upstream": forward_fp,
            "attribution": "sensitivity_and_replay_v1",
            "attribution_sum_tolerance": config["manifest"].get(
                "attribution_sum_tolerance", 1e-9
            ),
        }
    )
    validation_input = {
        "pipeline_version": PIPELINE_VERSION,
        "upstream": attribution_fp,
        "layer_tolerances": {
            layer["id"]: layer["tolerance"] for layer in config["manifest"]["layers"]
        },
        "end_to_end_tolerance": config["manifest"].get("end_to_end_tolerance", {}),
    }
    validation_fp = digest_obj(validation_input)
    fingerprints = {
        "calibration": calibration_fp,
        "forward": forward_fp,
        "attribution": attribution_fp,
        "validation": validation_fp,
    }
    pipeline_fp = digest_obj(
        {
            "pipeline_version": PIPELINE_VERSION,
            "config_id": config["config_id"],
            "stages": fingerprints,
        }
    )
    return {
        "stages": fingerprints,
        "pipeline": pipeline_fp,
        "calibration_input": calibration_inputs(config),
        "validation_input": validation_input,
    }


def _flatten_layer_for_diff(config: dict[str, Any]) -> dict[str, Any]:
    return {
        layer["id"]: {
            "op": layer["op"],
            "weight": layer["weight"],
            "activation": layer["activation"],
            "accumulator_bits": layer["accumulator_bits"],
            "tolerance": layer["tolerance"],
            "shape": config["shapes"]["layers"].get(layer["id"]),
        }
        for layer in config["manifest"]["layers"]
    }


def diff_configs(old: dict[str, Any] | None, new: dict[str, Any]) -> list[dict[str, Any]]:
    if old is None:
        return [
            {
                "stage": "calibration",
                "reason": "initial_run",
                "parameter": None,
                "layer": None,
            }
        ]
    reasons: list[dict[str, Any]] = []
    if old.get("input_hashes") != new.get("input_hashes"):
        old_hashes = old.get("input_hashes", {})
        new_hashes = new.get("input_hashes", {})
        for ref in sorted(set(old_hashes) | set(new_hashes)):
            if old_hashes.get(ref) == new_hashes.get(ref):
                continue
            layer_ids = []
            for layer in new["manifest"]["layers"]:
                if ref in (layer["weight"]["shard"], layer["activation"]["shard"]):
                    layer_ids.append(layer["id"])
            for layer_id in layer_ids or [None]:
                reasons.append(
                    {
                        "stage": "calibration",
                        "reason": "shard_content_changed",
                        "parameter": ref,
                        "layer": layer_id,
                    }
                )
    old_layers = _flatten_layer_for_diff(old)
    new_layers = _flatten_layer_for_diff(new)
    if old.get("manifest", {}).get("operator_chain") != new["manifest"].get(
        "operator_chain"
    ):
        reasons.append(
            {
                "stage": "calibration",
                "reason": "operator_chain_changed",
                "parameter": "operator_chain",
                "layer": None,
            }
        )
    if old.get("shapes") != new.get("shapes"):
        reasons.append(
            {
                "stage": "calibration",
                "reason": "shape_declaration_changed",
                "parameter": "shapes",
                "layer": None,
            }
        )
    for layer_id in sorted(set(old_layers) | set(new_layers)):
        before = old_layers.get(layer_id, {})
        after = new_layers.get(layer_id, {})
        if before == after:
            continue
        for role in ("weight", "activation"):
            old_quant = before.get(role, {}).get("quantization", {})
            new_quant = after.get(role, {}).get("quantization", {})
            for key, name in (
                ("bits", "bitwidth_changed"),
                ("mode", "quantization_mode_changed"),
                ("granularity", "granularity_changed"),
                ("channel_axis", "channel_axis_changed"),
            ):
                if old_quant.get(key) != new_quant.get(key):
                    reasons.append(
                        {
                            "stage": "calibration",
                            "reason": name,
                            "parameter": f"{role}.quantization.{key}",
                            "layer": layer_id,
                        }
                    )
        if before.get("accumulator_bits") != after.get("accumulator_bits"):
            reasons.append(
                {
                    "stage": "forward",
                    "reason": "accumulator_bits_changed",
                    "parameter": "accumulator_bits",
                    "layer": layer_id,
                }
            )
        if before.get("tolerance") != after.get("tolerance"):
            reasons.append(
                {
                    "stage": "validation",
                    "reason": "layer_tolerance_changed",
                    "parameter": "tolerance",
                    "layer": layer_id,
                }
            )
    if old.get("manifest", {}).get("end_to_end_tolerance") != new["manifest"].get(
        "end_to_end_tolerance"
    ):
        reasons.append(
            {
                "stage": "validation",
                "reason": "end_to_end_tolerance_changed",
                "parameter": "end_to_end_tolerance",
                "layer": None,
            }
        )
    if old.get("manifest", {}).get("attribution_sum_tolerance") != new[
        "manifest"
    ].get("attribution_sum_tolerance"):
        reasons.append(
            {
                "stage": "attribution",
                "reason": "attribution_sum_tolerance_changed",
                "parameter": "attribution_sum_tolerance",
                "layer": None,
            }
        )
    if old.get("manifest") != new.get("manifest") and not reasons:
        reasons.append(
            {
                "stage": "calibration",
                "reason": "configuration_changed",
                "parameter": "manifest",
                "layer": None,
            }
        )
    return reasons


def earliest_invalidated_stage(
    old_fingerprints: dict[str, str] | None,
    new_fingerprints: dict[str, str],
    *,
    cache_available: dict[str, bool],
) -> tuple[str | None, list[dict[str, Any]]]:
    if old_fingerprints is None:
        return STAGES[0], []
    for stage in STAGES:
        if old_fingerprints.get(stage) != new_fingerprints.get(stage):
            return stage, []
        if not cache_available.get(stage, False):
            return stage, [
                {
                    "stage": stage,
                    "reason": "cache_missing",
                    "parameter": None,
                    "layer": None,
                }
            ]
    return None, []


def load_config_by_id(root: Path, config_id: str | None) -> dict[str, Any] | None:
    if not config_id:
        return None
    path = root / "configs" / config_id / "config.json"
    if not path.is_file():
        return None
    return load_json(path)


def list_versions(root: Path) -> dict[str, Any]:
    registry = load_registry(root)
    result = {
        "schema_version": SCHEMA_VERSION,
        "current": registry.get("current"),
        "versions": {},
    }
    for version, info in sorted(registry.get("versions", {}).items()):
        config = load_config_by_id(root, info.get("config_id"))
        fingerprints = compute_stage_fingerprints(config)["stages"] if config else None
        result["versions"][version] = {
            **info,
            "fingerprints": fingerprints,
        }
    return result
