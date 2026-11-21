"""Canonical byte encodings for persisted stage artifacts."""

from __future__ import annotations

import json
from typing import Any

import numpy as np

from .canonical import canonical_json_bytes, fingerprint, sha256_bytes
from .errors import E_NUMERIC, E_STALE_CHECKPOINT, MatgenError
from .palette import validate_quantized_texture


def _json_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _decode_json(data: bytes, *, material_id: str | None, name: str) -> Any:
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message=f"artifact {name} is not valid canonical JSON",
            material_id=material_id,
            details={"artifact": name, "transition": "tamper"},
        ) from exc


def encode_control(control_artifact: dict[str, Any]) -> dict[str, bytes]:
    return {"control.json": _json_bytes(control_artifact)}


def decode_control(files: dict[str, bytes]) -> dict[str, Any]:
    data = files.get("control.json")
    if data is None:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="control artifact is incomplete",
            stage="control",
            details={"transition": "tamper"},
        )
    artifact = _decode_json(data, material_id=None, name="control.json")
    if not isinstance(artifact, dict) or len(artifact.get("control_vector", [])) != 32:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="control artifact has an invalid vector shape",
            material_id=artifact.get("material_id") if isinstance(artifact, dict) else None,
            stage="control",
            details={"transition": "tamper"},
        )
    return artifact


def encode_noise(noise_artifact: dict[str, Any]) -> dict[str, bytes]:
    field = np.asarray(noise_artifact["field"], dtype="<f8")
    octaves = np.stack(
        [np.asarray(field_values, dtype="<f8") for field_values in noise_artifact["octave_fields"]],
        axis=0,
    )
    field_bytes = field.tobytes(order="C")
    octave_bytes = octaves.tobytes(order="C")
    metadata = {
        "schema": noise_artifact["schema"],
        "material_id": noise_artifact["material_id"],
        "seed": noise_artifact["seed"],
        "octave_count": noise_artifact["octave_count"],
        "octave_definitions": [
            {
                **{key: value for key, value in definition.items() if not key.endswith("_decimal")},
                "frequency_decimal": str(definition["frequency_decimal"]),
                "amplitude_decimal": str(definition["amplitude_decimal"]),
            }
            for definition in noise_artifact["octave_definitions"]
        ],
        "amplitudes": noise_artifact["amplitudes"],
        "normalized_amplitudes": noise_artifact["normalized_amplitudes"],
        "decimal_normalized_amplitudes": [
            str(value) for value in noise_artifact["decimal_normalized_amplitudes"]
        ],
        "base_width": noise_artifact["base_width"],
        "base_height": noise_artifact["base_height"],
        "target_width": noise_artifact["target_width"],
        "target_height": noise_artifact["target_height"],
        "field_shape": list(field.shape),
        "octave_shape": list(octaves.shape),
        "field_sha256": sha256_bytes(field_bytes),
        "octave_fields_sha256": sha256_bytes(octave_bytes),
        "field_fingerprint": noise_artifact["field_fingerprint"],
        "dtype": "little-endian float64",
    }
    return {
        "noise.json": _json_bytes(metadata),
        "noise.f64": field_bytes,
        "octaves.f64": octave_bytes,
    }


def decode_noise(files: dict[str, bytes]) -> dict[str, Any]:
    required = {"noise.json", "noise.f64", "octaves.f64"}
    if not required.issubset(files):
        missing = sorted(required.difference(files))
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="noise artifact is incomplete",
            stage="noise",
            details={"transition": "tamper", "missing": missing},
        )
    metadata = _decode_json(files["noise.json"], material_id=None, name="noise.json")
    if not isinstance(metadata, dict):
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="noise metadata must be an object",
            stage="noise",
            details={"transition": "tamper"},
        )
    material_id = metadata.get("material_id")
    field_shape = tuple(int(value) for value in metadata.get("field_shape", []))
    octave_shape = tuple(int(value) for value in metadata.get("octave_shape", []))
    if len(field_shape) != 2 or len(octave_shape) != 3:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="noise metadata has invalid array shapes",
            material_id=material_id,
            stage="noise",
            details={"transition": "tamper"},
        )
    field_bytes = files["noise.f64"]
    octave_bytes = files["octaves.f64"]
    if sha256_bytes(field_bytes) != metadata.get("field_sha256") or sha256_bytes(
        octave_bytes
    ) != metadata.get("octave_fields_sha256"):
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="noise artifact internal digest mismatch",
            material_id=material_id,
            stage="noise",
            details={"transition": "tamper"},
        )
    expected_field_values = int(np.prod(field_shape))
    expected_octave_values = int(np.prod(octave_shape))
    if len(field_bytes) != expected_field_values * 8 or len(octave_bytes) != expected_octave_values * 8:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="noise artifact byte length does not match its shape",
            material_id=material_id,
            stage="noise",
            details={"transition": "tamper"},
        )
    field = np.frombuffer(field_bytes, dtype="<f8").reshape(field_shape).astype(np.float64, copy=True)
    octaves = np.frombuffer(octave_bytes, dtype="<f8").reshape(octave_shape).astype(np.float64, copy=True)
    if not np.all(np.isfinite(field)) or not np.all(np.isfinite(octaves)):
        raise MatgenError(
            code=E_NUMERIC,
            message="persisted noise artifact contains non-finite values",
            material_id=material_id,
            stage="noise",
        )
    return {
        **metadata,
        "field": field,
        "octave_fields": [octaves[index].copy() for index in range(octaves.shape[0])],
    }


def encode_palette(palette_artifact: dict[str, Any]) -> dict[str, bytes]:
    texture = validate_quantized_texture(
        palette_artifact["texture_rgba"],
        material_id=palette_artifact["material_id"],
    )
    indices = np.asarray(palette_artifact["palette_indices"], dtype="<u2")
    gray = np.asarray(palette_artifact["gray_resampled"], dtype="<f8")
    index_bytes = indices.tobytes(order="C")
    gray_bytes = gray.tobytes(order="C")
    texture_bytes = texture.tobytes(order="C")
    state = {
        "schema": "matgen.palette_state.v1",
        "material_id": palette_artifact["material_id"],
        "palette_spec": {
            "schema": palette_artifact["palette_spec"]["schema"],
            "material_id": palette_artifact["palette_spec"]["material_id"],
            "palette_rgb_u8": palette_artifact["palette_spec"]["palette_u8"].tolist(),
            "metadata": palette_artifact["palette_spec"]["metadata"],
            "source_fingerprint": palette_artifact["palette_spec"]["source_fingerprint"],
        },
        "derived": palette_artifact["derived"],
        "recipe_roughness": palette_artifact["recipe_roughness"],
        "recipe_metalness": palette_artifact["recipe_metalness"],
        "index_shape": list(indices.shape),
        "gray_shape": list(gray.shape),
        "texture_shape": list(texture.shape),
        "index_sha256": sha256_bytes(index_bytes),
        "gray_sha256": sha256_bytes(gray_bytes),
        "texture_sha256": sha256_bytes(texture_bytes),
        "png_sha256": palette_artifact["png_sha256"],
    }
    return {
        "material.json": _json_bytes(palette_artifact["material"]),
        "palette_state.json": _json_bytes(state),
        "palette_indices.u16": index_bytes,
        "gray.f64": gray_bytes,
        "texture.rgba8": texture_bytes,
        "texture.png": palette_artifact["texture_png"],
    }


def decode_palette(files: dict[str, bytes], *, control_artifact: dict[str, Any]) -> dict[str, Any]:
    required = {
        "material.json",
        "palette_state.json",
        "palette_indices.u16",
        "gray.f64",
        "texture.rgba8",
        "texture.png",
    }
    if not required.issubset(files):
        missing = sorted(required.difference(files))
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="palette artifact is incomplete",
            stage="palette",
            details={"transition": "tamper", "missing": missing},
        )
    state = _decode_json(files["palette_state.json"], material_id=None, name="palette_state.json")
    material = _decode_json(files["material.json"], material_id=None, name="material.json")
    if not isinstance(state, dict) or not isinstance(material, dict):
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="palette artifact metadata must be objects",
            stage="palette",
            details={"transition": "tamper"},
        )
    material_id = state.get("material_id")
    index_shape = tuple(int(value) for value in state.get("index_shape", []))
    gray_shape = tuple(int(value) for value in state.get("gray_shape", []))
    texture_shape = tuple(int(value) for value in state.get("texture_shape", []))
    if len(index_shape) != 2 or len(gray_shape) != 2 or len(texture_shape) != 3:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="palette artifact declares invalid shapes",
            material_id=material_id,
            stage="palette",
            details={"transition": "tamper"},
        )
    for name, shape, byte_width in (
        ("palette_indices.u16", index_shape, 2),
        ("gray.f64", gray_shape, 8),
        ("texture.rgba8", texture_shape, 1),
    ):
        if len(files[name]) != int(np.prod(shape)) * byte_width:
            raise MatgenError(
                code=E_STALE_CHECKPOINT,
                message=f"palette artifact {name} has an invalid byte length",
                material_id=material_id,
                stage="palette",
                details={"transition": "tamper"},
            )
    if (
        sha256_bytes(files["palette_indices.u16"]) != state.get("index_sha256")
        or sha256_bytes(files["gray.f64"]) != state.get("gray_sha256")
        or sha256_bytes(files["texture.rgba8"]) != state.get("texture_sha256")
    ):
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="palette artifact internal digest mismatch",
            material_id=material_id,
            stage="palette",
            details={"transition": "tamper"},
        )
    indices = np.frombuffer(files["palette_indices.u16"], dtype="<u2").reshape(index_shape).astype(np.int64)
    gray = np.frombuffer(files["gray.f64"], dtype="<f8").reshape(gray_shape).astype(np.float64, copy=True)
    texture = np.frombuffer(files["texture.rgba8"], dtype=np.uint8).reshape(texture_shape).copy()
    validate_quantized_texture(texture, material_id=material_id)
    palette_spec = state["palette_spec"]
    palette_spec["palette_u8"] = np.asarray(palette_spec["palette_rgb_u8"], dtype=np.uint8)
    palette_spec["palette_float"] = palette_spec["palette_u8"].astype(np.float64) / 255.0
    palette_artifact = {
        "schema": "matgen.palette_artifacts.v1",
        "material_id": material_id,
        "material": material,
        "texture_rgba": texture,
        "texture_png": files["texture.png"],
        "texture_sha256": state["texture_sha256"],
        "png_sha256": state["png_sha256"],
        "palette_indices": indices,
        "gray_resampled": gray,
        "palette_spec": palette_spec,
        "derived": state["derived"],
        "recipe_roughness": state["recipe_roughness"],
        "recipe_metalness": state["recipe_metalness"],
        "control_vector": control_artifact["control_vector"],
    }
    return palette_artifact


def encode_diagnostics(diagnostics: dict[str, Any]) -> dict[str, bytes]:
    return {"diagnostics.json": _json_bytes(diagnostics)}


def decode_diagnostics(files: dict[str, bytes]) -> dict[str, Any]:
    data = files.get("diagnostics.json")
    if data is None:
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="diagnostics artifact is incomplete",
            stage="diagnostics",
            details={"transition": "tamper"},
        )
    diagnostics = _decode_json(data, material_id=None, name="diagnostics.json")
    if not isinstance(diagnostics, dict):
        raise MatgenError(
            code=E_STALE_CHECKPOINT,
            message="diagnostics artifact must be an object",
            stage="diagnostics",
            details={"transition": "tamper"},
        )
    return diagnostics


def artifact_file_digest(files: dict[str, bytes]) -> str:
    return fingerprint(
        [
            {"name": name, "size": len(data), "sha256": sha256_bytes(data)}
            for name, data in sorted(files.items())
        ]
    )

