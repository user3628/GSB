"""Four-stage material generation pipeline with incremental checkpoint reuse."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from .artifacts import (
    decode_control,
    decode_diagnostics,
    decode_noise,
    decode_palette,
    encode_control,
    encode_diagnostics,
    encode_noise,
    encode_palette,
)
from .canonical import (
    atomic_write_bytes,
    canonical_json_bytes,
    fingerprint,
    read_json,
    sha256_bytes,
)
from .checkpoints import CheckpointStore, STAGE_NAMES, validate_material_id
from .config import (
    full_config_fingerprint,
    stage_config_fingerprint,
    stage_config_version_label,
)
from .diagnostics import build_diagnostics
from .errors import E_INVALID_RECIPE, E_INVALID_STAGE, MatgenError
from .noise import (
    compose_noise,
    recipe_noise_fingerprint,
    resolve_base_resolution,
    validate_noise_definition,
    validate_target_resolution,
)
from .palette import (
    build_palette_artifacts,
    load_palette,
    palette_stage_fingerprint,
    validate_unit_color,
    validate_unit_scalar,
)
from .prompt import control_fingerprint_input, generate_control_vector


REPORT_SCHEMA = "matgen.report.v1"
RUN_MANIFEST_SCHEMA = "matgen.run_manifest.v1"


def load_recipes(path: str | Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, dict) and isinstance(payload.get("materials"), list):
        recipes = payload["materials"]
    elif isinstance(payload, list):
        recipes = payload
    else:
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message="recipe file must be a JSON array or an object with a materials array",
        )
    seen: set[str] = set()
    validated: list[dict[str, Any]] = []
    for index, recipe in enumerate(recipes):
        if not isinstance(recipe, dict):
            raise MatgenError(
                code=E_INVALID_RECIPE,
                message="each material recipe must be an object",
                details={"recipe_index": index},
            )
        material_id = validate_material_id(recipe.get("material_id"))
        if material_id in seen:
            raise MatgenError(
                code=E_INVALID_RECIPE,
                message="duplicate material_id in recipe list",
                material_id=material_id,
            )
        seen.add(material_id)
        validated.append(copy.deepcopy(recipe))
    return sorted(validated, key=lambda item: str(item["material_id"]))


def _stage_input_control(recipe: dict[str, Any], config: dict[str, Any]) -> str:
    return fingerprint(
        {
            "control": control_fingerprint_input(recipe, int(config["hash_seed"])),
            "stage_config": stage_config_fingerprint(config, "control"),
        }
    )


def _stage_input_noise(
    recipe: dict[str, Any], config: dict[str, Any], control_record_digest: str
) -> str:
    return fingerprint(
        {
            "noise_recipe": recipe_noise_fingerprint(recipe, config, control_record_digest),
            "stage_config": stage_config_fingerprint(config, "noise"),
            "noise_algorithm": "splitmix64-value-noise-v1",
        }
    )


def _stage_input_palette(
    recipe: dict[str, Any],
    config: dict[str, Any],
    control_record_digest: str,
    noise_record_digest: str,
    palette_source_fingerprint: str,
) -> str:
    return fingerprint(
        {
            "palette_recipe": palette_stage_fingerprint(
                recipe,
                control_record_digest,
                noise_record_digest,
                palette_source_fingerprint,
            ),
            "stage_config": stage_config_fingerprint(config, "palette"),
            "palette_algorithm": "deterministic-nearest-rgb-v1",
        }
    )


def _stage_input_diagnostics(
    recipe: dict[str, Any],
    config: dict[str, Any],
    control_record_digest: str,
    noise_record_digest: str,
    palette_record_digest: str,
) -> str:
    return fingerprint(
        {
            "recipe_fingerprint": fingerprint(recipe),
            "control_digest": control_record_digest,
            "noise_digest": noise_record_digest,
            "palette_digest": palette_record_digest,
            "stage_config": stage_config_fingerprint(config, "diagnostics"),
            "diagnostics_algorithm": "decimal-reference-v1",
        }
    )


def _reuse_or_invalidate(
    store: CheckpointStore,
    material_id: str,
    stage_index: int,
    *,
    expected_input_fingerprint: str,
    expected_config_version: str,
) -> tuple[dict[str, Any], dict[str, bytes]] | None:
    try:
        record = store.load_stage(material_id, stage_index)
    except MatgenError as exc:
        if exc.code == E_INVALID_STAGE:
            return None
        raise
    if (
        record.get("stage_input_fingerprint") != expected_input_fingerprint
        or record.get("config_version") != expected_config_version
    ):
        store.invalidate_from(
            material_id,
            stage_index,
            reason=(
                "stage input fingerprint changed from "
                f"{record.get('stage_input_fingerprint')} to {expected_input_fingerprint}"
            ),
        )
        return None
    files = store.load_artifact_bytes(material_id, stage_index)
    return record, files


def _publish(
    store: CheckpointStore,
    *,
    recipe: dict[str, Any],
    config: dict[str, Any],
    stage_index: int,
    stage_input_fingerprint: str,
    artifacts: dict[str, bytes],
    upstream_record_digest: str,
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    stage = STAGE_NAMES[stage_index]
    return store.publish_stage(
        material_id=material_id,
        stage_index=stage_index,
        recipe_fingerprint=fingerprint(recipe),
        stage_input_fingerprint=stage_input_fingerprint,
        config_version=stage_config_version_label(config, stage),
        artifacts=artifacts,
        upstream_record_digest=upstream_record_digest,
    )


def _process_material(
    *,
    recipe: dict[str, Any],
    config: dict[str, Any],
    cards_path: str | Path | None,
    store: CheckpointStore,
    output_root: Path,
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    target_width, target_height = validate_target_resolution(recipe)
    resolve_base_resolution(recipe, config, target_width, target_height)
    validate_noise_definition(recipe)
    recipe_fingerprint = fingerprint(recipe)

    control_input = _stage_input_control(recipe, config)
    control_config_version = stage_config_version_label(config, "control")
    control_reuse = _reuse_or_invalidate(
        store,
        material_id,
        0,
        expected_input_fingerprint=control_input,
        expected_config_version=control_config_version,
    )
    if control_reuse is None:
        control_artifact = generate_control_vector(
            recipe.get("description"),
            material_id=material_id,
            hash_seed=int(config["hash_seed"]),
        )
        control_files = encode_control(control_artifact)
        control_record = _publish(
            store,
            recipe=recipe,
            config=config,
            stage_index=0,
            stage_input_fingerprint=control_input,
            artifacts=control_files,
            upstream_record_digest="",
        )
    else:
        control_record, control_files = control_reuse
        control_artifact = decode_control(control_files)

    noise_input = _stage_input_noise(recipe, config, control_record["record_digest"])
    noise_config_version = stage_config_version_label(config, "noise")
    noise_reuse = _reuse_or_invalidate(
        store,
        material_id,
        1,
        expected_input_fingerprint=noise_input,
        expected_config_version=noise_config_version,
    )
    if noise_reuse is None:
        noise_artifact = compose_noise(recipe, config)
        noise_files = encode_noise(noise_artifact)
        noise_record = _publish(
            store,
            recipe=recipe,
            config=config,
            stage_index=1,
            stage_input_fingerprint=noise_input,
            artifacts=noise_files,
            upstream_record_digest=control_record["record_digest"],
        )
    else:
        noise_record, noise_files = noise_reuse
        noise_artifact = decode_noise(noise_files)

    palette_spec = load_palette(recipe, cards_path=cards_path)
    palette_input = _stage_input_palette(
        recipe,
        config,
        control_record["record_digest"],
        noise_record["record_digest"],
        palette_spec["source_fingerprint"],
    )
    palette_config_version = stage_config_version_label(config, "palette")
    palette_reuse = _reuse_or_invalidate(
        store,
        material_id,
        2,
        expected_input_fingerprint=palette_input,
        expected_config_version=palette_config_version,
    )
    if palette_reuse is None:
        palette_artifact = build_palette_artifacts(
            recipe,
            control_artifact,
            noise_artifact,
            palette_spec,
        )
        palette_files = encode_palette(palette_artifact)
        palette_record = _publish(
            store,
            recipe=recipe,
            config=config,
            stage_index=2,
            stage_input_fingerprint=palette_input,
            artifacts=palette_files,
            upstream_record_digest=noise_record["record_digest"],
        )
    else:
        palette_record, palette_files = palette_reuse
        palette_artifact = decode_palette(palette_files, control_artifact=control_artifact)

    diagnostics_input = _stage_input_diagnostics(
        recipe,
        config,
        control_record["record_digest"],
        noise_record["record_digest"],
        palette_record["record_digest"],
    )
    diagnostics_config_version = stage_config_version_label(config, "diagnostics")
    diagnostics_reuse = _reuse_or_invalidate(
        store,
        material_id,
        3,
        expected_input_fingerprint=diagnostics_input,
        expected_config_version=diagnostics_config_version,
    )
    if diagnostics_reuse is None:
        diagnostics = build_diagnostics(
            recipe=recipe,
            config=config,
            control_artifact=control_artifact,
            noise_artifact=noise_artifact,
            palette_artifact=palette_artifact,
        )
        diagnostics_files = encode_diagnostics(diagnostics)
        diagnostics_record = _publish(
            store,
            recipe=recipe,
            config=config,
            stage_index=3,
            stage_input_fingerprint=diagnostics_input,
            artifacts=diagnostics_files,
            upstream_record_digest=palette_record["record_digest"],
        )
    else:
        diagnostics_record, diagnostics_files = diagnostics_reuse
        diagnostics = decode_diagnostics(diagnostics_files)

    records = [control_record, noise_record, palette_record, diagnostics_record]
    output_files = _write_material_output(
        output_root=output_root,
        material_id=material_id,
        recipe_fingerprint=recipe_fingerprint,
        palette_artifact=palette_artifact,
        diagnostics=diagnostics,
        records=records,
    )
    return {
        "material_id": material_id,
        "recipe": recipe,
        "control_record": control_record,
        "noise_record": noise_record,
        "palette_record": palette_record,
        "diagnostics_record": diagnostics_record,
        "palette_artifact": palette_artifact,
        "diagnostics": diagnostics,
        "output_files": output_files,
    }


def _write_material_output(
    *,
    output_root: Path,
    material_id: str,
    recipe_fingerprint: str,
    palette_artifact: dict[str, Any],
    diagnostics: dict[str, Any],
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    directory = output_root / validate_material_id(material_id)
    directory.mkdir(parents=True, exist_ok=True)
    texture_bytes = palette_artifact["texture_rgba"].tobytes(order="C")
    material_bytes = canonical_json_bytes(palette_artifact["material"]) + b"\n"
    diagnostics_bytes = canonical_json_bytes(diagnostics) + b"\n"
    result = {
        "schema": "matgen.material_result.v1",
        "material_id": material_id,
        "recipe_fingerprint": recipe_fingerprint,
        "texture_sha256": palette_artifact["texture_sha256"],
        "png_sha256": palette_artifact["png_sha256"],
        "material": palette_artifact["material"],
        "diagnostics_summary": {
            "error_budget": diagnostics["error_budget"],
            "attribution": {
                "contribution_order": diagnostics["attribution"]["contribution_order"],
                "perturbation_replay_order": diagnostics["attribution"]["perturbation_replay_order"],
                "review_items": diagnostics["attribution"]["review_items"],
            },
            "material_parameters": diagnostics["material_parameters"],
        },
        "stage_records": [
            {
                "stage": record["stage"],
                "stage_index": record["stage_index"],
                "sequence": record["sequence"],
                "stage_revision": record["stage_revision"],
                "global_sequence": record["global_sequence"],
                "stage_input_fingerprint": record["stage_input_fingerprint"],
                "artifact_digest": record["artifact_digest"],
                "record_digest": record["record_digest"],
            }
            for record in records
        ],
    }
    file_payloads = {
        "texture.rgba8": texture_bytes,
        "texture.png": palette_artifact["texture_png"],
        "material.json": material_bytes,
        "diagnostics.json": diagnostics_bytes,
        "result.json": _canonical_json(result),
    }
    file_summaries = []
    for name in sorted(file_payloads):
        data = file_payloads[name]
        atomic_write_bytes(directory / name, data)
        file_summaries.append(
            {
                "name": name,
                "size": len(data),
                "sha256": sha256_bytes(data),
            }
        )
    return file_summaries


def _canonical_json(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _build_report(
    *,
    config: dict[str, Any],
    completed: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    material_entries = []
    error_summary = []
    for item in sorted(completed, key=lambda entry: entry["material_id"]):
        diagnostics = item["diagnostics"]
        material = item["palette_artifact"]["material"]
        output_files = sorted(item["output_files"], key=lambda entry: entry["name"])
        material_entries.append(
            {
                "material_id": item["material_id"],
                "material": material,
                "stage_records": [
                    {
                        "stage": record["stage"],
                        "sequence": record["sequence"],
                        "stage_revision": record["stage_revision"],
                        "record_digest": record["record_digest"],
                    }
                    for record in (
                        item["control_record"],
                        item["noise_record"],
                        item["palette_record"],
                        item["diagnostics_record"],
                    )
                ],
                "output_files": output_files,
            }
        )
        error_summary.append(
            {
                "material_id": item["material_id"],
                "over_budget_octaves": diagnostics["error_budget"]["over_budget_octaves"],
                "closest_octave": diagnostics["error_budget"]["closest_octave"],
                "total_used_bound": diagnostics["error_budget"]["total_used_bound"],
                "total_budget_occupancy": diagnostics["error_budget"]["total_budget_occupancy"],
                "total_over_budget": diagnostics["error_budget"]["total_over_budget"],
                "review_required_octaves": [
                    entry["octave_index"] for entry in diagnostics["attribution"]["review_items"]
                ],
            }
        )
    run_digest = fingerprint(
        {
            "schema": RUN_MANIFEST_SCHEMA,
            "config_fingerprint": full_config_fingerprint(config),
            "materials": material_entries,
        }
    )
    report = {
        "schema": REPORT_SCHEMA,
        "run_digest": run_digest,
        "config_fingerprint": full_config_fingerprint(config),
        "materials": material_entries,
        "error_summary": sorted(error_summary, key=lambda entry: entry["material_id"]),
    }
    run_manifest = {
        "schema": RUN_MANIFEST_SCHEMA,
        "run_digest": run_digest,
        "config_fingerprint": full_config_fingerprint(config),
        "material_count": len(material_entries),
        "material_ids": [entry["material_id"] for entry in material_entries],
        "report_schema": REPORT_SCHEMA,
    }
    return report, run_manifest


def generate_all(
    *,
    workspace: str | Path,
    recipes_path: str | Path,
    cards_path: str | Path | None,
    config: dict[str, Any],
    output_root: str | Path,
    stop_after_materials: int | None = None,
) -> dict[str, Any]:
    """Run the deterministic four-stage pipeline and honour graceful stop requests."""

    workspace_path = Path(workspace).resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    output_path = Path(output_root)
    if not output_path.is_absolute():
        output_path = workspace_path / output_path
    output_path.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(workspace_path)
    if stop_after_materials is not None and (
        isinstance(stop_after_materials, bool) or stop_after_materials < 1
    ):
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message="stop_after_materials must be a positive integer",
        )
    if store.stop_requested():
        return {
            "status": "stopped",
            "completed_material_ids": [],
            "run_digest": None,
            "reason": "stop flag was already present",
        }
    recipes = load_recipes(recipes_path)
    # A new run invalidates any previously published aggregate report until it
    # reaches a complete terminal state again.
    (output_path / "report.json").unlink(missing_ok=True)
    (output_path / "run_manifest.json").unlink(missing_ok=True)
    completed: list[dict[str, Any]] = []
    stop_reason: str | None = None
    for recipe in recipes:
        material_id = str(recipe["material_id"])
        # Validate color/scalar inputs before a material becomes the current
        # indivisible unit.  Noise validation happens in its own stage so the
        # expected material-scoped error code is preserved.
        validate_unit_color(recipe.get("base_color"), material_id=material_id, label="base_color")
        validate_unit_scalar(recipe.get("roughness"), material_id=material_id, label="roughness")
        validate_unit_scalar(recipe.get("metalness"), material_id=material_id, label="metalness")
        completed.append(
            _process_material(
                recipe=recipe,
                config=config,
                cards_path=cards_path,
                store=store,
                output_root=output_path,
            )
        )
        if stop_after_materials is not None and len(completed) >= stop_after_materials:
            store.request_stop()
            stop_reason = "requested stop boundary reached"
            break
        if store.stop_requested():
            stop_reason = "stop request observed between material units"
            break
    if stop_reason is not None:
        return {
            "status": "stopped",
            "completed_material_ids": [
                item["material_id"] for item in sorted(completed, key=lambda entry: entry["material_id"])
            ],
            "run_digest": None,
            "reason": stop_reason,
        }

    report, run_manifest = _build_report(config=config, completed=completed)
    report_bytes = _canonical_json(report)
    atomic_write_bytes(output_path / "report.json", report_bytes)
    run_manifest["report_sha256"] = hashlib.sha256(report_bytes).hexdigest()
    atomic_write_bytes(output_path / "run_manifest.json", _canonical_json(run_manifest))
    return {
        "status": "complete",
        "completed_material_ids": [item["material_id"] for item in completed],
        "run_digest": report["run_digest"],
        "report": report,
        "run_manifest": run_manifest,
        "output_root": str(output_path),
    }


def get_status(workspace: str | Path) -> dict[str, Any]:
    return CheckpointStore(workspace).status()


def request_stop(workspace: str | Path) -> None:
    CheckpointStore(workspace).request_stop()


def clear_stop(workspace: str | Path) -> None:
    CheckpointStore(workspace).clear_stop()


def get_error_report(workspace: str | Path, material_id: str) -> dict[str, Any]:
    store = CheckpointStore(workspace)
    files = store.load_artifact_bytes(material_id, 3)
    return decode_diagnostics(files)


def get_material_result(workspace: str | Path, material_id: str) -> dict[str, Any]:
    store = CheckpointStore(workspace)
    records = [store.load_stage(material_id, stage_index) for stage_index in range(4)]
    palette_files = store.load_artifact_bytes(material_id, 2)
    diagnostics_files = store.load_artifact_bytes(material_id, 3)
    material = json.loads(palette_files["material.json"].decode("utf-8"))
    diagnostics = decode_diagnostics(diagnostics_files)
    return {
        "material_id": material_id,
        "material": material,
        "diagnostics": diagnostics,
        "stage_records": records,
        "texture_png": palette_files["texture.png"],
        "texture_rgba8": palette_files["texture.rgba8"],
    }


def get_versions(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "algorithm_version": config["algorithm_version"],
        "config_version": f"{config['algorithm_version']}+{full_config_fingerprint(config)[:16]}",
        "stages": {
            stage: stage_config_version_label(config, stage) for stage in STAGE_NAMES
        },
    }

