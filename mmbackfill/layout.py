from __future__ import annotations

import math
from typing import Any

from .canonical import digest_obj
from .errors import EngineError


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _invalid(message: str, *, field: str | None = None, value: Any = None) -> EngineError:
    details: dict[str, Any] = {}
    if field is not None:
        details["field"] = field
    if value is not None:
        details["value"] = value
    return EngineError("E_INVALID_LAYOUT", message, "layout_validation", details=details)


def _positive_int(value: Any, field: str) -> int:
    if not _is_int(value) or value <= 0:
        raise _invalid(f"{field} must be a positive integer", field=field, value=value)
    return value


def _finite_weight(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise EngineError(
            "E_NUMERIC",
            f"{field} must be a finite number",
            "vector_build",
            details={"field": field, "value": repr(value)},
        ) from exc
    if not math.isfinite(number):
        raise EngineError(
            "E_NUMERIC",
            f"{field} contains NaN or Inf",
            "vector_build",
            details={"field": field, "value": repr(value)},
        )
    return number


def normalize_layout(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _invalid("layout must be a JSON object")
    schema_version = raw.get("schema_version", "layout-v1")
    if schema_version != "layout-v1":
        raise _invalid("unsupported layout schema_version", field="schema_version", value=schema_version)
    layout_id = raw.get("layout_id", "layout")
    if not isinstance(layout_id, str) or not layout_id:
        raise _invalid("layout_id must be a non-empty string", field="layout_id", value=layout_id)

    vector_raw = raw.get("vector")
    if not isinstance(vector_raw, dict):
        raise _invalid("vector must be an object", field="vector")
    dimension = _positive_int(vector_raw.get("dimension"), "vector.dimension")
    visual_raw = vector_raw.get("visual", {})
    text_raw = vector_raw.get("text", {})
    if not isinstance(visual_raw, dict) or not isinstance(text_raw, dict):
        raise _invalid("vector.visual and vector.text must be objects")
    grid_rows = _positive_int(visual_raw.get("grid_rows", 4), "vector.visual.grid_rows")
    grid_cols = _positive_int(visual_raw.get("grid_cols", 4), "vector.visual.grid_cols")
    edge_bins = _positive_int(visual_raw.get("edge_bins", 8), "vector.visual.edge_bins")
    text_dimension = _positive_int(text_raw.get("dimension", 128), "vector.text.dimension")
    if grid_rows > 64 or grid_cols > 64 or edge_bins > 1024 or text_dimension > 8192:
        raise _invalid("vector dimensions exceed safety limits")

    expected_dimension = visual_dimension(grid_rows, grid_cols, edge_bins) + text_dimension
    if dimension != expected_dimension:
        raise EngineError(
            "E_DIM_MISMATCH",
            "declared vector dimension does not match the deterministic descriptor layout",
            "vector_build",
            details={
                "field": "vector.dimension",
                "declared": dimension,
                "expected": expected_dimension,
                "visual_dimension": visual_dimension(grid_rows, grid_cols, edge_bins),
                "text_dimension": text_dimension,
            },
        )

    fusion_raw = vector_raw.get("fusion", {})
    if not isinstance(fusion_raw, dict):
        raise _invalid("vector.fusion must be an object", field="vector.fusion")
    image_weight = _finite_weight(fusion_raw.get("image_weight", 0.45), "vector.fusion.image_weight")
    text_weight = _finite_weight(fusion_raw.get("text_weight", 0.55), "vector.fusion.text_weight")

    layers_raw = raw.get("layers")
    if not isinstance(layers_raw, list) or not layers_raw:
        raise _invalid("layers must be a non-empty array", field="layers")
    layers: list[dict[str, int]] = []
    seen_layers: set[int] = set()
    for idx, layer_raw in enumerate(layers_raw):
        if not isinstance(layer_raw, dict):
            raise _invalid("layer entries must be objects", field=f"layers[{idx}]")
        layer_no = layer_raw.get("layer")
        if not _is_int(layer_no) or layer_no < 0:
            raise _invalid("layer number must be a non-negative integer", field=f"layers[{idx}].layer", value=layer_no)
        if layer_no in seen_layers:
            raise _invalid("duplicate layer number", field=f"layers[{idx}].layer", value=layer_no)
        seen_layers.add(layer_no)
        max_vectors = _positive_int(layer_raw.get("max_vectors"), f"layers[{idx}].max_vectors")
        max_shards = _positive_int(layer_raw.get("max_shards", max_vectors), f"layers[{idx}].max_shards")
        layers.append({"layer": layer_no, "max_vectors": max_vectors, "max_shards": max_shards})

    shards_raw = raw.get("shards")
    if not isinstance(shards_raw, list) or not shards_raw:
        raise _invalid("shards must be a non-empty array", field="shards")
    layer_map = {item["layer"]: item for item in layers}
    shards: list[dict[str, Any]] = []
    shard_ids: set[str] = set()
    for idx, shard_raw in enumerate(shards_raw):
        if not isinstance(shard_raw, dict):
            raise _invalid("shard entries must be objects", field=f"shards[{idx}]")
        shard_id = shard_raw.get("shard_id")
        if not isinstance(shard_id, str) or not shard_id:
            raise _invalid("shard_id must be a non-empty string", field=f"shards[{idx}].shard_id", value=shard_id)
        if shard_id in shard_ids:
            raise _invalid("duplicate shard_id", field=f"shards[{idx}].shard_id", value=shard_id)
        shard_ids.add(shard_id)
        layer_no = shard_raw.get("layer")
        if not _is_int(layer_no) or layer_no not in layer_map:
            raise _invalid("shard references unknown layer", field=f"shards[{idx}].layer", value=layer_no)
        capacity = _positive_int(shard_raw.get("capacity"), f"shards[{idx}].capacity")
        if capacity > layer_map[layer_no]["max_vectors"]:
            raise _invalid(
                "layer max_vectors is smaller than a shard capacity",
                field=f"layers[{layer_no}].max_vectors",
                value=layer_map[layer_no]["max_vectors"],
            )
        fallbacks = shard_raw.get("fallbacks", [])
        if not isinstance(fallbacks, list) or any(not isinstance(item, str) or not item for item in fallbacks):
            raise _invalid("fallbacks must be an array of shard ids", field=f"shards[{idx}].fallbacks")
        if shard_id in fallbacks:
            raise _invalid("shard cannot fall back to itself", field=f"shards[{idx}].fallbacks", value=shard_id)
        shards.append(
            {
                "shard_id": shard_id,
                "layer": layer_no,
                "capacity": capacity,
                "fallbacks": list(fallbacks),
                "config_order": idx,
            }
        )

    shard_by_id = {item["shard_id"]: item for item in shards}
    per_layer_shards: dict[int, int] = {item["layer"]: 0 for item in layers}
    for shard in shards:
        per_layer_shards[shard["layer"]] += 1
        for fallback in shard["fallbacks"]:
            if fallback not in shard_by_id:
                raise _invalid("shard references an unknown fallback", field=f"shards[{shard['config_order']}].fallbacks", value=fallback)
            if shard_by_id[fallback]["layer"] != shard["layer"]:
                raise _invalid("fallback shards must be on the same layer", field=f"shards[{shard['config_order']}].fallbacks", value=fallback)
    for layer in layers:
        if per_layer_shards[layer["layer"]] > layer["max_shards"]:
            raise _invalid(
                "initial shard count exceeds layer max_shards",
                field=f"layers[{layer['layer']}].max_shards",
                value=layer["max_shards"],
            )

    return {
        "schema_version": "layout-v1",
        "layout_id": layout_id,
        "vector": {
            "dimension": dimension,
            "fusion": {"image_weight": image_weight, "text_weight": text_weight},
            "visual": {"grid_rows": grid_rows, "grid_cols": grid_cols, "edge_bins": edge_bins},
            "text": {"dimension": text_dimension},
        },
        "layers": layers,
        "shards": shards,
    }


def visual_dimension(grid_rows: int, grid_cols: int, edge_bins: int) -> int:
    return grid_rows * grid_cols * 3 * 3 + edge_bins


def vector_config(layout: dict[str, Any]) -> dict[str, Any]:
    return layout["vector"]


def planning_config(layout: dict[str, Any]) -> dict[str, Any]:
    return {"layers": layout["layers"], "shards": layout["shards"]}


def layout_version(layout: dict[str, Any]) -> str:
    return digest_obj(layout)


def vector_config_fingerprint(layout: dict[str, Any]) -> str:
    return digest_obj({"schema": "mmb-vector-config-v1", "vector": vector_config(layout)})


def planning_config_fingerprint(layout: dict[str, Any]) -> str:
    return digest_obj({"schema": "mmb-planning-config-v1", "planning": planning_config(layout)})


def shard_specs(layout: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["shard_id"]: item for item in layout["shards"]}
