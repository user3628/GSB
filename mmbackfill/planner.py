from __future__ import annotations

from typing import Any

from .canonical import digest_obj
from .errors import EngineError


def _assignment_signature(shard: dict[str, Any]) -> str:
    values = [
        {
            "text_id": entry["text_id"],
            "image_id": entry["image_id"],
            "ordinal": entry["ordinal"],
            "vector_digest": entry["vector_digest"],
        }
        for entry in shard["entries"]
    ]
    return digest_obj({"schema": "mmb-shard-membership-v1", "shard_id": shard["shard_id"], "entries": values})


def _choose_available(
    shard: dict[str, Any],
    shard_by_id: dict[str, dict[str, Any]],
    layer_counts: dict[int, int],
    layer_specs: dict[int, dict[str, int]],
) -> dict[str, Any] | None:
    layer = shard["layer"]
    if layer_counts[layer] + 1 > layer_specs[layer]["max_vectors"]:
        return None
    if len(shard["entries"]) < shard["capacity"]:
        return shard
    for fallback_id in shard["fallbacks"]:
        fallback = shard_by_id[fallback_id]
        if len(fallback["entries"]) < fallback["capacity"]:
            return fallback
    return None


def _new_split_id(parent_id: str, existing: set[str], counter: int) -> str:
    candidate = f"{parent_id}~split-{counter}"
    suffix = counter
    while candidate in existing:
        suffix += 1
        candidate = f"{parent_id}~split-{suffix}"
    return candidate


def plan_index(entries: list[dict[str, Any]], layout: dict[str, Any], version_fingerprints: dict[str, Any]) -> dict[str, Any]:
    """Deterministically place all entries into shards.

    Planning always starts from the declared shard topology and replays every
    entry in stable `(ordinal, text_id)` order. Consequently the final owned
    bytes do not depend on the order batches arrived.
    """
    ordered = sorted(entries, key=lambda item: (item["ordinal"], item["text_id"]))
    seen_vectors: dict[str, dict[str, Any]] = {}
    for entry in ordered:
        previous = seen_vectors.get(entry["vector_digest"])
        if previous is not None and previous["text_id"] != entry["text_id"]:
            conflicts = [
                {
                    "text_id": previous["text_id"],
                    "image_id": previous["image_id"],
                    "target_shard": previous["target_shard"],
                    "vector_digest": entry["vector_digest"],
                },
                {
                    "text_id": entry["text_id"],
                    "image_id": entry["image_id"],
                    "target_shard": entry["target_shard"],
                    "vector_digest": entry["vector_digest"],
                },
            ]
            raise EngineError(
                "E_DUPLICATE_ROUTE",
                "the same retrieval vector would be placed more than once",
                "backfill_planning",
                entries=conflicts,
                conflicts=conflicts,
                details={"shards": sorted({previous["target_shard"], entry["target_shard"]})},
            )
        seen_vectors[entry["vector_digest"]] = entry

    shard_by_id: dict[str, dict[str, Any]] = {}
    for spec in layout["shards"]:
        shard_by_id[spec["shard_id"]] = {
            "shard_id": spec["shard_id"],
            "layer": spec["layer"],
            "capacity": spec["capacity"],
            "fallbacks": list(spec["fallbacks"]),
            "config_order": spec["config_order"],
            "parent_shard_id": None,
            "split_generation": 0,
            "entries": [],
        }
    layer_specs = {item["layer"]: item for item in layout["layers"]}
    layer_counts: dict[int, int] = {item["layer"]: 0 for item in layout["layers"]}
    layer_shard_counts = {item["layer"]: 0 for item in layout["layers"]}
    for shard in shard_by_id.values():
        layer_shard_counts[shard["layer"]] += 1

    split_history: list[dict[str, Any]] = []
    split_migrations: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    split_counter = 0
    for entry in ordered:
        target_id = entry["target_shard"]
        target = shard_by_id.get(target_id)
        if target is None:
            raise EngineError(
                "E_UNKNOWN_REF",
                "text entry references an unknown target shard",
                "backfill_planning",
                entries=[{"text_id": entry["text_id"], "image_id": entry["image_id"]}],
                details={"target_shard": target_id, "known_shards": sorted(shard_by_id)},
            )
        layer = target["layer"]
        if layer_counts[layer] + 1 > layer_specs[layer]["max_vectors"]:
            raise EngineError(
                "E_SHARD_FULL",
                "layer vector limit is reached and no further split is possible",
                "backfill_planning",
                entries=[{"text_id": entry["text_id"], "image_id": entry["image_id"]}],
                details={
                    "target_shard": target_id,
                    "layer": layer,
                    "layer_vectors": layer_counts[layer],
                    "layer_max_vectors": layer_specs[layer]["max_vectors"],
                    "fallbacks": list(target["fallbacks"]),
                },
            )

        placement = _choose_available(target, shard_by_id, layer_counts, layer_specs)
        split_created = False
        if placement is None:
            if layer_shard_counts[layer] >= layer_specs[layer]["max_shards"]:
                raise EngineError(
                    "E_SHARD_FULL",
                    "target and fallback shards are full and layer max_shards is reached",
                    "backfill_planning",
                    entries=[{"text_id": entry["text_id"], "image_id": entry["image_id"]}],
                    details={
                        "target_shard": target_id,
                        "fallbacks": list(target["fallbacks"]),
                        "layer": layer,
                        "layer_shards": layer_shard_counts[layer],
                        "layer_max_shards": layer_specs[layer]["max_shards"],
                    },
                )

            split_counter += 1
            new_id = _new_split_id(target_id, set(shard_by_id), split_counter)
            current_entries = sorted(target["entries"], key=lambda item: (item["ordinal"], item["text_id"]))
            keep_count = (len(current_entries) + 1) // 2
            moved = current_entries[keep_count:]
            target["entries"] = current_entries[:keep_count]
            child = {
                "shard_id": new_id,
                "layer": layer,
                "capacity": target["capacity"],
                "fallbacks": [],
                "config_order": len(layout["shards"]) + split_counter - 1,
                "parent_shard_id": target_id,
                "split_generation": int(target["split_generation"]) + 1,
                "entries": list(moved),
            }
            shard_by_id[new_id] = child
            layer_shard_counts[layer] += 1
            move_records = [
                {
                    "text_id": item["text_id"],
                    "image_id": item["image_id"],
                    "ordinal": item["ordinal"],
                    "from_shard": target_id,
                    "to_shard": new_id,
                    "vector_digest": item["vector_digest"],
                }
                for item in moved
            ]
            split_migrations.extend(move_records)
            split_history.append(
                {
                    "split_id": f"split-{split_counter:04d}",
                    "parent_shard_id": target_id,
                    "new_shard_id": new_id,
                    "layer": layer,
                    "reason": "target_full_no_available_fallback",
                    "moved_entries": move_records,
                }
            )
            split_created = True
            placement = target if len(target["entries"]) < target["capacity"] else child

        assigned = placement
        item = {
            "text_id": entry["text_id"],
            "image_id": entry["image_id"],
            "ordinal": entry["ordinal"],
            "requested_shard": target_id,
            "assigned_shard": assigned["shard_id"],
            "vector_digest": entry["vector_digest"],
        }
        assigned["entries"].append(item)
        layer_counts[layer] += 1
        if assigned["shard_id"] != target_id:
            routes.append(
                {
                    "text_id": entry["text_id"],
                    "image_id": entry["image_id"],
                    "from_shard": target_id,
                    "to_shard": assigned["shard_id"],
                    "kind": "split" if split_created else "fallback",
                    "vector_digest": entry["vector_digest"],
                }
            )

    for shard in shard_by_id.values():
        shard["entries"] = sorted(shard["entries"], key=lambda item: (item["ordinal"], item["text_id"]))

    output_shards: list[dict[str, Any]] = []
    for shard_id in sorted(shard_by_id):
        shard = shard_by_id[shard_id]
        shard["entries"] = sorted(shard["entries"], key=lambda item: (item["ordinal"], item["text_id"]))
        entry_ids = [entry["text_id"] for entry in shard["entries"]]
        output_shards.append(
            {
                "shard_id": shard_id,
                "layer": shard["layer"],
                "capacity": shard["capacity"],
                "entry_count": len(shard["entries"]),
                "parent_shard_id": shard["parent_shard_id"],
                "split_generation": shard["split_generation"],
                "fallbacks": shard["fallbacks"],
                "entries": shard["entries"],
                "membership_digest": _assignment_signature(shard),
                "entry_ids": entry_ids,
            }
        )

    ownership_summary = {
        shard["shard_id"]: {
            "entry_count": shard["entry_count"],
            "entry_ids": shard["entry_ids"],
            "membership_digest": shard["membership_digest"],
        }
        for shard in output_shards
    }
    layer_summary = [
        {
            "layer": spec["layer"],
            "max_vectors": spec["max_vectors"],
            "max_shards": spec["max_shards"],
            "vector_count": layer_counts[spec["layer"]],
            "shard_count": layer_shard_counts[spec["layer"]],
            "shard_ids": sorted(shard["shard_id"] for shard in output_shards if shard["layer"] == spec["layer"]),
        }
        for spec in sorted(layout["layers"], key=lambda item: item["layer"])
    ]
    dataset_digest = digest_obj(
        {
            "schema": "mmb-dataset-v1",
            "entries": [
                {
                    "text_id": entry["text_id"],
                    "image_id": entry["image_id"],
                    "target_shard": entry["target_shard"],
                    "ordinal": entry["ordinal"],
                    "vector_digest": entry["vector_digest"],
                }
                for entry in ordered
            ],
        }
    )
    return {
        "schema_version": "index-v1",
        "layout_id": layout["layout_id"],
        "config_version": version_fingerprints["layout_config_version"],
        "version_fingerprints": version_fingerprints,
        "layers": layer_summary,
        "shards": output_shards,
        "ownership_summary": ownership_summary,
        "split_history": split_history,
        "migrations": split_migrations,
        "routes": sorted(routes, key=lambda item: item["text_id"]),
        "entry_count": len(ordered),
        "dataset_digest": dataset_digest,
    }


def membership_map(index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        shard["shard_id"]: {
            "entry_ids": list(shard["entry_ids"]),
            "membership_digest": shard["membership_digest"],
            "layer": shard["layer"],
            "capacity": shard["capacity"],
            "descriptor_digest": digest_obj(
                {
                    "schema": "mmb-shard-descriptor-v1",
                    "shard_id": shard["shard_id"],
                    "layer": shard["layer"],
                    "capacity": shard["capacity"],
                    "fallbacks": shard.get("fallbacks", []),
                    "parent_shard_id": shard.get("parent_shard_id"),
                    "split_generation": shard.get("split_generation", 0),
                }
            ),
        }
        for shard in index.get("shards", [])
    }
