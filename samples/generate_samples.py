from __future__ import annotations

import json
import random
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from joincost.formats.binary import decode_binary, encode_binary
from joincost.formats.jsonl import decode_jsonl, encode_jsonl
from joincost.model import finalize_snapshot, normalize_relation

SEED = 20261123
VERSION_V2 = 1001
VERSION_V1 = 1000
VERSION_REPLACEMENT = 1002
VERSION_V3 = 1003


RELATION_SPECS = [
    ("customers", 60_000, 750, 0.90, ["legacy_segment"]),
    ("orders", 1_200_000, 250, 0.72, []),
    ("order_items", 4_800_000, 750, 0.86, []),
    ("products", 80_000, 625, 0.61, []),
    ("shipments", 600_000, 500, 0.55, []),
    ("returns", 120_000, 400, 0.48, []),
]


PREDICATES = [
    {
        "id": "p_customer_orders",
        "kind": "equality",
        "relation_a": "customers",
        "column_a": "id",
        "relation_b": "orders",
        "column_b": "customer_id",
        "correlation_coefficient": 0.88,
    },
    {
        "id": "p_orders_items",
        "kind": "equality",
        "relation_a": "orders",
        "column_a": "id",
        "relation_b": "order_items",
        "column_b": "order_id",
    },
    {
        "id": "p_products_items",
        "kind": "equality",
        "relation_a": "products",
        "column_a": "id",
        "relation_b": "order_items",
        "column_b": "product_id",
        "correlation_coefficient": 0.70,
    },
    {
        "id": "p_orders_shipments",
        "kind": "equality",
        "relation_a": "orders",
        "column_a": "id",
        "relation_b": "shipments",
        "column_b": "order_id",
    },
    {
        "id": "p_items_returns",
        "kind": "equality",
        "relation_a": "order_items",
        "column_a": "id",
        "relation_b": "returns",
        "column_b": "order_item_id",
        "correlation_coefficient": 0.65,
    },
    {
        "id": "p_orders_recent_range",
        "kind": "range",
        "relation_a": "orders",
        "column_a": "created_at",
        "range_fraction_permille": 185,
    },
    {
        "id": "p_shipment_return_window",
        "kind": "range",
        "relation_a": "shipments",
        "column_a": "delivered_at",
        "relation_b": "returns",
        "column_b": "returned_at",
        "range_fraction_permille": 120,
    },
    {
        "id": "p_item_parent_self",
        "kind": "self_join",
        "relation_a": "order_items",
        "column_a": "parent_item_id",
        "column_b": "id",
        "correlation_coefficient": 0.42,
    },
]


def build_relation_set(version: int):
    rng = random.Random(SEED + version)
    relations = []
    for name, rows, permille, correlation, stale_keys in RELATION_SPECS:
        jitter = rng.randint(-2, 2)
        ratio = max(1, min(1000, permille + jitter)) / 1000
        relations.append(
            normalize_relation(
                name=name,
                row_count=rows,
                ndv_ratio_value=ratio,
                correlation_value=correlation,
                stale_keys=stale_keys,
                source_kind="permille_integer",
                source_precision_places=3,
            )
        )
    return relations


def build_replacement_relations():
    relations = build_relation_set(VERSION_REPLACEMENT)
    changed = []
    for relation in relations:
        multiplier = 1.0
        if relation.name == "orders":
            multiplier = 1.25
        elif relation.name == "order_items":
            multiplier = 1.0833333333
        elif relation.name == "shipments":
            multiplier = 0.85
        changed.append(
            normalize_relation(
                name=relation.name,
                row_count=int(relation.row_count * multiplier),
                ndv_ratio_value=relation.ndv_ratio,
                correlation_value=relation.correlation_coefficient,
                stale_keys=relation.stale_keys,
                source_kind="permille_integer",
                source_precision_places=3,
            )
        )
    return changed


def unsupported_v3_jsonl() -> bytes:
    record = {
        "type": "relation",
        "relation": "future_table",
        "row_count": 42,
        "ndv_permille": 500,
        "correlation_coefficient": 0.5,
        "stale_keys": [],
    }
    record_line = json.dumps(record, sort_keys=True, separators=(",", ":"))
    records_blob = (record_line + "\n").encode("utf-8")
    manifest = {
        "type": "manifest",
        "schema": "jcs.stats.jsonl",
        "format_version": 3,
        "snapshot_version": VERSION_V3,
        "record_count": 1,
        "records_crc32": f"{zlib.crc32(records_blob) & 0xFFFFFFFF:08x}",
        "required_features": ["future_required_field"],
    }
    manifest_line = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return (manifest_line + "\n").encode("utf-8") + records_blob


def main() -> int:
    here = Path(__file__).resolve().parent
    v2_relations = build_relation_set(VERSION_V2)
    base = finalize_snapshot(
        version=VERSION_V2,
        relations=v2_relations,
        format_name="binary",
        format_version=2,
        decision="ACCEPT",
        degradations=[],
    )
    binary_bytes = encode_binary(base, endian="little", format_version=2)
    decoded_binary = decode_binary(binary_bytes)
    jsonl_bytes = encode_jsonl(decoded_binary.snapshot, format_version=2)
    decoded_jsonl = decode_jsonl(jsonl_bytes)
    if decoded_binary.snapshot.digest != decoded_jsonl.snapshot.digest:
        raise RuntimeError("v2 binary and JSONL samples are not semantically equivalent")

    v1_base = finalize_snapshot(
        version=VERSION_V1,
        relations=build_relation_set(VERSION_V1),
        format_name="binary",
        format_version=1,
        decision="DEGRADE_ACCEPT",
        degradations=[],
    )
    v1_binary = encode_binary(v1_base, endian="big", format_version=1)
    v1_binary_decoded = decode_binary(v1_binary)
    v1_jsonl = encode_jsonl(v1_binary_decoded.snapshot, format_version=1)
    v1_jsonl_decoded = decode_jsonl(v1_jsonl)

    replacement_base = finalize_snapshot(
        version=VERSION_REPLACEMENT,
        relations=build_replacement_relations(),
        format_name="binary",
        format_version=2,
        decision="ACCEPT",
        degradations=[],
    )
    replacement_binary = encode_binary(replacement_base, endian="little", format_version=2)
    replacement_decoded = decode_binary(replacement_binary)
    replacement_jsonl = encode_jsonl(replacement_decoded.snapshot, format_version=2)
    replacement_jsonl_decoded = decode_jsonl(replacement_jsonl)
    if replacement_decoded.snapshot.digest != replacement_jsonl_decoded.snapshot.digest:
        raise RuntimeError("replacement binary and JSONL samples are not semantically equivalent")

    files = {
        "stats_v2.bin": binary_bytes,
        "stats_v2.jsonl": jsonl_bytes,
        "stats_v1_legacy.bin": v1_binary,
        "stats_v1_legacy.jsonl": v1_jsonl,
        "stats_v3_unsupported.jsonl": unsupported_v3_jsonl(),
        "stats_replacement.bin": replacement_binary,
        "stats_replacement.jsonl": replacement_jsonl,
    }
    for name, content in files.items():
        (here / name).write_bytes(content)
    (here / "predicates.json").write_text(
        json.dumps(PREDICATES, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    manifest = {
        "seed": SEED,
        "files": {
            "binary_v2": {
                "file": "stats_v2.bin",
                "snapshot_version": VERSION_V2,
                "digest": decoded_binary.snapshot.digest,
                "decision": decoded_binary.report["decision"],
                "record_count": len(decoded_binary.snapshot.relations),
            },
            "jsonl_v2": {
                "file": "stats_v2.jsonl",
                "snapshot_version": VERSION_V2,
                "digest": decoded_jsonl.snapshot.digest,
                "decision": decoded_jsonl.report["decision"],
                "record_count": len(decoded_jsonl.snapshot.relations),
            },
            "binary_v1_legacy": {
                "file": "stats_v1_legacy.bin",
                "snapshot_version": VERSION_V1,
                "digest": v1_binary_decoded.snapshot.digest,
                "decision": v1_binary_decoded.report["decision"],
                "degradations": len(v1_binary_decoded.report["degradations"]),
            },
            "jsonl_v1_legacy": {
                "file": "stats_v1_legacy.jsonl",
                "snapshot_version": VERSION_V1,
                "digest": v1_jsonl_decoded.snapshot.digest,
                "decision": v1_jsonl_decoded.report["decision"],
                "degradations": len(v1_jsonl_decoded.report["degradations"]),
            },
            "jsonl_v3_unsupported": {
                "file": "stats_v3_unsupported.jsonl",
                "snapshot_version": VERSION_V3,
                "expected_code": "E_FORMAT_UNSUPPORTED",
            },
            "binary_replacement": {
                "file": "stats_replacement.bin",
                "snapshot_version": VERSION_REPLACEMENT,
                "digest": replacement_decoded.snapshot.digest,
                "decision": replacement_decoded.report["decision"],
            },
            "jsonl_replacement": {
                "file": "stats_replacement.jsonl",
                "snapshot_version": VERSION_REPLACEMENT,
                "digest": replacement_jsonl_decoded.snapshot.digest,
                "decision": replacement_jsonl_decoded.report["decision"],
            },
        },
        "interoperable_v2": decoded_binary.snapshot.digest
        == decoded_jsonl.snapshot.digest,
    }
    (here / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

