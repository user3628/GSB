#!/usr/bin/env python3
"""Deterministic local sample generator (fixed seed, no external services).

Produces:
    <out>/shards/text-shard-00{1,2,3}.npz        ids, 64-d vectors, languages
    <out>/shards/vision-shard-00{1,2,3}.npy+.json  ids, 96-d vectors, text ids,
                                                 operator labels (one missing)
    <out>/manifest_v1.yaml                       structure version 1 manifest
    <out>/manifest_v2.yaml                       structure version 2 manifest
    <out>/inject/vision-shard-900-baddim.npy+.json  dim mismatch anomaly

Deleting <out> and re-running the generator rebuilds every input
byte-identically.
"""

import argparse
import json
import os

import numpy as np
import yaml

from align_engine.manifest import (deterministic_projection_matrix,
                                   dump_manifest)

SEED = 20261121
TEXT_DIM = 64
VISION_DIM = 96
SCALE_PERCENT = 75

TEXT_SHARDS = [
    ("text-shard-001", ["txt-0001", "txt-0002", "txt-0003", "txt-0004"],
     ["zh", "en", "zh", "en"]),
    ("text-shard-002", ["txt-0005", "txt-0006", "txt-0007", "txt-0008"],
     ["en", "zh", "mixed", "zh"]),
    ("text-shard-003", ["txt-0009", "txt-0010", "txt-0011"],
     ["zh", "en", "zh"]),
]

VISION_SHARDS = [
    ("vision-shard-001",
     ["vis-0001", "vis-0002", "vis-0003", "vis-0004"],
     ["txt-0001", "txt-0002", "txt-0003", "txt-0004"],
     ["op-encoder-a", "op-encoder-b", "op-encoder-a", "op-encoder-b"]),
    ("vision-shard-002",
     ["vis-0005", "vis-0006", "vis-0007", "vis-0008"],
     ["txt-0005", "txt-0006", "txt-0007", "txt-0008"],
     ["op-encoder-b", None, "op-encoder-a", "op-encoder-b"]),
    ("vision-shard-003",
     ["vis-0009", "vis-0010"],
     ["txt-0009", "txt-0010"],
     ["op-encoder-a", "op-encoder-b"]),
]


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(obj, ensure_ascii=False, indent=2,
                                sort_keys=True))
        handle.write("\n")


def generate(out_dir):
    rng = np.random.default_rng(SEED)
    shards_dir = os.path.join(out_dir, "shards")
    inject_dir = os.path.join(out_dir, "inject")
    os.makedirs(shards_dir, exist_ok=True)
    os.makedirs(inject_dir, exist_ok=True)

    for shard_id, ids, languages in TEXT_SHARDS:
        vectors = rng.standard_normal((len(ids), TEXT_DIM))
        np.savez(os.path.join(shards_dir, shard_id + ".npz"),
                 ids=np.asarray(ids), vectors=vectors,
                 languages=np.asarray(languages))

    for shard_id, ids, text_ids, operators in VISION_SHARDS:
        vectors = rng.standard_normal((len(ids), VISION_DIM))
        np.save(os.path.join(shards_dir, shard_id + ".npy"), vectors)
        _write_json(os.path.join(shards_dir, shard_id + ".json"), {
            "shard_id": shard_id,
            "modality": "vision",
            "dim": VISION_DIM,
            "ids": ids,
            "text_ids": text_ids,
            "operators": operators,
        })

    # Injected anomaly: sidecar declares 96 dims, matrix carries 64.
    bad_vectors = rng.standard_normal((2, TEXT_DIM))
    np.save(os.path.join(inject_dir, "vision-shard-900-baddim.npy"),
            bad_vectors)
    _write_json(os.path.join(inject_dir, "vision-shard-900-baddim.json"), {
        "shard_id": "vision-shard-900-baddim",
        "modality": "vision",
        "dim": VISION_DIM,
        "ids": ["vis-9001", "vis-9002"],
        "text_ids": ["txt-0001", "txt-0002"],
        "operators": ["op-encoder-a", "op-encoder-b"],
    })

    shard_refs = {
        "text": [sid for sid, _ids, _langs in TEXT_SHARDS],
        "vision": [sid for sid, _ids, _tids, _ops in VISION_SHARDS],
    }
    matrix = deterministic_projection_matrix(TEXT_DIM, VISION_DIM)
    manifest_v2 = {
        "manifest_version": 2,
        "project_id": "align-demo",
        "modalities": {"text": {"dim": TEXT_DIM},
                       "vision": {"dim": VISION_DIM}},
        "projection": {"scale": SCALE_PERCENT / 100.0,
                       "matrix": matrix.tolist()},
        "shards": shard_refs,
    }
    manifest_v1 = {
        "manifest_version": 1,
        "project_id": "align-demo",
        "modalities": {"text": {"dim": TEXT_DIM},
                       "vision": {"dim": 64}},
        "actual_vision_dim": VISION_DIM,
        "projection": {"scale_percent": SCALE_PERCENT},
        "shards": shard_refs,
    }
    for name, doc in (("manifest_v1.yaml", manifest_v1),
                      ("manifest_v2.yaml", manifest_v2)):
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fh:
            fh.write(dump_manifest(doc))
    return {"out_dir": out_dir, "seed": SEED,
            "text_shards": len(TEXT_SHARDS),
            "vision_shards": len(VISION_SHARDS)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="samples",
                        help="output directory (default: samples)")
    args = parser.parse_args()
    info = generate(args.out)
    print(json.dumps(info, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
