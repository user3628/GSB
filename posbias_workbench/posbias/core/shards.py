"""Deterministic local shard generation and on-disk storage."""

from __future__ import annotations

import hashlib
import json
import os

import numpy as np

from ..errors import NumericError
from .config import canonical_json


def shard_rng(seed: int, shard_index: int) -> np.random.Generator:
    # Stable independent stream per shard index.
    seq = np.random.SeedSequence([int(seed), int(shard_index), 0x510C])
    return np.random.default_rng(seq)


def generate_shard(*, seed, shard_index, batch, heads, seq_len, head_dim,
                   inject_nan=False):
    rng = shard_rng(seed, shard_index)
    shape = (batch, heads, seq_len, head_dim)
    query = rng.normal(loc=0.0, scale=1.0, size=shape).astype(np.float64)
    key = rng.normal(loc=0.0, scale=1.0, size=shape).astype(np.float64)
    if inject_nan:
        query.flat[0] = np.nan
    return {"query": query, "key": key}


def write_shard(directory, *, seed, shard_index, batch, heads, seq_len,
                head_dim, inject_nan=False):
    payload = generate_shard(
        seed=seed, shard_index=shard_index, batch=batch, heads=heads,
        seq_len=seq_len, head_dim=head_dim, inject_nan=inject_nan)
    os.makedirs(directory, exist_ok=True)
    base = os.path.join(directory, f"shard_{shard_index:02d}")
    np.save(base + ".q.npy", payload["query"])
    np.save(base + ".k.npy", payload["key"])
    meta = {
        "shard_index": shard_index,
        "seed": seed,
        "batch": batch,
        "heads": heads,
        "seq_len": seq_len,
        "head_dim": head_dim,
        "shape": list(payload["query"].shape),
        "inject_nan": inject_nan,
        "q_file": f"shard_{shard_index:02d}.q.npy",
        "k_file": f"shard_{shard_index:02d}.k.npy",
        "sha256": {
            "q": sha256_file(base + ".q.npy"),
            "k": sha256_file(base + ".k.npy"),
        },
    }
    with open(base + ".meta.json", "w", encoding="utf-8") as handle:
        handle.write(canonical_json(meta))
    return meta


def load_shard(directory, shard_index):
    base = os.path.join(directory, f"shard_{shard_index:02d}")
    query = np.load(base + ".q.npy", mmap_mode="r")
    key = np.load(base + ".k.npy", mmap_mode="r")
    with open(base + ".meta.json", "r", encoding="utf-8") as handle:
        meta = json.load(handle)
    if not np.all(np.isfinite(query)) or not np.all(np.isfinite(key)):
        bad = int(query.size - np.count_nonzero(np.isfinite(query)) +
                  key.size - np.count_nonzero(np.isfinite(key)))
        raise NumericError(
            "shard contains NaN/Inf values", stage="bias",
            scheme=None, context={"shard_index": shard_index,
                                  "non_finite": bad})
    return {"query": np.asarray(query, dtype=np.float64),
            "key": np.asarray(key, dtype=np.float64), "meta": meta}


def list_shard_metas(directory):
    metas = []
    for name in sorted(os.listdir(directory)):
        if name.endswith(".meta.json"):
            with open(os.path.join(directory, name), "r",
                      encoding="utf-8") as handle:
                metas.append(json.load(handle))
    return metas


def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()
