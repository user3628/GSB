"""Deterministic visual descriptor: per-block color moments + edge orientation
histograms, concatenated in row-major block order into a fixed-dim vector."""

import hashlib

import numpy as np
from PIL import Image

from .errors import CmrError, E_MISSING_FRAGMENT, E_QUERY_INVALID


def load_rgb(path, stage="features"):
    if not path or not __import__("os").path.exists(path):
        raise CmrError(E_MISSING_FRAGMENT, f"image fragment not found: {path}", stage=stage,
                       detail={"image_file": path})
    with Image.open(path) as im:
        arr = np.asarray(im.convert("RGB"), dtype=np.float64) / 255.0
    return arr


def _block_edges(n, parts):
    """Deterministic integer split of [0,n) into `parts` contiguous ranges."""
    edges = np.linspace(0, n, parts + 1).astype(np.int64)
    return [(int(edges[i]), int(edges[i + 1])) for i in range(parts)]


def block_descriptors(img, grid, orient_bins):
    """Return (grid*grid, 3+3+orient_bins) float64 array of per-block descriptors."""
    h, w, _ = img.shape
    rows = _block_edges(h, grid)
    cols = _block_edges(w, grid)
    blocks = []
    for r0, r1 in rows:
        for c0, c1 in cols:
            block = img[r0:r1, c0:c1, :]
            mean = block.mean(axis=(0, 1))
            std = block.std(axis=(0, 1))
            gray = block.mean(axis=2)
            gy = np.gradient(gray, axis=0)
            gx = np.gradient(gray, axis=1)
            mag = np.hypot(gx, gy)
            ang = np.arctan2(gy, gx) % np.pi
            bins = np.minimum((ang / np.pi * orient_bins).astype(np.int64), orient_bins - 1)
            hist = np.zeros(orient_bins, dtype=np.float64)
            np.add.at(hist, bins.ravel(), mag.ravel())
            hist /= float(block.shape[0] * block.shape[1])
            blocks.append(np.concatenate([mean, std, hist]))
    return np.stack(blocks, axis=0)


def extract_image_features(path, cfg, stage="features"):
    """Full visual feature vector of dimension cfg.image_dim."""
    img = load_rgb(path, stage=stage)
    blocks = block_descriptors(img, cfg.grid, cfg.orient_bins)
    feat = blocks.reshape(-1)
    assert feat.shape[0] == cfg.image_dim
    return feat


def block_hashes(img, grid, orient_bins):
    """Quantized per-block descriptor hashes -> deterministic visual terms."""
    blocks = block_descriptors(img, grid, orient_bins)
    terms = []
    for blk in blocks:
        quant = np.round(blk, 3)
        key = ",".join(format(float(x), ".3f") for x in quant)
        terms.append("v:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16])
    return terms


def validate_image_vector(vec, cfg, stage="query"):
    arr = np.asarray(vec, dtype=np.float64).reshape(-1)
    if arr.shape[0] != cfg.image_dim or not np.all(np.isfinite(arr)):
        raise CmrError(
            E_QUERY_INVALID,
            f"image feature vector dimension invalid: got {arr.shape[0]}, expect {cfg.image_dim}",
            stage=stage,
            detail={"got_dim": int(arr.shape[0]), "expect_dim": cfg.image_dim},
        )
    return arr
