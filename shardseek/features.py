"""Deterministic visual descriptors and normalized text features."""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .errors import (
    E_EMPTY_CAPTION,
    E_EMPTY_IMAGE,
    E_GRID_OUT_OF_BOUNDS,
    E_NUMERIC,
    error,
)
from .util import digest_obj, sha256_file


LATIN_TOKEN_RE = re.compile(r"[a-z0-9]+")
CJK_TOKEN_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
WHITESPACE_RE = re.compile(r"\s+")


def normalize_caption(caption: str, *, item_id: str | None = None) -> str:
    if not isinstance(caption, str):
        raise error(
            E_EMPTY_CAPTION,
            "Caption must be a string",
            stage="text_feature",
            item_id=item_id,
            details={"actual_type": type(caption).__name__},
        )
    normalized = unicodedata.normalize("NFKC", caption)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip().lower()
    if not normalized:
        raise error(
            E_EMPTY_CAPTION,
            "Caption is empty after NFKC, whitespace collapse, and lowercasing",
            stage="text_feature",
            item_id=item_id,
            details={"normalization": "NFKC_whitespace_lower"},
        )
    return normalized


def _hash_bucket(value: str, dimension: int) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % dimension


def tokenize(normalized_caption: str) -> list[str]:
    latin = LATIN_TOKEN_RE.findall(normalized_caption)
    cjk = CJK_TOKEN_RE.findall(normalized_caption)
    # Sorting the two independently found streams by source position keeps the result stable.
    matches: list[tuple[int, str]] = []
    for match in LATIN_TOKEN_RE.finditer(normalized_caption):
        matches.append((match.start(), match.group(0)))
    for match in CJK_TOKEN_RE.finditer(normalized_caption):
        matches.append((match.start(), match.group(0)))
    matches.sort(key=lambda pair: (pair[0], pair[1]))
    tokens = [token for _, token in matches]
    if not tokens and not latin and not cjk:
        return []
    return tokens


def extract_text_features(caption: str, feature_config: dict[str, Any], *, item_id: str | None = None) -> dict[str, Any]:
    normalized = normalize_caption(caption, item_id=item_id)
    bow_dim = int(feature_config["text_bow_dim"])
    ngram_dim = int(feature_config["text_ngram_dim"])
    tokens = tokenize(normalized)
    bow = np.zeros(bow_dim, dtype=np.float64)
    for token in tokens:
        bow[_hash_bucket(f"w:{token}", bow_dim)] += 1.0

    compact = WHITESPACE_RE.sub("", normalized)
    ngram_vector = np.zeros(ngram_dim, dtype=np.float64)
    ngram_count = 0
    for size in feature_config["char_ngram_sizes"]:
        size = int(size)
        if size <= 0 or size > len(compact):
            continue
        for start in range(0, len(compact) - size + 1):
            ngram = compact[start : start + size]
            ngram_vector[_hash_bucket(f"c{size}:{ngram}", ngram_dim)] += 1.0
            ngram_count += 1

    vector = np.concatenate([bow, ngram_vector]).astype(np.float64)
    if vector.shape != (bow_dim + ngram_dim,):
        raise error(
            E_NUMERIC,
            "Text descriptor dimension invariant failed",
            stage="text_feature",
            item_id=item_id,
            details={"expected": bow_dim + ngram_dim, "actual": list(vector.shape)},
        )
    if not np.all(np.isfinite(vector)):
        positions = np.argwhere(~np.isfinite(vector)).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Text descriptor contains NaN or Inf",
            stage="text_feature",
            item_id=item_id,
            details={"dimension_positions": positions[:16]},
        )
    summary = {
        "normalized_caption": normalized,
        "tokens": tokens,
        "token_count": len(tokens),
        "char_ngram_count": ngram_count,
        "bow_dim": bow_dim,
        "ngram_dim": ngram_dim,
        "feature_sha256": digest_obj(vector.tolist()),
    }
    return {"vector": vector, "summary": summary}


def _load_rgb(path: str | Path, item_id: str | None) -> tuple[np.ndarray, str]:
    image_path = Path(path)
    if not image_path.is_file() or image_path.stat().st_size == 0:
        raise error(
            E_EMPTY_IMAGE,
            "Image file is missing or empty",
            stage="visual_feature",
            item_id=item_id,
            details={"image_file": image_path.name},
        )
    try:
        with Image.open(image_path) as image:
            image.load()
            rgb = image.convert("RGB")
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise error(
            E_EMPTY_IMAGE,
            "Image cannot be decoded as a non-empty RGB raster",
            stage="visual_feature",
            item_id=item_id,
            details={"reason": type(exc).__name__},
        ) from exc
    array = np.asarray(rgb, dtype=np.float64)
    if array.size == 0 or array.ndim != 3 or array.shape[2] != 3:
        raise error(
            E_EMPTY_IMAGE,
            "Decoded image has no RGB pixels",
            stage="visual_feature",
            item_id=item_id,
            details={"shape": list(array.shape)},
        )
    return array / 255.0, sha256_file(image_path)


def extract_visual_features(
    image_path: str | Path,
    feature_config: dict[str, Any],
    *,
    item_id: str | None = None,
) -> dict[str, Any]:
    image, image_sha = _load_rgb(image_path, item_id)
    height, width = image.shape[:2]
    rows = int(feature_config["grid_rows"])
    cols = int(feature_config["grid_cols"])
    bins = int(feature_config["orientation_bins"])
    if rows <= 0 or cols <= 0 or bins <= 0 or rows > height or cols > width:
        raise error(
            E_GRID_OUT_OF_BOUNDS,
            "Grid does not fit inside the image",
            stage="visual_feature",
            item_id=item_id,
            details={
                "image_height": height,
                "image_width": width,
                "grid_rows": rows,
                "grid_cols": cols,
                "orientation_bins": bins,
            },
        )

    luminance = 0.299 * image[:, :, 0] + 0.587 * image[:, :, 1] + 0.114 * image[:, :, 2]
    gradient_y, gradient_x = np.gradient(luminance)
    magnitude = np.hypot(gradient_x, gradient_y)
    orientation = np.mod(np.arctan2(gradient_y, gradient_x), math.pi)

    blocks: list[dict[str, Any]] = []
    descriptors: list[np.ndarray] = []
    for row in range(rows):
        y0 = (row * height) // rows
        y1 = ((row + 1) * height) // rows
        for col in range(cols):
            x0 = (col * width) // cols
            x1 = ((col + 1) * width) // cols
            color = image[y0:y1, x0:x1, :].reshape(-1, 3)
            means = np.mean(color, axis=0, dtype=np.float64)
            stds = np.std(color, axis=0, dtype=np.float64)
            block_magnitude = magnitude[y0:y1, x0:x1].reshape(-1)
            block_orientation = orientation[y0:y1, x0:x1].reshape(-1)
            bin_ids = np.floor(block_orientation * bins / math.pi).astype(np.int64)
            np.clip(bin_ids, 0, bins - 1, out=bin_ids)
            histogram = np.bincount(bin_ids, weights=block_magnitude, minlength=bins).astype(np.float64)
            total_magnitude = float(np.sum(histogram, dtype=np.float64))
            if total_magnitude > 0.0:
                histogram /= total_magnitude
            descriptor = np.concatenate([means, stds, histogram]).astype(np.float64)
            descriptors.append(descriptor)
            blocks.append(
                {
                    "row": row,
                    "col": col,
                    "bounds": [x0, y0, x1, y1],
                    "mean_rgb": [float(v) for v in means],
                    "std_rgb": [float(v) for v in stds],
                    "gradient_histogram": [float(v) for v in histogram],
                    "gradient_magnitude_sum": total_magnitude,
                }
            )

    vector = np.concatenate(descriptors).astype(np.float64)
    expected_dim = rows * cols * (3 + 3 + bins)
    if vector.shape != (expected_dim,):
        raise error(
            E_NUMERIC,
            "Visual descriptor dimension invariant failed",
            stage="visual_feature",
            item_id=item_id,
            details={"expected": expected_dim, "actual": list(vector.shape)},
        )
    if not np.all(np.isfinite(vector)):
        positions = np.argwhere(~np.isfinite(vector)).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Visual descriptor contains NaN or Inf",
            stage="visual_feature",
            item_id=item_id,
            details={"dimension_positions": positions[:16]},
        )
    block_summary = {
        "grid_rows": rows,
        "grid_cols": cols,
        "orientation_bins": bins,
        "image_height": height,
        "image_width": width,
        "blocks": blocks,
        "descriptor_sha256": digest_obj(vector.tolist()),
    }
    return {
        "vector": vector,
        "summary": block_summary,
        "image_sha256": image_sha,
        "summary_sha256": digest_obj(block_summary),
    }


def feature_source_fingerprint(
    *,
    feature_config_fingerprint: str,
    image_sha256: str,
    normalized_caption: str,
    source: str | None = None,
    lang: str | None = None,
) -> str:
    return digest_obj(
        {
            "feature_config": feature_config_fingerprint,
            "image_sha256": image_sha256,
            "normalized_caption": normalized_caption,
            "source": source,
            "lang": lang,
        }
    )

