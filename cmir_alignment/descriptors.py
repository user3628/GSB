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

from .canonical import digest_file, digest_obj
from .configs import FeatureConfig
from .errors import CMIRError, E_MISSING_INPUT, missing_input, numeric_error


WORD_RE = re.compile(r"[a-z0-9]+|[\u3400-\u9fff]", re.UNICODE)
COMPACT_RE = re.compile(r"[a-z0-9\u3400-\u9fff]", re.UNICODE)


def normalize_caption(text: str) -> str:
    """NFKC/full-width normalization, Latin case folding and whitespace collapse."""
    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(text)).lower()
    normalized = " ".join(normalized.split())
    return normalized


def feature_hash(kind: str, value: str, dimension: int) -> int:
    payload = f"{kind}\x00{value}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % dimension


def _compact_with_offsets(normalized: str) -> tuple[str, list[int]]:
    chars: list[str] = []
    offsets: list[int] = []
    for match in COMPACT_RE.finditer(normalized):
        chars.append(match.group(0))
        offsets.append(match.start())
    return "".join(chars), offsets


def extract_text(
    caption: str,
    config: FeatureConfig,
    *,
    item_id: str,
) -> dict[str, Any]:
    normalized = normalize_caption(caption)
    if not normalized:
        raise missing_input(item_id, "caption", "caption is empty after normalization")

    records: list[dict[str, Any]] = []
    vector = np.zeros(config.text_dim, dtype=np.float64)

    for match in WORD_RE.finditer(normalized):
        value = match.group(0)
        weight = float(config.word_weight)
        bucket = feature_hash("word", value, config.text_dim)
        records.append(
            {
                "kind": "word",
                "value": value,
                "offset_start": match.start(),
                "offset_end": match.end(),
                "weight": weight,
                "hash_bin": bucket,
            }
        )
        vector[bucket] += weight

    compact, offsets = _compact_with_offsets(normalized)
    for ngram_size in range(config.char_ngram_min, config.char_ngram_max + 1):
        if len(compact) < ngram_size:
            continue
        for start in range(0, len(compact) - ngram_size + 1):
            value = compact[start : start + ngram_size]
            weight = float(config.char_weight) / float(ngram_size)
            bucket = feature_hash(f"char:{ngram_size}", value, config.text_dim)
            records.append(
                {
                    "kind": f"char_{ngram_size}gram",
                    "value": value,
                    "offset_start": offsets[start],
                    "offset_end": offsets[start + ngram_size - 1] + 1,
                    "weight": weight,
                    "hash_bin": bucket,
                }
            )
            vector[bucket] += weight

    records.sort(
        key=lambda row: (
            row["offset_start"],
            row["offset_end"],
            row["kind"],
            row["value"],
        )
    )
    norm = float(np.linalg.norm(vector))
    if not math.isfinite(norm) or norm <= 0.0:
        raise numeric_error(
            item_id,
            "text_feature_extraction",
            "text feature vector has zero or non-finite norm",
            norm=norm,
        )
    vector /= norm
    summary = {
        "schema": "cmir.text-feature.v1",
        "item_id": item_id,
        "normalized_caption": normalized,
        "features": records,
        "vector": vector.tolist(),
        "config_digest": config.digest(),
    }
    summary_digest = digest_obj(summary)
    return {
        "normalized_caption": normalized,
        "features": records,
        "vector": vector,
        "dim": config.text_dim,
        "summary_digest": summary_digest,
    }


def _block_boundaries(size: int, parts: int) -> list[int]:
    boundaries = np.rint(np.linspace(0, size, parts + 1)).astype(int).tolist()
    for left, right in zip(boundaries, boundaries[1:]):
        if right <= left:
            raise CMIRError(
                "E_INVALID_DATASET",
                "image is smaller than the configured visual grid",
                item_id="__image__",
                field="image_size",
                image_size=size,
                grid_parts=parts,
            )
    return boundaries


def extract_visual(image_path: str | Path, config: FeatureConfig, *, item_id: str) -> dict[str, Any]:
    path = Path(image_path)
    if not path.is_file():
        raise missing_input(
            item_id,
            "image_file",
            "image file does not exist",
            image_file=str(path),
        )
    try:
        with Image.open(path) as image:
            rgb = image.convert("RGB")
            pixels = np.asarray(rgb, dtype=np.float64) / 255.0
    except (OSError, UnidentifiedImageError, ValueError) as exc:
        raise missing_input(
            item_id,
            "image_file",
            "image file cannot be decoded as RGB PNG",
            image_file=str(path),
            reason=str(exc),
        ) from exc

    height, width, _ = pixels.shape
    row_bounds = _block_boundaries(height, config.grid_rows)
    col_bounds = _block_boundaries(width, config.grid_cols)
    gray = pixels @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float64)
    grad_y, grad_x = np.gradient(gray)
    magnitude = np.hypot(grad_x, grad_y)
    orientation = np.mod(np.arctan2(grad_y, grad_x), np.pi)
    orientation_bins = np.minimum(
        (orientation / np.pi * config.grad_bins).astype(np.int64), config.grad_bins - 1
    )

    blocks: list[dict[str, Any]] = []
    vector_parts: list[float] = []
    cursor = 0
    for row in range(config.grid_rows):
        y0, y1 = row_bounds[row], row_bounds[row + 1]
        for col in range(config.grid_cols):
            x0, x1 = col_bounds[col], col_bounds[col + 1]
            block = pixels[y0:y1, x0:x1, :]
            block_magnitude = magnitude[y0:y1, x0:x1]
            block_bins = orientation_bins[y0:y1, x0:x1]
            means = block.mean(axis=(0, 1))
            stds = block.std(axis=(0, 1))
            histogram = np.zeros(config.grad_bins, dtype=np.float64)
            flat_bins = block_bins.reshape(-1)
            flat_magnitude = block_magnitude.reshape(-1)
            for bin_index in range(config.grad_bins):
                histogram[bin_index] = float(
                    flat_magnitude[flat_bins == bin_index].sum()
                )
            total = float(histogram.sum())
            if total > 0.0:
                histogram /= total
            block_vector = np.concatenate((means, stds, histogram))
            index_start = cursor
            index_end = cursor + int(block_vector.size)
            cursor = index_end
            vector_parts.extend(float(value) for value in block_vector.tolist())
            blocks.append(
                {
                    "row": row,
                    "col": col,
                    "pixel_bounds": [x0, y0, x1, y1],
                    "descriptor_slice": [index_start, index_end],
                    "mean": means.tolist(),
                    "std": stds.tolist(),
                    "gradient_hist": histogram.tolist(),
                }
            )

    vector = np.asarray(vector_parts, dtype=np.float64)
    if vector.shape != (config.visual_dim,):
        raise numeric_error(
            item_id,
            "visual_feature_extraction",
            "visual descriptor dimension does not match configuration",
            expected_dim=config.visual_dim,
            actual_dim=int(vector.size),
        )
    if not np.all(np.isfinite(vector)):
        raise numeric_error(
            item_id,
            "visual_feature_extraction",
            "visual descriptor contains NaN or Inf",
        )
    normalized_vector = vector / max(float(np.linalg.norm(vector)), 1e-12)
    image_digest = digest_file(path)
    summary = {
        "schema": "cmir.visual-feature.v1",
        "item_id": item_id,
        "image_digest": image_digest,
        "image_size": [width, height],
        "blocks": blocks,
        "vector": normalized_vector.tolist(),
        "config_digest": config.digest(),
    }
    return {
        "image_digest": image_digest,
        "image_size": [width, height],
        "blocks": blocks,
        "vector": normalized_vector,
        "dim": config.visual_dim,
        "summary_digest": digest_obj(summary),
    }
