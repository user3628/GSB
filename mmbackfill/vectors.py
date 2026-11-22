from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import Counter
from typing import Any

import numpy as np

from .canonical import digest_obj
from .errors import EngineError
from .layout import visual_dimension
from .png import PNGError, decode_rgb_png_bytes


TOKEN_RE = re.compile(r"[a-z0-9_]+|[\u3400-\u4dbf\u4e00-\u9fff]+|[^\s]")
QUANTIZE_DIGITS = 12


def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        raise EngineError("E_INVALID_INPUT", "text must be a string", "vector_build")
    folded = unicodedata.normalize("NFKC", text)
    folded = " ".join(folded.split())
    return folded.lower()


def _tokenize(normalized: str) -> list[str]:
    tokens: list[str] = []
    for match in TOKEN_RE.finditer(normalized):
        value = match.group(0)
        if any("\u3400" <= char <= "\u4dbf" or "\u4e00" <= char <= "\u9fff" for char in value):
            tokens.extend(list(value))
        else:
            tokens.append(value)
    return tokens


def _unit(vector: np.ndarray) -> np.ndarray | None:
    norm = float(np.linalg.norm(vector))
    if not math.isfinite(norm) or norm <= 1e-15:
        return None
    return vector / norm


def visual_descriptor(pixels: np.ndarray, visual: dict[str, int]) -> np.ndarray:
    rows = int(visual["grid_rows"])
    cols = int(visual["grid_cols"])
    edge_bins = int(visual["edge_bins"])
    height, width, channels = pixels.shape
    if channels != 3 or height < rows or width < cols:
        raise EngineError(
            "E_EMPTY_INPUT",
            "image is too small or not RGB for the declared visual grid",
            "vector_build",
            details={"height": height, "width": width, "grid_rows": rows, "grid_cols": cols},
        )
    image = pixels.astype(np.float64) / 255.0
    row_parts = np.array_split(np.arange(height), rows)
    col_parts = np.array_split(np.arange(width), cols)
    features: list[float] = []
    for row_idx in row_parts:
        for col_idx in col_parts:
            block = image[row_idx[:, None], col_idx[None, :], :]
            flat = block.reshape(-1, 3)
            for channel in range(3):
                values = flat[:, channel]
                mean = float(values.mean())
                std = float(values.std())
                skew = 0.0
                if std > 1e-12:
                    skew = float(np.mean(((values - mean) / std) ** 3))
                    if not math.isfinite(skew):
                        skew = 0.0
                features.extend((mean, std, skew))

    luma = image[:, :, 0] * 0.299 + image[:, :, 1] * 0.587 + image[:, :, 2] * 0.114
    gx = np.zeros_like(luma)
    gy = np.zeros_like(luma)
    if width >= 3:
        gx[:, 1:-1] = luma[:, 2:] - luma[:, :-2]
    if height >= 3:
        gy[1:-1, :] = luma[2:, :] - luma[:-2, :]
    magnitude = np.hypot(gx, gy)
    angle = np.arctan2(gy, gx)
    bin_index = np.floor((angle + np.pi) * edge_bins / (2.0 * np.pi)).astype(np.int64)
    np.clip(bin_index, 0, edge_bins - 1, out=bin_index)
    hist = np.bincount(bin_index.ravel(), weights=magnitude.ravel(), minlength=edge_bins)
    total = float(hist.sum())
    if total > 1e-15:
        hist = hist / total
    features.extend(float(item) for item in hist)
    vector = np.asarray(features, dtype=np.float64)
    if vector.size != visual_dimension(rows, cols, edge_bins):
        raise EngineError("E_DIM_MISMATCH", "visual descriptor dimension mismatch", "vector_build")
    unit = _unit(vector)
    if unit is None:
        return vector
    return unit


def text_descriptor(normalized: str, dimension: int) -> np.ndarray:
    tokens = _tokenize(normalized)
    if not tokens:
        return np.zeros(dimension, dtype=np.float64)
    vector = np.zeros(dimension, dtype=np.float64)
    counts = Counter(tokens)
    for token in sorted(counts):
        count = counts[token]
        raw = b"mmb-text-token-v1\0" + token.encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        idx_a = int.from_bytes(digest[0:4], "big") % dimension
        idx_b = int.from_bytes(digest[4:8], "big") % dimension
        sign_a = 1.0 if digest[8] & 1 else -1.0
        sign_b = 1.0 if digest[9] & 1 else -1.0
        weight = 1.0 + math.log1p(count)
        vector[idx_a] += sign_a * weight
        vector[idx_b] += sign_b * weight
    return vector


def quantize_vector(vector: np.ndarray) -> np.ndarray:
    unit = _unit(np.asarray(vector, dtype=np.float64))
    if unit is None:
        raise EngineError("E_EMPTY_INPUT", "vector has zero norm", "vector_build")
    result = np.round(unit, QUANTIZE_DIGITS)
    result_unit = _unit(result)
    if result_unit is None:
        raise EngineError("E_EMPTY_INPUT", "quantized vector has zero norm", "vector_build")
    return np.round(result_unit, QUANTIZE_DIGITS)


def vector_digest(vector: np.ndarray | list[float]) -> str:
    values = np.asarray(vector, dtype=np.float64).tolist()
    return digest_obj({"schema": "mmb-unit-vector-v1", "values": values})


def vector_source_key(
    *,
    layout: dict[str, Any],
    text_id: str,
    image_id: str,
    raw_text: str,
    normalized_text: str,
    image_sha256: str,
    image_language: str = "und",
    override: Any = None,
) -> str:
    payload: dict[str, Any] = {
        "schema": "mmb-vector-source-v1",
        "vector_config_fingerprint": digest_obj({"schema": "mmb-vector-config-v1", "vector": layout["vector"]}),
        "text_id": text_id,
        "image_id": image_id,
        "raw_text": raw_text,
        "image_language": image_language,
        "normalized_text": normalized_text,
        "image_sha256": image_sha256,
    }
    if override is not None:
        payload["override"] = override
    return digest_obj(payload)


def validate_override(raw: Any, layout: dict[str, Any], *, text_id: str, image_id: str) -> np.ndarray:
    dimension = int(layout["vector"]["dimension"])
    try:
        vector = np.asarray(raw, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise EngineError(
            "E_DIM_MISMATCH",
            "injected vector is not a numeric array",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
            details={"expected_dimension": dimension},
        ) from exc
    if vector.ndim != 1 or vector.size != dimension:
        raise EngineError(
            "E_DIM_MISMATCH",
            "injected vector dimension does not match layout",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
            details={"expected_dimension": dimension, "actual_dimension": int(vector.size)},
        )
    if not bool(np.all(np.isfinite(vector))):
        raise EngineError(
            "E_NUMERIC",
            "injected vector contains NaN or Inf",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
            details={"expected_dimension": dimension},
        )
    try:
        return quantize_vector(vector)
    except EngineError as exc:
        exc.entries = [{"text_id": text_id, "image_id": image_id}]
        raise


def build_retrieval_vector(
    *,
    text_id: str,
    image_id: str,
    image_bytes: bytes | None,
    text: str,
    layout: dict[str, Any],
    image_language: str = "und",
    override: Any = None,
) -> tuple[np.ndarray, str, str, str]:
    """Return (vector, digest, normalized_text, vector_source_key)."""
    vector_spec = layout["vector"]
    dimension = int(vector_spec["dimension"])
    image_weight = float(vector_spec["fusion"]["image_weight"])
    text_weight = float(vector_spec["fusion"]["text_weight"])
    if not math.isfinite(image_weight) or not math.isfinite(text_weight):
        raise EngineError(
            "E_NUMERIC",
            "fusion weights contain NaN or Inf",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
        )
    if image_bytes is None:
        raise EngineError(
            "E_EMPTY_INPUT",
            "PNG file is missing or unreadable",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
        )
    try:
        pixels = decode_rgb_png_bytes(image_bytes)
    except PNGError as exc:
        raise EngineError(
            "E_EMPTY_INPUT",
            "PNG file is missing or unreadable",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
            details={"reason": str(exc)},
        ) from exc
    normalized = normalize_text(text)
    if not normalized:
        raise EngineError(
            "E_EMPTY_INPUT",
            "normalized text is empty",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
        )
    try:
        visual = visual_descriptor(pixels, vector_spec["visual"])
    except EngineError as exc:
        exc.entries = [{"text_id": text_id, "image_id": image_id}]
        raise
    textual = text_descriptor(normalized, int(vector_spec["text"]["dimension"]))
    text_unit = _unit(textual)
    if text_unit is None:
        raise EngineError(
            "E_EMPTY_INPUT",
            "text hash vector is empty or cancelled to zero",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
        )
    visual_unit = _unit(visual)
    if visual_unit is None:
        visual_unit = np.zeros_like(visual)
    fused = np.concatenate((image_weight * visual_unit, text_weight * text_unit))
    if fused.size != dimension:
        raise EngineError(
            "E_DIM_MISMATCH",
            "fused vector dimension does not match layout",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
            details={"expected_dimension": dimension, "actual_dimension": int(fused.size)},
        )
    if not bool(np.all(np.isfinite(fused))):
        raise EngineError(
            "E_NUMERIC",
            "fused vector contains NaN or Inf",
            "vector_build",
            entries=[{"text_id": text_id, "image_id": image_id}],
        )
    if override is not None:
        vector = validate_override(override, layout, text_id=text_id, image_id=image_id)
    else:
        try:
            vector = quantize_vector(fused)
        except EngineError as exc:
            exc.entries = [{"text_id": text_id, "image_id": image_id}]
            raise
    image_sha = hashlib.sha256(image_bytes).hexdigest()
    source_key = vector_source_key(
        layout=layout,
        text_id=text_id,
        image_id=image_id,
        raw_text=text,
        normalized_text=normalized,
        image_sha256=image_sha,
        image_language=image_language,
        override=override,
    )
    return vector, vector_digest(vector), normalized, source_key
