"""Stage 1: image decoding, dimension validation and fixed normalization."""

import numpy as np
from PIL import Image

from .errors import BusinessError, E_DIM_MISMATCH, E_NUMERIC
from .hashing import round_floats, file_digest, digest, canonical

STAGE = "stage1_normalize"


def decode_png(path):
    """Decode a PNG into an HxWx3 uint8 RGB array."""
    with Image.open(path) as image:
        rgb = image.convert("RGB")
        return np.asarray(rgb, dtype=np.uint8).copy()


def normalize(array_uint8):
    """Fixed normalization rule: pixels / 255.0 as float64."""
    return array_uint8.astype(np.float64) / 255.0


def _summary(matrix):
    flat = matrix.reshape(-1, 3)
    return {
        "channel_mean": [float(value) for value in flat.mean(axis=0)],
        "channel_variance": [float(value) for value in flat.var(axis=0)],
        "pixel_mean": float(flat.mean()),
        "pixel_variance": float(flat.var()),
    }


def run(image_path, metadata, injection=None):
    """Execute stage 1 for one image.

    Returns (record, normalized_float_array). The record is the serializable
    stage product; the float array stays in memory for downstream stages.
    """
    image_id = metadata["image_id"]
    raw_digest = file_digest(image_path)
    declared_width = int(metadata["declared_width"])
    declared_height = int(metadata["declared_height"])

    raw = decode_png(image_path)
    actual_height, actual_width = raw.shape[0], raw.shape[1]

    if injection is not None and injection.kind == "dimension_mismatch" and injection.targets(image_id):
        declared_width = int(round(declared_width + abs(injection.value) + 1))

    if (declared_width, declared_height) != (actual_width, actual_height):
        raise BusinessError(
            E_DIM_MISMATCH,
            f"declared dimensions do not match decoded pixels for {image_id}",
            {
                "image_id": image_id,
                "declared": {"width": declared_width, "height": declared_height},
                "actual": {"width": actual_width, "height": actual_height},
            },
        )

    pixels = normalize(raw)

    if injection is not None and injection.kind == "nan_pixels" and injection.targets(image_id):
        pixels = pixels.copy()
        pixels[0, 0, 0] = np.nan

    if pixels.size == 0 or actual_width == 0 or actual_height == 0:
        raise BusinessError(
            E_NUMERIC,
            f"empty image cannot be normalized for {image_id}",
            {"image_id": image_id, "dimension": "all"},
        )
    if not np.isfinite(pixels).all():
        bad = int(np.count_nonzero(~np.isfinite(pixels)))
        raise BusinessError(
            E_NUMERIC,
            f"non-finite pixels detected for {image_id}",
            {"image_id": image_id, "dimension": "pixels", "bad_pixels": bad},
        )

    is_black = bool(np.all(pixels <= 0.0))
    record = {
        "stage": STAGE,
        "image_id": image_id,
        "input_fingerprint": digest(
            {"raw_sha256": raw_digest, "declared": [declared_width, declared_height]}
        ),
        "product": {
            "width": actual_width,
            "height": actual_height,
            "channels": 3,
            "valid_pixels": int(actual_width * actual_height),
            "is_empty": False,
            "is_all_black": is_black,
            "normalization": "float64 divide by 255",
            "summary": _summary(pixels),
            "raw_sha256": raw_digest,
        },
    }
    record["product"] = round_floats(record["product"])
    record["product_digest"] = digest(record["product"])
    record["fingerprint"] = digest(
        {"input": record["input_fingerprint"], "product": record["product_digest"]}
    )
    return record, pixels


def canonical_summary(record):
    """Byte-stable rendering of a stage-1 summary."""
    return canonical(record["product"]["summary"])
