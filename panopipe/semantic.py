"""Semantic image decoding and instance-to-panoptic merging."""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .errors import E_INVALID_CONFIG, E_UNKNOWN_COLOR, PipelineError
from .fingerprint import digest_bytes
from .manifest import ShardSpec
from .model import (
    BACKGROUND_CATEGORY_ID,
    BACKGROUND_CATEGORY_NAME,
    PanopticModel,
    SegmentInfo,
    derive_segment_attributes,
)


@dataclass(frozen=True)
class DecodeResult:
    shard_id: str
    width: int
    height: int
    category_ids: np.ndarray
    source_encoding: str
    source_fingerprint: str

    def to_meta(self, spec: ShardSpec) -> dict[str, Any]:
        return {
            "format": "PANO_DECODE_V1",
            "shard_id": self.shard_id,
            "width": self.width,
            "height": self.height,
            "source_encoding": self.source_encoding,
            "source_fingerprint": self.source_fingerprint,
            "categories": [category.to_v2_dict() for category in spec.categories],
        }


def _rgb_to_key(rgb: np.ndarray) -> np.ndarray:
    rgb64 = rgb.astype(np.int64)
    return (rgb64[..., 0] << 16) | (rgb64[..., 1] << 8) | rgb64[..., 2]


def _key_to_hex(key: int) -> str:
    return "#{:06X}".format(int(key) & 0xFFFFFF)


def decode_semantic(image_path: Path, spec: ShardSpec) -> DecodeResult:
    """Decode an RGB or palette PNG into manifest category ids."""

    try:
        source = image_path.read_bytes()
    except FileNotFoundError as exc:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Shard image is missing",
            {"shard_id": spec.shard_id, "path": str(image_path)},
        ) from exc
    try:
        with Image.open(io.BytesIO(source)) as image:
            source_format = image.format
            mode = image.mode
            if source_format != "PNG":
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Shard image must be a PNG",
                    {"shard_id": spec.shard_id, "format": source_format},
                )
            indexed = mode in {"P", "PA"}
            rgb_image = image.convert("RGB")
            rgb = np.asarray(rgb_image, dtype=np.uint8)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Unable to decode shard image",
            {"shard_id": spec.shard_id, "path": str(image_path), "reason": str(exc)},
        ) from exc

    height, width = rgb.shape[:2]
    if spec.width is not None and spec.width != width:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Manifest width does not match image width",
            {"shard_id": spec.shard_id, "manifest": spec.width, "image": width},
        )
    if spec.height is not None and spec.height != height:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Manifest height does not match image height",
            {"shard_id": spec.shard_id, "manifest": spec.height, "image": height},
        )
    encoding_aliases = {"palette": "indexed", "png_indexed": "indexed", "palette_png": "indexed", "rgb_png": "rgb"}
    expected_encoding = encoding_aliases.get(spec.source_encoding.lower(), spec.source_encoding.lower())
    detected_encoding = "indexed" if indexed else "rgb"
    if expected_encoding not in {"auto", "any", detected_encoding}:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Image encoding does not match manifest declaration",
            {"shard_id": spec.shard_id, "expected": spec.source_encoding, "detected": detected_encoding},
        )

    color_to_category = {
        (category.color[0] << 16) | (category.color[1] << 8) | category.color[2]: category.category_id
        for category in spec.categories
    }
    keys = _rgb_to_key(rgb).reshape(-1)
    unique, inverse, counts = np.unique(keys, return_inverse=True, return_counts=True)
    unknown = [int(key) for key in unique if int(key) not in color_to_category]
    if unknown:
        flat_positions = np.arange(keys.size, dtype=np.int64)
        unknown_details = []
        for key in sorted(unknown):
            mask = keys == key
            position = int(flat_positions[mask][0])
            y, x = divmod(position, width)
            unknown_details.append(
                {
                    "color": _key_to_hex(key),
                    "pixel_count": int(np.count_nonzero(mask)),
                    "sample_xy": [x, y],
                }
            )
        raise PipelineError(
            E_UNKNOWN_COLOR,
            "Image contains colors absent from the manifest category mapping",
            {
                "shard_id": spec.shard_id,
                "unknown_pixel_count": int(sum(item["pixel_count"] for item in unknown_details)),
                "unknown_color_count": len(unknown_details),
                "unknown_colors": unknown_details,
                "image_path": spec.image_ref,
                "detected_encoding": detected_encoding,
            },
        )

    unique_category_ids = np.fromiter(
        (color_to_category[int(key)] for key in unique),
        dtype=np.int64,
        count=len(unique),
    )
    category_ids = unique_category_ids[inverse].reshape(height, width)
    return DecodeResult(
        shard_id=spec.shard_id,
        width=width,
        height=height,
        category_ids=category_ids.astype(np.int64, copy=False),
        source_encoding=detected_encoding,
        source_fingerprint=digest_bytes(source),
    )


def _connected_components(mask: np.ndarray) -> list[np.ndarray]:
    """Return 4-connected component masks in deterministic scan order."""

    height, width = mask.shape
    visited = np.zeros_like(mask, dtype=bool)
    components: list[np.ndarray] = []
    for y in range(height):
        for x in range(width):
            if not mask[y, x] or visited[y, x]:
                continue
            visited[y, x] = True
            stack = [(y, x)]
            coords: list[tuple[int, int]] = []
            while stack:
                cy, cx = stack.pop()
                coords.append((cy, cx))
                for ny, nx in ((cy - 1, cx), (cy + 1, cx), (cy, cx - 1), (cy, cx + 1)):
                    if 0 <= ny < height and 0 <= nx < width and mask[ny, nx] and not visited[ny, nx]:
                        visited[ny, nx] = True
                        stack.append((ny, nx))
            component = np.zeros_like(mask, dtype=bool)
            ys = np.fromiter((coord[0] for coord in coords), dtype=np.int64, count=len(coords))
            xs = np.fromiter((coord[1] for coord in coords), dtype=np.int64, count=len(coords))
            component[ys, xs] = True
            components.append(component)
    return components


def _mask_bbox(mask: np.ndarray) -> tuple[int, int, int, int]:
    ys, xs = np.nonzero(mask)
    if len(xs) == 0:
        return (0, 0, 0, 0)
    x0, x1 = int(xs.min()), int(xs.max())
    y0, y1 = int(ys.min()), int(ys.max())
    return (x0, y0, x1 - x0 + 1, y1 - y0 + 1)


def merge_instances(decoded: DecodeResult, spec: ShardSpec) -> PanopticModel:
    """Apply per-category connected components and merge into panoptic ids."""

    labels = np.zeros((decoded.height, decoded.width), dtype=np.int64)
    segments: list[SegmentInfo] = []
    next_segment_id = spec.instance_start_id

    for category in sorted(spec.categories, key=lambda item: item.category_id):
        category_mask = decoded.category_ids == category.category_id
        if not np.any(category_mask) or category.confidence < spec.confidence_threshold:
            continue
        if category.thing:
            instance_index = 0
            for component in _connected_components(category_mask):
                area = int(np.count_nonzero(component))
                if area < spec.min_instance_area:
                    continue
                labels[component] = next_segment_id
                segments.append(
                    derive_segment_attributes(
                        segment_id=next_segment_id,
                        category_name=category.name,
                        category_id=category.category_id,
                        instance_index=instance_index,
                        mask=component,
                        confidence=category.confidence,
                    )
                )
                next_segment_id += 1
                instance_index += 1
        else:
            area = int(np.count_nonzero(category_mask))
            if area < spec.min_instance_area:
                continue
            labels[category_mask] = next_segment_id
            segments.append(
                derive_segment_attributes(
                    segment_id=next_segment_id,
                    category_name=category.name,
                    category_id=category.category_id,
                    instance_index=0,
                    mask=category_mask,
                    confidence=category.confidence,
                )
            )
            next_segment_id += 1

    background_mask = labels == 0
    if np.any(background_mask):
        segments.append(
            SegmentInfo(
                segment_id=0,
                category_name=BACKGROUND_CATEGORY_NAME,
                category_id=BACKGROUND_CATEGORY_ID,
                instance_index=0,
                pixel_count=int(np.count_nonzero(background_mask)),
                bbox=_mask_bbox(background_mask),
                confidence=1.0,
            )
        )
    segments.sort(key=lambda segment: segment.segment_id)
    return PanopticModel(
        shard_id=decoded.shard_id,
        width=decoded.width,
        height=decoded.height,
        labels=labels,
        segments=segments,
    )
