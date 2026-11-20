"""Unified internal panoptic model and deterministic validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .errors import E_INVALID_CONFIG, PipelineError
from .fingerprint import digest_bytes, digest_json


BACKGROUND_CATEGORY_ID = 0
BACKGROUND_CATEGORY_NAME = "_background"


@dataclass(frozen=True)
class SegmentInfo:
    segment_id: int
    category_name: str
    category_id: int
    instance_index: int
    pixel_count: int
    bbox: tuple[int, int, int, int]
    confidence: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.segment_id,
            "category_name": self.category_name,
            "category_id": self.category_id,
            "instance_index": self.instance_index,
            "pixel_count": self.pixel_count,
            "bbox": list(self.bbox),
            "confidence": round(float(self.confidence), 8),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SegmentInfo":
        try:
            bbox = value["bbox"]
            return cls(
                segment_id=int(value["id"]),
                category_name=str(value["category_name"]),
                category_id=int(value["category_id"]),
                instance_index=int(value["instance_index"]),
                pixel_count=int(value["pixel_count"]),
                bbox=(int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])),
                confidence=float(value.get("confidence", 1.0)),
            )
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise PipelineError(
                E_INVALID_CONFIG,
                "Malformed segment attribute",
                {"segment": value, "reason": str(exc)},
            ) from exc


@dataclass
class PanopticModel:
    shard_id: str
    width: int
    height: int
    labels: np.ndarray
    segments: list[SegmentInfo]

    def __post_init__(self) -> None:
        if self.width <= 0 or self.height <= 0:
            raise PipelineError(E_INVALID_CONFIG, "Image dimensions must be positive")
        if self.labels.shape != (self.height, self.width):
            raise PipelineError(
                E_INVALID_CONFIG,
                "Segment label map shape does not match width/height",
                {"shape": list(self.labels.shape), "width": self.width, "height": self.height},
            )
        if not np.issubdtype(self.labels.dtype, np.integer):
            raise PipelineError(E_INVALID_CONFIG, "Segment label map must use an integer dtype")
        self.labels = self.labels.astype(np.int64, copy=False)
        self.segments = sorted(self.segments, key=lambda item: item.segment_id)
        self.validate()

    def validate(self) -> None:
        ids = [segment.segment_id for segment in self.segments]
        if len(ids) != len(set(ids)):
            raise PipelineError(E_INVALID_CONFIG, "Duplicate segment ids in segment table")
        if not ids:
            raise PipelineError(E_INVALID_CONFIG, "Segment table cannot be empty")
        known = set(ids)
        present = set(int(v) for v in np.unique(self.labels))
        unknown = sorted(present - known)
        if unknown:
            raise PipelineError(
                E_INVALID_CONFIG,
                "Label map contains ids absent from the segment table",
                {"segment_ids": unknown},
            )
        for segment in self.segments:
            if segment.segment_id < 0 or segment.category_id < 0 or segment.instance_index < 0:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment ids, category ids and instance indexes must be non-negative",
                    {"segment": segment.to_dict()},
                )
            if segment.pixel_count <= 0:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment pixel count must be positive",
                    {"segment_id": segment.segment_id, "pixel_count": segment.pixel_count},
                )
            if not 0.0 <= segment.confidence <= 1.0:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment confidence must be in [0, 1]",
                    {"segment_id": segment.segment_id, "confidence": segment.confidence},
                )
            count = int(np.count_nonzero(self.labels == segment.segment_id))
            if count != segment.pixel_count:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment pixel count mismatch",
                    {"segment_id": segment.segment_id, "stored": segment.pixel_count, "actual": count},
                )
            if count == 0:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment table contains an empty segment",
                    {"segment_id": segment.segment_id},
                )
            ys, xs = np.nonzero(self.labels == segment.segment_id)
            actual_bbox = (
                int(xs.min()),
                int(ys.min()),
                int(xs.max() - xs.min() + 1),
                int(ys.max() - ys.min() + 1),
            )
            if actual_bbox != segment.bbox:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Segment bounding box mismatch",
                    {
                        "segment_id": segment.segment_id,
                        "stored": list(segment.bbox),
                        "actual": list(actual_bbox),
                    },
                )
        total = sum(segment.pixel_count for segment in self.segments)
        if total != self.width * self.height:
            raise PipelineError(
                E_INVALID_CONFIG,
                "Segment pixel counts do not cover the image",
                {"sum": total, "expected": self.width * self.height},
            )

    @property
    def segment_by_id(self) -> dict[int, SegmentInfo]:
        return {segment.segment_id: segment for segment in self.segments}

    @property
    def pixel_count(self) -> int:
        return int(self.width * self.height)

    def attributes(self) -> list[dict[str, Any]]:
        return [segment.to_dict() for segment in self.segments]

    def to_dict(self, *, include_segments: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "format": "PANO_MODEL_V1",
            "shard_id": self.shard_id,
            "width": self.width,
            "height": self.height,
            "segment_count": len(self.segments),
            "pixel_count": self.pixel_count,
        }
        if include_segments:
            result["segments"] = self.attributes()
        return result

    def digest(self) -> str:
        return digest_bytes(self.labels.tobytes(order="C") + digest_json(self.to_dict()).encode("ascii"))

    def equality_projection(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "width": self.width,
            "height": self.height,
            "segments": self.attributes(),
        }


def derive_segment_attributes(
    *,
    segment_id: int,
    category_name: str,
    category_id: int,
    instance_index: int,
    mask: np.ndarray,
    confidence: float,
) -> SegmentInfo:
    ys, xs = np.nonzero(mask)
    if len(xs) == 0:
        raise PipelineError(E_INVALID_CONFIG, "Cannot derive attributes from an empty segment mask")
    x0 = int(xs.min())
    y0 = int(ys.min())
    x1 = int(xs.max())
    y1 = int(ys.max())
    return SegmentInfo(
        segment_id=segment_id,
        category_name=category_name,
        category_id=category_id,
        instance_index=instance_index,
        pixel_count=int(len(xs)),
        bbox=(x0, y0, x1 - x0 + 1, y1 - y0 + 1),
        confidence=confidence,
    )


def expected_segment_attributes(labels: np.ndarray, segments: Iterable[SegmentInfo]) -> list[SegmentInfo]:
    result: list[SegmentInfo] = []
    for segment in segments:
        result.append(
            derive_segment_attributes(
                segment_id=segment.segment_id,
                category_name=segment.category_name,
                category_id=segment.category_id,
                instance_index=segment.instance_index,
                mask=labels == segment.segment_id,
                confidence=segment.confidence,
            )
        )
    return result
