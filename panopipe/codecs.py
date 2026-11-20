"""Lossless encoders/decoders for panoptic mask interchange formats."""

from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import numpy as np
from PIL import Image
from PIL.PngImagePlugin import PngInfo

from .errors import (
    E_FORMAT_ERROR,
    E_PALETTE_CAPACITY,
    E_SEGMENT_ID_OVERFLOW,
    PipelineError,
)
from .fingerprint import canonical_json
from .model import PanopticModel, SegmentInfo, expected_segment_attributes


RLE_PREFIX = "PANO-RLE-1:"
RLE_JSON_FORMAT = "PANO_RLE_JSON_V1"
COCO_FORMAT = "PANO_COCO_V1"


@dataclass(frozen=True)
class CodecOptions:
    palette_max_colors: int = 256
    segment_id_bits: int = 24
    png_compress_level: int = 6

    def validate(self) -> None:
        if not 1 <= self.palette_max_colors <= 256:
            raise PipelineError(
                E_PALETTE_CAPACITY,
                "palette_max_colors must be between 1 and 256",
                {"value": self.palette_max_colors},
            )
        if not 1 <= self.segment_id_bits <= 24:
            raise PipelineError(
                E_SEGMENT_ID_OVERFLOW,
                "segment_id_bits must be between 1 and 24",
                {"value": self.segment_id_bits},
            )
        if not 0 <= self.png_compress_level <= 9:
            raise PipelineError(E_FORMAT_ERROR, "png_compress_level must be between 0 and 9")

    def fingerprint(self) -> dict[str, Any]:
        return {
            "palette_max_colors": self.palette_max_colors,
            "segment_id_bits": self.segment_id_bits,
            "png_compress_level": self.png_compress_level,
        }


def _segment_id_limit(options: CodecOptions) -> int:
    return (1 << options.segment_id_bits) - 1


def check_segment_ids(model: PanopticModel, options: CodecOptions) -> None:
    limit = _segment_id_limit(options)
    max_id = max(segment.segment_id for segment in model.segments)
    if max_id > limit or int(model.labels.max(initial=0)) > limit:
        raise PipelineError(
            E_SEGMENT_ID_OVERFLOW,
            "Segment id exceeds configured bit width; refusing to truncate",
            {
                "shard_id": model.shard_id,
                "max_segment_id": max_id,
                "segment_id_bits": options.segment_id_bits,
                "maximum": limit,
                "degradation": {
                    "code": "D_SEGMENT_ID_BITS_EXCEEDED",
                    "error_code": E_SEGMENT_ID_OVERFLOW,
                    "limit": limit,
                    "actual": max_id,
                    "action": "format_not_encoded",
                    "lossless": False,
                },
            },
        )


def _metadata(model: PanopticModel, format_name: str) -> dict[str, Any]:
    return {
        "format": format_name,
        "shard_id": model.shard_id,
        "width": model.width,
        "height": model.height,
        "segments": model.attributes(),
    }


def _segments_from_metadata(metadata: Mapping[str, Any]) -> list[SegmentInfo]:
    values = metadata.get("segments")
    if not isinstance(values, list) or not values:
        raise PipelineError(E_FORMAT_ERROR, "Encoded mask metadata is missing segments")
    return [SegmentInfo.from_dict(value) for value in values]


def _validate_metadata_segments(
    labels: np.ndarray,
    segments: Iterable[SegmentInfo],
    *,
    shard_id: str,
    width: int,
    height: int,
) -> PanopticModel:
    segment_list = list(segments)
    by_id = {segment.segment_id: segment for segment in segment_list}
    if len(by_id) != len(segment_list):
        raise PipelineError(E_FORMAT_ERROR, "Encoded metadata contains duplicate segment ids")
    label_ids = sorted(int(value) for value in np.unique(labels))
    if set(label_ids) != set(by_id):
        raise PipelineError(
            E_FORMAT_ERROR,
            "Encoded label ids and segment metadata do not match",
            {"labels": label_ids, "metadata": sorted(by_id)},
        )
    expected = {
        segment.segment_id: segment
        for segment in expected_segment_attributes(labels, segment_list)
    }
    for segment in segment_list:
        regenerated = expected[segment.segment_id]
        if regenerated.pixel_count != segment.pixel_count or regenerated.bbox != segment.bbox:
            raise PipelineError(
                E_FORMAT_ERROR,
                "Encoded segment attributes are inconsistent with the label map",
                {
                    "segment_id": segment.segment_id,
                    "encoded_pixel_count": segment.pixel_count,
                    "actual_pixel_count": regenerated.pixel_count,
                    "encoded_bbox": list(segment.bbox),
                    "actual_bbox": list(regenerated.bbox),
                },
            )
    return PanopticModel(
        shard_id=shard_id,
        width=width,
        height=height,
        labels=labels,
        segments=segment_list,
    )


def encode_png(model: PanopticModel, options: CodecOptions | None = None) -> bytes:
    options = options or CodecOptions()
    options.validate()
    check_segment_ids(model, options)
    ids = [int(value) for value in np.unique(model.labels)]
    if len(ids) > options.palette_max_colors:
        raise PipelineError(
            E_PALETTE_CAPACITY,
            "Palette entry count exceeds configured limit; refusing to truncate segment ids",
            {
                "shard_id": model.shard_id,
                "palette_entries": len(ids),
                "palette_max_colors": options.palette_max_colors,
                "degradation": {
                    "code": "D_PALETTE_CAPACITY_EXCEEDED",
                    "error_code": E_PALETTE_CAPACITY,
                    "limit": options.palette_max_colors,
                    "actual": len(ids),
                    "action": "format_not_encoded",
                    "lossless": False,
                },
            },
        )
    index_for_id = {segment_id: index for index, segment_id in enumerate(ids)}
    vector = np.vectorize(index_for_id.__getitem__, otypes=[np.uint8])
    indices = vector(model.labels).astype(np.uint8, copy=False)
    image = Image.fromarray(indices, mode="P")
    palette: list[int] = []
    for segment_id in ids:
        palette.extend(((segment_id >> 16) & 0xFF, (segment_id >> 8) & 0xFF, segment_id & 0xFF))
    image.putpalette(palette)
    pnginfo = PngInfo()
    pnginfo.add_itxt("panopipe", canonical_json(_metadata(model, "PANO_PNG_PALETTE_V1")), zip=False)
    buffer = io.BytesIO()
    image.save(
        buffer,
        format="PNG",
        pnginfo=pnginfo,
        optimize=False,
        compress_level=options.png_compress_level,
    )
    return buffer.getvalue()


def decode_png(data: bytes, options: CodecOptions | None = None) -> PanopticModel:
    options = options or CodecOptions()
    options.validate()
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.format != "PNG" or image.mode not in {"P", "PA"}:
                raise PipelineError(
                    E_FORMAT_ERROR,
                    "PNG panoptic mask must use indexed color mode",
                    {"format": image.format, "mode": image.mode},
                )
            raw_metadata = image.info.get("panopipe")
            if not isinstance(raw_metadata, str):
                raise PipelineError(E_FORMAT_ERROR, "PNG panoptic mask is missing panopipe metadata")
            rgb = np.asarray(image.convert("RGB"), dtype=np.uint8)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(E_FORMAT_ERROR, "Unable to decode PNG panoptic mask", {"reason": str(exc)}) from exc
    try:
        metadata = json.loads(raw_metadata)
    except json.JSONDecodeError as exc:
        raise PipelineError(E_FORMAT_ERROR, "PNG panopipe metadata is not valid JSON") from exc
    labels = (
        (rgb[..., 0].astype(np.int64) << 16)
        | (rgb[..., 1].astype(np.int64) << 8)
        | rgb[..., 2].astype(np.int64)
    )
    check_model_arrays(labels, metadata, options)
    return _validate_metadata_segments(
        labels,
        _segments_from_metadata(metadata),
        shard_id=str(metadata["shard_id"]),
        width=int(metadata["width"]),
        height=int(metadata["height"]),
    )


def check_model_arrays(labels: np.ndarray, metadata: Mapping[str, Any], options: CodecOptions) -> None:
    if labels.ndim != 2:
        raise PipelineError(E_FORMAT_ERROR, "Decoded label array must be two dimensional")
    if labels.shape != (int(metadata.get("height", -1)), int(metadata.get("width", -1))):
        raise PipelineError(
            E_FORMAT_ERROR,
            "Decoded label shape does not match encoded metadata",
            {"shape": list(labels.shape), "metadata": {k: metadata.get(k) for k in ("width", "height")}},
        )
    maximum = int(labels.max(initial=0))
    if maximum > _segment_id_limit(options):
        raise PipelineError(
            E_SEGMENT_ID_OVERFLOW,
            "Decoded segment id exceeds configured bit width",
            {
                "max_segment_id": maximum,
                "segment_id_bits": options.segment_id_bits,
                "maximum": _segment_id_limit(options),
            },
        )


def encode_varint(value: int) -> bytes:
    if value < 0:
        raise PipelineError(E_FORMAT_ERROR, "Varints cannot encode negative values")
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


def decode_varints(data: bytes) -> list[int]:
    values: list[int] = []
    current = 0
    shift = 0
    for byte in data:
        current |= (byte & 0x7F) << shift
        if byte & 0x80:
            shift += 7
            if shift > 63:
                raise PipelineError(E_FORMAT_ERROR, "Varint is too large")
        else:
            values.append(current)
            current = 0
            shift = 0
    if shift:
        raise PipelineError(E_FORMAT_ERROR, "Truncated varint stream")
    return values


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception as exc:
        raise PipelineError(E_FORMAT_ERROR, "Invalid base64url payload") from exc


def encode_rle(model: PanopticModel, options: CodecOptions | None = None) -> str:
    options = options or CodecOptions()
    options.validate()
    check_segment_ids(model, options)
    flat = model.labels.reshape(-1)
    changes = np.flatnonzero(flat[1:] != flat[:-1]) + 1
    boundaries = np.concatenate(([0], changes, [flat.size])).astype(np.int64)
    counts = np.diff(boundaries)
    values = flat[boundaries[:-1]]
    payload = bytearray()
    for count, value in zip(counts.tolist(), values.tolist()):
        payload.extend(encode_varint(int(count)))
        payload.extend(encode_varint(int(value)))
    header = _metadata(model, "PANO_RLE_HEADER_V1")
    header["scan_order"] = "row-major"
    return f"{RLE_PREFIX}{b64url_encode(canonical_json(header).encode('utf-8'))}:{b64url_encode(bytes(payload))}"


def decode_rle(value: str, options: CodecOptions | None = None) -> PanopticModel:
    options = options or CodecOptions()
    options.validate()
    if not isinstance(value, str) or not value.startswith(RLE_PREFIX):
        raise PipelineError(E_FORMAT_ERROR, "RLE string must start with PANO-RLE-1:")
    body = value[len(RLE_PREFIX) :]
    if body.count(":") != 1:
        raise PipelineError(E_FORMAT_ERROR, "Malformed RLE string")
    header_part, payload_part = body.split(":", 1)
    try:
        header = json.loads(b64url_decode(header_part).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PipelineError(E_FORMAT_ERROR, "RLE header is not valid JSON") from exc
    numbers = decode_varints(b64url_decode(payload_part))
    if len(numbers) % 2:
        raise PipelineError(E_FORMAT_ERROR, "RLE payload must contain count/id pairs")
    width = int(header.get("width", -1))
    height = int(header.get("height", -1))
    if width <= 0 or height <= 0:
        raise PipelineError(E_FORMAT_ERROR, "RLE header has invalid dimensions")
    total = width * height
    counts: list[int] = []
    ids: list[int] = []
    for index in range(0, len(numbers), 2):
        count, segment_id = numbers[index], numbers[index + 1]
        if count <= 0:
            raise PipelineError(E_FORMAT_ERROR, "RLE run counts must be positive")
        counts.append(count)
        ids.append(segment_id)
    if sum(counts) != total:
        raise PipelineError(
            E_FORMAT_ERROR,
            "RLE runs do not cover the image",
            {"decoded_pixels": sum(counts), "expected": total},
        )
    flat = np.repeat(np.asarray(ids, dtype=np.int64), np.asarray(counts, dtype=np.int64))
    labels = flat.reshape(height, width)
    check_model_arrays(labels, header, options)
    return _validate_metadata_segments(
        labels,
        _segments_from_metadata(header),
        shard_id=str(header["shard_id"]),
        width=width,
        height=height,
    )


def rle_to_json(value: str) -> str:
    return canonical_json({"format": RLE_JSON_FORMAT, "rle": value})


def rle_from_json(value: str | Mapping[str, Any]) -> str:
    try:
        data = json.loads(value) if isinstance(value, str) else dict(value)
    except json.JSONDecodeError as exc:
        raise PipelineError(E_FORMAT_ERROR, "RLE JSON wrapper is invalid") from exc
    if data.get("format") != RLE_JSON_FORMAT or not isinstance(data.get("rle"), str):
        raise PipelineError(E_FORMAT_ERROR, "Unexpected RLE JSON wrapper format")
    return data["rle"]


def encode_binary_counts(mask: np.ndarray) -> dict[str, Any]:
    flat = mask.reshape(-1).astype(np.uint8, copy=False)
    changes = np.flatnonzero(flat[1:] != flat[:-1]) + 1
    boundaries = np.concatenate(([0], changes, [flat.size])).astype(np.int64)
    counts = np.diff(boundaries).astype(np.int64)
    if flat.size and flat[0] == 1:
        counts = np.concatenate(([0], counts)).astype(np.int64)
    payload = b"".join(encode_varint(int(count)) for count in counts)
    return {
        "size": [int(mask.shape[0]), int(mask.shape[1])],
        "order": "row-major",
        "counts": b64url_encode(payload),
    }


def decode_binary_counts(value: Mapping[str, Any]) -> np.ndarray:
    try:
        height, width = int(value["size"][0]), int(value["size"][1])
        order = value.get("order")
        counts = decode_varints(b64url_decode(str(value["counts"])))
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise PipelineError(E_FORMAT_ERROR, "Malformed COCO RLE object") from exc
    if order != "row-major":
        raise PipelineError(E_FORMAT_ERROR, "Only row-major COCO RLE is supported", {"order": order})
    if height <= 0 or width <= 0:
        raise PipelineError(E_FORMAT_ERROR, "COCO RLE dimensions must be positive")
    total = height * width
    if sum(counts) != total:
        raise PipelineError(E_FORMAT_ERROR, "COCO RLE counts do not cover the image")
    flat = np.zeros(total, dtype=bool)
    position = 0
    value_bit = 0
    for count in counts:
        if count < 0:
            raise PipelineError(E_FORMAT_ERROR, "COCO RLE counts cannot be negative")
        if value_bit:
            flat[position : position + count] = True
        position += count
        value_bit ^= 1
    return flat.reshape(height, width)


def encode_coco_json(model: PanopticModel, options: CodecOptions | None = None) -> str:
    options = options or CodecOptions()
    options.validate()
    check_segment_ids(model, options)
    segments_info: list[dict[str, Any]] = []
    for segment in model.segments:
        item = segment.to_dict()
        item.update(
            {
                "iscrowd": 0,
                "rle": encode_binary_counts(model.labels == segment.segment_id),
            }
        )
        segments_info.append(item)
    value = {
        "format": COCO_FORMAT,
        "shard_id": model.shard_id,
        "image_id": model.shard_id,
        "file_name": f"{model.shard_id}.png",
        "width": model.width,
        "height": model.height,
        "image": {"file_name": f"{model.shard_id}.png", "width": model.width, "height": model.height},
        "segments_info": segments_info,
    }
    return canonical_json(value) + "\n"


def decode_coco_json(value: str | Mapping[str, Any], options: CodecOptions | None = None) -> PanopticModel:
    options = options or CodecOptions()
    options.validate()
    try:
        data = json.loads(value) if isinstance(value, str) else dict(value)
    except json.JSONDecodeError as exc:
        raise PipelineError(E_FORMAT_ERROR, "COCO JSON is not valid JSON") from exc
    if data.get("format") != COCO_FORMAT:
        raise PipelineError(
            E_FORMAT_ERROR,
            "Unexpected COCO-style JSON format",
            {"format": data.get("format")},
        )
    try:
        width = int(data["width"])
        height = int(data["height"])
        shard_id = str(data["shard_id"])
        segments_info = data["segments_info"]
    except (KeyError, TypeError, ValueError) as exc:
        raise PipelineError(E_FORMAT_ERROR, "COCO JSON is missing required fields") from exc
    if not isinstance(segments_info, list) or not segments_info:
        raise PipelineError(E_FORMAT_ERROR, "COCO JSON segments_info cannot be empty")
    labels = np.full((height, width), -1, dtype=np.int64)
    segments: list[SegmentInfo] = []
    for item in segments_info:
        if not isinstance(item, Mapping):
            raise PipelineError(E_FORMAT_ERROR, "Malformed COCO segment entry")
        rle_value = item.get("rle")
        if not isinstance(rle_value, Mapping):
            raise PipelineError(E_FORMAT_ERROR, "COCO segment is missing an RLE object")
        mask = decode_binary_counts(rle_value)
        if mask.shape != (height, width):
            raise PipelineError(E_FORMAT_ERROR, "COCO segment RLE size does not match image")
        segment = SegmentInfo.from_dict(item)
        if np.any(labels[mask] != -1):
            raise PipelineError(
                E_FORMAT_ERROR,
                "COCO segment RLE masks overlap",
                {"segment_id": segment.segment_id},
            )
        labels[mask] = segment.segment_id
        segments.append(segment)
    if np.any(labels == -1):
        raise PipelineError(
            E_FORMAT_ERROR,
            "COCO segment RLE masks do not cover every pixel",
            {"uncovered_pixels": int(np.count_nonzero(labels == -1))},
        )
    check_model_arrays(labels, data, options)
    return _validate_metadata_segments(
        labels,
        segments,
        shard_id=shard_id,
        width=width,
        height=height,
    )


def segment_tables_equal(left: PanopticModel, right: PanopticModel) -> bool:
    return [segment.to_dict() for segment in left.segments] == [
        segment.to_dict() for segment in right.segments
    ]


def label_maps_equal(left: PanopticModel, right: PanopticModel) -> bool:
    return (
        left.width == right.width
        and left.height == right.height
        and np.array_equal(left.labels, right.labels)
    )
