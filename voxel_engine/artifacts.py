"""Canonical binary encodings for stage artifacts.

Layout: b"VEA" + format-tag byte + uint32 LE header length + canonical JSON
header + raw little-endian arrays. The header carries every statistic needed
for status queries so artifact bodies only need parsing for verification.
Encodings are byte-deterministic for identical inputs.
"""

from __future__ import annotations

import json

import numpy as np

from .serialization import canonical_json, f8, i4, u4

MAGIC = b"VEA"


class ArtifactCorrupt(ValueError):
    pass


def _pack(tag: bytes, header: dict, bodies: list) -> bytes:
    head = canonical_json(header)
    out = [MAGIC, tag, len(head).to_bytes(4, "little"), head]
    out.extend(bodies)
    return b"".join(out)


def _unpack(tag: bytes, data: bytes):
    if len(data) < 8 or data[:3] != MAGIC or data[3:4] != tag:
        raise ArtifactCorrupt("bad magic or tag")
    head_len = int.from_bytes(data[4:8], "little")
    head_end = 8 + head_len
    if head_end > len(data):
        raise ArtifactCorrupt("truncated header")
    try:
        header = json.loads(data[8:head_end].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactCorrupt(f"bad header: {exc}") from exc
    return header, data[head_end:]


def encode_projection(result: dict) -> bytes:
    header = {
        "format": "projection",
        "version": 1,
        "view_id": result["view_id"],
        "count": int(result["count"]),
        "clipped_count": int(result["clipped_count"]),
        "landmark_count": int(result["landmark_count"]),
    }
    bodies = [
        f8(result["t"]).tobytes(),
        f8(result["xyz"]).tobytes(),
        f8(result["rgb"]).tobytes(),
        u4(result["landmark_index"]).tobytes(),
        u4(result["sample_index"]).tobytes(),
    ]
    return _pack(b"1", header, bodies)


def decode_projection(data: bytes) -> dict:
    header, body = _unpack(b"1", data)
    count = int(header["count"])
    sizes = [8 * count, 24 * count, 24 * count, 4 * count, 4 * count]
    if sum(sizes) != len(body):
        raise ArtifactCorrupt("projection body size mismatch")
    off = 0
    arrays = []
    for size, dtype, shape in zip(
        sizes,
        ("<f8", "<f8", "<f8", "<u4", "<u4"),
        ((count,), (count, 3), (count, 3), (count,), (count,)),
    ):
        arrays.append(np.frombuffer(body[off:off + size], dtype=dtype).reshape(shape))
        off += size
    return {
        "view_id": header["view_id"],
        "count": count,
        "clipped_count": int(header["clipped_count"]),
        "landmark_count": int(header["landmark_count"]),
        "t": arrays[0],
        "xyz": arrays[1],
        "rgb": arrays[2],
        "landmark_index": arrays[3],
        "sample_index": arrays[4],
    }


def encode_voxel_block(block: dict, stats: dict) -> bytes:
    """Encode a normalized stage-2 block (coord -> (color_avg, count, weight))."""
    voxels = block["voxels"]
    coords = sorted(voxels.keys())
    color_avg = []
    counts = []
    weights = []
    for coord in coords:
        avg, count, weight = voxels[coord]
        color_avg.append(avg)
        counts.append(count)
        weights.append(weight)
    header = {
        "format": "voxel_block",
        "version": 1,
        "view_id": block["view_id"],
        "voxel_count": len(coords),
        "occupied_voxels": int(stats["occupied_voxels"]),
        "occluded_skips": int(stats["occluded_skips"]),
        "total_hits": int(stats["total_hits"]),
    }
    bodies = [
        i4(np.asarray(coords, dtype=np.int32).reshape(len(coords), 3)).tobytes(),
        f8(np.asarray(color_avg, dtype=np.float64).reshape(len(coords), 3)).tobytes(),
        u4(np.asarray(counts, dtype=np.uint32)).tobytes(),
        f8(np.asarray(weights, dtype=np.float64)).tobytes(),
    ]
    return _pack(b"2", header, bodies)


def decode_voxel_block(data: bytes) -> dict:
    header, body = _unpack(b"2", data)
    count = int(header["voxel_count"])
    sizes = [12 * count, 24 * count, 4 * count, 8 * count]
    if sum(sizes) != len(body):
        raise ArtifactCorrupt("voxel block body size mismatch")
    off = 0
    chunks = []
    for size in sizes:
        chunks.append(body[off:off + size])
        off += size
    coords = np.frombuffer(chunks[0], dtype="<i4").reshape(count, 3)
    colors = np.frombuffer(chunks[1], dtype="<f8").reshape(count, 3)
    counts = np.frombuffer(chunks[2], dtype="<u4").reshape(count)
    weights = np.frombuffer(chunks[3], dtype="<f8").reshape(count)
    voxels = {}
    for idx in range(count):
        coord = (int(coords[idx, 0]), int(coords[idx, 1]), int(coords[idx, 2]))
        cnt = int(counts[idx])
        avg = colors[idx]
        voxels[coord] = (
            (float(avg[0]), float(avg[1]), float(avg[2])),
            cnt,
            float(weights[idx]),
        )
    return {
        "view_id": header["view_id"],
        "voxels": voxels,
        "occupied_voxels": int(header["occupied_voxels"]),
        "occluded_skips": int(header["occluded_skips"]),
        "total_hits": int(header["total_hits"]),
    }


def _encode_field(tag: bytes, fmt: str, field: dict, extra_header: dict) -> bytes:
    voxels = field["field"]
    coords = sorted(voxels.keys())
    colors = [voxels[c][0] for c in coords]
    weights = [voxels[c][1] for c in coords]
    header = {"format": fmt, "version": 1, "voxel_count": len(coords)}
    header.update(extra_header)
    bodies = [
        i4(np.asarray(coords, dtype=np.int32).reshape(len(coords), 3)).tobytes(),
        f8(np.asarray(colors, dtype=np.float64).reshape(len(coords), 3)).tobytes(),
        f8(np.asarray(weights, dtype=np.float64)).tobytes(),
    ]
    return _pack(tag, header, bodies)


def _decode_field(tag: bytes, data: bytes):
    header, body = _unpack(tag, data)
    count = int(header["voxel_count"])
    sizes = [12 * count, 24 * count, 8 * count]
    if sum(sizes) != len(body):
        raise ArtifactCorrupt("field body size mismatch")
    off = 0
    chunks = []
    for size in sizes:
        chunks.append(body[off:off + size])
        off += size
    coords = np.frombuffer(chunks[0], dtype="<i4").reshape(count, 3)
    colors = np.frombuffer(chunks[1], dtype="<f8").reshape(count, 3)
    weights = np.frombuffer(chunks[2], dtype="<f8").reshape(count)
    field = {}
    for idx in range(count):
        coord = (int(coords[idx, 0]), int(coords[idx, 1]), int(coords[idx, 2]))
        field[coord] = (
            (float(colors[idx, 0]), float(colors[idx, 1]), float(colors[idx, 2])),
            float(weights[idx]),
        )
    return header, {"field": field, "voxel_count": count}


def encode_fused_field(fused: dict) -> bytes:
    return _encode_field(b"3", "fused_field", fused, {})


def decode_fused_field(data: bytes) -> dict:
    _header, field = _decode_field(b"3", data)
    return field


def encode_sparse_field(result: dict) -> bytes:
    return _encode_field(b"4", "sparse_field", result, {"stats": result["stats"]})


def decode_sparse_field(data: bytes):
    header, field = _decode_field(b"4", data)
    return {
        "field": field["field"],
        "stats": header["stats"],
        "voxel_count": field["voxel_count"],
    }
