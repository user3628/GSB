"""Stage 2 - per-view voxel contribution volume.

Each stage-1 sample is quantized into the scene voxel grid:

    index = floor((point - scene_origin) / voxel_resolution)

A sample splats into every voxel whose center lies within
``truncation_distance`` (world units) of the sample point, with weight
k(d) = max(0, 1 - d / truncation_distance) (k = 1 when the distance is 0).
Neighbor voxels outside the grid are skipped silently.

Error semantics (stable code E_VOXEL_INVALID):
  * a sample color containing Inf/NaN, or
  * a sample whose own quantized voxel index is negative / out of grid bounds
aborts the stage with the shard id and the offending voxel index attached.

Merging: hits into one voxel merge in deterministic arrival order (record
order = landmark order, then sample order). A sample whose depth exceeds the
target voxel's minimum seen depth by more than the truncation distance is
considered occluded at that voxel: it is skipped and counted.
"""

from __future__ import annotations

import math

import numpy as np

from ..errors import E_VOXEL_INVALID, EngineError


def grid_dims(config: dict) -> tuple:
    res = float(config["voxel_resolution"])
    return tuple(int(math.ceil(float(s) / res)) for s in config["scene_size"])


def _quantize(point, origin, res):
    return (
        int(math.floor((point[0] - origin[0]) / res)),
        int(math.floor((point[1] - origin[1]) / res)),
        int(math.floor((point[2] - origin[2]) / res)),
    )


def _invalid(view_id, voxel, reason):
    raise EngineError(
        E_VOXEL_INVALID,
        f"invalid voxel contribution in shard '{view_id}': {reason}",
        {"view_id": view_id, "voxel": list(voxel), "reason": reason},
    )


def run_voxelization(records: dict, config: dict) -> dict:
    """Run stage 2 for one view's stage-1 records."""
    view_id = records["view_id"]
    res = float(config["voxel_resolution"])
    trunc = float(config["truncation_distance"])
    origin = tuple(float(x) for x in config["scene_origin"])
    dims = grid_dims(config)
    feature_dim = int(config["voxel_feature_dim"])
    xyz = records["xyz"]
    rgb = records["rgb"]
    depths = records["t"]
    count = int(records["count"])

    if rgb.shape[1] != feature_dim:
        _invalid(view_id, (0, 0, 0), "feature_dim_mismatch")

    off_max = int(math.ceil(trunc / res)) if trunc > 0 else 0
    offsets = [
        (dx, dy, dz)
        for dx in range(-off_max, off_max + 1)
        for dy in range(-off_max, off_max + 1)
        for dz in range(-off_max, off_max + 1)
    ]
    offsets.sort()

    voxels = {}  # (i,j,k) -> [color_sum(3), count, weight, min_depth]
    occluded = 0

    for row in range(count):
        color = rgb[row]
        point = xyz[row]
        depth = float(depths[row])
        base = _quantize(point, origin, res)
        if not np.isfinite(color).all():
            _invalid(view_id, base, "non_finite_color")
        if any(b < 0 for b in base) or any(b >= d for b, d in zip(base, dims)):
            _invalid(view_id, base, "voxel_out_of_bounds")

        base_entry = voxels.get(base)
        if base_entry is not None and depth > base_entry[3] + trunc:
            occluded += 1
            continue

        for dx, dy, dz in offsets:
            nb = (base[0] + dx, base[1] + dy, base[2] + dz)
            if (
                nb[0] < 0 or nb[1] < 0 or nb[2] < 0
                or nb[0] >= dims[0] or nb[1] >= dims[1] or nb[2] >= dims[2]
            ):
                continue
            cx = origin[0] + (nb[0] + 0.5) * res
            cy = origin[1] + (nb[1] + 0.5) * res
            cz = origin[2] + (nb[2] + 0.5) * res
            dist = math.sqrt(
                (cx - point[0]) ** 2 + (cy - point[1]) ** 2 + (cz - point[2]) ** 2
            )
            if dist > trunc:
                continue
            weight = 1.0 if trunc == 0.0 else max(0.0, 1.0 - dist / trunc)
            if weight <= 0.0:
                continue
            entry = voxels.get(nb)
            if entry is None:
                entry = [[0.0, 0.0, 0.0], 0, 0.0, depth]
                voxels[nb] = entry
            entry[0][0] += float(color[0])
            entry[0][1] += float(color[1])
            entry[0][2] += float(color[2])
            entry[1] += 1
            entry[2] += weight
            if depth < entry[3]:
                entry[3] = depth

    return {
        "view_id": view_id,
        "voxels": voxels,
        "occupied_voxels": len(voxels),
        "occluded_skips": occluded,
        "total_hits": count,
    }
