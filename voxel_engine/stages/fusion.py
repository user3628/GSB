"""Stage 3 - scene voxel field fusion.

Per-view voxel blocks are fused into the global field in a fixed, explicitly
recomputable order: shard order for the views, ascending voxel coordinates
inside each view block, ascending neighbor offsets inside each splat.

Fusion consumes *normalized* blocks: coord -> (color_avg(3), count, weight),
where color_avg = color_sum / count computed with true division in exactly
one canonical place, so blocks decoded from artifacts are bit-identical to
blocks held in memory.

For a global voxel g the fused value is

    color(g)  = sum_v w(v, g) * count(v) * color_avg(v) / norm(g)
    norm(g)   = sum_v w(v, g) * count(v)
    weight(g) = norm(g)

where v ranges over all view-local voxels (across every view, in the order
above) with |v - g| <= fusion_radius in voxel-index space, and the kernel is
w = max(0, 1 - |v - g| / fusion_radius)  (w = 1 at distance 0; when
fusion_radius == 0 only the identical voxel contributes with w = 1).

Because every accumulation happens in a fixed order with IEEE float64
arithmetic, the fused field is byte-identical for identical inputs and the
normalization above can be recomputed by hand for verification.
"""

from __future__ import annotations

import math


def fusion_offsets(radius: float):
    off_max = int(math.ceil(radius)) if radius > 0 else 0
    offsets = []
    for dx in range(-off_max, off_max + 1):
        for dy in range(-off_max, off_max + 1):
            for dz in range(-off_max, off_max + 1):
                dist = math.sqrt(dx * dx + dy * dy + dz * dz)
                if radius == 0.0:
                    if dist == 0.0:
                        offsets.append(((dx, dy, dz), 1.0))
                    continue
                if dist > radius:
                    continue
                weight = max(0.0, 1.0 - dist / radius)
                if weight > 0.0:
                    offsets.append(((dx, dy, dz), weight))
    offsets.sort(key=lambda item: item[0])
    return offsets


def normalize_block(raw_block: dict) -> dict:
    """Convert a stage-2 raw block into the canonical normalized form."""
    voxels = {}
    for coord, entry in raw_block["voxels"].items():
        color_sum, count, weight, _min_depth = entry
        voxels[coord] = (
            (color_sum[0] / count, color_sum[1] / count, color_sum[2] / count),
            count,
            weight,
        )
    return {"view_id": raw_block["view_id"], "voxels": voxels}


def run_fusion(blocks: list, fusion_radius: float) -> dict:
    """Fuse ordered normalized per-view blocks into the global voxel field."""
    radius = float(fusion_radius)
    offsets = fusion_offsets(radius)
    acc = {}  # coord -> [color_acc(3), norm]

    for block in blocks:
        voxels = block["voxels"]
        for coord in sorted(voxels.keys()):
            avg, count, _weight = voxels[coord]
            for (dx, dy, dz), kernel in offsets:
                g = (coord[0] + dx, coord[1] + dy, coord[2] + dz)
                w = kernel * count
                entry = acc.get(g)
                if entry is None:
                    entry = [[0.0, 0.0, 0.0], 0.0]
                    acc[g] = entry
                entry[0][0] += w * avg[0]
                entry[0][1] += w * avg[1]
                entry[0][2] += w * avg[2]
                entry[1] += w

    field = {}
    for coord in sorted(acc.keys()):
        color_acc, norm = acc[coord]
        if norm <= 0.0:
            continue
        inv = 1.0 / norm
        field[coord] = (
            (color_acc[0] * inv, color_acc[1] * inv, color_acc[2] * inv),
            norm,
        )
    return {"field": field, "voxel_count": len(field)}
