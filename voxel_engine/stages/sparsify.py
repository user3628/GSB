"""Stage 4 - sparsification and summary.

Voxels whose normalized weight (weight / max_weight) falls below the
configured sparsity threshold are pruned. After pruning:
  * an empty scene, or
  * any remaining voxel with zero weight
aborts the stage with E_SPARSE_EMPTY, keeping the pre/post-prune counts in
the error details.

The summary reports global voxel occupancy, a color summary and the sparsity
statistics; it is serialized canonically so byte-level comparison across
builds is meaningful.
"""

from __future__ import annotations

from ..errors import E_SPARSE_EMPTY, EngineError


def _empty(reason: str, pre_count: int, post_count: int):
    raise EngineError(
        E_SPARSE_EMPTY,
        f"sparsification produced an unusable field: {reason}",
        {"reason": reason, "pre_prune_count": pre_count, "post_prune_count": post_count},
    )


def run_sparsification(field: dict, threshold: float) -> dict:
    """Prune the fused field and compute occupancy/color/sparsity stats."""
    voxels = field["field"]
    pre_count = len(voxels)
    if pre_count == 0:
        _empty("empty_input_field", 0, 0)

    threshold = float(threshold)
    max_weight = max(weight for _color, weight in voxels.values())
    cutoff = threshold * max_weight

    kept = {}
    for coord in sorted(voxels.keys()):
        color, weight = voxels[coord]
        if weight >= cutoff:
            kept[coord] = (color, weight)

    post_count = len(kept)
    if post_count == 0:
        _empty("all_voxels_pruned", pre_count, 0)
    zero_weight = [coord for coord, (_c, w) in kept.items() if w == 0.0]
    if zero_weight:
        _empty("zero_weight_voxel", pre_count, post_count)

    inv = 1.0 / post_count
    mean_color = [0.0, 0.0, 0.0]
    min_color = [float("inf")] * 3
    max_color = [float("-inf")] * 3
    for color, _weight in kept.values():
        for channel in range(3):
            value = color[channel]
            mean_color[channel] += value * inv
            min_color[channel] = min(min_color[channel], value)
            max_color[channel] = max(max_color[channel], value)

    coords = sorted(kept.keys())
    stats = {
        "threshold": threshold,
        "max_weight": max_weight,
        "pre_prune_count": pre_count,
        "post_prune_count": post_count,
        "pruned_count": pre_count - post_count,
        "sparsity_ratio": (pre_count - post_count) / pre_count,
        "occupancy": post_count,
        "color_summary": {
            "mean": mean_color,
            "min": min_color,
            "max": max_color,
        },
        "bounds_min": list(coords[0]),
        "bounds_max": list(coords[-1]),
    }
    return {"field": kept, "stats": stats}
