"""Stage 3: boundary refinement and class-conflict resolution.

For each of ``refine_iterations`` synchronous sweeps, every pixel looks at its
8-neighborhood class histogram. The candidate class is the neighborhood
majority; ties are resolved by the configured class priority and, if still
tied, by the lowest class id (fully deterministic). A pixel adopts the
candidate only when the candidate's neighbor count reaches
``fusion_weight * (number of valid neighbors)``; otherwise it keeps its
current label. Synchronous updates make every sweep order independent.
"""

from __future__ import annotations

import numpy as np

from .common import neighbor_count_map, valid_neighbor_count


def run_postprocess(labels: np.ndarray, tile) -> np.ndarray:
    current = labels.astype(np.uint8).copy()
    if tile.refine_iterations <= 0:
        return current

    class_ids = sorted(int(c) for c in tile.color_map.values())
    rank = {class_id: idx for idx, class_id in enumerate(tile.class_priority)}
    # Most preferred first; a strict-greater update rule then keeps the most
    # preferred class among equal counts (deterministic tie-break).
    order = sorted(class_ids, key=lambda c: (rank.get(c, len(rank)), c))

    valid = valid_neighbor_count(current.shape, 8).astype(np.float64)
    threshold = tile.fusion_weight * valid

    for _ in range(tile.refine_iterations):
        best_count = np.full(current.shape, -1, dtype=np.int32)
        best_class = current.copy()
        for class_id in order:
            count = neighbor_count_map(current == class_id, 8)
            update = count > best_count
            best_count[update] = count[update]
            best_class[update] = class_id
        adopt = (best_count > 0) & (best_count.astype(np.float64) >= threshold) & (best_class != current)
        current = np.where(adopt, best_class, current).astype(np.uint8)
    return current
