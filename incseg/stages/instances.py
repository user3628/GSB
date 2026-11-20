"""Stage 2: instance separation via connected components + area filtering.

Components are discovered in a fully deterministic order: classes ascending,
pixels in row-major scan order, BFS with a fixed neighbor order. Instances
smaller than ``min_area`` are filtered out (their pixels keep their semantic
label but receive instance id 0). Remaining instances are re-numbered
consecutively from 1 in discovery order.
"""

from __future__ import annotations

import numpy as np

_NEIGHBORS_4 = ((-1, 0), (1, 0), (0, -1), (0, 1))
_NEIGHBORS_8 = _NEIGHBORS_4 + ((-1, -1), (-1, 1), (1, -1), (1, 1))


def run_instances(labels: np.ndarray, connectivity: int, min_area: int):
    height, width = labels.shape
    offsets = _NEIGHBORS_8 if connectivity == 8 else _NEIGHBORS_4
    mask = np.zeros((height, width), dtype=np.uint16)
    visited = np.zeros((height, width), dtype=bool)
    items = []
    next_id = 1

    for class_id in sorted(int(c) for c in np.unique(labels) if c != 0):
        class_pixels = labels == class_id
        for start_y in range(height):
            row = class_pixels[start_y]
            for start_x in range(width):
                if not row[start_x] or visited[start_y, start_x]:
                    continue
                stack = [(start_y, start_x)]
                visited[start_y, start_x] = True
                pixels = []
                while stack:
                    y, x = stack.pop()
                    pixels.append((y, x))
                    for dy, dx in offsets:
                        ny, nx = y + dy, x + dx
                        if (0 <= ny < height and 0 <= nx < width
                                and class_pixels[ny, nx] and not visited[ny, nx]):
                            visited[ny, nx] = True
                            stack.append((ny, nx))
                area = len(pixels)
                if area < min_area:
                    continue
                ys = [p[0] for p in pixels]
                xs = [p[1] for p in pixels]
                for y, x in pixels:
                    mask[y, x] = next_id
                items.append({
                    "instance_id": next_id,
                    "class_id": class_id,
                    "area": area,
                    "bbox": [min(xs), min(ys), max(xs), max(ys)],
                })
                next_id += 1
    return mask, items
