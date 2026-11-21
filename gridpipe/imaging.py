"""Small deterministic image primitives used by generation and extraction."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import numpy as np
from PIL import Image, ImageDraw

from .errors import E_IO, PipelineError


Rect = Tuple[int, int, int, int]


def load_rgb_and_gray(path: str | Path) -> Tuple[np.ndarray, np.ndarray]:
    try:
        with Image.open(path) as image:
            rgb = np.asarray(image.convert("RGB"), dtype=np.uint8)
            gray = np.asarray(image.convert("L"), dtype=np.uint8)
    except Exception as exc:  # pragma: no cover - exercised through public API
        raise PipelineError(
            E_IO,
            f"cannot read image: {exc}",
            diagnostics={"path": str(path), "exception": type(exc).__name__},
        ) from exc
    return rgb.copy(), gray.copy()


def connected_components(
    mask: np.ndarray, *, return_labels: bool = False
) -> List[Dict[str, Any]] | Tuple[np.ndarray, List[Dict[str, Any]]]:
    """Return 8-connected components using deterministic run-length CCL.

    The result includes a 1-pixel-exclusive bounding box and foreground area.
    Components are sorted by ``(x0, y0, x1, y1)``.
    """

    if mask.ndim != 2:
        raise ValueError("mask must be two dimensional")
    height, width = mask.shape
    if height == 0 or width == 0:
        return []

    parent: Dict[int, int] = {}
    run_meta: Dict[int, Tuple[int, int, int, int]] = {}
    run_instances: List[Tuple[int, int, int, int]] = []

    def find(node: int) -> int:
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != node:
            next_node = parent[node]
            parent[node] = root
            node = next_node
        return root

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    previous: List[Tuple[int, int, int]] = []
    next_identifier = 0
    for y in range(height):
        row = mask[y]
        padded = np.empty(width + 2, dtype=np.int8)
        padded[0] = 0
        padded[-1] = 0
        padded[1:-1] = row.astype(np.int8)
        transitions = np.flatnonzero(np.diff(padded))
        current: List[Tuple[int, int, int]] = []
        for start, end in zip(transitions[0::2].tolist(), transitions[1::2].tolist()):
            identifier = next_identifier
            next_identifier += 1
            parent[identifier] = identifier
            run_meta[identifier] = (start, y, end, y + 1)
            run_instances.append((identifier, start, y, end))
            current.append((identifier, start, end))

        left_index = 0
        right_index = 0
        while left_index < len(previous) and right_index < len(current):
            _, left_start, left_end = previous[left_index]
            right_id, right_start, right_end = current[right_index]
            if left_end < right_start:
                left_index += 1
            elif right_end < left_start:
                right_index += 1
            else:
                union(previous[left_index][0], right_id)
                if left_end < right_end:
                    left_index += 1
                elif right_end < left_end:
                    right_index += 1
                else:
                    left_index += 1
                    right_index += 1
        previous = current

    aggregated: Dict[int, Dict[str, int]] = {}
    for identifier, (x0, y0, x1, y1) in run_meta.items():
        root = find(identifier)
        area = x1 - x0
        item = aggregated.get(root)
        if item is None:
            aggregated[root] = {
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "area": area,
            }
        else:
            item["x0"] = min(item["x0"], x0)
            item["y0"] = min(item["y0"], y0)
            item["x1"] = max(item["x1"], x1)
            item["y1"] = max(item["y1"], y1)
            item["area"] += area

    result = [dict(item) for item in aggregated.values()]
    result.sort(key=lambda item: (item["x0"], item["y0"], item["x1"], item["y1"]))
    for index, item in enumerate(result, start=1):
        item["label"] = index
    if not return_labels:
        return result

    labels = np.zeros((height, width), dtype=np.int32)
    # Build a root -> final label map without relying on run ordering.
    aggregate_roots = list(aggregated.keys())
    sorted_roots = sorted(
        aggregate_roots,
        key=lambda root: (
            aggregated[root]["x0"],
            aggregated[root]["y0"],
            aggregated[root]["x1"],
            aggregated[root]["y1"],
        ),
    )
    root_labels = {root: index + 1 for index, root in enumerate(sorted_roots)}
    for identifier, start, y, end in run_instances:
        labels[y, start:end] = root_labels[find(identifier)]
    return labels, result


def contiguous_groups(indices: Sequence[int], merge_gap: int = 0) -> List[Tuple[int, int]]:
    """Group sorted integer indices into half-open intervals."""

    if len(indices) == 0:
        return []
    groups: List[Tuple[int, int]] = []
    start = int(indices[0])
    end = start + 1
    for raw in indices[1:]:
        value = int(raw)
        if value <= end + merge_gap:
            end = value + 1
        else:
            groups.append((start, end))
            start = value
            end = value + 1
    groups.append((start, end))
    return groups


def column_groups(
    mask: np.ndarray, x0: int, x1: int, y0: int, y1: int, merge_gap: int = 0
) -> List[Tuple[int, int]]:
    if x1 <= x0 or y1 <= y0:
        return []
    presence = mask[y0:y1, x0:x1].any(axis=0)
    return contiguous_groups(np.flatnonzero(presence) + x0, merge_gap)


def row_groups(
    mask: np.ndarray, x0: int, x1: int, y0: int, y1: int, merge_gap: int = 0
) -> List[Tuple[int, int]]:
    if x1 <= x0 or y1 <= y0:
        return []
    presence = mask[y0:y1, x0:x1].any(axis=1)
    return contiguous_groups(np.flatnonzero(presence) + y0, merge_gap)


FONT_5X7: Dict[str, Tuple[str, ...]] = {
    "0": ("01110", "10001", "10011", "10101", "11001", "10001", "01110"),
    "1": ("00100", "01100", "00100", "00100", "00100", "00100", "01110"),
    "2": ("01110", "10001", "00001", "00010", "00100", "01000", "11111"),
    "3": ("11110", "00001", "00001", "01110", "00001", "00001", "11110"),
    "4": ("00010", "00110", "01010", "10010", "11111", "00010", "00010"),
    "5": ("11111", "10000", "10000", "11110", "00001", "00001", "11110"),
    "6": ("00110", "01000", "10000", "11110", "10001", "10001", "01110"),
    "7": ("11111", "00001", "00010", "00100", "01000", "01000", "01000"),
    "8": ("01110", "10001", "10001", "01110", "10001", "10001", "01110"),
    "9": ("01110", "10001", "10001", "01111", "00001", "00010", "01100"),
    "-": ("00000", "00000", "00000", "11111", "00000", "00000", "00000"),
    ".": ("00000", "00000", "00000", "00000", "00000", "00000", "00100"),
    " ": ("00000", "00000", "00000", "00000", "00000", "00000", "00000"),
    "A": ("01110", "10001", "10001", "11111", "10001", "10001", "10001"),
    "B": ("11110", "10001", "10001", "11110", "10001", "10001", "11110"),
    "C": ("01111", "10000", "10000", "10000", "10000", "10000", "01111"),
    "D": ("11110", "10001", "10001", "10001", "10001", "10001", "11110"),
    "E": ("11111", "10000", "10000", "11110", "10000", "10000", "11111"),
    "F": ("11111", "10000", "10000", "11110", "10000", "10000", "10000"),
    "G": ("01111", "10000", "10000", "10111", "10001", "10001", "01111"),
    "H": ("10001", "10001", "10001", "11111", "10001", "10001", "10001"),
    "I": ("01110", "00100", "00100", "00100", "00100", "00100", "01110"),
    "J": ("00111", "00010", "00010", "00010", "00010", "10010", "01100"),
    "K": ("10001", "10010", "10100", "11000", "10100", "10010", "10001"),
    "L": ("10000", "10000", "10000", "10000", "10000", "10000", "11111"),
    "M": ("10001", "11011", "10101", "10101", "10001", "10001", "10001"),
    "N": ("10001", "11001", "10101", "10011", "10001", "10001", "10001"),
    "O": ("01110", "10001", "10001", "10001", "10001", "10001", "01110"),
    "P": ("11110", "10001", "10001", "11110", "10000", "10000", "10000"),
    "Q": ("01110", "10001", "10001", "10001", "10101", "10010", "01101"),
    "R": ("11110", "10001", "10001", "11110", "10100", "10010", "10001"),
    "S": ("01111", "10000", "10000", "01110", "00001", "00001", "11110"),
    "T": ("11111", "00100", "00100", "00100", "00100", "00100", "00100"),
    "U": ("10001", "10001", "10001", "10001", "10001", "10001", "01110"),
    "V": ("10001", "10001", "10001", "10001", "10001", "01010", "00100"),
    "W": ("10001", "10001", "10001", "10101", "10101", "10101", "01010"),
    "X": ("10001", "10001", "01010", "00100", "01010", "10001", "10001"),
    "Y": ("10001", "10001", "01010", "00100", "00100", "00100", "00100"),
    "Z": ("11111", "00001", "00010", "00100", "01000", "10000", "11111"),
}


def _pseudo_glyph(character: str) -> Tuple[str, ...]:
    digest = hashlib.sha256(character.encode("utf-8")).digest()
    rows: List[str] = []
    for row_index in range(7):
        bits = digest[row_index]
        row = "".join("1" if (bits >> column) & 1 else "0" for column in range(5))
        if row_index in (0, 6):
            row = "11111"
        if row_index in (1, 2, 3, 4, 5):
            row = "1" + row[1:4] + "1"
        rows.append(row)
    return tuple(rows)


def _glyph_for(character: str) -> Tuple[str, ...]:
    return FONT_5X7.get(character.upper(), _pseudo_glyph(character))


def bitmap_text_width(text: str, scale: int = 2, underline: bool = False) -> int:
    if not text:
        return 0
    width = ((len(text) * 6) - 1) * scale
    return width


def draw_bitmap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    x: int,
    y: int,
    *,
    scale: int = 2,
    fill: Tuple[int, int, int] = (20, 20, 20),
    underline: bool = False,
) -> int:
    """Draw a compact, deterministic 5x7 bitmap string.

    Non-ASCII characters intentionally use a deterministic block glyph.  This
    lets the sample render mixed Chinese/English labels without requiring a
    system font or an OCR model.
    """

    cursor = x
    for character in text:
        glyph = _glyph_for(character)
        for row_index, row in enumerate(glyph):
            for column_index, bit in enumerate(row):
                if bit == "1":
                    left = cursor + column_index * scale
                    top = y + row_index * scale
                    draw.rectangle(
                        [left, top, left + scale - 1, top + scale - 1], fill=fill
                    )
        cursor += 6 * scale
    width = bitmap_text_width(text, scale)
    if underline and text:
        underline_y = y + 7 * scale + 2
        draw.rectangle([x, underline_y, x + max(width - 1, 0), underline_y], fill=fill)
    return width


def read_bitmap_character(mask: np.ndarray, x: int, y: int, scale: int) -> str:
    observed: List[str] = []
    for row_index in range(7):
        row: List[str] = []
        for column_index in range(5):
            block = mask[
                y + row_index * scale : y + (row_index + 1) * scale,
                x + column_index * scale : x + (column_index + 1) * scale,
            ]
            row.append("1" if block.size and float(block.mean()) >= 0.45 else "0")
        observed.append("".join(row))

    best_character = "?"
    best_score = -1
    for character, glyph in FONT_5X7.items():
        if character == " ":
            continue
        score = 0
        for observed_row, expected_row in zip(observed, glyph):
            for observed_bit, expected_bit in zip(observed_row, expected_row):
                score += 1 if observed_bit == expected_bit else 0
        if score > best_score:
            best_score = score
            best_character = character
    if best_score < 30:
        return "?"
    return best_character


def decode_bitmap_label(
    mask: np.ndarray, bbox: Tuple[int, int, int, int], scale: int = 2
) -> str:
    x0, y0, x1, _ = bbox
    width = x1 - x0
    if width <= 0:
        return ""
    character_advance = 6 * scale
    count = max(1, int(round((width / scale + 1) / 6)))
    characters: List[str] = []
    for index in range(count):
        characters.append(
            read_bitmap_character(mask, x0 + index * character_advance, y0, scale)
        )
    return "".join(characters).strip()


def rect_intersection(left: Iterable[float], right: Iterable[float]) -> float:
    lx0, ly0, lx1, ly1 = list(left)
    rx0, ry0, rx1, ry1 = list(right)
    width = max(0.0, min(lx1, rx1) - max(lx0, rx0))
    height = max(0.0, min(ly1, ry1) - max(ly0, ry0))
    return width * height


def rect_union(left: Iterable[float], right: Iterable[float]) -> float:
    lx0, ly0, lx1, ly1 = list(left)
    rx0, ry0, rx1, ry1 = list(right)
    left_area = max(0.0, lx1 - lx0) * max(0.0, ly1 - ly0)
    right_area = max(0.0, rx1 - rx0) * max(0.0, ry1 - ry0)
    return left_area + right_area - rect_intersection(left, right)


def iou(left: Iterable[float], right: Iterable[float]) -> float:
    union = rect_union(left, right)
    if union <= 0:
        return 0.0
    return rect_intersection(left, right) / union
