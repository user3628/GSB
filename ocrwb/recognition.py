"""Deterministic hash bag-of-words nearest-neighbor text recognition.

Templates are the canonical glyph grids built from the manifest truth
character set.  Candidate line crops are segmented into character cells via a
vertical projection, normalized to the canonical cell size, hashed into a
small bag-of-words feature and matched against the template library with a
deterministic normalized Hamming-style distance.
"""

from __future__ import annotations

import math

import numpy as np

from .config import WorkbenchConfig
from .errors import RecognizeInvalid
from .glyphs import CELL_H, CELL_W, binarize, canonical_cell, deshear, glyph_grid, to_grayscale
from .hashing import digest_json

def cell_feature(cell: np.ndarray) -> np.ndarray:
    """Flattened binary canonical grid used as the hash bag-of-words vector.

    Each of the deterministic cell positions is a binary "word"; matching two
    cells therefore reduces to a normalized Hamming distance on this vector.
    """
    return cell.astype(np.float64).reshape(-1)


def build_template_library(characters: list[str], config: WorkbenchConfig) -> dict:
    """Build deterministic per-character canonical templates."""
    templates = {}
    for char in sorted(set(characters)):
        grid = glyph_grid(char)
        templates[char] = {
            "grid": grid,
            "grid_digest": digest_json(grid.tolist())[:12],
        }
    return {
        "size": len(templates),
        "characters": sorted(templates),
        "grids": {char: templates[char]["grid"].tolist() for char in sorted(templates)},
        "grid_digests": {char: templates[char]["grid_digest"] for char in sorted(templates)},
    }


def estimate_cell_width(binary: np.ndarray) -> int:
    """Estimate the glyph pitch from foreground column run lengths."""
    column_filled = binary.sum(axis=0) > 0
    runs: list[int] = []
    run = 0
    for filled in column_filled:
        if filled:
            run += 1
        elif run:
            runs.append(run)
            run = 0
    if run:
        runs.append(run)
    if not runs:
        return CELL_W
    median_run = float(np.median(runs))
    scale = max(1, int(round(median_run / 5.0)))
    return CELL_W * scale


def segment_cells(binary: np.ndarray) -> list[np.ndarray]:
    """Deterministically split a line crop into fixed-pitch character cells.

    Glyphs are rendered on a constant grid so the pitch is stable; a projection
    run count seeds the number of cells, then equal-width windows are aligned
    to the foreground bounding box and snapped to empty column boundaries.
    """
    if binary.size == 0:
        return []
    column_filled = binary.sum(axis=0) > 0
    filled_columns = np.flatnonzero(column_filled)
    if filled_columns.size == 0:
        return []
    left, right = int(filled_columns[0]), int(filled_columns[-1]) + 1
    pitch = estimate_cell_width(binary)
    count = max(1, int(round((right - left) / pitch)))
    cells: list[np.ndarray] = []
    for index in range(count):
        x0 = left + int(round(index * (right - left) / count))
        x1 = left + int(round((index + 1) * (right - left) / count))
        cell = binary[:, x0:x1]
        if cell.shape[1] > 0 and cell.sum() > 0:
            cells.append(cell)
    return cells


def _distance(feature_a: np.ndarray, feature_b: np.ndarray) -> float:
    # Normalized Hamming distance over the canonical pixel bag-of-words.
    return float(np.abs(feature_a - feature_b).sum() / max(1, feature_a.size))


def recognize_candidate(
    image,
    candidate: dict,
    library: dict,
    config: WorkbenchConfig,
) -> dict:
    """Recognize one detection candidate, retaining structured errors."""
    candidate_id = candidate["candidate_id"]
    if not library or library.get("size", 0) == 0:
        raise RecognizeInvalid(
            "template library is empty",
            {"candidate_id": candidate_id, "reason": "empty_library"},
        )
    gray = to_grayscale(image)
    mask = binarize(gray, config.binarize.threshold)
    box = candidate["pixel_box"]
    crop = mask[box["y0"] : box["y1"], box["x0"] : box["x1"]]
    crop = deshear(crop)
    if crop.shape[0] < 3 or crop.shape[1] < 3:
        raise RecognizeInvalid(
            "candidate size is incompatible with template size",
            {
                "candidate_id": candidate_id,
                "reason": "size_mismatch",
                "crop_shape": list(crop.shape),
                "template_shape": [CELL_H, CELL_W],
            },
        )
    raw_cells = segment_cells(crop)
    raw_cells = [cell for cell in raw_cells if cell.sum() > 0]
    if not raw_cells:
        raise RecognizeInvalid(
            "candidate crop contains no foreground pixels",
            {"candidate_id": candidate_id, "reason": "empty_crop"},
        )

    characters = library["characters"]
    library_grids = [np.asarray(library["grids"][char], dtype=np.uint8) for char in characters]
    distances: list[float] = []
    matched: list[str] = []
    per_cell: list[dict] = []
    for cell_index, raw_cell in enumerate(raw_cells):
        normalized = canonical_cell(raw_cell)
        feature = cell_feature(normalized)
        cell_distances = [_distance(feature, cell_feature(grid)) for grid in library_grids]
        if any(math.isnan(value) for value in cell_distances):
            raise RecognizeInvalid(
                "NaN encountered while matching candidate",
                {"candidate_id": candidate_id, "reason": "nan_distance", "cell_index": cell_index},
            )
        best_index = min(range(len(cell_distances)), key=lambda idx: (cell_distances[idx], characters[idx]))
        best_distance = cell_distances[best_index]
        matched.append(characters[best_index])
        distances.append(best_distance)
        per_cell.append(
            {
                "cell_index": cell_index,
                "matched": characters[best_index],
                "distance": round(best_distance, config.confidence_decimals),
            }
        )

    mean_distance = float(np.mean(distances))
    text = "".join(matched)
    if not text:
        raise RecognizeInvalid(
            "recognition produced an empty text",
            {"candidate_id": candidate_id, "reason": "empty_text"},
        )
    confidence = round(max(0.0, 1.0 - mean_distance), config.confidence_decimals)
    return {
        "candidate_id": candidate_id,
        "text": text,
        "confidence": confidence,
        "mean_distance": round(mean_distance, config.confidence_decimals),
        "cell_count": len(raw_cells),
        "template_count": library["size"],
        "per_cell": per_cell,
    }


def recognize_shard(image, detection_result: dict, library: dict, config: WorkbenchConfig) -> dict:
    """Recognize every candidate of a shard, collecting per-candidate errors."""
    results = []
    errors = []
    for candidate in detection_result["candidates"]:
        try:
            results.append(recognize_candidate(image, candidate, library, config))
        except RecognizeInvalid as exc:
            errors.append(exc.to_dict())
    return {
        "image_id": detection_result["image_id"],
        "template_count": library.get("size", 0),
        "recognized_count": len(results),
        "results": results,
        "errors": errors,
    }


def recognize_cost(strategy_id: str, candidate_count: int, template_count: int, cell_count: int) -> int:
    """Abstract deterministic recognition cost for budget accounting."""
    tier = {"recog_fast": 1, "recog_balanced": 2, "recog_deep": 4}[strategy_id]
    return tier * (4 + cell_count * (2 + template_count // 4) + 3 * candidate_count)


# Strategy-specific library subsampling keeps the three tiers genuinely
# different.  ``all_characters`` is ordered by descending truth frequency then
# code point, so cheap tiers keep only the most common glyphs.
def strategy_library_characters(all_characters: list[str], strategy_id: str) -> list[str]:
    ordered = list(dict.fromkeys(all_characters))
    total = len(ordered)
    if strategy_id == "recog_fast":
        return ordered[: max(10, total // 2)]
    if strategy_id == "recog_balanced":
        return ordered[: max(16, (total * 3) // 4)]
    return ordered


def truth_frequency_order(shards: list[dict]) -> list[str]:
    """Return manifest characters ordered by descending truth frequency."""
    counts: dict[str, int] = {}
    for shard in shards:
        for truth in shard["truth"]:
            for char in truth["text"]:
                counts[char] = counts.get(char, 0) + 1
    return sorted(counts, key=lambda char: (-counts[char], char))
