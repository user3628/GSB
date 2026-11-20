"""Self-contained deterministic glyph rasterizer.

No system fonts are used: Latin letters and digits come from an embedded 5x7
bitmap font while every other character (notably CJK) gets a stable bit
pattern derived from its code point.  The same character therefore always
rasterizes to the same pixels on every machine, which is what the hash
bag-of-words recognizer relies on.
"""

from __future__ import annotations

import numpy as np
from PIL import Image

CELL_W = 7
CELL_H = 9

# Classic 5x7 bitmap font, column-major, bit 0 = top row.
FONT_5X7 = {
    "0": (0x3E, 0x51, 0x49, 0x45, 0x3E),
    "1": (0x00, 0x42, 0x7F, 0x40, 0x00),
    "2": (0x42, 0x61, 0x51, 0x49, 0x46),
    "3": (0x21, 0x41, 0x45, 0x4B, 0x31),
    "4": (0x18, 0x14, 0x12, 0x7F, 0x10),
    "5": (0x27, 0x45, 0x45, 0x45, 0x39),
    "6": (0x3C, 0x4A, 0x49, 0x49, 0x30),
    "7": (0x01, 0x71, 0x09, 0x05, 0x03),
    "8": (0x36, 0x49, 0x49, 0x49, 0x36),
    "9": (0x06, 0x49, 0x49, 0x29, 0x1E),
    "A": (0x7E, 0x11, 0x11, 0x11, 0x7E),
    "B": (0x7F, 0x49, 0x49, 0x49, 0x36),
    "C": (0x3E, 0x41, 0x41, 0x41, 0x22),
    "D": (0x7F, 0x41, 0x41, 0x22, 0x1C),
    "E": (0x7F, 0x49, 0x49, 0x49, 0x41),
    "F": (0x7F, 0x09, 0x09, 0x09, 0x01),
    "G": (0x3E, 0x41, 0x49, 0x49, 0x7A),
    "H": (0x7F, 0x08, 0x08, 0x08, 0x7F),
    "I": (0x00, 0x41, 0x7F, 0x41, 0x00),
    "J": (0x20, 0x40, 0x41, 0x3F, 0x01),
    "K": (0x7F, 0x08, 0x14, 0x22, 0x41),
    "L": (0x7F, 0x40, 0x40, 0x40, 0x40),
    "M": (0x7F, 0x02, 0x0C, 0x02, 0x7F),
    "N": (0x7F, 0x04, 0x08, 0x10, 0x7F),
    "O": (0x3E, 0x41, 0x41, 0x41, 0x3E),
    "P": (0x7F, 0x09, 0x09, 0x09, 0x06),
    "Q": (0x3E, 0x41, 0x51, 0x21, 0x5E),
    "R": (0x7F, 0x11, 0x31, 0x51, 0x0E),
    "S": (0x46, 0x49, 0x49, 0x49, 0x31),
    "T": (0x01, 0x01, 0x7F, 0x01, 0x01),
    "U": (0x3F, 0x40, 0x40, 0x40, 0x3F),
    "V": (0x1F, 0x20, 0x40, 0x20, 0x1F),
    "W": (0x3F, 0x40, 0x38, 0x40, 0x3F),
    "X": (0x63, 0x14, 0x08, 0x14, 0x63),
    "Y": (0x07, 0x08, 0x70, 0x08, 0x07),
    "Z": (0x61, 0x51, 0x49, 0x45, 0x43),
    "-": (0x08, 0x08, 0x08, 0x08, 0x08),
    ".": (0x00, 0x60, 0x60, 0x00, 0x00),
    ",": (0x00, 0x40, 0x60, 0x20, 0x00),
    ":": (0x00, 0x36, 0x36, 0x00, 0x00),
    "!": (0x00, 0x00, 0x5F, 0x00, 0x00),
    "?": (0x02, 0x01, 0x51, 0x09, 0x06),
    " ": (0x00, 0x00, 0x00, 0x00, 0x00),
}


def _splitmix32(value: int) -> int:
    value = (value + 0x9E3779B9) & 0xFFFFFFFF
    value = ((value ^ (value >> 16)) * 0x85EBCA6B) & 0xFFFFFFFF
    value = ((value ^ (value >> 13)) * 0xC2B2AE35) & 0xFFFFFFFF
    return value ^ (value >> 16)


def _fallback_grid(char: str) -> np.ndarray:
    """Stable 5x7 pattern for characters absent from the embedded font."""
    seed = ord(char)
    pattern = np.zeros((7, 5), dtype=np.uint8)
    for row in range(7):
        bits = _splitmix32(seed + row * 97)
        for col in range(5):
            pattern[row, col] = 1 if (bits >> (col * 5 + 1)) & 1 else 0
    # Guarantee a legible, non-empty foreground core for every glyph.
    pattern[1:6, 1:4] |= 1
    pattern[3, :] = 1
    return pattern


def glyph_grid(char: str) -> np.ndarray:
    """Return the canonical 9x7 binary grid for ``char`` (foreground=1)."""
    key = char.upper() if char and char[0].isalpha() else char
    columns = FONT_5X7.get(key)
    if columns is None:
        small = _fallback_grid(char)
    else:
        small = np.zeros((7, 5), dtype=np.uint8)
        for col, byte in enumerate(columns):
            for row in range(7):
                small[row, col] = (byte >> row) & 1
    grid = np.zeros((CELL_H, CELL_W), dtype=np.uint8)
    grid[1:8, 1:6] = small
    return grid


def rasterize_line(text: str, scale: int) -> np.ndarray:
    """Rasterize a whole text line into a binary image (foreground=1)."""
    scale = max(1, int(scale))
    line = np.zeros((CELL_H * scale, CELL_W * scale * len(text)), dtype=np.uint8)
    for index, char in enumerate(text):
        grid = glyph_grid(char)
        scaled = np.kron(grid, np.ones((scale, scale), dtype=np.uint8))
        x0 = index * CELL_W * scale
        line[:, x0 : x0 + CELL_W * scale] = scaled
    return line


def shear_line(binary: np.ndarray, shear_hundredths: int) -> np.ndarray:
    """Apply a deterministic integer vertical shear (columns keep their data)."""
    height, width = binary.shape
    if shear_hundredths == 0 or width == 0:
        return binary.copy()
    shifts = [int(round(shear_hundredths * x / 100.0)) for x in range(width)]
    shift_min, shift_max = min(shifts), max(shifts)
    extra = shift_max - shift_min
    shifted = np.zeros((height + extra, width), dtype=np.uint8)
    for x in range(width):
        offset = shifts[x] - shift_min
        column = binary[:, x]
        shifted[offset : offset + height, x] = column
    return shifted


def render_shard(
    width: int,
    height: int,
    lines: list[dict],
    background=(245, 245, 245),
    text_color=(30, 30, 30),
) -> tuple[Image.Image, list[dict]]:
    """Render an RGB shard with colored line bands.

    ``lines`` is a list of ``{"text", "scale", "tilt", "band_color", "x", "y"}``
    records.  Returns the PIL image plus exact pixel boxes for every line:
    ``{"x0", "y0", "x1", "y1", "text"}``.
    """
    image = Image.new("RGB", (width, height), tuple(background))
    pixels = image.load()
    boxes: list[dict] = []
    for spec in lines:
        binary = rasterize_line(spec["text"], spec["scale"])
        binary = shear_line(binary, spec.get("tilt", 0))
        line_h, line_w = binary.shape
        x0, y0 = spec["x"], spec["y"]
        band = tuple(spec["band_color"])
        for y in range(y0, min(height, y0 + line_h)):
            for x in range(x0, min(width, x0 + line_w)):
                pixels[x, y] = band
        ys, xs = np.nonzero(binary)
        for dy, dx in zip(ys.tolist(), xs.tolist()):
            x, y = x0 + dx, y0 + dy
            if 0 <= x < width and 0 <= y < height:
                pixels[x, y] = tuple(text_color)
        boxes.append(
            {"x0": x0, "y0": y0, "x1": min(width, x0 + line_w), "y1": min(height, y0 + line_h), "text": spec["text"]}
        )
    return image, boxes


def to_grayscale(image: Image.Image) -> np.ndarray:
    return np.asarray(image.convert("L"), dtype=np.uint8)


def binarize(gray: np.ndarray, threshold: int) -> np.ndarray:
    """Fixed-threshold binarization returning foreground=1 as uint8."""
    return (gray < threshold).astype(np.uint8)


def canonical_cell(binary: np.ndarray) -> np.ndarray:
    """Aspect-preserving normalization onto the canonical 7x9 canvas.

    The foreground bounding box is cropped, uniformly down/up-scaled with
    nearest sampling and centered, so padding and scale never shift glyphs.
    """
    if binary.size == 0 or binary.sum() == 0:
        return np.zeros((CELL_H, CELL_W), dtype=np.uint8)
    rows, cols = np.nonzero(binary)
    cropped = binary[rows.min() : rows.max() + 1, cols.min() : cols.max() + 1]
    height, width = cropped.shape
    scale = min((CELL_H - 2) / height, (CELL_W - 2) / width)
    new_h = max(1, int(round(height * scale)))
    new_w = max(1, int(round(width * scale)))
    image = Image.fromarray((cropped * 255).astype(np.uint8), mode="L")
    resized = image.resize((new_w, new_h), Image.Resampling.NEAREST)
    array = (np.asarray(resized, dtype=np.uint8) > 0).astype(np.uint8)
    canvas = np.zeros((CELL_H, CELL_W), dtype=np.uint8)
    y0 = (CELL_H - new_h) // 2
    x0 = (CELL_W - new_w) // 2
    canvas[y0 : y0 + new_h, x0 : x0 + new_w] = array
    return canvas


def deshear(binary: np.ndarray) -> np.ndarray:
    """Remove a linear vertical shear estimated from per-column centers."""
    height, width = binary.shape
    if width < 2 or height == 0:
        return binary.copy()
    centers = np.full(width, np.nan, dtype=np.float64)
    for x in range(width):
        rows = np.nonzero(binary[:, x])[0]
        if rows.size:
            centers[x] = rows.mean()
    valid = ~np.isnan(centers)
    valid_x = np.nonzero(valid)[0]
    if valid_x.size < 2:
        return binary.copy()
    slope = float(np.polyfit(valid_x, centers[valid], 1)[0])
    if abs(slope) < 1e-9:
        return binary.copy()
    corrected = np.zeros_like(binary)
    for x in range(width):
        shift = int(round(slope * x))
        source_lo = max(0, shift)
        source_hi = min(height, height + shift)
        target_lo = max(0, -shift)
        target_hi = min(height, height - shift)
        corrected[target_lo:target_hi, x] = binary[source_lo:source_hi, x]
    return corrected
