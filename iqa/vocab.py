"""Primitive vocabulary and deterministic prompt token mapping."""

import re
import unicodedata

from .hashing import fnv1a64, digest

COLOR_PRIMITIVES = ("red", "green", "blue", "cyan", "magenta", "yellow", "white", "black")
TEXTURE_PRIMITIVES = ("smooth", "stripes", "grid", "noise", "gradient", "blocks")
UNKNOWN_PRIMITIVE = "unknown"

# Explicit bilingual lexicon. Tokens not present here fall back to the
# explicit unknown primitive and are counted as fallbacks.
KNOWN = {
    "红": "red", "红色": "red", "red": "red",
    "绿": "green", "绿色": "green", "green": "green",
    "蓝": "blue", "蓝色": "blue", "blue": "blue",
    "青": "cyan", "cyan": "cyan",
    "紫": "magenta", "品红": "magenta", "magenta": "magenta",
    "黄": "yellow", "黄色": "yellow", "yellow": "yellow",
    "白": "white", "白色": "white", "white": "white",
    "黑": "black", "黑色": "black", "black": "black",
    "平滑": "smooth", "光滑": "smooth", "smooth": "smooth",
    "条纹": "stripes", "stripe": "stripes", "stripes": "stripes",
    "网格": "grid", "grid": "grid",
    "噪声": "noise", "噪音": "noise", "noise": "noise",
    "渐变": "gradient", "gradient": "gradient",
    "色块": "blocks", "几何": "blocks", "block": "blocks", "blocks": "blocks",
}

# Representative RGB value for each colour primitive (0-255 scale).
PRIMITIVE_RGB = {
    "red": (255, 0, 0),
    "green": (0, 255, 0),
    "blue": (0, 0, 255),
    "cyan": (0, 255, 255),
    "magenta": (255, 0, 255),
    "yellow": (255, 255, 0),
    "white": (255, 255, 255),
    "black": (0, 0, 0),
    "unknown": (128, 128, 128),
}

_TOKEN_RE = re.compile(r"[A-Za-z]+|[\u4e00-\u9fff]")


def normalize_prompt(text):
    """NFKC normalization, full/half-width unification and whitespace folding."""
    normalized = unicodedata.normalize("NFKC", str(text))
    return " ".join(normalized.split())


def tokenize(text):
    """Split into Latin runs and individual CJK ideographs."""
    return _TOKEN_RE.findall(normalize_prompt(text))


def map_token(token):
    """Map a token to (kind, primitive, fell_back).

    Known lexicon entries map directly. Any other token is hashed with FNV-1a
    to determine its primitive kind and then mapped to the explicit unknown
    primitive, recording the fallback.
    """
    lowered = token.lower()
    if lowered in KNOWN:
        primitive = KNOWN[lowered]
        kind = "color" if primitive in COLOR_PRIMITIVES else "texture"
        return kind, primitive, False
    kind = "color" if fnv1a64(lowered) % 2 == 0 else "texture"
    return kind, UNKNOWN_PRIMITIVE, True


def analyze_prompt(text):
    tokens = tokenize(text)
    mappings = []
    fallback_count = 0
    for token in tokens:
        kind, primitive, fell_back = map_token(token)
        if fell_back:
            fallback_count += 1
        mappings.append(
            {"token": token, "kind": kind, "primitive": primitive, "fallback": fell_back}
        )
    return {
        "normalized": normalize_prompt(text),
        "tokens": mappings,
        "token_count": len(mappings),
        "fallback_count": fallback_count,
    }


def vocab_version():
    """Hash-words-table fingerprint."""
    return digest(
        {
            "colors": list(COLOR_PRIMITIVES),
            "textures": list(TEXTURE_PRIMITIVES),
            "known": KNOWN,
            "unknown": UNKNOWN_PRIMITIVE,
        }
    )[0:16]
