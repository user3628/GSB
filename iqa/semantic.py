"""Stage 3: text-image prompt consistency."""

import numpy as np

from .hashing import digest, round_floats
from .vocab import analyze_prompt, vocab_version, PRIMITIVE_RGB, COLOR_PRIMITIVES, TEXTURE_PRIMITIVES

STAGE = "stage3_semantic"
UNKNOWN_NEUTRAL_SCORE = 0.5


def _color_affinity(pixels, primitive):
    target = np.array(PRIMITIVE_RGB[primitive], dtype=np.float64) / 255.0
    distance = np.abs(pixels - target).mean(axis=2)
    return float(np.clip(1.0 - distance.mean(), 0.0, 1.0))


def _texture_affinity(pixels, primitive):
    gray = pixels.mean(axis=2)
    if primitive == "smooth":
        score = 1.0 - float(gray.std())
    elif primitive == "gradient":
        column_means = gray.mean(axis=0)
        row_means = gray.mean(axis=1)
        variation = max(float(np.abs(np.diff(column_means)).mean()), float(np.abs(np.diff(row_means)).mean()))
        score = np.clip(variation * 8.0, 0.0, 1.0)
    elif primitive == "stripes":
        column_means = gray.mean(axis=0)
        if column_means.size < 2:
            score = 0.0
        else:
            score = np.clip(float(np.abs(np.diff(column_means)).mean()) * 12.0, 0.0, 1.0)
    elif primitive == "grid":
        column_means = gray.mean(axis=0)
        row_means = gray.mean(axis=1)
        horizontal = float(np.abs(np.diff(column_means)).mean()) if column_means.size > 1 else 0.0
        vertical = float(np.abs(np.diff(row_means)).mean()) if row_means.size > 1 else 0.0
        score = np.clip((horizontal + vertical) * 6.0, 0.0, 1.0)
    elif primitive == "noise":
        diff = np.abs(np.diff(gray, axis=1))
        score = np.clip(float(diff.mean()) * 8.0, 0.0, 1.0)
    elif primitive == "blocks":
        h, w = gray.shape
        regions = []
        for top in range(0, h, max(h // 4, 1)):
            for left in range(0, w, max(w // 4, 1)):
                regions.append(gray[top:top + max(h // 4, 1), left:left + max(w // 4, 1)].mean())
        score = np.clip(float(np.std(regions)) * 4.0, 0.0, 1.0)
    else:
        score = UNKNOWN_NEUTRAL_SCORE
    return float(np.clip(score, 0.0, 1.0))


def run(pixels, metadata):
    image_id = metadata["image_id"]
    prompt = metadata["prompt"]
    analysis = analyze_prompt(prompt)
    token_results = []
    for mapping in analysis["tokens"]:
        primitive = mapping["primitive"]
        if mapping["kind"] == "color":
            if primitive in COLOR_PRIMITIVES:
                affinity = _color_affinity(pixels, primitive)
            else:
                affinity = UNKNOWN_NEUTRAL_SCORE
        else:
            if primitive in TEXTURE_PRIMITIVES:
                affinity = _texture_affinity(pixels, primitive)
            else:
                affinity = UNKNOWN_NEUTRAL_SCORE
        token_results.append({**mapping, "affinity": affinity})

    if token_results:
        score = float(np.mean([item["affinity"] for item in token_results]))
    else:
        score = UNKNOWN_NEUTRAL_SCORE
    score = float(np.clip(score, 0.0, 1.0))

    product = {
        "normalized_prompt": analysis["normalized"],
        "token_count": analysis["token_count"],
        "fallback_count": analysis["fallback_count"],
        "tokens": token_results,
        "score": score,
        "valid_pixels": int(pixels.shape[0] * pixels.shape[1]),
        "skipped_reason": None,
        "vocab_version": vocab_version(),
    }
    record = {
        "stage": STAGE,
        "image_id": image_id,
        "input_fingerprint": digest(
            {"prompt": analysis["normalized"], "vocab_version": vocab_version()}
        ),
        "product": product,
    }
    product = round_floats(product)
    record["product"] = product
    record["product_digest"] = digest(product)
    record["fingerprint"] = digest(
        {"input": record["input_fingerprint"], "product": record["product_digest"]}
    )
    return record
