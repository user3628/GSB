"""Stage 2: per-dimension visual quality metrics."""

import numpy as np

from .errors import BusinessError, E_MISSING_REFERENCE
from .hashing import digest, round_floats
from .normalize import normalize, decode_png

STAGE = "stage2_metrics"


def _grayscale(pixels):
    return pixels @ np.array([0.299, 0.587, 0.114], dtype=np.float64)


def _laplacian(gray):
    interior = gray[1:-1, 1:-1]
    response = (
        gray[:-2, 1:-1]
        + gray[2:, 1:-1]
        + gray[1:-1, :-2]
        + gray[1:-1, 2:]
        - 4.0 * interior
    )
    return response


def sharpness(pixels):
    """Normalized variance of the Laplacian response."""
    gray = _grayscale(pixels)
    if gray.shape[0] < 3 or gray.shape[1] < 3:
        return {"score": 0.0, "valid_pixels": 0, "skipped_reason": "image smaller than 3x3"}
    response = _laplacian(gray)
    variance = float(response.var())
    score = float(variance / (1.0 + variance))
    return {
        "score": score,
        "valid_pixels": int(response.size),
        "skipped_reason": None,
    }


def _saturation(pixels):
    max_channel = pixels.max(axis=2)
    min_channel = pixels.min(axis=2)
    denom = np.maximum(max_channel, 1e-12)
    return (max_channel - min_channel) / denom


def color_no_reference(pixels):
    """Colour cast plus saturation spread when no reference exists."""
    means = pixels.reshape(-1, 3).mean(axis=0)
    cast = float(np.abs(means - means.mean()).mean())
    saturation = _saturation(pixels)
    spread = float(saturation.std())
    score = float(np.clip(1.0 - 0.5 * cast - 0.5 * spread, 0.0, 1.0))
    return {
        "score": score,
        "valid_pixels": int(pixels.shape[0] * pixels.shape[1]),
        "skipped_reason": None,
        "cast": cast,
        "saturation_spread": spread,
    }


def _histogram_distance(reference, candidate):
    total = 0.0
    bins = 8
    for channel in range(3):
        ref_hist, _ = np.histogram(reference[:, :, channel], bins=bins, range=(0.0, 1.0))
        cand_hist, _ = np.histogram(candidate[:, :, channel], bins=bins, range=(0.0, 1.0))
        ref_norm = ref_hist.astype(np.float64) / max(ref_hist.sum(), 1)
        cand_norm = cand_hist.astype(np.float64) / max(cand_hist.sum(), 1)
        total += float(0.5 * np.abs(ref_norm - cand_norm).sum())
    return total / 3.0


def color_with_reference(pixels, reference):
    """Reference-based colour histogram distance."""
    if reference.shape != pixels.shape:
        reference = np.array(
            [
                [
                    reference[
                        min(int(row * reference.shape[0] / pixels.shape[0]), reference.shape[0] - 1),
                        min(int(col * reference.shape[1] / pixels.shape[1]), reference.shape[1] - 1),
                    ]
                    for col in range(pixels.shape[1])
                ]
                for row in range(pixels.shape[0])
            ],
            dtype=np.float64,
        )
    distance = _histogram_distance(reference, pixels)
    return {
        "score": float(1.0 - distance),
        "valid_pixels": int(pixels.shape[0] * pixels.shape[1]),
        "skipped_reason": None,
        "histogram_distance": distance,
    }


def _gradient_angles(pixels):
    gray = _grayscale(pixels)
    gy = np.zeros_like(gray)
    gx = np.zeros_like(gray)
    gy[1:-1, :] = gray[2:, :] - gray[:-2, :]
    gx[:, 1:-1] = gray[:, 2:] - gray[:, :-2]
    return np.arctan2(gy, gx), (np.abs(gx) + np.abs(gy)) > 1e-6


def structure_fidelity(pixels, reference):
    """Local gradient-direction agreement against a reference image."""
    if reference.shape != pixels.shape:
        reference = np.array(
            [
                [
                    reference[
                        min(int(row * reference.shape[0] / pixels.shape[0]), reference.shape[0] - 1),
                        min(int(col * reference.shape[1] / pixels.shape[1]), reference.shape[1] - 1),
                    ]
                    for col in range(pixels.shape[1])
                ]
                for row in range(pixels.shape[0])
            ],
            dtype=np.float64,
        )
    angles_a, mask_a = _gradient_angles(pixels)
    angles_b, mask_b = _gradient_angles(reference)
    mask = mask_a & mask_b
    valid = int(mask.sum())
    if valid == 0:
        return {"score": 0.0, "valid_pixels": 0, "skipped_reason": "no salient gradient pixels"}
    diff = np.abs((angles_a - angles_b + np.pi) % (2 * np.pi) - np.pi)
    agreement = float(np.cos(diff[mask]).mean())
    score = float((agreement + 1.0) / 2.0)
    return {"score": score, "valid_pixels": valid, "skipped_reason": None}


def _unavailable(reason):
    return {"score": None, "valid_pixels": 0, "skipped_reason": reason}


def load_reference(reference_path, image_id):
    if not reference_path:
        return None
    import os

    if not os.path.exists(reference_path):
        raise BusinessError(
            E_MISSING_REFERENCE,
            f"declared reference file does not exist for {image_id}",
            {"image_id": image_id, "reference_path": reference_path},
        )
    return normalize(decode_png(reference_path))


def run(pixels, metadata, reference_path, injection=None):
    image_id = metadata["image_id"]
    effective_reference = reference_path
    if injection is not None and injection.kind == "missing_reference" and injection.targets(image_id):
        effective_reference = reference_path + ".missing"
    reference = load_reference(effective_reference, image_id)

    sharp = sharpness(pixels)
    if reference is None:
        color = color_no_reference(pixels)
        structure = _unavailable("no reference image declared; structure fidelity is not applicable")
        reference_fingerprint = None
    else:
        color = color_with_reference(pixels, reference)
        structure = structure_fidelity(pixels, reference)
        from .hashing import file_digest

        reference_fingerprint = file_digest(reference_path)

    product = {
        "sharpness": sharp,
        "color": color,
        "structure": structure,
        "reference_used": reference is not None,
        "reference_fingerprint": reference_fingerprint,
    }
    record = {
        "stage": STAGE,
        "image_id": image_id,
        "input_fingerprint": digest(
            {
                "pixels_digest": digest(pixels.reshape(-1).tolist()),
                "reference": reference_fingerprint,
            }
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
