from __future__ import annotations

import base64
import copy
import json
import re
from dataclasses import dataclass, replace
from typing import Any

import numpy as np

from . import CODEC_VERSION, SCHEMA_VERSION, __version__
from .errors import E_NUMERIC, E_VALIDATION, WorkbenchError
from .hashing import canonical_json, digest_array
from .masks import MaskRegion

FRAGMENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
OUTPUT_FORMATS = {"png", "jpeg", "json"}


def default_metadata(
    source_format: str = "internal",
    *,
    source_schema_version: int = SCHEMA_VERSION,
    **extra: Any,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "codec_version": CODEC_VERSION,
        "workbench_version": __version__,
        "source_schema_version": source_schema_version,
        "source_format": source_format,
    }
    metadata.update(extra)
    return metadata


def _validate_metadata(metadata: dict[str, Any]) -> None:
    if not isinstance(metadata, dict):
        raise WorkbenchError(E_VALIDATION, "Version metadata must be an object")
    if not isinstance(metadata.get("schema_version"), int):
        raise WorkbenchError(E_VALIDATION, "Version metadata must contain integer schema_version")
    try:
        canonical_json(metadata)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(
            E_NUMERIC,
            "Version metadata must be finite, JSON-compatible data",
            pixel=[0, 0],
            details={"field": "metadata"},
        ) from exc


def _validate_pixels(pixels: np.ndarray, fragment_id: str | None = None) -> None:
    if not isinstance(pixels, np.ndarray):
        raise WorkbenchError(E_VALIDATION, "Pixels must be a NumPy array", fragment_id=fragment_id)
    if pixels.ndim != 3 or pixels.shape[2] != 3:
        raise WorkbenchError(
            E_VALIDATION,
            "Pixels must have shape (height, width, 3)",
            fragment_id=fragment_id,
            details={"shape": list(pixels.shape)},
        )
    if pixels.dtype != np.uint8:
        if np.issubdtype(pixels.dtype, np.floating):
            finite = np.isfinite(pixels)
            if not finite.all():
                y, x, _ = np.argwhere(~finite)[0].tolist()
                raise WorkbenchError(
                    E_NUMERIC,
                    "Pixel matrix contains NaN or Inf",
                    fragment_id=fragment_id,
                    pixel=[int(x), int(y)],
                    details={"field": "pixels"},
                )
        raise WorkbenchError(
            E_VALIDATION,
            "Pixel matrix must use uint8 channel values",
            fragment_id=fragment_id,
            details={"dtype": str(pixels.dtype)},
        )


@dataclass
class FragmentModel:
    fragment_id: str
    pixels: np.ndarray
    mask: MaskRegion
    style_reference: str
    style_strength: float
    repair_iterations: int
    output_format: str
    metadata: dict[str, Any]
    loss_items: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not isinstance(self.fragment_id, str) or not FRAGMENT_ID_RE.fullmatch(self.fragment_id):
            raise WorkbenchError(
                E_VALIDATION,
                "fragment_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
                fragment_id=str(self.fragment_id),
            )
        _validate_pixels(self.pixels, self.fragment_id)
        if not isinstance(self.mask, MaskRegion):
            raise WorkbenchError(E_VALIDATION, "mask must be a MaskRegion", fragment_id=self.fragment_id)
        if not isinstance(self.style_reference, str) or not self.style_reference:
            raise WorkbenchError(E_VALIDATION, "style_reference must be a non-empty string", fragment_id=self.fragment_id)
        try:
            strength = float(self.style_strength)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_NUMERIC,
                "Style strength must be numeric",
                fragment_id=self.fragment_id,
                pixel=[0, 0],
                details={"field": "style_strength"},
            ) from exc
        if not np.isfinite(strength) or strength < 0.0 or strength > 1.0:
            raise WorkbenchError(
                E_NUMERIC,
                "Style strength must be finite and within [0, 1]",
                fragment_id=self.fragment_id,
                pixel=[0, 0],
                details={"field": "style_strength", "value": repr(self.style_strength)},
            )
        self.style_strength = strength
        if isinstance(self.repair_iterations, bool) or not isinstance(self.repair_iterations, int):
            raise WorkbenchError(E_VALIDATION, "repair_iterations must be an integer", fragment_id=self.fragment_id)
        if self.repair_iterations < 0 or self.repair_iterations > 10_000:
            raise WorkbenchError(
                E_VALIDATION,
                "repair_iterations must be within [0, 10000]",
                fragment_id=self.fragment_id,
                details={"value": self.repair_iterations},
            )
        normalized_format = str(self.output_format).lower()
        if normalized_format == "jpg":
            normalized_format = "jpeg"
        if normalized_format not in OUTPUT_FORMATS:
            raise WorkbenchError(
                E_VALIDATION,
                "output_format must be png, jpeg or json",
                fragment_id=self.fragment_id,
                details={"value": self.output_format},
            )
        self.output_format = normalized_format
        _validate_metadata(self.metadata)
        self.metadata = copy.deepcopy(self.metadata)
        if self.loss_items is None:
            self.loss_items = []
        if not isinstance(self.loss_items, list) or not all(isinstance(item, dict) for item in self.loss_items):
            raise WorkbenchError(E_VALIDATION, "loss_items must be a list of objects", fragment_id=self.fragment_id)
        try:
            canonical_json(self.loss_items)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_NUMERIC,
                "loss_items must be finite, JSON-compatible data",
                fragment_id=self.fragment_id,
                pixel=[0, 0],
            ) from exc
        self.loss_items = copy.deepcopy(self.loss_items)
        self.pixels = np.array(self.pixels, dtype=np.uint8, copy=True, order="C")

    @property
    def width(self) -> int:
        return int(self.pixels.shape[1])

    @property
    def height(self) -> int:
        return int(self.pixels.shape[0])

    @property
    def pixel_digest(self) -> str:
        return digest_array(self.pixels, role="rgb")

    @property
    def mask_digest(self) -> str:
        return self.mask.digest(self.width, self.height)

    @property
    def mask_pixels(self) -> np.ndarray:
        return self.mask.rasterize(self.width, self.height)

    def with_pixels(
        self,
        pixels: np.ndarray,
        *,
        metadata: dict[str, Any] | None = None,
        loss_items: list[dict[str, Any]] | None = None,
    ) -> FragmentModel:
        return replace(
            self,
            pixels=np.array(pixels, dtype=np.uint8, copy=True, order="C"),
            metadata=copy.deepcopy(self.metadata if metadata is None else metadata),
            loss_items=copy.deepcopy(self.loss_items if loss_items is None else loss_items),
        )

    def semantic_dict(self, *, include_pixel_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "fragment_id": self.fragment_id,
            "width": self.width,
            "height": self.height,
            "mask_rects": self.mask.to_lists(),
            "mask_pixel_digest": self.mask_digest,
            "style_reference": self.style_reference,
            "style_strength": self.style_strength,
            "repair_iterations": self.repair_iterations,
            "output_format": self.output_format,
            "metadata": copy.deepcopy(self.metadata),
            "loss_items": copy.deepcopy(self.loss_items),
        }
        if include_pixel_hash:
            result["pixel_digest"] = self.pixel_digest
        return result

    def to_internal_dict(self) -> dict[str, Any]:
        """Serialize the lossless internal project representation."""
        return {
            "fragment_id": self.fragment_id,
            "pixels": {
                "encoding": "base64-raw-rgb-u8",
                "width": self.width,
                "height": self.height,
                "channels": 3,
                "dtype": "uint8",
                "data": base64.b64encode(np.ascontiguousarray(self.pixels).tobytes()).decode("ascii"),
                "sha256": self.pixel_digest,
            },
            "mask": {
                "coordinate_space": "normalized-xyxy-half-open",
                "rects": self.mask.to_lists(),
                "pixel_sha256": self.mask_digest,
            },
            "style_reference": self.style_reference,
            "style_strength": self.style_strength,
            "repair_iterations": self.repair_iterations,
            "output_format": self.output_format,
            "metadata": copy.deepcopy(self.metadata),
            "loss_items": copy.deepcopy(self.loss_items),
        }

    @classmethod
    def from_internal_dict(cls, value: dict[str, Any]) -> FragmentModel:
        try:
            pixel_info = value["pixels"]
            width = int(pixel_info["width"])
            height = int(pixel_info["height"])
            if pixel_info.get("encoding") != "base64-raw-rgb-u8" or int(pixel_info.get("channels", 0)) != 3:
                raise KeyError("encoding")
            raw = base64.b64decode(pixel_info["data"], validate=True)
            expected = width * height * 3
            if len(raw) != expected:
                raise WorkbenchError(
                    E_VALIDATION,
                    "Internal pixel payload length does not match dimensions",
                    fragment_id=value.get("fragment_id"),
                    details={"expected": expected, "actual": len(raw)},
                )
            pixels = np.frombuffer(raw, dtype=np.uint8).reshape(height, width, 3).copy()
            declared = pixel_info.get("sha256")
            actual = digest_array(pixels, role="rgb")
            if declared is not None and declared != actual:
                raise WorkbenchError(
                    E_VALIDATION,
                    "Internal pixel payload digest mismatch",
                    fragment_id=value.get("fragment_id"),
                    details={"declared": declared, "actual": actual},
                )
            mask_info = value.get("mask", {})
            mask = MaskRegion.from_dict(mask_info.get("rects", []))
            model = cls(
                fragment_id=str(value["fragment_id"]),
                pixels=pixels,
                mask=mask,
                style_reference=str(value["style_reference"]),
                style_strength=float(value["style_strength"]),
                repair_iterations=int(value["repair_iterations"]),
                output_format=str(value.get("output_format", "png")),
                metadata=copy.deepcopy(value["metadata"]),
                loss_items=copy.deepcopy(value.get("loss_items", [])),
            )
            declared_mask = mask_info.get("pixel_sha256")
            if declared_mask is not None and declared_mask != model.mask_digest:
                raise WorkbenchError(
                    E_VALIDATION,
                    "Internal mask payload digest mismatch",
                    fragment_id=model.fragment_id,
                    details={"declared": declared_mask, "actual": model.mask_digest},
                )
            return model
        except WorkbenchError:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WorkbenchError(
                E_VALIDATION,
                f"Invalid internal fragment structure: {exc}",
                fragment_id=str(value.get("fragment_id")) if isinstance(value, dict) else None,
            ) from exc


def same_model_semantics(left: FragmentModel, right: FragmentModel, *, compare_pixels: bool = True) -> bool:
    return left.semantic_dict(include_pixel_hash=compare_pixels) == right.semantic_dict(include_pixel_hash=compare_pixels)

