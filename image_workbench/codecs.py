from __future__ import annotations

import copy
import io
import json
from typing import Any

import numpy as np
from PIL import Image, PngImagePlugin

from .errors import E_FORMAT_LOSS, E_NUMERIC, E_VALIDATION, WorkbenchError
from .hashing import canonical_json, digest_array, digest_json
from .masks import MaskRegion
from .model import FragmentModel

DESCRIPTOR_KIND = "image-workbench-descriptor"
DESCRIPTOR_VERSION = 1
PNG_DESCRIPTOR_KEY = "iwb:descriptor"


def loss_item(code: str, field: str, message: str, **extra: Any) -> dict[str, Any]:
    item = {
        "code": code,
        "stage": "format-export",
        "field": field,
        "message": message,
        "severity": "explicit",
    }
    item.update(extra)
    return item


def _jpeg_loss_items() -> list[dict[str, Any]]:
    return [
        loss_item(
            "jpeg_alpha_channel_dropped",
            "alpha",
            "JPEG has no alpha channel; the repair mask is reconstructed from the companion JSON descriptor.",
        ),
        loss_item(
            "jpeg_metadata_externalized",
            "metadata",
            "Version metadata is kept in the companion JSON descriptor rather than the JPEG bitstream.",
        ),
        loss_item(
            "jpeg_pixel_quantization",
            "pixels",
            "JPEG colour encoding is lossy; imported RGB values can differ from the internal source matrix.",
        ),
    ]


def descriptor_for_model(
    model: FragmentModel,
    image_format: str,
    *,
    capabilities: dict[str, Any] | None = None,
    loss_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    image_format = image_format.lower()
    if image_format == "jpg":
        image_format = "jpeg"
    if image_format not in {"png", "jpeg"}:
        raise WorkbenchError(E_VALIDATION, "Descriptor image_format must be png or jpeg")
    losses = copy.deepcopy(model.loss_items if loss_items is None else loss_items)
    caps = capabilities or {
        "alpha": image_format == "png",
        "lossless_pixels": image_format == "png",
        "embedded_metadata": image_format == "png",
        "external_mask_descriptor": image_format == "jpeg",
    }
    descriptor = {
        "kind": DESCRIPTOR_KIND,
        "descriptor_version": DESCRIPTOR_VERSION,
        "image": {
            "format": image_format,
            "width": model.width,
            "height": model.height,
            "source_pixel_digest": model.pixel_digest,
        },
        "fragment": {
            "id": model.fragment_id,
            "mask": {
                "coordinate_space": "normalized-xyxy-half-open",
                "rects": model.mask.to_lists(),
                "pixel_sha256": model.mask_digest,
            },
            "style_reference": model.style_reference,
            "style_strength": model.style_strength,
            "repair_iterations": model.repair_iterations,
            "output_format": model.output_format,
            "metadata": copy.deepcopy(model.metadata),
            "loss_items": losses,
        },
        "capabilities": caps,
        "loss_items": copy.deepcopy(losses),
    }
    descriptor["descriptor_digest"] = digest_json(
        {key: value for key, value in descriptor.items() if key != "descriptor_digest"}
    )
    return descriptor


def descriptor_bytes(descriptor: dict[str, Any], *, pretty: bool = True) -> bytes:
    if pretty:
        return (json.dumps(descriptor, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    return canonical_json(descriptor).encode("utf-8")


def parse_descriptor(descriptor: dict[str, Any] | bytes | str) -> dict[str, Any]:
    def reject_constant(value: str) -> Any:
        raise WorkbenchError(
            E_NUMERIC,
            "JSON descriptor contains NaN or Inf",
            pixel=[0, 0],
            details={"field": "descriptor", "value": value},
        )

    if isinstance(descriptor, bytes):
        try:
            descriptor = json.loads(descriptor.decode("utf-8"), parse_constant=reject_constant)
        except WorkbenchError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkbenchError(E_FORMAT_LOSS, "JSON descriptor is not valid UTF-8 JSON", details={"lost_items": ["descriptor"]}) from exc
    elif isinstance(descriptor, str):
        try:
            descriptor = json.loads(descriptor, parse_constant=reject_constant)
        except WorkbenchError:
            raise
        except json.JSONDecodeError as exc:
            raise WorkbenchError(E_FORMAT_LOSS, "JSON descriptor is invalid", details={"lost_items": ["descriptor"]}) from exc
    if not isinstance(descriptor, dict):
        raise WorkbenchError(E_FORMAT_LOSS, "JSON descriptor root must be an object", details={"lost_items": ["descriptor"]})
    if descriptor.get("kind") != DESCRIPTOR_KIND:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "JSON descriptor kind is not supported",
            details={"lost_items": ["descriptor_kind"], "actual": descriptor.get("kind")},
        )
    declared = descriptor.get("descriptor_digest")
    if declared is not None:
        try:
            actual = digest_json({key: value for key, value in descriptor.items() if key != "descriptor_digest"})
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_NUMERIC,
                "JSON descriptor contains non-finite or non-JSON data",
                fragment_id=str(descriptor.get("fragment", {}).get("id")) or None,
                pixel=[0, 0],
                details={"field": "descriptor"},
            ) from exc
        if actual != declared:
            raise WorkbenchError(
                E_FORMAT_LOSS,
                "JSON descriptor digest does not match its content",
                details={"lost_items": ["descriptor_integrity"], "declared": declared, "actual": actual},
            )
    return descriptor


def encode_png(
    model: FragmentModel,
    *,
    embed_descriptor: bool = True,
    compress_level: int = 6,
) -> tuple[bytes, dict[str, Any]]:
    """Encode lossless RGB pixels and use alpha=0 for the complete repair mask."""
    if int(compress_level) not in range(10):
        raise WorkbenchError(E_VALIDATION, "PNG compress_level must be within [0, 9]")
    descriptor = descriptor_for_model(model, "png", loss_items=model.loss_items)
    mask = model.mask.rasterize(model.width, model.height)
    alpha = np.where(mask, 0, 255).astype(np.uint8)
    rgba = np.dstack([model.pixels, alpha])
    image = Image.fromarray(rgba, mode="RGBA")
    output = io.BytesIO()
    info = PngImagePlugin.PngInfo()
    if embed_descriptor:
        info.add_text(PNG_DESCRIPTOR_KEY, canonical_json(descriptor), zip=False)
    image.save(output, format="PNG", pnginfo=info, optimize=False, compress_level=int(compress_level))
    return output.getvalue(), descriptor


def _decode_rgb(image_bytes: bytes, *, expected_format: str) -> tuple[np.ndarray, np.ndarray | None, dict[str, Any]]:
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image.load()
            actual_format = (image.format or "").lower()
            if expected_format == "png" and actual_format != "png":
                raise WorkbenchError(
                    E_FORMAT_LOSS,
                    "Image payload is not PNG",
                    details={"lost_items": ["image_format"], "actual": actual_format},
                )
            if expected_format == "jpeg" and actual_format not in {"jpeg", "jpg"}:
                raise WorkbenchError(
                    E_FORMAT_LOSS,
                    "Image payload is not JPEG",
                    details={"lost_items": ["image_format"], "actual": actual_format},
                )
            info = dict(image.info)
            if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
                rgba = image.convert("RGBA")
                array = np.asarray(rgba, dtype=np.uint8)
                return array[:, :, :3].copy(), array[:, :, 3].copy(), info
            rgb = image.convert("RGB")
            array = np.asarray(rgb, dtype=np.uint8)
            return array.copy(), None, info
    except WorkbenchError:
        raise
    except Exception as exc:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            f"Unable to decode {expected_format.upper()} image: {exc}",
            details={"lost_items": ["image_payload"]},
        ) from exc


def decode_png(
    image_bytes: bytes,
    descriptor: dict[str, Any] | bytes | str | None = None,
    *,
    require_descriptor: bool = True,
) -> FragmentModel:
    pixels, alpha, info = _decode_rgb(image_bytes, expected_format="png")
    descriptor_value = descriptor
    if descriptor_value is None:
        descriptor_value = info.get(PNG_DESCRIPTOR_KEY)
    if descriptor_value is None:
        if require_descriptor:
            raise WorkbenchError(
                E_FORMAT_LOSS,
                "PNG metadata descriptor is missing; style, iterations and version metadata cannot be recovered",
                details={"lost_items": ["fragment_id", "style_strength", "repair_iterations", "version_metadata"]},
            )
        inferred = MaskRegion.from_alpha(alpha if alpha is not None else np.full(pixels.shape[:2], 255, dtype=np.uint8))
        from .model import default_metadata

        return FragmentModel(
            fragment_id="png-import",
            pixels=pixels,
            mask=inferred,
            style_reference="self",
            style_strength=0.0,
            repair_iterations=1,
            output_format="png",
            metadata=default_metadata("png-partial", source_schema_version=1),
            loss_items=[loss_item("png_descriptor_missing", "metadata", "Partial PNG import used defaults.")],
        )
    descriptor_obj = parse_descriptor(descriptor_value)
    model = _model_from_pixels_and_descriptor(pixels, descriptor_obj, expected_format="png")
    if alpha is None:
        alpha = np.full((model.height, model.width), 255, dtype=np.uint8)
    alpha_mask = MaskRegion.from_alpha(alpha).rasterize(model.width, model.height)
    if not np.array_equal(alpha_mask, model.mask_pixels):
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "PNG alpha mask does not match the embedded descriptor mask",
            fragment_id=model.fragment_id,
            details={"lost_items": ["mask"], "reason": "alpha_descriptor_mismatch"},
        )
    return model


def encode_jpeg(model: FragmentModel, *, quality: int = 95, subsampling: int = 0) -> tuple[bytes, dict[str, Any]]:
    if not 1 <= int(quality) <= 100:
        raise WorkbenchError(E_VALIDATION, "JPEG quality must be within [1, 100]")
    if int(subsampling) not in {0, 1, 2}:
        raise WorkbenchError(E_VALIDATION, "JPEG subsampling must be 0, 1 or 2")
    losses = copy.deepcopy(model.loss_items)
    existing_codes = {item.get("code") for item in losses}
    for item in _jpeg_loss_items():
        if item["code"] not in existing_codes:
            losses.append(item)
    descriptor = descriptor_for_model(
        model,
        "jpeg",
        loss_items=losses,
        capabilities={
            "alpha": False,
            "lossless_pixels": False,
            "embedded_metadata": False,
            "external_mask_descriptor": True,
            "jpeg_quality": int(quality),
            "jpeg_subsampling": int(subsampling),
        },
    )
    image = Image.fromarray(model.pixels, mode="RGB")
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=int(quality), subsampling=int(subsampling), optimize=False)
    return output.getvalue(), descriptor


def decode_jpeg(
    image_bytes: bytes,
    descriptor: dict[str, Any] | bytes | str | None,
) -> FragmentModel:
    if descriptor is None:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "JPEG companion JSON descriptor is required to recover a lossless mask",
            details={"lost_items": ["mask", "fragment_id", "style_strength", "repair_iterations", "version_metadata"]},
        )
    descriptor_obj = parse_descriptor(descriptor)
    _validate_descriptor_degradation(descriptor_obj, expected_format="jpeg")
    pixels, _, _ = _decode_rgb(image_bytes, expected_format="jpeg")
    return _model_from_pixels_and_descriptor(pixels, descriptor_obj, expected_format="jpeg")


def _validate_descriptor_degradation(descriptor: dict[str, Any], *, expected_format: str) -> None:
    image_info = descriptor.get("image", {})
    actual_format = str(image_info.get("format", "")).lower()
    if actual_format == "jpg":
        actual_format = "jpeg"
    if actual_format != expected_format:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "Descriptor image format does not match the image payload",
            details={"lost_items": ["image_format"], "expected": expected_format, "actual": actual_format},
        )
    codes = {item.get("code") for item in descriptor.get("loss_items", []) if isinstance(item, dict)}
    if expected_format == "jpeg" and "jpeg_alpha_channel_dropped" not in codes:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "JPEG descriptor lacks the alpha-channel degradation marker required for mask reconstruction",
            details={"lost_items": ["degradation_marker", "mask"]},
        )


def _model_from_pixels_and_descriptor(
    pixels: np.ndarray,
    descriptor: dict[str, Any],
    *,
    expected_format: str,
) -> FragmentModel:
    _validate_descriptor_degradation(descriptor, expected_format=expected_format)
    image_info = descriptor.get("image", {})
    try:
        expected_width = int(image_info["width"])
        expected_height = int(image_info["height"])
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "Descriptor is missing valid image dimensions",
            details={"lost_items": ["image_dimensions"]},
        ) from exc
    actual_height, actual_width = pixels.shape[:2]
    if (expected_width, expected_height) != (actual_width, actual_height):
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "Descriptor dimensions do not match the image payload",
            fragment_id=str(descriptor.get("fragment", {}).get("id")) or None,
            details={
                "lost_items": ["mask", "style_strength", "repair_iterations", "version_metadata"],
                "expected": [expected_width, expected_height],
                "actual": [actual_width, actual_height],
            },
        )
    fragment = descriptor.get("fragment")
    if not isinstance(fragment, dict):
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "Descriptor fragment metadata is missing",
            details={"lost_items": ["fragment_id", "mask", "style_strength", "version_metadata"]},
        )
    try:
        mask = MaskRegion.from_dict(fragment.get("mask", {}).get("rects", []))
        model = FragmentModel(
            fragment_id=str(fragment["id"]),
            pixels=pixels,
            mask=mask,
            style_reference=str(fragment["style_reference"]),
            style_strength=float(fragment["style_strength"]),
            repair_iterations=int(fragment["repair_iterations"]),
            output_format=str(fragment.get("output_format", "png")),
            metadata=copy.deepcopy(fragment["metadata"]),
            loss_items=copy.deepcopy(descriptor.get("loss_items", fragment.get("loss_items", []))),
        )
    except WorkbenchError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            f"Descriptor metadata cannot reconstruct the model: {exc}",
            fragment_id=str(fragment.get("id")) or None,
            details={"lost_items": ["descriptor_fields"]},
        ) from exc
    declared_mask = fragment.get("mask", {}).get("pixel_sha256")
    if declared_mask is not None and declared_mask != model.mask_digest:
        raise WorkbenchError(
            E_FORMAT_LOSS,
            "Descriptor mask cannot reconstruct the original pixel set",
            fragment_id=model.fragment_id,
            details={"lost_items": ["mask"], "declared": declared_mask, "actual": model.mask_digest},
        )
    return model


def model_from_json_descriptor(
    image_bytes: bytes,
    descriptor: dict[str, Any] | bytes | str,
    *,
    image_format: str,
) -> FragmentModel:
    descriptor_obj = parse_descriptor(descriptor)
    format_lower = image_format.lower()
    if format_lower == "jpg":
        format_lower = "jpeg"
    if format_lower not in {"png", "jpeg"}:
        raise WorkbenchError(E_VALIDATION, "image_format must be png or jpeg")
    _validate_descriptor_degradation(descriptor_obj, expected_format=format_lower)
    pixels, alpha, _ = _decode_rgb(image_bytes, expected_format=format_lower)
    model = _model_from_pixels_and_descriptor(pixels, descriptor_obj, expected_format=format_lower)
    if format_lower == "png":
        if alpha is None:
            alpha = np.full((model.height, model.width), 255, dtype=np.uint8)
        if not np.array_equal(MaskRegion.from_alpha(alpha).rasterize(model.width, model.height), model.mask_pixels):
            raise WorkbenchError(
                E_FORMAT_LOSS,
                "PNG alpha mask and JSON descriptor mask disagree",
                fragment_id=model.fragment_id,
                details={"lost_items": ["mask"]},
            )
    return model


def describe_pixels_digest(pixels: np.ndarray) -> str:
    return digest_array(pixels, role="rgb")

