from __future__ import annotations

import copy

import numpy as np
import pytest

from image_workbench.codecs import (
    decode_jpeg,
    decode_png,
    encode_jpeg,
    encode_png,
    model_from_json_descriptor,
    parse_descriptor,
)
from image_workbench.errors import E_FORMAT_LOSS, E_NUMERIC, WorkbenchError
from image_workbench.project_io import build_project_document, load_project_document


def test_png_roundtrip_is_lossless(fragment_a) -> None:
    payload, descriptor = encode_png(fragment_a)
    imported = decode_png(payload, descriptor)
    assert np.array_equal(imported.pixels, fragment_a.pixels)
    assert imported.mask.to_lists() == fragment_a.mask.to_lists()
    assert np.array_equal(imported.mask_pixels, fragment_a.mask_pixels)
    assert imported.style_strength == fragment_a.style_strength
    assert imported.style_reference == fragment_a.style_reference
    assert imported.metadata == fragment_a.metadata
    assert imported.loss_items == fragment_a.loss_items


def test_png_embedded_descriptor_roundtrip(fragment_a) -> None:
    payload, _ = encode_png(fragment_a)
    imported = decode_png(payload)
    assert imported.semantic_dict() == fragment_a.semantic_dict()


def test_json_descriptor_plus_png_roundtrip(fragment_a) -> None:
    payload, descriptor = encode_png(fragment_a)
    descriptor_json = descriptor.copy()
    imported = model_from_json_descriptor(payload, descriptor_json, image_format="png")
    assert imported.semantic_dict() == fragment_a.semantic_dict()


def test_jpeg_roundtrip_preserves_mask_and_records_losses(fragment_a) -> None:
    payload, descriptor = encode_jpeg(fragment_a, quality=95)
    imported = decode_jpeg(payload, descriptor)
    assert np.array_equal(imported.mask_pixels, fragment_a.mask_pixels)
    assert imported.mask.to_lists() == fragment_a.mask.to_lists()
    assert imported.style_strength == fragment_a.style_strength
    assert imported.style_reference == fragment_a.style_reference
    assert imported.repair_iterations == fragment_a.repair_iterations
    assert imported.metadata == fragment_a.metadata
    codes = {item["code"] for item in imported.loss_items}
    assert {"jpeg_alpha_channel_dropped", "jpeg_metadata_externalized", "jpeg_pixel_quantization"} <= codes
    # JPEG may quantize colours, but that must be explicitly declared rather than silent.
    if not np.array_equal(imported.pixels, fragment_a.pixels):
        assert "jpeg_pixel_quantization" in codes


def test_jpeg_missing_descriptor_is_format_loss(fragment_a) -> None:
    payload, _ = encode_jpeg(fragment_a)
    with pytest.raises(WorkbenchError) as caught:
        decode_jpeg(payload, None)
    assert caught.value.code == E_FORMAT_LOSS
    assert "mask" in caught.value.details["lost_items"]


def test_jpeg_dimension_mismatch_is_format_loss(fragment_a) -> None:
    _, descriptor = encode_jpeg(fragment_a)
    other = np.zeros((7, 9, 3), dtype=np.uint8)
    other[:] = np.arange(7 * 9 * 3, dtype=np.uint8).reshape(7, 9, 3)
    mismatched_payload, _ = encode_jpeg(fragment_a.with_pixels(other))
    with pytest.raises(WorkbenchError) as caught:
        decode_jpeg(mismatched_payload, descriptor)
    assert caught.value.code == E_FORMAT_LOSS
    assert "mask" in caught.value.details["lost_items"]
    assert caught.value.details["expected"] == [fragment_a.width, fragment_a.height]


def test_descriptor_digest_tampering_is_format_loss(fragment_a) -> None:
    _, descriptor = encode_jpeg(fragment_a)
    tampered = copy.deepcopy(descriptor)
    tampered["fragment"]["style_strength"] = 0.1
    with pytest.raises(WorkbenchError) as caught:
        parse_descriptor(tampered)
    assert caught.value.code == E_FORMAT_LOSS
    assert "descriptor_integrity" in caught.value.details["lost_items"]


def test_descriptor_nan_is_numeric_error() -> None:
    payload = b'{"kind":"image-workbench-descriptor","descriptor_version":1,"value":NaN}'
    with pytest.raises(WorkbenchError) as caught:
        parse_descriptor(payload)
    assert caught.value.code == E_NUMERIC
    assert caught.value.pixel == [0, 0]


def test_internal_project_json_roundtrip(manifest) -> None:
    document = build_project_document(manifest.fragments, project_metadata=manifest.metadata, project_name=manifest.project_name)
    fragments, metadata, project_name = load_project_document(document)
    assert project_name == manifest.project_name
    assert metadata == manifest.metadata
    assert len(fragments) == len(manifest.fragments)
    for left, right in zip(fragments, manifest.fragments, strict=True):
        assert np.array_equal(left.pixels, right.pixels)
        assert left.semantic_dict() == right.semantic_dict()

