from __future__ import annotations

from pathlib import Path

import pytest

from panopipe.codecs import (
    CodecOptions,
    decode_coco_json,
    decode_png,
    decode_rle,
    encode_coco_json,
    encode_png,
    encode_rle,
    label_maps_equal,
    rle_from_json,
    rle_to_json,
    segment_tables_equal,
)
from panopipe.errors import E_PALETTE_CAPACITY, E_SEGMENT_ID_OVERFLOW, PipelineError
from panopipe.manifest import ParseOptions, load_manifest
from panopipe.semantic import decode_semantic, merge_instances


def _model(demo_dir: Path):
    manifest = load_manifest(
        demo_dir / "manifest_v2.json",
        options=ParseOptions(confidence_threshold=0.6),
    )
    shard = manifest.shards[0]
    return merge_instances(decode_semantic(shard.image_path, shard), shard)


def test_all_formats_round_trip_losslessly_and_deterministically(demo_dir: Path) -> None:
    model = _model(demo_dir)
    options = CodecOptions()
    png1 = encode_png(model, options)
    png2 = encode_png(model, options)
    assert png1 == png2
    png_model = decode_png(png1, options)
    assert label_maps_equal(model, png_model)
    assert segment_tables_equal(model, png_model)

    rle1 = encode_rle(model, options)
    rle2 = encode_rle(model, options)
    assert rle1 == rle2
    assert label_maps_equal(model, decode_rle(rle1, options))
    assert label_maps_equal(
        model,
        decode_rle(rle_from_json(rle_to_json(rle1)), options),
    )

    coco1 = encode_coco_json(model, options)
    coco2 = encode_coco_json(model, options)
    assert coco1 == coco2
    coco_model = decode_coco_json(coco1, options)
    assert label_maps_equal(model, coco_model)
    assert segment_tables_equal(model, coco_model)
    assert sum(segment.pixel_count for segment in coco_model.segments) == model.width * model.height


def test_palette_capacity_is_explicit_and_never_truncates(demo_dir: Path) -> None:
    model = _model(demo_dir)
    with pytest.raises(PipelineError) as exc_info:
        encode_png(model, CodecOptions(palette_max_colors=2))
    assert exc_info.value.code == E_PALETTE_CAPACITY
    assert exc_info.value.details["degradation"]["lossless"] is False
    assert exc_info.value.details["palette_entries"] > 2


def test_segment_id_bit_limit_is_explicit_and_never_truncates(demo_dir: Path) -> None:
    model = _model(demo_dir)
    with pytest.raises(PipelineError) as exc_info:
        encode_png(model, CodecOptions(segment_id_bits=6))
    assert exc_info.value.code == E_SEGMENT_ID_OVERFLOW
    assert exc_info.value.details["degradation"]["lossless"] is False
    assert exc_info.value.details["max_segment_id"] == max(s.segment_id for s in model.segments)
