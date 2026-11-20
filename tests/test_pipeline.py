from __future__ import annotations

from dataclasses import replace

import numpy as np

from image_workbench.hashing import digest_array
from image_workbench.pipeline import run_pipeline


def _run(manifest, cache=None, **kwargs):
    return run_pipeline(
        manifest.fragments,
        manifest.format_config,
        previous_cache=cache,
        **kwargs,
    )


def test_full_pipeline_is_deterministic_and_cacheable(manifest) -> None:
    first = _run(manifest, record_sink=lambda record: None)
    second = _run(manifest, cache=first.cache)
    assert first.summary_digest == second.summary_digest
    assert all(entry["reused"] for result in second.results.values() for entry in result.cache.statuses.values())
    assert len(first.results) == 5
    for result in first.results.values():
        assert result.repaired_pixels.dtype == np.uint8
        assert result.styled_pixels.dtype == np.uint8
        assert len(result.stage_records) == 4


def test_strength_change_invalidates_style_and_export_only(manifest) -> None:
    first = _run(manifest)
    models = list(manifest.fragments)
    target_index = next(index for index, item in enumerate(models) if item.fragment_id == "fragment_a")
    models[target_index] = replace(models[target_index], style_strength=0.9)
    changed = run_pipeline(models, manifest.format_config, previous_cache=first.cache)
    statuses = changed.results["fragment_a"].cache.statuses
    assert statuses["source"]["reused"] and statuses["repair"]["reused"]
    assert statuses["style"]["invalidated"] and statuses["export"]["invalidated"]
    assert "style strength changed" in statuses["style"]["reason"]
    assert changed.results["fragment_b"].cache.statuses["style"]["reused"]


def test_mask_change_invalidates_repair_downstream(manifest) -> None:
    first = _run(manifest)
    models = list(manifest.fragments)
    index = next(index for index, item in enumerate(models) if item.fragment_id == "fragment_a")
    from image_workbench.masks import MaskRect, MaskRegion

    models[index] = replace(models[index], mask=MaskRegion((MaskRect(0.1, 0.1, 0.4, 0.4),)))
    changed = run_pipeline(models, manifest.format_config, previous_cache=first.cache)
    statuses = changed.results["fragment_a"].cache.statuses
    assert statuses["source"]["reused"]
    assert statuses["repair"]["invalidated"]
    assert statuses["repair"]["reason"] == "repair mask changed"
    assert statuses["style"]["invalidated"] and statuses["export"]["invalidated"]


def test_iteration_change_invalidates_repair(manifest) -> None:
    first = _run(manifest)
    models = list(manifest.fragments)
    index = next(index for index, item in enumerate(models) if item.fragment_id == "fragment_a")
    models[index] = replace(models[index], repair_iterations=9)
    changed = run_pipeline(models, manifest.format_config, previous_cache=first.cache)
    assert changed.results["fragment_a"].cache.statuses["repair"]["reason"] == "repair iterations changed"


def test_format_config_change_invalidates_export_only(manifest) -> None:
    first = _run(manifest)
    config = {**manifest.format_config, "jpeg": {**manifest.format_config["jpeg"], "quality": 80}}
    changed = run_pipeline(manifest.fragments, config, previous_cache=first.cache)
    statuses = changed.results["fragment_a"].cache.statuses
    assert statuses["source"]["reused"] and statuses["repair"]["reused"] and statuses["style"]["reused"]
    assert statuses["export"]["invalidated"]
    assert statuses["export"]["reason"] == "format capability configuration changed"


def test_style_reference_pixel_change_invalidates_style(manifest) -> None:
    first = _run(manifest)
    models = list(manifest.fragments)
    reference = models[0]
    pixels = reference.pixels.copy()
    pixels[0, 0] = np.mod(pixels[0, 0].astype(np.int16) + 17, 256).astype(np.uint8)
    models[0] = reference.with_pixels(pixels)
    changed = run_pipeline(models, manifest.format_config, previous_cache=first.cache)
    target = changed.results["fragment_a"].cache.statuses
    assert target["source"]["reused"] and target["repair"]["reused"]
    assert target["style"]["invalidated"]
    assert "reference pixel content changed" in target["style"]["reason"]
    assert changed.results["style_ref"].cache.statuses["source"]["reason"] == "pixel content changed"


def test_graceful_stop_finishes_current_fragment(manifest) -> None:
    state = {"stop": False}

    def on_complete(fragment_id: str) -> None:
        state["stop"] = True

    result = run_pipeline(
        manifest.fragments,
        manifest.format_config,
        stop_requested=lambda: state["stop"],
        on_fragment_complete=on_complete,
    )
    assert result.stopped is True
    assert result.processed_order == ["style_ref"]
    assert result.stop_after_fragment == "style_ref"
    assert len(result.results["style_ref"].stage_records) == 4


def test_reused_upstream_artifacts_are_byte_identical(manifest) -> None:
    first = _run(manifest)
    second = _run(manifest, cache=first.cache)
    for fragment_id, first_result in first.results.items():
        second_result = second.results[fragment_id]
        assert digest_array(first_result.repaired_pixels) == digest_array(second_result.repaired_pixels)
        assert digest_array(first_result.styled_pixels) == digest_array(second_result.styled_pixels)
        assert first_result.export_bundle.semantic_digest == second_result.export_bundle.semantic_digest

