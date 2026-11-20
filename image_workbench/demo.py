from __future__ import annotations

import copy
import shutil
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .atomic import atomic_write_json
from .codecs import decode_jpeg, decode_png, model_from_json_descriptor
from .demo_data import generate_demo_data
from .errors import (
    E_FORMAT_LOSS,
    E_NUMERIC,
    E_SNAPSHOT_DIGEST,
    E_STALE_SNAPSHOT,
    WorkbenchError,
)
from .hashing import digest_json
from .legacy import export_upgraded_manifest, upgrade_legacy_manifest
from .manifest import load_manifest
from .model import FragmentModel
from .pipeline import run_pipeline
from .project_io import load_project
from .service import WorkbenchService
from .snapshots import finalize_snapshot

EXPECTED_SAMPLE_DIGEST = "444291c68e0861de51c62283d222107eb85829772f02538b6cae9185e9d35351"
EXPECTED_PIPELINE_SUMMARY_DIGEST = "692aee759bb4c936733fa9b39f6fc4901ec97d5a501779d63e4737c9a91a587c"


class DemoRecorder:
    def __init__(self) -> None:
        self.checks: list[dict[str, Any]] = []
        self.error_codes: list[dict[str, Any]] = []

    def check(self, name: str, condition: bool, **details: Any) -> None:
        self.checks.append({"name": name, "ok": bool(condition), "details": details})
        if not condition:
            raise AssertionError(name)

    def capture_error(self, name: str, expected_code: str, action: Callable[[], Any]) -> WorkbenchError:
        try:
            action()
        except WorkbenchError as error:
            matches = error.code == expected_code
            self.error_codes.append(
                {
                    "name": name,
                    "expected": expected_code,
                    "actual": error.code,
                    "fragment_id": error.fragment_id,
                    "pixel": error.pixel,
                    "details": copy.deepcopy(error.details),
                }
            )
            self.check(name, matches, expected=expected_code, actual=error.code, error=error.to_dict())
            return error
        raise AssertionError(f"{name}: expected {expected_code}, but no error was raised")


def _small_jpeg(width: int, height: int) -> bytes:
    import io

    pixels = np.zeros((height, width, 3), dtype=np.uint8)
    pixels[:] = np.arange(height * width * 3, dtype=np.uint8).reshape(height, width, 3)
    output = io.BytesIO()
    Image.fromarray(pixels, mode="RGB").save(output, format="JPEG", quality=90, subsampling=0)
    return output.getvalue()


def run_demo(root: str | Path, *, runtime_dir: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root).resolve()
    demo_runtime = Path(runtime_dir).resolve() if runtime_dir is not None else root_path / "runtime" / "demo"
    if demo_runtime.exists():
        shutil.rmtree(demo_runtime)
    demo_runtime.mkdir(parents=True, exist_ok=True)
    recorder = DemoRecorder()
    report: dict[str, Any] = {
        "ok": False,
        "python_runtime": __import__("sys").version.split()[0],
        "root": str(root_path),
        "runtime": str(demo_runtime),
        "expected": {
            "sample_digest": EXPECTED_SAMPLE_DIGEST,
            "pipeline_summary_digest": EXPECTED_PIPELINE_SUMMARY_DIGEST,
        },
        "checks": recorder.checks,
        "error_codes": recorder.error_codes,
    }
    try:
        sample = generate_demo_data(root_path / "data")
        recorder.check(
            "deterministic sample digest",
            sample["sample_digest"] == EXPECTED_SAMPLE_DIGEST,
            actual=sample["sample_digest"],
        )
        recorder.check("sample has at least four fragments", sample["fragment_count"] >= 4, count=sample["fragment_count"])
        recorder.check("sample contains PNG and JPEG", set(sample["formats"]) == {"png", "jpeg"})
        report["sample_digest"] = sample["sample_digest"]

        service = WorkbenchService(root_path, runtime_dir=demo_runtime, autoload=False)
        service.import_manifest(root_path / "data" / "manifest.json")
        first_run = service.run()
        report["pipeline_summary_digest"] = first_run["summary_digest"]
        recorder.check(
            "fixed complete-run summary digest",
            first_run["summary_digest"] == EXPECTED_PIPELINE_SUMMARY_DIGEST,
            actual=first_run["summary_digest"],
        )
        recorder.check("all five fragments processed", len(first_run["processed_order"]) == 5)
        recorder.check("snapshot advanced to sequence 1", first_run["snapshot"]["to"] == 1, snapshot=first_run["snapshot"])

        second_run = service.run()
        recorder.check("repeat run summary is fixed", second_run["summary_digest"] == first_run["summary_digest"])
        recorder.check(
            "repeat run reuses all upstream and export artifacts",
            all(status["reused"] for fragment in second_run["stage_statuses"].values() for status in fragment.values()),
        )

        internal_path = demo_runtime / "internal_project.json"
        exported = service.export_project(internal_path, artifact="style")
        internal_models, _, _ = load_project(internal_path)
        expected_a = service.fragments["fragment_a"].with_pixels(service.cache.fragments["fragment_a"].artifacts["style"])
        internal_a = next(model for model in internal_models if model.fragment_id == "fragment_a")
        recorder.check("internal project export written", Path(exported["path"]).exists(), path=exported["path"])
        recorder.check(
            "internal project roundtrip preserves pixels, mask, style and metadata",
            internal_a.semantic_dict() == expected_a.semantic_dict(),
        )

        png_result = service.convert("fragment_a", "png", demo_runtime / "exports")
        png_model = decode_png(
            Path(png_result["files"]["image"]).read_bytes(),
            Path(png_result["files"]["descriptor"]).read_bytes(),
        )
        recorder.check(
            "PNG lossless roundtrip pixel equality",
            np.array_equal(png_model.pixels, expected_a.pixels),
        )
        recorder.check(
            "PNG mask pixel-set difference is zero",
            int(np.count_nonzero(png_model.mask_pixels != expected_a.mask_pixels)) == 0,
        )
        recorder.check(
            "PNG style and version metadata roundtrip",
            png_model.style_strength == expected_a.style_strength and png_model.metadata == expected_a.metadata,
        )

        json_result = service.convert("fragment_a", "json", demo_runtime / "exports")
        json_descriptor = Path(json_result["files"]["descriptor"]).read_bytes()
        json_model = model_from_json_descriptor(
            Path(png_result["files"]["image"]).read_bytes(),
            json_descriptor,
            image_format="png",
        )
        recorder.check("JSON descriptor + PNG reconstructs the model", json_model.semantic_dict() == expected_a.semantic_dict())

        jpeg_result = service.convert("fragment_a", "jpeg", demo_runtime / "exports")
        jpeg_model = decode_jpeg(
            Path(jpeg_result["files"]["image"]).read_bytes(),
            Path(jpeg_result["files"]["descriptor"]).read_bytes(),
        )
        jpeg_loss_codes = {item["code"] for item in jpeg_result["loss_items"]}
        recorder.check(
            "JPEG degradation items are explicit",
            {
                "jpeg_alpha_channel_dropped",
                "jpeg_metadata_externalized",
                "jpeg_pixel_quantization",
            }
            <= jpeg_loss_codes,
            loss_codes=sorted(jpeg_loss_codes),
        )
        recorder.check(
            "JPEG + sidecar mask pixel-set difference is zero",
            int(np.count_nonzero(jpeg_model.mask_pixels != expected_a.mask_pixels)) == 0,
        )
        recorder.check(
            "JPEG sidecar preserves style and version metadata",
            jpeg_model.style_strength == expected_a.style_strength and jpeg_model.metadata == expected_a.metadata,
        )

        recorder.capture_error(
            "JPEG without descriptor",
            E_FORMAT_LOSS,
            lambda: decode_jpeg(Path(jpeg_result["files"]["image"]).read_bytes(), None),
        )
        recorder.capture_error(
            "JPEG descriptor dimension mismatch",
            E_FORMAT_LOSS,
            lambda: decode_jpeg(
                _small_jpeg(7, 9),
                Path(jpeg_result["files"]["descriptor"]).read_bytes(),
            ),
        )

        recorder.capture_error(
            "invalid style strength",
            E_NUMERIC,
            lambda: service.update_fragment("fragment_a", {"style_strength": 1.5}),
        )

        def nan_pixel_model() -> Any:
            bad_pixels = service.fragments["fragment_a"].pixels.astype(np.float64)
            bad_pixels[2, 3, 1] = np.nan
            return FragmentModel(
                fragment_id="fragment_a",
                pixels=bad_pixels,
                mask=service.fragments["fragment_a"].mask,
                style_reference="style_ref",
                style_strength=0.5,
                repair_iterations=3,
                output_format="png",
                metadata=service.fragments["fragment_a"].metadata,
            )

        numeric_error = recorder.capture_error("NaN pixel matrix", E_NUMERIC, nan_pixel_model)
        recorder.check(
            "numeric error carries fragment and pixel coordinate",
            numeric_error.fragment_id == "fragment_a" and numeric_error.pixel == [3, 2],
            fragment_id=numeric_error.fragment_id,
            pixel=numeric_error.pixel,
        )

        legacy, upgraded_document = upgrade_legacy_manifest(root_path / "data" / "legacy_manifest.json")
        business_fragment = next(
            model for model in legacy.fragments if model.metadata.get("legacy_style_strength_percent") == 60
        )
        expected_mask = np.zeros((20, 20), dtype=bool)
        expected_mask[5:10, 5:10] = True
        recorder.check(
            "legacy v1 pixel mask upgrades with zero pixel-set difference",
            int(np.count_nonzero(business_fragment.mask_pixels != expected_mask)) == 0,
        )
        recorder.check(
            "legacy percentage normalizes to semantically equal strength",
            business_fragment.style_strength == 60 / 100,
            actual=business_fragment.style_strength,
        )
        recorder.check("legacy fragments receive stable generated IDs", all(model.fragment_id.startswith("legacy-") for model in legacy.fragments))
        recorder.check("legacy upgrade emits schema v2", upgraded_document["schema_version"] == 2)
        upgraded_path = demo_runtime / "legacy_upgraded_v2.json"
        export_upgraded_manifest(legacy, upgraded_path)
        upgraded_reload = load_manifest(upgraded_path)
        recorder.check("upgraded v2 manifest can be re-imported", len(upgraded_reload.fragments) == len(legacy.fragments))

        service.update_fragment("fragment_a", {"style_strength": 0.75})
        strength_run = service.run()
        strength_status = strength_run["stage_statuses"]["fragment_a"]
        recorder.check(
            "strength change reuses source/repair and invalidates style/export",
            strength_status["source"]["reused"]
            and strength_status["repair"]["reused"]
            and strength_status["style"]["invalidated"]
            and strength_status["export"]["invalidated"],
        )
        recorder.check("strength invalidation reason is observable", "style strength changed" in strength_status["style"]["reason"])
        recorder.check(
            "unaffected fragment reuse remains intact",
            strength_run["stage_statuses"]["fragment_b"]["style"]["reused"],
        )

        changed_config = copy.deepcopy(manifest_config(service))
        changed_config["jpeg"]["quality"] = 80
        service.set_format_config(changed_config)
        config_run = service.run()
        config_status = config_run["stage_statuses"]["fragment_b"]
        recorder.check(
            "format capability change invalidates export only",
            config_status["source"]["reused"]
            and config_status["repair"]["reused"]
            and config_status["style"]["reused"]
            and config_status["export"]["invalidated"],
        )
        recorder.check(
            "format invalidation reason is observable",
            config_status["export"]["reason"] == "format capability configuration changed",
        )

        service.update_fragment("fragment_a", {"mask_rects": [[0.2, 0.2, 0.4, 0.4]]})
        mask_run = service.run()
        mask_status = mask_run["stage_statuses"]["fragment_a"]
        recorder.check(
            "mask change reuses source and invalidates repair downstream",
            mask_status["source"]["reused"] and mask_status["repair"]["invalidated"],
        )
        recorder.check("mask invalidation reason is observable", mask_status["repair"]["reason"] == "repair mask changed")

        fragment_c = service.fragments["fragment_c"]
        changed_pixels = fragment_c.pixels.copy()
        changed_pixels[0, 0] = np.mod(changed_pixels[0, 0].astype(np.int16) + 13, 256).astype(np.uint8)
        service.fragments["fragment_c"] = fragment_c.with_pixels(changed_pixels)
        pixel_run = service.run()
        recorder.check(
            "pixel content change invalidates source and downstream",
            pixel_run["stage_statuses"]["fragment_c"]["source"]["invalidated"]
            and pixel_run["stage_statuses"]["fragment_c"]["repair"]["invalidated"],
        )
        recorder.check(
            "pixel invalidation reason is observable",
            pixel_run["stage_statuses"]["fragment_c"]["source"]["reason"] == "pixel content changed",
        )

        stop_state = {"stop": False}
        stopped_pipeline = run_pipeline(
            service._fragment_list(),
            service.format_config,
            previous_cache=service.cache,
            stop_requested=lambda: stop_state["stop"],
            on_fragment_complete=lambda fragment_id: stop_state.update(stop=True),
        )
        recorder.check(
            "graceful stop completes the current fragment before exiting",
            stopped_pipeline.stopped
            and len(stopped_pipeline.processed_order) == 1
            and stopped_pipeline.stop_after_fragment == "style_ref",
            processed=stopped_pipeline.processed_order,
        )
        service.request_stop()
        recorder.check("stop request is observable", service.control.is_requested())
        service.resume()
        recorder.check("resume clears the stop request", not service.control.is_requested())

        current_snapshot = service.snapshots.read_current()
        assert current_snapshot is not None
        recorder.check(
            "rollback snapshot classification",
            service.snapshots.inspect_candidate(current_snapshot)["status"] == "rollback",
        )
        mismatched = copy.deepcopy(current_snapshot)
        mismatched["digest"] = "0" * 64
        recorder.check(
            "digest mismatch snapshot classification",
            service.snapshots.inspect_candidate(mismatched)["status"] == "digest_mismatch",
        )
        gap = finalize_snapshot(int(current_snapshot["sequence"]) + 2, [])
        recorder.check("snapshot gap classification", service.snapshots.inspect_candidate(gap)["status"] == "gap")
        recorder.capture_error("stale duplicate publication", E_STALE_SNAPSHOT, lambda: service.publish_snapshot(current_snapshot))
        recorder.capture_error("stale gap publication", E_STALE_SNAPSHOT, lambda: service.publish_snapshot(gap))
        recorder.capture_error("snapshot digest mismatch publication", E_SNAPSHOT_DIGEST, lambda: service.publish_snapshot(mismatched))

        report["ok"] = all(check["ok"] for check in recorder.checks)
        report["check_count"] = len(recorder.checks)
        report["passed_count"] = sum(1 for check in recorder.checks if check["ok"])
        report["snapshot_sequence"] = service.snapshot_status().get("sequence")
        report["final_status_digest"] = digest_json(service.status())
        report["exception"] = None
    except Exception as exc:  # noqa: BLE001 - demo must persist a complete failure report
        report["ok"] = False
        report["exception"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        report["check_count"] = len(recorder.checks)
        report["passed_count"] = sum(1 for check in recorder.checks if check["ok"])
    atomic_write_json(demo_runtime / "report.json", report)
    report["report_path"] = str(demo_runtime / "report.json")
    return report


def manifest_config(service: WorkbenchService) -> dict[str, Any]:
    return copy.deepcopy(service.format_config)

