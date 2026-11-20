from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

os.environ.setdefault("WORKBENCH_HTTP_LOG", "0")

from PIL import Image

from workbench.api import WorkbenchHTTPServer
from workbench.config import load_config
from workbench.demo import reset_outputs, run_demo
from workbench.generator import generate_demo
from workbench.importer import import_dataset
from workbench.injection import append_injection, build_operation, clear_injections
from workbench.pipeline import (
    clear_stop,
    read_report,
    request_stop,
    run_pipeline,
    workspace_status,
)
from workbench.store import CheckpointStore
from workbench.util import StaleCheckpointError, WorkbenchError, atomic_write_json, read_json


BASELINE_REPORT_SHA256 = "19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f"


def _make_workspace(root: Path) -> None:
    generate_demo(root, clean=True)
    clear_injections(root)
    reset_outputs(root)


class DeterminismTests(unittest.TestCase):
    def test_generator_png_manifest_and_profile_are_byte_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = generate_demo(root, clean=True)
            first_manifest = (root / "samples" / "manifest.json").read_bytes()
            first_images = {
                path.name: path.read_bytes() for path in sorted((root / "samples" / "images").glob("*.png"))
            }
            second = generate_demo(root, clean=True)
            self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
            self.assertEqual(first_manifest, (root / "samples" / "manifest.json").read_bytes())
            self.assertEqual(
                first_images,
                {path.name: path.read_bytes() for path in sorted((root / "samples" / "images").glob("*.png"))},
            )

            run_pipeline(root)
            profiles_first = {
                key: record["artifact"]["profile_digest"]
                for key, record in CheckpointStore(root).all("preprocess").items()
            }
            reports_first = (root / "reports" / "report.json").read_bytes()
            reset_outputs(root)
            run_pipeline(root)
            profiles_second = {
                key: record["artifact"]["profile_digest"]
                for key, record in CheckpointStore(root).all("preprocess").items()
            }
            self.assertEqual(profiles_first, profiles_second)
            self.assertEqual(reports_first, (root / "reports" / "report.json").read_bytes())

    def test_baseline_metrics_and_fixed_report_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            result = run_pipeline(root)
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.report_sha256, BASELINE_REPORT_SHA256)
            report = read_report(root)
            assert report is not None
            self.assertEqual(report["metrics"]["micro"]["tp"], 6)
            self.assertEqual(report["metrics"]["micro"]["fp"], 2)
            self.assertEqual(report["metrics"]["micro"]["fn"], 1)
            self.assertEqual(report["metrics"]["cooccurrence"]["consistency"], 0.8)
            self.assertEqual({failure["code"] for failure in report["failures"]}, {
                "E_DIM_MISMATCH",
                "E_NUMERIC",
                "E_UNKNOWN_LABEL",
            })
            self.assertEqual([item["code"] for item in report["conflicts"]], ["E_LABEL_CONFLICT"])

    def test_demo_stop_resume_and_injection_restore_are_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summary = run_demo(root, stop_after=5)
            self.assertTrue(summary["stop_resume"]["byte_identical_to_baseline"])
            self.assertTrue(summary["injection"]["restored_byte_identical_to_baseline"])
            self.assertTrue(summary["injection"]["unaffected_score_artifact_unchanged"])
            self.assertEqual(summary["injection"]["error_code"], "E_SCORE_LENGTH")
            self.assertEqual(summary["injection"]["earliest_invalidated_stage"], "score")
            self.assertEqual(summary["baseline"]["report_sha256"], BASELINE_REPORT_SHA256)


class ErrorAndInjectionTests(unittest.TestCase):
    def _run_with_injection(self, operation: dict, expected_code: str, target_sample: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            before = {
                key: record["artifact_digest"]
                for key, record in CheckpointStore(root).all("score").items()
            }
            append_injection(root, operation)
            result = run_pipeline(root)
            self.assertEqual(result.status, "completed")
            report = read_report(root)
            assert report is not None
            matching = [
                failure
                for failure in report["failures"]
                if failure.get("code") == expected_code
                and (failure.get("sample_key") == target_sample or failure.get("image_id") in {None, target_sample})
            ]
            self.assertTrue(matching, f"missing {expected_code} for {target_sample}: {report['failures']}")
            unaffected_key = "s03_empty" if target_sample != "s03_empty" else "s01_dual"
            after = CheckpointStore(root).get("score", unaffected_key)
            self.assertIsNotNone(after)
            self.assertEqual(before[unaffected_key], after["artifact_digest"])

    def test_dim_mismatch_injection(self) -> None:
        self._run_with_injection(
            build_operation("dim_mismatch", "s01_dual", width=99, height=88),
            "E_DIM_MISMATCH",
            "s01_dual",
        )

    def test_unknown_label_injection(self) -> None:
        self._run_with_injection(
            build_operation("unknown_label", "s02_single", label="alien"),
            "E_UNKNOWN_LABEL",
            "s02_single",
        )

    def test_nan_and_inf_injection(self) -> None:
        self._run_with_injection(
            build_operation("numeric", "s02_single", label="vehicle", value="NaN"),
            "E_NUMERIC",
            "s02_single",
        )
        self._run_with_injection(
            build_operation("numeric", "s02_single", label="vehicle", value="Inf"),
            "E_NUMERIC",
            "s02_single",
        )

    def test_missing_field_injection(self) -> None:
        self._run_with_injection(
            build_operation("missing_field", "s02_single", field="image_id"),
            "E_MISSING_FIELD",
            "missing-image-id:1:images/s02_single.png",
        )

    def test_score_length_injection(self) -> None:
        self._run_with_injection(
            build_operation("score_length", "s02_single", label="vehicle"),
            "E_SCORE_LENGTH",
            "s02_single",
        )

    def test_empty_truth_is_legal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            record = CheckpointStore(root).get("score", "s03_empty")
            self.assertEqual(record["artifact"]["status"], "ok")
            self.assertEqual(record["artifact"]["positive_labels"], [])
            report = read_report(root)
            assert report is not None
            self.assertFalse(any(item["sample_key"] == "s03_empty" for item in report["failures"]))


class IncrementalTests(unittest.TestCase):
    def test_threshold_invalidates_score_and_preserves_stage1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            store = CheckpointStore(root)
            before_stage1 = {key: rec["artifact_digest"] for key, rec in store.all("preprocess").items()}
            before_stage2 = {key: rec["artifact_digest"] for key, rec in store.all("score").items()}
            config_path = root / "configs" / "default.json"
            config = read_json(config_path)
            config["scoring"]["default_threshold"] = 0.61
            atomic_write_json(config_path, config, pretty=True)
            result = run_pipeline(root)
            self.assertEqual(result.status, "completed")
            after_stage1 = {key: rec["artifact_digest"] for key, rec in store.all("preprocess").items()}
            self.assertEqual(before_stage1, after_stage1)
            self.assertNotEqual(before_stage2["s01_dual"], store.get("score", "s01_dual")["artifact_digest"])
            latest = next(item for item in reversed(store.invalidations(100)) if item["sample_key"] == "s01_dual")
            self.assertEqual(latest["earliest_stage"], "score")
            self.assertIn("scoring_config", latest["changes"][0]["changed_parts"])
            self.assertEqual(
                store.get("score", "s01_dual")["artifact"]["decisions"]["cat"]["threshold"],
                0.71817,
            )

    def test_block_size_and_pixel_changes_invalidate_preprocess(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            store = CheckpointStore(root)
            before = store.get("preprocess", "s01_dual")["artifact_digest"]
            config_path = root / "configs" / "default.json"
            config = read_json(config_path)
            config["preprocess"]["block_size"] = 4
            atomic_write_json(config_path, config, pretty=True)
            run_pipeline(root)
            after_block = store.get("preprocess", "s01_dual")["artifact_digest"]
            self.assertNotEqual(before, after_block)
            latest = next(item for item in reversed(store.invalidations(100)) if item["sample_key"] == "s01_dual")
            self.assertEqual(latest["earliest_stage"], "preprocess")

            image_path = root / "samples" / "images" / "s01_dual.png"
            with Image.open(image_path) as opened:
                image = opened.convert("RGB")
                image.putpixel((0, 0), (1, 2, 3))
                image.save(image_path, format="PNG", compress_level=9, optimize=False)
            run_pipeline(root)
            after_pixel = store.get("preprocess", "s01_dual")["artifact_digest"]
            self.assertNotEqual(after_block, after_pixel)
            latest = next(item for item in reversed(store.invalidations(100)) if item["sample_key"] == "s01_dual")
            self.assertEqual(latest["earliest_stage"], "preprocess")
            self.assertIn("image_sha256", latest["changes"][0]["changed_parts"])

    def test_truth_and_score_changes_invalidate_score_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            store = CheckpointStore(root)
            stage1_before = store.get("preprocess", "s01_dual")["artifact_digest"]
            unaffected_before = store.get("score", "s02_single")["artifact_digest"]
            manifest_path = root / "samples" / "manifest.json"
            manifest = read_json(manifest_path)
            manifest["samples"][0]["truth"][0]["lower_bound"] = 0.97
            atomic_write_json(manifest_path, manifest, pretty=True)
            run_pipeline(root)
            self.assertEqual(stage1_before, store.get("preprocess", "s01_dual")["artifact_digest"])
            self.assertEqual(unaffected_before, store.get("score", "s02_single")["artifact_digest"])
            latest = next(item for item in reversed(store.invalidations(100)) if item["sample_key"] == "s01_dual")
            self.assertEqual(latest["earliest_stage"], "score")
            self.assertIn("truth", latest["changes"][0]["changed_parts"])

            before_score = store.get("score", "s01_dual")["artifact_digest"]
            manifest = read_json(manifest_path)
            manifest["samples"][0]["scores"]["cat"] = 0.12
            atomic_write_json(manifest_path, manifest, pretty=True)
            run_pipeline(root)
            self.assertNotEqual(before_score, store.get("score", "s01_dual")["artifact_digest"])
            latest = next(item for item in reversed(store.invalidations(100)) if item["sample_key"] == "s01_dual")
            self.assertEqual(latest["earliest_stage"], "score")
            self.assertIn("scores", latest["changes"][0]["changed_parts"])

    def test_status_exposes_stale_fingerprints_before_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            run_pipeline(root)
            config_path = root / "configs" / "default.json"
            config = read_json(config_path)
            config["scoring"]["default_threshold"] = 0.58
            atomic_write_json(config_path, config, pretty=True)
            status = workspace_status(root)
            sample = next(item for item in status["samples"] if item["sample_key"] == "s01_dual")
            self.assertEqual(sample["earliest_stale_stage"], "score")
            self.assertNotEqual(
                sample["stages"]["score"]["input_fingerprint"],
                sample["stages"]["score"]["current_fingerprint"],
            )

    def test_local_import_copies_files_and_updates_default_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            imported = import_dataset(root, "samples/manifest.json", name="copy")
            self.assertEqual(imported["sample_count"], 8)
            self.assertTrue((root / imported["manifest"]).exists())
            config = load_config(root, "configs/default.json")
            self.assertEqual(config.data["dataset"]["manifest"], imported["manifest"])
            result = run_pipeline(root)
            self.assertEqual(result.status, "completed")
            report = read_report(root)
            assert report is not None
            self.assertEqual(report["metrics"]["micro"]["tp"], 6)


def _publish_worker(root_str: str, expected_sequence: int, queue: multiprocessing.Queue) -> None:
    try:
        store = CheckpointStore(Path(root_str))
        store.publish(
            stage="preprocess",
            artifact={"status": "ok", "sample_key": f"sample-{os.getpid()}"},
            sample_key=f"sample-{os.getpid()}",
            image_id=f"sample-{os.getpid()}",
            input_fingerprint=f"input-{os.getpid()}",
            input_parts={"pid": os.getpid()},
            config_version="sha256:test",
            expected_sequence=expected_sequence,
        )
        queue.put({"ok": True, "pid": os.getpid()})
    except WorkbenchError as exc:
        queue.put({"ok": False, "code": exc.code, "pid": os.getpid(), "details": exc.details})
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put({"ok": False, "code": type(exc).__name__, "pid": os.getpid(), "details": str(exc)})


class CheckpointStoreTests(unittest.TestCase):
    def _publish(self, store: CheckpointStore, key: str, fingerprint: str = "fp", artifact: dict | None = None) -> dict:
        return store.publish(
            stage="preprocess",
            artifact=artifact or {"status": "ok", "sample_key": key},
            sample_key=key,
            image_id=key,
            input_fingerprint=fingerprint,
            input_parts={"key": key, "fingerprint": fingerprint},
            config_version="sha256:test",
        )

    def test_duplicate_and_cross_sample_publish_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp))
            self._publish(store, "a")
            with self.assertRaises(StaleCheckpointError) as duplicate:
                self._publish(store, "a")
            self.assertEqual(duplicate.exception.details["reason"], "duplicate_publish")
            with self.assertRaises(StaleCheckpointError) as crossed:
                store.publish(
                    stage="preprocess",
                    artifact={"status": "ok", "sample_key": "b"},
                    sample_key="b",
                    image_id="b",
                    input_fingerprint="b",
                    input_parts={"b": 1},
                    config_version="sha256:test",
                    expected_sample_key="a",
                )
            self.assertEqual(crossed.exception.details["reason"], "cross_sample_publish")

    def test_two_processes_only_advance_one_contiguous_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx = multiprocessing.get_context("fork")
            queue = ctx.Queue()
            workers = [
                ctx.Process(target=_publish_worker, args=(str(root), 1, queue))
                for _ in range(2)
            ]
            for worker in workers:
                worker.start()
            results = [queue.get(timeout=10) for _ in workers]
            for worker in workers:
                worker.join(timeout=10)
            successes = [item for item in results if item["ok"]]
            stale = [item for item in results if not item["ok"] and item["code"] == "E_STALE_CHECKPOINT"]
            self.assertEqual(len(successes), 1, results)
            self.assertEqual(len(stale), 1, results)
            summary = CheckpointStore(root).summary("preprocess")
            self.assertEqual(summary["record_count"], 1)
            self.assertEqual(summary["sequence"], 1)

    def test_snapshot_rollback_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = CheckpointStore(root)
            self._publish(store, "a", "fp-a")
            backup = root / "stage1_snapshot"
            shutil.copytree(store.stage_dir("preprocess"), backup)
            self._publish(store, "b", "fp-b")
            shutil.rmtree(store.stage_dir("preprocess"))
            shutil.copytree(backup, store.stage_dir("preprocess"))
            with self.assertRaises(StaleCheckpointError) as stale:
                store.get("preprocess", "a")
            self.assertEqual(stale.exception.details["reason"], "snapshot_sequence_regression")

    def test_snapshot_digest_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = CheckpointStore(root)
            self._publish(store, "a")
            index_path = store.index_path("preprocess")
            index = read_json(index_path)
            index["snapshot_digest"] = "0" * 64
            atomic_write_json(index_path, index)
            with self.assertRaises(StaleCheckpointError) as stale:
                store.get("preprocess", "a")
            self.assertEqual(stale.exception.details["reason"], "snapshot_digest_mismatch")


class StopAndHTTPTests(unittest.TestCase):
    def test_cooperative_stop_file_and_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            request_stop(root, "unit_test")
            stopped = run_pipeline(root, clear_stop_flag=False)
            self.assertEqual(stopped.status, "stopped")
            self.assertEqual(stopped.stop_reason, "stop_request")
            clear_stop(root)
            resumed = run_pipeline(root, mode="resume")
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(resumed.report_sha256, BASELINE_REPORT_SHA256)

    def test_http_dashboard_run_report_and_injection_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_workspace(root)
            server = WorkbenchHTTPServer(("127.0.0.1", 0), root)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            base = f"http://{host}:{port}"
            try:
                html = urllib.request.urlopen(base + "/", timeout=5).read().decode("utf-8")
                self.assertIn("本地图像分类", html)
                health = json.loads(urllib.request.urlopen(base + "/api/health", timeout=5).read())
                self.assertTrue(health["ok"])
                request = urllib.request.Request(
                    base + "/api/run",
                    data=json.dumps({"mode": "run"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                accepted = json.loads(urllib.request.urlopen(request, timeout=5).read())
                self.assertTrue(accepted["accepted"])
                deadline = time.time() + 15
                completed = False
                while time.time() < deadline:
                    status = json.loads(urllib.request.urlopen(base + "/api/status", timeout=5).read())
                    if status["job"]["last_result"]:
                        completed = True
                        break
                    if status["job"]["last_error"]:
                        self.fail(status["job"]["last_error"])
                    time.sleep(0.1)
                self.assertTrue(completed, "HTTP-triggered run did not finish")
                report = json.loads(urllib.request.urlopen(base + "/api/report", timeout=5).read())
                self.assertEqual(report["metrics"]["micro"]["tp"], 6)
                digest = urllib.request.urlopen(base + "/api/report.sha256", timeout=5).read().decode().strip()
                self.assertEqual(digest, BASELINE_REPORT_SHA256)
                exported = urllib.request.urlopen(base + "/api/export", timeout=5).read()
                self.assertEqual(json.loads(exported)["dataset"]["dataset_id"], "deterministic-cv-demo-v1")
                injection = {
                    "operation": build_operation("score_length", "s08_multi", label="cat")
                }
                request = urllib.request.Request(
                    base + "/api/inject",
                    data=json.dumps(injection).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                injected = json.loads(urllib.request.urlopen(request, timeout=5).read())
                self.assertEqual(injected["operations"][0]["type"], "score_length")
                urllib.request.urlopen(
                    urllib.request.Request(base + "/api/clear-injection", data=b"{}", headers={"Content-Type": "application/json"}, method="POST"),
                    timeout=5,
                ).read()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
