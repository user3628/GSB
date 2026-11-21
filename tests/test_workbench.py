from __future__ import annotations

import json
import math
import multiprocessing
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import numpy as np

from blocklab.activation import activation_schedule, compute_activation_block
from blocklab.checkpoints import publish_block, scan_stage
from blocklab.config import load_config, parse_config
from blocklab.engine import ExecutionEngine
from blocklab.errors import (
    E_INVALID_STAGE,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    BusinessError,
)
from blocklab.http_server import LocalApplication, LocalHTTPServer
from blocklab.tensorio import load_npz_tensor, npy_bytes
from blocklab.util import atomic_write_json, digest_bytes
from blocklab.workbench import Workbench

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
CONFIGS = ROOT / "configs"


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pause_worker(root: str, workdir: str, job_id: str, barrier, queue) -> None:
    try:
        wb = Workbench(Path(root), workdir)
        state = wb.sync_job(job_id)
        job, paths, tensor, config, source_shape, _declared = wb.load_context(job_id)
        schedule = activation_schedule(source_shape, config.row_block, config.col_block)
        item = schedule[0]
        block, metadata = compute_activation_block(
            tensor.array,
            item,
            config.activation,
            block_context={"job_id": job_id, "shard_id": job["shard_id"]},
        )
        barrier.wait(timeout=10)
        record = publish_block(
            paths,
            stage="activation",
            epoch=int(state["stage_epochs"]["activation"]),
            requested_sequence=1,
            plan_fingerprint=state["stage_plans"]["activation"]["fingerprint"],
            input_fingerprint=tensor.digest,
            config_version=state["stage_plans"]["activation"]["config_version"],
            workspace_config_version=config.version,
            schedule=schedule,
            schedule_item=item,
            artifact_bytes=npy_bytes(block),
            product_kind="npy",
            status=metadata["status"],
            metadata=metadata,
        )
        queue.put({"ok": True, "sequence": record["sequence"]})
    except BusinessError as error:
        queue.put({"ok": False, "code": error.code, "details": error.details})
    except Exception as error:  # pragma: no cover - surfaced through the queue
        queue.put({"ok": False, "code": "E_TEST", "details": {"message": repr(error)}})


class BlockLabTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix=".blocklab-test-", dir=ROOT)
        self.base = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def workbench(self, name: str) -> Workbench:
        return Workbench(ROOT, self.base / name)

    def import_sample(
        self,
        wb: Workbench,
        sample: str,
        config: str,
        *,
        job_id: str | None = None,
        manifest: Path | None = None,
    ) -> dict[str, Any]:
        return wb.import_job(
            source_path=f"samples/{sample}.npz",
            config_path=f"configs/{config}.yaml",
            job_id=job_id or sample,
            manifest_path=manifest,
        )

    def assert_contiguous_sequences(self, wb: Workbench, job_id: str) -> None:
        bundle = wb.scan_all_stages(job_id)
        for stage in ("activation", "reorder", "reduce"):
            scan = bundle["scans"][stage]
            sequences = [record["sequence"] for record in scan["records"]]
            self.assertEqual(sequences, list(range(1, len(sequences) + 1)), stage)

    def checkpoint_record_bytes(self, wb: Workbench, job_id: str) -> dict[str, bytes]:
        root = wb.paths.job(job_id).checkpoints_dir
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in sorted(root.rglob("seq-*.json"))
        }

    def output_bytes(self, wb: Workbench, job_id: str) -> dict[str, bytes]:
        output = wb.paths.job(job_id).outputs_dir
        return {path.name: path.read_bytes() for path in sorted(output.iterdir()) if path.is_file()}


class SampleAndKernelTests(BlockLabTestCase):
    def test_yaml_aliases_and_case_are_normalized(self) -> None:
        config = parse_config(
            {
                "version": 7,
                "activation": "GELU",
                "reorder_mode": "head_seq_swap",
                "row_block_size": 3,
                "col_block_size": 5,
                "sum_strategy": "STABLE_SUM",
                "tolerances": {"abs": 1e-8, "rel": 1e-6},
                "target_shape": [2, 7, 3, 5],
            }
        )
        self.assertEqual(config.activation, "gelu")
        self.assertEqual(config.reorder_mode, "swap_head_seq")
        self.assertEqual((config.row_block, config.col_block), (3, 5))
        self.assertEqual(config.sum_strategy, "sum")
        self.assertEqual((config.abs_tol, config.rel_tol), (1e-8, 1e-6))

    def test_generator_is_byte_deterministic(self) -> None:
        from samples.generator import generate

        first = self.base / "sample-first"
        second = self.base / "sample-second"
        generate(first)
        generate(second)
        for name in (
            "tensor_4d_gelu_swap.npz",
            "matrix_silu_tile.npz",
            "tensor_4d_sigmoid_tile.npz",
            "tensor_4d_gelu_diag.npz",
            "shape_manifest.json",
        ):
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes(), name)
            self.assertEqual((first / name).read_bytes(), (SAMPLES / name).read_bytes(), name)

    def test_activation_reference_reorder_and_legal_ranges(self) -> None:
        wb = self.workbench("kernels")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "tensor_4d_gelu_swap", "gelu_swap", job_id="gelu")
        engine.run_job("gelu")
        source = load_npz_tensor(SAMPLES / "tensor_4d_gelu_swap.npz").array
        activation = np.load(wb.paths.job("gelu").output_path("activation.npy"))
        reordered = np.load(wb.paths.job("gelu").output_path("reordered.npy"))
        erf = np.vectorize(math.erf, otypes=[float])
        expected = 0.5 * source * (1.0 + erf(source / math.sqrt(2.0)))
        np.testing.assert_allclose(activation, expected, rtol=1e-9, atol=3e-7)
        np.testing.assert_array_equal(reordered, np.transpose(activation, (0, 2, 1, 3)))
        self.assertEqual(reordered.shape, (2, 7, 3, 5))
        finite = activation[np.isfinite(activation)]
        self.assertGreaterEqual(float(finite.min()), -0.171)
        self.assertLessEqual(float(finite.max()), 1.0e4)

        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="silu")
        engine.run_job("silu")
        activation2 = np.load(wb.paths.job("silu").output_path("activation.npy"))
        reordered2 = np.load(wb.paths.job("silu").output_path("reordered.npy"))
        self.assertEqual(activation2.shape, (9, 11))
        self.assertEqual(reordered2.shape, (9, 11))
        np.testing.assert_array_equal(np.sort(activation2, axis=None), np.sort(reordered2, axis=None))
        finite2 = activation2[np.isfinite(activation2)]
        self.assertGreaterEqual(float(finite2.min()), -0.279)

        self.import_sample(wb, "tensor_4d_sigmoid_tile", "sigmoid_tile", job_id="sigmoid")
        engine.run_job("sigmoid")
        activation3 = np.load(wb.paths.job("sigmoid").output_path("activation.npy"))
        reordered3 = np.load(wb.paths.job("sigmoid").output_path("reordered.npy"))
        self.assertGreaterEqual(float(activation3.min()), 0.0)
        self.assertLessEqual(float(activation3.max()), 1.0)
        np.testing.assert_array_equal(np.sort(activation3, axis=None), np.sort(reordered3, axis=None))

    def test_completed_blocks_are_not_recomputed(self) -> None:
        wb = self.workbench("reuse")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="reuse")
        first = engine.run_job("reuse")
        before = self.output_bytes(wb, "reuse")
        checkpoint_files = sorted(wb.paths.job("reuse").checkpoints_dir.rglob("seq-*.json"))
        checkpoint_bytes = {path: path.read_bytes() for path in checkpoint_files}
        second = engine.run_job("reuse")
        self.assertEqual(first["computed_blocks"], 27)
        self.assertEqual(second["computed_blocks"], 0)
        self.assertEqual(second["reused_blocks"], 27)
        self.assertEqual(before, self.output_bytes(wb, "reuse"))
        self.assertEqual(sorted(wb.paths.job("reuse").checkpoints_dir.rglob("seq-*.json")), checkpoint_files)
        for path, data in checkpoint_bytes.items():
            self.assertEqual(path.read_bytes(), data)


class RecoveryTests(BlockLabTestCase):
    def _full_reference(self, name: str) -> tuple[Workbench, dict[str, bytes]]:
        wb = self.workbench(f"{name}-full")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "tensor_4d_gelu_swap", "gelu_swap", job_id="resume-case")
        engine.run_job("resume-case")
        return wb, self.output_bytes(wb, "resume-case")

    def test_mid_stage_stop_resume_is_byte_identical(self) -> None:
        _full_wb, reference = self._full_reference("stopresume")
        wb = self.workbench("stopresume-resume")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "tensor_4d_gelu_swap", "gelu_swap", job_id="resume-case")
        stopped = engine.run_job("resume-case", stop_after_blocks=2)
        self.assertEqual(stopped["status"], "stopped")
        self.assertEqual(stopped["computed_blocks"], 2)
        self.assertEqual(stopped["stages"]["activation"]["valid_blocks"], 2)
        resumed = engine.run_job("resume-case")
        self.assertEqual(resumed["stages"]["activation"]["reused_blocks"], 2)
        self.assertEqual(resumed["stages"]["activation"]["computed_blocks"], 26)
        self.assertTrue(resumed["complete"])
        self.assertEqual(reference, self.output_bytes(wb, "resume-case"))
        self.assertEqual(
            self.checkpoint_record_bytes(_full_wb, "resume-case"),
            self.checkpoint_record_bytes(wb, "resume-case"),
        )
        self.assert_contiguous_sequences(wb, "resume-case")

    def test_corrupt_and_duplicate_records_are_ignored(self) -> None:
        _full_wb, reference = self._full_reference("corruption")

        corrupt_wb = self.workbench("corruption-bad")
        corrupt_engine = ExecutionEngine(corrupt_wb)
        self.import_sample(corrupt_wb, "tensor_4d_gelu_swap", "gelu_swap", job_id="resume-case")
        corrupt_engine.run_job("resume-case", stop_after_blocks=2)
        stage_dir = corrupt_wb.paths.job("resume-case").stage_dir("activation", 1)
        second = next(stage_dir.glob("seq-00000002-*.json"))
        second.write_text('{"broken":', encoding="utf-8")
        scan = corrupt_wb.scan_all_stages("resume-case")["scans"]["activation"]
        self.assertEqual(scan["valid_blocks"], 1)
        self.assertTrue(scan["ignored"])
        corrupt_engine.run_job("resume-case")
        self.assertEqual(reference, self.output_bytes(corrupt_wb, "resume-case"))

        duplicate_wb = self.workbench("corruption-duplicate")
        duplicate_engine = ExecutionEngine(duplicate_wb)
        self.import_sample(duplicate_wb, "tensor_4d_gelu_swap", "gelu_swap", job_id="resume-case")
        duplicate_engine.run_job("resume-case", stop_after_blocks=2)
        duplicate_stage = duplicate_wb.paths.job("resume-case").stage_dir("activation", 1)
        first = next(duplicate_stage.glob("seq-00000001-*.json"))
        (duplicate_stage / "seq-00000001-duplicate.json").write_bytes(first.read_bytes())
        duplicate_scan = duplicate_wb.scan_all_stages("resume-case")["scans"]["activation"]
        self.assertEqual(duplicate_scan["valid_blocks"], 0)
        self.assertTrue(duplicate_scan["ignored"])
        duplicate_engine.run_job("resume-case")
        self.assertEqual(reference, self.output_bytes(duplicate_wb, "resume-case"))


class InvalidationTests(BlockLabTestCase):
    def test_incremental_invalidation_and_reuse(self) -> None:
        wb = self.workbench("invalidation")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="inval")
        engine.run_job("inval")
        act_before = wb.paths.job("inval").output_path("activation.npy").read_bytes()
        reo_before = wb.paths.job("inval").output_path("reordered.npy").read_bytes()

        wb.import_job(
            source_path="samples/matrix_silu_tile.npz",
            config_path="configs/silu_tile_mean.yaml",
            job_id="inval",
        )
        events = wb.invalidations("inval")["events"]
        event = events[-1]
        self.assertEqual(event["earliest_stage"], "reduce")
        self.assertEqual(event["unaffected_stages"], ["activation", "reorder"])
        self.assertIn("reduction_policy_changed", event["reasons"])
        mean_run = engine.run_job("inval")
        self.assertEqual(mean_run["stages"]["activation"]["reused_blocks"], 9)
        self.assertEqual(mean_run["stages"]["reorder"]["reused_blocks"], 9)
        self.assertEqual(mean_run["stages"]["reduce"]["computed_blocks"], 9)
        self.assertEqual(act_before, wb.paths.job("inval").output_path("activation.npy").read_bytes())
        self.assertEqual(reo_before, wb.paths.job("inval").output_path("reordered.npy").read_bytes())

        manifest = _json(SAMPLES / "shape_manifest.json")
        for entry in manifest["shards"]:
            if entry["id"] == "matrix_silu_tile":
                entry["target_shape"] = [11, 9]
        swap_manifest = self.base / "swap_manifest.json"
        swap_manifest.write_text(json.dumps(manifest), encoding="utf-8")
        wb.import_job(
            source_path="samples/matrix_silu_tile.npz",
            config_path="configs/silu_tile_swap.yaml",
            job_id="inval",
            manifest_path=swap_manifest,
        )
        event = wb.invalidations("inval")["events"][-1]
        self.assertEqual(event["earliest_stage"], "reorder")
        self.assertEqual(event["unaffected_stages"], ["activation"])
        swap_run = engine.run_job("inval")
        self.assertEqual(swap_run["stages"]["activation"]["reused_blocks"], 9)
        self.assertEqual(swap_run["stages"]["reorder"]["computed_blocks"], 6)
        self.assertEqual(act_before, wb.paths.job("inval").output_path("activation.npy").read_bytes())
        self.assertEqual(np.load(wb.paths.job("inval").output_path("reordered.npy")).shape, (11, 9))
        self.assert_contiguous_sequences(wb, "inval")

    def test_activation_block_size_and_input_changes_start_at_activation(self) -> None:
        wb = self.workbench("invalidation-activation")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="input-change")
        engine.run_job("input-change")

        gelu_config = self.base / "gelu_tile.yaml"
        gelu_config.write_text(
            """version: 1
activation: {type: gelu}
reorder: {mode: tile, target_shape: [9, 11]}
blocking: {row_block: 4, col_block: 5}
reduction: {strategy: sum, abs_tol: 1.0e-12, rel_tol: 1.0e-9}
""",
            encoding="utf-8",
        )
        wb.import_job(source_path="samples/matrix_silu_tile.npz", config_path=gelu_config, job_id="input-change")
        event = wb.invalidations("input-change")["events"][-1]
        self.assertEqual(event["earliest_stage"], "activation")
        self.assertIn("activation_type_changed", event["reasons"])
        engine.run_job("input-change")

        small_block_config = self.base / "small_blocks.yaml"
        small_block_config.write_text(
            """version: 1
activation: {type: gelu}
reorder: {mode: tile, target_shape: [9, 11]}
blocking: {row_block: 3, col_block: 3}
reduction: {strategy: sum, abs_tol: 1.0e-12, rel_tol: 1.0e-9}
""",
            encoding="utf-8",
        )
        wb.import_job(source_path="samples/matrix_silu_tile.npz", config_path=small_block_config, job_id="input-change")
        event = wb.invalidations("input-change")["events"][-1]
        self.assertEqual(event["earliest_stage"], "activation")
        self.assertIn("block_size_changed", event["reasons"])
        engine.run_job("input-change")

        source_copy = self.base / "mutable.npz"
        tensor = load_npz_tensor(SAMPLES / "matrix_silu_tile.npz").array.copy()
        np.savez(source_copy, tensor=tensor)
        wb.import_job(source_path=source_copy, config_path=small_block_config, job_id="mutable")
        engine.run_job("mutable")
        tensor[0, 0] = 123.0
        np.savez(source_copy, tensor=tensor)
        wb.import_job(source_path=source_copy, config_path=small_block_config, job_id="mutable", manifest_path=None)
        event = wb.invalidations("mutable")["events"][-1]
        self.assertEqual(event["earliest_stage"], "activation")
        self.assertIn("input_content_changed", event["reasons"])
        resumed = engine.run_job("mutable")
        self.assertEqual(resumed["stages"]["activation"]["computed_blocks"], resumed["stages"]["activation"]["expected_blocks"])


class ErrorAndConcurrencyTests(BlockLabTestCase):
    def test_invalid_stage_shape_mismatch_numeric_and_stale_errors(self) -> None:
        wb = self.workbench("errors")
        engine = ExecutionEngine(wb)
        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="invalid-stage")
        with self.assertRaises(BusinessError) as invalid:
            engine.run_job("invalid-stage", stages=["reduce"])
        self.assertEqual(invalid.exception.code, E_INVALID_STAGE)
        self.assertEqual(invalid.exception.details["requested_stage"], "reduce")
        self.assertIn("current_available_sequence", invalid.exception.details)

        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="shape")
        paths = wb.paths.job("shape")
        job = _json(paths.job_file)
        job["declared_target_shape"] = [99, 99]
        atomic_write_json(paths.job_file, job)
        with self.assertRaises(BusinessError) as shape_error:
            engine.run_job("shape")
        self.assertEqual(shape_error.exception.code, E_SHAPE_MISMATCH)
        self.assertEqual(shape_error.exception.details["declared_shape"], [99, 99])
        self.assertEqual(shape_error.exception.details["actual_shape"], [9, 11])

        self.import_sample(wb, "tensor_4d_gelu_diag", "gelu_swap_diag", job_id="numeric")
        numeric = engine.run_job("numeric")
        self.assertEqual(numeric["status"], "completed_with_errors")
        self.assertEqual(numeric["business_code"], E_NUMERIC)
        status = wb.job_status("numeric")
        self.assertEqual(status["business_code"], E_NUMERIC)
        diagnostics = wb.diagnostics("numeric")["diagnostics"]
        self.assertTrue(any(item["stage"] == "activation" for item in diagnostics))
        activation_diag = next(
            item for item in diagnostics if item["stage"] == "activation" and item["diagnostics"].get("nonfinite")
        )
        values = [
            value
            for item in diagnostics
            if item["stage"] == "activation" and item["diagnostics"].get("nonfinite")
            for value in item["diagnostics"]["nonfinite"]["values"]
        ]
        self.assertIn("NaN", values)
        self.assertIn("Infinity", values)
        self.assertIn("-Infinity", values)
        self.assertIn("row_range", activation_diag["diagnostics"])
        self.assertIn("col_range", activation_diag["diagnostics"])
        self.assertIn("shard_id", activation_diag["diagnostics"])
        self.assertTrue(any(item["diagnostics"].get("zero_rows") for item in diagnostics))
        activation = np.load(wb.paths.job("numeric").output_path("activation.npy"))
        self.assertEqual(int(np.isnan(activation).sum()), 1)
        self.assertEqual(int(np.isposinf(activation).sum()), 1)
        self.assertEqual(int(np.isneginf(activation).sum()), 1)

        stale_wb = self.workbench("stale")
        self.import_sample(stale_wb, "matrix_silu_tile", "silu_tile", job_id="stale")
        state = stale_wb.sync_job("stale")
        job, paths, tensor, config, source_shape, _declared = stale_wb.load_context("stale")
        schedule = activation_schedule(source_shape, config.row_block, config.col_block)
        block, metadata = compute_activation_block(
            tensor.array,
            schedule[0],
            config.activation,
            block_context={"job_id": "stale", "shard_id": job["shard_id"]},
        )
        kwargs = dict(
            paths=paths,
            stage="activation",
            epoch=1,
            plan_fingerprint=state["stage_plans"]["activation"]["fingerprint"],
            input_fingerprint=tensor.digest,
            config_version=state["stage_plans"]["activation"]["config_version"],
            workspace_config_version=config.version,
            schedule=schedule,
            artifact_bytes=npy_bytes(block),
            product_kind="npy",
            status=metadata["status"],
            metadata=metadata,
        )
        with self.assertRaises(BusinessError) as cross_block:
            publish_block(requested_sequence=1, schedule_item=schedule[1], **kwargs)
        self.assertEqual(cross_block.exception.code, E_STALE_CHECKPOINT)
        publish_block(requested_sequence=1, schedule_item=schedule[0], **kwargs)
        with self.assertRaises(BusinessError) as duplicate:
            publish_block(requested_sequence=1, schedule_item=schedule[0], **kwargs)
        self.assertEqual(duplicate.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(BusinessError) as wrong_next:
            publish_block(requested_sequence=2, schedule_item=schedule[0], **kwargs)
        self.assertEqual(wrong_next.exception.code, E_STALE_CHECKPOINT)

    def test_concurrent_publish_only_one_sequence_wins(self) -> None:
        wb = self.workbench("concurrent")
        self.import_sample(wb, "matrix_silu_tile", "silu_tile", job_id="race")
        context = multiprocessing.get_context("fork")
        barrier = context.Barrier(2)
        queue = context.Queue()
        processes = [
            context.Process(
                target=_pause_worker,
                args=(str(ROOT), str(wb.paths.workdir), "race", barrier, queue),
            )
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
            self.assertEqual(process.exitcode, 0)
        results = [queue.get(timeout=5), queue.get(timeout=5)]
        successes = [item for item in results if item["ok"]]
        stale = [item for item in results if not item["ok"]]
        self.assertEqual(len(successes), 1, results)
        self.assertEqual(len(stale), 1, results)
        self.assertEqual(stale[0]["code"], E_STALE_CHECKPOINT)
        state = wb.sync_job("race")
        job, paths, tensor, config, source_shape, _declared = wb.load_context("race")
        schedule = activation_schedule(source_shape, config.row_block, config.col_block)
        scan = scan_stage(
            paths,
            stage="activation",
            epoch=1,
            plan_fingerprint=state["stage_plans"]["activation"]["fingerprint"],
            input_fingerprint=tensor.digest,
            config_version=state["stage_plans"]["activation"]["config_version"],
            schedule=schedule,
        )
        self.assertEqual(scan["valid_blocks"], 1)
        self.assertEqual(scan["records"][0]["sequence"], 1)


class InterfaceTests(BlockLabTestCase):
    def _request(self, base: str, method: str, path: str, body: dict[str, Any] | None = None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            base + path,
            data=data,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None, response.headers
        except urllib.error.HTTPError as error:
            try:
                return error.code, json.loads(error.read()), error.headers
            finally:
                error.close()

    def test_http_api_stop_resume_export_and_static_ui(self) -> None:
        app = LocalApplication(ROOT, str(self.base / "http-work"))
        server = LocalHTTPServer(("127.0.0.1", 0), app)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            status, health, _ = self._request(base, "GET", "/api/health")
            self.assertEqual(status, 200)
            self.assertTrue(health["local_only"])
            with urllib.request.urlopen(base + "/", timeout=10) as response:
                self.assertIn(b"BlockLab", response.read())
            with urllib.request.urlopen(base + "/static/app.js", timeout=10) as response:
                self.assertIn(b"runStages", response.read())

            status, imported, _ = self._request(
                base, "POST", "/api/import", {"sample_id": "matrix_silu_tile", "job_id": "http"}
            )
            self.assertEqual(status, 200, imported)
            status, invalid, _ = self._request(
                base, "POST", "/api/jobs/http/run", {"stage": "reduce", "background": False}
            )
            self.assertEqual(status, 409)
            self.assertEqual(invalid["error"]["code"], E_INVALID_STAGE)
            status, run, _ = self._request(
                base, "POST", "/api/jobs/http/run", {"stage": "all", "background": False}
            )
            self.assertEqual(status, 200)
            self.assertTrue(run["complete"])
            status, summary, _ = self._request(base, "GET", "/api/jobs/http/export")
            self.assertEqual(status, 200)
            self.assertTrue(Path(ROOT / summary["saved_path"]).exists())
            status, blocks, _ = self._request(base, "GET", "/api/jobs/http/blocks?stage=reduce")
            self.assertEqual(status, 200)
            self.assertEqual(len(blocks["blocks"]), 9)
            status, reorder_blocks, _ = self._request(base, "GET", "/api/jobs/http/blocks?stage=reorder")
            self.assertEqual(status, 200)
            self.assertIn("source_region", reorder_blocks["blocks"][0]["metadata"])
            self.assertIn("target_region", reorder_blocks["blocks"][0]["metadata"])

            self._request(base, "POST", "/api/import", {"sample_id": "tensor_4d_gelu_swap", "job_id": "stop-resume"})
            status, started, _ = self._request(
                base,
                "POST",
                "/api/jobs/stop-resume/run",
                {"stage": "activation", "background": True, "block_delay_ms": 220},
            )
            self.assertEqual(status, 202, started)
            time.sleep(0.45)
            status, stopped, _ = self._request(base, "POST", "/api/jobs/stop-resume/stop", {})
            self.assertEqual(status, 200)
            deadline = time.time() + 8
            stopped_status = None
            while time.time() < deadline:
                status, stopped_status, _ = self._request(base, "GET", "/api/jobs/stop-resume/status")
                run_state = stopped_status.get("run_state")
                if run_state and run_state.get("status") == "stopped":
                    break
                time.sleep(0.12)
            self.assertEqual(stopped_status["run_state"]["status"], "stopped")
            status, resumed, _ = self._request(
                base,
                "POST",
                "/api/jobs/stop-resume/resume",
                {"stage": "all", "background": False},
            )
            self.assertEqual(status, 200, resumed)
            self.assertTrue(resumed["complete"])
            status, final_status, _ = self._request(base, "GET", "/api/jobs/stop-resume/status")
            self.assertTrue(final_status["complete"])
        finally:
            server.shutdown()
            app.cleanup()
            server.server_close()
            thread.join(timeout=3)

    def test_cli_import_run_status_and_export(self) -> None:
        workdir = self.base / "cli-work"
        base_command = [sys.executable, "-m", "blocklab", "--root", str(ROOT), "--workdir", str(workdir)]

        def run(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                base_command + list(args),
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        imported = run(
            "--json",
            "import",
            "--shard",
            "samples/matrix_silu_tile.npz",
            "--config",
            "configs/silu_tile.yaml",
            "--job",
            "cli-job",
        )
        self.assertEqual(imported.returncode, 0, imported.stderr)
        self.assertEqual(json.loads(imported.stdout)["job_id"], "cli-job")
        executed = run("run", "--job", "cli-job", "--json")
        self.assertEqual(executed.returncode, 0, executed.stderr)
        self.assertTrue(json.loads(executed.stdout)["complete"])
        status = run("status", "--job", "cli-job")
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertIn("activation", status.stdout)
        export_path = self.base / "cli-export.json"
        exported = run("export", "--job", "cli-job", "--out", str(export_path), "--json")
        self.assertEqual(exported.returncode, 0, exported.stderr)
        self.assertTrue(export_path.exists())
        self.assertEqual(json.loads(exported.stdout)["sha256"], digest_bytes(export_path.read_bytes()))


if __name__ == "__main__":
    unittest.main()

