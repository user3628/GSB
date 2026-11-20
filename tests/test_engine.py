"""Acceptance tests for the multimodal alignment interop engine.

Run with:  python3 -m unittest discover -s tests -v
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np

import sample_gen
from align_engine import codecs, service
from align_engine.checkpoint import CheckpointStore, genesis_head
from align_engine.config import EngineConfig
from align_engine.errors import EngineError
from align_engine.importer import import_project
from align_engine.ir import Shard
from align_engine.pipeline import Workspace, run_pipeline, workspace_output_digests
from align_engine.quantize import dequantize, quantize


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="align-test-")
        self.samples = os.path.join(self.tmp, "samples")
        sample_gen.generate(self.samples)
        self.manifest_v1 = os.path.join(self.samples, "manifest_v1.yaml")
        self.manifest_v2 = os.path.join(self.samples, "manifest_v2.yaml")
        self.shards_dir = os.path.join(self.samples, "shards")
        self.inject_dir = os.path.join(self.samples, "inject")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def ws(self, name="ws"):
        return os.path.join(self.tmp, name)

    def import_v2(self, workspace, extra=False):
        return service.do_import(
            workspace, self.manifest_v2, self.shards_dir,
            extra_shards_dir=self.inject_dir if extra else None)


class TestSamples(BaseCase):
    def test_sample_layout(self):
        text = [f for f in os.listdir(self.shards_dir) if f.endswith(".npz")]
        vision = [f for f in os.listdir(self.shards_dir) if f.endswith(".npy")]
        self.assertEqual(len(text), 3)
        self.assertEqual(len(vision), 3)
        self.assertTrue(os.path.exists(self.manifest_v1))
        self.assertTrue(os.path.exists(self.manifest_v2))
        self.assertTrue(os.path.isdir(self.inject_dir))

    def test_generator_deterministic(self):
        other = os.path.join(self.tmp, "samples2")
        sample_gen.generate(other)
        for rel in ("shards/text-shard-001.npz", "manifest_v1.yaml",
                    "manifest_v2.yaml"):
            with open(os.path.join(self.samples, rel), "rb") as fh:
                first = fh.read()
            with open(os.path.join(other, rel), "rb") as fh:
                self.assertEqual(first, fh.read(), rel)


class TestFormats(BaseCase):
    def test_three_format_roundtrip_fidelity(self):
        workspace = self.ws()
        self.import_v2(workspace)
        project = codecs.read_project(
            "json_project", os.path.join(workspace, "project", "project.json"))
        reference = project.canonical_bytes()
        out = os.path.join(self.tmp, "fmt")
        os.makedirs(out)
        targets = {
            "json_project": os.path.join(out, "p.json"),
            "npz_pack": os.path.join(out, "p.npz"),
            "npy_dir": os.path.join(out, "p_dir"),
        }
        for fmt, path in targets.items():
            codecs.write_project(fmt, project, path)
            reloaded = codecs.read_project(fmt, path)
            self.assertEqual(reloaded.canonical_bytes(), reference, fmt)
            self.assertEqual([s.shard_id for s in reloaded.shards],
                             [s.shard_id for s in project.shards])
            self.assertEqual([s.dim for s in reloaded.shards],
                             [s.dim for s in project.shards])

    def test_quantization_restore_precision(self):
        rng = np.random.default_rng(7)
        vectors = rng.standard_normal((16, 96))
        for bits in (8, 16, 24):
            q, scales = quantize(vectors, bits)
            restored = dequantize(q, scales)
            err = np.max(np.abs(restored - vectors))
            self.assertLessEqual(err, max(scales) / 2 + 1e-12)

    def test_npz_deterministic_bytes(self):
        workspace = self.ws()
        self.import_v2(workspace)
        project = codecs.read_project(
            "json_project", os.path.join(workspace, "project", "project.json"))
        a = os.path.join(self.tmp, "a.npz")
        b = os.path.join(self.tmp, "b.npz")
        codecs.write_project("npz_pack", project, a)
        codecs.write_project("npz_pack", project, b)
        with open(a, "rb") as fh:
            self.assertEqual(fh.read(), open(b, "rb").read())


class TestUpgrade(BaseCase):
    def test_v1_upgrade_equivalent_to_v2(self):
        report = service.do_upgrade(
            self.manifest_v1, self.manifest_v2, self.shards_dir,
            os.path.join(self.tmp, "upgrade"),
            out_path=os.path.join(self.tmp, "upgrade", "v2_reexported.yaml"))
        self.assertTrue(report["equal"], report)
        for name, ok in report["checks"].items():
            self.assertTrue(ok, name)
        self.assertTrue(report["reexport_v2_manifest"]["equal"])
        notes = report["upgrade_notes"]
        self.assertEqual(notes["vision_dim_corrected"]["registered"], 64)
        self.assertEqual(notes["vision_dim_corrected"]["actual"], 96)
        self.assertEqual(notes["scale_percent"], 75)


class TestPipeline(BaseCase):
    def test_full_run_summary_deterministic(self):
        ws_a, ws_b = self.ws("a"), self.ws("b")
        self.import_v2(ws_a)
        self.import_v2(ws_b)
        run_pipeline(ws_a)
        run_pipeline(ws_b)
        summary_a = service.do_summary(ws_a)
        summary_b = service.do_summary(ws_b)
        self.assertEqual(summary_a["digest"], summary_b["digest"])
        self.assertEqual(summary_a["totals"]["uncovered_text_ids"], 1)
        self.assertEqual(summary_a["totals"]["degraded_samples"], 1)
        reasons = {u["reason"] for u in summary_a["coverage"]["uncovered"]}
        self.assertEqual(reasons, {"no_vision_match"})
        degraded = summary_a["coverage"]["degraded"]
        self.assertEqual(degraded[0]["reason"], "missing_operator_label")
        self.assertEqual(workspace_output_digests(Workspace(ws_a)),
                         workspace_output_digests(Workspace(ws_b)))

    def test_stop_resume_byte_identical(self):
        ws_fresh, ws_stop = self.ws("fresh"), self.ws("stop")
        self.import_v2(ws_fresh)
        self.import_v2(ws_stop)
        run_pipeline(ws_fresh)
        stopped = run_pipeline(ws_stop, stop_after_units=3)
        self.assertEqual(stopped["status"], "stopped")
        resumed = run_pipeline(ws_stop)
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(workspace_output_digests(Workspace(ws_fresh)),
                         workspace_output_digests(Workspace(ws_stop)))

    def test_dim_mismatch_error_and_no_partial_write(self):
        workspace = self.ws()
        self.import_v2(workspace, extra=True)
        with self.assertRaises(EngineError) as ctx:
            run_pipeline(workspace)
        err = ctx.exception
        self.assertEqual(err.code, "E_DIM_MISMATCH")
        self.assertEqual(err.details["shard_id"], "vision-shard-900-baddim")
        self.assertEqual(err.details["declared_dim"], 96)
        self.assertEqual(err.details["actual_dim"], 64)
        store = CheckpointStore(workspace)
        state = store.load_verified()
        self.assertNotIn("vision-shard-900-baddim", state["units"])

    def test_invalid_stage_errors(self):
        workspace = self.ws()
        self.import_v2(workspace)
        with self.assertRaises(EngineError) as ctx:
            run_pipeline(workspace, target_stage="concat")
        self.assertEqual(ctx.exception.code, "E_INVALID_STAGE")
        with self.assertRaises(EngineError) as ctx:
            run_pipeline(workspace, target_stage="9")
        self.assertEqual(ctx.exception.code, "E_INVALID_STAGE")

    def test_invalidation_propagation_on_config_change(self):
        workspace = self.ws()
        self.import_v2(workspace)
        run_pipeline(workspace)
        service.do_config_set(workspace, "operator_weights.op-encoder-b", 0.9)
        report = run_pipeline(workspace)
        reused = {e["stage"] for e in report["events"]
                  if e["event"] == "reuse"}
        rebuilt = {e["stage"] for e in report["events"]
                   if e["event"] == "rebuild"}
        self.assertEqual(reused, {"validate", "project"})
        self.assertEqual(rebuilt, {"concat", "export"})
        invalidations = service.do_invalidations(workspace)["invalidations"]
        self.assertTrue(invalidations)
        self.assertEqual(invalidations[0]["reason"], "fingerprint_mismatch")
        summary = service.do_summary(workspace)
        self.assertEqual(summary["config_version"],
                         Workspace(workspace).load_config().config_version())

    def test_projection_change_invalidates_stage2_downstream(self):
        workspace = self.ws()
        self.import_v2(workspace)
        run_pipeline(workspace)
        project_path = os.path.join(workspace, "project", "project.json")
        with open(project_path) as fh:
            doc = json.load(fh)
        doc["projection"]["matrix"][0][0] += 0.5
        from align_engine.util import canonical_json_bytes
        with open(project_path, "wb") as fh:
            fh.write(canonical_json_bytes(doc))
        report = run_pipeline(workspace)
        reused = {e["stage"] for e in report["events"]
                  if e["event"] == "reuse"}
        rebuilt = {e["stage"] for e in report["events"]
                   if e["event"] == "rebuild"}
        self.assertEqual(reused, {"validate"})
        self.assertEqual(rebuilt, {"project", "concat", "export"})


class TestCheckpointStore(BaseCase):
    def _record(self, unit, stage, **kw):
        record = {
            "unit": unit, "stage": stage,
            "stage_index": {"validate": 1, "project": 2}.get(stage, 1),
            "fingerprint": "fp-" + unit + "-" + stage,
            "input_fingerprint": "in",
            "structure_fingerprint": "struct",
            "config_version": "cfg",
            "artifact_digest": "0" * 64,
            "artifact_path": "artifacts/x.json",
            "artifact_summary": {},
        }
        record.update(kw)
        return record

    def test_sequence_and_chain_protection(self):
        store = CheckpointStore(self.tmp)
        store.publish(self._record("shard-a", "validate"))
        with self.assertRaises(EngineError) as ctx:
            store.publish(self._record("shard-a", "project", seq=1))
        self.assertEqual(ctx.exception.code, "E_STALE_CHECKPOINT")
        with self.assertRaises(EngineError) as ctx:
            store.publish(self._record("shard-a", "project", seq=5))
        self.assertEqual(ctx.exception.code, "E_STALE_CHECKPOINT")
        with self.assertRaises(EngineError) as ctx:
            store.publish(self._record("shard-b", "validate",
                                       prev_head=genesis_head("shard-a")))
        self.assertEqual(ctx.exception.code, "E_STALE_CHECKPOINT")
        store.publish(self._record("shard-a", "project"))
        with self.assertRaises(EngineError) as ctx:
            store.publish(self._record("shard-a", "project"))
        self.assertEqual(ctx.exception.code, "E_STALE_CHECKPOINT")

    def test_tampered_artifact_detected(self):
        workspace = self.ws()
        self.import_v2(workspace)
        run_pipeline(workspace)
        artifact = os.path.join(workspace, "artifacts", "s2_project",
                                "text-shard-001.json")
        with open(artifact, "ab") as fh:
            fh.write(b"tampered")
        with self.assertRaises(EngineError) as ctx:
            run_pipeline(workspace)
        err = ctx.exception
        self.assertEqual(err.code, "E_STALE_CHECKPOINT")
        self.assertIn("recorded_digest", err.details)
        self.assertIn("actual_digest", err.details)

    def test_checkpoint_replacement_detected(self):
        ws_a, ws_b = self.ws("a"), self.ws("b")
        self.import_v2(ws_a)
        run_pipeline(ws_a)
        self.import_v2(ws_b)
        service.do_config_set(ws_b, "operator_weights.op-encoder-b", 0.9)
        run_pipeline(ws_b)
        shutil.rmtree(os.path.join(ws_a, "checkpoints"))
        shutil.copytree(os.path.join(ws_b, "checkpoints"),
                        os.path.join(ws_a, "checkpoints"))
        with self.assertRaises(EngineError) as ctx:
            run_pipeline(ws_a)
        self.assertEqual(ctx.exception.code, "E_STALE_CHECKPOINT")


class TestHttpWorkbench(BaseCase):
    def test_http_api(self):
        import urllib.request
        from align_engine.workbench.server import serve_in_thread
        workspace = self.ws()
        self.import_v2(workspace)
        run_pipeline(workspace)
        server, port = serve_in_thread(workspace)
        try:
            def get(path):
                with urllib.request.urlopen(
                        "http://127.0.0.1:{}{}".format(port, path)) as resp:
                    return json.loads(resp.read())
            self.assertTrue(get("/api/health")["ok"])
            status = get("/api/status")
            self.assertEqual(len(status["units"]), 19)
            self.assertTrue(all(u["status"] == "valid"
                                for u in status["units"]))
            uncovered = get("/api/uncovered")
            self.assertEqual(len(uncovered["uncovered"]), 1)
            fingerprints = get("/api/fingerprints")
            self.assertIn("structure_fingerprint", fingerprints)
            checkpoints = get("/api/checkpoints")
            self.assertEqual(len(checkpoints["records"]), 19)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
