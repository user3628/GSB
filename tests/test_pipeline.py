"""停止恢复、失效传播、并发保护与 HTTP 端到端测试。"""

import copy
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request

import uvicorn

from mmpipe import STAGES
from mmpipe.codec import canonical_dumps
from mmpipe.errors import E_STALE_CHECKPOINT, PipeError
from mmpipe.pipeline import StopController, expected_inputs, run_pipeline
from mmpipe.samplegen import generate
from mmpipe.storage import Workspace
from mmpipe import pipeline
from mmpipe.verifier import verify_workspace


class PipelineBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mmpipe-it-")
        self.sample_dir = os.path.join(self.tmp, "samples")
        generate(self.sample_dir)
        self.ws_dir = os.path.join(self.tmp, "ws")
        self.workspace = Workspace(self.ws_dir)
        with open(os.path.join(self.sample_dir, "pipeline_config.json"), "rb") as f:
            self.config = json.loads(f.read())
        self.texts = pipeline.load_texts(os.path.join(self.sample_dir, "texts.jsonl"))
        self.images = pipeline.load_images(os.path.join(self.sample_dir, "images.json"))

    def full_reference(self, name):
        ws = Workspace(os.path.join(self.tmp, name))
        run_pipeline(ws, self.config, self.texts, self.images,
                     StopController(ws))
        return ws


class TestStopResume(PipelineBase):
    def test_stop_after_one_unit_then_resume_matches_full_run(self):
        reference = self.full_reference("reference")
        stop = StopController(self.workspace)

        def auto_stop():
            time.sleep(0.05)
            stop.request()

        threading.Thread(target=auto_stop, daemon=True).start()
        stopped_stage = None
        try:
            run_pipeline(self.workspace, self.config, self.texts, self.images,
                         stop=stop, unit_delay=0.05)
        except PipeError as exc:
            self.assertEqual(exc.code, "E_STOP_REQUESTED")
            stopped_stage = exc.stage
        self.assertIsNotNone(stopped_stage)

        seqs = [cp["seq"] for cp in self.workspace.load_valid_checkpoints()]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))

        stop2 = StopController(self.workspace)
        stop2.clear()
        report = run_pipeline(self.workspace, self.config, self.texts,
                              self.images, stop=stop2)
        self.assertEqual(self.workspace.head_seq(), 4)
        self.assertIn(stopped_stage, report["executed"])

        for stage in STAGES:
            self.assertEqual(
                canonical_dumps(self.workspace.load_current_product(stage)),
                canonical_dumps(reference.load_current_product(stage)),
                stage)
        verify = verify_workspace(self.workspace)
        self.assertTrue(verify["ok"], verify)

    def test_stop_flag_file_detected(self):
        stop = StopController(self.workspace)
        self.workspace.request_stop()
        self.assertTrue(stop.requested())
        stop.clear()
        self.assertFalse(stop.requested())


class TestInvalidation(PipelineBase):
    def _published_seqs(self):
        return {stage: self.workspace.stage_state(stage)["seq"]
                for stage in STAGES}

    def test_text_change_invalidates_all_from_first_stage(self):
        self.full_reference("reference")
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        before = self._published_seqs()

        changed = copy.deepcopy(self.texts)
        changed[0]["text"] = changed[0]["text"] + " updated"
        run_pipeline(self.workspace, self.config, changed, self.images,
                     StopController(self.workspace))
        after = self._published_seqs()
        for stage in STAGES:
            self.assertGreater(after[stage], before[stage], stage)
        inv = self.workspace.invalidations()
        self.assertEqual(inv[0]["stage"], "text_encoding")
        self.assertEqual({i["stage"] for i in inv}, set(STAGES))

    def test_weight_change_invalidates_only_downstream(self):
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        before = self._published_seqs()

        old_fusion = canonical_dumps(
            self.workspace.load_current_product("fusion_scoring"))
        changed_cfg = copy.deepcopy(self.config)
        changed_cfg["fusion_weights"] = {"similarity": 0.8, "operator": 0.2}
        run_pipeline(self.workspace, changed_cfg, self.texts, self.images,
                     StopController(self.workspace))
        after = self._published_seqs()
        self.assertEqual(after["text_encoding"], before["text_encoding"])
        self.assertEqual(after["visual_alignment"], before["visual_alignment"])
        self.assertGreater(after["fusion_scoring"], before["fusion_scoring"])
        self.assertGreater(after["aggregation"], before["aggregation"])
        self.assertNotEqual(
            old_fusion,
            canonical_dumps(self.workspace.load_current_product("fusion_scoring")))

        rerun = run_pipeline(self.workspace, changed_cfg, self.texts,
                             self.images, StopController(self.workspace))
        self.assertEqual(rerun["reused"], list(STAGES))
        inv_stages = {i["stage"] for i in self.workspace.invalidations()}
        self.assertEqual(inv_stages, {"fusion_scoring", "aggregation"})

    def test_projection_change_invalidates_visual_onward(self):
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        before = self._published_seqs()
        changed_cfg = copy.deepcopy(self.config)
        changed_cfg["projection_matrix"][0][0] += 0.123
        run_pipeline(self.workspace, changed_cfg, self.texts, self.images,
                     StopController(self.workspace))
        after = self._published_seqs()
        self.assertEqual(after["text_encoding"], before["text_encoding"])
        for stage in STAGES[1:]:
            self.assertGreater(after[stage], before[stage], stage)

    def test_confidence_change_observed_config_version(self):
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        changed_cfg = copy.deepcopy(self.config)
        changed_cfg["operator_confidence"]["op_rgb"] = 0.55
        run_pipeline(self.workspace, changed_cfg, self.texts, self.images,
                     StopController(self.workspace))
        inv = self.workspace.invalidations()
        self.assertEqual(inv[0]["stage"], "fusion_scoring")
        self.assertEqual(inv[0]["component"], "fusion_config_or_upstream")
        state = self.workspace.stage_state("fusion_scoring")
        self.assertEqual(state["status"], "published")

    def test_no_change_means_full_reuse(self):
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        report = run_pipeline(self.workspace, self.config, self.texts,
                              self.images, StopController(self.workspace))
        self.assertEqual(report["reused"], list(STAGES))
        self.assertEqual(self.workspace.head_seq(), 4)


class TestConcurrentPublish(PipelineBase):
    def test_two_workers_compete_for_next_seq(self):
        from mmpipe import text_encoding, visual_alignment
        text_product = text_encoding.run(self.texts, self.config)
        exp = expected_inputs(self.config, self.texts, self.images)
        self.workspace.publish("text_encoding", text_product,
                               exp["text_encoding"]["input"],
                               exp["text_encoding"]["config"])
        head = self.workspace.head_seq()
        visual_product = visual_alignment.run(self.images, text_product,
                                              self.config)
        outcomes = []
        barrier = threading.Barrier(2)

        def worker(slot):
            ws = Workspace(self.ws_dir)
            barrier.wait()
            try:
                cp = ws.publish("visual_alignment", visual_product,
                                exp["visual_alignment"]["input"],
                                exp["visual_alignment"]["config"],
                                expected_base_seq=head)
                outcomes.append((slot, "ok", cp["seq"]))
            except PipeError as exc:
                outcomes.append((slot, exc.code, None))

        threads = [threading.Thread(target=worker, args=(i,)) for i in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        codes = sorted(o[1] for o in outcomes)
        self.assertEqual(codes, [E_STALE_CHECKPOINT, "ok"])
        winner = [o for o in outcomes if o[1] == "ok"][0]
        self.assertEqual(winner[2], head + 1)
        self.assertEqual(self.workspace.head_seq(), head + 1)

    def test_missing_predecessor_rejected(self):
        exp = expected_inputs(self.config, self.texts, self.images)
        with self.assertRaises(PipeError) as ctx:
            self.workspace.publish("fusion_scoring", {"stage": "fusion_scoring"},
                                   exp["fusion_scoring"]["input"],
                                   exp["fusion_scoring"]["config"])
        self.assertEqual(ctx.exception.code, "E_INVALID_STAGE")


class TestHTTP(PipelineBase):
    def test_http_end_to_end(self):
        from mmpipe.api import create_app
        app = create_app(self.ws_dir)
        config = uvicorn.Config(app, host="127.0.0.1", port=0,
                                log_level="warning")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)
        base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"

        def call(method, path, payload=None):
            data = None if payload is None else json.dumps(payload).encode()
            req = urllib.request.Request(base + path, data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as exc:
                return exc.code, json.loads(exc.read())

        try:
            status, body = call("GET", "/health")
            self.assertEqual(status, 200)
            status, body = call("POST", "/import", {
                "texts": os.path.join(self.sample_dir, "texts.jsonl"),
                "images": os.path.join(self.sample_dir, "images.json"),
                "config": os.path.join(self.sample_dir, "pipeline_config.json")})
            self.assertEqual(status, 200)
            status, body = call("POST", "/run", {})
            self.assertEqual(status, 200)
            for _ in range(100):
                _, body = call("GET", "/run/status")
                if not body["running"]:
                    break
                time.sleep(0.05)
            self.assertIsNone(body["last_error"])
            status, body = call("GET", "/status")
            self.assertEqual(body["head_seq"], 4)
            status, body = call("GET", "/aggregate")
            self.assertEqual(status, 200)
            self.assertIn("t003",
                          body["aggregate"]["summary"]["conflict_sample_ids"])
            status, body = call("POST", "/stop")
            self.assertEqual(status, 200)
            self.assertTrue(body["stop_requested"])
        finally:
            server.should_exit = True
            thread.join(timeout=10)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestCrashRecovery(PipelineBase):
    def test_corrupt_checkpoint_self_heal_and_resume(self):
        from pathlib import Path
        from mmpipe.storage import CP_DIR
        reference = self.full_reference("crash_reference")
        run_pipeline(self.workspace, self.config, self.texts, self.images,
                     StopController(self.workspace))
        corrupt = Path(self.ws_dir) / CP_DIR / "cp_000003.json"
        corrupt.write_text('{"seq":3,"broken"', encoding="utf-8")

        reopened = Workspace(self.ws_dir)
        self.assertEqual(reopened.head_seq(), 2)
        self.assertEqual(
            [Path(q).name for q in reopened.quarantined].sort(),
            ["cp_000003.json", "cp_000004.json"].sort())
        current = reopened.state()["current"]
        self.assertEqual(current["text_encoding"]["status"], "published")
        self.assertNotIn("fusion_scoring",
                         {k for k, v in current.items()
                          if v["status"] == "published"})

        stop = StopController(reopened)
        stop.clear()
        run_pipeline(reopened, self.config, self.texts, self.images, stop=stop)
        self.assertEqual(reopened.head_seq(), 4)
        for stage in STAGES:
            self.assertEqual(
                canonical_dumps(reopened.load_current_product(stage)),
                canonical_dumps(reference.load_current_product(stage)),
                stage)
        self.assertTrue(verify_workspace(reopened)["ok"])
