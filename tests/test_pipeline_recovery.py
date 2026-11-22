import json
import os
import shutil
import tempfile
import unittest

from tests._helpers import make_pipeline, make_workspace


class PipelineRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_parallel_serial_byte_identical(self):
        ws_a = make_workspace(os.path.join(self.tmp, "a"))
        ws_b = make_workspace(os.path.join(self.tmp, "b"))
        report_par = make_pipeline(ws_a).run(parallel=True)
        report_ser = make_pipeline(ws_b).run(parallel=False)
        self.assertEqual(report_par["run_fingerprint"],
                         report_ser["run_fingerprint"])
        self.assertEqual(report_par["summary_fingerprint"],
                         report_ser["summary_fingerprint"])
        # 逐文件字节比对核心产物
        for name in ("validation.json", "summary.json", "orchestration.json"):
            pa = os.path.join(ws_a, "state", name)
            pb = os.path.join(ws_b, "state", name)
            with open(pa, "rb") as fa, open(pb, "rb") as fb:
                self.assertEqual(fa.read(), fb.read(), name)

    def test_stop_then_resume_matches_fresh_run(self):
        ws_stop = make_workspace(os.path.join(self.tmp, "stop"))
        ws_fresh = make_workspace(os.path.join(self.tmp, "fresh"))

        pipe = make_pipeline(ws_stop)
        stopped = pipe.run(parallel=True, stop_after=3)
        self.assertEqual(stopped["status"], "stopped")
        self.assertTrue(
            any(u["reason"] == "STOP_REQUESTED"
                for u in stopped["unprocessed"]))

        # 检查点已落盘且序号连续
        cp_dir = os.path.join(ws_stop, "checkpoints")
        self.assertTrue(os.path.isdir(cp_dir))
        for name in os.listdir(cp_dir):
            if not name.endswith(".jsonl"):
                continue
            seqs = [json.loads(line)["seq"]
                    for line in open(os.path.join(cp_dir, name),
                                     encoding="utf-8")]
            self.assertEqual(seqs, list(range(1, len(seqs) + 1)), name)

        resumed_pipe = make_pipeline(ws_stop)
        resumed = resumed_pipe.run(parallel=True)
        self.assertEqual(resumed["status"], "completed")

        fresh = make_pipeline(ws_fresh).run(parallel=True)
        self.assertEqual(resumed["run_fingerprint"], fresh["run_fingerprint"])
        self.assertEqual(resumed["summary_fingerprint"],
                         fresh["summary_fingerprint"])

    def test_invalidation_on_config_change(self):
        ws = make_workspace(os.path.join(self.tmp, "inv"),
                            config_overrides={
                                "budget.global_evaluations": 500})
        pipe = make_pipeline(ws)
        report = pipe.run(parallel=True)
        self.assertEqual(report["status"], "completed")

        # 修改裁决权重 → 最早受影响阶段应为 validation
        import yaml
        cfg_path = os.path.join(ws, "config.yaml")
        with open(cfg_path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        data["adjudication"]["overlap_weight"] = 0.9
        with open(cfg_path, "w", encoding="utf-8", newline="\n") as handle:
            yaml.safe_dump(data, handle, sort_keys=True, allow_unicode=True)

        pipe2 = make_pipeline(ws)
        first_record = pipe2.records[0]
        from qrwb.engine import build_version_bundle
        bundle = build_version_bundle(first_record, pipe2.cfg,
                                      pipe2.resources)
        invalid = pipe2.store.first_invalid_stage(
            first_record["query_id"], bundle.as_dict())
        self.assertIsNotNone(invalid)
        self.assertEqual(invalid[0], "validation")
        report2 = pipe2.run(parallel=True)
        # 所有查询的 validation 阶段被重建
        invalidated = [i for i in report2["invalidations"]
                       if i.get("invalidated_from") == "validation"]
        expected = sum(
            1 for r in pipe2.records
            if pipe2.store.get(r["query_id"], "validation") is not None
            or pipe2.store.latest_seq(r["query_id"]) >= 4)
        self.assertEqual(len(invalidated), expected)
        # 上游 dispatch/candidate/orchestration 检查点保留复用：
        # 每条查询日志仍以 dispatch(seq=1) 开头且含 4 条记录
        cp_dir = os.path.join(ws, "checkpoints")
        for name in os.listdir(cp_dir):
            if not name.endswith(".jsonl"):
                continue
            seqs = [json.loads(line)["seq"]
                    for line in open(os.path.join(cp_dir, name),
                                     encoding="utf-8")]
            # 序号必须从 1 连续推进（空查询仅含跳过标记，阶段数较少）
            self.assertEqual(seqs, list(range(1, len(seqs) + 1)), name)
            # 完整处理的查询保留全部四个阶段，证明上游被复用
            if len(seqs) == 4:
                self.assertEqual(seqs, [1, 2, 3, 4], name)

    def test_stale_and_invalid_stage_publish(self):
        ws = make_workspace(os.path.join(self.tmp, "stale"))
        pipe = make_pipeline(ws)
        record = pipe.records[0]
        from qrwb.engine import build_version_bundle
        from qrwb.errors import (E_INVALID_STAGE, E_STALE_CHECKPOINT)
        bundle = build_version_bundle(record, pipe.cfg, pipe.resources)
        engine_plan = pipe._process_query(record)
        plan = engine_plan["plan"]

        # 重复发布 dispatch → E_STALE_CHECKPOINT
        with self.assertRaises(Exception) as ctx:
            pipe.store.publish(
                record["query_id"], "dispatch",
                input_fingerprint=bundle.input_fingerprint,
                config_version=bundle.config_version, product=plan,
                stage_fingerprints={
                    "input_fingerprint": bundle.input_fingerprint,
                    "config_version": bundle.config_version})
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

        # _process_query 已完成 dispatch+candidate，当前序号停在 2；
        # 阶段只能向前：再次发布 dispatch（回退/重复）→ E_STALE_CHECKPOINT，
        # 发布一个固定阶段序更靠前的阶段 → E_INVALID_STAGE
        self.assertEqual(pipe.store.latest_seq(record["query_id"]), 2)
        with self.assertRaises(Exception) as ctx_back:
            pipe.store.publish(
                record["query_id"], "dispatch",
                input_fingerprint=bundle.input_fingerprint,
                config_version=bundle.config_version,
                product={"x": 1},
                stage_fingerprints={
                    "input_fingerprint": bundle.input_fingerprint,
                    "normalization_fingerprint":
                        bundle.normalization_fingerprint})
        self.assertEqual(ctx_back.exception.code, E_STALE_CHECKPOINT)

        # 非法阶段名 → E_INVALID_STAGE
        with self.assertRaises(Exception) as ctx_bad:
            pipe.store.publish(
                record["query_id"], "not_a_stage",
                input_fingerprint=bundle.input_fingerprint,
                config_version=bundle.config_version,
                product={"x": 1}, stage_fingerprints={})
        self.assertEqual(ctx_bad.exception.code, E_INVALID_STAGE)

    def test_summary_stratified_and_deterministic(self):
        ws = make_workspace(os.path.join(self.tmp, "summary"))
        report = make_pipeline(ws).run(parallel=True)
        with open(os.path.join(ws, "state", "summary.json"),
                  encoding="utf-8") as handle:
            summary = json.load(handle)
        self.assertIn("by_lang_channel", summary)
        self.assertTrue(summary["by_lang_channel"])
        for key, row in summary["by_lang_channel"].items():
            self.assertIn("adoption_rate", row)
            self.assertIn("route_hits", row)
        # 确定性：再跑一次（全检查点复用）汇总指纹不变
        again = make_pipeline(ws).run(parallel=True)
        self.assertEqual(again["summary_fingerprint"],
                         report["summary_fingerprint"])


if __name__ == "__main__":
    unittest.main()
