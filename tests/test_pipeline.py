from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from telemetry_align.errors import TelemetryError
from telemetry_align.samples import mutate_source
from telemetry_align.workspace import AlignmentWorkspace


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _workspace(self, name: str) -> AlignmentWorkspace:
        workspace = AlignmentWorkspace(self.root / name)
        workspace.init_demo(reset=True)
        return workspace

    def test_full_run_is_byte_deterministic(self) -> None:
        first = self._workspace("first")
        second = self._workspace("second")
        result_first = first.align()
        result_second = second.align()
        self.assertEqual(result_first["status"], "completed")
        self.assertEqual(result_first["fingerprints"], result_second["fingerprints"])
        self.assertEqual(
            (first.root / "out/alignment_summary.json").read_bytes(),
            (second.root / "out/alignment_summary.json").read_bytes(),
        )
        first_head = first.store.head()
        second_head = second.store.head()
        for stage, key in (("normalize", "grid"), ("fusion", None), ("summary", None)):
            first_entry = first_head["catalog"][stage]
            second_entry = second_head["catalog"][stage]
            if key:
                first_entry = first_entry[key]
                second_entry = second_entry[key]
            self.assertEqual(
                (first.root / first_entry["artifact_path"]).read_bytes(),
                (second.root / second_entry["artifact_path"]).read_bytes(),
            )
        self.assertEqual(
            first.store.summary()["sequence"],
            [1, 2, 3, 4],
        )

    def test_conflicts_gaps_and_tie_break(self) -> None:
        workspace = self._workspace("conflicts")
        workspace.align()
        conflicts = workspace.conflicts()["conflicts"]
        at_1100 = [
            item
            for item in conflicts
            if item["timestamp_ms"] == 1100 and item["channel"] == "temperature_c"
        ]
        self.assertEqual(len(at_1100), 1)
        self.assertEqual(at_1100[0]["winner"], "sensor_b")
        self.assertTrue(at_1100[0]["priority_tie"])
        self.assertEqual(
            {participant["source_id"] for participant in at_1100[0]["participants"]},
            {"sensor_b", "sensor_c"},
        )
        gaps = workspace.gaps()
        self.assertEqual(gaps["count"], 6)
        self.assertEqual(len(gaps["point_gaps"]), 6)
        self.assertEqual(
            {gap["index"] for gap in gaps["point_gaps"]}, {22, 23, 24}
        )
        summary = workspace.summary()
        self.assertEqual(summary["counts"]["point_gap_count"], 3)
        gap_points = [p for p in summary["alignment"] if p["status"] == "gap"]
        self.assertEqual(len(gap_points), 3)
        self.assertTrue(all(p["missing_channels"] for p in gap_points))

    def test_out_of_range_is_cleaned_without_failing(self) -> None:
        workspace = self._workspace("cleaning")
        workspace.align()
        cleaning = workspace.cleaning()
        report = cleaning["reports"]["sensor_a"]
        self.assertGreaterEqual(report["out_of_range_count"], 1)
        self.assertEqual(report["out_of_range"][0]["timestamp_ms"], 500)
        self.assertEqual(report["out_of_range"][0]["channel"], "temperature_c")

    def test_incremental_source_then_config_weight(self) -> None:
        workspace = self._workspace("incremental")
        first = workspace.align()
        before = workspace.summary()
        mutation = workspace.mutate_source_and_rebuild(sensor_id="sensor_a")
        incremental = mutation["alignment"]
        self.assertEqual(incremental["invalidations"]["earliest_stage"], "normalize")
        self.assertIn("normalize:sensor_b", incremental["reused"])
        self.assertIn("normalize:sensor_c", incremental["reused"])
        self.assertIn("resample:sensor_b", incremental["reused"])
        self.assertIn("resample:sensor_c", incremental["reused"])
        self.assertEqual(incremental["checkpoint_sequence"], [1, 2, 3, 4, 5, 6, 7, 8])
        after = workspace.summary()
        self.assertEqual(
            before["fusion_result_digest"] == after["fusion_result_digest"], False
        )

        config_path = workspace.root / "data/configs/config_v2.yaml"
        data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        data["fusion"]["weights"]["sensor_a"] = 0.61
        config_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        weight_run = workspace.align()
        self.assertEqual(weight_run["invalidations"]["earliest_stage"], "fusion")
        self.assertEqual(weight_run["executed_stages"], ["fusion", "summary"])
        self.assertEqual(weight_run["checkpoint_sequence"], list(range(1, 11)))
        noop = workspace.align()
        self.assertEqual(noop["status"], "noop")
        self.assertEqual(noop["checkpoint_sequence"], list(range(1, 11)))

    def test_tolerance_and_grid_changes_select_earliest_stage(self) -> None:
        workspace = self._workspace("stage-selection")
        workspace.align()
        config_path = workspace.root / "data/configs/config_v2.yaml"
        data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        data["tolerance_ms"] = 150
        config_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        tolerance_run = workspace.align()
        self.assertEqual(tolerance_run["invalidations"]["earliest_stage"], "resample")
        self.assertEqual(
            tolerance_run["executed_stages"], ["resample", "fusion", "summary"]
        )
        self.assertEqual(tolerance_run["checkpoint_sequence"], [1, 2, 3, 4, 5, 6, 7])

        data["grid"]["step_ms"] = 50
        config_path.write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        grid_run = workspace.align()
        self.assertEqual(grid_run["invalidations"]["earliest_stage"], "normalize")
        self.assertEqual(
            grid_run["executed_stages"],
            ["normalize", "resample", "fusion", "summary"],
        )
        self.assertEqual(
            grid_run["checkpoint_sequence"], list(range(1, 12))
        )

    def test_stale_result_is_refused_before_rebuild(self) -> None:
        workspace = self._workspace("stale-result")
        workspace.align()
        mutate_source(workspace.root, "sensor_a")
        with self.assertRaises(TelemetryError) as raised:
            workspace.summary()
        self.assertEqual(raised.exception.code, "E_STALE_RESULT")
        with self.assertRaises(TelemetryError) as raised:
            workspace.conflicts()
        self.assertEqual(raised.exception.code, "E_STALE_RESULT")
        rebuilt = workspace.align()
        self.assertEqual(rebuilt["status"], "completed")
        self.assertIsNotNone(workspace.summary())

    def test_v1_matches_v2_and_roundtrip_does_not_rebuild(self) -> None:
        native = self._workspace("native")
        v1 = self._workspace("v1")
        native_run = native.align(config_name="v2")
        v1_run = v1.align(config_name="v1")
        native_summary = native.summary()
        v1_summary = v1.summary()
        self.assertEqual(
            native_summary["grid"]["fingerprint"],
            v1_summary["grid"]["fingerprint"],
        )
        self.assertEqual(
            native_summary["fusion_result_digest"],
            v1_summary["fusion_result_digest"],
        )
        upgrade = v1.upgrade_v1_config()
        self.assertTrue(upgrade["equivalent"])
        v1.import_inputs(
            configs={"v2_upgraded": {"path": upgrade["path"]}},
            active_config="v2_upgraded",
        )
        upgraded_run = v1.align(config_name="v2_upgraded")
        self.assertEqual(upgraded_run["status"], "noop")
        self.assertEqual(upgraded_run["checkpoint_sequence"], v1_run["checkpoint_sequence"])
        self.assertEqual(
            native_run["fingerprints"]["fusion"],
            v1_run["fingerprints"]["fusion"],
        )

    def test_duplicate_and_non_monotonic_time_return_e_time_invalid(self) -> None:
        workspace = self._workspace("bad-time")
        path = workspace.root / "data/sources/sensor_a.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        first = json.loads(lines[0])
        second = json.loads(lines[1])
        second["timestamp_ms"] = first["timestamp_ms"]
        lines[1] = json.dumps(second, ensure_ascii=False, sort_keys=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(TelemetryError) as raised:
            workspace.align()
        error = raised.exception
        self.assertEqual(error.code, "E_TIME_INVALID")
        source_details = error.details["sources"][0]
        self.assertEqual(source_details["sensor_id"], "sensor_a")
        self.assertEqual(source_details["duplicates"][0]["timestamp_ms"], 0)
        diagnostics = workspace.root / error.details["diagnostics_path"]
        self.assertTrue(diagnostics.exists())
        self.assertTrue(diagnostics.read_text(encoding="utf-8").strip().startswith("{"))

        workspace = self._workspace("bad-order")
        path = workspace.root / "data/sources/sensor_b.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        lines[1], lines[2] = lines[2], lines[1]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(TelemetryError) as raised:
            workspace.align()
        self.assertEqual(raised.exception.code, "E_TIME_INVALID")
        detail = raised.exception.details["sources"][0]
        self.assertEqual(detail["sensor_id"], "sensor_b")
        self.assertEqual(detail["non_monotonic"][0]["timestamp_ms"], 150)

    def test_grid_invalid_from_config(self) -> None:
        for step in (0, -100, 500):
            with self.subTest(step=step):
                workspace = self._workspace(f"grid-{step}")
                path = workspace.root / "data/configs/config_v2.yaml"
                data = yaml.safe_load(path.read_text(encoding="utf-8"))
                data["grid"]["step_ms"] = step
                path.write_text(
                    yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
                    encoding="utf-8",
                )
                with self.assertRaises(TelemetryError) as raised:
                    workspace.align()
                self.assertEqual(raised.exception.code, "E_GRID_INVALID")
                self.assertEqual(workspace.store.sequence(), [])

    def test_stop_resume_matches_clean_run_bytes(self) -> None:
        clean = self._workspace("clean")
        interrupted = self._workspace("interrupted")
        clean.align()
        interrupted.request_stop("resample")
        stopped = interrupted.align()
        self.assertEqual(stopped["status"], "stopped")
        self.assertEqual(stopped["executed_stages"], ["normalize", "resample"])
        resumed = interrupted.align(resume=True)
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["executed_stages"], ["fusion", "summary"])
        self.assertEqual(interrupted.store.sequence(), [1, 2, 3, 4])
        for relative in (
            "out/alignment_summary.json",
            "out/cleaning_report.json",
        ):
            self.assertEqual(
                (clean.root / relative).read_bytes(),
                (interrupted.root / relative).read_bytes(),
            )
        self.assertEqual(clean.summary()["fusion_result_digest"], interrupted.summary()["fusion_result_digest"])


if __name__ == "__main__":
    unittest.main()

