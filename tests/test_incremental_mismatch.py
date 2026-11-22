import unittest

from ragwb.errors import E_INCREMENTAL_MISMATCH, WorkbenchError
from ragwb.incremental import rebuild_incremental
from ragwb.io_utils import write_json
from ragwb.pipeline import Pipeline
from tests.helpers import (copy_data, fresh_run_dir, full_run,
                           load_config, make_workspace, write_config)


class IncrementalMismatchTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.clean_run = fresh_run_dir(self.data_dir, "clean")
        full_run(self.data_dir, self.clean_run)

    def test_mismatch_raises_stable_business_code(self):
        changed = copy_data(self.data_dir, "-mismatch")
        changed_run = fresh_run_dir(changed, "changed-run")
        pipeline = Pipeline(changed, changed_run)
        # Tamper with the clean reference summary after the changed run has
        # been computed, forcing a byte-level divergence on comparison.
        original_run = pipeline.run()
        tampered = dict(original_run)
        tampered["run_digest"] = "0" * 64
        write_json(
            f"{changed_run}/summary.json", tampered, pretty=True)
        with self.assertRaises(WorkbenchError) as caught:
            rebuild_incremental(
                pipeline, data_dir_baseline=self.data_dir,
                run_dir_clean=changed_run,
                changed_data_dir=changed,
                changed_run_dir=fresh_run_dir(changed, "other-run"))
        self.assertEqual(caught.exception.code, E_INCREMENTAL_MISMATCH)

    def test_each_config_change_is_observed_in_fingerprints(self):
        from ragwb.incremental import detect_change
        changes = [
            ("retrieval", lambda cfg: cfg["retrieval"].update(
                {"depth": 99})),
            ("alignment_tolerance", lambda cfg: cfg["retrieval"].update(
                {"alignment_tolerance": 0.5})),
            ("answer", lambda cfg: cfg["answer"].update(
                {"template": "新答案：{answer_body}|{references}"})),
            ("scale_tiers", lambda cfg: cfg["scale_tiers"]["small"].update(
                {"max_paragraphs": 5})),
        ]
        turn_index = 0
        for label, mutate in changes:
            changed = copy_data(self.data_dir, f"-cfg-{label}")
            config = load_config(changed)
            mutate(config)
            write_config(changed, config)
            pipeline = Pipeline(changed, self.clean_run)
            turn = sorted(pipeline.sessions["sess-s1-aurora"],
                          key=lambda row: row["turn_index"])[turn_index]
            report = detect_change(
                pipeline, "sess-s1-aurora", turn,
                {"entity": None, "doc_id": None})
            self.assertTrue(
                report.invalid_stages,
                f"config change {label} must invalidate something")
            self.assertTrue(
                all(report.reasons[stage] in (
                    "configuration_version_changed",
                    "input_content_fingerprint_changed",
                    "upstream_stage_invalidated")
                    for stage in report.invalid_stages))


if __name__ == "__main__":
    unittest.main()
