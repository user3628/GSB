import os
import unittest

from ragwb.incremental import detect_change
from ragwb.io_utils import read_json
from ragwb.pipeline import Pipeline, StopController
from tests.helpers import (copy_data, fresh_run_dir, full_run,
                           make_workspace, modify_kb_paragraph,
                           modify_session_question)


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()

    def test_stop_resumes_byte_identical_to_clean_run(self):
        clean_run = fresh_run_dir(self.data_dir, "clean")
        clean_summary = full_run(self.data_dir, clean_run)

        stopped_run = fresh_run_dir(self.data_dir, "stopped")
        pipeline = Pipeline(self.data_dir, stopped_run)
        # Request a stop before the second turn completes; the runner
        # finishes the current turn unit and then exits.
        pipeline.stop.request_stop()
        partial = pipeline.run(session_ids=["sess-s1-aurora"])
        self.assertTrue(partial["stopped"])

        # The stop flag is file-backed and survives process replacement.
        self.assertTrue(StopController(stopped_run).stop_requested())
        resumed = Pipeline(self.data_dir, stopped_run)
        resumed.stop.clear()
        resumed_summary = resumed.run()
        self.assertEqual(
            resumed_summary["run_digest"], clean_summary["run_digest"])
        for session_id, digest in clean_summary["answer_digests"].items():
            self.assertEqual(
                resumed_summary["answer_digests"][session_id], digest)

        # Checkpoint sequence numbers stay continuous across recovery.
        for session_id in clean_summary["sessions"]:
            clean_state = Pipeline(
                self.data_dir, clean_run).store.load_state(session_id)
            resumed_state = Pipeline(
                self.data_dir, stopped_run).store.load_state(session_id)
            self.assertEqual(clean_state["next_turn"],
                             resumed_state["next_turn"])


class IncrementalTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.clean_run = fresh_run_dir(self.data_dir, "clean")
        full_run(self.data_dir, self.clean_run)

    def test_question_change_invalidates_from_query_stage(self):
        changed = copy_data(self.data_dir, "-changed")
        modify_session_question(
            changed, "sess-s1-aurora", 0, "极光电池的重量是多少？")
        pipeline = Pipeline(changed, self.clean_run)
        turn = sorted(pipeline.sessions["sess-s1-aurora"],
                      key=lambda row: row["turn_index"])[0]
        report = detect_change(pipeline, "sess-s1-aurora", turn,
                               {"entity": None, "doc_id": None})
        self.assertEqual(report.earliest_stage, "query_rewrite")
        self.assertEqual(
            report.invalid_stages,
            ["query_rewrite", "retrieval", "alignment",
             "answer_assembly"])
        self.assertEqual(
            report.reasons["query_rewrite"],
            "input_content_fingerprint_changed")

    def test_kb_version_change_invalidates_from_retrieval(self):
        changed = copy_data(self.data_dir, "-kbchanged")
        modify_kb_paragraph(changed, "kb-0001", version=9)
        pipeline = Pipeline(changed, self.clean_run)
        turn = sorted(pipeline.sessions["sess-s1-aurora"],
                      key=lambda row: row["turn_index"])[0]
        report = detect_change(pipeline, "sess-s1-aurora", turn,
                               {"entity": None, "doc_id": None})
        self.assertEqual(report.earliest_stage, "retrieval")
        self.assertIn("query_rewrite", report.reused_stages)
        self.assertNotIn("retrieval", report.reused_stages)

    def test_incremental_rebuild_matches_clean_build(self):
        from ragwb.incremental import rebuild_incremental
        changed = copy_data(self.data_dir, "-rebuild")
        # Reproduce identical inputs -> the incremental run must match the
        # clean build byte-for-byte.
        changed_run = fresh_run_dir(changed, "rebuild-run")
        result = rebuild_incremental(
            Pipeline(changed, changed_run),
            data_dir_baseline=self.data_dir,
            run_dir_clean=self.clean_run,
            changed_data_dir=changed,
            changed_run_dir=changed_run)
        self.assertEqual(result["status"], "match")
        self.assertEqual(
            result["run_digest"],
            read_json(os.path.join(self.clean_run, "summary.json"))[
                "run_digest"])


if __name__ == "__main__":
    unittest.main()
