from __future__ import annotations

import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from sumengine.config import config_from_file, normalize_config
from sumengine.engine import SummaryEngine
from sumengine.samples import load_documents
from sumengine.utils import canonical_bytes

from .common import SAMPLES, workspace_with_samples


class EngineIntegrationTests(unittest.TestCase):
    def test_serial_parallel_export_is_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            serial = workspace_with_samples(root, "serial")
            parallel = workspace_with_samples(root, "parallel")
            serial_run = serial.run(mode="serial")
            parallel_run = parallel.run(mode="parallel")
            self.assertEqual(serial_run["schedule"]["accepted_doc_ids"], parallel_run["schedule"]["accepted_doc_ids"])
            self.assertEqual(canonical_bytes(serial_run), canonical_bytes(parallel_run))
            self.assertEqual([doc["doc_id"] for doc in parallel_run["documents"]], sorted(doc["doc_id"] for doc in parallel_run["documents"]))
            serial.export()
            parallel.export()
            serial_bytes = (serial.store.exports_dir / "final_summaries.json").read_bytes()
            parallel_bytes = (parallel.store.exports_dir / "final_summaries.json").read_bytes()
            self.assertEqual(serial_bytes, parallel_bytes)
            self.assertTrue(any(item["code"] == "E_GLOBAL_BUDGET" for item in parallel_run["schedule"]["unprocessed"]))

    def test_unchanged_rerun_uses_cache_and_keeps_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            first = engine.run(mode="serial")
            engine.export()
            first_bytes = (engine.store.exports_dir / "final_summaries.json").read_bytes()
            second = engine.run(mode="serial")
            engine.export()
            second_bytes = (engine.store.exports_dir / "final_summaries.json").read_bytes()
            self.assertEqual(first["status"], "complete")
            self.assertEqual(second["budget_usage"]["evaluations_used"], 0)
            self.assertTrue(all(set(doc["reused_stages"]) == {"extract", "compress", "template", "fusion", "final"} for doc in second["documents"]))
            self.assertEqual(first_bytes, second_bytes)

    def test_saved_config_is_selected_by_new_engine_instances(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            workspace = Path(temp) / "workspace"
            engine = workspace_with_samples(Path(temp))
            config = config_from_file(SAMPLES / "config.yaml")
            config["compression"]["synonym_replacements"]["迅速"] = "很快"
            engine.save_config(config)
            reopened = SummaryEngine(workspace)
            self.assertEqual(reopened.config_path.suffix, ".json")
            self.assertEqual(reopened.get_config()["compression"]["synonym_replacements"]["迅速"], "很快")

    def test_incremental_stage_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            engine.run(mode="serial")
            config = config_from_file(SAMPLES / "config.yaml")
            original_extract = {
                doc["doc_id"]: doc["stage_fingerprints"]["extract"]
                for doc in engine.run(mode="serial", config=config)["documents"]
            }

            synonym_config = deepcopy(config)
            synonym_config["compression"]["synonym_replacements"]["迅速"] = "较快"
            synonym_status = engine.status(config=synonym_config)
            self.assertEqual(synonym_status["documents"][0]["earliest_affected_stage"], "compress")
            synonym_run = engine.run(mode="serial", config=synonym_config)
            for doc in synonym_run["documents"]:
                self.assertEqual(doc["stage_fingerprints"]["extract"], original_extract[doc["doc_id"]])
                self.assertIn("extract", doc["reused_stages"])
                self.assertIn("template", doc["reused_stages"])
                self.assertIn("compress", doc["changed_stages"])
            weight_config = deepcopy(synonym_config)
            weight_config["fusion"]["weights"]["coverage"] = 1.75
            weight_status = engine.status(config=weight_config)
            self.assertEqual(weight_status["documents"][0]["earliest_affected_stage"], "fusion")
            weight_run = engine.run(mode="serial", config=weight_config)
            for doc in weight_run["documents"]:
                self.assertEqual(set(doc["reused_stages"]), {"extract", "compress", "template"})
                self.assertEqual(doc["changed_stages"], ["fusion", "final"])

    def test_document_content_change_rebuilds_route_stages(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            engine.run(mode="serial")
            documents = load_documents(SAMPLES / "documents.jsonl")
            changed = False
            for doc in documents:
                if doc["doc_id"] == "news-zh-002":
                    doc["body"] += "补充说明：新增一项公开数据核验流程。"
                    changed = True
            self.assertTrue(changed)
            engine.import_documents(documents)
            run = engine.run(mode="serial")
            target = next(doc for doc in run["documents"] if doc["doc_id"] == "news-zh-002")
            self.assertEqual(target["reused_stages"], [])
            self.assertEqual(target["changed_stages"], ["extract", "compress", "template", "fusion", "final"])

    def test_switching_configuration_back_does_not_look_like_duplicate_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            original = config_from_file(SAMPLES / "config.yaml")
            changed = deepcopy(original)
            changed["compression"]["synonym_replacements"]["迅速"] = "较快"
            engine.run(mode="serial", config=original)
            engine.run(mode="serial", config=changed)
            restored = engine.run(mode="serial", config=original)
            self.assertEqual(restored["status"], "complete")
            self.assertFalse(restored["documents_with_errors"])
            self.assertEqual(len(restored["documents"]), 4)

    def test_edit_versions_and_review_flag_after_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            engine.run(mode="serial")
            first = engine.edit_summary("news-zh-002", "第一版人工摘要，保留试点范围与公开数据承诺。")
            second = engine.edit_summary("news-zh-002", "第二版人工摘要，增加后续复核安排。")
            self.assertEqual((first["edit_version"], second["edit_version"]), (1, 2))
            exported = engine.export_set()
            edited = next(doc for doc in exported["documents"] if doc["doc_id"] == "news-zh-002")
            self.assertEqual(edited["summary"], "第二版人工摘要，增加后续复核安排。")
            self.assertEqual(edited["edit_version"], 2)

            documents = load_documents(SAMPLES / "documents.jsonl")
            for doc in documents:
                if doc["doc_id"] == "news-zh-002":
                    doc["title"] = doc["title"] + "（更新）"
            engine.import_documents(documents)
            run = engine.run(mode="serial")
            rebuilt = next(doc for doc in run["documents"] if doc["doc_id"] == "news-zh-002")
            self.assertEqual(rebuilt["summary"], "第二版人工摘要，增加后续复核安排。")
            self.assertTrue(rebuilt["final"]["needs_review"])
            self.assertEqual(rebuilt["final"]["review_reason"], "INPUT_OR_CONFIG_CHANGED_SINCE_EDIT")

    def test_stop_then_resume_matches_full_run_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            full = workspace_with_samples(root, "full")
            stopped = workspace_with_samples(root, "stopped")
            full.run(mode="serial")
            full.export()
            stop_run = stopped.run(mode="serial", stop_after_docs=2)
            self.assertEqual(stop_run["status"], "stopped")
            self.assertTrue(stop_run["deferred"])
            resume_run = stopped.resume(mode="serial")
            self.assertEqual(resume_run["status"], "complete")
            self.assertFalse(resume_run["deferred"])
            stopped.export()
            self.assertEqual(
                (full.store.exports_dir / "final_summaries.json").read_bytes(),
                (stopped.store.exports_dir / "final_summaries.json").read_bytes(),
            )

    def test_preexisting_stop_request_is_honored_and_resume_clears_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            engine.request_stop("before-run")
            stopped = engine.run(mode="serial")
            self.assertEqual(stopped["status"], "stopped")
            self.assertEqual(len(stopped["deferred"]), 4)
            self.assertFalse(stopped["documents"])
            resumed = engine.resume(mode="serial")
            self.assertEqual(resumed["status"], "complete")
            self.assertFalse(engine.store.stop_requested())

    def test_worker_error_is_isolated_and_latency_error_is_structured(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            good = load_documents(SAMPLES / "documents.jsonl")[1]
            bad = {
                "doc_id": "bad-short",
                "title": "短讯",
                "body": "短讯。",
                "language": "zh",
                "source": "desk",
                "target_length": "short",
            }
            engine.import_documents([bad, good])
            run = engine.run(mode="serial")
            self.assertEqual(run["status"], "complete_with_errors")
            self.assertEqual(run["documents_with_errors"][0]["code"], "E_INPUT_TOO_SHORT")
            self.assertEqual(run["documents_with_errors"][0]["doc_id"], "bad-short")
            self.assertEqual([doc["doc_id"] for doc in run["documents"]], [good["doc_id"]])

            config = normalize_config({})
            config["budget"]["latency_budget_ms"] = 1
            latency = engine.run(mode="serial", config=config)
            self.assertFalse(latency["ok"])
            self.assertEqual(latency["status"], "error")
            self.assertEqual(latency["error"]["code"], "E_LATENCY_BUDGET")


if __name__ == "__main__":
    unittest.main()
