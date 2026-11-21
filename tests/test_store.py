from __future__ import annotations

import tempfile
import unittest
import multiprocessing
from pathlib import Path

from sumengine.errors import EngineError
from sumengine.store import StateStore
from sumengine.utils import digest


def checkpoint_record(
    doc_id: str = "doc-1",
    stage: str = "extract",
    marker: str = "a",
    edit_version: int = 0,
) -> dict:
    product = {"stage": stage, "marker": marker, "doc_id": doc_id}
    return {
        "stage": stage,
        "doc_id": doc_id,
        "input_fingerprint": digest({"doc": doc_id}),
        "stage_fingerprint": digest({"stage": stage, "marker": marker}),
        "config_version": digest({"config": 1}),
        "schedule_fingerprint": digest({"budget": 1}),
        "edit_version": edit_version,
        "product_digest": digest(product),
        "needs_review": False,
    }


def _concurrent_publish(args: tuple[str, str]) -> int:
    root, marker = args
    store = StateStore(Path(root))
    store.initialize()
    return int(store.publish_checkpoint(checkpoint_record(marker=marker))["sequence"])


class StateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = StateStore(self.root)
        self.store.initialize()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_checkpoint_sequence_and_atomic_journal(self) -> None:
        first = self.store.publish_checkpoint(checkpoint_record(marker="a"))
        second = self.store.publish_checkpoint(checkpoint_record(stage="final", marker="b"))
        self.assertEqual((first["sequence"], second["sequence"]), (1, 2))
        journal = self.store.load_checkpoints()
        self.assertEqual(journal["next_sequence"], 3)
        self.assertEqual(len(journal["records"]), 2)
        self.assertTrue(all(record["product_digest"] for record in journal["records"]))
        self.assertEqual(list(self.root.joinpath("state").glob(".*.tmp")), [])

    def test_duplicate_stale_and_cross_document_publications(self) -> None:
        record = checkpoint_record(marker="a")
        self.store.publish_checkpoint(record)
        with self.assertRaises(EngineError) as duplicate:
            self.store.publish_checkpoint(record)
        self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")

        stale = checkpoint_record(stage="final", marker="b")
        stale["sequence"] = 1
        with self.assertRaises(EngineError) as stale_error:
            self.store.publish_checkpoint(stale)
        self.assertEqual(stale_error.exception.code, "E_STALE_CHECKPOINT")

        crossed = checkpoint_record(doc_id="doc-2", stage="final", marker="c")
        with self.assertRaises(EngineError) as cross_error:
            self.store.publish_checkpoint(crossed, expected_doc_id="doc-1")
        self.assertEqual(cross_error.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(len(self.store.checkpoint_records()), 1)

    def test_older_edit_version_cannot_overwrite_newer_publication(self) -> None:
        self.store.publish_checkpoint(checkpoint_record(stage="final", marker="v2", edit_version=2))
        older = checkpoint_record(stage="final", marker="v1-late", edit_version=1)
        with self.assertRaises(EngineError) as context:
            self.store.publish_checkpoint(older)
        self.assertEqual(context.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(self.store.checkpoint_records()[-1]["edit_version"], 2)

    def test_products_and_edit_versions_are_monotonic(self) -> None:
        products = {
            "extract": {
                "fingerprint": "fp-extract",
                "edit_version": 0,
                "product": {"route": "extractive", "text": "hello"},
            },
            "final": {
                "fingerprint": "fp-final",
                "edit_version": 0,
                "product": {"summary": "hello"},
            },
        }
        self.store.save_products(
            "doc-1",
            input_fingerprint="input",
            config_version="config",
            schedule_fingerprint="schedule",
            stages=products,
        )
        loaded = self.store.load_products("doc-1")
        self.assertEqual(loaded["stages"]["extract"]["product"]["text"], "hello")
        self.assertEqual(loaded["stages"]["extract"]["product_digest"], digest({"route": "extractive", "text": "hello"}))

        first = self.store.append_edit("doc-1", "first", base_fusion_fingerprint="fusion")
        second = self.store.append_edit("doc-1", "second", base_fusion_fingerprint="fusion")
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        self.assertEqual([item["version"] for item in second["history"]], [1, 2])
        self.assertEqual(self.store.get_edit("doc-1")["text"], "second")

    def test_file_lock_serializes_concurrent_process_publication(self) -> None:
        context = multiprocessing.get_context("spawn")
        with context.Pool(processes=4) as pool:
            sequences = pool.map(
                _concurrent_publish,
                [(str(self.root), f"marker-{index}") for index in range(4)],
            )
        self.assertEqual(sorted(sequences), [1, 2, 3, 4])
        journal = self.store.load_checkpoints()
        self.assertEqual(journal["next_sequence"], 5)
        self.assertEqual(len(journal["records"]), 4)
        self.assertEqual(sorted(record["sequence"] for record in journal["records"]), [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
