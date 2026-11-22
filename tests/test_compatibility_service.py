from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from slo_budget import codec
from slo_budget.errors import SloError
from slo_budget.service import SloService

from tests.support import (
    AS_OF_MS,
    CURRENT_BIN,
    CURRENT_JSONL,
    FUTURE_BIN,
    FUTURE_JSONL,
    LEGACY_BIN,
    LEGACY_JSONL,
    EVENTS,
    metric_map,
)


class CompatibilityServiceTests(unittest.TestCase):
    def test_compatibility_matrix_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with SloService(Path(tmp) / "matrix.db", clock_ms=lambda: AS_OF_MS) as service:
                document = service.compatibility()
                self.assertEqual(document["current_version"], 2)
                self.assertEqual(document["matrix"]["binary"]["1"]["verdict"], "DEGRADE")
                self.assertEqual(document["matrix"]["binary"]["2"]["verdict"], "ACCEPT")
                self.assertEqual(document["matrix"]["binary"]["3"]["verdict"], "REJECT")
                for path, verdict, code in (
                    (CURRENT_BIN, "ACCEPT", None),
                    (CURRENT_JSONL, "ACCEPT", None),
                    (LEGACY_BIN, "DEGRADE", None),
                    (LEGACY_JSONL, "DEGRADE", None),
                    (FUTURE_BIN, "REJECT", "E_FORMAT_UNSUPPORTED"),
                    (FUTURE_JSONL, "REJECT", "E_FORMAT_UNSUPPORTED"),
                ):
                    result = service.preflight(path)
                    self.assertEqual(result["verdict"], verdict)
                    self.assertEqual(result["business_code"], code)

    def test_future_import_rejected_and_old_state_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with SloService(Path(tmp) / "future.db", clock_ms=lambda: AS_OF_MS + 5) as service:
                service.replace_snapshot(
                    CURRENT_BIN,
                    batch_id="current-base",
                )
                before_state = service.storage.active_snapshot()
                before_records = service.storage.window_records()
                with self.assertRaises(SloError) as caught:
                    service.replace_snapshot(
                        FUTURE_BIN,
                        batch_id="future-rejected",
                    )
                self.assertEqual(caught.exception.code, "E_FORMAT_UNSUPPORTED")
                self.assertEqual(service.storage.active_snapshot(), before_state)
                self.assertEqual(service.storage.window_records(), before_records)
                report = service.storage.get_import_report("future-rejected")
                self.assertEqual(report["status"], "ABORTED")
                self.assertEqual(report["business_code"], "E_FORMAT_UNSUPPORTED")

    def test_future_import_on_empty_database_imports_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with SloService(Path(tmp) / "empty.db", clock_ms=lambda: AS_OF_MS) as service:
                with self.assertRaises(SloError) as caught:
                    service.import_file(FUTURE_JSONL, batch_id="future-jsonl")
                self.assertEqual(caught.exception.code, "E_FORMAT_UNSUPPORTED")
                self.assertEqual(service.storage.active_snapshot()["snapshot_sequence"], 0)
                self.assertEqual(service.storage.window_records(), ())

    def test_legacy_degrade_preserves_sli_burn_and_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            current_service = SloService(Path(tmp) / "current.db", clock_ms=lambda: AS_OF_MS)
            legacy_service = SloService(Path(tmp) / "legacy.db", clock_ms=lambda: AS_OF_MS)
            try:
                current_service.replace_snapshot(CURRENT_BIN, batch_id="current")
                legacy_service.replace_snapshot(LEGACY_JSONL, batch_id="legacy")
                current = current_service.query(
                    service="checkout-api",
                    sli_type="availability",
                    as_of_ms=AS_OF_MS,
                )
                legacy = legacy_service.query(
                    service="checkout-api",
                    sli_type="availability",
                    as_of_ms=AS_OF_MS,
                )
                self.assertEqual(current["alert"]["grade"], "CRITICAL")
                self.assertEqual(legacy["alert"]["grade"], "CRITICAL")
                current_windows = metric_map(current)
                legacy_windows = metric_map(legacy)
                for window_ms in current_windows:
                    self.assertEqual(current_windows[window_ms]["sli"], legacy_windows[window_ms]["sli"])
                    self.assertEqual(
                        current_windows[window_ms]["error_budget"]["burn_rate"],
                        legacy_windows[window_ms]["error_budget"]["burn_rate"],
                    )
                    self.assertEqual(legacy_windows[window_ms]["counts"]["late_count"], 0)
            finally:
                current_service.close()
                legacy_service.close()

    def test_import_binary_and_jsonl_establish_same_internal_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary_db = Path(tmp) / "binary.db"
            jsonl_db = Path(tmp) / "jsonl.db"
            with SloService(binary_db, clock_ms=lambda: AS_OF_MS) as service:
                service.import_file(CURRENT_BIN, batch_id="bin")
                binary_records = service.storage.window_records()
                binary_state = service.storage.active_snapshot()
            with SloService(jsonl_db, clock_ms=lambda: AS_OF_MS) as service:
                service.import_file(CURRENT_JSONL, batch_id="jsonl")
                jsonl_records = service.storage.window_records()
                jsonl_state = service.storage.active_snapshot()
            self.assertEqual(
                [record.internal_dict() for record in binary_records],
                [record.internal_dict() for record in jsonl_records],
            )
            self.assertEqual(binary_state["snapshot_sequence"], jsonl_state["snapshot_sequence"])

    def test_invalid_count_aborts_import_report(self) -> None:
        invalid = Path(__file__).resolve().parents[1] / "samples" / "invalid" / "count-invalid.jsonl"
        with tempfile.TemporaryDirectory() as tmp:
            with SloService(Path(tmp) / "invalid.db", clock_ms=lambda: AS_OF_MS) as service:
                with self.assertRaises(SloError) as caught:
                    service.import_file(invalid, batch_id="invalid-count")
                self.assertEqual(caught.exception.code, "E_COUNT_INVALID")
                self.assertIsNotNone(caught.exception.offset)
                report = service.storage.get_import_report("invalid-count")
                self.assertEqual(report["status"], "ABORTED")
                self.assertEqual(report["business_code"], "E_COUNT_INVALID")
                self.assertEqual(service.storage.window_records(), ())

    def test_event_file_analysis_is_repeatable_and_crosschecked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with SloService(Path(tmp) / "events.db", clock_ms=lambda: AS_OF_MS) as service:
                result = service.analyze_events_file(
                    events_path=EVENTS,
                    service="checkout-api",
                    sli_type="availability",
                    as_of_ms=AS_OF_MS,
                )
                self.assertEqual(result["alert"]["grade"], "CRITICAL")
                self.assertEqual(result["sources"]["bucket_ms"], 300_000)
                self.assertEqual(
                    result["sources"]["events_checksum_sha256"],
                    hashlib.sha256(EVENTS.read_bytes()).hexdigest(),
                )
                self.assertTrue(
                    all(
                        item["attribution"] == "exact_match"
                        for item in result["consistency"]["cross_source"]
                    )
                )


if __name__ == "__main__":
    unittest.main()
