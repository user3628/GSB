from __future__ import annotations

import dataclasses
import shutil
import tempfile
import unittest
from pathlib import Path

from slo_budget import codec
from slo_budget.binary_format import encode_binary_bytes, write_binary_snapshot
from slo_budget.errors import SloError
from slo_budget.policy import BoundaryPolicy

from tests.support import (
    CURRENT_BIN,
    CURRENT_JSONL,
    FUTURE_BIN,
    FUTURE_JSONL,
    LEGACY_BIN,
    LEGACY_JSONL,
    SAMPLES,
)


class CodecTests(unittest.TestCase):
    def test_current_binary_and_jsonl_are_interchangeable(self) -> None:
        binary = codec.decode(CURRENT_BIN)
        jsonl = codec.decode(CURRENT_JSONL)
        self.assertEqual(len(binary.records), 48)
        self.assertEqual(
            [record.internal_dict() for record in binary.records],
            [record.internal_dict() for record in jsonl.records],
        )
        self.assertEqual(binary.negotiation.verdict, "ACCEPT")
        self.assertEqual(jsonl.negotiation.verdict, "ACCEPT")
        self.assertTrue(all(item.rounding_bit == 0 for item in binary.rounding))
        self.assertGreater(sum(item.rounding_bit for item in jsonl.rounding), 0)
        self.assertLess(jsonl.cumulative_rounding_error_us, 0)
        self.assertEqual(
            sum(item.rounding_error_us for item in jsonl.rounding),
            jsonl.cumulative_rounding_error_us,
        )

    def test_legacy_degrades_missing_late_count_and_relative_time(self) -> None:
        current = codec.decode(CURRENT_BIN)
        for path in (LEGACY_BIN, LEGACY_JSONL):
            negotiation = codec.preflight(path)
            self.assertEqual(negotiation.verdict, "DEGRADE")
            self.assertTrue(negotiation.degradations)
            legacy = codec.decode(path)
            self.assertEqual(len(legacy.degradations), 48)
            self.assertTrue(all(record.late_count == 0 for record in legacy.records))
            self.assertEqual(
                [record.window_start_ms for record in legacy.records],
                [record.window_start_ms for record in current.records],
            )
            actions = {
                action["action"]
                for degradation in legacy.degradations
                for action in degradation["actions"]
            }
            self.assertIn("relative_time_converted", actions)
            self.assertIn("late_count_defaulted", actions)
        self.assertLess(codec.decode(LEGACY_JSONL).cumulative_rounding_error_us, 0)

    def test_future_required_extensions_are_rejected_before_decode(self) -> None:
        for path in (FUTURE_BIN, FUTURE_JSONL):
            negotiation = codec.preflight(path)
            self.assertEqual(negotiation.verdict, "REJECT")
            self.assertEqual(negotiation.code, "E_FORMAT_UNSUPPORTED")
            with self.assertRaises(SloError) as caught:
                codec.decode(path)
            self.assertEqual(caught.exception.code, "E_FORMAT_UNSUPPORTED")
            self.assertIsNotNone(caught.exception.offset)

    def test_truncated_and_bad_crc_report_offsets(self) -> None:
        paths = [
            SAMPLES / "corrupt" / "current-v2-truncated.bin",
            SAMPLES / "corrupt" / "current-v2-bad-crc.bin",
            SAMPLES / "corrupt" / "current-v2-truncated.jsonl",
        ]
        for path in paths:
            with self.assertRaises(SloError) as caught:
                codec.decode(path)
            self.assertEqual(caught.exception.code, "E_RECORD_CORRUPT")
            self.assertIsInstance(caught.exception.offset, int)
            self.assertGreaterEqual(caught.exception.offset, 0)

    def test_record_length_prefix_is_bounds_checked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad-length.bin"
            data = bytearray(CURRENT_BIN.read_bytes())
            data[40:44] = (0).to_bytes(4, "little")
            path.write_bytes(bytes(data))
            with self.assertRaises(SloError) as caught:
                codec.decode(path)
            self.assertEqual(caught.exception.code, "E_RECORD_CORRUPT")
            self.assertEqual(caught.exception.offset, 40)

    def test_service_field_length_limit_is_enforced(self) -> None:
        with self.assertRaises(SloError) as caught:
            codec.decode(
                CURRENT_BIN,
                boundaries=BoundaryPolicy(max_service_name_bytes=4),
            )
        self.assertEqual(caught.exception.code, "E_RECORD_CORRUPT")
        self.assertIsNotNone(caught.exception.offset)

    def test_big_endian_roundtrip(self) -> None:
        parsed = codec.decode(CURRENT_BIN)
        manifest = dataclasses.replace(parsed.manifest, file_format="binary")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "big-endian.bin"
            payload = encode_binary_bytes(
                parsed.records,
                manifest,
                endian="big",
                version=2,
            )
            path.write_bytes(payload)
            self.assertEqual(payload[9], 0x42)
            decoded = codec.decode(path)
            self.assertEqual(
                [record.internal_dict() for record in decoded.records],
                [record.internal_dict() for record in parsed.records],
            )

    def test_binary_export_roundtrip_and_little_endian_marker(self) -> None:
        parsed = codec.decode(CURRENT_BIN)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "roundtrip.bin"
            write_binary_snapshot(
                path,
                parsed.records,
                dataclasses.replace(parsed.manifest, file_format="binary"),
                endian="little",
                version=2,
            )
            data = path.read_bytes()
            self.assertEqual(data[9], 0x4C)
            decoded = codec.decode(path)
            self.assertEqual(
                [record.persisted_dict() for record in decoded.records],
                [record.persisted_dict() for record in parsed.records],
            )

    def test_jsonl_truncation_rules_and_rounding_accounting(self) -> None:
        floor = codec.decode(CURRENT_JSONL, truncation_rule="floor")
        ceil = codec.decode(CURRENT_JSONL, truncation_rule="ceil")
        half_up = codec.decode(CURRENT_JSONL, truncation_rule="half_up")
        for floor_item, ceil_item, half_item in zip(
            floor.rounding, ceil.rounding, half_up.rounding, strict=True
        ):
            expected_floor = floor_item.source_value // 1000
            self.assertEqual(floor_item.normalized_ms, expected_floor)
            self.assertEqual(
                ceil_item.normalized_ms,
                expected_floor + (1 if floor_item.remainder_us else 0),
            )
            self.assertEqual(
                half_item.normalized_ms,
                expected_floor + (1 if floor_item.remainder_us >= 500 else 0),
            )
            self.assertIn(floor_item.rounding_bit, {0, 1})
            self.assertEqual(
                floor_item.rounding_error_us,
                floor_item.normalized_ms * 1000 - floor_item.source_value,
            )
        self.assertGreater(ceil.cumulative_rounding_error_us, 0)

    def test_declared_format_mismatch_is_rejected(self) -> None:
        with self.assertRaises(SloError) as caught:
            codec.preflight(CURRENT_BIN, expected_format="jsonl")
        self.assertEqual(caught.exception.code, "E_FORMAT_UNSUPPORTED")


if __name__ == "__main__":
    unittest.main()
