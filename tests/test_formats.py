from __future__ import annotations

import json
import unittest

from support import corrupt_first_record_length, sample_bytes

from joincost.errors import BusinessError, E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED
from joincost.formats import decode_snapshot, detect_format, encode_snapshot
from joincost.formats.binary import decode_binary
from joincost.formats.jsonl import decode_jsonl
from joincost.model import canonical_json


class FormatTests(unittest.TestCase):
    def test_v2_binary_and_jsonl_are_semantically_equal(self):
        binary = sample_bytes("stats_v2.bin")
        jsonl = sample_bytes("stats_v2.jsonl")
        self.assertEqual(detect_format(binary), "binary")
        self.assertEqual(detect_format(jsonl), "jsonl")
        decoded_binary = decode_binary(binary)
        decoded_jsonl = decode_jsonl(jsonl)
        self.assertEqual(decoded_binary.snapshot.digest, decoded_jsonl.snapshot.digest)
        self.assertEqual(decoded_binary.report["decision"], "ACCEPT")
        self.assertEqual(decoded_jsonl.report["decision"], "ACCEPT")
        self.assertEqual(len(decoded_binary.snapshot.relations), 6)

    def test_binary_both_endian_markers_roundtrip(self):
        decoded = decode_binary(sample_bytes("stats_v2.bin"))
        for endian in ("little", "big"):
            encoded = encode_snapshot(
                decoded.snapshot, "binary", endian=endian, format_version=2
            )
            self.assertEqual(encoded[5], 0x4C if endian == "little" else 0x42)
            again = decode_binary(encoded)
            self.assertEqual(again.snapshot.digest, decoded.snapshot.digest)

    def test_legacy_versions_degrade_with_details(self):
        binary = decode_binary(sample_bytes("stats_v1_legacy.bin"))
        jsonl = decode_jsonl(sample_bytes("stats_v1_legacy.jsonl"))
        self.assertEqual(binary.report["decision"], "DEGRADE_ACCEPT")
        self.assertEqual(jsonl.report["decision"], "DEGRADE_ACCEPT")
        self.assertTrue(binary.report["degradations"])
        self.assertTrue(jsonl.report["degradations"])
        self.assertTrue(
            all(item.correlation_fixed == 1_000_000_000 for item in binary.snapshot.relations)
        )
        self.assertTrue(
            all(item.correlation_fixed == 1_000_000_000 for item in jsonl.snapshot.relations)
        )
        self.assertEqual(binary.snapshot.relations[0].correlation_fixed, 1_000_000_000)
        self.assertEqual(jsonl.snapshot.relations[0].correlation_fixed, 1_000_000_000)
        self.assertTrue(
            any(item["field"] == "ndv_ratio" for item in binary.report["degradations"])
        )
        self.assertTrue(
            any(item["field"] == "ndv_ratio" for item in jsonl.report["degradations"])
        )

    def test_high_version_is_rejected_with_import_report(self):
        with self.assertRaises(BusinessError) as raised:
            decode_jsonl(sample_bytes("stats_v3_unsupported.jsonl"))
        self.assertEqual(raised.exception.code, E_FORMAT_UNSUPPORTED)
        report = raised.exception.details["import_report"]
        self.assertEqual(report["decision"], "REJECT")
        self.assertTrue(report["compatibility"]["reasons"])

    def test_bad_body_crc_is_corrupt(self):
        data = bytearray(sample_bytes("stats_v2.bin"))
        data[-1] ^= 0x01
        with self.assertRaises(BusinessError) as raised:
            decode_binary(bytes(data))
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_bad_header_crc_is_corrupt(self):
        data = bytearray(sample_bytes("stats_v2.bin"))
        data[10] ^= 0x01
        with self.assertRaises(BusinessError) as raised:
            decode_binary(bytes(data))
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_record_length_mismatch_is_corrupt(self):
        data = corrupt_first_record_length(sample_bytes("stats_v2.bin"))
        with self.assertRaises(BusinessError) as raised:
            decode_binary(data)
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)
        self.assertIn("record length", raised.exception.message)

    def test_truncated_binary_is_corrupt(self):
        data = sample_bytes("stats_v2.bin")
        with self.assertRaises(BusinessError) as raised:
            decode_binary(data[:-3])
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_jsonl_record_count_mismatch_is_corrupt(self):
        lines = sample_bytes("stats_v2.jsonl").decode("utf-8").splitlines()
        manifest = json.loads(lines[0])
        manifest["record_count"] += 1
        mutated = (canonical_json(manifest) + "\n" + "\n".join(lines[1:]) + "\n").encode()
        with self.assertRaises(BusinessError) as raised:
            decode_jsonl(mutated)
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_jsonl_record_crc_mismatch_is_corrupt(self):
        lines = sample_bytes("stats_v2.jsonl").decode("utf-8").splitlines()
        record = json.loads(lines[1])
        record["row_count"] += 1
        mutated = (lines[0] + "\n" + canonical_json(record) + "\n" + "\n".join(lines[2:]) + "\n").encode()
        with self.assertRaises(BusinessError) as raised:
            decode_jsonl(mutated)
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_unknown_required_feature_is_unsupported(self):
        lines = sample_bytes("stats_v2.jsonl").decode("utf-8").splitlines()
        manifest = json.loads(lines[0])
        manifest["required_features"].append("unknown_mandatory_extension")
        mutated = (canonical_json(manifest) + "\n" + "\n".join(lines[1:]) + "\n").encode()
        with self.assertRaises(BusinessError) as raised:
            decode_jsonl(mutated)
        self.assertEqual(raised.exception.code, E_FORMAT_UNSUPPORTED)

    def test_truncated_magic_is_corrupt(self):
        with self.assertRaises(BusinessError) as raised:
            decode_snapshot(b"JC")
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)

    def test_unknown_payload_is_unsupported(self):
        with self.assertRaises(BusinessError) as raised:
            decode_snapshot(b"not-a-snapshot")
        self.assertEqual(raised.exception.code, E_FORMAT_UNSUPPORTED)


if __name__ == "__main__":
    unittest.main()

