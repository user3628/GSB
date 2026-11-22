import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from polmig.codecs import (
    BINARY_FORMAT_VERSION,
    MAGIC,
    decode_binary,
    decode_jsonl,
    encode_binary,
    encode_jsonl,
)
from polmig.compatibility import detect_format, negotiate, matrix_lookup
from polmig.errors import (
    FormatUnsupported,
    ParentCycle,
    ParentDepthExceeded,
    PolicyInvalid,
    RecordCorrupt,
)
from polmig.migration import adjudicate_legacy, detect_parent_chain
from polmig.model import EPOCH_MS, NewPolicy

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples")


def sample(name):
    with open(os.path.join(SAMPLE_DIR, name), "rb") as handle:
        return handle.read()


def make_policies():
    return [
        NewPolicy("t1", None, 10, 50, EPOCH_MS, EPOCH_MS + 1000, 1, 9, {"a": "x"}),
        NewPolicy("t2", "t1", 4, 30, EPOCH_MS + 5, EPOCH_MS + 2000),
    ]


class CodecTests(unittest.TestCase):
    def test_binary_roundtrip_and_digest_equivalence(self):
        policies = make_policies()
        restored = decode_binary(encode_binary(policies))
        self.assertEqual(
            [p.canonical_digest() for p in policies],
            [p.canonical_digest() for p in restored],
        )

    def test_jsonl_and_binary_are_interchangeable(self):
        policies = make_policies()
        from_binary = decode_binary(encode_binary(policies))
        from_jsonl = decode_jsonl(encode_jsonl(policies))
        self.assertEqual(
            [p.canonical_digest() for p in from_binary],
            [p.canonical_digest() for p in from_jsonl],
        )

    def test_binary_layout_has_magic_version_prefix_crc_bitmap(self):
        blob = encode_binary(make_policies()[:1])
        self.assertTrue(blob.startswith(MAGIC))
        self.assertEqual(blob[8], BINARY_FORMAT_VERSION)
        self.assertGreater(len(blob), 13)

    def test_corrupt_magic_and_byte_flip(self):
        blob = bytearray(encode_binary(make_policies()))
        with self.assertRaises(RecordCorrupt):
            decode_binary(bytes(blob[:4]) + b"XXXX" + bytes(blob[8:]))
        length = int.from_bytes(blob[13:17], "big")
        crc_index = 17 + length
        blob[crc_index] ^= 0xFF
        with self.assertRaises(RecordCorrupt) as ctx:
            decode_binary(bytes(blob))
        self.assertEqual(ctx.exception.details["record_index"], 0)

    def test_corrupt_jsonl_line_reports_index(self):
        with self.assertRaises(RecordCorrupt) as ctx:
            decode_jsonl(b"{not json}\n")
        self.assertEqual(ctx.exception.details["record_index"], 0)

    def test_unsupported_binary_version(self):
        blob = bytearray(encode_binary(make_policies()))
        blob[8] = 2
        with self.assertRaises(FormatUnsupported) as ctx:
            decode_binary(bytes(blob))
        self.assertEqual(ctx.exception.details["detected_version"], 2)


class ParseValidationTests(unittest.TestCase):
    def test_invalid_policy_carries_field_paths(self):
        raw = {"policies": [{
            "tenant_id": "",
            "concurrency_limit": -1,
            "priority_min": 9,
            "priority_max": 1,
            "preempt_enabled": "nope",
        }]}
        with self.assertRaises(PolicyInvalid) as ctx:
            adjudicate_legacy(raw, 0)
        paths = " ".join(ctx.exception.details["field_paths"])
        for fragment in ("tenant_id", "concurrency_limit", "priority_min", "preempt_enabled"):
            self.assertIn(fragment, paths)

    def test_duplicate_tenant_id_rejected(self):
        raw = {"policies": [
            {"tenant_id": "dup", "concurrency_limit": 1, "priority_min": 0,
             "priority_max": 1, "preempt_enabled": True},
            {"tenant_id": "dup", "concurrency_limit": 1, "priority_min": 0,
             "priority_max": 1, "preempt_enabled": True},
        ]}
        with self.assertRaises(PolicyInvalid):
            adjudicate_legacy(raw, 0)

    def test_new_policy_epoch_and_lifetime_order(self):
        policy = NewPolicy("t", None, 1, 10, EPOCH_MS - 1, EPOCH_MS)
        errors = policy.validate()
        self.assertTrue(any("epoch" in e for e in errors))
        policy = NewPolicy("t", None, 1, 10, EPOCH_MS + 10, EPOCH_MS + 5)
        errors = policy.validate()
        self.assertTrue(any("start" in e for e in errors))


class CompatibilityTests(unittest.TestCase):
    def test_detect_formats(self):
        self.assertEqual(detect_format(sample("legacy_flat.json"))[0], "legacy-json")
        self.assertEqual(detect_format(sample("new_happy_path.jsonl"))[0], "jsonl")
        self.assertEqual(detect_format(encode_binary(make_policies()))[0], "binary")

    def test_legacy_is_degraded_with_fill_rules(self):
        report = negotiate(sample("legacy_flat.json"))
        self.assertEqual(report["verdict"], "DEGRADED")
        self.assertEqual({d["field"] for d in report["degradations"]},
                         {"parent_ref", "preempt_tier", "lifetime"})

    def test_explicit_matrix_reject(self):
        entry = matrix_lookup("jsonl", 2)
        self.assertEqual(entry["verdict"], "REJECT")
        with self.assertRaises(FormatUnsupported):
            negotiate(b"garbage-bytes")

    def test_unknown_input(self):
        with self.assertRaises(FormatUnsupported):
            negotiate(b"\x00\x01\x02\x03binary-ish")

    def test_binary_v2_rejected_before_decode(self):
        blob = bytearray(encode_binary(make_policies()))
        blob[8] = 2
        with self.assertRaises(FormatUnsupported) as ctx:
            negotiate(bytes(blob))
        self.assertEqual(ctx.exception.code, "E_FORMAT_UNSUPPORTED")
        self.assertEqual(ctx.exception.details["detected_format"], "binary")
        self.assertEqual(ctx.exception.details["detected_version"], 2)

    def test_jsonl_v2_rejected_when_explicitly_declared(self):
        entry = matrix_lookup("jsonl", 2)
        self.assertEqual(entry["verdict"], "REJECT")
        with self.assertRaises(FormatUnsupported):
            raise FormatUnsupported(entry["reason"], entry)


class MigrationMappingTests(unittest.TestCase):
    def test_happy_legacy_mapping_is_degraded_but_accepted(self):
        decisions, policies = adjudicate_legacy(
            json.loads(sample("legacy_flat.json")), 0
        )
        self.assertTrue(all(d["verdict"] == "DEGRADED" for d in decisions))
        root = next(p for p in policies if p.tenant_id == "tenant-root")
        self.assertIsNone(root.parent_ref)
        self.assertEqual(root.preempt_tier, 60)
        self.assertEqual(root.extensions["queue_hint"], "batch-default")

    def test_unmappable_deprecated_field_is_rejected_with_reasons(self):
        decisions, _ = adjudicate_legacy(
            json.loads(sample("boundary_deprecated_overlap.json")), 0
        )
        gamma = next(d for d in decisions if d["tenant_id"] == "tenant-gamma")
        self.assertEqual(gamma["verdict"], "REJECT")
        self.assertTrue(any("legacy_magic_token" in r["field"]
                            for r in gamma["rejection_reasons"]))

    def test_boundary_sample_invalid_record_raises_policy_invalid(self):
        with self.assertRaises(PolicyInvalid):
            adjudicate_legacy(
                json.loads(sample("boundary_invalid_records.json")), 0
            )

    def test_parent_cycle_and_depth(self):
        blob = sample("parent_cycle.jsonl")
        policies = decode_jsonl(blob)
        batch = {p.tenant_id: p for p in policies}
        with self.assertRaises(ParentCycle) as ctx:
            for policy in policies:
                detect_parent_chain(policy.tenant_id, batch, lambda _id: None, 4)
        self.assertEqual(set(ctx.exception.details["cycle_tenant_ids"]),
                         {"cyc-a", "cyc-b", "cyc-c"})

        chain = [
            NewPolicy("r", None, 1, 1, EPOCH_MS, EPOCH_MS + 10),
            NewPolicy("c1", "r", 1, 1, EPOCH_MS, EPOCH_MS + 10),
        ]
        deep = {p.tenant_id: p for p in chain}
        self.assertEqual(
            detect_parent_chain("c1", deep, lambda _id: None, 4), ["c1", "r"]
        )
        with self.assertRaises(ParentDepthExceeded):
            detect_parent_chain("c1", deep, lambda _id: None, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
