import os
import tempfile
import unittest

from exp_tracker import binary
from exp_tracker.errors import TrackerError
from exp_tracker.models import Artifact, MetricPoint, Run
from exp_tracker.samples import build_sample_runs, tamper_binary_crc


class BinaryCodecTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "runs.etrk")
        self.runs = build_sample_runs()

    def test_roundtrip_v2_little_endian_field_fidelity(self):
        binary.encode_file(self.runs, self.path, version=binary.BIN_V2, endian="little")
        decoded = [run for _, run in binary.decode_file(self.path)]
        self.assertEqual(len(decoded), len(self.runs))
        for original, loaded in zip(self.runs, decoded):
            self.assertEqual(original.run_id, loaded.run_id)
            self.assertEqual(original.experiment, loaded.experiment)
            self.assertEqual(original.params, loaded.params)
            self.assertEqual(original.toolchain_version, loaded.toolchain_version)
            self.assertEqual(original.metric_signature(), loaded.metric_signature())
            self.assertEqual(original.artifacts, loaded.artifacts)
            self.assertEqual(original.expires_at_us, loaded.expires_at_us)

    def test_big_endian_roundtrip(self):
        binary.encode_file(self.runs, self.path, endian="big")
        info = binary.inspect_file(self.path)
        self.assertEqual(info["endian"], "big")
        decoded = [run for _, run in binary.decode_file(self.path)]
        self.assertEqual(decoded[0].run_id, self.runs[0].run_id)

    def test_v1_ms_precision_is_coarser_but_on_ms_grid(self):
        run = self.runs[0]
        binary.encode_file([run], self.path, version=binary.BIN_V1)
        info = binary.inspect_file(self.path)
        self.assertEqual(info["format_version"], binary.BIN_V1)
        decoded = [r for _, r in binary.decode_file(self.path)][0]
        for point in decoded.metrics:
            self.assertEqual(point.time_us % 1000, 0)

    def test_crc_tamper_reports_record_index(self):
        binary.encode_file(self.runs, self.path)
        tamper_binary_crc(self.path, target_record=2)
        with self.assertRaises(TrackerError) as ctx:
            list(binary.decode_file(self.path))
        self.assertEqual(ctx.exception.code, "E_RECORD_CORRUPT")
        self.assertEqual(ctx.exception.context["record_index"], 2)

    def test_bad_magic_is_unsupported(self):
        with open(self.path, "wb") as handle:
            handle.write(b"NOPE" + bytes([2, 1]))
        with self.assertRaises(TrackerError) as ctx:
            binary.inspect_file(self.path)
        self.assertEqual(ctx.exception.code, "E_FORMAT_UNSUPPORTED")

    def test_truncated_length_prefix_is_corrupt(self):
        binary.encode_file(self.runs, self.path)
        with open(self.path, "rb") as handle:
            blob = bytearray(handle.read())
        blob = blob[:10]  # cut inside the first length prefix
        with open(self.path, "wb") as handle:
            handle.write(blob)
        with self.assertRaises(TrackerError) as ctx:
            list(binary.decode_file(self.path))
        self.assertEqual(ctx.exception.code, "E_RECORD_CORRUPT")

    def test_optional_fields_absent_when_empty(self):
        run = Run(run_id="r", experiment="e", params={}, metrics=[],
                  toolchain_version="1.0.0", last_write_us=1)
        binary.encode_file([run], self.path)
        decoded = [r for _, r in binary.decode_file(self.path)][0]
        self.assertEqual(decoded.params, {})
        self.assertEqual(decoded.artifacts, [])
        self.assertIsNone(decoded.base_time_us)

    def test_unknown_version_rejected(self):
        binary.encode_file(self.runs, self.path)
        with open(self.path, "rb") as handle:
            blob = bytearray(handle.read())
        blob[4] = 9
        with open(self.path, "wb") as handle:
            handle.write(blob)
        with self.assertRaises(TrackerError) as ctx:
            list(binary.decode_file(self.path))
        self.assertEqual(ctx.exception.code, "E_FORMAT_UNSUPPORTED")


if __name__ == "__main__":
    unittest.main()
