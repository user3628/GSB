import json
import os
import tempfile
import unittest

from exp_tracker import jsonl_fmt, negotiation
from exp_tracker.errors import TrackerError
from exp_tracker.samples import build_sample_runs, generate_sample_files


class JsonlNegotiationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.paths = generate_sample_files(self.tmp)
        self.runs = build_sample_runs()

    def test_jsonl_roundtrip_preserves_fields(self):
        decoded = [run for _, run in jsonl_fmt.decode_file(self.paths["jsonl_v2"])]
        for original, loaded in zip(self.runs, decoded):
            self.assertEqual(original.params, loaded.params)
            self.assertEqual(original.metric_signature(), loaded.metric_signature())
            self.assertEqual(original.toolchain_version, loaded.toolchain_version)
            self.assertEqual(original.expires_at_us, loaded.expires_at_us)

    def test_precheck_is_state_free(self):
        result = negotiation.precheck(self.paths["binary_v2"])
        self.assertEqual(result.verdict, negotiation.ACCEPT)
        result = negotiation.precheck(self.paths["jsonl_v1_omit_params"])
        self.assertEqual(result.verdict, negotiation.DOWNGRADE_ACCEPT)
        self.assertFalse(result.params_present)

    def test_downgrade_backfills_default_params(self):
        # Build a v1 shard with one explicit-params run and one omitted run.
        mixed = os.path.join(self.tmp, "mixed_v1.jsonl")
        with_params = dict(self.runs[0].to_dict())
        no_params = dict(self.runs[1].to_dict())
        no_params.pop("params", None)
        for body in (with_params, no_params):
            body["format_version"] = jsonl_fmt.JSON_V1
        with open(mixed, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(with_params, sort_keys=True) + "\n")
            handle.write(json.dumps(no_params, sort_keys=True) + "\n")
        runs, neg = negotiation.decode_with_negotiation(mixed)
        self.assertEqual(neg.verdict, negotiation.DOWNGRADE_ACCEPT)
        target = next(run for run in runs if run.run_id == self.runs[1].run_id)
        self.assertEqual(target.params, negotiation.DEFAULT_PARAMS)
        self.assertTrue(target.downgrades)
        # The run that already carried params keeps them untouched.
        other = next(run for run in runs if run.run_id == self.runs[0].run_id)
        self.assertEqual(other.params, self.runs[0].params)
        self.assertNotEqual(other.params, negotiation.DEFAULT_PARAMS)

    def test_binary_v1_downgrade_marks_ms_precision(self):
        runs, neg = negotiation.decode_with_negotiation(self.paths["binary_v1"])
        self.assertEqual(neg.precision, "ms")
        self.assertEqual(neg.verdict, negotiation.DOWNGRADE_ACCEPT)
        self.assertTrue(all(run.downgrades for run in runs))

    def test_retired_version_rejected_without_import(self):
        retired = os.path.join(self.tmp, "retired.jsonl")
        with open(retired, "w", encoding="utf-8") as handle:
            handle.write('{"format_version":0,"run_id":"x","experiment":"e",'
                         '"metrics":[],"toolchain_version":"1.0.0","last_write_us":1}\n')
        with self.assertRaises(TrackerError) as ctx:
            negotiation.precheck(retired)
        self.assertEqual(ctx.exception.code, "E_VERSION_RETIRED")

    def test_unknown_future_version_unsupported(self):
        future = os.path.join(self.tmp, "future.jsonl")
        with open(future, "w", encoding="utf-8") as handle:
            handle.write('{"format_version":99,"run_id":"x","experiment":"e",'
                         '"metrics":[],"toolchain_version":"1.0.0","last_write_us":1}\n')
        with self.assertRaises(TrackerError) as ctx:
            negotiation.precheck(future)
        self.assertEqual(ctx.exception.code, "E_FORMAT_UNSUPPORTED")

    def test_corrupt_json_line_reports_record_index(self):
        bad = os.path.join(self.tmp, "bad.jsonl")
        good_line = json.dumps({
            "format_version": 2, "run_id": "ok", "experiment": "e",
            "params": {}, "metrics": [], "toolchain_version": "1.0.0",
            "last_write_us": 1,
        })
        with open(bad, "w", encoding="utf-8") as handle:
            handle.write(good_line + "\n")
            handle.write('{not json\n')
        iterator = jsonl_fmt.decode_file(bad)
        next(iterator)
        with self.assertRaises(TrackerError) as ctx:
            next(iterator)
        self.assertEqual(ctx.exception.code, "E_RECORD_CORRUPT")
        self.assertEqual(ctx.exception.context["record_index"], 1)


if __name__ == "__main__":
    unittest.main()
