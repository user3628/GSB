import os
import tempfile
import unittest

from exp_tracker import binary, jsonl_fmt, negotiation
from exp_tracker.samples import build_sample_runs
from exp_tracker.store import ExperimentStore


class CrossFormatInteropTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.runs = build_sample_runs()

    def _export_both(self):
        bin_path = os.path.join(self.tmp, "a.etrk")
        json_path = os.path.join(self.tmp, "a.jsonl")
        binary.encode_file(self.runs, bin_path, version=binary.BIN_V2)
        jsonl_fmt.encode_file(self.runs, json_path, version=jsonl_fmt.JSON_V2)
        return bin_path, json_path

    def test_cross_format_field_fidelity(self):
        bin_path, json_path = self._export_both()
        from_bin = dict((r.run_id, r) for _, r in binary.decode_file(bin_path))
        from_json = dict((r.run_id, r) for _, r in jsonl_fmt.decode_file(json_path))
        self.assertEqual(set(from_bin), set(from_json))
        for run_id, original in ((r.run_id, r) for r in self.runs):
            self.assertEqual(from_bin[run_id].params, original.params)
            self.assertEqual(from_json[run_id].params, original.params)
            self.assertEqual(from_bin[run_id].metric_signature(),
                             original.metric_signature())
            self.assertEqual(from_json[run_id].metric_signature(),
                             original.metric_signature())
            self.assertEqual(from_bin[run_id].toolchain_version,
                             from_json[run_id].toolchain_version)
            self.assertEqual(from_bin[run_id].artifacts, from_json[run_id].artifacts)

    def test_import_json_then_reexport_binary_keeps_state(self):
        _, json_path = self._export_both()
        db = os.path.join(self.tmp, "store.db")
        store = ExperimentStore(db)
        runs, neg = negotiation.decode_with_negotiation(json_path)
        store.import_runs(runs, neg.to_dict(), "rep-interop")
        re_bin = os.path.join(self.tmp, "re.etrk")
        binary.encode_file(store.list_runs(), re_bin)
        re_decoded = dict((r.run_id, r) for _, r in binary.decode_file(re_bin))
        for run in self.runs:
            self.assertEqual(re_decoded[run.run_id].metric_signature(),
                             run.metric_signature())
            self.assertEqual(re_decoded[run.run_id].params, run.params)
        store.close()

    def test_mixed_precision_values_survive(self):
        bin_path, _ = self._export_both()
        decoded = dict((r.run_id, r) for _, r in binary.decode_file(bin_path))
        for run in self.runs:
            for original, loaded in zip(run.metrics, decoded[run.run_id].metrics):
                self.assertEqual(original.value, loaded.value)
                self.assertEqual(type(loaded.value), float)


if __name__ == "__main__":
    unittest.main()
