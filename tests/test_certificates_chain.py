import copy
import json
import os
import shutil
import tempfile
import unittest

from tls_workbench import chain as chain_mod
from tls_workbench import scoring, timeutil
from tls_workbench.certificates import load_chain
from tls_workbench.engine import SampleStore
from tls_workbench.errors import Codes, WorkbenchError
from tls_workbench.samplegen import VERIFY_WALL, generate_all


class GeneratedSampleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_dir = tempfile.mkdtemp(prefix="tls-wb-")
        generate_all(cls.base_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.base_dir, ignore_errors=True)

    def test_der_metadata_extraction(self):
        chain = load_chain(os.path.join(self.base_dir, "certs", "der", "rsa2048_chain"))
        leaf = chain["certificates"]["leaf"]
        self.assertEqual(leaf["public_key_algorithm"], "RSA")
        self.assertEqual(leaf["rsa_modulus_bits"], 2048)
        self.assertEqual(leaf["signature_algorithm"], "SHA256_WITH_RSA")
        self.assertTrue(leaf["not_after_ms"] % 1000 == 0)

    def test_ec_curve_metadata(self):
        chain = load_chain(os.path.join(self.base_dir, "certs", "der", "ec256_chain"))
        leaf = chain["certificates"]["leaf"]
        self.assertEqual(leaf["public_key_algorithm"], "EC")
        self.assertEqual(leaf["ec_curve"], "P-256")

    def test_path_order_and_signatures(self):
        store = SampleStore(self.base_dir)
        chain = store.load("rsa2048_chain")
        instant = timeutil.build_verification_instant(VERIFY_WALL, 0)
        path = chain_mod.validate_chain(chain, instant)
        self.assertEqual(path.ordered_roles, ("leaf", "intermediate", "root"))
        self.assertTrue(all(item["verified"] for item in path.signature_checks))

    def test_zero_margin_leaf_is_valid(self):
        store = SampleStore(self.base_dir)
        chain = store.load("rsa2048_chain")
        instant = timeutil.build_verification_instant(VERIFY_WALL, 0)
        path = chain_mod.validate_chain(chain, instant)
        leaf_margin = next(item for item in path.margins if item["role"] == "leaf")
        self.assertEqual(leaf_margin["margin_after_ms"], 0)
        self.assertTrue(leaf_margin["is_valid"])

    def test_expired_chain_fails_validity(self):
        # Fresh store so an earlier test's in-memory snapshot cannot leak in.
        store = SampleStore(self.base_dir)
        store._chains.clear()
        chain = store.load("expired_chain")
        instant = timeutil.build_verification_instant(VERIFY_WALL, 0)
        with self.assertRaises(WorkbenchError) as ctx:
            chain_mod.validate_chain(chain, instant)
        self.assertEqual(ctx.exception.code, Codes.CERT_EXPIRED)

    def test_corrupt_der_raises_chain_invalid_and_keeps_snapshot(self):
        store = SampleStore(self.base_dir)
        store.load("rsa2048_chain")
        leaf_path = os.path.join(
            self.base_dir, "certs", "der", "rsa2048_chain", "leaf.der"
        )
        with open(leaf_path, "rb") as handle:
            backup = handle.read()
        try:
            with open(leaf_path, "wb") as handle:
                handle.write(b"not-a-der-cert")
            fresh_store = SampleStore(self.base_dir)
            with self.assertRaises(WorkbenchError) as ctx:
                fresh_store.reload_preserving_snapshot("rsa2048_chain")
            self.assertEqual(ctx.exception.code, Codes.CHAIN_INVALID)
            # A previously populated store keeps its good snapshot.
            self.assertIn("leaf", store.load("rsa2048_chain")["certificates"])
        finally:
            with open(leaf_path, "wb") as handle:
                handle.write(backup)

    def test_missing_root_raises_chain_invalid(self):
        chain_dir = os.path.join(self.base_dir, "certs", "der", "rsa2048_chain")
        metadata_path = os.path.join(chain_dir, "chain_metadata.json")
        with open(metadata_path, encoding="utf-8") as handle:
            metadata = json.load(handle)
        metadata["certificates"] = [
            item for item in metadata["certificates"] if item["role"] != "root"
        ]
        broken_dir = tempfile.mkdtemp(dir=self.base_dir)
        for entry in metadata["certificates"]:
            source = os.path.join(chain_dir, entry["der_file"])
            with open(source, "rb") as source_handle, open(
                os.path.join(broken_dir, entry["der_file"]), "wb"
            ) as target_handle:
                target_handle.write(source_handle.read())
        with open(os.path.join(broken_dir, "chain_metadata.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(metadata, handle)
        with self.assertRaises(WorkbenchError) as ctx:
            load_chain(broken_dir)
        self.assertEqual(ctx.exception.code, Codes.CHAIN_INVALID)

    def test_path_loop_detection(self):
        store = SampleStore(self.base_dir)
        chain = copy.deepcopy(store.load("rsa2048_chain"))
        # Point the intermediate's issuer back at the leaf to create a
        # leaf <-> intermediate cycle that can never terminate at root.
        leaf_subject = chain["certificates"]["leaf"]["subject"]
        chain["certificates"]["intermediate"]["issuer"] = leaf_subject
        with self.assertRaises(WorkbenchError) as ctx:
            chain_mod.build_path(chain)
        self.assertEqual(ctx.exception.code, Codes.PATH_LOOP)

    def test_scoring_is_bit_for_bit_deterministic(self):
        store = SampleStore(self.base_dir)
        chain = store.load("rsa3072_chain")
        instant = timeutil.build_verification_instant(VERIFY_WALL, 0)
        path = chain_mod.validate_chain(chain, instant)
        first = scoring.score_chain(chain, path)
        second = scoring.score_chain(chain, path)
        self.assertEqual(
            first["total_score_fixed"], second["total_score_fixed"]
        )
        self.assertEqual(
            json.dumps(first, sort_keys=True, default=str),
            json.dumps(second, sort_keys=True, default=str),
        )
        self.assertIn("steps", first["certificate_scores"][0])

    def test_chain_comparison_tie_break_is_lexicographic(self):
        store = SampleStore(self.base_dir)
        instant = timeutil.build_verification_instant(VERIFY_WALL, 0)
        left = self._score(store, "ec256_chain", instant)
        right = dict(left)
        right["chain_id"] = "zzz-copy"
        result = scoring.compare_chains(left, right)
        self.assertEqual(result["relation"], "TOLERANCE_TIE")
        self.assertEqual(result["stronger_chain_id"], "ec256_chain")

    @staticmethod
    def _score(store, chain_id, instant):
        chain = store.load(chain_id)
        path = chain_mod.validate_chain(chain, instant)
        return scoring.score_chain(chain, path)


if __name__ == "__main__":
    unittest.main()
