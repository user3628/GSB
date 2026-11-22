import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from polmig.api import build_server
from polmig.codecs import encode_binary
from polmig.errors import (
    ConflictingBatch,
    LifetimeOverlap,
    ParentCycle,
    ParentDepthExceeded,
    StaleBase,
)
from polmig.model import EPOCH_MS, NewPolicy
from polmig.service import MigrationService

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples")


def sample(name):
    with open(os.path.join(SAMPLE_DIR, name), "rb") as handle:
        return handle.read()


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "migration.db")
        self.service = MigrationService(self.db_path, max_parent_depth=4)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, batch_id, payload, base_version=0):
        return self.service.submit_batch(
            {"batch_id": batch_id, "base_version": base_version}, payload
        )


class CommitTests(ServiceTestBase):
    def test_legacy_happy_path_commits_version_one(self):
        result = self.submit("b-legacy", sample("legacy_flat.json"))
        self.assertEqual(result["status"], "COMMITTED")
        self.assertEqual(result["committed_version"], 1)
        self.assertTrue(all(d["verdict"] == "DEGRADED" for d in result["decisions"]))
        self.assertEqual(self.service.store.current_version(), 1)
        self.assertEqual(len(self.service.store.active_new_policies()), 3)

    def test_binary_and_jsonl_commit_equivalent_digests(self):
        policies = [
            NewPolicy("bin-1", None, 7, 40, EPOCH_MS, EPOCH_MS + 1000, 0, 9, {"k": "v"})
        ]
        r1 = self.submit("b-bin", encode_binary(policies))
        blob_digest = r1["decisions"][0]["canonical_digest"]
        service2 = MigrationService(os.path.join(self.tmp.name, "second.db"))
        r2 = service2.submit_batch(
            {"batch_id": "b-json", "base_version": 0},
            json.dumps(policies[0].to_dict()).encode("utf-8"),
        )
        self.assertEqual(blob_digest, r2["decisions"][0]["canonical_digest"])

    def test_rejected_unmappable_batch_rolls_back(self):
        with self.assertRaises(Exception):
            self.submit("b-bad", sample("boundary_deprecated_overlap.json"))
        self.assertEqual(self.service.store.current_version(), 0)
        row = self.service.store.get_batch("b-bad")
        self.assertEqual(row["status"], "REJECTED")
        self.assertEqual(row["error_code"], "E_MIGRATION_REJECTED")
        self.assertEqual(len(self.service.store.active_new_policies()), 0)

    def test_lifetime_overlap_blocks_commit(self):
        self.submit("b-base", sample("new_happy_path.jsonl"))
        with self.assertRaises(LifetimeOverlap) as ctx:
            self.submit("b-overlap", sample("lifetime_overlap.jsonl"), base_version=1)
        details = ctx.exception.details
        self.assertEqual(details["tenant_id"], "org-child-a")
        self.assertIn("start_ms", details["incoming"])
        self.assertIn("start_ms", details["existing"])
        self.assertEqual(self.service.store.current_version(), 1)

    def test_parent_cycle_depth_and_missing_reference(self):
        with self.assertRaises(ParentCycle) as ctx:
            self.submit("b-cycle", sample("parent_cycle.jsonl"))
        self.assertEqual(len(ctx.exception.details["cycle_tenant_ids"]), 4)
        with self.assertRaises(Exception):
            self.submit("b-missing", sample("parent_missing.jsonl"))
        self.submit("b-base", sample("new_happy_path.jsonl"))
        with self.assertRaises(ParentDepthExceeded) as ctx:
            self.submit("b-depth", sample("parent_depth.jsonl"), base_version=1)
        self.assertEqual(ctx.exception.details["depth"], 5)

    def test_stale_base_and_monotonic_versions(self):
        self.submit("b-first", sample("new_happy_path.jsonl"))
        with self.assertRaises(StaleBase):
            self.submit("b-stale", sample("legacy_flat.json"), base_version=0)
        result = self.submit("b-second", sample("legacy_flat.json"), base_version=1)
        self.assertEqual(result["committed_version"], 2)

    def test_duplicate_batch_idempotent_and_conflict_detection(self):
        first = self.submit("b-dup", sample("legacy_flat.json"))
        second = self.submit("b-dup", sample("legacy_flat.json"))
        self.assertTrue(second.get("reused"))
        self.assertEqual(
            first["committed_version"], second["committed_version"]
        )
        tampered = sample("legacy_flat.json").replace(b"tenant-alpha", b"tenant-alpha-x")
        with self.assertRaises(ConflictingBatch):
            self.submit("b-dup", tampered)

    def test_concurrent_same_base_only_one_wins(self):
        barrier = threading.Barrier(2)
        outcomes = []

        def worker(payload_suffix):
            barrier.wait()
            payload = sample("new_happy_path.jsonl").replace(
                b"org-root", ("org-root" + payload_suffix).encode()
            )
            try:
                outcomes.append(self.service.submit_batch(
                    {"batch_id": "b-conc-%s" % payload_suffix, "base_version": 0},
                    payload,
                )["status"])
            except StaleBase as exc:
                outcomes.append(exc.code)

        threads = [threading.Thread(target=worker, args=(suffix,))
                   for suffix in ("-x", "-y")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(outcomes), ["COMMITTED", "E_STALE_BASE"])
        self.assertEqual(self.service.store.current_version(), 1)

    def test_corrupt_payload_marks_batch_rejected_and_rolls_back(self):
        from polmig.errors import RecordCorrupt, PolicyInvalid
        blob = bytearray(encode_binary([
            NewPolicy("c1", None, 3, 20, EPOCH_MS, EPOCH_MS + 100)
        ]))
        length = int.from_bytes(blob[13:17], "big")
        blob[17 + length] ^= 0xFF
        with self.assertRaises(RecordCorrupt):
            self.submit("b-corrupt", bytes(blob))
        self.assertEqual(self.service.store.current_version(), 0)
        self.assertEqual(self.service.store.get_batch("b-corrupt")["status"], "REJECTED")
        self.assertEqual(self.service.store.get_batch("b-corrupt")["error_code"],
                         "E_RECORD_CORRUPT")
        with self.assertRaises(PolicyInvalid):
            self.submit("b-invalid", sample("boundary_invalid_records.json"))
        self.assertEqual(self.service.store.get_batch("b-invalid")["error_code"],
                         "E_POLICY_INVALID")
        self.assertEqual(self.service.store.current_version(), 0)

    def test_parent_chain_view_reads_committed_version(self):
        self.submit("b-base", sample("new_happy_path.jsonl"))
        view = self.service.parent_chain_view("org-child-b")
        self.assertEqual([n["tenant_id"] for n in view["chain"]],
                         ["org-child-b", "org-child-a", "org-root"])
        self.assertEqual(view["depth"], 2)

    def test_plan_diff_reports_adds(self):
        result = self.submit("b-base", sample("new_happy_path.jsonl"))
        self.assertTrue(all(entry["action"] == "ADD" for entry in result["plan_diff"]))
        stored = self.service.plan_diff("b-base")
        self.assertEqual({e["tenant_id"] for e in stored},
                         {"org-root", "org-child-a", "org-child-b"})


class RecoveryTests(ServiceTestBase):
    def _run_crashed_submit(self, hook, batch_id):
        script = (
            "import sys; sys.path.insert(0, %r);"
            "from polmig.service import MigrationService;"
            "svc = MigrationService(%r, crash_hook=%r);"
            "svc.submit_batch({'batch_id': %r, 'base_version': 0},"
            " open(%r, 'rb').read())"
        ) % (
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            self.db_path, hook, batch_id,
            os.path.join(SAMPLE_DIR, "legacy_flat.json"),
        )
        import subprocess
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True
        )
        self.assertEqual(proc.returncode, 137, proc.stderr)

    def test_hard_kill_after_intent_is_recovered(self):
        self._run_crashed_submit("after_intent", "b-crash")
        revived = MigrationService(self.db_path)
        self.assertEqual(revived.store.current_version(), 0)
        report = revived.recovery_report
        self.assertEqual(report["pending_intents_found"], 1)
        self.assertEqual(
            report["events"][0]["action"],
            "ROLLED_BACK_TO_LAST_COMPLETE_VERSION",
        )
        self.assertEqual(revived.store.get_batch("b-crash")["status"], "ABORTED")
        self.assertEqual(len(revived.store.active_new_policies()), 0)

    def test_hard_kill_during_commit_rolls_back_and_marks_aborted(self):
        self._run_crashed_submit("after_commit_inside", "b-crash-mid")
        revived = MigrationService(self.db_path)
        self.assertEqual(revived.store.current_version(), 0)
        self.assertEqual(revived.store.active_new_policies(), [])
        report = revived.recovery_report
        self.assertEqual(report["pending_intents_found"], 1)
        self.assertEqual(
            report["events"][0]["action"],
            "ROLLED_BACK_TO_LAST_COMPLETE_VERSION",
        )
        self.assertEqual(revived.store.get_batch("b-crash-mid")["status"], "ABORTED")

    def test_hard_kill_after_durable_commit_leaves_consistent_state(self):
        self._run_crashed_submit("after_commit_outside", "b-crash-commit")
        revived = MigrationService(self.db_path)
        self.assertEqual(revived.store.current_version(), 1)
        self.assertEqual(len(revived.store.active_new_policies()), 3)
        self.assertEqual(
            revived.store.get_batch("b-crash-commit")["status"], "COMMITTED"
        )
        self.assertEqual(revived.recovery_report["pending_intents_found"], 0)


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "migration.db")
        self.server = build_server(self.db_path, "127.0.0.1", 0)
        self.host, self.port = self.server.server_address
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def _url(self, path):
        return "http://%s:%d%s" % (self.host, self.port, path)

    def _post_json(self, path, envelope):
        req = urllib.request.Request(
            self._url(path),
            data=json.dumps(envelope).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read())

    def _get(self, path):
        with urllib.request.urlopen(self._url(path), timeout=10) as resp:
            return resp.status, json.loads(resp.read())

    def test_full_http_workflow_and_error_envelope(self):
        status, body = self._post_json("/api/migration/batches", {
            "batch_id": "http-1",
            "base_version": 0,
            "payload": sample("legacy_flat.json").decode("utf-8"),
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["committed_version"], 1)

        status, body = self._get("/api/migration/batches/http-1?detail=decisions")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["decisions"]), 3)

        status, body = self._get("/api/migration/batches/http-1/diff")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["plan_diff"]), 3)

        status, body = self._post_json("/api/migration/batches", {
            "batch_id": "http-stale", "base_version": 0,
            "payload": sample("new_happy_path.jsonl").decode("utf-8"),
        })
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "E_STALE_BASE")

        status, body = self._get("/api/parent-chain/tenant-alpha")
        self.assertEqual(status, 200)
        self.assertEqual(body["chain"][0]["tenant_id"], "tenant-alpha")

        status, body = self._get("/api/recovery")
        self.assertEqual(status, 200)
        self.assertIn("events", body)

    def test_binary_octet_post(self):
        policies = [NewPolicy("http-bin", None, 3, 20, EPOCH_MS, EPOCH_MS + 999)]
        req = urllib.request.Request(
            self._url("/api/migration/batches"),
            data=encode_binary(policies),
            headers={
                "Content-Type": "application/octet-stream",
                "X-Batch-Id": "http-bin",
                "X-Base-Version": "0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
        self.assertEqual(body["committed_version"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
