from __future__ import annotations

import threading
import unittest
from pathlib import Path

from mvcc_engine.client import ApiError, MVCCClient
from mvcc_engine.engine import MVCCEngine
from mvcc_engine.http_api import build_server

from tests.tmp_helpers import make_settings, temporary_data_dir


class HttpApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = temporary_data_dir()
        self.addCleanup(self.temp.cleanup)
        self.engine = MVCCEngine(make_settings(Path(self.temp.name), edit_budget_ms=5000))
        self.server = build_server(self.engine, "127.0.0.1", 0, quiet=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.client = MVCCClient(f"http://{host}:{port}", timeout=5.0)

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3.0)
        self.engine.graceful_shutdown("http_test_teardown")

    def test_health_and_json_error_envelope(self) -> None:
        health = self.client.health()
        self.assertEqual("ready", health["status"])
        self.assertEqual("startup_recovery", health["recovery"]["report_kind"])
        with self.assertRaises(ApiError) as caught:
            self.client.begin("SERIALIZABLE", "bad-isolation")
        self.assertEqual("E_INVALID_ISOLATION", caught.exception.code)
        self.assertEqual(400, caught.exception.status)
        self.assertIn("allowed", caught.exception.context)
        with self.assertRaises(ApiError) as missing:
            self.client.request("GET", "/does-not-exist")
        self.assertEqual("E_ROUTE_NOT_FOUND", missing.exception.code)

    def test_edit_save_commit_over_http(self) -> None:
        txn = self.client.begin("REPEATABLE_READ", "http-txn")["txn_id"]
        opened = self.client.open_edit(txn, "accounts", "A-1001")
        self.assertFalse(opened["lock"]["locked"])
        saved = self.client.save(
            txn,
            opened["session_id"],
            opened["row_version"],
            {"name": "HTTP edit", "balance": 101},
        )
        self.assertEqual(2, saved["new_row_version"])
        read = self.client.read(txn, "accounts", "A-1001")
        self.assertEqual("HTTP edit", read["row"]["data"]["name"])
        get_read = self.client.request("GET", f"/txns/{txn}/rows/accounts/A-1001")
        self.assertEqual("HTTP edit", get_read["row"]["data"]["name"])
        session_read = self.client.request(
            "GET", f"/txns/{txn}/edit-sessions/{opened['session_id']}"
        )
        self.assertEqual("open", session_read["session"]["status"])
        first_commit = self.client.commit(txn, {"request_id": "http-commit"})
        replay_commit = self.client.commit(txn, {"request_id": "http-commit"})
        self.assertTrue(replay_commit["replayed"])
        self.assertEqual(first_commit["commit_seq"], replay_commit["commit_seq"])
        with self.assertRaises(ApiError) as conflicting:
            self.client.commit(txn, {"request_id": "different"})
        self.assertEqual("E_CONFLICTING_TXN", conflicting.exception.code)

    def test_stale_http_error_has_latest_context(self) -> None:
        txn_a = self.client.begin("REPEATABLE_READ", "http-a")["txn_id"]
        txn_b = self.client.begin("REPEATABLE_READ", "http-b")["txn_id"]
        edit_a = self.client.open_edit(txn_a, "accounts", "A-1002")
        edit_b = self.client.open_edit(txn_b, "accounts", "A-1002")
        self.client.save(txn_a, edit_a["session_id"], edit_a["row_version"], {"name": "A"})
        self.client.commit(txn_a)
        with self.assertRaises(ApiError) as stale:
            self.client.save(txn_b, edit_b["session_id"], edit_b["row_version"], {"name": "B"})
        self.assertEqual("E_EDIT_STALE", stale.exception.code)
        self.assertEqual(2, stale.exception.context["latest"]["row_version"])
        self.client.rollback(txn_b)


if __name__ == "__main__":
    unittest.main(verbosity=2)

