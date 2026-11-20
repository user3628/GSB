from __future__ import annotations

import json
import threading
import time
import unittest
from pathlib import Path

from mvcc_engine.engine import MVCCEngine
from mvcc_engine.errors import AppError

from tests.tmp_helpers import ROOT, make_settings, temporary_data_dir


class EngineTestBase(unittest.TestCase):
    engine_kwargs: dict[str, object] = {}

    def setUp(self) -> None:
        self.temp = temporary_data_dir()
        self.addCleanup(self.temp.cleanup)
        self.settings = make_settings(Path(self.temp.name), **self.engine_kwargs)
        self.engine = MVCCEngine(self.settings)
        self.addCleanup(self._cleanup_engine)

    def _cleanup_engine(self) -> None:
        # Do not abort active transactions during teardown of crash tests; those
        # tests intentionally create a second engine before the first is closed.
        if getattr(self, "skip_engine_cleanup", False):
            return
        try:
            self.engine.graceful_shutdown("unit_test_teardown")
        except Exception:
            pass

    def assert_error(self, code: str, fn, *args, **kwargs) -> AppError:
        with self.assertRaises(AppError) as caught:
            fn(*args, **kwargs)
        self.assertEqual(code, caught.exception.code, caught.exception)
        return caught.exception


class IsolationAndVisibilityTests(EngineTestBase):
    def test_repeatable_read_and_snapshot_use_first_read_commit_seq(self) -> None:
        rr = self.engine.open_transaction("REPEATABLE_READ", "rr-1")["txn_id"]
        first = self.engine.read_row(rr, "accounts", "A-1001")
        self.assertEqual(1, first["row"]["row_version"])
        snapshot_id = first["snapshot_id"]
        self.assertTrue(snapshot_id.startswith("snapshot:"))

        writer = self.engine.open_transaction("READ_COMMITTED", "writer-1")["txn_id"]
        edit = self.engine.open_edit_session(writer, "accounts", "A-1001")
        saved = self.engine.save_edit_session(
            writer,
            edit["session_id"],
            edit["row_version"],
            {"name": "Committed newer value", "balance": 1300},
        )
        committed = self.engine.commit_transaction(writer)
        self.assertEqual(2, committed["commit_seq"])
        self.assertEqual(2, saved["new_row_version"])

        second = self.engine.read_row(rr, "accounts", "A-1001")
        self.assertEqual(snapshot_id, second["snapshot_id"])
        self.assertEqual(1, second["row"]["row_version"])
        self.assertEqual("Northwind Studio", second["row"]["data"]["name"])

        rc = self.engine.open_transaction("READ_COMMITTED", "rc-1")["txn_id"]
        rc_read = self.engine.read_row(rc, "accounts", "A-1001")
        self.assertEqual(2, rc_read["row"]["row_version"])
        self.assertEqual("Committed newer value", rc_read["row"]["data"]["name"])

        snap = self.engine.open_transaction("SNAPSHOT", "snap-1")["txn_id"]
        snap_read = self.engine.read_row(snap, "accounts", "A-1001")
        self.assertEqual(2, snap_read["row"]["row_version"])

        self.engine.rollback_transaction(rr)
        self.engine.commit_transaction(rc)
        self.engine.commit_transaction(snap)

    def test_row_query_uses_one_statement_snapshot(self) -> None:
        txn = self.engine.open_transaction("READ_COMMITTED", "query-1")["txn_id"]
        result = self.engine.query_rows(txn, "accounts")
        self.assertEqual(4, result["count"])
        self.assertTrue(result["snapshot_id"].startswith("read_committed:"))
        for row in result["rows"]:
            self.assertEqual(1, row["row_version"])
            self.assertIsNotNone(row["created_commit_seq"])
        self.engine.commit_transaction(txn)

    def test_own_uncommitted_write_is_visible_only_to_owner(self) -> None:
        owner = self.engine.open_transaction("REPEATABLE_READ", "owner-1")["txn_id"]
        other = self.engine.open_transaction("REPEATABLE_READ", "other-1")["txn_id"]
        owner_edit = self.engine.open_edit_session(owner, "accounts", "A-1003")
        other_edit = self.engine.open_edit_session(other, "accounts", "A-1003")
        self.engine.save_edit_session(
            owner,
            owner_edit["session_id"],
            owner_edit["row_version"],
            {"name": "Owner pending", "balance": 1},
        )
        own = self.engine.read_row(owner, "accounts", "A-1003")
        outside = self.engine.read_row(other, "accounts", "A-1003")
        self.assertEqual("Owner pending", own["row"]["data"]["name"])
        self.assertEqual("Fabrikam Labs", outside["row"]["data"]["name"])
        self.assertEqual(1, outside["row"]["row_version"])
        self.engine.rollback_transaction(owner)
        self.engine.rollback_transaction(other)

    def test_delete_visibility_and_stale_editor_after_commit(self) -> None:
        editor = self.engine.open_transaction("REPEATABLE_READ", "delete-editor")["txn_id"]
        edit = self.engine.open_edit_session(editor, "documents", "DOC-003")
        observer = self.engine.open_transaction("REPEATABLE_READ", "delete-observer")["txn_id"]
        before_delete = self.engine.read_row(observer, "documents", "DOC-003")
        self.assertIsNotNone(before_delete["row"])

        deleter = self.engine.open_transaction("READ_COMMITTED", "deleter")["txn_id"]
        deleted = self.engine.delete_row(deleter, "documents", "DOC-003", 1)
        self.assertTrue(deleted["deleted"])
        self.assertIsNone(self.engine.read_row(deleter, "documents", "DOC-003")["row"])
        still_visible = self.engine.read_row(observer, "documents", "DOC-003")
        self.assertIsNotNone(still_visible["row"])
        self.engine.commit_transaction(deleter)

        current = self.engine.read_row(observer, "documents", "DOC-003")
        self.assertIsNotNone(current["row"])
        fresh = self.engine.open_transaction("READ_COMMITTED", "delete-fresh")["txn_id"]
        self.assertIsNone(self.engine.read_row(fresh, "documents", "DOC-003")["row"])
        stale = self.assert_error(
            "E_EDIT_STALE",
            self.engine.save_edit_session,
            editor,
            edit["session_id"],
            edit["row_version"],
            {"title": "cannot revive stale deleted row"},
        )
        self.assertIsNone(stale.context["latest"])
        self.assert_error(
            "E_ROW_NOT_FOUND",
            self.engine.open_edit_session,
            fresh,
            "documents",
            "DOC-003",
        )
        self.engine.rollback_transaction(editor)
        self.engine.rollback_transaction(observer)
        self.engine.commit_transaction(fresh)


class EditSessionTests(EngineTestBase):
    def test_stale_save_returns_latest_editable_version(self) -> None:
        first = self.engine.open_transaction("REPEATABLE_READ", "editor-a")["txn_id"]
        second = self.engine.open_transaction("REPEATABLE_READ", "editor-b")["txn_id"]
        edit_a = self.engine.open_edit_session(first, "accounts", "A-1001")
        edit_b = self.engine.open_edit_session(second, "accounts", "A-1001")

        self.engine.save_edit_session(
            first,
            edit_a["session_id"],
            edit_a["row_version"],
            {"name": "First committed edit", "balance": 2000},
        )
        self.engine.commit_transaction(first)

        error = self.assert_error(
            "E_EDIT_STALE",
            self.engine.save_edit_session,
            second,
            edit_b["session_id"],
            edit_b["row_version"],
            {"name": "Stale overwrite", "balance": 5},
        )
        self.assertEqual(2, error.context["latest"]["row_version"])
        self.assertEqual("First committed edit", error.context["latest"]["data"]["name"])
        self.assertEqual("superseded_or_deleted_after_editor_open", error.context["stale_reason"])
        self.engine.rollback_transaction(second)

    def test_expiry_is_checked_even_when_version_is_unchanged(self) -> None:
        short_temp = temporary_data_dir()
        self.addCleanup(short_temp.cleanup)
        short_engine = MVCCEngine(
            make_settings(Path(short_temp.name), edit_budget_ms=60)
        )
        self.addCleanup(short_engine.graceful_shutdown, "expiry_teardown")
        txn = short_engine.open_transaction("REPEATABLE_READ", "expiry-1")["txn_id"]
        edit = short_engine.open_edit_session(txn, "accounts", "A-1002")
        time.sleep(0.10)
        error = self.assert_error(
            "E_EDIT_EXPIRED",
            short_engine.save_edit_session,
            txn,
            edit["session_id"],
            edit["row_version"],
            {"name": "Too late", "balance": 1},
        )
        self.assertEqual(60, error.context["budget_ms"])
        self.assertGreaterEqual(error.context["elapsed_ms"], 60)
        self.assertEqual(
            "server_arrival_time_minus_persisted_opened_at",
            error.context["decision_basis"],
        )
        short_engine.rollback_transaction(txn)

    def test_wrong_version_for_open_session_is_stale(self) -> None:
        txn = self.engine.open_transaction("SNAPSHOT", "wrong-version")["txn_id"]
        edit = self.engine.open_edit_session(txn, "documents", "DOC-001")
        error = self.assert_error(
            "E_EDIT_STALE",
            self.engine.save_edit_session,
            txn,
            edit["session_id"],
            edit["row_version"] + 1,
            {"title": "bad"},
        )
        self.assertEqual(edit["row_version"], error.context["session_row_version"])
        self.engine.rollback_transaction(txn)


class SavepointTests(EngineTestBase):
    def test_savepoint_restores_versions_sessions_locks_and_keeps_snapshot(self) -> None:
        txn = self.engine.open_transaction("REPEATABLE_READ", "sp-1")["txn_id"]
        baseline = self.engine.read_row(txn, "accounts", "A-1001")
        fixed_snapshot = baseline["snapshot_id"]

        edit_one = self.engine.open_edit_session(txn, "accounts", "A-1001")
        self.engine.save_edit_session(
            txn,
            edit_one["session_id"],
            edit_one["row_version"],
            {"name": "Before savepoint", "balance": 77},
        )
        savepoint = self.engine.create_savepoint(txn, "before_second_row")
        self.assertEqual(fixed_snapshot, savepoint["snapshot_id"])
        self.assertEqual(1, len(savepoint["locks_held"]))

        edit_two = self.engine.open_edit_session(txn, "accounts", "A-1002")
        self.engine.save_edit_session(
            txn,
            edit_two["session_id"],
            edit_two["row_version"],
            {"name": "After savepoint", "balance": 88},
        )
        query_before = self.engine.query_rows(txn, "accounts")
        before_by_id = {row["row_id"]: row for row in query_before["rows"]}
        self.assertEqual("After savepoint", before_by_id["A-1002"]["data"]["name"])

        rolled = self.engine.rollback_to_savepoint(txn, "before_second_row")
        self.assertEqual(1, rolled["undone_writes"])
        self.assertEqual(fixed_snapshot, rolled["snapshot_id"])
        self.assertEqual(1, len(rolled["locks_held"]))
        self.assertEqual("A-1001", rolled["locks_held"][0]["row_id"])
        self.assertGreaterEqual(rolled["restored_sessions"], 1)

        after = self.engine.query_rows(txn, "accounts")
        by_id = {row["row_id"]: row for row in after["rows"]}
        self.assertEqual("Before savepoint", by_id["A-1001"]["data"]["name"])
        self.assertEqual("Contoso Retail", by_id["A-1002"]["data"]["name"])
        self.assertEqual(1, by_id["A-1002"]["row_version"])

        self.assert_error("E_EDIT_SESSION_NOT_FOUND", self.engine.get_edit_session, txn, edit_two["session_id"])
        restored_session = self.engine.get_edit_session(txn, edit_one["session_id"])
        self.assertEqual("open", restored_session["session"]["status"])
        self.assertEqual(2, restored_session["session"]["row_version"])

        locks = self.engine.locks_snapshot()
        held = {(item["table"], item["row_id"]) for item in locks["locks"] if item["holder_txn_id"] == txn}
        self.assertEqual({("accounts", "A-1001")}, held)

        self.engine.commit_transaction(txn)
        observer = self.engine.open_transaction("READ_COMMITTED", "observer-sp")["txn_id"]
        row_one = self.engine.read_row(observer, "accounts", "A-1001")["row"]
        row_two = self.engine.read_row(observer, "accounts", "A-1002")["row"]
        self.assertEqual("Before savepoint", row_one["data"]["name"])
        self.assertEqual(2, row_one["row_version"])
        self.assertEqual(1, row_two["row_version"])
        self.engine.commit_transaction(observer)


class LockAndBudgetTests(EngineTestBase):
    engine_kwargs = {
        "lock_wait_ms": 130,
        "max_waiters": 4,
        "max_active_txns": 8,
    }

    def test_rc_fail_fast_rr_timeout_and_real_wakeup(self) -> None:
        holder = self.engine.open_transaction("REPEATABLE_READ", "holder")["txn_id"]
        holder_edit = self.engine.open_edit_session(holder, "accounts", "A-1004")
        self.engine.save_edit_session(
            holder,
            holder_edit["session_id"],
            holder_edit["row_version"],
            {"name": "Held write", "balance": 10},
        )

        rc = self.engine.open_transaction("READ_COMMITTED", "rc-conflict")["txn_id"]
        rc_edit = self.engine.open_edit_session(rc, "accounts", "A-1004")
        start = time.monotonic()
        conflict = self.assert_error(
            "E_WRITE_CONFLICT",
            self.engine.save_edit_session,
            rc,
            rc_edit["session_id"],
            rc_edit["row_version"],
            {"name": "RC no wait", "balance": 11},
        )
        self.assertLess(time.monotonic() - start, 0.1)
        self.assertEqual(holder, conflict.context["holder_txn_id"])
        self.assertEqual("READ_COMMITTED_FAIL_FAST", conflict.context["policy"])

        waiting = self.engine.open_transaction("REPEATABLE_READ", "waiter-timeout")["txn_id"]
        waiting_edit = self.engine.open_edit_session(waiting, "accounts", "A-1004")
        start = time.monotonic()
        timeout = self.assert_error(
            "E_LOCK_TIMEOUT",
            self.engine.save_edit_session,
            waiting,
            waiting_edit["session_id"],
            waiting_edit["row_version"],
            {"name": "Timeout", "balance": 12},
        )
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.115)
        self.assertLess(elapsed, 0.8)
        self.assertGreaterEqual(timeout.context["waited_ms"], 110)
        self.assertEqual("accounts", timeout.context["lock"]["table"])
        self.assertEqual("A-1004", timeout.context["lock"]["row_id"])
        self.assertIn("Condition.wait", timeout.context["wait_primitive"])

        wake = self.engine.open_transaction("REPEATABLE_READ", "waiter-wake")["txn_id"]
        wake_edit = self.engine.open_edit_session(wake, "accounts", "A-1004")
        result_holder: dict[str, object] = {}

        def save_after_wake() -> None:
            try:
                result_holder["result"] = self.engine.save_edit_session(
                    wake,
                    wake_edit["session_id"],
                    wake_edit["row_version"],
                    {"name": "Woken and saved", "balance": 13},
                )
            except BaseException as error:  # pragma: no cover - diagnostic capture
                result_holder["error"] = error

        thread = threading.Thread(target=save_after_wake, name="lock-wake-test")
        thread.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            waiting_now = self.engine.locks_snapshot()["waiting_count"]
            if waiting_now:
                break
            time.sleep(0.005)
        self.assertEqual(1, self.engine.locks_snapshot()["waiting_count"])
        self.engine.rollback_transaction(holder)
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", result_holder, result_holder.get("error"))
        self.assertEqual("Woken and saved", result_holder["result"]["row"]["data"]["name"])
        self.engine.commit_transaction(wake)
        self.engine.rollback_transaction(rc)
        self.engine.rollback_transaction(waiting)

    def test_txn_limit_and_wait_queue_capacity(self) -> None:
        self.engine.graceful_shutdown("reset_for_budget_test")
        self.engine = MVCCEngine(
            make_settings(
                Path(self.temp.name),
                lock_wait_ms=500,
                max_waiters=1,
                max_active_txns=3,
            )
        )
        a = self.engine.open_transaction("REPEATABLE_READ", "cap-a")["txn_id"]
        b = self.engine.open_transaction("REPEATABLE_READ", "cap-b")["txn_id"]
        c = self.engine.open_transaction("REPEATABLE_READ", "cap-c")["txn_id"]
        limit = self.assert_error(
            "E_TXN_LIMIT",
            self.engine.open_transaction,
            "READ_COMMITTED",
            "cap-d",
        )
        self.assertEqual(3, limit.context["max_active_txns"])

        a_edit = self.engine.open_edit_session(a, "documents", "DOC-002")
        self.engine.save_edit_session(
            a,
            a_edit["session_id"],
            a_edit["row_version"],
            {"title": "Locked", "state": "draft"},
        )
        b_edit = self.engine.open_edit_session(b, "documents", "DOC-002")
        b_result: dict[str, object] = {}

        def wait_on_a() -> None:
            try:
                b_result["result"] = self.engine.save_edit_session(
                    b,
                    b_edit["session_id"],
                    b_edit["row_version"],
                    {"title": "Queue occupant", "state": "draft"},
                )
            except BaseException as error:  # pragma: no cover - diagnostic capture
                b_result["error"] = error

        thread = threading.Thread(target=wait_on_a)
        thread.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and self.engine.locks_snapshot()["waiting_count"] == 0:
            time.sleep(0.005)
        self.assertEqual(1, self.engine.locks_snapshot()["waiting_count"])

        c_edit = self.engine.open_edit_session(c, "documents", "DOC-002")
        full = self.assert_error(
            "E_LOCK_QUEUE_FULL",
            self.engine.save_edit_session,
            c,
            c_edit["session_id"],
            c_edit["row_version"],
            {"title": "Rejected queue", "state": "draft"},
        )
        self.assertEqual(1, full.context["max_waiters"])

        self.engine.rollback_transaction(a)
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", b_result, b_result.get("error"))
        self.engine.rollback_transaction(b)
        self.engine.rollback_transaction(c)

    def test_cancel_waiting_transaction_wakes_it_and_releases_resources(self) -> None:
        holder = self.engine.open_transaction("REPEATABLE_READ", "cancel-holder")["txn_id"]
        holder_edit = self.engine.open_edit_session(holder, "accounts", "A-1002")
        self.engine.save_edit_session(
            holder,
            holder_edit["session_id"],
            holder_edit["row_version"],
            {"name": "Cancel holder", "balance": 1},
        )
        waiter = self.engine.open_transaction("REPEATABLE_READ", "cancel-waiter")["txn_id"]
        waiter_edit = self.engine.open_edit_session(waiter, "accounts", "A-1002")
        captured: dict[str, object] = {}

        def wait_then_cancel() -> None:
            try:
                self.engine.save_edit_session(
                    waiter,
                    waiter_edit["session_id"],
                    waiter_edit["row_version"],
                    {"name": "Must not save", "balance": 2},
                )
            except BaseException as error:
                captured["error"] = error

        thread = threading.Thread(target=wait_then_cancel)
        thread.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and self.engine.locks_snapshot()["waiting_count"] == 0:
            time.sleep(0.005)
        self.assertEqual(1, self.engine.locks_snapshot()["waiting_count"])
        self.engine.rollback_transaction(waiter, "explicit_cancel")
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        error = captured.get("error")
        self.assertIsInstance(error, AppError)
        self.assertEqual("E_TXN_CANCELED", error.code)
        self.assertEqual("rolled_back", self.engine.get_transaction(waiter)["status"])
        self.assertEqual(0, self.engine.locks_snapshot()["waiting_count"])
        self.engine.rollback_transaction(holder)


class DeadlockTests(EngineTestBase):
    engine_kwargs = {"lock_wait_ms": 1500, "max_waiters": 4, "max_active_txns": 6}

    def test_cycle_selects_deterministic_youngest_victim_and_wakes_other(self) -> None:
        first = self.engine.open_transaction("REPEATABLE_READ", "deadlock-a")["txn_id"]
        time.sleep(0.002)
        second = self.engine.open_transaction("REPEATABLE_READ", "deadlock-b")["txn_id"]

        a_row1 = self.engine.open_edit_session(first, "accounts", "A-1001")
        a_row2 = self.engine.open_edit_session(first, "accounts", "A-1002")
        b_row1 = self.engine.open_edit_session(second, "accounts", "A-1001")
        b_row2 = self.engine.open_edit_session(second, "accounts", "A-1002")

        self.engine.save_edit_session(
            first, a_row1["session_id"], a_row1["row_version"], {"name": "A owns row1"}
        )
        self.engine.save_edit_session(
            second, b_row2["session_id"], b_row2["row_version"], {"name": "B owns row2"}
        )

        a_result: dict[str, object] = {}

        def a_waits_for_b() -> None:
            try:
                a_result["result"] = self.engine.save_edit_session(
                    first,
                    a_row2["session_id"],
                    a_row2["row_version"],
                    {"name": "A takes row2"},
                )
            except BaseException as error:  # pragma: no cover - diagnostic capture
                a_result["error"] = error

        thread = threading.Thread(target=a_waits_for_b)
        thread.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if self.engine.locks_snapshot()["wait_graph"].get(first) == second:
                break
            time.sleep(0.005)
        self.assertEqual(second, self.engine.locks_snapshot()["wait_graph"].get(first))

        victim = self.assert_error(
            "E_DEADLOCK_VICTIM",
            self.engine.save_edit_session,
            second,
            b_row1["session_id"],
            b_row1["row_version"],
            {"name": "B takes row1"},
        )
        self.assertEqual(second, victim.context["victim_txn_id"])
        self.assertEqual({first, second}, set(victim.context["cycle"]))

        thread.join(timeout=3.0)
        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", a_result, a_result.get("error"))
        self.assertEqual("A takes row2", a_result["result"]["row"]["data"]["name"])
        self.engine.commit_transaction(first)

        second_state = self.engine.get_transaction(second)
        self.assertEqual("rolled_back", second_state["status"])
        self.assertEqual("deadlock_victim", second_state["abort_reason"])
        observer = self.engine.open_transaction("READ_COMMITTED", "deadlock-observer")["txn_id"]
        row2 = self.engine.read_row(observer, "accounts", "A-1002")["row"]
        self.assertEqual("A takes row2", row2["data"]["name"])
        self.engine.commit_transaction(observer)


class IdempotencyTests(EngineTestBase):
    def test_duplicate_open_and_commit_are_idempotent_with_conflict_detection(self) -> None:
        opened = self.engine.open_transaction("REPEATABLE_READ", "idem-1")
        replay_open = self.engine.open_transaction("REPEATABLE_READ", "idem-1")
        self.assertTrue(replay_open["replayed"])
        self.assertEqual(opened["txn_seq"], replay_open["txn_seq"])
        self.assert_error(
            "E_CONFLICTING_TXN",
            self.engine.open_transaction,
            "READ_COMMITTED",
            "idem-1",
        )

        first = self.engine.commit_transaction("idem-1", {"request_id": "commit-A"})
        second = self.engine.commit_transaction("idem-1", {"request_id": "commit-A"})
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["commit_seq"], second["commit_seq"])
        conflicting = self.assert_error(
            "E_CONFLICTING_TXN",
            self.engine.commit_transaction,
            "idem-1",
            {"request_id": "commit-B"},
        )
        self.assertIn("committed_request_hash", conflicting.context)

        observer = self.engine.open_transaction("READ_COMMITTED", "idem-observer")["txn_id"]
        self.engine.commit_transaction(observer)
        conn = self.engine.storage.connect()
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM committed_transactions WHERE txn_id='idem-1'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(1, count)


class AtomicReaderTests(EngineTestBase):
    engine_kwargs = {"max_active_txns": 12}

    def test_readers_never_observe_half_committed_update(self) -> None:
        writer = self.engine.open_transaction("READ_COMMITTED", "atomic-writer")["txn_id"]
        reader = self.engine.open_transaction("READ_COMMITTED", "atomic-reader")["txn_id"]
        writer_edit = self.engine.open_edit_session(writer, "accounts", "A-1003")
        expected_version = writer_edit["row_version"]
        done = threading.Event()
        failures: list[str] = []
        observed_versions: set[int] = set()

        def read_loop() -> None:
            try:
                while not done.is_set():
                    result = self.engine.read_row(reader, "accounts", "A-1003")
                    row = result["row"]
                    if row is None:
                        failures.append("row temporarily disappeared during commit")
                        return
                    observed_versions.add(int(row["row_version"]))
            except BaseException as error:  # pragma: no cover - diagnostic capture
                failures.append(repr(error))

        thread = threading.Thread(target=read_loop)
        thread.start()
        try:
            for index in range(20):
                if index > 0:
                    writer_edit = self.engine.open_edit_session(writer, "accounts", "A-1003")
                saved = self.engine.save_edit_session(
                    writer,
                    writer_edit["session_id"],
                    writer_edit["row_version"],
                    {"name": f"atomic-{index}", "balance": index},
                )
                expected_version = saved["new_row_version"]
                if index % 3 == 0:
                    self.engine.commit_transaction(writer)
                    writer = self.engine.open_transaction("READ_COMMITTED", f"atomic-writer-{index}")["txn_id"]
                    writer_edit = self.engine.open_edit_session(writer, "accounts", "A-1003")
            self.engine.commit_transaction(writer)
        finally:
            done.set()
            thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertTrue(observed_versions)
        final = self.engine.read_row(reader, "accounts", "A-1003")["row"]
        self.assertIsNotNone(final)
        self.assertGreaterEqual(final["row_version"], 20)
        self.engine.commit_transaction(reader)


class RecoveryTests(unittest.TestCase):
    def test_forced_termination_recovery_keeps_only_committed_facts(self) -> None:
        temp = temporary_data_dir()
        self.addCleanup(temp.cleanup)
        data_dir = Path(temp.name)
        settings = make_settings(data_dir, lock_wait_ms=500, edit_budget_ms=5000)

        first_engine = MVCCEngine(settings)
        committed = first_engine.open_transaction("READ_COMMITTED", "survivor")["txn_id"]
        survivor_edit = first_engine.open_edit_session(committed, "accounts", "A-1001")
        first_engine.save_edit_session(
            committed,
            survivor_edit["session_id"],
            survivor_edit["row_version"],
            {"name": "Committed survivor", "balance": 999},
        )
        first_engine.commit_transaction(committed, {"request_id": "survivor-commit"})

        active = first_engine.open_transaction("REPEATABLE_READ", "crash-active")["txn_id"]
        first_edit = first_engine.open_edit_session(active, "accounts", "A-1002")
        first_engine.save_edit_session(
            active,
            first_edit["session_id"],
            first_edit["row_version"],
            {"name": "Uncommitted crash write", "balance": -1},
        )
        first_engine.create_savepoint(active, "crash-point")
        second_edit = first_engine.open_edit_session(active, "accounts", "A-1003")
        first_engine.save_edit_session(
            active,
            second_edit["session_id"],
            second_edit["row_version"],
            {"name": "Second uncommitted write", "balance": -2},
        )

        # Simulate SIGKILL/TerminateProcess: no graceful_shutdown, rollback, or
        # close handshake is performed.  A new process must reconstruct state.
        second_engine = MVCCEngine(settings)
        self.addCleanup(second_engine.graceful_shutdown, "recovery_test_teardown")
        report = second_engine.get_recovery_report()
        self.assertEqual("startup_recovery", report["report_kind"])
        self.assertEqual("recovered_unfinished_transactions", report["status"])
        self.assertEqual(["crash-active"], [item["txn_id"] for item in report["recovered_txns"]])
        self.assertGreaterEqual(report["uncommitted_versions_removed"], 2)
        self.assertGreaterEqual(report["edit_sessions_released"], 2)
        self.assertGreaterEqual(report["lock_resources_released"], 1)
        self.assertFalse(report["graceful_exit_report_available"])
        self.assertTrue(settings.recovery_report_path.exists())
        persisted_report = json.loads(settings.recovery_report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["status"], persisted_report["status"])
        self.assertTrue(settings.intent_log_path.exists())
        self.assertGreater(settings.intent_log_path.stat().st_size, 0)

        observer = second_engine.open_transaction("READ_COMMITTED", "recovery-observer")["txn_id"]
        survivor_row = second_engine.read_row(observer, "accounts", "A-1001")["row"]
        reverted_two = second_engine.read_row(observer, "accounts", "A-1002")["row"]
        reverted_three = second_engine.read_row(observer, "accounts", "A-1003")["row"]
        self.assertEqual("Committed survivor", survivor_row["data"]["name"])
        self.assertEqual(2, survivor_row["row_version"])
        self.assertEqual("Contoso Retail", reverted_two["data"]["name"])
        self.assertEqual(1, reverted_two["row_version"])
        self.assertEqual("Fabrikam Labs", reverted_three["data"]["name"])
        self.assertEqual(1, reverted_three["row_version"])
        second_engine.commit_transaction(observer)

        active_state = second_engine.get_transaction("crash-active")
        self.assertEqual("recovered_aborted", active_state["status"])
        self.assertEqual("startup_recovery", active_state["abort_reason"])

        replay = second_engine.commit_transaction(
            "survivor", {"request_id": "survivor-commit"}
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(2, replay["commit_seq"])
        with self.assertRaises(AppError) as conflict:
            second_engine.commit_transaction(
                "survivor", {"request_id": "different-after-restart"}
            )
        self.assertEqual("E_CONFLICTING_TXN", conflict.exception.code)


if __name__ == "__main__":
    unittest.main(verbosity=2)

