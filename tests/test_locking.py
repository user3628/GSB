"""File lock behavior: mutual exclusion and structured timeout."""

from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from voxel_engine.errors import E_LOCK_TIMEOUT, EngineError
from voxel_engine.locking import FileLock


class LockingTest(unittest.TestCase):
    def test_acquire_and_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.lock"
            with FileLock(path, timeout_ms=500):
                pass
            with FileLock(path, timeout_ms=500):
                pass

    def test_lock_timeout_is_structured(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.lock"
            holder = FileLock(path, timeout_ms=1000)
            holder.acquire()
            errors = []

            def contender():
                try:
                    FileLock(path, timeout_ms=300, poll_interval_s=0.02).acquire()
                except EngineError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=contender)
            started = time.monotonic()
            thread.start()
            thread.join()
            holder.release()
            self.assertEqual(len(errors), 1)
            error = errors[0]
            self.assertEqual(error.code, E_LOCK_TIMEOUT)
            self.assertEqual(error.details["timeout_ms"], 300)
            self.assertIn("lock_path", error.details)
            self.assertGreaterEqual(time.monotonic() - started, 0.25)

    def test_lock_held_blocks_other_handle(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.lock"
            with FileLock(path, timeout_ms=500):
                acquired = []

                def try_lock():
                    try:
                        FileLock(path, timeout_ms=100).acquire()
                        acquired.append(True)
                    except EngineError:
                        acquired.append(False)

                thread = threading.Thread(target=try_lock)
                thread.start()
                thread.join()
                self.assertEqual(acquired, [False])


if __name__ == "__main__":
    unittest.main()
