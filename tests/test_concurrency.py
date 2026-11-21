"""跨进程文件锁并发发布保护测试（真实多进程，而非线程）。"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest

from recon3d.checkpoint import CheckpointStore

WORKER = """
import sys
from recon3d.checkpoint import CheckpointStore
run_dir, worker_id = sys.argv[1], sys.argv[2]
store = CheckpointStore(run_dir)
for i in range(5):
    store.publish("features", f"tile_{worker_id}_{i}", f"in-{worker_id}-{i}", "cfg-1", f"art-{worker_id}-{i}")
"""


class ConcurrencyTests(unittest.TestCase):
    def test_multiprocess_continuous_sequence(self):
        with tempfile.TemporaryDirectory() as tmp:
            worker = os.path.join(tmp, "worker.py")
            with open(worker, "w") as handle:
                handle.write(WORKER)
            env = dict(os.environ, PYTHONPATH=os.getcwd())
            procs = [
                subprocess.Popen([sys.executable, worker, tmp, name], env=env)
                for name in ("a", "b")
            ]
            for proc in procs:
                self.assertEqual(proc.wait(timeout=60), 0)
            seqs = [record["seq"] for record in CheckpointStore(tmp).load_journal()]
            self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
            self.assertEqual(len(seqs), 10)


if __name__ == "__main__":
    unittest.main()
