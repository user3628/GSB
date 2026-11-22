import multiprocessing
import os
import tempfile
import unittest

from qrwb.checkpoints import CheckpointStore
from qrwb.errors import E_STALE_CHECKPOINT
from qrwb.util import FileLock, atomic_write_text


def _publisher(workspace: str, query_id: str, stage_seq: int,
               barrier) -> None:
    from qrwb.config import STAGES
    store = CheckpointStore(workspace)
    barrier.wait()
    stage = STAGES[stage_seq - 1]
    fps = {
        1: {"input_fingerprint": "in", "config_version": "cfg"},
        2: {"input_fingerprint": "in", "config_version": "cfg",
            "synonym_fingerprint": "s", "glyph_fingerprint": "g",
            "bridge_fingerprint": "b", "routes_fingerprint": "r"},
    }[stage_seq]
    try:
        store.publish(
            query_id, stage,
            input_fingerprint="in", config_version="cfg",
            product={"seq_marker": stage_seq},
            stage_fingerprints=fps)
    except Exception as exc:  # 子进程回传错误码字符串
        with open(os.path.join(workspace, f"mp-{os.getpid()}.err"),
                  "w", encoding="utf-8") as handle:
            handle.write(getattr(exc, "code", type(exc).__name__))


class CheckpointLockingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_atomic_write_leaves_no_half_files(self):
        target = os.path.join(self.tmp, "a.json")
        atomic_write_text(target, "complete\n")
        self.assertEqual(open(target, encoding="utf-8").read(), "complete\n")
        leftovers = [n for n in os.listdir(self.tmp) if n.startswith(".tmp-")]
        self.assertEqual(leftovers, [])

    def test_file_lock_mutual_exclusion(self):
        lock_path = os.path.join(self.tmp, "locks", "x.lock")
        with FileLock(lock_path):
            self.assertTrue(os.path.exists(lock_path))

    def test_multiprocess_stale_publish_rejected(self):
        ws = os.path.join(self.tmp, "mpws")
        os.makedirs(ws, exist_ok=True)
        store = CheckpointStore(ws)
        qid = "q-mp"
        # 先正常发布 seq=1
        store.publish(qid, "dispatch", input_fingerprint="in",
                      config_version="cfg", product={"v": 1},
                      stage_fingerprints={"input_fingerprint": "in",
                                          "config_version": "cfg"})
        ctx = multiprocessing.get_context("fork")
        barrier = ctx.Barrier(2)
        procs = [
            ctx.Process(target=_publisher, args=(ws, qid, 1, barrier)),
            ctx.Process(target=_publisher, args=(ws, qid, 2, barrier)),
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=30)
        for proc in procs:
            self.assertEqual(proc.exitcode, 0)
        err_files = [f for f in os.listdir(ws) if f.startswith("mp-")]
        codes = set()
        for name in err_files:
            codes.add(open(os.path.join(ws, name), encoding="utf-8").read())
        # seq=1 的重复发布必然被拒；seq=2 成功
        self.assertIn(E_STALE_CHECKPOINT, codes)
        # 父进程 store 是旧内存索引，需重新从磁盘装载子进程的发布结果
        reloaded = CheckpointStore(ws)
        self.assertIsNotNone(reloaded.get(qid, "candidate"))
        self.assertIsNone(reloaded.get(qid, "orchestration"))

    def test_checkpoint_sequence_continuous(self):
        store = CheckpointStore(os.path.join(self.tmp, "seqws"))
        qid = "q-seq"
        fps = {"input_fingerprint": "in", "config_version": "cfg"}
        for stage in ("dispatch", "candidate", "orchestration", "validation"):
            if stage == "candidate":
                fps = {**fps, "synonym_fingerprint": "s",
                       "glyph_fingerprint": "g", "bridge_fingerprint": "b",
                       "routes_fingerprint": "r"}
            if stage == "orchestration":
                fps = {"input_fingerprint": "in", "config_version": "cfg",
                       "budget_fingerprint": "bud"}
            if stage == "validation":
                fps = {"input_fingerprint": "in", "config_version": "cfg",
                       "adjudication_fingerprint": "adj"}
            store.publish(qid, stage, input_fingerprint="in",
                          config_version="cfg", product={"stage": stage},
                          stage_fingerprints=fps)
        seqs = [r["seq"] for r in store._records[qid]]
        self.assertEqual(seqs, [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
