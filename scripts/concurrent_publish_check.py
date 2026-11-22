"""Two-process concurrent publish verification using real OS file locks.

Run directly:  python scripts/concurrent_publish_check.py
No administrator privileges are required; the lock is an advisory user-space
file handle (fcntl.flock on POSIX, msvcrt.locking on Windows).
"""

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from asr_pipeline.checkpoint import CheckpointStore, FileLock
from asr_pipeline.errors import InvalidStageError, StaleCheckpointError


def worker(workspace, shard_id, stage, artifact, index):
    store = CheckpointStore(workspace)
    input_fp = f"input-{shard_id}"
    cfg_fp = "cfg-v1"
    try:
        record = store.publish(shard_id, stage, artifact, input_fp, cfg_fp)
        return {"worker": index, "ok": True, "seq": record["seq"]}
    except StaleCheckpointError as exc:
        return {"worker": index, "ok": False, "code": exc.code}


def main():
    workspace = tempfile.mkdtemp(prefix="asr_concurrent_")
    lock_path = os.path.join(workspace, "publish.lock")

    helper = os.path.join(os.path.dirname(__file__), "scripts_concurrent_helper.py")
    with FileLock(lock_path):
        blocked = subprocess.run(
            [sys.executable, helper, "try-lock", workspace],
            capture_output=True,
            text=True,
            timeout=10,
        )
    assert blocked.returncode == 0, blocked.stderr
    assert json.loads(blocked.stdout)["acquired"] is False
    print("real file lock blocks a second process while held: OK")

    released = subprocess.run(
        [sys.executable, helper, "try-lock", workspace],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(released.stdout)["acquired"] is True
    print("lock available again after release: OK")

    env = dict(os.environ, PYTHONPATH=ROOT)
    procs = []
    for index in range(2):
        procs.append(
            subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "--worker",
                 str(index), workspace],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )
    outcomes = [json.loads(proc.stdout.readline()) for proc in procs]
    for proc in procs:
        proc.wait()
    winners = [item for item in outcomes if item["ok"]]
    losers = [item for item in outcomes if not item["ok"]]
    assert len(winners) == 1, outcomes
    assert len(losers) == 1 and losers[0]["code"] == "E_STALE_CHECKPOINT", outcomes
    print("concurrent publish -> one winner, one", losers[0]["code"], outcomes)

    store = CheckpointStore(workspace)
    try:
        store.publish("orphan", "decode", {"x": 1}, "input-orphan", "cfg-v1")
    except InvalidStageError as exc:
        print("missing prerequisite stage ->", exc.code)
    else:
        raise AssertionError("expected E_INVALID_STAGE")

    print("ALL CONCURRENT PUBLISH CHECKS PASSED")


def worker_main():
    index = int(sys.argv[2])
    workspace = sys.argv[3]
    artifact = {"worker_marker": index, "payload": list(range(100))}
    print(json.dumps(worker(workspace, "race-shard", "frontend", artifact, index)))


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--worker":
        worker_main()
    else:
        main()
