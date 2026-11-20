"""Stage checkpoints: atomic record writes, file-locked monotonic sequence,
and publish protection (E_STALE_CHECKPOINT / E_INVALID_STAGE)."""

import contextlib
import fcntl
import json
import os

from .canon import canon_json
from .errors import CmrError, E_INVALID_STAGE, E_STALE_CHECKPOINT
from .config import STAGES

GLOBAL_SHARDS = {"__index__", "__recall__"}


def atomic_write_bytes(path, data):
    """Write bytes atomically: tmp file + fsync + os.replace + dir fsync."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(os.path.dirname(path), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def atomic_write_json(path, obj):
    atomic_write_bytes(path, (canon_json(obj) + "\n").encode("utf-8"))


def read_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


class StateStore:
    """Paths and locking for the state directory."""

    def __init__(self, state_dir):
        self.state_dir = state_dir
        for sub in ("shards", "checkpoints", "index", "recall", "published", "injections", "locks"):
            os.makedirs(os.path.join(state_dir, sub), exist_ok=True)

    def path(self, *parts):
        return os.path.join(self.state_dir, *parts)

    @contextlib.contextmanager
    def lock(self):
        """Exclusive advisory file lock guarding checkpoint publication."""
        lock_path = self.path("locks", "publish.lock")
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        with open(lock_path, "a+b") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    # ---- checkpoint records ----
    def list_checkpoints(self):
        ckpt_dir = self.path("checkpoints")
        records = []
        for name in sorted(os.listdir(ckpt_dir)):
            if name.endswith(".json") and not name.startswith("."):
                records.append(read_json(os.path.join(ckpt_dir, name)))
        return records

    def last_seq(self):
        seqs = [rec["seq"] for rec in self.list_checkpoints() if rec and "seq" in rec]
        return max(seqs) if seqs else 0

    def valid_checkpoint(self, shard, stage, stage_fp, shard_fp):
        for rec in self.list_checkpoints():
            if (rec.get("shard") == shard and rec.get("stage") == stage
                    and rec.get("stage_fp") == stage_fp and rec.get("shard_fp") == shard_fp):
                return rec
        return None

    def publish_checkpoint(self, record, expected_seq=None):
        """Publish under lock. Only consecutive sequence numbers are accepted.

        expected_seq: caller's view of the next sequence (optimistic publish);
        mismatch -> E_STALE_CHECKPOINT. Missing upstream stage artifact ->
        E_INVALID_STAGE.
        """
        stage = record["stage"]
        shard = record["shard"]
        if stage not in STAGES:
            raise CmrError(E_INVALID_STAGE, f"unknown stage '{stage}'", stage=stage)
        idx = STAGES.index(stage)
        if idx > 0 and shard not in GLOBAL_SHARDS:
            upstream = STAGES[idx - 1]
            if not self._shard_stage_done(shard, upstream):
                raise CmrError(
                    E_INVALID_STAGE,
                    f"upstream stage '{upstream}' artifact missing for shard '{shard}'",
                    stage=stage, detail={"missing_stage": upstream, "shard": shard},
                )
        with self.lock():
            next_seq = self.last_seq() + 1
            if expected_seq is not None and expected_seq != next_seq:
                raise CmrError(
                    E_STALE_CHECKPOINT,
                    f"stale checkpoint seq {expected_seq}, workspace expects {next_seq}",
                    stage=stage, detail={"expected": next_seq, "got": expected_seq},
                )
            record = dict(record)
            record["seq"] = next_seq
            name = f"{next_seq:06d}_{shard}_{stage}.json"
            atomic_write_json(self.path("checkpoints", name), record)
        return record

    def _shard_stage_done(self, shard, stage):
        return any(rec.get("shard") == shard and rec.get("stage") == stage
                   for rec in self.list_checkpoints())

    def clear_stage_artifacts(self, stages):
        """Remove checkpoints and artifacts of the given stages (invalidation)."""
        ckpt_dir = self.path("checkpoints")
        for name in sorted(os.listdir(ckpt_dir)):
            if not name.endswith(".json"):
                continue
            rec = read_json(os.path.join(ckpt_dir, name))
            if rec and rec.get("stage") in stages:
                os.remove(os.path.join(ckpt_dir, name))
        if "features" in stages or "projection" in stages:
            shards_dir = self.path("shards")
            if os.path.isdir(shards_dir):
                for shard in os.listdir(shards_dir):
                    shard_dir = os.path.join(shards_dir, shard)
                    if not os.path.isdir(shard_dir):
                        continue
                    for fname in list(os.listdir(shard_dir)):
                        if "features" in stages and fname.startswith("features"):
                            os.remove(os.path.join(shard_dir, fname))
                        if "projection" in stages and fname.startswith("projected"):
                            os.remove(os.path.join(shard_dir, fname))
        if "index" in stages:
            for fname in ("index.json", "vectors.npy"):
                p = self.path("index", fname)
                if os.path.exists(p):
                    os.remove(p)
        if "recall" in stages:
            p = self.path("recall", "recall.json")
            if os.path.exists(p):
                os.remove(p)

    # ---- stop / resume ----
    def stop_requested(self):
        return os.path.exists(self.path("STOP_REQUESTED"))

    def request_stop(self):
        atomic_write_bytes(self.path("STOP_REQUESTED"), b"stop\n")

    def clear_stop(self):
        p = self.path("STOP_REQUESTED")
        if os.path.exists(p):
            os.remove(p)
