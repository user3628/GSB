"""Atomic stage checkpoints, real file locks and publish guards.

A checkpoint record is written atomically (temp file + ``fsync`` + ``rename``)
so a crash at any point never leaves a half-written record.  Cross-process
publishing uses a POSIX ``fcntl.flock`` on a dedicated lock file, and every
publish validates stage topology and monotonic sequence numbers.
"""

import fcntl
import os
from contextlib import contextmanager

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, LegalCiteError
from .serde import fingerprint, read_json, write_json_atomic

STAGE_ORDER = ("parse", "normalize", "resolve", "evidence")


@contextmanager
def file_lock(lock_path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _index_path(ckpt_dir):
    return ckpt_dir / "checkpoint_index.json"


def load_index(ckpt_dir):
    path = _index_path(ckpt_dir)
    if not path.exists():
        return {"next_seq": 1, "records": []}
    return read_json(path)


def _atomic_write_index(ckpt_dir, index):
    path = _index_path(ckpt_dir)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        import json
        fh.write(json.dumps(index, ensure_ascii=False, sort_keys=True,
                            indent=2))
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def last_completed_stage(index):
    if not index["records"]:
        return None
    return index["records"][-1]["stage"]


def publish_checkpoint(ckpt_dir, stage, input_digest, config_version,
                       artifact_digest, pipeline_fingerprint):
    """Publish a checkpoint under a real file lock with sequence checks."""
    if stage not in STAGE_ORDER:
        raise LegalCiteError(E_INVALID_STAGE, f"unknown stage {stage!r}",
                             {"stage": stage})
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    lock_path = ckpt_dir / "publish.lock"
    with file_lock(lock_path):
        index = load_index(ckpt_dir)
        seq = index["next_seq"]
        prev_stage = last_completed_stage(index)
        expected_index = STAGE_ORDER.index(stage)
        if prev_stage is not None:
            prev_index = STAGE_ORDER.index(prev_stage)
            if expected_index <= prev_index:
                raise LegalCiteError(
                    E_STALE_CHECKPOINT,
                    "duplicate, stale or out-of-order stage publish",
                    {"stage": stage, "previous_stage": prev_stage,
                     "next_seq": seq},
                )
        if expected_index != len(index["records"]):
            raise LegalCiteError(
                E_INVALID_STAGE,
                "missing prerequisite stage checkpoint",
                {"stage": stage, "have": [r["stage"] for r in index["records"]]},
            )
        record = {
            "seq": seq,
            "stage": stage,
            "input_digest": input_digest,
            "config_version": config_version,
            "artifact_digest": artifact_digest,
            "pipeline_fingerprint": pipeline_fingerprint,
        }
        record_path = ckpt_dir / f"ckpt_{seq:04d}_{stage}.json"
        write_json_atomic(record_path, record)
        index["records"].append(record)
        index["next_seq"] = seq + 1
        _atomic_write_index(ckpt_dir, index)
        return record


def reusable_checkpoints(index, stage_fingerprints):
    """Return stages whose stored fingerprints still match current inputs."""
    reusable = []
    for record in index["records"]:
        expected = stage_fingerprints.get(record["stage"])
        if expected is None or record["pipeline_fingerprint"] != expected:
            break
        reusable.append(record["stage"])
    return reusable


def reset_checkpoints(ckpt_dir):
    if not ckpt_dir.exists():
        return
    for path in ckpt_dir.glob("ckpt_*.json"):
        path.unlink()
    idx = _index_path(ckpt_dir)
    if idx.exists():
        idx.unlink()


def content_digest(obj):
    return fingerprint(obj)
