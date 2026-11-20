"""Persistent workspace state: config, sample registry, artifacts, journal.

Journal records are appended atomically (one file per record, seq in the
file name). Recovery scans for the longest consistent prefix: contiguous
seqs starting at 1, parseable JSON, and artifact digests that match the
recorded values. Anything beyond is treated as a torn tail and ignored.
"""

from __future__ import annotations

import glob
import json
import os

from .errors import InvalidStageError, StaleCheckpointError
from .util import atomic_write, canonical_json, file_lock, sha256_bytes

PREDECESSOR = {"infer": None, "encode": "infer", "select": "encode", "summarize": "select"}


class Store:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)

    @property
    def config_path(self):
        return os.path.join(self.root, "config.json")

    @property
    def sample_path(self):
        return os.path.join(self.root, "sample.json")

    @property
    def journal_dir(self):
        return os.path.join(self.root, "journal")

    @property
    def artifacts_dir(self):
        return os.path.join(self.root, "artifacts")

    @property
    def lock_path(self):
        return os.path.join(self.root, "publish.lock")

    @property
    def stop_path(self):
        return os.path.join(self.root, "STOP_REQUESTED")

    def ensure(self):
        os.makedirs(self.journal_dir, exist_ok=True)
        os.makedirs(self.artifacts_dir, exist_ok=True)

    def load_config_store(self):
        if not os.path.exists(self.config_path):
            return None
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_config_store(self, obj):
        atomic_write(self.config_path, canonical_json(obj))

    def load_sample_registry(self):
        if not os.path.exists(self.sample_path):
            return None
        with open(self.sample_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_sample_registry(self, obj):
        atomic_write(self.sample_path, canonical_json(obj))

    def artifact_meta_path(self, cache_key: str) -> str:
        return os.path.join(self.artifacts_dir, cache_key + ".json")

    def artifact_matrix_path(self, cache_key: str) -> str:
        return os.path.join(self.artifacts_dir, cache_key + ".f64")

    def cache_has(self, cache_key: str) -> bool:
        return os.path.exists(self.artifact_meta_path(cache_key))

    def write_artifact(self, cache_key: str, meta_bytes: bytes, matrix_bytes=None):
        atomic_write(self.artifact_meta_path(cache_key), meta_bytes)
        if matrix_bytes is not None:
            atomic_write(self.artifact_matrix_path(cache_key), matrix_bytes)

    def read_artifact(self, cache_key: str):
        with open(self.artifact_meta_path(cache_key), "rb") as f:
            meta = f.read()
        matrix = None
        mpath = self.artifact_matrix_path(cache_key)
        if os.path.exists(mpath):
            with open(mpath, "rb") as f:
                matrix = f.read()
        return meta, matrix

    def _journal_path(self, seq: int, stage: str) -> str:
        return os.path.join(self.journal_dir, "%06d_%s.json" % (seq, stage))

    def recover(self):
        """Longest consistent journal prefix; torn/corrupt tails are ignored."""
        records = []
        seq = 1
        while True:
            matches = glob.glob(os.path.join(self.journal_dir, "%06d_*.json" % seq))
            if len(matches) != 1:
                break
            try:
                with open(matches[0], "rb") as f:
                    record = json.loads(f.read().decode("utf-8"))
            except (OSError, ValueError):
                break
            if not isinstance(record, dict) or record.get("seq") != seq:
                break
            required = ("stage", "sample_fp", "config_version", "stage_fp",
                        "cache_key", "upstream_fp", "artifact_digest")
            if any(k not in record for k in required):
                break
            try:
                meta, matrix = self.read_artifact(record["cache_key"])
            except OSError:
                break
            if sha256_bytes(meta) != record["artifact_digest"]:
                break
            if "matrix_digest" in record:
                if matrix is None or sha256_bytes(matrix) != record["matrix_digest"]:
                    break
            records.append(record)
            seq += 1
        return records

    def head_seq(self) -> int:
        return len(self.recover())

    def find_record(self, cache_key: str):
        for record in self.recover():
            if record["cache_key"] == cache_key:
                return record
        return None

    def publish(self, record: dict) -> dict:
        """Publish a journal record under the workspace file lock.

        Only strictly consecutive sequence numbers are accepted; a lagging or
        duplicate seq raises E_STALE_CHECKPOINT, and a record whose
        predecessor stage was never published raises E_INVALID_STAGE.
        """
        with file_lock(self.lock_path):
            records = self.recover()
            head = len(records)
            if record["seq"] != head + 1:
                raise StaleCheckpointError(
                    "stale checkpoint: record seq does not extend the journal head",
                    {"record_seq": record["seq"], "head_seq": head,
                     "stage": record.get("stage")},
                )
            stage = record["stage"]
            predecessor = PREDECESSOR[stage]
            if predecessor is not None:
                ok = any(
                    r["stage"] == predecessor and r["stage_fp"] == record["upstream_fp"]
                    for r in records
                )
                if not ok:
                    raise InvalidStageError(
                        "predecessor stage artifact missing for this lineage",
                        {"stage": stage, "requires": predecessor,
                         "upstream_fp": record["upstream_fp"]},
                    )
            atomic_write(self._journal_path(record["seq"], stage), canonical_json(record))
        return record

    def request_stop(self):
        atomic_write(self.stop_path, b"stop\n")

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_path)

    def clear_stop(self):
        try:
            os.unlink(self.stop_path)
        except OSError:
            pass
