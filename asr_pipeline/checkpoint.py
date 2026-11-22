"""Atomic stage checkpoints with real OS file locks and monotonic sequence."""

import contextlib
import json
import os

from .config import STAGES
from .errors import CorruptCheckpointError, InvalidStageError, StaleCheckpointError
from .hashing import canonical_json, digest_json, sha256_file

try:
    import fcntl

    _PLATFORM = "posix"
except ImportError:
    import msvcrt

    _PLATFORM = "windows"

STAGE_ORDER = {stage: index for index, stage in enumerate(STAGES)}


class FileLock:
    """Advisory lock backed by a real OS handle, no admin rights required."""

    def __init__(self, path):
        self.path = path
        self.handle = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.handle = open(self.path, "a+b")
        if _PLATFORM == "posix":
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        else:
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_LOCK, 1)
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.handle is None:
            return
        if _PLATFORM == "posix":
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        else:
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        self.handle.close()
        self.handle = None


class CheckpointStore:
    def __init__(self, workspace_dir):
        self.root = os.path.abspath(workspace_dir)
        self.shard_dir = os.path.join(self.root, "shards")
        self.lock_path = os.path.join(self.root, "publish.lock")
        self.sequence_path = os.path.join(self.root, "sequence.json")
        os.makedirs(self.shard_dir, exist_ok=True)

    def shard_path(self, shard_id, stage):
        directory = os.path.join(self.shard_dir, shard_id)
        return os.path.join(directory, f"{stage}.json")

    def _read_sequence(self):
        if not os.path.exists(self.sequence_path):
            return {"counter": 0}
        with open(self.sequence_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_sequence(self, data):
        tmp = self.sequence_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(data))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.sequence_path)

    def load_record(self, shard_id, stage):
        path = self.shard_path(shard_id, stage)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read()
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CorruptCheckpointError(
                "checkpoint record is not valid JSON",
                {"shard_id": shard_id, "stage": stage, "detail": str(exc)},
            )
        required = ("shard_id", "stage", "input_fingerprint", "config_fingerprint",
                    "artifact_digest", "seq", "artifact")
        if any(key not in record for key in required):
            raise CorruptCheckpointError(
                "checkpoint record is missing required fields",
                {"shard_id": shard_id, "stage": stage},
            )
        if record["shard_id"] != shard_id or record["stage"] != stage:
            raise CorruptCheckpointError(
                "checkpoint identity does not match its location",
                {"shard_id": shard_id, "stage": stage},
            )
        expected_digest = digest_json(record["artifact"])
        if expected_digest != record["artifact_digest"]:
            raise CorruptCheckpointError(
                "checkpoint artifact digest does not match",
                {"shard_id": shard_id, "stage": stage},
            )
        return record

    def publish(self, shard_id, stage, artifact, input_fingerprint, config_fingerprint):
        if stage not in STAGE_ORDER:
            raise InvalidStageError("unknown stage", {"stage": stage})
        with FileLock(self.lock_path):
            seq_state = self._read_sequence()
            stage_index = STAGE_ORDER[stage]
            for prior_index in range(stage_index):
                prior = STAGES[prior_index]
                if not os.path.exists(self.shard_path(shard_id, prior)):
                    raise InvalidStageError(
                        "missing prerequisite stage artifact",
                        {"shard_id": shard_id, "stage": prior},
                    )
            existing = None
            if os.path.exists(self.shard_path(shard_id, stage)):
                try:
                    existing = self.load_record(shard_id, stage)
                except CorruptCheckpointError:
                    existing = None
            if existing is not None:
                if existing.get("input_fingerprint") == input_fingerprint and \
                        existing.get("config_fingerprint") == config_fingerprint:
                    raise StaleCheckpointError(
                        "duplicate publish for an identical stage record",
                        {"shard_id": shard_id, "stage": stage, "seq": existing["seq"]},
                    )
                raise StaleCheckpointError(
                    "stage was already published with a different fingerprint",
                    {"shard_id": shard_id, "stage": stage, "seq": existing["seq"]},
                )
            expected_seq = seq_state["counter"] + 1
            record = {
                "shard_id": shard_id,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
                "artifact_digest": digest_json(artifact),
                "seq": expected_seq,
                "artifact": artifact,
            }
            target = self.shard_path(shard_id, stage)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(canonical_json(record))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
            self._write_sequence({"counter": expected_seq})
            return record

    def force_rebuild(self, shard_id, stage, artifact, input_fingerprint, config_fingerprint):
        """Remove this stage and every downstream stage before republishing."""
        with FileLock(self.lock_path):
            stage_index = STAGE_ORDER[stage]
            for downstream in STAGES[stage_index:]:
                target = self.shard_path(shard_id, downstream)
                if os.path.exists(target):
                    os.remove(target)
            seq_state = self._read_sequence()
            expected_seq = seq_state["counter"] + 1
            record = {
                "shard_id": shard_id,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
                "artifact_digest": digest_json(artifact),
                "seq": expected_seq,
                "artifact": artifact,
            }
            target = self.shard_path(shard_id, stage)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(canonical_json(record))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
            self._write_sequence({"counter": expected_seq})
            return record

    def recover(self):
        """Scan all records, ignoring corrupt/incomplete/duplicate sequences."""
        ignored = []
        valid = {}
        if not os.path.isdir(self.shard_dir):
            return {"valid": {}, "ignored": ignored}
        for shard_id in sorted(os.listdir(self.shard_dir)):
            directory = os.path.join(self.shard_dir, shard_id)
            if not os.path.isdir(directory):
                continue
            valid[shard_id] = {}
            for stage in STAGES:
                path = self.shard_path(shard_id, stage)
                if not os.path.exists(path):
                    continue
                try:
                    record = self.load_record(shard_id, stage)
                except CorruptCheckpointError as exc:
                    ignored.append(exc.to_dict())
                    continue
                if record["seq"] in [item["seq"] for item in valid[shard_id].values()]:
                    ignored.append(
                        {
                            "code": "E_CORRUPT_RECORD",
                            "message": "duplicate monotonic sequence ignored",
                            "context": {"shard_id": shard_id, "stage": stage, "seq": record["seq"]},
                        }
                    )
                    continue
                valid[shard_id][stage] = record
        return {"valid": valid, "ignored": ignored}

    def summary_path(self):
        return os.path.join(self.root, "summary.json")

    def write_summary(self, summary):
        target = self.summary_path()
        tmp = target + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(summary))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)

    def read_summary(self):
        path = self.summary_path()
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def stage_status(self):
        state = self.recover()
        result = {}
        for shard_id, records in state["valid"].items():
            result[shard_id] = {
                stage: {
                    "seq": records[stage]["seq"],
                    "artifact_digest": records[stage]["artifact_digest"],
                    "input_fingerprint": records[stage]["input_fingerprint"],
                    "config_fingerprint": records[stage]["config_fingerprint"],
                }
                for stage in records
            }
        return {"shards": result, "ignored": state["ignored"]}
