"""Atomic stage checkpoints, file locks and version-based invalidation."""

from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import tempfile

from ..errors import InvalidStageError, StaleCheckpointError
from .config import canonical_json, content_fingerprint

STAGES = ("config", "bias", "binning", "tolerance")
STAGE_ORDER = {name: idx for idx, name in enumerate(STAGES)}


def atomic_write(path, payload: dict):
    """Write JSON via temp file + fsync + atomic rename.

    A crash at any point leaves either the old file or the complete new one,
    never a half-written record.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


@contextlib.contextmanager
def file_lock(lock_path):
    os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield handle
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


class CheckpointStore:
    def __init__(self, directory):
        self.directory = directory
        self.lock_path = os.path.join(directory, ".publish.lock")
        os.makedirs(directory, exist_ok=True)

    def path_for(self, stage):
        return os.path.join(self.directory, f"stage_{stage}.json")

    def load(self, stage):
        path = self.path_for(stage)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def all_stages(self):
        return {stage: self.load(stage) for stage in STAGES
                if self.load(stage) is not None}

    def publish(self, stage, record):
        expected_index = STAGE_ORDER[stage]
        if expected_index > 0:
            predecessor = STAGES[expected_index - 1]
            prev_record = self.load(predecessor)
            if prev_record is None:
                raise InvalidStageError(
                    f"missing predecessor checkpoint: {predecessor}",
                    stage=stage,
                    context={"required_stage": predecessor})
        with file_lock(self.lock_path):
            existing = self.load(stage)
            if existing is not None:
                if existing["sequence"] > record["sequence"]:
                    raise StaleCheckpointError(
                        "late checkpoint rejected: higher sequence exists",
                        stage=stage,
                        context={"existing": existing["sequence"],
                                 "attempted": record["sequence"]})
                if existing["fingerprint"] == record["fingerprint"] and \
                        existing["sequence"] == record["sequence"]:
                    raise StaleCheckpointError(
                        "duplicate checkpoint rejected",
                        stage=stage,
                        context={"sequence": record["sequence"]})
                if existing["sequence"] == record["sequence"] and \
                        existing["fingerprint"] != record["fingerprint"]:
                    raise StaleCheckpointError(
                        "same-sequence cross-stage mismatch rejected",
                        stage=stage,
                        context={"sequence": record["sequence"]})
            atomic_write(self.path_for(stage), record)
        return record

    def reusable(self, stage, expected_fingerprint):
        record = self.load(stage)
        return record is not None and \
            record["fingerprint"] == expected_fingerprint

    def invalidate_from(self, stage):
        """Delete the given stage and every downstream checkpoint."""
        start = STAGE_ORDER[stage]
        for later in STAGES[start:]:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(self.path_for(later))

    def next_sequence(self):
        sequences = [record["sequence"]
                     for record in self.all_stages().values()]
        return (max(sequences) + 1) if sequences else 1


def build_record(stage, *, sequence, input_fingerprint, config_version,
                 artifact_summary):
    return {
        "stage": stage,
        "sequence": int(sequence),
        "input_fingerprint": input_fingerprint,
        "config_version": config_version,
        "artifact_summary": artifact_summary,
        "fingerprint": content_fingerprint({
            "stage": stage,
            "sequence": int(sequence),
            "input_fingerprint": input_fingerprint,
            "config_version": config_version,
            "artifact_summary": artifact_summary,
        }),
    }
