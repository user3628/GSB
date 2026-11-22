"""Stage checkpoint persistence, real file locks and cooperative stop handling."""
from __future__ import annotations

import os
import time

from .codec import digest, read_json, write_json
from .config import STAGE_ORDER
from .errors import InvalidStageError, StaleCheckpointError

try:
    import fcntl  # POSIX
except ImportError:  # pragma: no cover - Windows fallback
    import msvcrt

    class _FcntlShim:
        LOCK_EX = 2
        LOCK_UN = 8
        LOCK_NB = 4

        @staticmethod
        def flock(handle, operation):
            if operation & _FcntlShim.LOCK_UN:
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            elif operation & _FcntlShim.LOCK_NB:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)

    fcntl = _FcntlShim()


class FileLock:
    """Cross-process advisory lock backed by an OS file lock."""

    def __init__(self, directory, name="pipeline.lock", timeout: float = 30.0):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, name)
        self.timeout = timeout
        self._handle = None

    def __enter__(self):
        self._handle = open(self.path, "a+")
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def __exit__(self, exc_type, exc, traceback):
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


class CheckpointStore:
    """Manages atomic stage products, monotonic sequence numbers and stop markers."""

    def __init__(self, directory):
        self.directory = os.path.abspath(directory)
        os.makedirs(os.path.join(self.directory, "stages"), exist_ok=True)
        os.makedirs(os.path.join(self.directory, "checkpoints"), exist_ok=True)
        self.lock_path = os.path.join(self.directory, "pipeline.lock")
        self.stop_path = os.path.join(self.directory, "STOP")
        self.index_path = os.path.join(self.directory, "checkpoints", "index.json")

    # ---------- stop handling ----------
    def request_stop(self) -> None:
        write_json(self.stop_path, {"stop": True})

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_path):
            os.unlink(self.stop_path)

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_path)

    def check_stop(self, ref_id=None) -> None:
        from .errors import StopRequested

        if self.stop_requested():
            raise StopRequested(at_ref=ref_id)

    # ---------- index helpers ----------
    def _read_index(self):
        if not os.path.exists(self.index_path):
            return {"next_seq": 1, "records": []}
        return read_json(self.index_path)

    def _write_index(self, index):
        write_json(self.index_path, index)

    def stage_product_path(self, stage: str) -> str:
        return os.path.join(self.directory, "stages", "%s.json" % stage)

    def stage_record_path(self, stage: str) -> str:
        return os.path.join(self.directory, "checkpoints", "%s.record.json" % stage)

    def has_stage(self, stage: str) -> bool:
        return os.path.exists(self.stage_product_path(stage))

    def get_stage(self, stage: str):
        return read_json(self.stage_product_path(stage))

    def latest_seq(self) -> int:
        return self._read_index()["next_seq"] - 1

    # ---------- publishing ----------
    def publish(self, stage: str, product: dict, input_digest: str, config_digest: str,
                fingerprint: str, proposed_seq: int | None = None) -> dict:
        """Atomically persist a stage product plus its checkpoint record under a file lock."""
        if stage not in STAGE_ORDER:
            raise InvalidStageError("unknown stage %s" % stage, {"stage": stage})
        stage_index = STAGE_ORDER.index(stage)
        with FileLock(self.directory):
            index = self._read_index()
            expected_seq = index["next_seq"]
            if proposed_seq is not None and proposed_seq != expected_seq:
                raise StaleCheckpointError(
                    "checkpoint sequence %s is not the expected successor %d"
                    % (proposed_seq, expected_seq),
                    {"stage": stage, "proposed": proposed_seq, "expected": expected_seq},
                )
            if stage_index > 0 and not self.has_stage(STAGE_ORDER[stage_index - 1]):
                raise InvalidStageError(
                    "predecessor stage %s is missing" % STAGE_ORDER[stage_index - 1],
                    {"stage": stage, "missing_predecessor": STAGE_ORDER[stage_index - 1]},
                )
            product_digest = digest(product)
            record = {
                "seq": expected_seq,
                "stage": stage,
                "input_digest": input_digest,
                "config_digest": config_digest,
                "product_digest": product_digest,
                "fingerprint": fingerprint,
            }
            write_json(self.stage_product_path(stage), product)
            write_json(self.stage_record_path(stage), record)
            index["records"].append(record)
            index["next_seq"] = expected_seq + 1
            self._write_index(index)
            return record

    def truncate_from(self, stage: str) -> list:
        """Invalidate a stage and every downstream stage, preserving upstream reuse."""
        if stage not in STAGE_ORDER:
            raise InvalidStageError("unknown stage %s" % stage, {"stage": stage})
        removed = []
        with FileLock(self.directory):
            index = self._read_index()
            cut = STAGE_ORDER.index(stage)
            keep_records = []
            for record in index["records"]:
                if STAGE_ORDER.index(record["stage"]) >= cut:
                    removed.append(record["stage"])
                    for path in (self.stage_product_path(record["stage"]),
                                 self.stage_record_path(record["stage"])):
                        if os.path.exists(path):
                            os.unlink(path)
                else:
                    keep_records.append(record)
            index["records"] = keep_records
            index["next_seq"] = len(keep_records) + 1
            self._write_index(index)
        return removed

    def records(self):
        return self._read_index()["records"]

    def fingerprints(self) -> dict:
        return {record["stage"]: record["fingerprint"] for record in self.records()}
