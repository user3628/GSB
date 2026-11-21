"""Atomic stage checkpoints, file locks and publish-time concurrency guards."""

import json
import os
import tempfile
import fcntl

from .errors import BusinessError, E_STALE_CHECKPOINT, E_INVALID_STAGE
from .hashing import canonical, digest

STAGE_ORDER = (
    "stage1_normalize",
    "stage2_metrics",
    "stage3_semantic",
    "stage4_fusion",
)


def atomic_write_json(path, payload):
    """Write JSON atomically; a crash leaves no half-written file."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(canonical(payload))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


class CheckpointStore:
    def __init__(self, root):
        self.root = root
        self.checkpoint_dir = os.path.join(root, "checkpoints")
        self.state_dir = os.path.join(root, "state")
        self.lock_path = os.path.join(self.state_dir, "publish.lock")
        self.sequence_path = os.path.join(self.state_dir, "sequence.json")
        self.stop_path = os.path.join(self.state_dir, "stop.flag")
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.state_dir, exist_ok=True)

    def path_for(self, image_id, stage):
        return os.path.join(self.checkpoint_dir, f"{image_id}__{stage}.json")

    def load(self, image_id, stage):
        path = self.path_for(image_id, stage)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.loads(handle.read())

    def all_records(self):
        records = []
        if not os.path.isdir(self.checkpoint_dir):
            return records
        for name in sorted(os.listdir(self.checkpoint_dir)):
            if name.endswith(".json"):
                with open(os.path.join(self.checkpoint_dir, name), "r", encoding="utf-8") as handle:
                    records.append(json.loads(handle.read()))
        return sorted(records, key=lambda item: (item["image_id"], STAGE_ORDER.index(item["stage"])))

    def _read_state(self, handle):
        handle.seek(0)
        raw = handle.read()
        if not raw:
            return {"next_sequence": 1, "published": {}}
        return json.loads(raw)

    def _write_state(self, handle, state):
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(state, sort_keys=True, ensure_ascii=False))
        handle.flush()
        os.fsync(handle.fileno())

    def publish(self, image_id, stage, record, expected_previous=None,
                force_sequence=None, allow_replace=False):
        """Publish a stage record under a real file lock.

        A missing predecessor raises E_INVALID_STAGE. Lagging, duplicate or
        cross-sample publishes raise E_STALE_CHECKPOINT. An upstream-driven
        rebuild passes ``allow_replace=True`` and keeps the original sequence.
        """
        stage_index = STAGE_ORDER.index(stage)
        if stage_index > 0:
            predecessor = STAGE_ORDER[stage_index - 1]
            previous_path = self.path_for(image_id, predecessor)
            if not os.path.exists(previous_path):
                raise BusinessError(
                    E_INVALID_STAGE,
                    f"missing predecessor {predecessor} for {image_id}",
                    {"image_id": image_id, "stage": stage, "predecessor": predecessor},
                )
            if expected_previous is not None:
                with open(previous_path, "r", encoding="utf-8") as handle:
                    stored = json.loads(handle.read())
                if stored.get("fingerprint") != expected_previous.get("fingerprint"):
                    raise BusinessError(
                        E_INVALID_STAGE,
                        "predecessor fingerprint does not match the declared input",
                        {"image_id": image_id, "stage": stage},
                    )

        os.makedirs(self.state_dir, exist_ok=True)
        with open(self.lock_path, "a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            state = self._read_state(lock_handle)
            key = f"{image_id}:{stage}"
            target_path = self.path_for(image_id, stage)
            if os.path.exists(target_path):
                with open(target_path, "r", encoding="utf-8") as existing_handle:
                    existing = json.loads(existing_handle.read())
                incoming_seq = force_sequence or existing["sequence"]
                if incoming_seq < existing["sequence"]:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "lagging checkpoint publish rejected",
                        {
                            "image_id": image_id,
                            "stage": stage,
                            "existing_sequence": existing["sequence"],
                            "incoming_sequence": incoming_seq,
                        },
                    )
                same_content = digest(
                    {key: value for key, value in existing.items() if key != "sequence"}
                ) == digest(record)
                if same_content and not allow_replace:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "duplicate checkpoint publish rejected",
                        {"image_id": image_id, "stage": stage, "sequence": incoming_seq},
                    )
                if not allow_replace and incoming_seq <= existing["sequence"]:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "repeated or lagging checkpoint publish rejected",
                        {
                            "image_id": image_id,
                            "stage": stage,
                            "existing_sequence": existing["sequence"],
                            "incoming_sequence": incoming_seq,
                        },
                    )
                if incoming_seq > state["next_sequence"]:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "cross-sample or out-of-order checkpoint publish rejected",
                        {
                            "image_id": image_id,
                            "stage": stage,
                            "incoming_sequence": incoming_seq,
                            "next_sequence": state["next_sequence"],
                        },
                    )
                sequence = existing["sequence"] if allow_replace else incoming_seq
            else:
                if key in state["published"]:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "record missing on disk but already marked published",
                        {"image_id": image_id, "stage": stage},
                    )
                sequence = force_sequence or state["next_sequence"]
                if sequence != state["next_sequence"]:
                    raise BusinessError(
                        E_STALE_CHECKPOINT,
                        "non-monotonic checkpoint sequence rejected",
                        {
                            "image_id": image_id,
                            "stage": stage,
                            "incoming_sequence": sequence,
                            "next_sequence": state["next_sequence"],
                        },
                    )
                state["next_sequence"] = sequence + 1
                state["published"][key] = sequence

            payload = dict(record)
            payload["sequence"] = sequence
            atomic_write_json(target_path, payload)
            self._write_state(lock_handle, state)
            return sequence

    def request_stop(self):
        with open(self.stop_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("stop\n")
            handle.flush()
            os.fsync(handle.fileno())

    def stop_requested(self):
        return os.path.exists(self.stop_path)

    def clear_stop(self):
        if os.path.exists(self.stop_path):
            os.unlink(self.stop_path)
