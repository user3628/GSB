"""Checkpoint store with atomic records, file locks and sequence chains.

Every completed (unit, stage) publishes a checkpoint record containing:
unit (shard) id, stage name, input content fingerprint, structure version
fingerprint, config version, artifact digest/summary and a monotonic
per-unit sequence number plus a hash chain head.

Concurrency & integrity rules:

* publishes are serialized with an exclusive file lock;
* only contiguous sequence advancement is accepted (seq == last + 1);
* stale, duplicate or cross-shard publishes are rejected with
  ``E_STALE_CHECKPOINT``;
* an append-only journal is the source of truth; on load the journal is
  replayed and verified against checkpoint files and artifact digests -
  any wholesale replacement (sequence rollback or digest mismatch) is
  reported as ``E_STALE_CHECKPOINT`` with a diff, never silently ignored.
"""

import json
import os

from .errors import E_STALE_CHECKPOINT, EngineError
from .util import (atomic_write, canonical_json_bytes, file_lock,
                   sha256_file, sha256_hex)

STAGE_ORDER = ["validate", "project", "concat", "export"]
STAGE_INDEX = {name: i + 1 for i, name in enumerate(STAGE_ORDER)}


def genesis_head(unit):
    return sha256_hex(("genesis|" + unit).encode("utf-8"))


def chain_head(prev_head, record):
    material = (prev_head + "|" + record["fingerprint"] + "|"
                + record["artifact_digest"])
    return sha256_hex(material.encode("utf-8"))


def _stale(message, details):
    return EngineError(E_STALE_CHECKPOINT, message, details)


class CheckpointStore:
    def __init__(self, workspace_root):
        self.root = os.path.abspath(workspace_root)
        self.dir = os.path.join(self.root, "checkpoints")
        self.journal_path = os.path.join(self.dir, "journal.jsonl")
        self.lock_path = os.path.join(self.dir, ".lock")

    def record_path(self, unit, stage):
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in unit)
        return os.path.join(self.dir, "{}__{}.json".format(safe, stage))

    def _replay(self):
        """Replay the journal into {unit: {seq, head, stages}}."""
        units = {}
        invalidations = []
        if not os.path.exists(self.journal_path):
            return {"units": units, "invalidations": invalidations}
        with open(self.journal_path, "r", encoding="utf-8") as handle:
            lines = [ln for ln in handle.read().splitlines() if ln.strip()]
        for lineno, line in enumerate(lines, 1):
            entry = json.loads(line)
            kind = entry.get("type")
            if kind == "reset":
                units = {}
                continue
            if kind == "invalidate":
                invalidations.append(entry)
                self._apply_invalidate(units, entry, lineno)
                continue
            if kind != "publish":
                raise _stale("journal corrupted: unknown entry type",
                             {"line": lineno, "type": kind})
            record = entry["record"]
            unit = record["unit"]
            state = units.setdefault(
                unit, {"seq": 0, "head": genesis_head(unit), "stages": {}})
            if record["seq"] != state["seq"] + 1:
                raise _stale(
                    "journal sequence rollback or gap detected",
                    {"line": lineno, "unit": unit,
                     "expected_seq": state["seq"] + 1,
                     "found_seq": record["seq"]})
            if record["prev_head"] != state["head"]:
                raise _stale(
                    "journal hash chain mismatch",
                    {"line": lineno, "unit": unit,
                     "expected_prev_head": state["head"],
                     "found_prev_head": record["prev_head"]})
            if record["stage"] in state["stages"]:
                raise _stale(
                    "journal corrupted: duplicate stage publish",
                    {"line": lineno, "unit": unit, "stage": record["stage"]})
            state["seq"] = record["seq"]
            state["head"] = record["head"]
            state["stages"][record["stage"]] = record
        return {"units": units, "invalidations": invalidations}

    def _apply_invalidate(self, units, entry, lineno):
        per_unit = {}
        for unit, stage in entry["entries"]:
            per_unit.setdefault(unit, []).append(stage)
        for unit, stages in per_unit.items():
            state = units.get(unit)
            if state is None:
                raise _stale("journal invalidate references unknown unit",
                             {"line": lineno, "unit": unit})
            present = sorted(state["stages"], key=STAGE_INDEX.get)
            to_drop = sorted(stages, key=STAGE_INDEX.get)
            if not to_drop or present[-len(to_drop):] != to_drop:
                raise _stale(
                    "journal invalidate is not a contiguous suffix",
                    {"line": lineno, "unit": unit, "present": present,
                     "dropped": to_drop})
            for stage in to_drop:
                state["stages"].pop(stage, None)

    def load_verified(self):
        """Replay journal and verify on-disk records + artifact digests."""
        with file_lock(self.lock_path):
            state = self._replay()
            self._verify_files(state["units"])
            return state

    def _verify_files(self, units):
        expected = set()
        for unit, state in units.items():
            for stage, record in state["stages"].items():
                path = self.record_path(unit, stage)
                expected.add(os.path.abspath(path))
                if not os.path.exists(path):
                    raise _stale("checkpoint record file missing",
                                 {"unit": unit, "stage": stage})
                with open(path, "r", encoding="utf-8") as handle:
                    on_disk = json.load(handle)
                if on_disk != record:
                    raise _stale(
                        "checkpoint record mismatch (possible replacement)",
                        {"unit": unit, "stage": stage,
                         "diff": _diff_records(record, on_disk)})
                artifact = os.path.join(self.root, record["artifact_path"])
                if not os.path.exists(artifact):
                    raise _stale("checkpoint artifact missing",
                                 {"unit": unit, "stage": stage,
                                  "artifact_path": record["artifact_path"]})
                actual = sha256_file(artifact)
                if actual != record["artifact_digest"]:
                    raise _stale(
                        "checkpoint artifact digest mismatch",
                        {"unit": unit, "stage": stage,
                         "recorded_digest": record["artifact_digest"],
                         "actual_digest": actual})
        if os.path.isdir(self.dir):
            for name in os.listdir(self.dir):
                path = os.path.abspath(os.path.join(self.dir, name))
                if name in ("journal.jsonl", ".lock"):
                    continue
                if name.endswith(".json") and path not in expected:
                    raise _stale("unexpected checkpoint file",
                                 {"file": name})

    def publish(self, record):
        """Validate and publish a checkpoint record under the file lock."""
        with file_lock(self.lock_path):
            state = self._replay()["units"]
            unit_state = state.get(record["unit"]) or {
                "seq": 0, "head": genesis_head(record["unit"]),
                "stages": {}}
            expected_seq = unit_state["seq"] + 1
            if record.get("seq") is not None and record["seq"] != expected_seq:
                raise _stale(
                    "non-contiguous sequence (stale or duplicate publish)",
                    {"unit": record["unit"],
                     "expected_seq": expected_seq,
                     "found_seq": record["seq"]})
            if (record.get("prev_head") is not None
                    and record["prev_head"] != unit_state["head"]):
                raise _stale(
                    "hash chain mismatch (cross-shard or stale publish)",
                    {"unit": record["unit"],
                     "expected_prev_head": unit_state["head"],
                     "found_prev_head": record["prev_head"]})
            if record["stage"] in unit_state["stages"]:
                raise _stale(
                    "duplicate stage publish",
                    {"unit": record["unit"], "stage": record["stage"]})
            if unit_state["stages"]:
                max_index = max(STAGE_INDEX[s] for s in unit_state["stages"])
                if STAGE_INDEX[record["stage"]] != max_index + 1:
                    raise _stale(
                        "stage publish order violation within unit",
                        {"unit": record["unit"], "stage": record["stage"],
                         "published_stages": sorted(unit_state["stages"])})
            record = dict(record)
            record["seq"] = expected_seq
            record["prev_head"] = unit_state["head"]
            record["head"] = chain_head(unit_state["head"], record)
            atomic_write(self.record_path(record["unit"], record["stage"]),
                         canonical_json_bytes(record))
            self._append_journal({"type": "publish", "record": record})
            return record

    def invalidate(self, entries, reason, old_fingerprint, new_fingerprint):
        """Drop (unit, stage) records + artifacts; journal the invalidation."""
        entries = [list(e) for e in entries]
        with file_lock(self.lock_path):
            state = self._replay()["units"]
            for unit, stage in entries:
                unit_state = state.get(unit)
                if unit_state is None or stage not in unit_state["stages"]:
                    continue
                record = unit_state["stages"][stage]
                for path in (self.record_path(unit, stage),
                             os.path.join(self.root, record["artifact_path"])):
                    if os.path.exists(path):
                        os.unlink(path)
                unit_state["stages"].pop(stage)
            self._append_journal({
                "type": "invalidate",
                "entries": entries,
                "reason": reason,
                "old_fingerprint": old_fingerprint,
                "new_fingerprint": new_fingerprint,
            })

    def reset(self):
        with file_lock(self.lock_path):
            if os.path.isdir(self.dir):
                for name in os.listdir(self.dir):
                    if name == ".lock":
                        continue
                    path = os.path.join(self.dir, name)
                    if os.path.isfile(path):
                        os.unlink(path)
            os.makedirs(self.dir, exist_ok=True)
            self._append_journal({"type": "reset"})

    def _append_journal(self, entry):
        os.makedirs(self.dir, exist_ok=True)
        line = canonical_json_bytes(entry)
        with open(self.journal_path, "ab") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def invalidations(self):
        if not os.path.exists(self.journal_path):
            return []
        with file_lock(self.lock_path):
            return self._replay()["invalidations"]


def _diff_records(expected, actual):
    diff = {}
    for key in sorted(set(expected) | set(actual)):
        old = expected.get(key, "<missing>")
        new = actual.get(key, "<missing>")
        if old != new:
            diff[key] = {"journal": old, "on_disk": new}
    return diff
