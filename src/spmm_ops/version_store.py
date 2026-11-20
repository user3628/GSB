"""Atomic, concurrent-safe versioning for block plans and result caches."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import os
from pathlib import Path
import shutil
from typing import Any, Mapping

import numpy as np

from .errors import SpMMError, fail
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json,
    file_lock,
    json_digest,
    output_digest,
    read_json,
    sha256_hex,
)


GENESIS_DIGEST = "genesis"
STORE_FORMAT = "spmm-version-store"
STORE_FORMAT_VERSION = 1
ANCHOR_FORMAT = "spmm-version-anchor"


@dataclass(frozen=True)
class CacheLookup:
    hit: bool
    output: np.ndarray | None
    reason: str
    record_sequence: int | None = None
    metadata: dict[str, Any] | None = None


class VersionStore:
    """A linearithmic append-only history persisted as one atomic JSON file.

    Every record has a monotonic sequence and a digest chained to its
    predecessor. Expected sequence/digest checks make sequence rollback or a
    wholesale replacement observable to concurrent publishers.
    """

    def __init__(self, root: str | os.PathLike[str], *, lock_timeout: float = 30.0) -> None:
        self.root = Path(root)
        self.lock_path = self.root / ".lock"
        self.state_path = self.root / "state.json"
        self.anchor_path = self.root / "anchor.json"
        self.cache_dir = self.root / "cache"
        self.lock_timeout = float(lock_timeout)

    def _empty_state(self) -> dict[str, Any]:
        state: dict[str, Any] = {
            "format": STORE_FORMAT,
            "format_version": STORE_FORMAT_VERSION,
            "records": [],
            "active_scheme": None,
            "head": {"sequence": 0, "digest": GENESIS_DIGEST, "kind": "genesis"},
        }
        state["state_digest"] = self._state_digest(state)
        return state

    @staticmethod
    def _record_digest(record: Mapping[str, Any]) -> str:
        return json_digest(
            {
                "sequence": int(record["sequence"]),
                "kind": record["kind"],
                "prev_digest": record["prev_digest"],
                "payload": record["payload"],
            }
        )

    @staticmethod
    def _state_digest(state: Mapping[str, Any]) -> str:
        content = {key: value for key, value in state.items() if key != "state_digest"}
        return json_digest(content)

    def _stale(self, message: str, **details: Any) -> None:
        fail("E_STALE_PUBLISH", message, details=details, location={"kind": "version_store"})

    def _validate_state(self, state: Mapping[str, Any]) -> None:
        if state.get("format") != STORE_FORMAT or int(state.get("format_version", -1)) != STORE_FORMAT_VERSION:
            self._stale("version-store format is not recognized", state_format=state.get("format"))
        if state.get("state_digest") != self._state_digest(state):
            self._stale(
                "version-store state digest mismatch (possible wholesale replacement)",
                expected_digest=state.get("state_digest"),
                actual_digest=self._state_digest(state),
            )
        records = state.get("records")
        if not isinstance(records, list):
            self._stale("version-store records are malformed")
        previous = GENESIS_DIGEST
        for index, record in enumerate(records, start=1):
            if not isinstance(record, dict) or int(record.get("sequence", -1)) != index:
                self._stale(
                    "version sequence is not continuous from one",
                    expected_sequence=index,
                    actual_sequence=record.get("sequence") if isinstance(record, dict) else None,
                )
            if record.get("prev_digest") != previous:
                self._stale(
                    "version digest chain is broken",
                    sequence=index,
                    expected_prev_digest=previous,
                    actual_prev_digest=record.get("prev_digest"),
                )
            expected_record_digest = self._record_digest(record)
            if record.get("digest") != expected_record_digest:
                self._stale(
                    "version record digest mismatch",
                    sequence=index,
                    expected_digest=expected_record_digest,
                    actual_digest=record.get("digest"),
                )
            previous = str(record["digest"])
        head = state.get("head")
        if records:
            last = records[-1]
            expected_head = {
                "sequence": int(last["sequence"]),
                "digest": last["digest"],
                "kind": last["kind"],
            }
            if head != expected_head:
                self._stale(
                    "version head does not match the final record (possible rollback)",
                    expected_head=expected_head,
                    actual_head=head,
                )
        elif head != {"sequence": 0, "digest": GENESIS_DIGEST, "kind": "genesis"}:
            self._stale("empty version store has a non-genesis head", actual_head=head)

        active = state.get("active_scheme")
        if active is not None:
            sequence = int(active.get("sequence", -1))
            if not (1 <= sequence <= len(records)):
                self._stale("active scheme points outside the version history", active_scheme=active)
            record = records[sequence - 1]
            if record.get("kind") != "scheme" or record.get("digest") != active.get("digest"):
                self._stale("active scheme pointer is inconsistent", active_scheme=active)

    def _load_locked(self) -> dict[str, Any]:
        if not self.state_path.exists():
            if self.anchor_path.exists():
                self._stale(
                    "version-store state disappeared after a committed anchor",
                    kind="state_missing",
                )
            return self._empty_state()
        try:
            state = read_json(self.state_path)
        except SpMMError:
            raise
        except Exception as exc:
            self._stale("cannot read version-store state", cause=str(exc))
        self._validate_state(state)
        self._check_and_update_anchor_locked(state)
        return state

    def _write_locked(self, state: dict[str, Any]) -> None:
        state["state_digest"] = self._state_digest(state)
        atomic_write_json(self.state_path, state)
        self._write_anchor_locked(state)

    @staticmethod
    def _anchor_digest(anchor: Mapping[str, Any]) -> str:
        content = {key: value for key, value in anchor.items() if key != "anchor_digest"}
        return json_digest(content)

    def _read_anchor_locked(self) -> dict[str, Any] | None:
        if not self.anchor_path.exists():
            return None
        try:
            anchor = read_json(self.anchor_path)
        except Exception as exc:
            self._stale("cannot read the version anchor", cause=str(exc))
        if anchor.get("format") != ANCHOR_FORMAT or int(anchor.get("format_version", -1)) != 1:
            self._stale("version anchor format is not recognized")
        if anchor.get("anchor_digest") != self._anchor_digest(anchor):
            self._stale("version anchor digest mismatch")
        return anchor

    def _check_and_update_anchor_locked(self, state: Mapping[str, Any]) -> None:
        anchor = self._read_anchor_locked()
        if anchor is None:
            self._write_anchor_locked(state)
            return
        anchor_records = anchor.get("record_digests")
        state_records = [record["digest"] for record in state["records"]]
        if not isinstance(anchor_records, list):
            self._stale("version anchor record history is malformed")
        common = min(len(anchor_records), len(state_records))
        for index in range(common):
            if anchor_records[index] != state_records[index]:
                self._stale(
                    "version history diverges from the committed anchor",
                    sequence=index + 1,
                    anchor_digest=anchor_records[index],
                    state_digest=state_records[index],
                    relation="history_replacement",
                )
        anchor_head = anchor.get("head", {})
        state_head = state["head"]
        anchor_sequence = int(anchor_head.get("sequence", -1))
        state_sequence = int(state_head["sequence"])
        if state_sequence < anchor_sequence:
            self._stale(
                "version sequence moved backwards relative to the committed anchor",
                anchor_sequence=anchor_sequence,
                state_sequence=state_sequence,
                relation="sequence_regression",
            )
        if state_sequence == anchor_sequence:
            if state_head["digest"] != anchor_head.get("digest"):
                self._stale(
                    "version head digest changed at the same sequence",
                    sequence=state_sequence,
                    anchor_digest=anchor_head.get("digest"),
                    state_digest=state_head["digest"],
                    relation="head_replacement",
                )
            if state["state_digest"] != anchor.get("state_digest"):
                self._stale(
                    "version state digest changed at the same sequence",
                    sequence=state_sequence,
                    relation="state_replacement",
                )
        if state_sequence != anchor_sequence or state["state_digest"] != anchor.get("state_digest"):
            self._write_anchor_locked(state)

    def _write_anchor_locked(self, state: Mapping[str, Any]) -> None:
        anchor: dict[str, Any] = {
            "format": ANCHOR_FORMAT,
            "format_version": 1,
            "record_digests": [record["digest"] for record in state["records"]],
            "head": dict(state["head"]),
            "state_digest": state["state_digest"],
        }
        anchor["anchor_digest"] = self._anchor_digest(anchor)
        atomic_write_json(self.anchor_path, anchor)

    def _assert_expected(
        self,
        state: Mapping[str, Any],
        expected_sequence: int | None,
        expected_digest: str | None,
    ) -> None:
        if expected_sequence is None:
            return
        if int(expected_sequence) < 0:
            fail("E_USAGE", "expected_sequence must be nonnegative", details={"value": expected_sequence})
        head = state["head"]
        actual_sequence = int(head["sequence"])
        if actual_sequence != int(expected_sequence):
            self._stale(
                "publisher is behind or ahead of the current version sequence",
                expected_sequence=int(expected_sequence),
                actual_sequence=actual_sequence,
                relation=(
                    "sequence_regression"
                    if actual_sequence < int(expected_sequence)
                    else "stale_publisher"
                ),
            )
        if expected_digest is not None and head["digest"] != expected_digest:
            self._stale(
                "version head digest differs at the same sequence (possible wholesale replacement)",
                expected_sequence=int(expected_sequence),
                actual_sequence=actual_sequence,
                expected_digest=expected_digest,
                actual_digest=head["digest"],
            )

    def _append_record(
        self,
        state: dict[str, Any],
        *,
        kind: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        records = state["records"]
        sequence = len(records) + 1
        previous = records[-1]["digest"] if records else GENESIS_DIGEST
        record: dict[str, Any] = {
            "sequence": sequence,
            "kind": kind,
            "prev_digest": previous,
            "payload": dict(payload),
        }
        record["digest"] = self._record_digest(record)
        records.append(record)
        state["head"] = {"sequence": sequence, "digest": record["digest"], "kind": kind}
        return record

    @staticmethod
    def _record_summary(record: Mapping[str, Any]) -> dict[str, Any]:
        payload = record.get("payload", {})
        summary = {
            "sequence": int(record["sequence"]),
            "kind": record["kind"],
            "digest": record["digest"],
            "prev_digest": record["prev_digest"],
        }
        for key in ("scheme", "cache_key", "scheme_sequence", "scheme_digest", "output_digest"):
            if key in payload:
                summary[key] = payload[key]
        return summary

    def status(
        self,
        *,
        expected_sequence: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            self._assert_expected(state, expected_sequence, expected_digest)
            records = state["records"]
            cache_files = 0
            cache_bytes = 0
            if self.cache_dir.exists():
                for path in self.cache_dir.glob("*.npy"):
                    cache_files += 1
                    cache_bytes += path.stat().st_size
            return {
                "format": STORE_FORMAT,
                "format_version": STORE_FORMAT_VERSION,
                "sequence": int(state["head"]["sequence"]),
                "digest": state["head"]["digest"],
                "kind": state["head"]["kind"],
                "active_scheme": state.get("active_scheme"),
                "state_digest": state["state_digest"],
                "record_count": len(records),
                "scheme_version_count": sum(1 for record in records if record["kind"] == "scheme"),
                "cache_version_count": sum(1 for record in records if record["kind"] == "cache"),
                "records": [self._record_summary(record) for record in records],
                "cache_file_count": cache_files,
                "cache_bytes": cache_bytes,
            }

    def publish_scheme(
        self,
        payload: Mapping[str, Any],
        *,
        expected_sequence: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            self._assert_expected(state, expected_sequence, expected_digest)
            record = self._append_record(state, kind="scheme", payload=payload)
            state["active_scheme"] = {"sequence": record["sequence"], "digest": record["digest"]}
            self._write_locked(state)
            return self._record_summary(record)

    def ensure_scheme(
        self,
        payload: Mapping[str, Any],
        *,
        expected_sequence: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        """Publish a scheme once; an identical active scheme is idempotent."""

        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            self._assert_expected(state, expected_sequence, expected_digest)
            active = state.get("active_scheme")
            if active is not None:
                active_record = state["records"][int(active["sequence"]) - 1]
                if canonical_json(active_record["payload"]) == canonical_json(dict(payload)):
                    return self._record_summary(active_record)
            record = self._append_record(state, kind="scheme", payload=payload)
            state["active_scheme"] = {"sequence": record["sequence"], "digest": record["digest"]}
            self._write_locked(state)
            return self._record_summary(record)

    def _cache_path(self, cache_key: str) -> Path:
        if len(cache_key) != 64 or any(character not in "0123456789abcdef" for character in cache_key):
            fail("E_USAGE", "cache_key must be a lowercase SHA-256 digest", details={"cache_key": cache_key})
        return self.cache_dir / f"{cache_key}.npy"

    def _encode_npy(self, output: np.ndarray) -> bytes:
        buffer = BytesIO()
        np.save(buffer, output, allow_pickle=False)
        return buffer.getvalue()

    @staticmethod
    def _decode_npy(data: bytes) -> np.ndarray:
        return np.load(BytesIO(data), allow_pickle=False)

    def _find_active_cache_record(
        self,
        state: Mapping[str, Any],
        cache_key: str,
        scheme_sequence: int,
        scheme_digest: str,
    ) -> dict[str, Any] | None:
        for record in reversed(state["records"]):
            payload = record.get("payload", {})
            if (
                record.get("kind") == "cache"
                and payload.get("cache_key") == cache_key
                and int(payload.get("scheme_sequence", -1)) == int(scheme_sequence)
                and payload.get("scheme_digest") == scheme_digest
            ):
                return record
        return None

    def lookup_cache(
        self,
        cache_key: str,
        *,
        scheme_sequence: int,
        scheme_digest: str,
    ) -> CacheLookup:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            active = state.get("active_scheme")
            if active is None or int(active["sequence"]) != int(scheme_sequence) or active["digest"] != scheme_digest:
                return CacheLookup(False, None, "active_scheme_changed")
            record = self._find_active_cache_record(state, cache_key, scheme_sequence, scheme_digest)
            if record is None:
                return CacheLookup(False, None, "cache_version_not_present")
            path = self._cache_path(cache_key)
            if not path.exists():
                return CacheLookup(False, None, "cache_file_missing", int(record["sequence"]))
            try:
                output = self._decode_npy(path.read_bytes())
            except (OSError, ValueError) as exc:
                return CacheLookup(False, None, f"cache_file_unreadable:{exc}", int(record["sequence"]))
            payload = record["payload"]
            if tuple(output.shape) != tuple(payload.get("shape", [])) or str(output.dtype) != payload.get("dtype"):
                return CacheLookup(False, None, "cache_metadata_mismatch", int(record["sequence"]))
            if output_digest(output) != payload.get("output_digest"):
                return CacheLookup(False, None, "cache_digest_mismatch", int(record["sequence"]))
            return CacheLookup(True, output, "hit", int(record["sequence"]), dict(record["payload"]))

    def publish_cache(
        self,
        cache_key: str,
        output: np.ndarray,
        payload: Mapping[str, Any],
        *,
        expected_sequence: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            self._assert_expected(state, expected_sequence, expected_digest)
            active = state.get("active_scheme")
            if active is None:
                self._stale("cannot publish a cache result without an active scheme")
            scheme_sequence = int(payload.get("scheme_sequence", -1))
            scheme_digest = payload.get("scheme_digest")
            if scheme_sequence != int(active["sequence"]) or scheme_digest != active["digest"]:
                self._stale(
                    "cache result belongs to a stale or different block scheme",
                    payload_scheme_sequence=scheme_sequence,
                    payload_scheme_digest=scheme_digest,
                    active_scheme_sequence=int(active["sequence"]),
                    active_scheme_digest=active["digest"],
                )

            existing = self._find_active_cache_record(state, cache_key, scheme_sequence, str(scheme_digest))
            if existing is not None:
                path = self._cache_path(cache_key)
                if path.exists():
                    try:
                        cached = self._decode_npy(path.read_bytes())
                        if output_digest(cached) == existing["payload"].get("output_digest"):
                            return self._record_summary(existing)
                    except (OSError, ValueError):
                        pass

            digest = output_digest(output)
            record_payload = dict(payload)
            record_payload.update(
                {
                    "cache_key": cache_key,
                    "scheme_sequence": scheme_sequence,
                    "scheme_digest": scheme_digest,
                    "output_digest": digest,
                    "shape": [int(x) for x in output.shape],
                    "dtype": str(output.dtype),
                }
            )
            path = self._cache_path(cache_key)
            atomic_write_bytes(path, self._encode_npy(output))
            record = self._append_record(state, kind="cache", payload=record_payload)
            self._write_locked(state)
            return self._record_summary(record)

    def clear_cache(self) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            self._load_locked()
            removed = 0
            removed_bytes = 0
            if self.cache_dir.exists():
                for path in sorted(self.cache_dir.glob("*.npy")):
                    removed += 1
                    removed_bytes += path.stat().st_size
                    path.unlink()
            return {"removed_files": removed, "removed_bytes": removed_bytes}

    def list_cache(self) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            active = state.get("active_scheme")
            files: list[dict[str, Any]] = []
            if self.cache_dir.exists():
                for path in sorted(self.cache_dir.glob("*.npy")):
                    files.append(
                        {
                            "cache_key": path.stem,
                            "bytes": path.stat().st_size,
                            "sha256_of_npy_file": sha256_hex(path.read_bytes()),
                        }
                    )
            return {
                "active_scheme": active,
                "files": files,
                "file_count": len(files),
                "total_bytes": sum(item["bytes"] for item in files),
            }

    def prune_cache(self) -> dict[str, Any]:
        with file_lock(self.lock_path, timeout=self.lock_timeout):
            state = self._load_locked()
            active = state.get("active_scheme")
            keep: set[str] = set()
            if active is not None:
                for record in state["records"]:
                    payload = record.get("payload", {})
                    if (
                        record.get("kind") == "cache"
                        and int(payload.get("scheme_sequence", -1)) == int(active["sequence"])
                        and payload.get("scheme_digest") == active["digest"]
                        and isinstance(payload.get("cache_key"), str)
                    ):
                        keep.add(payload["cache_key"])
            removed = 0
            removed_bytes = 0
            if self.cache_dir.exists():
                for path in sorted(self.cache_dir.glob("*.npy")):
                    if path.stem not in keep:
                        removed += 1
                        removed_bytes += path.stat().st_size
                        path.unlink()
            return {"kept_cache_keys": sorted(keep), "removed_files": removed, "removed_bytes": removed_bytes}

