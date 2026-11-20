"""Workspace storage: import validation, atomic records, locks, snapshots."""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from .audio import SAMPLE_RATE, file_fingerprint, read_wav
from .errors import (EImportValidation, EInvalidStage, EStaleSnapshot,
                     EStaleVersion, WorkbenchError)
from .timeline import Segment, Timeline, timeline_from_dict

SNAPSHOT_REASON_RUN = "run"
SNAPSHOT_REASON_EDIT = "edit"
SNAPSHOT_REASON_CONFIG = "config"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(data: Any) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def digest_json(data: Any) -> str:
    return hashlib.sha256(canonical_json(data)).hexdigest()


class FileLock:
    """Portable cross-process lock (msvcrt on Windows, fcntl elsewhere)."""

    def __init__(self, path: str):
        self.path = path
        self._fh = None
        self._kind = None

    def acquire(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "a+b")
        try:
            import fcntl  # POSIX
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
            self._kind = "fcntl"
        except ImportError:  # pragma: no cover - Windows path
            import msvcrt
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
            self._kind = "msvcrt"

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if self._kind == "fcntl":
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            else:  # pragma: no cover - Windows path
                import msvcrt
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def atomic_write_bytes(path: str, payload: bytes) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise


def atomic_write_json(path: str, data: Any) -> None:
    atomic_write_bytes(path, canonical_json(data))


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


@dataclass
class ShardInfo:
    shard_id: str
    order: int
    wav_path: str
    n_samples: int
    duration_ms: int
    n_speakers: int
    content_fingerprint: str
    segments: List[Dict[str, Any]]
    overlap_ms: List[List[int]]
    raw: Dict[str, Any]


class Workspace:
    """On-disk layout and all persistence concerns for one workbench."""

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.shards_dir = os.path.join(self.root, "shards")
        self.artifacts_dir = os.path.join(self.root, "artifacts")
        self.timelines_dir = os.path.join(self.root, "timelines")
        self.snapshots_dir = os.path.join(self.root, "snapshots")
        self.exports_dir = os.path.join(self.root, "exports")
        self.meta_dir = os.path.join(self.root, "meta")
        for d in (self.shards_dir, self.artifacts_dir, self.timelines_dir,
                  self.snapshots_dir, self.exports_dir, self.meta_dir):
            os.makedirs(d, exist_ok=True)
        self.publish_lock = FileLock(os.path.join(self.meta_dir, "publish.lock"))
        self.run_lock = FileLock(os.path.join(self.meta_dir, "run.lock"))
        self.stop_file = os.path.join(self.meta_dir, "stop.flag")
        self.manifest_path = os.path.join(self.root, "manifest.json")
        self.state_path = os.path.join(self.meta_dir, "state.json")

    # ---------- import / validation ----------
    def import_manifest(self, manifest_path: Optional[str] = None
                        ) -> List[ShardInfo]:
        path = manifest_path or self.manifest_path
        manifest = read_json(path)
        shards = manifest.get("shards")
        if not isinstance(shards, list) or not shards:
            raise EImportValidation("manifest contains no shards")
        infos: List[ShardInfo] = []
        seen_ids = set()
        for entry in sorted(shards, key=lambda e: e["order"]):
            shard_id = entry["shard_id"]
            if shard_id in seen_ids:
                raise EImportValidation("duplicate shard id",
                                        {"shard_id": shard_id})
            seen_ids.add(shard_id)
            wav_path = os.path.join(self.root, entry["wav"])
            if not os.path.isfile(wav_path):
                raise EImportValidation("wav file missing",
                                        {"shard_id": shard_id, "path": entry["wav"]})
            samples = read_wav(wav_path)
            n_samples = len(samples)
            if n_samples != entry["n_samples"]:
                raise EImportValidation(
                    "sample count does not match manifest",
                    {"shard_id": shard_id, "wav_samples": n_samples,
                     "manifest_samples": entry["n_samples"]})
            duration_ms = n_samples * 1000 // SAMPLE_RATE
            if entry["sample_rate"] != SAMPLE_RATE:
                raise EImportValidation("unexpected sample rate",
                                        {"shard_id": shard_id})
            if entry["duration_ms"] > duration_ms:
                raise EImportValidation(
                    "declared duration exceeds PCM duration upper bound",
                    {"shard_id": shard_id,
                     "declared_ms": entry["duration_ms"],
                     "bound_ms": duration_ms})
            segs = entry.get("segments", [])
            for seg in segs:
                if seg["end_ms"] <= seg["start_ms"] or seg["start_ms"] < 0:
                    raise EImportValidation("illegal reference segment",
                                            {"segment": seg})
                if seg["end_ms"] > duration_ms:
                    raise EImportValidation(
                        "reference segment exceeds duration upper bound",
                        {"segment": seg, "bound_ms": duration_ms})
                if seg["confidence"] not in ("high", "low"):
                    raise EImportValidation("illegal confidence flag",
                                            {"segment": seg})
            fp = file_fingerprint(wav_path)
            infos.append(ShardInfo(
                shard_id=shard_id, order=entry["order"], wav_path=wav_path,
                n_samples=n_samples, duration_ms=duration_ms,
                n_speakers=entry["n_speakers"], content_fingerprint=fp,
                segments=segs, overlap_ms=entry.get("overlap_ms", []),
                raw=entry))
        # Orders must be a dense 0..n-1 sequence.
        orders = sorted(info.order for info in infos)
        if orders != list(range(len(infos))):
            raise EImportValidation("shard order must be dense starting at 0",
                                   {"orders": orders})
        self._write_manifest_meta(manifest, infos)
        self._bootstrap_timelines(infos)
        return infos

    def _write_manifest_meta(self, manifest: Dict[str, Any],
                             infos: List[ShardInfo]) -> None:
        record = {
            "manifest_digest": digest_json(manifest),
            "shards": [{
                "shard_id": info.shard_id,
                "order": info.order,
                "n_samples": info.n_samples,
                "duration_ms": info.duration_ms,
                "n_speakers": info.n_speakers,
                "content_fingerprint": info.content_fingerprint,
            } for info in infos],
        }
        atomic_write_json(self.state_path, record)

    def _bootstrap_timelines(self, infos: List[ShardInfo]) -> None:
        for info in infos:
            path = self.timeline_path(info.shard_id)
            if os.path.exists(path):
                continue
            segs = [Segment(s["seg_id"], s["start_ms"], s["end_ms"],
                            s["speaker"], s["confidence"],
                            bool(s.get("overlap", False)))
                    for s in info.segments]
            tl = Timeline(info.shard_id, info.duration_ms, segs, version=0)
            self.save_timeline(tl)

    def load_manifest(self) -> List[ShardInfo]:
        if not os.path.exists(self.state_path):
            raise EInvalidStage("workspace has no imported manifest")
        state = read_json(self.state_path)
        infos = []
        for entry in state["shards"]:
            raw = read_json(self.manifest_path)
            raw_entry = next(e for e in raw["shards"]
                             if e["shard_id"] == entry["shard_id"])
            infos.append(ShardInfo(
                shard_id=entry["shard_id"], order=entry["order"],
                wav_path=os.path.join(self.root, raw_entry["wav"]),
                n_samples=entry["n_samples"],
                duration_ms=entry["duration_ms"],
                n_speakers=entry["n_speakers"],
                content_fingerprint=entry["content_fingerprint"],
                segments=raw_entry["segments"],
                overlap_ms=raw_entry.get("overlap_ms", []), raw=raw_entry))
        return sorted(infos, key=lambda i: i.order)

    # ---------- timelines ----------
    def timeline_path(self, shard_id: str) -> str:
        return os.path.join(self.timelines_dir, f"{shard_id}.json")

    def save_timeline(self, timeline: Timeline) -> None:
        atomic_write_json(self.timeline_path(timeline.shard_id),
                          timeline.to_dict())

    def load_timeline(self, shard_id: str) -> Timeline:
        path = self.timeline_path(shard_id)
        if not os.path.exists(path):
            raise EInvalidStage(f"no timeline for shard {shard_id!r}",
                                {"shard_id": shard_id})
        return timeline_from_dict(read_json(path))

    # ---------- artifacts ----------
    def artifact_path(self, shard_id: str, stage: str, name: str) -> str:
        d = os.path.join(self.artifacts_dir, shard_id, stage)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, name)

    def stage_dir(self, shard_id: str, stage: str) -> str:
        d = os.path.join(self.artifacts_dir, shard_id, stage)
        os.makedirs(d, exist_ok=True)
        return d

    def has_stage(self, shard_id: str, stage: str) -> bool:
        return os.path.exists(os.path.join(
            self.artifacts_dir, shard_id, stage, "result.json"))

    def read_stage_result(self, shard_id: str, stage: str) -> Dict[str, Any]:
        path = os.path.join(self.artifacts_dir, shard_id, stage, "result.json")
        if not os.path.exists(path):
            raise EInvalidStage(f"stage {stage!r} missing for {shard_id!r}",
                                {"shard_id": shard_id, "stage": stage})
        return read_json(path)

    # ---------- snapshots ----------
    def snapshot_path(self, shard_id: str, stage: str) -> str:
        return os.path.join(self.snapshots_dir, f"{shard_id}__{stage}.json")

    def write_snapshot(self, shard_id: str, stage: str, record: Dict[str, Any]
                       ) -> None:
        record = dict(record)
        record["snapshot_digest"] = digest_json(
            {k: v for k, v in record.items() if k != "snapshot_digest"})
        atomic_write_json(self.snapshot_path(shard_id, stage), record)

    def read_snapshot(self, shard_id: str, stage: str) -> Optional[Dict[str, Any]]:
        path = self.snapshot_path(shard_id, stage)
        if not os.path.exists(path):
            return None
        return read_json(path)

    def iter_snapshots(self) -> Iterator[Dict[str, Any]]:
        if not os.path.isdir(self.snapshots_dir):
            return
        for name in sorted(os.listdir(self.snapshots_dir)):
            if name.endswith(".json"):
                yield read_json(os.path.join(self.snapshots_dir, name))

    def verify_snapshot(self, record: Dict[str, Any]) -> None:
        """Detect rollback/tamper against the artifact the snapshot describes."""
        expected_digest = record.get("snapshot_digest")
        without = {k: v for k, v in record.items() if k != "snapshot_digest"}
        if digest_json(without) != expected_digest:
            raise EStaleSnapshot("tamper", "snapshot digest mismatch",
                                 {"shard_id": record.get("shard_id"),
                                  "stage": record.get("stage")})
        result_path = os.path.join(
            self.artifacts_dir, record["shard_id"], record["stage"], "result.json")
        if not os.path.exists(result_path):
            raise EStaleSnapshot("tamper", "snapshot references missing artifact",
                                 {"shard_id": record["shard_id"],
                                  "stage": record["stage"]})
        result = read_json(result_path)
        if digest_json(result.get("summary", {})) != record["artifact_digest"]:
            raise EStaleSnapshot("tamper", "artifact summary does not match snapshot",
                                 {"shard_id": record["shard_id"],
                                  "stage": record["stage"]})
        if result.get("stage_version") != record["stage_version"]:
            kind = "rollback" if result["stage_version"] < record["stage_version"] else "tamper"
            raise EStaleSnapshot(kind, "stage version mismatch",
                                 {"shard_id": record["shard_id"],
                                  "stage": record["stage"]})

    # ---------- stop / high-water mark ----------
    def request_stop(self) -> None:
        atomic_write_bytes(self.stop_file, b"stop\n")

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_file):
            os.remove(self.stop_file)

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_file)

    def high_water_path(self) -> str:
        return os.path.join(self.meta_dir, "highwater.json")

    def write_high_water(self, records: List[Dict[str, Any]]) -> None:
        atomic_write_json(self.high_water_path(), records)

    def read_high_water(self) -> List[Dict[str, Any]]:
        path = self.high_water_path()
        return read_json(path) if os.path.exists(path) else []

    def read_stage_artifact_json(self, shard_id: str, stage: str,
                                 name: str) -> Any:
        path = os.path.join(self.artifacts_dir, shard_id, stage, name)
        if not os.path.exists(path):
            raise EInvalidStage(f"artifact {name!r} missing",
                                {"shard_id": shard_id, "stage": stage})
        return read_json(path)

    def draft_path(self, shard_id: str) -> str:
        return os.path.join(self.meta_dir, f"draft-{shard_id}.json")

    def save_draft(self, shard_id: str, timeline: Timeline) -> None:
        atomic_write_json(self.draft_path(shard_id), timeline.to_dict())

    def load_draft(self, shard_id: str) -> Timeline:
        path = self.draft_path(shard_id)
        if not os.path.exists(path):
            raise ENotFound("no unpublished draft", {"shard_id": shard_id})
        return timeline_from_dict(read_json(path))

    def discard_draft(self, shard_id: str) -> None:
        path = self.draft_path(shard_id)
        if os.path.exists(path):
            os.remove(path)
