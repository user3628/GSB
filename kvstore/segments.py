from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable

from .errors import StoreError, record_corrupt
from .formats import decode_binary, decode_binary_record_at, encode_binary
from .model import DecodedRecord, Record, SegmentInfo


_TXN_RE = re.compile(r"^(seg|compact)-(\d{12})-([A-Za-z0-9_.-]+)$")
_SNAP_RE = re.compile(r"^snap-(\d{12})-(\d{12})-([A-Za-z0-9_.-]+)$")


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


class SegmentManager:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._cache: dict[tuple[str, str], bytes] = {}
        self._cache_lock = threading.RLock()

    def allocate(
        self,
        *,
        seq: int,
        kind: str,
        txn_id: str,
        snapshot_seq: int | None = None,
    ) -> tuple[str, Path, Path]:
        safe_txn = re.sub(r"[^A-Za-z0-9_.-]", "_", txn_id)[:64]
        if kind == "snapshot":
            if snapshot_seq is None:
                raise ValueError("snapshot_seq is required for snapshot segments")
            stem = f"snap-{snapshot_seq:012d}-{seq:012d}-{safe_txn}"
        elif kind == "compact":
            stem = f"compact-{seq:012d}-{safe_txn}"
        elif kind == "txn":
            stem = f"seg-{seq:012d}-{safe_txn}"
        else:
            raise ValueError(f"unknown segment kind: {kind}")
        path = self.root / f"{stem}.kvs"
        return stem, path, self.meta_path(path)

    @staticmethod
    def meta_path(path: Path) -> Path:
        return path.with_name(path.name + ".meta.json")

    def write_segment(
        self,
        *,
        segment_id: str,
        path: Path,
        seq: int,
        kind: str,
        records: Iterable[Record],
        created_at_ms: int,
        batch_id: str | None = None,
        snapshot_seq: int | None = None,
    ) -> tuple[SegmentInfo, list[DecodedRecord]]:
        record_list = list(records)
        blob = encode_binary(record_list, endian="little")
        # Decode once before publishing so offsets and decoded content are known
        # to belong to exactly the bytes that are about to be fsynced.
        decoded = decode_binary(blob)
        if len(decoded) != len(record_list):
            raise StoreError(
                "E_INTERNAL",
                "internal segment encode/decode count mismatch",
                {"expected": len(record_list), "actual": len(decoded)},
            )
        digest = hashlib.sha256(blob).hexdigest()
        meta = {
            "segment_id": segment_id,
            "seq": seq,
            "kind": kind,
            "sha256": digest,
            "record_count": len(record_list),
            "created_at_ms": created_at_ms,
            "batch_id": batch_id,
            "snapshot_seq": snapshot_seq,
            "binary_format_version": 1,
            "endianness": "little",
        }
        meta_bytes = json.dumps(
            meta, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        tmp_seg = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
        tmp_meta = path.with_name(f".{path.name}.meta.tmp-{uuid.uuid4().hex}")
        try:
            with open(tmp_seg, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            with open(tmp_meta, "wb") as handle:
                handle.write(meta_bytes)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_seg, path)
            os.replace(tmp_meta, self.meta_path(path))
            dir_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            tmp_seg.unlink(missing_ok=True)
            tmp_meta.unlink(missing_ok=True)
        with self._cache_lock:
            self._cache[(segment_id, digest)] = blob
        return (
            SegmentInfo(
                segment_id=segment_id,
                path=str(path),
                meta_path=str(self.meta_path(path)),
                seq=seq,
                kind=kind,
                sha256=digest,
                record_count=len(record_list),
                created_at_ms=created_at_ms,
                batch_id=batch_id,
                snapshot_seq=snapshot_seq,
            ),
            decoded,
        )

    def scan(self, *, recover_missing_meta: bool = True) -> tuple[list[SegmentInfo], list[dict[str, Any]]]:
        issues: list[dict[str, Any]] = []
        # Temps are never a complete transaction boundary. They are reported
        # and removed; a matching intent is rolled back by Store startup.
        for temp in self.root.glob(".*.tmp-*"):
            if temp.is_file():
                issues.append(
                    {
                        "kind": "partial_temp_removed",
                        "path": str(temp),
                        "bytes": temp.stat().st_size,
                    }
                )
                temp.unlink(missing_ok=True)

        infos: list[SegmentInfo] = []
        for path in sorted(self.root.glob("*.kvs")):
            parsed = self._parse_name(path.stem)
            if parsed is None:
                issues.append(
                    {
                        "kind": "unrecognized_segment_filename",
                        "path": str(path),
                    }
                )
                continue
            kind, seq, snapshot_seq = parsed
            try:
                blob = path.read_bytes()
                actual_sha = hashlib.sha256(blob).hexdigest()
                meta_path = self.meta_path(path)
                meta: dict[str, Any] | None = None
                if meta_path.exists():
                    try:
                        meta = json.loads(meta_path.read_text("utf-8"))
                    except (json.JSONDecodeError, OSError) as exc:
                        issues.append(
                            {
                                "kind": "segment_meta_invalid",
                                "path": str(path),
                                "meta_path": str(meta_path),
                                "error": str(exc),
                            }
                        )
                        continue
                    expected_sha = meta.get("sha256")
                    if expected_sha != actual_sha:
                        issues.append(
                            {
                                "kind": "segment_digest_mismatch",
                                "path": str(path),
                                "expected_sha256": expected_sha,
                                "actual_sha256": actual_sha,
                            }
                        )
                        continue
                decoded = decode_binary(blob)
                if meta is None:
                    if not recover_missing_meta:
                        issues.append(
                            {
                                "kind": "segment_meta_missing",
                                "path": str(path),
                            }
                        )
                        continue
                    meta = {
                        "segment_id": path.stem,
                        "seq": seq,
                        "kind": kind,
                        "sha256": actual_sha,
                        "record_count": len(decoded),
                        "created_at_ms": int(path.stat().st_mtime * 1000),
                        "batch_id": None,
                        "snapshot_seq": snapshot_seq,
                    }
                    meta_bytes = json.dumps(
                        meta, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                    ).encode("utf-8")
                    _atomic_write(meta_path, meta_bytes)
                    issues.append(
                        {
                            "kind": "segment_meta_recovered",
                            "path": str(path),
                            "sha256": actual_sha,
                        }
                    )
                if int(meta.get("record_count", -1)) != len(decoded):
                    issues.append(
                        {
                            "kind": "segment_record_count_mismatch",
                            "path": str(path),
                            "meta_count": meta.get("record_count"),
                            "actual_count": len(decoded),
                        }
                    )
                    continue
                if int(meta.get("seq", seq)) != seq:
                    issues.append(
                        {
                            "kind": "segment_seq_mismatch",
                            "path": str(path),
                            "filename_seq": seq,
                            "meta_seq": meta.get("seq"),
                        }
                    )
                    continue
                info = SegmentInfo(
                    segment_id=str(meta.get("segment_id", path.stem)),
                    path=str(path),
                    meta_path=str(meta_path),
                    seq=seq,
                    kind=str(meta.get("kind", kind)),
                    sha256=actual_sha,
                    record_count=len(decoded),
                    created_at_ms=int(meta.get("created_at_ms", 0)),
                    batch_id=meta.get("batch_id"),
                    snapshot_seq=(
                        int(meta["snapshot_seq"])
                        if meta.get("snapshot_seq") is not None
                        else snapshot_seq
                    ),
                )
                infos.append(info)
                with self._cache_lock:
                    self._cache[(info.segment_id, info.sha256)] = blob
            except StoreError as exc:
                issues.append(
                    {
                        "kind": "segment_invalid",
                        "path": str(path),
                        "code": exc.code,
                        "error": exc.message,
                        "context": exc.context,
                    }
                )
            except OSError as exc:
                issues.append(
                    {"kind": "segment_read_failed", "path": str(path), "error": str(exc)}
                )
        infos.sort(key=lambda info: info.seq)
        seqs: set[int] = set()
        for info in infos:
            if info.seq in seqs:
                issues.append(
                    {
                        "kind": "duplicate_segment_sequence",
                        "seq": info.seq,
                        "segment_id": info.segment_id,
                    }
                )
            seqs.add(info.seq)
        return infos, issues

    def _parse_name(self, stem: str) -> tuple[str, int, int | None] | None:
        snap = _SNAP_RE.match(stem)
        if snap:
            return "snapshot", int(snap.group(2)), int(snap.group(1))
        txn = _TXN_RE.match(stem)
        if txn:
            return txn.group(1), int(txn.group(2)), None
        return None

    def read_segment(self, info: SegmentInfo, *, verify_digest: bool = True) -> list[DecodedRecord]:
        if verify_digest:
            blob = Path(info.path).read_bytes()
            actual = hashlib.sha256(blob).hexdigest()
            if actual != info.sha256:
                raise StoreError(
                    "E_SEGMENT_INVALID",
                    "segment digest changed after it was loaded",
                    {
                        "segment_id": info.segment_id,
                        "expected_sha256": info.sha256,
                        "actual_sha256": actual,
                    },
                )
            with self._cache_lock:
                self._cache[(info.segment_id, info.sha256)] = blob
        else:
            blob = self._cached_blob(info)
        return decode_binary(blob)

    def _cached_blob(self, info: SegmentInfo) -> bytes:
        key = (info.segment_id, info.sha256)
        with self._cache_lock:
            cached = self._cache.get(key)
        if cached is not None:
            return cached
        blob = Path(info.path).read_bytes()
        with self._cache_lock:
            self._cache[key] = blob
        return blob

    def read_record(
        self,
        info: SegmentInfo,
        *,
        offset: int,
        length: int,
        expected_version: int,
        key: str,
    ) -> Record:
        try:
            blob = self._cached_blob(info)
        except OSError as exc:
            raise StoreError(
                "E_SEGMENT_INVALID",
                "segment file cannot be read",
                {"segment_id": info.segment_id, "path": info.path, "error": str(exc)},
            ) from exc
        decoded = decode_binary_record_at(
            blob, offset, expected_length=length, record_index=0
        )
        record = decoded.record
        if record.version != expected_version or record.key != key:
            raise record_corrupt(
                "segment record does not match the current key/version index",
                record_index=0,
                byte_offset=offset,
                format_name="binary",
                segment_id=info.segment_id,
                expected_key=key,
                actual_key=record.key,
                expected_version=expected_version,
                actual_version=record.version,
            )
        return record

    def verify(self, info: SegmentInfo) -> tuple[bool, dict[str, Any]]:
        path = Path(info.path)
        try:
            blob = path.read_bytes()
        except OSError as exc:
            return False, {
                "kind": "segment_missing",
                "segment_id": info.segment_id,
                "path": str(path),
                "error": str(exc),
            }
        actual = hashlib.sha256(blob).hexdigest()
        if actual != info.sha256:
            return False, {
                "kind": "segment_digest_mismatch",
                "segment_id": info.segment_id,
                "path": str(path),
                "expected_sha256": info.sha256,
                "actual_sha256": actual,
            }
        try:
            meta = json.loads(Path(info.meta_path).read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return False, {
                "kind": "segment_meta_invalid",
                "segment_id": info.segment_id,
                "meta_path": info.meta_path,
                "error": str(exc),
            }
        if meta.get("sha256") != actual:
            return False, {
                "kind": "segment_summary_mismatch",
                "segment_id": info.segment_id,
                "summary_sha256": meta.get("sha256"),
                "actual_sha256": actual,
            }
        try:
            decoded = decode_binary(blob)
        except StoreError as exc:
            return False, {
                "kind": "segment_record_invalid",
                "segment_id": info.segment_id,
                "code": exc.code,
                "error": exc.message,
                "context": exc.context,
            }
        if len(decoded) != info.record_count:
            return False, {
                "kind": "segment_record_count_mismatch",
                "segment_id": info.segment_id,
                "expected": info.record_count,
                "actual": len(decoded),
            }
        return True, {"kind": "ok", "segment_id": info.segment_id, "sha256": actual}

