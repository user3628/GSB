from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .atomic import atomic_write_bytes
from .audio import (
    StubTranscriptionAdapter,
    TranscriptionAdapter,
    file_sha256,
    load_manifest,
)
from .config import (
    CURRENT_SCHEMA_VERSION,
    WorkbenchConfig,
    canonical_json_bytes,
    digest_value,
)
from .errors import (
    WorkbenchError,
    incompat_format,
    stale_snapshot,
    stale_version,
    timeline_error,
)
from .formats import DecodeResult, decode_json, decode_text, encode_text, encode_json
from .locking import FileLock
from .models import (
    ChunkInfo,
    Word,
    WorkspaceState,
    chunks_as_dict,
    sorted_words,
    timeline_digest,
    timeline_regions,
    validate_timeline,
    words_as_list,
)


def compute_input_fingerprint(chunks: Mapping[str, ChunkInfo]) -> str:
    return digest_value(
        {
            chunk_id: {
                "fingerprint": chunk.fingerprint,
                "duration_ms": chunk.duration_ms,
                "order": chunk.order,
                "language": chunk.language,
            }
            for chunk_id, chunk in sorted(
                chunks.items(), key=lambda item: (item[1].order, item[0])
            )
        }
    )


def _word_order(words: Iterable[Word]) -> list[Word]:
    return sorted(
        words,
        key=lambda w: (w.chunk_id, w.start_ms, w.end_ms, w.entry_id),
    )


def changes_between(old_words: Sequence[Word], new_words: Sequence[Word]) -> dict[str, Any]:
    old = {word.entry_id: word for word in old_words}
    new = {word.entry_id: word for word in new_words}
    added = [_word_order([new[key] for key in new.keys() - old.keys()])]
    deleted = [_word_order([old[key] for key in old.keys() - new.keys()])]
    modified: list[dict[str, Any]] = []
    for entry_id in old.keys() & new.keys():
        before = old[entry_id]
        after = new[entry_id]
        if before.semantic_key() != after.semantic_key():
            modified.append(
                {
                    "entry_id": entry_id,
                    "source_version": after.source_version,
                    "before": before.to_dict(),
                    "after": after.to_dict(),
                }
            )
    modified.sort(key=lambda item: item["entry_id"])
    return {
        "added": [word.to_dict() for word in added[0]],
        "deleted": [word.to_dict() for word in deleted[0]],
        "modified": modified,
    }


class Workspace:
    """Versioned, crash-recoverable workspace persisted under one directory."""

    def __init__(
        self,
        root: str | Path,
        *,
        config: WorkbenchConfig = WorkbenchConfig(),
        lock_timeout_s: float = 10.0,
    ) -> None:
        self.root = Path(root).resolve()
        self.config = config
        self.snapshot_path = self.root / "snapshot.json"
        self.versions_dir = self.root / "versions"
        self.lock_path = self.root / ".workbench.lock"
        self.stopping_path = self.root / ".stopping.json"
        self.lock_timeout_s = lock_timeout_s

    def _lock(self) -> FileLock:
        return FileLock(self.lock_path, self.lock_timeout_s)

    @staticmethod
    def _snapshot_body(state: WorkspaceState) -> dict[str, Any]:
        return {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "version": state.version,
            "parent_version": state.parent_version,
            "parent_snapshot_digest": state.parent_snapshot_digest,
            "kind": state.kind,
            "source_versions": list(state.source_versions),
            "input_fingerprint": state.input_fingerprint,
            "format_fingerprint": state.format_fingerprint,
            "timeline_digest": state.timeline_digest,
            "chunks": chunks_as_dict(state.chunks),
            "words": words_as_list(state.sorted_words()),
            "history": state.history,
            "migrations": state.migrations,
            "merge_records": state.merge_records,
            "metadata": state.metadata,
        }

    def _serialize_state(self, state: WorkspaceState) -> bytes:
        body = self._snapshot_body(state)
        state.snapshot_digest = digest_value(body)
        snapshot = dict(body)
        snapshot["snapshot_digest"] = state.snapshot_digest
        return (
            json.dumps(
                snapshot,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    def _state_from_snapshot(
        self,
        value: Mapping[str, Any],
        *,
        source: str,
    ) -> WorkspaceState:
        try:
            if int(value["schema_version"]) != CURRENT_SCHEMA_VERSION:
                raise stale_snapshot(
                    "Snapshot schema is not supported",
                    reason="malformed_snapshot",
                    source=source,
                    schema_version=value.get("schema_version"),
                )
            if not isinstance(value.get("chunks"), (dict, list)):
                raise stale_snapshot(
                    "Snapshot chunks are missing",
                    reason="malformed_snapshot",
                    source=source,
                )
            if not isinstance(value.get("words"), list):
                raise stale_snapshot(
                    "Snapshot words are missing",
                    reason="malformed_snapshot",
                    source=source,
                )
            chunks: dict[str, ChunkInfo] = {}
            chunk_items = value["chunks"].values() if isinstance(value["chunks"], dict) else value["chunks"]
            for item in chunk_items:
                chunk = ChunkInfo.from_dict(item)
                chunks[chunk.chunk_id] = chunk
            words = [Word.from_dict(item) for item in value["words"]]
            state = WorkspaceState(
                version=int(value["version"]),
                parent_version=int(value.get("parent_version", 0)),
                parent_snapshot_digest=(
                    str(value["parent_snapshot_digest"])
                    if value.get("parent_snapshot_digest") is not None
                    else None
                ),
                kind=str(value.get("kind", "unknown")),
                source_versions=[int(v) for v in value.get("source_versions", [])],
                chunks=chunks,
                words=sorted_words(words, chunks),
                history=list(value.get("history", [])),
                migrations=list(value.get("migrations", [])),
                merge_records=list(value.get("merge_records", [])),
                input_fingerprint=str(value.get("input_fingerprint", "")),
                format_fingerprint=str(value.get("format_fingerprint", "")),
                timeline_digest=str(value.get("timeline_digest", "")),
                snapshot_digest=str(value.get("snapshot_digest", "")),
                metadata=dict(value.get("metadata", {})),
            )
        except WorkbenchError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise stale_snapshot(
                "Snapshot structure is malformed",
                reason="malformed_snapshot",
                source=source,
                detail=str(exc),
            ) from exc

        body = {key: item for key, item in value.items() if key != "snapshot_digest"}
        actual_digest = digest_value(body)
        if not state.snapshot_digest or actual_digest != state.snapshot_digest:
            raise stale_snapshot(
                "Snapshot digest does not match its contents",
                reason="tamper",
                source=source,
                expected_snapshot_digest=state.snapshot_digest,
                actual_snapshot_digest=actual_digest,
                version=state.version,
            )
        actual_timeline_digest = timeline_digest(state.chunks, state.sorted_words())
        if actual_timeline_digest != state.timeline_digest:
            raise stale_snapshot(
                "Snapshot timeline summary does not match its entries",
                reason="summary_mismatch",
                source=source,
                version=state.version,
                expected_timeline_digest=state.timeline_digest,
                actual_timeline_digest=actual_timeline_digest,
            )
        expected_format_fingerprint = self.config.fingerprint()
        if state.format_fingerprint != expected_format_fingerprint:
            raise stale_snapshot(
                "Snapshot format configuration fingerprint has changed",
                reason="format_fingerprint_mismatch",
                source=source,
                version=state.version,
                expected_format_fingerprint=expected_format_fingerprint,
                actual_format_fingerprint=state.format_fingerprint,
            )
        expected_input_fingerprint = compute_input_fingerprint(state.chunks)
        if state.input_fingerprint != expected_input_fingerprint:
            raise stale_snapshot(
                "Snapshot input fingerprint does not match its chunk metadata",
                reason="input_fingerprint_mismatch",
                source=source,
                version=state.version,
                expected_input_fingerprint=expected_input_fingerprint,
                actual_input_fingerprint=state.input_fingerprint,
            )
        try:
            validate_timeline(state.words, state.chunks, self.config)
        except WorkbenchError as exc:
            raise stale_snapshot(
                "Snapshot timeline is invalid",
                reason="timeline_invalid",
                source=source,
                version=state.version,
                timeline_error=exc.details,
            ) from exc
        return state

    def _read_snapshot_file(self, path: Path) -> WorkspaceState:
        try:
            raw = path.read_bytes()
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise stale_snapshot(
                "Snapshot is unreadable or malformed",
                reason="malformed_snapshot",
                source=str(path),
                detail=str(exc),
            ) from exc
        if not isinstance(value, dict):
            raise stale_snapshot(
                "Snapshot must be a JSON object",
                reason="malformed_snapshot",
                source=str(path),
            )
        return self._state_from_snapshot(value, source=str(path))

    def _archive_path(self, version: int) -> Path:
        return self.versions_dir / f"v{version:08d}.json"

    def _read_archives(self) -> dict[int, WorkspaceState]:
        archives: dict[int, WorkspaceState] = {}
        if not self.versions_dir.exists():
            return archives
        for path in sorted(self.versions_dir.glob("v*.json")):
            try:
                version_text = path.stem[1:]
                version = int(version_text)
            except ValueError:
                continue
            state = self._read_snapshot_file(path)
            if state.version != version:
                raise stale_snapshot(
                    "Snapshot archive filename and internal version disagree",
                    reason="tamper",
                    source=str(path),
                    filename_version=version,
                    internal_version=state.version,
                )
            archives[version] = state
        return archives

    def _verify_input_files(self, state: WorkspaceState) -> None:
        for chunk in state.chunks.values():
            path = Path(chunk.path)
            if not path.is_file():
                raise stale_snapshot(
                    "Input audio chunk is missing",
                    reason="input_fingerprint_mismatch",
                    version=state.version,
                    chunk_id=chunk.chunk_id,
                    path=chunk.path,
                )
            actual = file_sha256(path)
            if actual != chunk.fingerprint:
                raise stale_snapshot(
                    "Input audio chunk fingerprint has changed",
                    reason="input_fingerprint_mismatch",
                    version=state.version,
                    chunk_id=chunk.chunk_id,
                    expected_fingerprint=chunk.fingerprint,
                    actual_fingerprint=actual,
                )

    def _load_state_unlocked(self) -> WorkspaceState | None:
        if not self.snapshot_path.exists():
            return None
        current = self._read_snapshot_file(self.snapshot_path)
        archives = self._read_archives()
        if archives:
            latest_version = max(archives)
            latest = archives[latest_version]
            if current.version < latest_version:
                raise stale_snapshot(
                    "Snapshot version rolled back behind an archived version",
                    reason="rollback",
                    current_version=current.version,
                    latest_archived_version=latest_version,
                    current_snapshot_digest=current.snapshot_digest,
                    latest_archived_snapshot_digest=latest.snapshot_digest,
                )
            if current.version == latest_version:
                if current.snapshot_digest != latest.snapshot_digest:
                    raise stale_snapshot(
                        "Current snapshot does not match the archived snapshot at the same version",
                        reason="tamper",
                        version=current.version,
                        current_snapshot_digest=current.snapshot_digest,
                        archived_snapshot_digest=latest.snapshot_digest,
                    )
            else:
                if current.version != latest_version + 1:
                    raise stale_snapshot(
                        "Snapshot version jumped past the archived monotonic chain",
                        reason="version_gap",
                        current_version=current.version,
                        latest_archived_version=latest_version,
                    )
                if current.input_fingerprint != latest.input_fingerprint:
                    raise stale_snapshot(
                        "Snapshot continues the version chain but changed input fingerprints",
                        reason="input_fingerprint_mismatch",
                        current_version=current.version,
                        latest_archived_version=latest_version,
                        current_input_fingerprint=current.input_fingerprint,
                        previous_input_fingerprint=latest.input_fingerprint,
                    )
                known_parent_digests = {
                    state.snapshot_digest for state in archives.values()
                }
                if current.parent_snapshot_digest not in known_parent_digests:
                    raise stale_snapshot(
                        "Snapshot parent digest does not match any archived snapshot",
                        reason="broken_chain",
                        current_version=current.version,
                        latest_archived_version=latest_version,
                        actual_parent_digest=current.parent_snapshot_digest,
                    )
        self._verify_input_files(current)
        latest_version = max(archives) if archives else 0
        if current.version > latest_version:
            payload = self.snapshot_path.read_bytes()
            try:
                atomic_write_bytes(
                    self._archive_path(current.version),
                    payload,
                )
            except OSError:
                pass
        return current

    def _write_archive_unlocked(self, state: WorkspaceState, payload: bytes) -> None:
        archive_path = self._archive_path(state.version)
        if archive_path.exists():
            existing = self._read_snapshot_file(archive_path)
            if existing.snapshot_digest != state.snapshot_digest:
                raise stale_version(
                    "Refusing to overwrite a different archived version",
                    version=state.version,
                    existing_snapshot_digest=existing.snapshot_digest,
                    new_snapshot_digest=state.snapshot_digest,
                )
            return
        atomic_write_bytes(archive_path, payload)

    def _save_state_unlocked(self, state: WorkspaceState) -> None:
        payload = self._serialize_state(state)
        # Current snapshot is the commit point. The archive follows; if a hard
        # stop lands between the two writes, the next load can verify the parent
        # digest and repair the missing archive without advancing twice.
        atomic_write_bytes(self.snapshot_path, payload)
        try:
            self._write_archive_unlocked(state, payload)
        except OSError:
            # The current snapshot is already durable and self-contained.
            pass

    def _assert_not_stopping(self) -> None:
        if self.stopping_path.exists():
            try:
                marker = json.loads(self.stopping_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                marker = {"status": "draining"}
            raise WorkbenchError(
                "E_STOPPING",
                "The workspace is draining after a stop request",
                marker,
            )

    @staticmethod
    def _parent_history(parent: WorkspaceState) -> list[dict[str, Any]]:
        history = list(parent.history)
        summary = parent.history_summary()
        summary["changes"] = parent.metadata.get("changes", {})
        history.append(summary)
        return history

    def _new_state(
        self,
        *,
        parent: WorkspaceState | None,
        version: int,
        kind: str,
        chunks: Mapping[str, ChunkInfo],
        words: Sequence[Word],
        source_versions: Sequence[int] = (),
        migrations: Sequence[dict[str, Any]] = (),
        merge_records: Sequence[dict[str, Any]] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> WorkspaceState:
        validate_timeline(words, chunks, self.config)
        state = WorkspaceState(
            version=version,
            parent_version=parent.version if parent else 0,
            parent_snapshot_digest=parent.snapshot_digest if parent else None,
            kind=kind,
            source_versions=[int(v) for v in source_versions],
            chunks=dict(chunks),
            words=sorted_words(words, chunks),
            history=self._parent_history(parent) if parent else [],
            migrations=list(migrations),
            merge_records=list(merge_records),
            input_fingerprint=compute_input_fingerprint(chunks),
            format_fingerprint=self.config.fingerprint(),
            timeline_digest=timeline_digest(chunks, sorted_words(words, chunks)),
            metadata=dict(metadata or {}),
        )
        state.metadata["changes"] = changes_between(
            parent.words if parent else [], state.words
        )
        return state

    def _load_version_unlocked(self, version: int) -> WorkspaceState:
        current = self._load_state_unlocked()
        if current is not None and current.version == version:
            return current
        archive = self._archive_path(version)
        if not archive.exists():
            raise WorkbenchError(
                "E_NOT_FOUND",
                "Requested version does not exist",
                {"version": version},
            )
        state = self._read_snapshot_file(archive)
        self._verify_input_files(state)
        return state

    def _publish_expected(
        self,
        current: WorkspaceState | None,
        expected_version: int,
        operations: Sequence[Mapping[str, Any]],
    ) -> None:
        actual_version = current.version if current else 0
        if actual_version != expected_version:
            raise stale_version(
                "Expected base version is no longer current; edit was not committed",
                expected_version=expected_version,
                latest_version=actual_version,
                replayable_edits=list(operations),
                replay_against_version=actual_version,
            )

    def _apply_operations(
        self,
        state: WorkspaceState,
        operations: Sequence[Mapping[str, Any]],
        new_version: int,
    ) -> list[Word]:
        words = list(state.words)
        for operation_index, operation in enumerate(operations):
            if not isinstance(operation, Mapping):
                raise WorkbenchError(
                    "E_INVALID_REQUEST",
                    "Each edit operation must be an object",
                    {"operation_index": operation_index},
                )
            op = str(operation.get("op", ""))
            if op == "add_word":
                word = self._operation_word(
                    operation,
                    state.chunks,
                    new_version,
                    operation_index,
                )
                if any(existing.entry_id == word.entry_id for existing in words):
                    raise WorkbenchError(
                        "E_INVALID_REQUEST",
                        "Edit would create a duplicate entry_id",
                        {"entry_id": word.entry_id, "operation_index": operation_index},
                    )
                words.append(word)
            elif op == "update_word":
                entry_id = str(operation.get("entry_id", ""))
                match_index = next(
                    (index for index, word in enumerate(words) if word.entry_id == entry_id),
                    None,
                )
                if match_index is None:
                    raise WorkbenchError(
                        "E_NOT_FOUND",
                        "Edit references an unknown entry_id",
                        {"entry_id": entry_id, "operation_index": operation_index},
                    )
                old = words[match_index]
                replacements: dict[str, Any] = {}
                for field in ("start_ms", "end_ms", "text", "confidence", "language"):
                    if field in operation:
                        replacements[field] = operation[field]
                if "start_ms" in replacements:
                    replacements["start_ms"] = int(replacements["start_ms"])
                if "end_ms" in replacements:
                    replacements["end_ms"] = int(replacements["end_ms"])
                if "confidence" in replacements:
                    replacements["confidence"] = float(replacements["confidence"])
                if "text" in replacements:
                    replacements["text"] = str(replacements["text"])
                if "language" in replacements:
                    replacements["language"] = str(replacements["language"])
                words[match_index] = Word(
                    entry_id=old.entry_id,
                    chunk_id=old.chunk_id,
                    start_ms=replacements.get("start_ms", old.start_ms),
                    end_ms=replacements.get("end_ms", old.end_ms),
                    text=replacements.get("text", old.text),
                    confidence=replacements.get("confidence", old.confidence),
                    language=replacements.get("language", old.language),
                    source_version=new_version,
                    source_entry_ids=old.source_entry_ids + (old.entry_id,),
                    merged_from_versions=old.merged_from_versions,
                    conflict_id=old.conflict_id,
                )
            elif op == "delete_word":
                entry_id = str(operation.get("entry_id", ""))
                before = len(words)
                words = [word for word in words if word.entry_id != entry_id]
                if len(words) == before:
                    raise WorkbenchError(
                        "E_NOT_FOUND",
                        "Delete references an unknown entry_id",
                        {"entry_id": entry_id, "operation_index": operation_index},
                    )
            elif op == "replace_chunk":
                chunk_id = str(operation.get("chunk_id", ""))
                if chunk_id not in state.chunks:
                    raise WorkbenchError(
                        "E_NOT_FOUND",
                        "Replace references an unknown chunk_id",
                        {"chunk_id": chunk_id, "operation_index": operation_index},
                    )
                raw_words = operation.get("words")
                if not isinstance(raw_words, list):
                    raise WorkbenchError(
                        "E_INVALID_REQUEST",
                        "replace_chunk requires a words array",
                        {"operation_index": operation_index},
                    )
                words = [word for word in words if word.chunk_id != chunk_id]
                for word_index, raw_word in enumerate(raw_words):
                    if not isinstance(raw_word, Mapping):
                        raise WorkbenchError(
                            "E_INVALID_REQUEST",
                            "replace_chunk words must be objects",
                            {"operation_index": operation_index, "word_index": word_index},
                        )
                    payload = dict(raw_word)
                    payload.setdefault("chunk_id", chunk_id)
                    words.append(
                        self._operation_word(
                            payload,
                            state.chunks,
                            new_version,
                            operation_index * 100_000 + word_index,
                            allow_source_metadata=True,
                        )
                    )
            else:
                raise WorkbenchError(
                    "E_INVALID_REQUEST",
                    "Unknown edit operation",
                    {"op": op, "operation_index": operation_index},
                )
        validate_timeline(words, state.chunks, self.config)
        return sorted_words(words, state.chunks)

    def _operation_word(
        self,
        value: Mapping[str, Any],
        chunks: Mapping[str, ChunkInfo],
        new_version: int,
        sequence: int,
        *,
        allow_source_metadata: bool = False,
    ) -> Word:
        chunk_id = str(value.get("chunk_id", ""))
        if chunk_id not in chunks:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Word references an unknown chunk_id",
                {"chunk_id": chunk_id, "sequence": sequence},
            )
        required = ("start_ms", "end_ms", "text")
        missing = [field for field in required if field not in value]
        if missing:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Word operation is missing required fields",
                {"missing": missing, "sequence": sequence},
            )
        start_ms = int(value["start_ms"])
        end_ms = int(value["end_ms"])
        text = str(value["text"])
        confidence = float(value.get("confidence", 0.5))
        language = str(value.get("language") or chunks[chunk_id].language)
        entry_id = str(value.get("entry_id", ""))
        if not entry_id:
            entry_id = (
                f"e-{chunk_id}-v{new_version}-{sequence:06d}-"
                f"{digest_value({'chunk_id': chunk_id, 'start_ms': start_ms, 'end_ms': end_ms, 'text': text, 'sequence': sequence})[:10]}"
            )
        source_entry_ids = tuple(str(item) for item in value.get("source_entry_ids", []))
        if allow_source_metadata and not source_entry_ids and value.get("entry_id"):
            source_entry_ids = (str(value["entry_id"]),)
        return Word(
            entry_id=entry_id,
            chunk_id=chunk_id,
            start_ms=start_ms,
            end_ms=end_ms,
            text=text,
            confidence=confidence,
            language=language,
            source_version=new_version,
            source_entry_ids=source_entry_ids,
            merged_from_versions=tuple(
                int(item) for item in value.get("merged_from_versions", [])
            ),
            conflict_id=(
                str(value["conflict_id"])
                if value.get("conflict_id") is not None
                else None
            ),
        )

    def bootstrap(
        self,
        manifest_path: str | Path,
        adapter: TranscriptionAdapter | None = None,
    ) -> dict[str, Any]:
        adapter = adapter or StubTranscriptionAdapter(self.config)
        manifest_path = Path(manifest_path).resolve()
        manifest, chunks = load_manifest(manifest_path)
        with self._lock():
            self._assert_not_stopping()
            current = self._load_state_unlocked()
            expected_input = compute_input_fingerprint(chunks)
            if current is not None:
                if current.input_fingerprint != expected_input:
                    raise stale_snapshot(
                        "Task manifest inputs do not match the initialized workspace",
                        reason="input_fingerprint_mismatch",
                        current_input_fingerprint=current.input_fingerprint,
                        supplied_input_fingerprint=expected_input,
                        current_version=current.version,
                    )
                return self._state_response(current)
            words: list[Word] = []
            for chunk in sorted(chunks.values(), key=lambda item: (item.order, item.chunk_id)):
                words.extend(
                    adapter.transcribe(
                        chunk_id=chunk.chunk_id,
                        audio_path=Path(chunk.path),
                        duration_ms=chunk.duration_ms,
                        reference_text=chunk.reference_text,
                        language=chunk.language,
                        source_version=1,
                    )
                )
            state = self._new_state(
                parent=None,
                version=1,
                kind="bootstrap",
                chunks=chunks,
                words=words,
                metadata={
                    "adapter": getattr(adapter, "name", adapter.__class__.__name__),
                    "manifest_path": str(manifest_path),
                    "manifest_seed": manifest.get("seed"),
                },
            )
            state.metadata["changes"] = changes_between([], state.words)
            self._save_state_unlocked(state)
            return self._state_response(state)

    def load_state(self) -> WorkspaceState:
        with self._lock():
            state = self._load_state_unlocked()
        if state is None:
            raise WorkbenchError(
                "E_NOT_FOUND",
                "Workspace has not been initialized",
                {"root": str(self.root)},
            )
        return state

    def load_version(self, version: int) -> WorkspaceState:
        with self._lock():
            return self._load_version_unlocked(version)

    def _state_response(self, state: WorkspaceState) -> dict[str, Any]:
        return {
            "ok": True,
            "version": state.version,
            "parent_version": state.parent_version,
            "kind": state.kind,
            "source_versions": state.source_versions,
            "word_count": len(state.words),
            "chunk_word_counts": {
                chunk_id: sum(1 for word in state.words if word.chunk_id == chunk_id)
                for chunk_id in state.chunks
            },
            "snapshot_digest": state.snapshot_digest,
            "timeline_digest": state.timeline_digest,
            "input_fingerprint": state.input_fingerprint,
            "format_fingerprint": state.format_fingerprint,
            "migrations": state.migrations,
            "merge_records": state.merge_records,
        }

    def edit(
        self,
        operations: Sequence[Mapping[str, Any]],
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        with self._lock():
            current = self._load_state_unlocked()
            self._publish_expected(current, expected_version, operations)
            self._assert_not_stopping()
            assert current is not None
            new_version = current.version + 1
            words = self._apply_operations(current, operations, new_version)
            state = self._new_state(
                parent=current,
                version=new_version,
                kind="edit",
                chunks=current.chunks,
                words=words,
                metadata={
                    "adapter": current.metadata.get("adapter"),
                    "edit_operations": list(operations),
                },
                migrations=current.migrations,
                merge_records=current.merge_records,
            )
            self._save_state_unlocked(state)
            response = self._state_response(state)
            response["changes"] = state.metadata.get("changes", {})
            return response

    def fork_edit(
        self,
        operations: Sequence[Mapping[str, Any]],
        *,
        parent_version: int,
        expected_version: int,
    ) -> dict[str, Any]:
        """Publish a monotonic version whose content branches from an older one.

        The new version number is still current+1. Its parent is deliberately
        the selected archived version, which lets merge tests exercise genuine
        divergent branches without skipping version numbers.
        """

        with self._lock():
            current = self._load_state_unlocked()
            self._publish_expected(current, expected_version, operations)
            self._assert_not_stopping()
            assert current is not None
            base = self._load_version_unlocked(parent_version)
            new_version = current.version + 1
            words = self._apply_operations(base, operations, new_version)
            state = self._new_state(
                parent=base,
                version=new_version,
                kind="fork",
                chunks=base.chunks,
                words=words,
                source_versions=[parent_version],
                metadata={
                    "adapter": current.metadata.get("adapter"),
                    "fork_parent_version": parent_version,
                    "edit_operations": list(operations),
                },
                migrations=base.migrations,
                merge_records=base.merge_records,
            )
            self._save_state_unlocked(state)
            response = self._state_response(state)
            response["changes"] = state.metadata.get("changes", {})
            return response

    def _check_import_order(self, decoded: DecodeResult) -> None:
        seen_chunks: dict[str, int] = {}
        for index, word in enumerate(decoded.words):
            previous = seen_chunks.get(word.chunk_id)
            if previous is not None and word.start_ms < previous:
                raise timeline_error(
                    chunk_id=word.chunk_id,
                    entry_index=index,
                    entry_id=word.entry_id,
                    violation="out_of_order",
                    message="Imported words must be ordered by start_ms",
                )
            seen_chunks[word.chunk_id] = word.start_ms

    def import_text(
        self,
        *,
        content: str,
        fmt: str,
        expected_version: int,
        chunk_id: str | None = None,
    ) -> dict[str, Any]:
        with self._lock():
            current = self._load_state_unlocked()
            self._publish_expected(current, expected_version, [])
            self._assert_not_stopping()
            assert current is not None
            if chunk_id is not None and chunk_id not in current.chunks:
                raise WorkbenchError(
                    "E_NOT_FOUND",
                    "Import target chunk does not exist",
                    {"chunk_id": chunk_id},
                )
            default_language = (
                current.chunks[chunk_id].language
                if chunk_id is not None
                else next(iter(current.chunks.values())).language
            )
            decoded = decode_text(
                content,
                fmt,
                known_chunks=current.chunks,
                default_language=default_language,
                default_chunk_id=chunk_id,
                config=self.config,
            )
            self._check_import_order(decoded)
            imported_chunk_ids = {word.chunk_id for word in decoded.words}
            if chunk_id is not None:
                foreign = imported_chunk_ids - {chunk_id}
                if foreign:
                    raise incompat_format(
                        "Subtitle import contains unexpected chunk ids",
                        target_chunk_id=chunk_id,
                        unexpected_chunk_ids=sorted(foreign),
                    )
                imported_chunk_ids = {chunk_id}
            for imported_chunk_id in imported_chunk_ids:
                if imported_chunk_id not in current.chunks:
                    raise incompat_format(
                        "Imported transcript references a chunk that is not in the workspace",
                        chunk_id=imported_chunk_id,
                    )
                decoded_duration = decoded.chunks[imported_chunk_id].duration_ms
                current_duration = current.chunks[imported_chunk_id].duration_ms
                if decoded_duration and decoded_duration != current_duration:
                    raise incompat_format(
                        "Imported chunk duration does not match the audio chunk",
                        chunk_id=imported_chunk_id,
                        imported_duration_ms=decoded_duration,
                        workspace_duration_ms=current_duration,
                    )
            new_version = current.version + 1
            kept = [
                word for word in current.words if word.chunk_id not in imported_chunk_ids
            ]
            imported_words: list[Word] = []
            existing_ids = {word.entry_id for word in kept}
            for index, word in enumerate(decoded.words):
                entry_id = word.entry_id
                if entry_id in existing_ids or any(
                    candidate.entry_id == entry_id for candidate in imported_words
                ):
                    entry_id = (
                        f"i-{word.chunk_id}-v{new_version}-{index:06d}-"
                        f"{digest_value([word.to_dict(), index])[:10]}"
                    )
                existing_ids.add(entry_id)
                imported_words.append(
                    Word(
                        entry_id=entry_id,
                        chunk_id=word.chunk_id,
                        start_ms=word.start_ms,
                        end_ms=word.end_ms,
                        text=word.text,
                        confidence=word.confidence,
                        language=word.language,
                        source_version=new_version,
                        source_entry_ids=word.source_entry_ids or (word.entry_id,),
                        merged_from_versions=word.merged_from_versions,
                        conflict_id=word.conflict_id,
                    )
                )
            combined = kept + imported_words
            validate_timeline(combined, current.chunks, self.config)
            migrations = list(current.migrations) + [
                dict(item, imported_at_version=new_version)
                for item in decoded.migrations
            ]
            kind = "migration" if decoded.migrations else "import"
            state = self._new_state(
                parent=current,
                version=new_version,
                kind=kind,
                chunks=current.chunks,
                words=combined,
                metadata={
                    "adapter": current.metadata.get("adapter"),
                    "import_format": decoded.format,
                    "import_chunk_ids": sorted(imported_chunk_ids),
                    "loss_report": decoded.loss_report,
                    "source_schema_version": decoded.source_schema_version,
                },
                migrations=migrations,
                merge_records=current.merge_records,
            )
            self._save_state_unlocked(state)
            response = self._state_response(state)
            response["changes"] = state.metadata.get("changes", {})
            response["loss_report"] = decoded.loss_report
            return response

    def export(
        self,
        *,
        fmt: str,
        version: int | None = None,
        chunk_id: str | None = None,
    ) -> dict[str, Any]:
        state = self.load_state() if version is None else self.load_version(version)
        chunks: Mapping[str, ChunkInfo] = state.chunks
        words: Sequence[Word] = state.words
        if chunk_id is not None:
            if chunk_id not in state.chunks:
                raise WorkbenchError(
                    "E_NOT_FOUND", "Export chunk does not exist", {"chunk_id": chunk_id}
                )
            chunks = {chunk_id: state.chunks[chunk_id]}
            words = [word for word in state.words if word.chunk_id == chunk_id]
        content = encode_text(
            fmt,
            chunks,
            words,
            self.config,
            version=state.version,
        )
        return {
            "ok": True,
            "version": state.version,
            "format": fmt,
            "chunk_id": chunk_id,
            "word_count": len(words),
            "content": content,
            "loss_report": [],
        }

    def version_history(self) -> list[dict[str, Any]]:
        with self._lock():
            current = self._load_state_unlocked()
            states = self._read_archives()
            if current is not None:
                states[current.version] = current
        if not states:
            raise WorkbenchError(
                "E_NOT_FOUND", "Workspace has not been initialized"
            )
        entries: list[dict[str, Any]] = []
        for version in sorted(states):
            state = states[version]
            summary = state.history_summary()
            summary["changes"] = state.metadata.get("changes", {})
            entries.append(summary)
        return entries

    def diff_versions(self, left: int, right: int) -> dict[str, Any]:
        left_state = self.load_version(left)
        right_state = self.load_version(right)
        changes = changes_between(left_state.words, right_state.words)
        return {
            "ok": True,
            "left_version": left,
            "right_version": right,
            "left_word_count": len(left_state.words),
            "right_word_count": len(right_state.words),
            "counts": {
                "added": len(changes["added"]),
                "deleted": len(changes["deleted"]),
                "modified": len(changes["modified"]),
            },
            **changes,
        }

    def _ancestor_chain_unlocked(self, state: WorkspaceState) -> list[WorkspaceState]:
        chain = [state]
        seen = {state.version}
        version = state.parent_version
        while version > 0 and version not in seen:
            parent = self._load_version_unlocked(version)
            chain.append(parent)
            seen.add(parent.version)
            version = parent.parent_version
        return chain

    def _common_ancestor_unlocked(
        self, left: WorkspaceState, right: WorkspaceState
    ) -> WorkspaceState | None:
        left_versions = {state.version: state for state in self._ancestor_chain_unlocked(left)}
        for candidate in self._ancestor_chain_unlocked(right):
            if candidate.version in left_versions:
                return left_versions[candidate.version]
        return None

    @staticmethod
    def _merge_word_sources(
        left: Word,
        right: Word,
        left_version: int,
        right_version: int,
        *,
        conflict_id: str | None = None,
    ) -> Word:
        sources = tuple(sorted({*left.source_entry_ids, *right.source_entry_ids}))
        versions = tuple(
            sorted(
                {
                    *left.merged_from_versions,
                    *right.merged_from_versions,
                    left_version,
                    right_version,
                }
            )
        )
        return Word(
            entry_id=left.entry_id,
            chunk_id=left.chunk_id,
            start_ms=left.start_ms,
            end_ms=left.end_ms,
            text=left.text,
            confidence=left.confidence,
            language=left.language,
            source_version=max(left.source_version, right.source_version),
            source_entry_ids=sources,
            merged_from_versions=versions,
            conflict_id=conflict_id,
        )

    @staticmethod
    def _merge_candidate(word: Word | None) -> dict[str, Any] | None:
        return word.to_dict() if word is not None else None

    def merge_versions(
        self,
        *,
        left_version: int,
        right_version: int,
        strategy: str,
        expected_version: int,
    ) -> dict[str, Any]:
        if strategy not in self.config.merge_strategies:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Unknown merge strategy",
                {"strategy": strategy, "allowed": list(self.config.merge_strategies)},
            )
        with self._lock():
            current = self._load_state_unlocked()
            self._publish_expected(current, expected_version, [])
            self._assert_not_stopping()
            assert current is not None
            left = self._load_version_unlocked(left_version)
            right = self._load_version_unlocked(right_version)
            base = self._common_ancestor_unlocked(left, right)
            base_map = base.word_map() if base else {}
            left_map = left.word_map()
            right_map = right.word_map()
            new_version = current.version + 1
            conflicts: list[dict[str, Any]] = []
            chosen: dict[str, dict[str, Any]] = {}

            def record_conflict(
                reason: str,
                entry_id: str,
                left_word: Word | None,
                right_word: Word | None,
                resolution: str,
                kept: Word | None,
            ) -> None:
                conflicts.append(
                    {
                        "conflict_id": f"mc-{new_version}-{len(conflicts) + 1:03d}",
                        "reason": reason,
                        "entry_id": entry_id,
                        "left": self._merge_candidate(left_word),
                        "right": self._merge_candidate(right_word),
                        "strategy": strategy,
                        "resolution": resolution,
                        "kept_entry_id": kept.entry_id if kept else None,
                    }
                )

            all_ids = sorted(base_map.keys() | left_map.keys() | right_map.keys())
            for entry_id in all_ids:
                base_word = base_map.get(entry_id)
                left_word = left_map.get(entry_id)
                right_word = right_map.get(entry_id)
                left_key = left_word.semantic_key() if left_word else None
                right_key = right_word.semantic_key() if right_word else None
                base_key = base_word.semantic_key() if base_word else None
                kept: Word | None = None
                origins: set[str] = set()
                changed = False
                if left_word is not None and right_word is not None:
                    if left_key == right_key:
                        kept = self._merge_word_sources(
                            left_word, right_word, left.version, right.version
                        )
                        origins = {"A", "B"}
                        changed = base_word is None or left_key != base_key
                    elif base_word is not None and left_key == base_key:
                        kept = right_word
                        origins = {"B"}
                        changed = True
                    elif base_word is not None and right_key == base_key:
                        kept = left_word
                        origins = {"A"}
                        changed = True
                    else:
                        candidates = (left_word, right_word)
                        if strategy == "last-wins":
                            kept = right_word
                            origins = {"B"}
                            resolution = "right_version_wins"
                        else:
                            conflict_id = f"mc-{new_version}-{len(conflicts) + 1:03d}"
                            kept = Word(
                                entry_id=left_word.entry_id,
                                chunk_id=left_word.chunk_id,
                                start_ms=left_word.start_ms,
                                end_ms=left_word.end_ms,
                                text=left_word.text,
                                confidence=left_word.confidence,
                                language=left_word.language,
                                source_version=left_word.source_version,
                                source_entry_ids=left_word.source_entry_ids,
                                merged_from_versions=tuple(
                                    sorted(
                                        {
                                            *left_word.merged_from_versions,
                                            left.version,
                                            right.version,
                                        }
                                    )
                                ),
                                conflict_id=conflict_id,
                            )
                            origins = {"A"}
                            resolution = "conflict_marked_left_candidate_retained"
                        record_conflict(
                            "same_id_divergent_change",
                            entry_id,
                            candidates[0],
                            candidates[1],
                            resolution,
                            kept,
                        )
                        changed = True
                elif left_word is not None:
                    if base_word is None:
                        kept = left_word
                        origins = {"A"}
                        changed = True
                    elif left_key == base_key:
                        kept = None
                        origins = {"B"}
                        changed = True
                        record_conflict(
                            "delete_vs_modify" if right_word is not None else "delete",
                            entry_id,
                            left_word,
                            right_word,
                            "right_version_deleted",
                            None,
                        )
                    else:
                        if strategy == "last-wins":
                            kept = None
                            origins = {"B"}
                            resolution = "right_version_deletion_wins"
                        else:
                            conflict_id = f"mc-{new_version}-{len(conflicts) + 1:03d}"
                            kept = Word(
                                entry_id=left_word.entry_id,
                                chunk_id=left_word.chunk_id,
                                start_ms=left_word.start_ms,
                                end_ms=left_word.end_ms,
                                text=left_word.text,
                                confidence=left_word.confidence,
                                language=left_word.language,
                                source_version=left_word.source_version,
                                source_entry_ids=left_word.source_entry_ids,
                                merged_from_versions=tuple(
                                    sorted(
                                        {
                                            *left_word.merged_from_versions,
                                            left.version,
                                            right.version,
                                        }
                                    )
                                ),
                                conflict_id=conflict_id,
                            )
                            origins = {"A"}
                            resolution = "conflict_marked_left_candidate_retained"
                        record_conflict(
                            "delete_vs_modify",
                            entry_id,
                            left_word,
                            right_word,
                            resolution,
                            kept,
                        )
                        changed = True
                elif right_word is not None:
                    if base_word is None:
                        kept = right_word
                        origins = {"B"}
                        changed = True
                    elif right_key == base_key:
                        kept = None
                        origins = {"A"}
                        changed = True
                        record_conflict(
                            "delete",
                            entry_id,
                            left_word,
                            right_word,
                            "left_version_deleted",
                            None,
                        )
                    else:
                        if strategy == "last-wins":
                            kept = right_word
                            origins = {"B"}
                        else:
                            conflict_id = f"mc-{new_version}-{len(conflicts) + 1:03d}"
                            kept = Word(
                                entry_id=right_word.entry_id,
                                chunk_id=right_word.chunk_id,
                                start_ms=right_word.start_ms,
                                end_ms=right_word.end_ms,
                                text=right_word.text,
                                confidence=right_word.confidence,
                                language=right_word.language,
                                source_version=right_word.source_version,
                                source_entry_ids=right_word.source_entry_ids,
                                merged_from_versions=tuple(
                                    sorted(
                                        {
                                            *right_word.merged_from_versions,
                                            left.version,
                                            right.version,
                                        }
                                    )
                                ),
                                conflict_id=conflict_id,
                            )
                            origins = {"B"}
                        record_conflict(
                            "delete_vs_modify",
                            entry_id,
                            left_word,
                            right_word,
                            "right_version_retained"
                            if strategy == "last-wins"
                            else "conflict_marked_right_candidate_retained",
                            kept,
                        )
                        changed = True
                if kept is not None:
                    chosen[kept.entry_id] = {
                        "word": kept,
                        "origins": origins,
                        "changed": changed,
                    }

            conflicts_by_id = {
                conflict["conflict_id"]: conflict for conflict in conflicts
            }
            chunk_ids = sorted(
                current.chunks,
                key=lambda cid: (current.chunks[cid].order, cid),
            )
            for chunk_id in chunk_ids:
                items = sorted(
                    (
                        item
                        for item in chosen.values()
                        if item["word"].chunk_id == chunk_id
                    ),
                    key=lambda item: (
                        item["word"].start_ms,
                        item["word"].end_ms,
                        item["word"].entry_id,
                    ),
                )
                remove: set[str] = set()
                for left_index, left_item in enumerate(items):
                    if left_item["word"].entry_id in remove:
                        continue
                    for right_item in items[left_index + 1 :]:
                        if right_item["word"].entry_id in remove:
                            continue
                        if right_item["word"].start_ms >= left_item["word"].end_ms:
                            break
                        if not (
                            left_item["origins"].isdisjoint(right_item["origins"])
                            and (left_item["changed"] or right_item["changed"])
                        ):
                            continue
                        if strategy == "last-wins":
                            if "A" in left_item["origins"] and "B" in right_item["origins"]:
                                loser = left_item
                            elif "B" in left_item["origins"] and "A" in right_item["origins"]:
                                loser = right_item
                            else:
                                loser = left_item
                            winner = right_item if loser is left_item else left_item
                            remove.add(loser["word"].entry_id)
                            resolution = "right_version_overlap_wins"
                        else:
                            conflict_id = f"mc-{new_version}-{len(conflicts) + 1:03d}"
                            if "A" in left_item["origins"]:
                                winner = left_item
                                loser = right_item
                            else:
                                winner = right_item
                                loser = left_item
                            conflict_id_value = conflict_id
                            winner_word = winner["word"]
                            winner["word"] = Word(
                                entry_id=winner_word.entry_id,
                                chunk_id=winner_word.chunk_id,
                                start_ms=winner_word.start_ms,
                                end_ms=winner_word.end_ms,
                                text=winner_word.text,
                                confidence=winner_word.confidence,
                                language=winner_word.language,
                                source_version=winner_word.source_version,
                                source_entry_ids=winner_word.source_entry_ids,
                                merged_from_versions=winner_word.merged_from_versions,
                                conflict_id=conflict_id_value,
                            )
                            chosen[winner_word.entry_id] = winner
                            remove.add(loser["word"].entry_id)
                            resolution = "conflict_marked_overlap_winner_retained"
                        conflict_value = {
                            "conflict_id": conflict_id
                            if strategy == "mark-conflict"
                            else f"mc-{new_version}-{len(conflicts) + 1:03d}",
                            "reason": "overlapping_interval_from_different_versions",
                            "entry_id": left_item["word"].entry_id,
                            "left": self._merge_candidate(left_item["word"]),
                            "right": self._merge_candidate(right_item["word"]),
                            "strategy": strategy,
                            "resolution": resolution,
                            "kept_entry_id": winner["word"].entry_id,
                        }
                        conflicts.append(conflict_value)
                        conflicts_by_id[conflict_value["conflict_id"]] = conflict_value
                for entry_id in remove:
                    chosen.pop(entry_id, None)

            final_words: list[Word] = []
            for item in chosen.values():
                word = item["word"]
                versions = tuple(
                    sorted(
                        {
                            *word.merged_from_versions,
                            left.version,
                            right.version,
                        }
                    )
                )
                if versions != word.merged_from_versions:
                    word = Word(
                        entry_id=word.entry_id,
                        chunk_id=word.chunk_id,
                        start_ms=word.start_ms,
                        end_ms=word.end_ms,
                        text=word.text,
                        confidence=word.confidence,
                        language=word.language,
                        source_version=word.source_version,
                        source_entry_ids=word.source_entry_ids,
                        merged_from_versions=versions,
                        conflict_id=word.conflict_id,
                    )
                    item["word"] = word
                final_words.append(word)
            validate_timeline(final_words, current.chunks, self.config)
            merge_record = {
                "merge_version": new_version,
                "left_version": left_version,
                "right_version": right_version,
                "common_ancestor_version": base.version if base else None,
                "strategy": strategy,
                "conflict_count": len(conflicts),
                "conflicts": conflicts,
            }
            state = self._new_state(
                parent=current,
                version=new_version,
                kind="merge",
                chunks=current.chunks,
                words=final_words,
                source_versions=[left_version, right_version],
                migrations=current.migrations,
                merge_records=[*current.merge_records, merge_record],
                metadata={
                    "adapter": current.metadata.get("adapter"),
                    "merge_strategy": strategy,
                    "merge_left_version": left_version,
                    "merge_right_version": right_version,
                },
            )
            self._save_state_unlocked(state)
            response = self._state_response(state)
            response["changes"] = state.metadata.get("changes", {})
            response["merge"] = merge_record
            return response

    @staticmethod
    def _signature(result: DecodeResult) -> bytes:
        return canonical_json_bytes(
            {
                "chunks": chunks_as_dict(result.chunks),
                "words": words_as_list(result.words),
            }
        )

    def roundtrip_check(
        self,
        *,
        version: int | None = None,
        chunk_id: str | None = None,
    ) -> dict[str, Any]:
        state = self.load_state() if version is None else self.load_version(version)
        if chunk_id is not None and chunk_id not in state.chunks:
            raise WorkbenchError(
                "E_NOT_FOUND", "Round-trip chunk does not exist", {"chunk_id": chunk_id}
            )
        chunk_ids = [chunk_id] if chunk_id else sorted(
            state.chunks, key=lambda cid: (state.chunks[cid].order, cid)
        )
        reports: list[dict[str, Any]] = []
        for target_chunk_id in chunk_ids:
            chunks = {target_chunk_id: state.chunks[target_chunk_id]}
            words = [word for word in state.words if word.chunk_id == target_chunk_id]
            validate_timeline(words, chunks, self.config)
            base_json = encode_json(chunks, words, self.config, version=state.version)
            base = decode_json(base_json, known_chunks=chunks)
            base_signature = self._signature(base)
            base_regions = timeline_regions(base.words, base.chunks)
            for fmt in ("srt", "webvtt"):
                subtitle = encode_text(fmt, chunks, words, self.config)
                decoded_subtitle = decode_text(
                    subtitle,
                    fmt,
                    known_chunks=chunks,
                    default_language=state.chunks[target_chunk_id].language,
                    default_chunk_id=target_chunk_id,
                    config=self.config,
                )
                validate_timeline(decoded_subtitle.words, decoded_subtitle.chunks, self.config)
                roundtrip_json = encode_json(
                    decoded_subtitle.chunks,
                    decoded_subtitle.words,
                    self.config,
                    version=state.version,
                )
                final = decode_json(roundtrip_json, known_chunks=chunks)
                final_signature = self._signature(final)
                final_regions = timeline_regions(final.words, final.chunks)
                exact = base_signature == final_signature
                regions_exact = base_regions == final_regions
                report = {
                    "chunk_id": target_chunk_id,
                    "chain": f"json->{fmt}->json",
                    "exact": exact,
                    "timeline_regions_exact": regions_exact,
                    "word_count": len(final.words),
                    "loss_report": decoded_subtitle.loss_report,
                    "gaps_and_overlaps": final_regions,
                }
                reports.append(report)
                if not exact or not regions_exact:
                    raise WorkbenchError(
                        "E_ROUNDTRIP_MISMATCH",
                        "Round-trip changed the timeline",
                        {
                            "chunk_id": target_chunk_id,
                            "chain": report["chain"],
                            "signature_equal": exact,
                            "timeline_regions_equal": regions_exact,
                            "loss_report": decoded_subtitle.loss_report,
                        },
                    )
        return {
            "ok": True,
            "version": state.version,
            "checks": reports,
            "all_exact": all(report["exact"] for report in reports),
        }

    def request_stop(self) -> dict[str, Any]:
        with self._lock():
            state = self._load_state_unlocked()
            if state is None:
                raise WorkbenchError(
                    "E_NOT_FOUND", "Workspace has not been initialized"
                )
            marker = {
                "status": "draining",
                "version": state.version,
                "snapshot_digest": state.snapshot_digest,
            }
            atomic_write_bytes(
                self.stopping_path,
                (json.dumps(marker, sort_keys=True) + "\n").encode("utf-8"),
            )
            return {"ok": True, **marker}

    def recover(self) -> dict[str, Any]:
        with self._lock():
            state = self._load_state_unlocked()
            if state is None:
                raise WorkbenchError(
                    "E_NOT_FOUND", "No consistent snapshot is available for recovery"
                )
            if self.stopping_path.exists():
                self.stopping_path.unlink()
            response = self._state_response(state)
            response["recovered"] = True
            response["replayed_versions"] = []
            return response

    def snapshot_info(self) -> dict[str, Any]:
        with self._lock():
            state = self._load_state_unlocked()
            if state is None:
                return {"ok": True, "initialized": False, "root": str(self.root)}
            archives = self._read_archives()
            return {
                "ok": True,
                "initialized": True,
                "version": state.version,
                "snapshot_digest": state.snapshot_digest,
                "input_fingerprint": state.input_fingerprint,
                "format_fingerprint": state.format_fingerprint,
                "timeline_digest": state.timeline_digest,
                "archived_versions": sorted(archives),
                "stopping": self.stopping_path.exists(),
            }
