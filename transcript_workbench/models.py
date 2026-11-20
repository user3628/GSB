from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .config import CURRENT_SCHEMA_VERSION, WorkbenchConfig, digest_value
from .errors import timeline_error


@dataclass(frozen=True)
class Word:
    """A word-level timeline entry. All times are integer milliseconds."""

    entry_id: str
    chunk_id: str
    start_ms: int
    end_ms: int
    text: str
    confidence: float
    language: str
    source_version: int
    source_entry_ids: tuple[str, ...] = ()
    merged_from_versions: tuple[int, ...] = ()
    conflict_id: str | None = None

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "chunk_id": self.chunk_id,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "text": self.text,
            "confidence": self.confidence,
            "language": self.language,
            "source_version": self.source_version,
            "source_entry_ids": list(self.source_entry_ids),
            "merged_from_versions": list(self.merged_from_versions),
            "conflict_id": self.conflict_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Word":
        return cls(
            entry_id=str(value["entry_id"]),
            chunk_id=str(value["chunk_id"]),
            start_ms=int(value["start_ms"]),
            end_ms=int(value["end_ms"]),
            text=str(value["text"]),
            confidence=float(value["confidence"]),
            language=str(value["language"]),
            source_version=int(value.get("source_version", 0)),
            source_entry_ids=tuple(str(v) for v in value.get("source_entry_ids", [])),
            merged_from_versions=tuple(
                int(v) for v in value.get("merged_from_versions", [])
            ),
            conflict_id=(
                str(value["conflict_id"])
                if value.get("conflict_id") is not None
                else None
            ),
        )

    def semantic_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "chunk_id": self.chunk_id,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "text": self.text,
            "confidence": self.confidence,
            "language": self.language,
        }

    def semantic_key(self) -> tuple[Any, ...]:
        return (
            self.entry_id,
            self.chunk_id,
            self.start_ms,
            self.end_ms,
            self.text,
            self.confidence,
            self.language,
        )


@dataclass(frozen=True)
class ChunkInfo:
    chunk_id: str
    order: int
    file: str
    path: str
    duration_ms: int
    language: str
    reference_text: str
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "order": self.order,
            "file": self.file,
            "path": self.path,
            "duration_ms": self.duration_ms,
            "language": self.language,
            "reference_text": self.reference_text,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ChunkInfo":
        return cls(
            chunk_id=str(value["chunk_id"]),
            order=int(value["order"]),
            file=str(value.get("file", "")),
            path=str(value.get("path", "")),
            duration_ms=int(value["duration_ms"]),
            language=str(value.get("language", "")),
            reference_text=str(value.get("reference_text", "")),
            fingerprint=str(value.get("fingerprint", "")),
        )


def chunks_as_dict(
    chunks: Mapping[str, ChunkInfo] | Iterable[ChunkInfo],
) -> dict[str, Any]:
    if isinstance(chunks, Mapping):
        values = chunks.values()
    else:
        values = chunks
    return {
        chunk.chunk_id: chunk.to_dict()
        for chunk in sorted(values, key=lambda c: (c.order, c.chunk_id))
    }


def words_as_list(words: Iterable[Word]) -> list[dict[str, Any]]:
    return [word.to_dict() for word in words]


def timeline_digest(
    chunks: Mapping[str, ChunkInfo],
    words: Sequence[Word],
) -> str:
    return digest_value(
        {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "chunks": chunks_as_dict(chunks),
            "words": words_as_list(words),
        }
    )


def sorted_words(words: Iterable[Word], chunks: Mapping[str, ChunkInfo]) -> list[Word]:
    return sorted(
        words,
        key=lambda w: (
            chunks[w.chunk_id].order if w.chunk_id in chunks else 10**9,
            w.start_ms,
            w.end_ms,
            w.entry_id,
        ),
    )


def _validate_chunk_words(
    chunk: ChunkInfo,
    words: Sequence[Word],
    config: WorkbenchConfig,
) -> None:
    previous: Word | None = None
    previous_index: int | None = None
    seen_ids: set[str] = set()
    for index, word in enumerate(words):
        if word.entry_id in seen_ids:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="duplicate_entry_id",
                message=f"Duplicate entry id {word.entry_id!r} in {chunk.chunk_id}",
            )
        seen_ids.add(word.entry_id)
        if not isinstance(word.start_ms, int) or not isinstance(word.end_ms, int):
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="non_integer_time",
                message="Word times must be integer milliseconds",
            )
        if word.start_ms < 0:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="negative_start",
                message="Word start_ms must not be negative",
                start_ms=word.start_ms,
            )
        if word.end_ms < word.start_ms:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="end_before_start",
                message="Word end_ms must not be earlier than start_ms",
                start_ms=word.start_ms,
                end_ms=word.end_ms,
            )
        if word.end_ms == word.start_ms:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="zero_length",
                message="Zero-length words are not allowed",
                start_ms=word.start_ms,
                end_ms=word.end_ms,
            )
        if word.end_ms > chunk.duration_ms:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="end_beyond_chunk",
                message="Word end_ms exceeds the chunk duration",
                end_ms=word.end_ms,
                duration_ms=chunk.duration_ms,
            )
        if not word.language:
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="missing_language",
                message="Every word must carry a language tag",
            )
        if not (0.0 <= word.confidence <= 1.0):
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="confidence_out_of_range",
                message="Word confidence must be between 0 and 1",
                confidence=word.confidence,
            )
        if previous is not None:
            if word.start_ms < previous.start_ms:
                raise timeline_error(
                    chunk_id=chunk.chunk_id,
                    entry_index=index,
                    entry_id=word.entry_id,
                    violation="out_of_order",
                    message="Words must be ordered by start_ms",
                    previous_entry_index=previous_index,
                    previous_entry_id=previous.entry_id,
                )
            if word.start_ms < previous.end_ms:
                raise timeline_error(
                    chunk_id=chunk.chunk_id,
                    entry_index=index,
                    entry_id=word.entry_id,
                    violation="overlap",
                    message="Word intervals must not overlap",
                    previous_entry_index=previous_index,
                    previous_entry_id=previous.entry_id,
                    previous_end_ms=previous.end_ms,
                    start_ms=word.start_ms,
                )
            gap_ms = word.start_ms - previous.end_ms
            if gap_ms > config.max_gap_ms:
                raise timeline_error(
                    chunk_id=chunk.chunk_id,
                    entry_index=index,
                    entry_id=word.entry_id,
                    violation="gap_exceeds_tolerance",
                    message="Idle gap exceeds the configured tolerance",
                    gap_ms=gap_ms,
                    max_gap_ms=config.max_gap_ms,
                    previous_entry_index=previous_index,
                    previous_entry_id=previous.entry_id,
                )
        previous = word
        previous_index = index


def validate_timeline(
    words: Sequence[Word],
    chunks: Mapping[str, ChunkInfo],
    config: WorkbenchConfig = WorkbenchConfig(),
) -> None:
    unknown = [word for word in words if word.chunk_id not in chunks]
    if unknown:
        first = unknown[0]
        raise timeline_error(
            chunk_id=first.chunk_id,
            entry_index=0,
            entry_id=first.entry_id,
            violation="unknown_chunk",
            message="Timeline references an unknown chunk",
        )
    all_ids: set[str] = set()
    for word in words:
        if word.entry_id in all_ids:
            chunk = chunks[word.chunk_id]
            chunk_words = [w for w in words if w.chunk_id == chunk.chunk_id]
            index = next(
                i for i, candidate in enumerate(chunk_words) if candidate is word
            )
            raise timeline_error(
                chunk_id=chunk.chunk_id,
                entry_index=index,
                entry_id=word.entry_id,
                violation="duplicate_entry_id",
                message="Entry ids must be unique across the workspace",
            )
        all_ids.add(word.entry_id)

    for chunk in sorted(chunks.values(), key=lambda c: (c.order, c.chunk_id)):
        chunk_words = sorted(
            (word for word in words if word.chunk_id == chunk.chunk_id),
            key=lambda w: (w.start_ms, w.end_ms, w.entry_id),
        )
        _validate_chunk_words(chunk, chunk_words, config)


def timeline_regions(
    words: Sequence[Word],
    chunks: Mapping[str, ChunkInfo],
) -> dict[str, list[dict[str, int]]]:
    result: dict[str, list[dict[str, int]]] = {}
    for chunk_id in sorted(chunks, key=lambda cid: (chunks[cid].order, cid)):
        chunk_words = sorted(
            (word for word in words if word.chunk_id == chunk_id),
            key=lambda w: (w.start_ms, w.end_ms, w.entry_id),
        )
        gaps: list[dict[str, int]] = []
        overlaps: list[dict[str, int]] = []
        for left_index, (left, right) in enumerate(zip(chunk_words, chunk_words[1:])):
            if right.start_ms > left.end_ms:
                gaps.append(
                    {
                        "after_entry_index": left_index,
                        "before_entry_index": left_index + 1,
                        "start_ms": left.end_ms,
                        "end_ms": right.start_ms,
                        "duration_ms": right.start_ms - left.end_ms,
                    }
                )
            elif right.start_ms < left.end_ms:
                overlaps.append(
                    {
                        "left_entry_index": left_index,
                        "right_entry_index": left_index + 1,
                        "overlap_ms": left.end_ms - right.start_ms,
                    }
                )
        result[chunk_id] = gaps + overlaps
    return result


def subset_words(words: Sequence[Word], chunk_id: str | None) -> list[Word]:
    if chunk_id is None:
        return list(words)
    return [word for word in words if word.chunk_id == chunk_id]


@dataclass
class WorkspaceState:
    version: int
    parent_version: int
    parent_snapshot_digest: str | None
    kind: str
    source_versions: list[int]
    chunks: dict[str, ChunkInfo]
    words: list[Word]
    history: list[dict[str, Any]] = field(default_factory=list)
    migrations: list[dict[str, Any]] = field(default_factory=list)
    merge_records: list[dict[str, Any]] = field(default_factory=list)
    input_fingerprint: str = ""
    format_fingerprint: str = ""
    timeline_digest: str = ""
    snapshot_digest: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def sorted_words(self) -> list[Word]:
        return sorted_words(self.words, self.chunks)

    def word_map(self) -> dict[str, Word]:
        return {word.entry_id: word for word in self.words}

    def history_summary(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "parent_version": self.parent_version,
            "kind": self.kind,
            "source_versions": list(self.source_versions),
            "words": len(self.words),
            "chunks": [
                self.chunks[cid].chunk_id
                for cid in sorted(
                    self.chunks, key=lambda c: (self.chunks[c].order, c)
                )
            ],
            "migration_count": len(self.migrations),
            "merge_record_count": len(self.merge_records),
            "timeline_digest": self.timeline_digest,
            "snapshot_digest": self.snapshot_digest,
        }
