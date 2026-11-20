from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .config import (
    CURRENT_SCHEMA_VERSION,
    HEADER_PREFIX,
    METADATA_PREFIX,
    WorkbenchConfig,
    canonical_json_bytes,
    digest_value,
)
from .errors import incompat_format
from .models import ChunkInfo, Word, chunks_as_dict, words_as_list


_TIMESTAMP_RANGE = re.compile(
    r"^\s*(?P<h1>\d{1,3}):(?P<m1>\d{2}):(?P<s1>\d{2})[,.](?P<ms1>\d{3})"
    r"\s*-->\s*"
    r"(?P<h2>\d{1,3}):(?P<m2>\d{2}):(?P<s2>\d{2})[,.](?P<ms2>\d{3})"
    r"(?:\s+.*)?$"
)


@dataclass
class DecodeResult:
    format: str
    words: list[Word]
    chunks: dict[str, ChunkInfo]
    migrations: list[dict[str, Any]] = field(default_factory=list)
    loss_report: list[dict[str, Any]] = field(default_factory=list)
    source_schema_version: int = CURRENT_SCHEMA_VERSION
    metadata: dict[str, Any] = field(default_factory=dict)


def _json_line(prefix: str, value: Mapping[str, Any]) -> str:
    return prefix + json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def format_timestamp(ms: int, separator: str) -> str:
    if ms < 0:
        raise ValueError("Timestamp milliseconds must not be negative")
    hours, remainder = divmod(ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}{separator}{millis:03d}"


def parse_timestamp(value: str) -> int:
    match = re.fullmatch(
        r"\s*(\d{1,3}):(\d{2}):(\d{2})[,.](\d{3})\s*", value
    )
    if not match:
        raise incompat_format("Invalid subtitle timestamp", timestamp=value)
    hours, minutes, seconds, millis = (int(part) for part in match.groups())
    return ((hours * 60 + minutes) * 60 + seconds) * 1000 + millis


def _chunk_header(chunks: Mapping[str, ChunkInfo], fmt: str, config: WorkbenchConfig) -> dict[str, Any]:
    relative = []
    for chunk in sorted(chunks.values(), key=lambda c: (c.order, c.chunk_id)):
        value = chunk.to_dict()
        relative.append(value)
    return {
        "schema_version": CURRENT_SCHEMA_VERSION,
        "format": fmt,
        "time_unit": "ms",
        "config_fingerprint": config.fingerprint(),
        "chunks": relative,
    }


def _cue_metadata(word: Word, chunk: ChunkInfo) -> dict[str, Any]:
    return {
        "entry_id": word.entry_id,
        "chunk_id": word.chunk_id,
        "confidence": word.confidence,
        "language": word.language,
        "source_version": word.source_version,
        "source_entry_ids": list(word.source_entry_ids),
        "merged_from_versions": list(word.merged_from_versions),
        "conflict_id": word.conflict_id,
        "chunk_duration_ms": chunk.duration_ms,
        "chunk_order": chunk.order,
        "chunk_language": chunk.language,
    }


def encode_json(
    chunks: Mapping[str, ChunkInfo],
    words: Sequence[Word],
    config: WorkbenchConfig = WorkbenchConfig(),
    *,
    version: int | None = None,
) -> str:
    ordered_words = sorted(
        words,
        key=lambda w: (
            chunks[w.chunk_id].order if w.chunk_id in chunks else 10**9,
            w.start_ms,
            w.end_ms,
            w.entry_id,
        ),
    )
    value: dict[str, Any] = {
        "format": "workbench-json",
        "schema_version": CURRENT_SCHEMA_VERSION,
        "time_unit": "ms",
        "config_fingerprint": config.fingerprint(),
        "chunks": list(chunks_as_dict(chunks).values()),
        "words": words_as_list(ordered_words),
        "timeline_digest": digest_value(
            {
                "chunks": chunks_as_dict(chunks),
                "words": words_as_list(ordered_words),
            }
        ),
    }
    if version is not None:
        value["source"] = {"workbench_version": version}
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
    ) + "\n"


def encode_srt(
    chunks: Mapping[str, ChunkInfo],
    words: Sequence[Word],
    config: WorkbenchConfig = WorkbenchConfig(),
) -> str:
    ordered = sorted(
        words,
        key=lambda w: (
            chunks[w.chunk_id].order if w.chunk_id in chunks else 10**9,
            w.start_ms,
            w.end_ms,
            w.entry_id,
        ),
    )
    lines = [_json_line(HEADER_PREFIX, _chunk_header(chunks, "srt", config)), ""]
    for index, word in enumerate(ordered, start=1):
        chunk = chunks[word.chunk_id]
        lines.extend(
            [
                str(index),
                f"{format_timestamp(word.start_ms, ',')} --> {format_timestamp(word.end_ms, ',')}",
                word.text,
                _json_line(config.srt_metadata_envelope, _cue_metadata(word, chunk)),
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def encode_webvtt(
    chunks: Mapping[str, ChunkInfo],
    words: Sequence[Word],
    config: WorkbenchConfig = WorkbenchConfig(),
) -> str:
    ordered = sorted(
        words,
        key=lambda w: (
            chunks[w.chunk_id].order if w.chunk_id in chunks else 10**9,
            w.start_ms,
            w.end_ms,
            w.entry_id,
        ),
    )
    lines = [
        "WEBVTT",
        "",
        _json_line(HEADER_PREFIX, _chunk_header(chunks, "webvtt", config)),
        "",
    ]
    for index, word in enumerate(ordered, start=1):
        chunk = chunks[word.chunk_id]
        lines.extend(
            [
                f"cue-{index}",
                f"{format_timestamp(word.start_ms, '.')} --> {format_timestamp(word.end_ms, '.')}",
                word.text,
                _json_line(config.vtt_metadata_envelope, _cue_metadata(word, chunk)),
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _parse_json_object(raw: str, *, format_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise incompat_format(
            "Input is not valid JSON",
            format=format_name,
            line=exc.lineno,
            column=exc.colno,
        ) from exc
    if not isinstance(value, dict):
        raise incompat_format("Top-level JSON must be an object", format=format_name)
    return value


def _as_int_ms(value: Any, *, seconds: bool, field_name: str) -> int:
    if isinstance(value, bool):
        raise incompat_format("Boolean is not a valid time", field=field_name)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise incompat_format("Time is not numeric", field=field_name, value=value) from exc
    if seconds:
        return int(round(number * 1000.0))
    return int(round(number))


def _chunk_from_value(
    value: Mapping[str, Any],
    *,
    known: Mapping[str, ChunkInfo] | None,
    default_language: str,
) -> ChunkInfo:
    chunk_id = str(
        value.get("chunk_id")
        or value.get("id")
        or value.get("audio")
        or value.get("audio_file")
        or value.get("file")
        or value.get("source")
        or ""
    )
    if not chunk_id and known and len(known) == 1:
        chunk_id = next(iter(known))
    if not chunk_id:
        raise incompat_format("Chunk id cannot be determined", value=value)
    base = known.get(chunk_id) if known else None
    duration_raw = value.get("duration_ms")
    if duration_raw is None and value.get("duration") is not None:
        duration_raw = value.get("duration")
    duration_ms = (
        _as_int_ms(duration_raw, seconds=False, field_name="duration")
        if duration_raw is not None
        else (base.duration_ms if base else 0)
    )
    path = str(value.get("path") or (base.path if base else ""))
    file = str(value.get("file") or value.get("audio_file") or (base.file if base else path))
    return ChunkInfo(
        chunk_id=chunk_id,
        order=int(value.get("order", base.order if base else 0)),
        file=file,
        path=path,
        duration_ms=duration_ms,
        language=str(
            value.get("language")
            or value.get("lang")
            or (base.language if base else default_language)
        ),
        reference_text=str(
            value.get("reference_text") or (base.reference_text if base else "")
        ),
        fingerprint=str(value.get("fingerprint") or (base.fingerprint if base else "")),
    )


def decode_json(
    raw: str,
    *,
    known_chunks: Mapping[str, ChunkInfo] | None = None,
    default_language: str = "und",
    default_chunk_id: str | None = None,
) -> DecodeResult:
    value = _parse_json_object(raw, format_name="json")
    schema_raw = value.get("schema_version", value.get("schemaVersion", 0))
    try:
        source_schema = int(schema_raw)
    except (TypeError, ValueError) as exc:
        raise incompat_format(
            "schema_version must be an integer", schema_version=schema_raw
        ) from exc
    if source_schema > CURRENT_SCHEMA_VERSION:
        raise incompat_format(
            "JSON schema is newer than this workbench",
            source_schema_version=source_schema,
            supported_schema_version=CURRENT_SCHEMA_VERSION,
        )

    is_legacy = source_schema < CURRENT_SCHEMA_VERSION or "segments" in value
    rules: list[str] = []
    if is_legacy:
        rules.append(f"upgrade_schema_{source_schema}_to_{CURRENT_SCHEMA_VERSION}")

    chunks_value = value.get("chunks")
    chunks: dict[str, ChunkInfo] = {}
    if isinstance(chunks_value, dict):
        chunk_items: Iterable[Any] = chunks_value.values()
    elif isinstance(chunks_value, list):
        chunk_items = chunks_value
    else:
        chunk_items = []
    for item in chunk_items:
        if not isinstance(item, Mapping):
            raise incompat_format("Chunk entries must be objects")
        chunk = _chunk_from_value(
            item, known=known_chunks, default_language=default_language
        )
        chunks[chunk.chunk_id] = chunk

    raw_words = value.get("words", value.get("segments", value.get("entries")))
    if not isinstance(raw_words, list):
        raise incompat_format("JSON must contain a words or segments array")

    time_unit = str(value.get("time_unit", value.get("timeUnit", ""))).lower()
    time_unit_seconds = time_unit in {"s", "sec", "second", "seconds"}
    time_unit_ms = time_unit in {"", "ms", "millisecond", "milliseconds"}
    if not time_unit_seconds and not time_unit_ms:
        raise incompat_format("Unsupported time_unit", time_unit=time_unit)
    if is_legacy and not time_unit:
        time_unit_seconds = True
        rules.append("legacy_time_unit_seconds_to_milliseconds")
    if time_unit_seconds:
        rules.append("seconds_to_milliseconds")

    words: list[Word] = []
    for index, item in enumerate(raw_words):
        if not isinstance(item, Mapping):
            raise incompat_format("Word entries must be objects", entry_index=index)
        chunk_id = item.get("chunk_id", item.get("chunkId", item.get("audio", default_chunk_id)))
        if not chunk_id:
            if known_chunks and len(known_chunks) == 1:
                chunk_id = next(iter(known_chunks))
            elif len(chunks) == 1:
                chunk_id = next(iter(chunks))
            elif value.get("chunk_id"):
                chunk_id = value["chunk_id"]
            else:
                raise incompat_format(
                    "Word chunk id cannot be determined",
                    entry_index=index,
                )
        chunk_id = str(chunk_id)
        if chunk_id not in chunks:
            base = known_chunks.get(chunk_id) if known_chunks else None
            if base is not None:
                chunks[chunk_id] = base
            else:
                chunks[chunk_id] = _chunk_from_value(
                    {
                        "chunk_id": chunk_id,
                        "duration_ms": value.get("duration_ms", 0),
                        "language": item.get("language", item.get("lang", default_language)),
                    },
                    known=known_chunks,
                    default_language=default_language,
                )
        if "start_ms" in item:
            start_ms = _as_int_ms(item["start_ms"], seconds=False, field_name="start_ms")
        elif "start" in item:
            start_ms = _as_int_ms(item["start"], seconds=time_unit_seconds, field_name="start")
        else:
            raise incompat_format("Word start time is missing", entry_index=index)
        if "end_ms" in item:
            end_ms = _as_int_ms(item["end_ms"], seconds=False, field_name="end_ms")
        elif "end" in item:
            end_ms = _as_int_ms(item["end"], seconds=time_unit_seconds, field_name="end")
        else:
            raise incompat_format("Word end time is missing", entry_index=index)
        text = item.get("text", item.get("word", item.get("content")))
        if text is None:
            raise incompat_format("Word text is missing", entry_index=index)
        confidence_raw = item.get("confidence")
        if confidence_raw is None:
            confidence = 0.5
            if "missing_confidence_default_0.5" not in rules:
                rules.append("missing_confidence_default_0.5")
        else:
            try:
                confidence = float(confidence_raw)
            except (TypeError, ValueError) as exc:
                raise incompat_format(
                    "confidence must be numeric", entry_index=index
                ) from exc
        language = str(
            item.get("language")
            or item.get("lang")
            or chunks[chunk_id].language
            or default_language
        )
        if not (item.get("language") or item.get("lang")):
            if "missing_language_use_chunk_or_default" not in rules:
                rules.append("missing_language_use_chunk_or_default")
        entry_id_raw = item.get("entry_id", item.get("id"))
        if not entry_id_raw:
            entry_id = f"mig-{chunk_id}-{index:05d}-{digest_value([str(text), start_ms, end_ms])[:10]}"
            if "missing_entry_id_generated" not in rules:
                rules.append("missing_entry_id_generated")
        else:
            entry_id = str(entry_id_raw)
        source_version = int(item.get("source_version", 0))
        source_entry_ids = tuple(str(v) for v in item.get("source_entry_ids", []))
        if source_schema < CURRENT_SCHEMA_VERSION and not source_entry_ids and entry_id_raw:
            source_entry_ids = (str(entry_id_raw),)
        words.append(
            Word(
                entry_id=entry_id,
                chunk_id=chunk_id,
                start_ms=start_ms,
                end_ms=end_ms,
                text=str(text),
                confidence=confidence,
                language=language,
                source_version=source_version,
                source_entry_ids=source_entry_ids,
                merged_from_versions=tuple(
                    int(v) for v in item.get("merged_from_versions", [])
                ),
                conflict_id=(
                    str(item["conflict_id"])
                    if item.get("conflict_id") is not None
                    else None
                ),
            )
        )

    if not chunks:
        raise incompat_format("No chunk metadata is available")
    for chunk in chunks.values():
        if chunk.duration_ms <= 0:
            maxima = [word.end_ms for word in words if word.chunk_id == chunk.chunk_id]
            if not maxima:
                raise incompat_format(
                    "Chunk duration is missing and cannot be inferred",
                    chunk_id=chunk.chunk_id,
                )
            replacement = ChunkInfo(
                chunk_id=chunk.chunk_id,
                order=chunk.order,
                file=chunk.file,
                path=chunk.path,
                duration_ms=max(maxima),
                language=chunk.language,
                reference_text=chunk.reference_text,
                fingerprint=chunk.fingerprint,
            )
            chunks[chunk.chunk_id] = replacement
            if "chunk_duration_inferred_from_words" not in rules:
                rules.append("chunk_duration_inferred_from_words")

    migrations: list[dict[str, Any]] = []
    if rules:
        migrations.append(
            {
                "from_schema_version": source_schema,
                "to_schema_version": CURRENT_SCHEMA_VERSION,
                "source_format": "json",
                "rules": rules,
                "entry_count": len(words),
            }
        )
    return DecodeResult(
        format="json",
        words=words,
        chunks=chunks,
        migrations=migrations,
        source_schema_version=source_schema,
        metadata={"config_fingerprint": value.get("config_fingerprint")},
    )


def _parse_metadata_line(line: str, prefix: str) -> dict[str, Any] | None:
    if not line.startswith(prefix):
        return None
    try:
        value = json.loads(line[len(prefix) :])
    except json.JSONDecodeError as exc:
        raise incompat_format(
            "Subtitle metadata envelope is not valid JSON",
            envelope=prefix,
        ) from exc
    if not isinstance(value, dict):
        raise incompat_format("Subtitle metadata envelope must be an object")
    return value


def _parse_subtitle_blocks(raw: str, fmt: str) -> tuple[dict[str, Any], list[list[str]]]:
    text = raw.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    header: dict[str, Any] = {}
    kept: list[str] = []
    for line in lines:
        if line.startswith(HEADER_PREFIX):
            header = _parse_metadata_line(line, HEADER_PREFIX) or {}
            continue
        kept.append(line)
    if fmt == "webvtt":
        while kept and not kept[0].strip():
            kept.pop(0)
        if not kept or not kept[0].strip().upper().startswith("WEBVTT"):
            raise incompat_format("WebVTT input must start with WEBVTT")
        kept = kept[1:]
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in kept:
        if not line.strip():
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(line.rstrip("\n"))
    if current:
        blocks.append(current)
    return header, blocks


def decode_subtitle(
    raw: str,
    fmt: str,
    *,
    known_chunks: Mapping[str, ChunkInfo] | None = None,
    default_language: str = "und",
    default_chunk_id: str | None = None,
    config: WorkbenchConfig = WorkbenchConfig(),
    loss_on_missing_metadata: bool = True,
) -> DecodeResult:
    if fmt not in {"srt", "webvtt"}:
        raise incompat_format("Unsupported subtitle format", format=fmt)
    prefix = config.srt_metadata_envelope if fmt == "srt" else config.vtt_metadata_envelope
    header, blocks = _parse_subtitle_blocks(raw, fmt)
    chunks: dict[str, ChunkInfo] = {}
    header_chunks = header.get("chunks", []) if isinstance(header, dict) else []
    if isinstance(header_chunks, dict):
        header_chunks = list(header_chunks.values())
    if isinstance(header_chunks, list):
        for item in header_chunks:
            if isinstance(item, Mapping):
                chunk = _chunk_from_value(
                    item, known=known_chunks, default_language=default_language
                )
                chunks[chunk.chunk_id] = chunk
    if known_chunks:
        for chunk_id, chunk in known_chunks.items():
            chunks.setdefault(chunk_id, chunk)

    words: list[Word] = []
    loss_report: list[dict[str, Any]] = []
    inferred_durations: dict[str, int] = {}
    for block_index, block in enumerate(blocks):
        meta: dict[str, Any] = {}
        content_lines: list[str] = []
        time_line_index: int | None = None
        for line_index, line in enumerate(block):
            parsed_meta = _parse_metadata_line(line, prefix)
            if parsed_meta is not None:
                meta.update(parsed_meta)
            elif _TIMESTAMP_RANGE.match(line):
                time_line_index = line_index
        if time_line_index is None:
            raise incompat_format(
                "Subtitle cue has no valid timestamp range",
                format=fmt,
                cue_index=block_index,
            )
        match = _TIMESTAMP_RANGE.match(block[time_line_index])
        assert match is not None
        start_ms = (
            int(match.group("h1")) * 3_600_000
            + int(match.group("m1")) * 60_000
            + int(match.group("s1")) * 1000
            + int(match.group("ms1"))
        )
        end_ms = (
            int(match.group("h2")) * 3_600_000
            + int(match.group("m2")) * 60_000
            + int(match.group("s2")) * 1000
            + int(match.group("ms2"))
        )
        for line in block[time_line_index + 1 :]:
            if _parse_metadata_line(line, prefix) is None:
                content_lines.append(line)
        chunk_id = str(
            meta.get("chunk_id")
            or default_chunk_id
            or (next(iter(known_chunks)) if known_chunks and len(known_chunks) == 1 else "")
            or (next(iter(chunks)) if len(chunks) == 1 else "")
        )
        if not chunk_id:
            chunk_id = "subtitle-inferred"
        if chunk_id not in chunks:
            chunks[chunk_id] = _chunk_from_value(
                {
                    "chunk_id": chunk_id,
                    "duration_ms": meta.get("chunk_duration_ms", 0),
                    "language": meta.get("chunk_language", default_language),
                    "order": meta.get("chunk_order", len(chunks) + 1),
                },
                known=known_chunks,
                default_language=default_language,
            )
        cue_text = "\n".join(content_lines)
        confidence_raw = meta.get("confidence")
        if confidence_raw is None:
            confidence = 0.5
            if loss_on_missing_metadata:
                loss_report.append(
                    {
                        "field": "confidence",
                        "cue_index": block_index,
                        "chunk_id": chunk_id,
                        "reason": "subtitle_metadata_absent",
                        "default": 0.5,
                    }
                )
        else:
            confidence = float(confidence_raw)
        language = str(
            meta.get("language")
            or (chunks[chunk_id].language if chunk_id in chunks else "")
            or default_language
        )
        if not meta.get("language") and loss_on_missing_metadata:
            loss_report.append(
                {
                    "field": "language",
                    "cue_index": block_index,
                    "chunk_id": chunk_id,
                    "reason": "subtitle_metadata_absent",
                    "default": language,
                }
            )
        entry_id = str(
            meta.get("entry_id")
            or f"cue-{chunk_id}-{block_index:05d}-{digest_value([cue_text, start_ms, end_ms])[:10]}"
        )
        if not meta.get("entry_id") and loss_on_missing_metadata:
            loss_report.append(
                {
                    "field": "entry_id",
                    "cue_index": block_index,
                    "chunk_id": chunk_id,
                    "reason": "subtitle_metadata_absent",
                    "generated": entry_id,
                }
            )
        words.append(
            Word(
                entry_id=entry_id,
                chunk_id=chunk_id,
                start_ms=start_ms,
                end_ms=end_ms,
                text=cue_text,
                confidence=confidence,
                language=language,
                source_version=int(meta.get("source_version", 0)),
                source_entry_ids=tuple(
                    str(value) for value in meta.get("source_entry_ids", [])
                ),
                merged_from_versions=tuple(
                    int(value) for value in meta.get("merged_from_versions", [])
                ),
                conflict_id=(
                    str(meta["conflict_id"])
                    if meta.get("conflict_id") is not None
                    else None
                ),
            )
        )
        inferred_durations[chunk_id] = max(
            inferred_durations.get(chunk_id, 0), end_ms
        )

    if not words and not chunks:
        raise incompat_format("Subtitle input contains no cues or chunk metadata", format=fmt)
    if not chunks:
        raise incompat_format("Subtitle chunk metadata is unavailable", format=fmt)
    for chunk_id, chunk in list(chunks.items()):
        if chunk.duration_ms <= 0:
            chunks[chunk_id] = ChunkInfo(
                chunk_id=chunk.chunk_id,
                order=chunk.order,
                file=chunk.file,
                path=chunk.path,
                duration_ms=inferred_durations.get(chunk_id, 0),
                language=chunk.language,
                reference_text=chunk.reference_text,
                fingerprint=chunk.fingerprint,
            )
        if chunks[chunk_id].duration_ms <= 0:
            raise incompat_format(
                "Subtitle chunk duration is missing and cannot be inferred",
                chunk_id=chunk_id,
            )
    return DecodeResult(
        format=fmt,
        words=words,
        chunks=chunks,
        loss_report=loss_report,
        source_schema_version=CURRENT_SCHEMA_VERSION,
        metadata={
            "config_fingerprint": header.get("config_fingerprint"),
            "header_format": header.get("format"),
        },
    )


def decode_text(
    raw: str,
    fmt: str,
    *,
    known_chunks: Mapping[str, ChunkInfo] | None = None,
    default_language: str = "und",
    default_chunk_id: str | None = None,
    config: WorkbenchConfig = WorkbenchConfig(),
) -> DecodeResult:
    normalized = fmt.lower()
    if normalized == "srt":
        return decode_subtitle(
            raw,
            "srt",
            known_chunks=known_chunks,
            default_language=default_language,
            default_chunk_id=default_chunk_id,
            config=config,
        )
    if normalized in {"vtt", "webvtt"}:
        return decode_subtitle(
            raw,
            "webvtt",
            known_chunks=known_chunks,
            default_language=default_language,
            default_chunk_id=default_chunk_id,
            config=config,
        )
    if normalized == "json":
        return decode_json(
            raw,
            known_chunks=known_chunks,
            default_language=default_language,
            default_chunk_id=default_chunk_id,
        )
    raise incompat_format("Unknown transcript format", format=fmt)


def encode_text(
    fmt: str,
    chunks: Mapping[str, ChunkInfo],
    words: Sequence[Word],
    config: WorkbenchConfig = WorkbenchConfig(),
    *,
    version: int | None = None,
) -> str:
    normalized = fmt.lower()
    if normalized == "json":
        return encode_json(chunks, words, config, version=version)
    if normalized == "srt":
        return encode_srt(chunks, words, config)
    if normalized in {"vtt", "webvtt"}:
        return encode_webvtt(chunks, words, config)
    raise incompat_format("Unknown transcript format", format=fmt)


def canonical_decode_signature(result: DecodeResult) -> bytes:
    """Signature used by round-trip checks, including provenance metadata."""

    return canonical_json_bytes(
        {
            "chunks": chunks_as_dict(result.chunks),
            "words": words_as_list(result.words),
        }
    )
