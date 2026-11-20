from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

from .errors import BusinessError
from .models import (
    MAX_PRIORITY,
    MAX_WEIGHT,
    MAX_WINDOW,
    ConnectionConfig,
    FrameSpec,
    FrameType,
    SampleSpec,
)


FRAME_TYPES = {item.value for item in FrameType}
KNOWN_FLAG_BITS = {
    0x01: "END_STREAM",
    0x04: "END_HEADERS",
    0x08: "PADDED",
    0x20: "PRIORITY",
}


def _invalid(
    message: str,
    connection_id: str | None,
    frame_index: int | None = None,
    **context: Any,
) -> BusinessError:
    details = dict(context)
    if frame_index is not None:
        details["frame_index"] = frame_index
    return BusinessError(
        code="E_FRAME_INVALID",
        message=message,
        connection_id=connection_id,
        context=details,
    )


def _as_int(
    value: Any,
    *,
    field: str,
    connection_id: str | None,
    frame_index: int | None = None,
    default: int | None = None,
) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(
            f"{field} must be an integer",
            connection_id,
            frame_index,
            field=field,
            value=value,
        )
    return value


def _as_text(value: Any, *, field: str, connection_id: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(
            f"{field} must be a non-empty string",
            connection_id,
            field=field,
            value=value,
        )
    return value.strip()


def _normalize_flags(
    value: Any,
    *,
    frame_type: FrameType,
    connection_id: str | None,
    frame_index: int,
) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, int) and not isinstance(value, bool):
        flags = {name for bit, name in KNOWN_FLAG_BITS.items() if value & bit}
        if frame_type is FrameType.SETTINGS and value & 0x01:
            flags.discard("END_STREAM")
            flags.add("ACK")
        if value & ~sum(KNOWN_FLAG_BITS):
            raise _invalid(
                "flags integer contains unsupported bits",
                connection_id,
                frame_index,
                flags=value,
            )
        return frozenset(flags)
    if isinstance(value, str):
        parts: Iterable[str] = re.split(r"[\s,|]+", value)
    elif isinstance(value, (list, tuple, set)):
        expanded: list[str] = []
        for item in value:
            if isinstance(item, int) and not isinstance(item, bool):
                expanded.extend(
                    name for bit, name in KNOWN_FLAG_BITS.items() if item & bit
                )
            else:
                expanded.append(str(item))
        parts = expanded
    else:
        raise _invalid(
            "flags must be a string, list, set, or integer mask",
            connection_id,
            frame_index,
            flags=value,
        )
    flags = {part.strip().upper() for part in parts if part and part.strip()}
    aliases = {
        "ENDSTREAM": "END_STREAM",
        "ES": "END_STREAM",
        "ENDHEADERS": "END_HEADERS",
        "EH": "END_HEADERS",
        "CLOSE": "CONNECTION_CLOSE",
    }
    return frozenset(aliases.get(flag, flag) for flag in flags)


def _first_present(mapping: dict[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    return default


def _normalize_config(raw: Any, connection_id: str | None) -> ConnectionConfig | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise _invalid("config must be an object", connection_id, field="config")

    def get_int(name: str, default: int) -> int:
        value = raw.get(name, default)
        return _as_int(value, field=f"config.{name}", connection_id=connection_id)

    config = ConnectionConfig(
        connection_window=get_int("connection_window", 65_535),
        stream_window=get_int("stream_window", 65_535),
        round_budget=get_int("round_budget", 16_384),
        candidate_threshold=get_int("candidate_threshold", 256),
        candidate_limit=get_int("candidate_limit", 64),
        priority_scale=get_int("priority_scale", 4),
        max_rounds=get_int("max_rounds", 100_000),
    )
    if not 0 <= config.connection_window <= MAX_WINDOW:
        raise _invalid(
            "config.connection_window is outside the HTTP/2 window range",
            connection_id,
            field="config.connection_window",
        )
    if not 0 <= config.stream_window <= MAX_WINDOW:
        raise _invalid(
            "config.stream_window is outside the HTTP/2 window range",
            connection_id,
            field="config.stream_window",
        )
    if not 1 <= config.round_budget <= MAX_WINDOW:
        raise _invalid(
            "config.round_budget must be between 1 and 2^31-1",
            connection_id,
            field="config.round_budget",
        )
    if not 1 <= config.candidate_threshold <= 1_000_000:
        raise _invalid(
            "config.candidate_threshold must be positive",
            connection_id,
            field="config.candidate_threshold",
        )
    if not 1 <= config.candidate_limit <= 1_000_000:
        raise _invalid(
            "config.candidate_limit must be positive",
            connection_id,
            field="config.candidate_limit",
        )
    if not 2 <= config.priority_scale <= 256:
        raise _invalid(
            "config.priority_scale must be between 2 and 256",
            connection_id,
            field="config.priority_scale",
        )
    if not 1 <= config.max_rounds <= 10_000_000:
        raise _invalid(
            "config.max_rounds must be positive",
            connection_id,
            field="config.max_rounds",
        )
    if config.candidate_limit > config.candidate_threshold:
        config = ConnectionConfig(
            connection_window=config.connection_window,
            stream_window=config.stream_window,
            round_budget=config.round_budget,
            candidate_threshold=config.candidate_threshold,
            candidate_limit=config.candidate_threshold,
            priority_scale=config.priority_scale,
            max_rounds=config.max_rounds,
        )
    return config


def normalize_sample(raw: dict[str, Any]) -> SampleSpec:
    if not isinstance(raw, dict):
        raise _invalid("sample body must be a JSON object", None)
    sample_id = _as_text(raw.get("sample_id"), field="sample_id", connection_id=None)
    if len(sample_id) > 160:
        raise _invalid("sample_id is too long", None, field="sample_id")

    raw_frames = raw.get("frames")
    if not isinstance(raw_frames, list) or not raw_frames:
        raise _invalid("frames must be a non-empty array", None, field="frames")

    top_connection = raw.get("connection_id")
    if top_connection is not None:
        top_connection = _as_text(
            top_connection, field="connection_id", connection_id=None
        )

    frames: list[FrameSpec] = []
    observed_connections: set[str] = set()
    for index, item in enumerate(raw_frames):
        if not isinstance(item, dict):
            raise _invalid("each frame must be an object", top_connection, index)
        frame_conn_raw = _first_present(
            item, ("connection_id", "connectionId"), None
        )
        frame_conn = None
        if frame_conn_raw is not None:
            frame_conn = _as_text(
                frame_conn_raw, field="frame.connection_id", connection_id=top_connection
            )
            observed_connections.add(frame_conn)
        effective_conn = top_connection or frame_conn
        if effective_conn is None:
            raise _invalid(
                "connection_id is required at sample or frame level",
                None,
                index,
                field="connection_id",
            )
        if top_connection is not None and frame_conn is not None and frame_conn != top_connection:
            raise _invalid(
                "frame connection_id does not match sample connection_id",
                top_connection,
                index,
                frame_connection_id=frame_conn,
            )

        type_raw = _first_present(item, ("frame_type", "frameType", "type"), None)
        if type_raw is None:
            raise _invalid("frame_type is required", effective_conn, index)
        frame_type_text = str(type_raw).strip().upper()
        if frame_type_text not in FRAME_TYPES:
            raise _invalid(
                "unsupported frame type",
                effective_conn,
                index,
                frame_type=type_raw,
                allowed=sorted(FRAME_TYPES),
            )
        frame_type = FrameType(frame_type_text)

        stream_id = _as_int(
            _first_present(item, ("stream_id", "streamId")),
            field="stream_id",
            connection_id=effective_conn,
            frame_index=index,
        )
        if stream_id < 0:
            raise _invalid(
                "stream_id must not be negative",
                effective_conn,
                index,
                stream_id=stream_id,
            )
        if frame_type in {FrameType.HEADERS, FrameType.DATA, FrameType.RST_STREAM} and stream_id == 0:
            raise _invalid(
                f"{frame_type.value} requires a non-zero stream_id",
                effective_conn,
                index,
                stream_id=stream_id,
            )
        if frame_type is FrameType.SETTINGS and stream_id != 0:
            raise _invalid(
                "SETTINGS requires stream_id 0",
                effective_conn,
                index,
                stream_id=stream_id,
            )

        payload_bytes = _as_int(
            _first_present(item, ("payload_bytes", "payloadBytes", "payload_length", "length"), 0),
            field="payload_bytes",
            connection_id=effective_conn,
            frame_index=index,
            default=0,
        )
        if payload_bytes < 0:
            raise _invalid(
                "payload_bytes must not be negative",
                effective_conn,
                index,
                payload_bytes=payload_bytes,
            )
        if payload_bytes > MAX_WINDOW:
            raise _invalid(
                "payload_bytes exceeds 2^31-1",
                effective_conn,
                index,
                payload_bytes=payload_bytes,
            )

        window_increment = _as_int(
            _first_present(item, ("window_increment", "windowIncrement", "increment"), 0),
            field="window_increment",
            connection_id=effective_conn,
            frame_index=index,
            default=0,
        )
        if window_increment < 0:
            raise _invalid(
                "window_increment must not be negative",
                effective_conn,
                index,
                window_increment=window_increment,
            )
        if window_increment > MAX_WINDOW:
            raise _invalid(
                "window_increment exceeds 2^31-1",
                effective_conn,
                index,
                window_increment=window_increment,
            )

        direction = str(item.get("direction", "local")).strip().lower()
        if direction not in {"local", "remote"}:
            raise _invalid(
                "direction must be local or remote",
                effective_conn,
                index,
                direction=direction,
            )
        if frame_type is FrameType.DATA and direction != "local":
            raise _invalid(
                "DATA records in this workbench represent local outbound data",
                effective_conn,
                index,
                direction=direction,
            )

        weight = item.get("weight")
        if weight is not None:
            weight = _as_int(
                weight,
                field="weight",
                connection_id=effective_conn,
                frame_index=index,
            )
            if not 1 <= weight <= MAX_WEIGHT:
                raise _invalid(
                    "weight must be between 1 and 256",
                    effective_conn,
                    index,
                    weight=weight,
                )
        priority = item.get("priority")
        if priority is not None:
            priority = _as_int(
                priority,
                field="priority",
                connection_id=effective_conn,
                frame_index=index,
            )
            if not 0 <= priority <= MAX_PRIORITY:
                raise _invalid(
                    "priority must be between 0 and 255",
                    effective_conn,
                    index,
                    priority=priority,
                )

        frames.append(
            FrameSpec(
                index=index,
                connection_id=effective_conn,
                stream_id=stream_id,
                frame_type=frame_type,
                flags=_normalize_flags(
                    item.get("flags", []),
                    frame_type=frame_type,
                    connection_id=effective_conn,
                    frame_index=index,
                ),
                payload_bytes=payload_bytes,
                window_increment=window_increment,
                direction=direction,
                weight=weight,
                priority=priority,
            )
        )

    if observed_connections and len(observed_connections) > 1:
        raise _invalid(
            "a sample may target only one connection",
            top_connection,
            connection_ids=sorted(observed_connections),
        )
    connection_id = top_connection
    if connection_id is None:
        connection_id = next(iter(observed_connections), None)
    if connection_id is None:
        raise _invalid("connection_id could not be resolved", None)

    config = _normalize_config(raw.get("config"), connection_id)
    batch_size = _as_int(
        raw.get("batch_size", 128),
        field="batch_size",
        connection_id=connection_id,
        default=128,
    )
    if not 1 <= batch_size <= 1_000_000:
        raise _invalid(
            "batch_size must be between 1 and 1,000,000",
            connection_id,
            field="batch_size",
        )

    canonical = {
        "sample_id": sample_id,
        "connection_id": connection_id,
        "frames": [
            {
                "index": frame.index,
                "connection_id": frame.connection_id,
                "stream_id": frame.stream_id,
                "frame_type": frame.frame_type.value,
                "flags": sorted(frame.flags),
                "payload_bytes": frame.payload_bytes,
                "window_increment": frame.window_increment,
                "direction": frame.direction,
                "weight": frame.weight,
                "priority": frame.priority,
            }
            for frame in frames
        ],
        "config": (config or ConnectionConfig()).to_dict(),
    }
    canonical_json = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    content_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return SampleSpec(
        sample_id=sample_id,
        connection_id=connection_id,
        frames=tuple(frames),
        batch_size=batch_size,
        config=config,
        content_hash=content_hash,
        canonical_json=canonical_json,
    )
