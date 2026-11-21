from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .audio import inspect_wav
from .canonical import digest_obj
from .config import ZONE_LABELS, ZONE_ORDER
from .errors import CabinError


def _import_error(message: str, **details: Any) -> CabinError:
    return CabinError("E_IMPORT_INVALID", message, details=details, http_status=422)


def _require_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _import_error(f"{field} must be an integer", field=field, value=value)
    if minimum is not None and value < minimum:
        raise _import_error(f"{field} must be at least {minimum}", field=field, value=value)
    return value


def _validate_intervals(
    intervals: Any,
    *,
    zone_id: str,
    duration_ms: int,
    shard_id: str,
    field: str,
) -> list[list[int]]:
    if not isinstance(intervals, list):
        raise _import_error(
            f"{field} must be an interval list",
            shard_id=shard_id,
            zone_id=zone_id,
        )
    normalized: list[list[int]] = []
    previous_end = 0
    for raw in intervals:
        if not isinstance(raw, (list, tuple)) or len(raw) != 2:
            raise _import_error(
                f"{field} entries must be [start_ms, end_ms]",
                shard_id=shard_id,
                zone_id=zone_id,
                value=raw,
            )
        start_ms = _require_int(raw[0], f"{field}.start_ms", minimum=0)
        end_ms = _require_int(raw[1], f"{field}.end_ms", minimum=0)
        if end_ms <= start_ms:
            raise _import_error(
                f"{field} interval must have positive duration",
                shard_id=shard_id,
                zone_id=zone_id,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        if end_ms > duration_ms:
            raise _import_error(
                f"{field} exceeds annotated duration bound",
                shard_id=shard_id,
                zone_id=zone_id,
                start_ms=start_ms,
                end_ms=end_ms,
                duration_upper_bound_ms=duration_ms,
            )
        if start_ms < previous_end:
            raise _import_error(
                f"{field} intervals must be sorted and non-overlapping",
                shard_id=shard_id,
                zone_id=zone_id,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        normalized.append([start_ms, end_ms])
        previous_end = end_ms
    return normalized


def _normalize_hit(
    raw: Any,
    *,
    shard_id: str,
    zone_durations: dict[str, int],
    known_zones: set[str],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _import_error("word_hits entries must be objects", shard_id=shard_id)
    zone_id = raw.get("zone_id")
    if zone_id not in known_zones:
        raise _import_error(
            "word hit references an unknown zone",
            shard_id=shard_id,
            zone_id=zone_id,
        )
    word = raw.get("word")
    if not isinstance(word, str) or not word.strip():
        raise _import_error(
            "word hit word must be non-empty",
            shard_id=shard_id,
            zone_id=zone_id,
        )
    start_ms = _require_int(raw.get("start_ms"), "word_hits.start_ms", minimum=0)
    end_ms = _require_int(raw.get("end_ms"), "word_hits.end_ms", minimum=0)
    duration_ms = zone_durations[zone_id]
    if end_ms <= start_ms or end_ms > duration_ms:
        raise _import_error(
            "word hit interval is outside the annotated duration",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=start_ms,
            end_ms=end_ms,
            duration_upper_bound_ms=duration_ms,
        )
    strength_raw = raw.get("strength")
    if isinstance(strength_raw, bool) or not isinstance(strength_raw, (int, float)):
        raise _import_error(
            "word hit strength must be numeric",
            shard_id=shard_id,
            zone_id=zone_id,
        )
    strength = float(strength_raw)
    if not 0.0 <= strength <= 1.0:
        raise _import_error(
            "word hit strength must be within [0, 1]",
            shard_id=shard_id,
            zone_id=zone_id,
            strength=strength,
        )
    identity = {
        "shard_id": shard_id,
        "zone_id": zone_id,
        "word": word.strip(),
        "start_ms": start_ms,
        "end_ms": end_ms,
    }
    return {
        "hit_id": "hit-" + digest_obj(identity)[:16],
        **identity,
        "strength": round(strength, 9),
    }


def load_manifest(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).expanduser().resolve()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise _import_error("manifest does not exist", manifest_path=str(path)) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise _import_error(
            "manifest is not valid JSON",
            manifest_path=str(path),
            reason=str(exc),
        ) from exc
    if not isinstance(raw, dict):
        raise _import_error("manifest root must be an object", manifest_path=str(path))
    if _require_int(raw.get("schema_version"), "schema_version", minimum=1) != 1:
        raise _import_error("unsupported manifest schema_version")
    sample_rate = _require_int(raw.get("sample_rate"), "sample_rate", minimum=1)
    if sample_rate != 16000:
        raise _import_error("sample_rate must be 16000", sample_rate=sample_rate)
    duration_bound = _require_int(
        raw.get("duration_upper_bound_ms"),
        "duration_upper_bound_ms",
        minimum=1,
    )
    shards_raw = raw.get("shards")
    if not isinstance(shards_raw, list) or not shards_raw:
        raise _import_error("manifest must contain at least one shard")
    shards: list[dict[str, Any]] = []
    seen_shards: set[str] = set()
    for shard_raw in shards_raw:
        if not isinstance(shard_raw, dict):
            raise _import_error("shard entries must be objects")
        shard_id = shard_raw.get("shard_id")
        if not isinstance(shard_id, str) or not shard_id.strip():
            raise _import_error("shard_id must be a non-empty string")
        if shard_id in seen_shards:
            raise _import_error("duplicate shard_id", shard_id=shard_id)
        seen_shards.add(shard_id)
        wav_value = shard_raw.get("wav")
        if not isinstance(wav_value, str) or not wav_value:
            raise _import_error("shard wav path is required", shard_id=shard_id)
        wav_path = Path(wav_value)
        if not wav_path.is_absolute():
            wav_path = (path.parent / wav_path).resolve()
        wav_info = inspect_wav(wav_path)
        total_samples = _require_int(
            shard_raw.get("total_samples"),
            "total_samples",
            minimum=1,
        )
        if wav_info["total_samples"] != total_samples:
            raise _import_error(
                "WAV frame count does not match total_samples",
                shard_id=shard_id,
                wav_total_samples=wav_info["total_samples"],
                manifest_total_samples=total_samples,
            )
        zones_raw = shard_raw.get("zones")
        if not isinstance(zones_raw, list) or len(zones_raw) != len(ZONE_ORDER):
            raise _import_error(
                "each shard must declare exactly four zones",
                shard_id=shard_id,
            )
        zones: list[dict[str, Any]] = []
        seen_zones: set[str] = set()
        seen_channels: set[int] = set()
        previous_end = 0
        offset_sum = 0
        known_zones = set(ZONE_ORDER)
        for zone_raw in zones_raw:
            if not isinstance(zone_raw, dict):
                raise _import_error("zone entries must be objects", shard_id=shard_id)
            zone_id = zone_raw.get("zone_id")
            if zone_id not in known_zones:
                raise _import_error(
                    "unknown zone_id",
                    shard_id=shard_id,
                    zone_id=zone_id,
                    expected=list(ZONE_ORDER),
                )
            if zone_id in seen_zones:
                raise _import_error("duplicate zone_id", shard_id=shard_id, zone_id=zone_id)
            seen_zones.add(zone_id)
            channel = _require_int(zone_raw.get("channel"), "zones.channel", minimum=0)
            if channel >= len(ZONE_ORDER) or channel in seen_channels:
                raise _import_error(
                    "zone channel must be unique within 0..3",
                    shard_id=shard_id,
                    zone_id=zone_id,
                    channel=channel,
                )
            seen_channels.add(channel)
            start_sample = _require_int(
                zone_raw.get("start_sample"),
                "zones.start_sample",
                minimum=0,
            )
            end_sample = _require_int(
                zone_raw.get("end_sample"),
                "zones.end_sample",
                minimum=0,
            )
            if start_sample != previous_end:
                raise _import_error(
                    "zone sample offsets must be contiguous and ordered",
                    shard_id=shard_id,
                    zone_id=zone_id,
                    expected_start_sample=previous_end,
                    start_sample=start_sample,
                )
            if end_sample <= start_sample:
                raise _import_error(
                    "zone sample range must be positive",
                    shard_id=shard_id,
                    zone_id=zone_id,
                    start_sample=start_sample,
                    end_sample=end_sample,
                )
            length_samples = end_sample - start_sample
            if length_samples * 1000 > duration_bound * sample_rate:
                raise _import_error(
                    "zone duration exceeds annotated duration upper bound",
                    shard_id=shard_id,
                    zone_id=zone_id,
                    duration_ms=length_samples * 1000 // sample_rate,
                    duration_upper_bound_ms=duration_bound,
                )
            duration_ms = length_samples * 1000 // sample_rate
            zones.append(
                {
                    "zone_id": zone_id,
                    "label": zone_raw.get("label", ZONE_LABELS[zone_id]),
                    "channel": channel,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "duration_ms": duration_ms,
                }
            )
            offset_sum += end_sample - start_sample
            previous_end = end_sample
        if offset_sum != total_samples:
            raise _import_error(
                "sum of zone sample offsets does not match total_samples",
                shard_id=shard_id,
                zone_offset_sum=offset_sum,
                total_samples=total_samples,
            )
        if previous_end != total_samples:
            raise _import_error(
                "zone offsets do not cover the complete WAV shard",
                shard_id=shard_id,
                covered_samples=previous_end,
                total_samples=total_samples,
            )
        if seen_zones != known_zones:
            raise _import_error(
                "zone declarations are incomplete",
                shard_id=shard_id,
                missing=sorted(known_zones - seen_zones),
            )
        refs_raw = shard_raw.get("reference_activity_ms", {})
        if not isinstance(refs_raw, dict):
            raise _import_error(
                "reference_activity_ms must be an object",
                shard_id=shard_id,
            )
        refs: dict[str, list[list[int]]] = {}
        for zone in zones:
            zone_id = zone["zone_id"]
            refs[zone_id] = _validate_intervals(
                refs_raw.get(zone_id, []),
                zone_id=zone_id,
                duration_ms=zone["duration_ms"],
                shard_id=shard_id,
                field="reference_activity_ms",
            )
        extra_refs = set(refs_raw) - known_zones
        if extra_refs:
            raise _import_error(
                "reference_activity_ms references unknown zones",
                shard_id=shard_id,
                zones=sorted(extra_refs),
            )
        priorities_raw = shard_raw.get("priorities")
        if not isinstance(priorities_raw, dict):
            raise _import_error("priorities must be an object", shard_id=shard_id)
        extra_priorities = set(priorities_raw) - known_zones
        if extra_priorities:
            raise _import_error(
                "priorities reference unknown zones",
                shard_id=shard_id,
                zones=sorted(extra_priorities),
            )
        priorities: dict[str, int] = {}
        for zone_id in ZONE_ORDER:
            priorities[zone_id] = _require_int(
                priorities_raw.get(zone_id, 1),
                f"priorities.{zone_id}",
                minimum=1,
            )
        hits_raw = shard_raw.get("word_hits", [])
        if not isinstance(hits_raw, list):
            raise _import_error("word_hits must be a list", shard_id=shard_id)
        zone_durations = {zone["zone_id"]: zone["duration_ms"] for zone in zones}
        hits = [
            _normalize_hit(
                hit,
                shard_id=shard_id,
                zone_durations=zone_durations,
                known_zones=known_zones,
            )
            for hit in hits_raw
        ]
        hit_ids = [hit["hit_id"] for hit in hits]
        if len(hit_ids) != len(set(hit_ids)):
            raise _import_error("duplicate word hit identity", shard_id=shard_id)
        hits.sort(key=lambda hit: (hit["start_ms"], hit["end_ms"], hit["zone_id"], hit["word"], hit["hit_id"]))
        shards.append(
            {
                "shard_id": shard_id,
                "title": shard_raw.get("title", shard_id),
                "wav_path": wav_info["path"],
                "content_fingerprint": wav_info["content_fingerprint"],
                "total_samples": total_samples,
                "sample_rate": sample_rate,
                "duration_upper_bound_ms": duration_bound,
                "zones": zones,
                "reference_activity_ms": refs,
                "initial_priorities": priorities,
                "initial_word_hits": hits,
            }
        )
    shards.sort(key=lambda shard: shard["shard_id"])
    default_query_budget = raw.get("default_query_budget_ms", 20)
    if (
        isinstance(default_query_budget, bool)
        or not isinstance(default_query_budget, int)
        or default_query_budget < 0
    ):
        raise _import_error(
            "default_query_budget_ms must be a non-negative integer",
            value=default_query_budget,
        )
    return {
        "schema_version": 1,
        "manifest_path": str(path),
        "manifest_digest": digest_obj(raw),
        "sample_rate": sample_rate,
        "duration_upper_bound_ms": duration_bound,
        "default_query_budget_ms": default_query_budget,
        "shards": shards,
    }
