"""Session manifest parsing, validation and deterministic import."""

from __future__ import annotations

import json
import math
import shutil
import wave
from pathlib import Path
from typing import Any, Iterable

from .errors import CabinVoiceError
from .util import canonical_bytes, read_json, sha256_file, sha256_json


ZONE_FIELDS = {"zone_id", "label", "priority", "x_m", "y_m"}


def read_manifest(path: str | Path) -> dict[str, Any]:
    try:
        return read_json(path)
    except FileNotFoundError as exc:
        raise CabinVoiceError("E_NOT_FOUND", "manifest not found", {"path": str(path)}) from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CabinVoiceError("E_INVALID_MANIFEST", "manifest is not valid JSON", {"path": str(path)}) from exc


def wav_info(path: str | Path) -> dict[str, int]:
    try:
        with wave.open(str(path), "rb") as handle:
            return {
                "channels": handle.getnchannels(),
                "sample_width": handle.getsampwidth(),
                "sample_rate": handle.getframerate(),
                "frames": handle.getnframes(),
            }
    except (FileNotFoundError, wave.Error, EOFError) as exc:
        raise CabinVoiceError("E_INVALID_WAV", "unable to read WAV header", {"path": str(path)}) from exc


def _expect_int(value: Any, *, field: str, zone_id: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "manifest field must be an integer",
            {"zone_id": zone_id, "parameter": field, "value": value},
        )
    if minimum is not None and value < minimum:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "manifest integer is below its minimum",
            {"zone_id": zone_id, "parameter": field, "value": value, "minimum": minimum},
        )
    return value


def _expect_finite(value: Any, *, field: str, zone_id: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "manifest field must be numeric",
            {"zone_id": zone_id, "parameter": field, "value": value},
        ) from exc
    if not math.isfinite(number):
        raise CabinVoiceError(
            "E_NONFINITE_SCORE",
            "manifest contains a non-finite numeric value",
            {"zone_id": zone_id, "parameter": field, "value": value},
        )
    return number


def zone_ids(manifest: dict[str, Any]) -> list[str]:
    return [str(zone["zone_id"]) for zone in manifest.get("zones", [])]


def zone_map(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(zone["zone_id"]): zone for zone in manifest.get("zones", [])}


def lexicon_terms(manifest: dict[str, Any]) -> set[str]:
    return {str(item["term"]) for item in manifest.get("lexicon", [])}


def validate_manifest(manifest: dict[str, Any], base_dir: str | Path) -> None:
    base = Path(base_dir)
    if manifest.get("schema_version") != 1:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "unsupported manifest schema_version",
            {"zone_id": "<manifest>", "parameter": "schema_version"},
        )
    session_id = manifest.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "session_id is required",
            {"zone_id": "<manifest>", "parameter": "session_id"},
        )
    sample_rate = _expect_int(
        manifest.get("sample_rate"), field="sample_rate", zone_id="<manifest>", minimum=1
    )
    if sample_rate != 16000:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "demo engine requires 16 kHz input",
            {"zone_id": "<manifest>", "parameter": "sample_rate", "value": sample_rate},
        )

    zones = manifest.get("zones")
    if not isinstance(zones, list) or not zones:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "at least one zone is required",
            {"zone_id": "<manifest>", "parameter": "zones"},
        )
    seen_zones: set[str] = set()
    for position, zone in enumerate(zones):
        if not isinstance(zone, dict):
            raise CabinVoiceError(
                "E_INVALID_MANIFEST", "zone must be an object", {"zone_id": f"zones[{position}]"}
            )
        zid = str(zone.get("zone_id", ""))
        if not zid or zid in seen_zones:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "zone_id must be non-empty and unique",
                {"zone_id": zid or f"zones[{position}]", "parameter": "zone_id"},
            )
        seen_zones.add(zid)
        _expect_int(zone.get("priority"), field="priority", zone_id=zid, minimum=1)
        _expect_finite(zone.get("x_m", 0.0), field="x_m", zone_id=zid)
        _expect_finite(zone.get("y_m", 0.0), field="y_m", zone_id=zid)

    microphone = manifest.get("microphone", {"x_m": 0.0, "y_m": 0.0})
    _expect_finite(microphone.get("x_m"), field="microphone.x_m", zone_id="<microphone>")
    _expect_finite(microphone.get("y_m"), field="microphone.y_m", zone_id="<microphone>")

    if "lexicon" not in manifest:
        raise CabinVoiceError(
            "E_LEXICON_REFERENCE_MISSING",
            "session lexicon is missing",
            {"zone_id": "<manifest>", "parameter": "lexicon"},
        )
    lexicon = manifest.get("lexicon")
    if not isinstance(lexicon, list):
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "lexicon must be a list",
            {"zone_id": "<manifest>", "parameter": "lexicon"},
        )
    terms: set[str] = set()
    for item in lexicon:
        if not isinstance(item, dict) or not isinstance(item.get("term"), str) or not item["term"]:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "lexicon entries need a non-empty term",
                {"zone_id": "<manifest>", "parameter": "lexicon.term"},
            )
        if item["term"] in terms:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "lexicon terms must be unique",
                {"zone_id": "<manifest>", "parameter": "lexicon.term", "term": item["term"]},
            )
        terms.add(item["term"])

    chunks = manifest.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        raise CabinVoiceError(
            "E_INVALID_MANIFEST",
            "at least one chunk is required",
            {"zone_id": "<manifest>", "parameter": "chunks"},
        )
    seen_chunks: set[str] = set()
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise CabinVoiceError(
                "E_INVALID_MANIFEST", "chunk must be an object", {"zone_id": "<manifest>"}
            )
        chunk_id = str(chunk.get("chunk_id", ""))
        if not chunk_id or chunk_id in seen_chunks:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "chunk_id must be non-empty and unique",
                {"zone_id": "<manifest>", "parameter": "chunk_id", "chunk_id": chunk_id},
            )
        seen_chunks.add(chunk_id)
        wav_value = chunk.get("wav")
        if not isinstance(wav_value, str) or not wav_value:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "chunk wav path is required",
                {"zone_id": "<manifest>", "parameter": "wav", "chunk_id": chunk_id},
            )
        wav_path = (base / wav_value).resolve()
        info = wav_info(wav_path)
        if info["sample_rate"] != sample_rate or info["channels"] != 1 or info["sample_width"] != 2:
            raise CabinVoiceError(
                "E_INVALID_WAV",
                "WAV must be 16 kHz, mono, 16-bit PCM",
                {"zone_id": "<manifest>", "chunk_id": chunk_id, "wav": str(wav_path), **info},
            )
        segments = chunk.get("segments")
        if not isinstance(segments, list) or not segments:
            raise CabinVoiceError(
                "E_INVALID_MANIFEST",
                "chunk needs at least one segment",
                {"zone_id": "<manifest>", "chunk_id": chunk_id, "parameter": "segments"},
            )
        seen_segments: set[str] = set()
        for segment in segments:
            if not isinstance(segment, dict):
                raise CabinVoiceError(
                    "E_INVALID_MANIFEST",
                    "segment must be an object",
                    {"zone_id": "<manifest>", "chunk_id": chunk_id},
                )
            zid = str(segment.get("zone_id", ""))
            if zid not in seen_zones or zid in seen_segments:
                raise CabinVoiceError(
                    "E_INVALID_MANIFEST",
                    "segment zone_id must refer to a unique declared zone",
                    {"zone_id": zid, "chunk_id": chunk_id, "parameter": "segments.zone_id"},
                )
            seen_segments.add(zid)
            offset = _expect_int(
                segment.get("offset_samples"),
                field="offset_samples",
                zone_id=zid,
                minimum=0,
            )
            count = _expect_int(
                segment.get("sample_count"),
                field="sample_count",
                zone_id=zid,
                minimum=1,
            )
            if offset + count > info["frames"]:
                raise CabinVoiceError(
                    "E_ZONE_OFFSET_OUT_OF_BOUNDS",
                    "zone segment exceeds WAV frame count",
                    {
                        "zone_id": zid,
                        "chunk_id": chunk_id,
                        "parameter": "offset_samples",
                        "offset_samples": offset,
                        "sample_count": count,
                        "wav_frames": info["frames"],
                    },
                )
            for reference in segment.get("reference_active_ms", []):
                if (
                    not isinstance(reference, list)
                    or len(reference) != 2
                    or not all(isinstance(value, int) and not isinstance(value, bool) for value in reference)
                    or reference[0] < 0
                    or reference[1] <= reference[0]
                ):
                    raise CabinVoiceError(
                        "E_INVALID_MANIFEST",
                        "reference_active_ms intervals must be increasing integer pairs",
                        {"zone_id": zid, "chunk_id": chunk_id, "parameter": "reference_active_ms"},
                    )
            hits = segment.get("lexicon_hits", [])
            if not isinstance(hits, list):
                raise CabinVoiceError(
                    "E_INVALID_MANIFEST",
                    "lexicon_hits must be a list",
                    {"zone_id": zid, "chunk_id": chunk_id, "parameter": "lexicon_hits"},
                )
            for hit in hits:
                if not isinstance(hit, dict):
                    raise CabinVoiceError(
                        "E_INVALID_MANIFEST",
                        "lexicon hit must be an object",
                        {"zone_id": zid, "chunk_id": chunk_id, "parameter": "lexicon_hits"},
                    )
                term = hit.get("term")
                if term not in terms:
                    raise CabinVoiceError(
                        "E_LEXICON_REFERENCE_MISSING",
                        "lexicon hit references a term absent from the session lexicon",
                        {
                            "zone_id": zid,
                            "chunk_id": chunk_id,
                            "parameter": "lexicon_hits.term",
                            "term": term,
                        },
                    )
                strength = _expect_finite(hit.get("strength"), field="lexicon_hits.strength", zone_id=zid)
                if strength <= 0:
                    raise CabinVoiceError(
                        "E_NONFINITE_SCORE",
                        "lexicon hit strength must be finite and positive",
                        {
                            "zone_id": zid,
                            "chunk_id": chunk_id,
                            "parameter": "lexicon_hits.strength",
                            "value": strength,
                        },
                    )
                if "at_ms" in hit:
                    _expect_int(hit["at_ms"], field="lexicon_hits.at_ms", zone_id=zid, minimum=0)


def chunk_content_fingerprint(wav_path: str | Path, segment_entries: Iterable[dict[str, Any]]) -> str:
    segments = [
        {
            "zone_id": segment["zone_id"],
            "offset_samples": segment["offset_samples"],
            "sample_count": segment["sample_count"],
        }
        for segment in segment_entries
    ]
    return sha256_json(
        {
            "wav_sha256": sha256_file(wav_path),
            "segments": sorted(segments, key=lambda item: item["zone_id"]),
        }
    )


def normalized_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return canonical_bytes(manifest)


def import_manifest(source: str | Path, workspace_root: str | Path) -> Path:
    """Copy a manifest and its audio into a workspace with collision-free names."""

    source_path = Path(source).resolve()
    manifest = read_manifest(source_path)
    validate_manifest(manifest, source_path.parent)
    target_root = Path(workspace_root).resolve()
    audio_dir = target_root / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)
    rewritten = json.loads(json.dumps(manifest))
    source_base = source_path.parent
    for chunk in rewritten["chunks"]:
        original = (source_base / chunk["wav"]).resolve()
        suffix = original.suffix or ".wav"
        target_name = f"{chunk['chunk_id']}_{sha256_file(original)[:12]}{suffix}"
        target = audio_dir / target_name
        if not target.exists() or sha256_file(target) != sha256_file(original):
            temporary = target.with_name(f".{target.name}.importing")
            shutil.copyfile(original, temporary)
            temporary.replace(target)
        chunk["wav"] = f"audio/{target_name}"
    target_manifest = target_root / "manifest.json"
    target_manifest.parent.mkdir(parents=True, exist_ok=True)
    target_manifest.write_bytes(canonical_bytes(rewritten))
    return target_manifest
