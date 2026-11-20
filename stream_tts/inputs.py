"""JSONL sample import and validation."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .errors import TTSError
from .utils import atomic_write_text, canonical_json


@dataclass(frozen=True)
class Utterance:
    utt_id: str
    text: str
    language: str
    speaker: int
    prosody: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "utt_id": self.utt_id,
            "text": self.text,
            "language": self.language,
            "speaker": self.speaker,
            "prosody": self.prosody,
        }


def parse_samples(text: str, *, source: str = "<memory>") -> list[Utterance]:
    utterances: list[Utterance] = []
    seen: set[str] = set()
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise TTSError(
                "E_INVALID_INPUT",
                "invalid JSONL sample",
                {"source": source, "line": line_number, "detail": str(exc)},
            ) from exc
        if not isinstance(value, dict):
            raise TTSError(
                "E_INVALID_INPUT",
                "each JSONL line must contain an object",
                {"source": source, "line": line_number},
            )
        for key in ("utt_id", "text", "language", "speaker"):
            if key not in value:
                raise TTSError(
                    "E_INVALID_INPUT",
                    f"missing field {key!r}",
                    {"source": source, "line": line_number},
                )
        utt_id = value["utt_id"]
        if not isinstance(utt_id, str) or not utt_id:
            raise TTSError(
                "E_INVALID_INPUT",
                "utt_id must be a non-empty string",
                {"source": source, "line": line_number},
            )
        if utt_id in seen:
            raise TTSError(
                "E_INVALID_INPUT",
                "duplicate utt_id",
                {"source": source, "line": line_number, "utt_id": utt_id},
            )
        seen.add(utt_id)
        if not isinstance(value["text"], str):
            raise TTSError(
                "E_INVALID_INPUT",
                "text must be a string",
                {"source": source, "line": line_number, "utt_id": utt_id},
            )
        if value["language"] not in {"zh", "en"}:
            raise TTSError(
                "E_INVALID_INPUT",
                "language must be zh or en",
                {"source": source, "line": line_number, "utt_id": utt_id},
            )
        if (
            not isinstance(value["speaker"], int)
            or isinstance(value["speaker"], bool)
            or value["speaker"] < 0
        ):
            raise TTSError(
                "E_INVALID_INPUT",
                "speaker must be a non-negative integer",
                {"source": source, "line": line_number, "utt_id": utt_id},
            )
        prosody = value.get("prosody", {})
        if prosody is None:
            prosody = {}
        if not isinstance(prosody, dict):
            raise TTSError(
                "E_INVALID_INPUT",
                "prosody must be an object",
                {"source": source, "line": line_number, "utt_id": utt_id},
            )
        for key, raw in prosody.items():
            if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(float(raw)):
                raise TTSError(
                    "E_INVALID_INPUT",
                    f"prosody.{key} must be a finite number",
                    {"source": source, "line": line_number, "utt_id": utt_id},
                )
        utterances.append(
            Utterance(
                utt_id=utt_id,
                text=value["text"],
                language=value["language"],
                speaker=int(value["speaker"]),
                prosody=dict(prosody),
            )
        )
    if not utterances:
        raise TTSError("E_INVALID_INPUT", "sample file contains no utterances", {"source": source})
    return utterances


def load_samples(path: Path) -> list[Utterance]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise TTSError("E_INVALID_INPUT", f"sample file not found: {path}") from exc
    return parse_samples(text, source=str(path))


def samples_to_jsonl(utterances: Iterable[Utterance]) -> str:
    return "".join(canonical_json(item.to_dict()) + "\n" for item in utterances)


def write_samples(path: Path, utterances: Iterable[Utterance]) -> None:
    atomic_write_text(path, samples_to_jsonl(utterances))
