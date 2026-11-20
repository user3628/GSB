"""Unified request and manifest models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


STAGES = ("postprocess", "normalize", "decode", "summarize")
STAGE_INDEX = {name: index for index, name in enumerate(STAGES)}


@dataclass(frozen=True, slots=True)
class SamplingRequest:
    request_id: str
    logits_path: str
    temperature: float
    top_k: int | None
    top_p: float
    repetition_penalty: float
    seed: int
    steps: int
    prompt_tokens: tuple[int, ...]
    manifest_version: int
    vocab: tuple[str, ...]
    base_dir: str = "."

    @property
    def vocab_size(self) -> int:
        return len(self.vocab)

    def semantic_dict(self) -> dict[str, Any]:
        """Fields that can change sampling behaviour or stage reuse."""

        return {
            "request_id": self.request_id,
            "logits_path": self.logits_path,
            "temperature": self.temperature,
            "top_k": self.top_k,
            "top_p": self.top_p,
            "repetition_penalty": self.repetition_penalty,
            "seed": self.seed,
            "steps": self.steps,
            "prompt_tokens": list(self.prompt_tokens),
            "manifest_version": self.manifest_version,
            "vocab": list(self.vocab),
            "base_dir": self.base_dir,
        }

    def registry_dict(self) -> dict[str, Any]:
        return self.semantic_dict()

    def resolved_logits_path(self) -> str:
        from pathlib import Path

        source = Path(self.logits_path)
        if source.is_absolute():
            return str(source)
        return str((Path(self.base_dir) / source).resolve())


@dataclass(frozen=True, slots=True)
class Manifest:
    version: int
    requests: tuple[SamplingRequest, ...]
    vocab: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    conversions: dict[str, dict[str, Any]] = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    source_path: str | None = None
    manifest_digest: str | None = None

    @property
    def request_ids(self) -> tuple[str, ...]:
        return tuple(request.request_id for request in self.requests)
