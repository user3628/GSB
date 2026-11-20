"""Sample loading, fingerprinting and deterministic batching."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

from .determinism import sha256_text, stable_json
from .tokenizer import encode, pad_batch
import torch


@dataclass(frozen=True)
class Sample:
    id: str
    instruction: str
    output: str
    lang: str
    source: str

    @classmethod
    def from_dict(cls, payload: dict) -> "Sample":
        return cls(
            id=str(payload["id"]),
            instruction=payload["instruction"],
            output=payload["output"],
            lang=payload["lang"],
            source=payload["source"],
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "instruction": self.instruction,
            "output": self.output,
            "lang": self.lang,
            "source": self.source,
        }


def load_jsonl(path: str) -> list[Sample]:
    samples: list[Sample] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                samples.append(Sample.from_dict(json.loads(line)))
    return samples


def samples_fingerprint(samples: list[Sample]) -> str:
    body = stable_json([sample.to_dict() for sample in samples])
    return sha256_text(body)


def jsonl_fingerprint(path: str) -> str:
    return samples_fingerprint(load_jsonl(path))


def build_batch(samples: list[Sample], max_seq_len: int) -> tuple[torch.Tensor, torch.Tensor]:
    """Tokenise instruction -> output pairs and build input/target tensors.

    Targets are the shifted token ids with padding masked to -100, giving a
    deterministic cross-entropy loss.
    """
    encoded = []
    for sample in samples:
        joined = f"{sample.instruction}\n{sample.output}"
        encoded.append(encode(joined, max_seq_len))
    seq_len = min(max_seq_len, max(len(seq) for seq in encoded))
    encoded = [seq[:seq_len] for seq in encoded]
    padded = pad_batch(encoded, seq_len)
    tokens = torch.tensor(padded, dtype=torch.long)
    inputs = tokens[:, :-1].contiguous()
    targets = tokens[:, 1:].contiguous()
    targets[targets == 0] = -100
    return inputs, targets


def iter_step_batches(
    samples: list[Sample],
    batch_size: int,
    grad_accum: int,
    max_steps: int,
    max_seq_len: int,
    initial_cursor: int = 0,
    start_step: int = 0,
):
    """Yield ``max_steps`` deterministic batches; sample order never changes."""
    if not samples:
        raise ValueError("cannot train without samples")
    cursor = initial_cursor
    for step_index in range(start_step + 1, max_steps + 1):
        micro_batches = []
        for _ in range(grad_accum):
            chunk = []
            for _ in range(batch_size):
                chunk.append(samples[cursor % len(samples)])
                cursor += 1
            micro_batches.append(build_batch(chunk, max_seq_len))
        yield micro_batches
