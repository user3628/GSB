"""Normalization + SimHash near-duplicate detection stage.

For each sample the concatenated instruction/input/output text is
normalized (NFKC, whitespace folding, lowercased) and hashed to a 64-bit
SimHash. A 4x16-bit band index yields candidates; pairs within the
configurable Hamming threshold are duplicates. The first occurrence (in
input order) is kept; removed duplicates are recorded with their distance.
Empty or whitespace-only samples raise E_EMPTY_SAMPLE and are excluded.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import E_EMPTY_SAMPLE, WorkbenchError
from .models import InternalSample
from .simhash import BandIndex, hamming_distance, simhash64
from .textnorm import normalize_sample_text, simhash_features


@dataclass
class DedupConfig:
    hamming_threshold: int = 3

    def to_dict(self) -> dict[str, Any]:
        return {"hamming_threshold": self.hamming_threshold}


@dataclass
class DedupResult:
    kept: list[InternalSample] = field(default_factory=list)
    removed: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "kept": [s.sample_id for s in self.kept],
            "removed": self.removed,
            "errors": self.errors,
        }


def dedup_samples(
    samples: list[InternalSample],
    config: DedupConfig,
    should_stop=None,
) -> DedupResult:
    result = DedupResult()
    index = BandIndex()
    for sample in samples:
        normalized = normalize_sample_text(sample.instruction, sample.input, sample.output)
        if not normalized:
            err = WorkbenchError(
                E_EMPTY_SAMPLE,
                "sample text is empty after normalization",
                {"sample_id": sample.sample_id},
            )
            err.log()
            result.errors.append(err.to_dict()["error"])
        else:
            hash_value = simhash64(simhash_features(normalized))
            best: tuple[int, int] | None = None  # (distance, kept position)
            for position in index.candidates(hash_value):
                distance = hamming_distance(hash_value, index.hashes[position])
                if distance <= config.hamming_threshold:
                    if best is None or (distance, position) < best:
                        best = (distance, position)
            if best is None:
                index.add(hash_value)
                result.kept.append(sample)
            else:
                distance, position = best
                result.removed.append(
                    {
                        "sample_id": sample.sample_id,
                        "duplicate_of": result.kept[position].sample_id,
                        "distance": distance,
                    }
                )
        if should_stop is not None and should_stop():
            from .pipeline import GracefulStop

            raise GracefulStop("stop requested during dedup stage")
    return result
