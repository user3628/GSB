"""Reusable row-block and column-block online-softmax state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .digest import array_digest, sha256_json
from .numeric import NumericStats


@dataclass
class PartialBlockState:
    """The local softmax state for one (row block, column block) pair.

    ``m`` and ``l`` are local row maxima and exponent sums. ``o`` is the local
    weighted value sum (not normalized).  Keeping these block-local states is
    what lets a prefix correction recompute only affected column blocks while
    replaying the exact same merge order as a full computation.
    """

    col_block: int
    key_start: int
    key_end: int
    q_start: int
    q_end: int
    m: np.ndarray
    l: np.ndarray
    o: np.ndarray
    dependency_fingerprint: str
    numeric_stats: NumericStats = field(default_factory=NumericStats)

    def validate(self) -> None:
        if self.m.ndim != 3 or self.l.shape != self.m.shape:
            raise ValueError("partial m/l shape mismatch")
        if self.o.ndim != 4 or self.o.shape[:3] != self.m.shape:
            raise ValueError("partial o shape mismatch")
        if not (np.isfinite(self.l).all() and np.isfinite(self.o).all()):
            raise ValueError("partial state contains non-finite l/o")

    def metadata(self) -> dict[str, Any]:
        return {
            "col_block": self.col_block,
            "key_start": self.key_start,
            "key_end": self.key_end,
            "q_start": self.q_start,
            "q_end": self.q_end,
            "dependency_fingerprint": self.dependency_fingerprint,
            "m_sha256": array_digest(self.m),
            "l_sha256": array_digest(self.l),
            "o_sha256": array_digest(self.o),
            "numeric_stats": self.numeric_stats.to_dict(),
        }


@dataclass
class RowBlockState:
    shard_id: str
    row_block: int
    q_start: int
    q_end: int
    key_length: int
    partials: list[PartialBlockState]
    m: np.ndarray
    l: np.ndarray
    o: np.ndarray
    config_fingerprint: str
    q_fingerprint: str
    kv_fingerprint: str
    input_fingerprint: str
    numeric_stats: NumericStats = field(default_factory=NumericStats)
    run_id: str = ""
    sequence: int = 0

    def validate(self) -> None:
        if not self.partials:
            raise ValueError("row block state has no column-block partials")
        if self.m.ndim != 3 or self.l.shape != self.m.shape:
            raise ValueError("row m/l shape mismatch")
        if self.o.ndim != 4 or self.o.shape[:3] != self.m.shape:
            raise ValueError("row o shape mismatch")
        if not (np.isfinite(self.l).all() and np.isfinite(self.o).all()):
            raise ValueError("row state contains non-finite l/o")
        for partial in self.partials:
            partial.validate()
            if partial.m.shape != self.m.shape or partial.l.shape != self.l.shape:
                raise ValueError("partial m/l shape differs from row state")
            if partial.o.shape != self.o.shape:
                raise ValueError("partial o shape differs from row state")
            if (partial.q_start, partial.q_end) != (self.q_start, self.q_end):
                raise ValueError("partial q interval differs from row state")

    def output(self) -> np.ndarray:
        return np.ascontiguousarray(self.o / self.l[..., None])

    def copy_for_run(self, run_id: str, sequence: int = 0) -> "RowBlockState":
        return RowBlockState(
            shard_id=self.shard_id,
            row_block=self.row_block,
            q_start=self.q_start,
            q_end=self.q_end,
            key_length=self.key_length,
            partials=self.partials,
            m=self.m,
            l=self.l,
            o=self.o,
            config_fingerprint=self.config_fingerprint,
            q_fingerprint=self.q_fingerprint,
            kv_fingerprint=self.kv_fingerprint,
            input_fingerprint=self.input_fingerprint,
            numeric_stats=self.numeric_stats,
            run_id=run_id,
            sequence=sequence,
        )

    def to_summary(self) -> "RowBlockSummary":
        return RowBlockSummary(
            shard_id=self.shard_id,
            row_block=self.row_block,
            q_start=self.q_start,
            q_end=self.q_end,
            m=self.m,
            l=self.l,
            o=self.o,
            numeric_stats=self.numeric_stats,
            input_fingerprint=self.input_fingerprint,
            sequence=self.sequence,
            run_id=self.run_id,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "row_block": self.row_block,
            "q_start": self.q_start,
            "q_end": self.q_end,
            "key_length": self.key_length,
            "partial_blocks": len(self.partials),
            "m_sha256": array_digest(self.m),
            "l_sha256": array_digest(self.l),
            "o_sha256": array_digest(self.o),
            "output_sha256": array_digest(self.output()),
            "numeric_stats": self.numeric_stats.to_dict(),
        }

    def to_payload(self, sequence: int, run_id: str) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
        self.validate()
        covered_key_end = max(part.key_end for part in self.partials)
        local_m = np.ascontiguousarray(np.stack([part.m for part in self.partials], axis=0))
        local_l = np.ascontiguousarray(np.stack([part.l for part in self.partials], axis=0))
        local_o = np.ascontiguousarray(np.stack([part.o for part in self.partials], axis=0))
        arrays = {
            "local_m": local_m,
            "local_l": local_l,
            "local_o": local_o,
            "row_m": np.ascontiguousarray(self.m),
            "row_l": np.ascontiguousarray(self.l),
            "row_o": np.ascontiguousarray(self.o),
        }
        metadata = {
            "format": "flashblock/checkpoint-v1",
            "shard_id": self.shard_id,
            "run_id": run_id,
            "sequence": int(sequence),
            "row_block": self.row_block,
            "q_start": self.q_start,
            "q_end": self.q_end,
            "key_start": 0,
            "key_end": covered_key_end,
            "key_length": self.key_length,
            "config_fingerprint": self.config_fingerprint,
            "q_fingerprint": self.q_fingerprint,
            "kv_fingerprint": self.kv_fingerprint,
            "input_fingerprint": self.input_fingerprint,
            "numeric_stats": self.numeric_stats.to_dict(),
            "partials": [part.metadata() for part in self.partials],
            "row_m_sha256": array_digest(self.m),
            "row_l_sha256": array_digest(self.l),
            "row_o_sha256": array_digest(self.o),
            "array_shapes": {key: list(value.shape) for key, value in arrays.items()},
        }
        return metadata, arrays

    @classmethod
    def from_payload(
        cls,
        metadata: dict[str, Any],
        arrays: dict[str, np.ndarray],
    ) -> "RowBlockState":
        local_m = arrays["local_m"]
        local_l = arrays["local_l"]
        local_o = arrays["local_o"]
        descriptors = metadata["partials"]
        if local_m.shape[0] != len(descriptors) or local_o.shape[0] != len(descriptors):
            raise ValueError("local checkpoint stack does not match metadata")
        partials: list[PartialBlockState] = []
        for index, descriptor in enumerate(descriptors):
            partial = PartialBlockState(
                col_block=int(descriptor["col_block"]),
                key_start=int(descriptor["key_start"]),
                key_end=int(descriptor["key_end"]),
                q_start=int(descriptor["q_start"]),
                q_end=int(descriptor["q_end"]),
                m=np.ascontiguousarray(local_m[index]),
                l=np.ascontiguousarray(local_l[index]),
                o=np.ascontiguousarray(local_o[index]),
                dependency_fingerprint=str(descriptor["dependency_fingerprint"]),
                numeric_stats=NumericStats.from_dict(descriptor.get("numeric_stats")),
            )
            if array_digest(partial.m) != descriptor["m_sha256"]:
                raise ValueError("partial m checksum mismatch")
            if array_digest(partial.l) != descriptor["l_sha256"]:
                raise ValueError("partial l checksum mismatch")
            if array_digest(partial.o) != descriptor["o_sha256"]:
                raise ValueError("partial o checksum mismatch")
            partials.append(partial)

        row = cls(
            shard_id=str(metadata["shard_id"]),
            row_block=int(metadata["row_block"]),
            q_start=int(metadata["q_start"]),
            q_end=int(metadata["q_end"]),
            key_length=int(metadata["key_length"]),
            partials=partials,
            m=np.ascontiguousarray(arrays["row_m"]),
            l=np.ascontiguousarray(arrays["row_l"]),
            o=np.ascontiguousarray(arrays["row_o"]),
            config_fingerprint=str(metadata["config_fingerprint"]),
            q_fingerprint=str(metadata["q_fingerprint"]),
            kv_fingerprint=str(metadata["kv_fingerprint"]),
            input_fingerprint=str(metadata["input_fingerprint"]),
            numeric_stats=NumericStats.from_dict(metadata.get("numeric_stats")),
            run_id=str(metadata["run_id"]),
            sequence=int(metadata["sequence"]),
        )
        if array_digest(row.m) != metadata["row_m_sha256"]:
            raise ValueError("row m checksum mismatch")
        if array_digest(row.l) != metadata["row_l_sha256"]:
            raise ValueError("row l checksum mismatch")
        if array_digest(row.o) != metadata["row_o_sha256"]:
            raise ValueError("row o checksum mismatch")
        row.validate()
        return row

    @property
    def state_digest(self) -> str:
        return sha256_json(
            {
                "row": self.row_block,
                "q": [self.q_start, self.q_end],
                "keys": self.key_length,
                "m": array_digest(self.m),
                "l": array_digest(self.l),
                "o": array_digest(self.o),
            }
        )


@dataclass
class RowBlockSummary:
    """Compact completed row-block state used after local partials are released."""

    shard_id: str
    row_block: int
    q_start: int
    q_end: int
    m: np.ndarray
    l: np.ndarray
    o: np.ndarray
    numeric_stats: NumericStats = field(default_factory=NumericStats)
    input_fingerprint: str = ""
    sequence: int = 0
    run_id: str = ""

    def output(self) -> np.ndarray:
        return np.ascontiguousarray(self.o / self.l[..., None])

