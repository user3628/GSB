"""Budget-aware block planning with deterministic, proportional degradation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .acoustic import Prosody
from .calibration import calibration_digest
from .config import EngineConfig, SplitConfig
from .frontend import FrontendResult, PhonemeToken, TextChunk, rebuild_chunk


@dataclass(frozen=True)
class ExecutionBlock:
    block_index: int
    planned_chunk_index: int
    planned_chunk_text: str
    chunk: TextChunk
    frame_start: int
    frame_end: int
    budget_kind: str
    budget_ms: float
    estimated_ms: float
    over_budget: bool
    degraded: bool
    degrade_reason: str | None
    rejected_blocks: int
    split_level: int
    split_factor: int
    preprocess_depth: int
    worker_count: int

    @property
    def frame_count(self) -> int:
        return self.chunk.frame_count

    @property
    def sample_start(self) -> int:
        return self.frame_start * self._hop

    # sample_start/sample_end are attached by the scheduler through private fields.
    _hop: int = 1
    _sample_rate: int = 1

    @property
    def sample_end(self) -> int:
        return self.frame_end * self._hop

    def trace_record(self, *, utt_id: str, calibration: dict[str, Any]) -> dict[str, Any]:
        return {
            "utt_id": utt_id,
            "block_index": self.block_index,
            "planned_chunk_index": self.planned_chunk_index,
            "planned_chunk_text": self.planned_chunk_text,
            "block_text": self.chunk.text,
            "char_start": self.chunk.char_start,
            "char_end": self.chunk.char_end,
            "phone_start": self.chunk.phone_start,
            "phone_end": self.chunk.phone_end,
            "visible_chars": self.chunk.visible_chars,
            "frame_start": self.frame_start,
            "frame_end": self.frame_end,
            "frame_count": self.frame_count,
            "sample_start": self.sample_start,
            "sample_end": self.sample_end,
            "start_time_ms": round(self.sample_start * 1000.0 / self._sample_rate, 6),
            "end_time_ms": round(self.sample_end * 1000.0 / self._sample_rate, 6),
            "budget_kind": self.budget_kind,
            "budget_ms": self.budget_ms,
            "estimated_ms": self.estimated_ms,
            "over_budget": self.over_budget,
            "degraded": self.degraded,
            "degrade_reason": self.degrade_reason,
            "rejected_blocks": self.rejected_blocks,
            "split_level": self.split_level,
            "split_factor": self.split_factor,
            "preprocess_depth": self.preprocess_depth,
            "worker_count": self.worker_count,
            "accepted": True,
            "budget_basis": "frozen_local_calibration_v1",
            "calibration_digest": calibration_digest(calibration),
            "calibration_unit_ms": float(calibration["unit_ms"]),
            "timeout_cancellation_used": False,
        }


@dataclass(frozen=True)
class SchedulePlan:
    blocks: tuple[ExecutionBlock, ...]
    worker_count: int
    degradation_events: tuple[dict[str, Any], ...]

    @property
    def frame_count(self) -> int:
        return self.blocks[-1].frame_end if self.blocks else 0

    def trace_records(self, *, utt_id: str, calibration: dict[str, Any]) -> list[dict[str, Any]]:
        return [block.trace_record(utt_id=utt_id, calibration=calibration) for block in self.blocks]


def _estimate_ms(
    *,
    config: EngineConfig,
    calibration: dict[str, Any],
    visible_chars: int,
    frame_count: int,
    complexity: float,
    depth: int,
) -> float:
    units = config.budget.cost_units
    work_units = (
        units.base
        + units.per_visible_char * float(visible_chars)
        + units.per_frame * float(frame_count)
        + units.per_complexity * max(0.0, complexity)
        + units.per_depth * float(depth)
    )
    return float(calibration["unit_ms"]) * work_units


def _split_phones(phones: tuple[PhonemeToken, ...], factor: int) -> list[tuple[PhonemeToken, ...]]:
    factor = max(2, min(factor, len(phones)))
    base = len(phones) // factor
    remainder = len(phones) % factor
    groups: list[tuple[PhonemeToken, ...]] = []
    offset = 0
    for index in range(factor):
        size = base + (1 if index < remainder else 0)
        groups.append(phones[offset : offset + size])
        offset += size
    return groups


def plan_blocks(
    *,
    frontend: FrontendResult,
    config: EngineConfig,
    calibration: dict[str, Any],
    prosody: Prosody,
) -> SchedulePlan:
    """Plan execution blocks without changing any acoustic frame.

    When a candidate block exceeds its budget, its split factor is
    ``ceil(estimated/budget)`` (bounded by the number of phones and the configured
    split depth).  The original candidate is rejected and replaced by the
    proportional number of smaller blocks.  No thread is timed out or killed.
    """

    blocks: list[ExecutionBlock] = []
    events: list[dict[str, Any]] = []
    split_cfg: SplitConfig = config.split
    frame_cursor = 0

    def emit(
        *,
        planned_index: int,
        planned_text: str,
        phones: tuple[PhonemeToken, ...],
        split_level: int,
        inherited_over_budget: bool,
        inherited_reason: str | None,
        inherited_rejected: int,
        inherited_factor: int,
    ) -> None:
        nonlocal frame_cursor
        chunk = rebuild_chunk(frontend, planned_index, phones)
        block_index = len(blocks)
        budget_kind = "first_packet" if block_index == 0 else "continuation"
        budget_ms = (
            config.budget.first_packet_ms if block_index == 0 else config.budget.continuation_ms
        )
        max_depth = config.budget.max_preprocess_depth
        depth = max_depth
        estimated = _estimate_ms(
            config=config,
            calibration=calibration,
            visible_chars=chunk.visible_chars,
            frame_count=chunk.frame_count,
            complexity=prosody.complexity,
            depth=depth,
        )
        depth_reduced = False
        if estimated > budget_ms and depth > 0:
            depth = 0
            depth_reduced = True
            estimated = _estimate_ms(
                config=config,
                calibration=calibration,
                visible_chars=chunk.visible_chars,
                frame_count=chunk.frame_count,
                complexity=prosody.complexity,
                depth=depth,
            )
        over_budget = inherited_over_budget or estimated > budget_ms
        can_split = len(phones) > 1 and split_level < split_cfg.max_budget_splits
        if estimated > budget_ms and can_split:
            factor = int(math.ceil(estimated / budget_ms))
            factor = max(2, min(factor, len(phones)))
            groups = _split_phones(phones, factor)
            reason = (
                f"budget_overrun:estimated_{estimated:.6f}ms>budget_{budget_ms:.6f}ms:"
                f"reject_planned_block:split_{factor}"
            )
            events.append(
                {
                    "utt_id": frontend.utt_id,
                    "planned_chunk_index": planned_index,
                    "block_index_at_decision": block_index,
                    "budget_kind": budget_kind,
                    "budget_ms": budget_ms,
                    "estimated_ms": estimated,
                    "overflow_ratio": estimated / budget_ms,
                    "split_factor": factor,
                    "rejected_blocks": factor - 1,
                    "split_level": split_level,
                    "preprocess_depth": depth,
                    "depth_reduced": depth_reduced,
                    "reason": reason,
                }
            )
            for group in groups:
                emit(
                    planned_index=planned_index,
                    planned_text=planned_text,
                    phones=group,
                    split_level=split_level + 1,
                    inherited_over_budget=True,
                    inherited_reason=reason,
                    inherited_rejected=inherited_rejected + factor - 1,
                    inherited_factor=max(inherited_factor, factor),
                )
            return
        reason = inherited_reason
        degraded = inherited_over_budget or depth_reduced or estimated > budget_ms
        if estimated > budget_ms and not can_split:
            reason = (
                f"budget_overrun:estimated_{estimated:.6f}ms>budget_{budget_ms:.6f}ms:"
                "cannot_split_further:accepted_without_timeout"
            )
        elif depth_reduced and reason is None:
            reason = (
                f"depth_reduced_from_{max_depth}_to_0:estimated_{estimated:.6f}ms>"
                f"budget_{budget_ms:.6f}ms"
            )
        blocks.append(
            ExecutionBlock(
                block_index=block_index,
                planned_chunk_index=planned_index,
                planned_chunk_text=planned_text,
                chunk=chunk,
                frame_start=frame_cursor,
                frame_end=frame_cursor + chunk.frame_count,
                budget_kind=budget_kind,
                budget_ms=budget_ms,
                estimated_ms=estimated,
                over_budget=over_budget,
                degraded=degraded,
                degrade_reason=reason,
                rejected_blocks=inherited_rejected,
                split_level=split_level,
                split_factor=inherited_factor,
                preprocess_depth=depth,
                worker_count=0,
                _hop=config.hop_samples,
                _sample_rate=config.output.sample_rate,
            )
        )
        frame_cursor += chunk.frame_count

    for chunk in frontend.chunks:
        emit(
            planned_index=chunk.index,
            planned_text=chunk.text,
            phones=chunk.phonemes,
            split_level=0,
            inherited_over_budget=False,
            inherited_reason=None,
            inherited_rejected=0,
            inherited_factor=1,
        )

    if not blocks:
        return SchedulePlan(blocks=(), worker_count=1, degradation_events=tuple(events))

    degraded_any = any(block.degraded for block in blocks)
    requested_workers = min(config.budget.max_workers, max(1, len(blocks)))
    if degraded_any:
        max_factor = max(block.split_factor for block in blocks)
        requested_workers = min(config.budget.max_workers, max(1, max_factor))
    finalized = tuple(
        ExecutionBlock(
            block_index=block.block_index,
            planned_chunk_index=block.planned_chunk_index,
            planned_chunk_text=block.planned_chunk_text,
            chunk=block.chunk,
            frame_start=block.frame_start,
            frame_end=block.frame_end,
            budget_kind=block.budget_kind,
            budget_ms=block.budget_ms,
            estimated_ms=block.estimated_ms,
            over_budget=block.over_budget,
            degraded=block.degraded,
            degrade_reason=block.degrade_reason,
            rejected_blocks=block.rejected_blocks,
            split_level=block.split_level,
            split_factor=block.split_factor,
            preprocess_depth=block.preprocess_depth,
            worker_count=requested_workers,
            _hop=config.hop_samples,
            _sample_rate=config.output.sample_rate,
        )
        for block in blocks
    )
    return SchedulePlan(
        blocks=finalized,
        worker_count=requested_workers,
        degradation_events=tuple(events),
    )


def schedule_fingerprint(
    *, frontend_fingerprint: str, config: EngineConfig, calibration: dict[str, Any]
) -> str:
    from .utils import sha256_json

    return sha256_json(
        {
            "frontend": frontend_fingerprint,
            "budget": config.budget_digest(),
            "split": config.split_digest(),
            "calibration": calibration_digest(calibration),
            "algorithm": "proportional-reject-v1",
        }
    )
