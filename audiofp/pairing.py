"""Deterministic constellation fingerprint-pair generation."""

from __future__ import annotations

from dataclasses import dataclass

from .config import ShardConfig
from .dsp import Peak


@dataclass(frozen=True)
class FingerprintPair:
    pair_index: int
    anchor_frame: int
    anchor_freq: int
    target_frame: int
    target_freq: int
    dt: int
    anchor_amplitude: float
    target_amplitude: float

    def as_dict(self) -> dict[str, int | float]:
        return {
            "pair_index": self.pair_index,
            "anchor_frame": self.anchor_frame,
            "anchor_freq": self.anchor_freq,
            "target_frame": self.target_frame,
            "target_freq": self.target_freq,
            "dt": self.dt,
            "anchor_amplitude": self.anchor_amplitude,
            "target_amplitude": self.target_amplitude,
        }


@dataclass(frozen=True)
class PairResult:
    pairs: tuple[FingerprintPair, ...]
    anchors_scanned: int
    terminal_anchor_edges: int
    policy: str


def generate_fingerprint_pairs(peaks: tuple[Peak, ...], config: ShardConfig) -> PairResult:
    """Build anchor -> target edges in strict temporal look-ahead order.

    For every anchor, target peaks are ranked by descending amplitude and then
    by (frame, frequency bin) to break ties.  The final anchor in a shard is
    scanned but cannot wrap to the beginning of the shard: the explicit
    ``bounded_no_wrap`` policy keeps time-difference semantics honest.
    """

    ordered = sorted(peaks, key=lambda peak: (peak.frame, peak.freq))
    pairs: list[FingerprintPair] = []
    terminal_edges = 0
    for anchor_index, anchor in enumerate(ordered):
        if len(pairs) >= config.max_pairs:
            break
        candidates: list[tuple[float, int, int, Peak]] = []
        for target_index in range(anchor_index + 1, len(ordered)):
            target = ordered[target_index]
            dt = target.frame - anchor.frame
            if dt < config.time_delta_min:
                continue
            if dt > config.time_delta_max:
                break
            frequency_delta = abs(target.freq - anchor.freq)
            if not config.freq_delta_min <= frequency_delta <= config.freq_delta_max:
                continue
            candidates.append((-target.amplitude, target.frame, target.freq, target))
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        chosen = candidates[: config.targets_per_anchor]
        if anchor_index == len(ordered) - 1:
            terminal_edges += len(chosen)
        for _, _, _, target in chosen:
            if len(pairs) >= config.max_pairs:
                break
            pairs.append(
                FingerprintPair(
                    pair_index=len(pairs),
                    anchor_frame=anchor.frame,
                    anchor_freq=anchor.freq,
                    target_frame=target.frame,
                    target_freq=target.freq,
                    dt=target.frame - anchor.frame,
                    anchor_amplitude=anchor.amplitude,
                    target_amplitude=target.amplitude,
                )
            )
    return PairResult(
        pairs=tuple(pairs),
        anchors_scanned=len(ordered),
        terminal_anchor_edges=terminal_edges,
        policy=config.pairing_policy,
    )


def pairs_artifact(
    result: PairResult,
    config: ShardConfig,
    peaks_digest: str,
    stage_version: str,
) -> dict:
    return {
        "schema": "audiofp.pairs.v1",
        "shard_id": config.shard_id,
        "stage_version": stage_version,
        "input_digest": peaks_digest,
        "pairing_policy": result.policy,
        "terminal_anchor_policy": "bounded_no_wrap",
        "wraparound": False,
        "anchors_scanned": result.anchors_scanned,
        "terminal_anchor_edges": result.terminal_anchor_edges,
        "max_pairs": config.max_pairs,
        "targets_per_anchor": config.targets_per_anchor,
        "pair_count": len(result.pairs),
        "pairs": [pair.as_dict() for pair in result.pairs],
    }

