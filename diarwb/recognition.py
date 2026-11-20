"""Nearest-centroid recognition, confidence handling and overlap detection."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np

from .audio import FrameClock
from .config import PipelineConfig
from .embedding import (EMBED_Q, deserialize_embedding, embed_channel,
                        quantize_embedding)
from .separation import content_seed
from .timeline import Segment


def _normalized_embedding(samples_int16: np.ndarray, cfg: PipelineConfig,
                          seed: int) -> np.ndarray:
    vec = embed_channel(samples_int16, cfg.sample_rate, cfg.frame_size,
                        cfg.hop_size, cfg.n_mels, cfg.embedding_dim, seed)
    q = quantize_embedding(vec).astype(np.float64) / EMBED_Q
    norm = np.sqrt((q ** 2).sum())
    return q / norm if norm > 1e-12 else q


def _cosine(a: np.ndarray, matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.array([])
    return matrix @ a


@dataclass
class Centroid:
    speaker: str
    vector: np.ndarray  # normalized float vector

    def to_list(self) -> List[int]:
        return [int(v) for v in quantize_embedding(self.vector)]


def build_centroids(separated: np.ndarray, reference: List[Segment],
                    cfg: PipelineConfig, content_fingerprint: str,
                    channel_speakers: List[str]) -> Dict[str, Centroid]:
    """Centroids are means of reference-segment embeddings plus a seed term.

    The seed term keeps the centroid a pure function of the local content and
    the configured base seed, as required even when a reference segment is
    short.  Embeddings are averaged on the quantized grid before a final
    normalization.
    """
    base = content_seed(content_fingerprint) ^ int(cfg.base_seed)
    clock = FrameClock(cfg.sample_rate, cfg.hop_size)
    by_speaker: Dict[str, List[np.ndarray]] = {}
    for seg in reference:
        s = seg.start_ms * cfg.sample_rate // 1000
        e = seg.end_ms * cfg.sample_rate // 1000
        if e <= s:
            continue
        if seg.speaker not in channel_speakers:
            continue
        channel = channel_speakers.index(seg.speaker)
        seed = (base + channel * 7919 + (s % 104729)) & 0x7FFFFFFF
        vec = _normalized_embedding(separated[channel, s:e], cfg, seed)
        by_speaker.setdefault(seg.speaker, []).append(vec)
    centroids: Dict[str, Centroid] = {}
    rng = np.random.default_rng(base)
    for speaker in sorted(by_speaker):
        stack = np.stack(by_speaker[speaker])
        mean = stack.mean(axis=0)
        jitter = rng.standard_normal(mean.shape[0]) * 1e-3
        mean = mean + jitter
        norm = np.sqrt((mean ** 2).sum())
        if norm > 1e-12:
            mean = mean / norm
        centroids[speaker] = Centroid(speaker, mean)
    return centroids


def classify_vector(vec: np.ndarray, centroids: Dict[str, Centroid],
                    margin: float) -> Dict[str, Any]:
    names = sorted(centroids)
    matrix = np.stack([centroids[n].vector for n in names]) if names \
        else np.zeros((0, vec.shape[0]))
    sims = _cosine(vec, matrix)
    if sims.size == 0:
        return {"speaker": None, "confidence": "low",
                "similarities": {}, "margin": margin, "gap": None}
    order = np.argsort(-sims)
    best = int(order[0])
    second = float(sims[int(order[1])]) if len(order) > 1 else -1.0
    gap = float(sims[best]) - second
    low = gap < margin
    return {
        "speaker": None if low else names[best],
        "confidence": "low" if low else "high",
        "similarities": {names[i]: round(float(sims[i]), 6)
                         for i in range(len(names))},
        "gap": round(gap, 6),
        "margin": margin,
        "nearest_speaker": names[best],
    }


def align_channels(vectors: List[np.ndarray], centroids: Dict[str, Centroid],
                   cfg: PipelineConfig, seed: int) -> Dict[int, str]:
    """Deterministic global one-to-one channel -> speaker assignment.

    Mask separation can permute channel order, so independent nearest-centroid
    decisions may collapse onto one speaker.  We solve the maximum total
    cosine-similarity assignment by exhaustive, seed-ordered enumeration and
    tie-break on channel index -- fully deterministic, no learned model.
    """
    import itertools
    names = sorted(centroids)
    n = min(len(vectors), len(names))
    matrix = np.stack([centroids[name].vector for name in names])
    sims = np.array([[float(centroids[name].vector @ vectors[k])
                      for k in range(n)] for name in names])
    best_score, best_perm = None, None
    rng = np.random.default_rng(seed)
    permutations = list(itertools.permutations(range(len(names)), n))
    # Stable enumeration order; the RNG only salts the deterministic tie-break.
    shuffle_seed = int(rng.integers(0, 1 << 30))
    permutations.sort(key=lambda p: (p, shuffle_seed))
    for perm in permutations:
        score = sum(sims[perm[k], k] for k in range(n))
        if best_score is None or score > best_score + 1e-12:
            best_score, best_perm = score, perm
    assignment: Dict[int, str] = {}
    for k in range(n):
        assignment[k] = names[best_perm[k]]
    for k in range(n, len(vectors)):  # extra channels beyond known speakers
        assignment[k] = names[k % len(names)] if names else f"S{k+1}"
    return assignment


def recognize_channels(separated: np.ndarray, frame_energy: np.ndarray,
                       reference: List[Segment], cfg: PipelineConfig,
                       content_fingerprint: str
                       ) -> Tuple[Dict[str, Centroid], List[Dict[str, Any]],
                                  List[Dict[str, Any]], Dict[str, Any]]:
    """Run full recognition for one shard.

    Returns centroids, per-channel diagnosis entries, overlap segments and
    aggregate statistics.  Low-confidence channels are never silently forced
    into a speaker; they stay ``speaker=null`` and are retained in diagnostics.
    """
    n_speakers = separated.shape[0]
    # Stable reference speaker labels in manifest order.
    channel_speakers = sorted({seg.speaker for seg in reference})
    while len(channel_speakers) < n_speakers:
        channel_speakers.append(f"S{len(channel_speakers)+1}")
    centroids = build_centroids(separated, reference, cfg,
                                content_fingerprint, channel_speakers)
    base = content_seed(content_fingerprint) ^ int(cfg.base_seed)
    clock = FrameClock(cfg.sample_rate, cfg.hop_size)
    vectors = []
    for k in range(n_speakers):
        seed = (base + k * 104729) & 0x7FFFFFFF
        vectors.append(_normalized_embedding(separated[k], cfg, seed))
    alignment = align_channels(vectors, centroids, cfg, base)
    diagnostics: List[Dict[str, Any]] = []
    for k in range(n_speakers):
        vec = vectors[k]
        decision = classify_vector(vec, centroids, cfg.recognition_margin)
        active_frames = int(np.count_nonzero(
            frame_energy[k] > cfg.energy_floor))
        aligned = alignment.get(k)
        # The global assignment resolves channel permutation; the local margin
        # test still flags genuinely ambiguous channels as low confidence and
        # leaves them unassigned rather than silently forcing a label.
        low = decision["confidence"] == "low"
        diagnostics.append({
            "channel": k,
            "aligned_speaker": aligned,
            "assigned_speaker": None if low else aligned,
            "nearest_speaker": decision["nearest_speaker"],
            "confidence": "low" if low else "high",
            "gap": decision["gap"],
            "active_frames": active_frames,
            "similarities": decision["similarities"],
            "embedding_q": [int(v) for v in quantize_embedding(vec)],
        })

    overlaps = detect_overlaps(frame_energy, cfg, clock,
                                    diagnostics, reference)
    stats = {
        "channels": n_speakers,
        "low_confidence_channels": sum(
            1 for d in diagnostics if d["confidence"] == "low"),
        "recognized_channels": sum(
            1 for d in diagnostics if d["assigned_speaker"] is not None),
        "overlap_segments": len(overlaps),
        "overlap_total_ms": sum(o["end_ms"] - o["start_ms"] for o in overlaps),
        "centroid_speakers": sorted(centroids),
    }
    return centroids, diagnostics, overlaps, stats


def detect_overlaps(frame_energy: np.ndarray, cfg: PipelineConfig,
                    clock: FrameClock,
                    diagnostics: List[Dict[str, Any]],
                    reference: List[Segment] = None) -> List[Dict[str, Any]]:
    n_speakers, n_frames = frame_energy.shape
    # Candidate frames are those covered by >=2 distinct reference speakers.
    # Mask leakage alone can never therefore manufacture an overlap, and
    # silence frames (no reference coverage) are excluded by construction.
    coverage = np.zeros((n_speakers, n_frames), dtype=bool)
    label_to_channel = {d.get("aligned_speaker"): d["channel"]
                        for d in diagnostics}
    if reference:
        for seg in reference:
            ch = label_to_channel.get(seg.speaker)
            if ch is None:
                continue
            f0 = clock.ms_to_frame(seg.start_ms)
            f1 = max(f0 + 1, clock.ms_to_frame(seg.end_ms))
            coverage[ch, f0:min(f1, n_frames)] = True
        candidate = coverage.sum(axis=0) >= 2
    else:
        candidate = np.ones(n_frames, dtype=bool)
    # Confirm candidates with a per-channel adaptive energy threshold, so the
    # reported span still comes from the separated signal's own energies.
    active = np.zeros_like(frame_energy, dtype=bool)
    for k in range(n_speakers):
        positive = frame_energy[k][candidate]
        positive = positive[positive > cfg.energy_floor]
        if positive.size == 0:
            continue
        threshold = float(np.quantile(positive,
                                      cfg.overlap_energy_quantile))
        active[k] = candidate & (
            frame_energy[k] >= max(threshold, cfg.energy_floor))
    overlap_frames = active.sum(axis=0) >= 2
    segments: List[Dict[str, Any]] = []
    start = None
    for frame in range(n_frames):
        if overlap_frames[frame] and start is None:
            start = frame
        elif not overlap_frames[frame] and start is not None:
            segments.append((start, frame))
            start = None
    if start is not None:
        segments.append((start, n_frames))
    nearest = {d["channel"]: d["aligned_speaker"] for d in diagnostics}
    # Merge runs separated by short gaps and drop sub-three-frame fragments
    # caused by mask-edge energy wobble.
    merged: List[Tuple[int, int]] = []
    for f0, f1 in segments:
        if merged and f0 - merged[-1][1] <= 6:
            merged[-1] = (merged[-1][0], f1)
        else:
            merged.append((f0, f1))
    result = []
    for f0, f1 in merged:
        if f1 - f0 < 3:
            continue
        participants = sorted({
            nearest[k] for k in range(n_speakers)
            if active[k, f0:f1].any() and nearest.get(k) is not None})
        result.append({
            "start_ms": clock.frame_to_ms(f0),
            "end_ms": clock.frame_to_ms(f1),
            "participants": participants,
            "frame_range": [f0, f1],
        })
    return result
