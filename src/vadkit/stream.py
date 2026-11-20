"""Streaming VAD engine shared by frame-stream and batch entry points."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

import numpy as np

from .config import VADConfig
from .errors import VadError, input_length
from .features import FrameInfo, FeatureVector, score_frame


@dataclass(frozen=True)
class Endpoint:
    start_ms: float
    end_ms: float
    start_sample: int
    end_sample: int
    start_frame: int
    end_frame: int

    @property
    def duration_ms(self) -> float:
        return round(self.end_ms - self.start_ms, 3)

    def as_dict(self) -> dict[str, Any]:
        return {
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "duration_ms": self.duration_ms,
            "start_sample": self.start_sample,
            "end_sample": self.end_sample,
            "start_frame": self.start_frame,
            "end_frame": self.end_frame,
        }


@dataclass(frozen=True)
class FrameDecision:
    frame_index: int
    start_sample: int
    end_sample: int
    start_ms: float
    end_ms: float
    is_short: bool
    raw_state: str
    smoothing_state: str
    reason: str
    features: FeatureVector

    def as_dict(self) -> dict[str, Any]:
        return {
            "frame_index": self.frame_index,
            "start_sample": self.start_sample,
            "end_sample": self.end_sample,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "is_short": self.is_short,
            "raw_state": self.raw_state,
            "smoothing_state": self.smoothing_state,
            "reason": self.reason,
            **self.features.as_dict(),
        }


@dataclass(frozen=True)
class DetectionResult:
    chunk_id: str | None
    sample_count: int
    frame_count: int
    truncated_frame_count: int
    config_fingerprint: str
    endpoints: tuple[Endpoint, ...]
    frames: tuple[FrameDecision, ...]

    def as_dict(self, *, include_frames: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "chunk_id": self.chunk_id,
            "sample_count": self.sample_count,
            "frame_count": self.frame_count,
            "truncated_frame_count": self.truncated_frame_count,
            "config_fingerprint": self.config_fingerprint,
            "endpoints": [endpoint.as_dict() for endpoint in self.endpoints],
        }
        if include_frames:
            data["frames"] = [frame.as_dict() for frame in self.frames]
        return data


@dataclass
class _Candidate:
    start_sample: int
    start_frame: int
    last_speech_end_sample: int
    last_speech_end_frame: int
    gap_start_sample: int | None = None


def _ms(sample: int, sample_rate: int) -> float:
    return round(sample * 1000.0 / sample_rate, 3)


def coerce_pcm16(samples: Any, *, chunk_id: str | None = None) -> np.ndarray:
    if isinstance(samples, (bytes, bytearray, memoryview)):
        raw = bytes(samples)
        if len(raw) % 2:
            raise input_length(
                "PCM byte chunks must contain complete 16-bit samples",
                chunk_id=chunk_id,
                sample_position=len(raw) // 2,
                actual_bytes=len(raw),
            )
        return np.frombuffer(raw, dtype="<i2").astype(np.int16, copy=False)
    array = np.asarray(samples)
    if array.ndim != 1:
        raise input_length(
            "PCM input must be one-dimensional",
            chunk_id=chunk_id,
            sample_position=0,
            shape=list(array.shape),
        )
    if array.dtype != np.int16:
        if np.issubdtype(array.dtype, np.integer):
            minimum = int(array.min()) if array.size else 0
            maximum = int(array.max()) if array.size else 0
            if minimum < -32768 or maximum > 32767:
                raise VadError(
                    "E_FEATURE_MISMATCH",
                    "integer PCM values exceed the 16-bit range",
                    chunk_id=chunk_id,
                    details={"minimum": minimum, "maximum": maximum},
                )
            return array.astype(np.int16)
        raise VadError(
            "E_FEATURE_MISMATCH",
            "PCM input must use a 16-bit integer dtype",
            chunk_id=chunk_id,
            details={"dtype": str(array.dtype)},
        )
    return array


class StreamingVAD:
    """Incremental VAD session.

    Feed arbitrary sample blocks through :meth:`feed_samples`, or pre-framed
    windows through :meth:`feed_frame`. All state transitions, smoothing, and
    endpoint publication happen in exactly one code path.
    """

    def __init__(
        self,
        config: VADConfig | None = None,
        *,
        chunk_id: str | None = None,
        expected_sample_count: int | None = None,
        inject_feature_dimensions: int | None = None,
        reject_clipping: bool = True,
    ) -> None:
        self.config = (config or VADConfig()).validate()
        if expected_sample_count is not None and expected_sample_count < 0:
            raise VadError(
                "E_INPUT_LENGTH",
                "expected_sample_count must be non-negative",
                chunk_id=chunk_id,
                sample_position=0,
                details={"expected_sample_count": expected_sample_count},
            )
        self.chunk_id = chunk_id
        self.expected_sample_count = expected_sample_count
        self.inject_feature_dimensions = inject_feature_dimensions
        self.reject_clipping = reject_clipping

        self._buffer = np.empty(0, dtype=np.int16)
        self._buffer_origin = 0
        self._next_frame_start = 0
        self._total_received = 0
        self._all_zero = True
        self._clip_run = 0
        self._clip_run_start: int | None = None
        self._mode: str | None = None
        self._direct_frame_start = 0

        self._raw_speech = False
        self._candidate: _Candidate | None = None
        self._endpoints: list[Endpoint] = []
        self._frames: list[FrameDecision] = []
        self._finalized = False
        self._result: DetectionResult | None = None

    @property
    def total_received(self) -> int:
        return self._total_received

    @property
    def raw_state(self) -> str:
        return "speech" if self._raw_speech else "silence"

    @property
    def frames(self) -> tuple[FrameDecision, ...]:
        return tuple(self._frames)

    @property
    def published_endpoints(self) -> tuple[Endpoint, ...]:
        return tuple(self._endpoints)

    @property
    def tentative_endpoints(self) -> tuple[dict[str, Any], ...]:
        if self._candidate is None:
            return ()
        candidate = self._candidate
        end_sample = candidate.last_speech_end_sample
        status = "pending_silence" if candidate.gap_start_sample is not None else "open"
        return (
            {
                "status": "tentative",
                "candidate_state": status,
                "start_ms": _ms(candidate.start_sample, self.config.sample_rate),
                "end_ms": _ms(end_sample, self.config.sample_rate),
                "start_sample": candidate.start_sample,
                "end_sample": end_sample,
            },
        )

    def _assert_not_finalized(self) -> None:
        if self._finalized:
            raise VadError(
                "E_INVALID_STAGE",
                "stream session has already been finalized",
                chunk_id=self.chunk_id,
                sample_position=self._total_received,
            )

    def _set_mode(self, mode: str) -> None:
        if self._mode is None:
            self._mode = mode
        elif self._mode != mode:
            raise VadError(
                "E_INVALID_STAGE",
                "feed_samples and feed_frame cannot be mixed in one session",
                chunk_id=self.chunk_id,
                sample_position=self._total_received,
            )

    def _check_clipping(self, chunk: np.ndarray) -> None:
        if not self.reject_clipping:
            return
        peak = self.config.clip_peak_abs
        run = self._clip_run
        run_start = self._clip_run_start
        for offset, value in enumerate(chunk.tolist()):
            if abs(int(value)) >= peak:
                if run == 0:
                    run_start = self._total_received + offset
                run += 1
                if run >= self.config.clip_run_samples:
                    raise VadError(
                        "E_FEATURE_MISMATCH",
                        "hard-clipped PCM run invalidates feature extraction",
                        chunk_id=self.chunk_id,
                        sample_position=run_start,
                        details={
                            "clipped_samples": run,
                            "clip_peak_abs": peak,
                            "required_run": self.config.clip_run_samples,
                        },
                    )
            else:
                run = 0
                run_start = None
        self._clip_run = run
        self._clip_run_start = run_start

    def feed_samples(self, samples: Any) -> tuple[Endpoint, ...]:
        self._assert_not_finalized()
        self._set_mode("samples")
        chunk = coerce_pcm16(samples, chunk_id=self.chunk_id)
        if chunk.size == 0:
            return ()
        self._check_clipping(chunk)
        if self._all_zero and np.any(chunk != 0):
            self._all_zero = False
        if chunk.size:
            self._buffer = np.concatenate((self._buffer, chunk))
        self._total_received += int(chunk.size)
        before = len(self._endpoints)
        self._drain_full_frames()
        self._prune_buffer()
        return tuple(self._endpoints[before:])

    def feed_frame(self, frame: Any, *, is_short: bool = False) -> tuple[Endpoint, ...]:
        self._assert_not_finalized()
        self._set_mode("frames")
        chunk = coerce_pcm16(frame, chunk_id=self.chunk_id)
        if chunk.size == 0:
            raise VadError(
                "E_FEATURE_MISMATCH",
                "empty frame cannot be fed",
                chunk_id=self.chunk_id,
                sample_position=self._direct_frame_start,
                frame_index=len(self._frames),
            )
        if chunk.size > self.config.frame_samples:
            raise input_length(
                "pre-framed input exceeds the configured frame length",
                chunk_id=self.chunk_id,
                sample_position=self._total_received,
                actual_frame_samples=int(chunk.size),
                expected_frame_samples=self.config.frame_samples,
            )
        self._check_clipping(chunk)
        if self._all_zero and np.any(chunk != 0):
            self._all_zero = False
        start = self._direct_frame_start
        end = start + int(chunk.size)
        frame_info = FrameInfo(
            index=len(self._frames),
            start_sample=start,
            end_sample=end,
            is_short=is_short or chunk.size < self.config.frame_samples,
            samples=chunk,
        )
        before = len(self._endpoints)
        self._process_frame(frame_info)
        self._direct_frame_start += self.config.hop_samples
        self._total_received = max(self._total_received, end)
        return tuple(self._endpoints[before:])

    def _drain_full_frames(self) -> None:
        needed = self.config.frame_samples
        while self._next_frame_start + needed <= self._total_received:
            local = self._next_frame_start - self._buffer_origin
            if local < 0:
                raise VadError(
                    "E_INVALID_STAGE",
                    "internal frame buffer lost a required sample",
                    chunk_id=self.chunk_id,
                    sample_position=self._next_frame_start,
                )
            samples = self._buffer[local : local + needed]
            frame = FrameInfo(
                index=len(self._frames),
                start_sample=self._next_frame_start,
                end_sample=self._next_frame_start + needed,
                is_short=False,
                samples=samples,
            )
            self._process_frame(frame)
            self._next_frame_start += self.config.hop_samples

    def _flush_trailing_frames(self) -> None:
        while self._next_frame_start < self._total_received:
            local = self._next_frame_start - self._buffer_origin
            available = self._total_received - self._next_frame_start
            length = min(self.config.frame_samples, available)
            samples = self._buffer[local : local + length]
            frame = FrameInfo(
                index=len(self._frames),
                start_sample=self._next_frame_start,
                end_sample=self._next_frame_start + length,
                is_short=length < self.config.frame_samples,
                samples=samples,
            )
            self._process_frame(frame)
            self._next_frame_start += self.config.hop_samples

    def _prune_buffer(self) -> None:
        keep_from = self._next_frame_start
        remove = keep_from - self._buffer_origin
        if remove > 0:
            self._buffer = self._buffer[remove:].copy()
            self._buffer_origin = keep_from

    def _push_endpoint(self, start: _Candidate, end_sample: int) -> Endpoint:
        endpoint = Endpoint(
            start_ms=_ms(start.start_sample, self.config.sample_rate),
            end_ms=_ms(end_sample, self.config.sample_rate),
            start_sample=start.start_sample,
            end_sample=end_sample,
            start_frame=start.start_frame,
            end_frame=start.last_speech_end_frame,
        )
        self._endpoints.append(endpoint)
        return endpoint

    def _maybe_confirm_gap(self, frame_end_sample: int, reason: str) -> str:
        candidate = self._candidate
        if candidate is None or candidate.gap_start_sample is None:
            return reason
        silence_samples = frame_end_sample - candidate.gap_start_sample
        if silence_samples < self.config.min_silence_samples:
            return f"{reason};hold_min_silence"
        speech_samples = (
            candidate.last_speech_end_sample - candidate.start_sample
        )
        if speech_samples >= self.config.min_speech_samples:
            self._push_endpoint(candidate, candidate.last_speech_end_sample)
            self._candidate = None
            return f"{reason};confirm_min_silence"
        self._candidate = None
        return f"{reason};discard_below_min_speech"

    def _process_frame(self, frame: FrameInfo) -> None:
        try:
            features = score_frame(
                frame,
                self.config,
                injected_dimensions=self.inject_feature_dimensions,
            )
        except VadError as exc:
            if exc.chunk_id is None:
                exc.chunk_id = self.chunk_id
            raise
        score = features.weighted_score
        was_speech = self._raw_speech

        if not was_speech and score >= self.config.high_threshold:
            self._raw_speech = True
            if self._candidate is None:
                self._candidate = _Candidate(
                    start_sample=frame.start_sample,
                    start_frame=frame.index,
                    last_speech_end_sample=frame.end_sample,
                    last_speech_end_frame=frame.index,
                )
                reason = "enter_high_threshold"
            else:
                self._candidate.gap_start_sample = None
                self._candidate.last_speech_end_sample = frame.end_sample
                self._candidate.last_speech_end_frame = frame.index
                reason = "reenter_high_threshold_bridge"
        elif was_speech and score < self.config.low_threshold:
            self._raw_speech = False
            if self._candidate is not None:
                self._candidate.gap_start_sample = frame.start_sample
            reason = "exit_low_threshold"
        elif was_speech:
            if self._candidate is not None:
                self._candidate.last_speech_end_sample = frame.end_sample
                self._candidate.last_speech_end_frame = frame.index
            reason = "speech_hold"
        else:
            reason = (
                "silence_below_low_threshold"
                if score < self.config.low_threshold
                else "silence_between_thresholds"
            )

        if not self._raw_speech:
            reason = self._maybe_confirm_gap(frame.end_sample, reason)

        smoothing_state = "candidate" if self._candidate is not None else "silence"
        self._frames.append(
            FrameDecision(
                frame_index=frame.index,
                start_sample=frame.start_sample,
                end_sample=frame.end_sample,
                start_ms=_ms(frame.start_sample, self.config.sample_rate),
                end_ms=_ms(frame.end_sample, self.config.sample_rate),
                is_short=frame.is_short,
                raw_state="speech" if self._raw_speech else "silence",
                smoothing_state=smoothing_state,
                reason=reason,
                features=features,
            )
        )

    def finalize(self) -> DetectionResult:
        self._assert_not_finalized()
        if self.expected_sample_count is not None and self._total_received != self.expected_sample_count:
            raise input_length(
                "declared sample count does not match the received PCM length",
                chunk_id=self.chunk_id,
                sample_position=min(self._total_received, self.expected_sample_count),
                declared_samples=self.expected_sample_count,
                actual_samples=self._total_received,
            )
        if self._total_received == 0 or self._all_zero:
            raise VadError(
                "E_SILENT_INPUT",
                "input contains only digital silence",
                chunk_id=self.chunk_id,
                sample_position=0,
                details={"actual_samples": self._total_received},
            )

        if self._mode == "samples":
            self._flush_trailing_frames()
        elif self._mode == "frames" and self._candidate is not None:
            # A direct frame feed has no hidden trailing samples; finalization
            # only decides the currently open candidate.
            pass

        if self._candidate is not None:
            candidate = self._candidate
            end_sample = candidate.last_speech_end_sample
            speech_samples = end_sample - candidate.start_sample
            if speech_samples >= self.config.min_speech_samples:
                self._push_endpoint(candidate, end_sample)
            self._candidate = None

        self._finalized = True
        self._result = DetectionResult(
            chunk_id=self.chunk_id,
            sample_count=self._total_received,
            frame_count=len(self._frames),
            truncated_frame_count=sum(frame.is_short for frame in self._frames),
            config_fingerprint=self.config.fingerprint,
            endpoints=tuple(self._endpoints),
            frames=tuple(self._frames),
        )
        return self._result


def detect_samples(
    samples: Any,
    config: VADConfig | None = None,
    *,
    chunk_id: str | None = None,
    expected_sample_count: int | None = None,
    inject_feature_dimensions: int | None = None,
    reject_clipping: bool = True,
) -> DetectionResult:
    """Batch entry point implemented through the streaming engine."""

    session = StreamingVAD(
        config,
        chunk_id=chunk_id,
        expected_sample_count=expected_sample_count,
        inject_feature_dimensions=inject_feature_dimensions,
        reject_clipping=reject_clipping,
    )
    session.feed_samples(coerce_pcm16(samples, chunk_id=chunk_id))
    return session.finalize()


def stream_frames(
    frames: Iterator[np.ndarray],
    config: VADConfig | None = None,
    *,
    chunk_id: str | None = None,
    inject_feature_dimensions: int | None = None,
) -> DetectionResult:
    """Process an iterator of pre-framed windows through the same engine."""

    session = StreamingVAD(
        config,
        chunk_id=chunk_id,
        inject_feature_dimensions=inject_feature_dimensions,
    )
    for frame in frames:
        session.feed_frame(frame)
    return session.finalize()

