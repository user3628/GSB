"""Fixed-hash embedding and byte-stable streaming acoustic predictor."""

from __future__ import annotations

import hashlib
import io
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np

from .config import EngineConfig
from .errors import numeric_error
from .frontend import FrontendResult, PhonemeToken, TextChunk
from .utils import atomic_write_bytes, sha256_bytes


PARAMETER_NAMES = ("f0_hz", "envelope")


@dataclass(frozen=True)
class Prosody:
    rate: float = 1.0
    energy: float = 1.0
    pitch: float = 1.0
    complexity: float = 1.0

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "Prosody":
        raw = value or {}
        return cls(
            rate=float(raw.get("rate", 1.0)),
            energy=float(raw.get("energy", 1.0)),
            pitch=float(raw.get("pitch", 1.0)),
            complexity=float(raw.get("complexity", 1.0)),
        )

    def vector(self) -> list[float]:
        return [
            max(0.25, min(4.0, self.rate)),
            max(0.0, min(4.0, self.energy)),
            max(0.25, min(4.0, self.pitch)),
            max(0.0, min(128.0, self.complexity)),
            1.0,
        ]


@dataclass(frozen=True)
class AcousticResult:
    utt_id: str
    frame_start: int
    frame_end: int
    values: np.ndarray  # float64, shape (frame_end-frame_start, n_bands+2), C order
    markers: tuple[str, ...]

    @property
    def n_frames(self) -> int:
        return int(self.values.shape[0])

    @property
    def parameter_names(self) -> tuple[str, ...]:
        return tuple(f"band_{index}" for index in range(self.values.shape[1] - 2)) + PARAMETER_NAMES

    @property
    def bytes(self) -> bytes:
        return np.ascontiguousarray(self.values, dtype="<f8").tobytes(order="C")

    @property
    def digest(self) -> str:
        return sha256_bytes(self.bytes)


def _kind_one_hot(kind: str) -> list[float]:
    return [
        1.0 if kind == "cjk" else 0.0,
        1.0 if kind in {"latin", "digit"} else 0.0,
        1.0 if kind == "pause" else 0.0,
        1.0 if kind == "space" else 0.0,
    ]


def _hash_embedding(
    *, seed: int, speaker: int, phone: str, global_index: int, frame_in_phone: int, dim: int
) -> list[float]:
    key = f"{seed}|{speaker}|{phone}|{global_index}|{frame_in_phone}".encode("utf-8")
    data = hashlib.shake_256(key).digest(dim * 8)
    result: list[float] = []
    for i in range(dim):
        value = int.from_bytes(data[i * 8 : (i + 1) * 8], "little", signed=False)
        result.append((value / float((1 << 64) - 1)) * 2.0 - 1.0)
    return result


def _embedding_key(
    *, seed: int, speaker: int, phone: str, global_index: int, frame_in_phone: int
) -> tuple[int, int, str, int, int]:
    return (seed, speaker, phone, global_index, frame_in_phone)


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        z = math.exp(-min(value, 60.0))
        return 1.0 / (1.0 + z)
    z = math.exp(max(value, -60.0))
    return z / (1.0 + z)


def _linear_fixed_order(x: Sequence[float], weight: np.ndarray, bias: np.ndarray) -> list[float]:
    """Matrix-vector product with a single, fixed accumulation order."""

    n_in, n_out = weight.shape
    out = [0.0] * n_out
    for j in range(n_out):
        acc = float(bias[j])
        for i in range(n_in):
            acc += float(x[i]) * float(weight[i, j])
        out[j] = acc
    return out


def generate_weights(config: EngineConfig) -> dict[str, np.ndarray]:
    """Generate every model tensor from the configured seed.

    The digest is defined over canonical tensor names, dtypes, shapes and bytes,
    not over ZIP metadata in the .npz container.
    """

    rng = np.random.default_rng(config.model.seed)
    dims = [config.model.input_dim, *config.model.hidden_dims, config.model.output_dim]
    arrays: dict[str, np.ndarray] = {}
    for layer in range(len(dims) - 1):
        n_in, n_out = dims[layer], dims[layer + 1]
        scale = 1.0 / math.sqrt(float(n_in))
        arrays[f"W{layer}"] = np.ascontiguousarray(
            rng.uniform(-scale, scale, size=(n_in, n_out)), dtype="<f8"
        )
        arrays[f"b{layer}"] = np.ascontiguousarray(
            rng.uniform(-0.15, 0.15, size=(n_out,)), dtype="<f8"
        )
    return arrays


def weights_digest(arrays: dict[str, np.ndarray]) -> str:
    h = hashlib.sha256()
    for name in sorted(arrays):
        array = np.ascontiguousarray(arrays[name], dtype="<f8")
        h.update(name.encode("utf-8"))
        h.update(str(array.shape).encode("ascii"))
        h.update(array.tobytes(order="C"))
    return h.hexdigest()


def _write_weights(path: Path, arrays: dict[str, np.ndarray]) -> None:
    buffer = io.BytesIO()
    np.savez_compressed(buffer, **arrays)
    atomic_write_bytes(path, buffer.getvalue())


class AcousticPredictor:
    def __init__(self, config: EngineConfig, weights_path: Path | None = None):
        self.config = config
        self.weights_path = weights_path or config.resolved_path("acoustic_weights")
        expected = generate_weights(config)
        try:
            with np.load(self.weights_path, allow_pickle=False) as npz:
                loaded = {
                    name: np.ascontiguousarray(npz[name], dtype="<f8") for name in npz.files
                }
        except (FileNotFoundError, OSError, ValueError, KeyError):
            loaded = {}
        structurally_compatible = set(loaded) == set(expected) and all(
            loaded[name].shape == expected[name].shape for name in expected
        )
        if not structurally_compatible:
            # A changed model structure (or a missing weight file) is rebuilt from
            # the configured fixed seed. Existing same-shape weights are preserved
            # so externally supplied weight revisions remain detectable by digest.
            _write_weights(self.weights_path, expected)
            loaded = expected
        self.weights = loaded
        self.weight_digest = weights_digest(self.weights)
        self.layer_count = len(self.weights) // 2

    def _forward_frame(self, input_vector: list[float], prosody: Prosody) -> list[float]:
        current = input_vector
        for layer in range(self.layer_count):
            current = _linear_fixed_order(current, self.weights[f"W{layer}"], self.weights[f"b{layer}"])
            if layer != self.layer_count - 1:
                current = [math.tanh(value) for value in current]
        return current

    def _predict_phone(
        self,
        *,
        phone: PhonemeToken,
        speaker: int,
        prosody: Prosody,
        embedding_cache: dict[tuple[int, int, str, int, int], list[float]] | None = None,
    ) -> tuple[list[list[float]], list[str]]:
        rows: list[list[float]] = []
        markers: list[str] = []
        prosody_values = prosody.vector()
        kind = _kind_one_hot(phone.kind)
        speaker_offset = 1.0 + 0.04 * float((speaker - 1) % 4)
        for frame_in_phone in range(phone.frames):
            key = _embedding_key(
                seed=self.config.model.seed,
                speaker=speaker,
                phone=phone.phone,
                global_index=phone.global_index,
                frame_in_phone=frame_in_phone,
            )
            embedding = (
                embedding_cache.get(key)
                if embedding_cache is not None
                else None
            )
            if embedding is None:
                embedding = _hash_embedding(
                    seed=self.config.model.seed,
                    speaker=speaker,
                    phone=phone.phone,
                    global_index=phone.global_index,
                    frame_in_phone=frame_in_phone,
                    dim=self.config.model.text_embedding_dim,
                )
            raw = self._forward_frame(embedding + prosody_values + kind, prosody)
            bands = []
            for index in range(self.config.model.n_bands):
                tilt = 1.0 - self.config.synthesis.harmonic_rolloff * (
                    index / max(1, self.config.model.n_bands - 1)
                )
                bands.append(_sigmoid(raw[index] * 1.7) * max(0.0, prosody.energy) * tilt)
            f0_unit = _sigmoid(raw[self.config.model.n_bands])
            f0 = 70.0 * math.exp(math.log(520.0 / 70.0) * f0_unit)
            f0 *= speaker_offset * max(0.25, prosody.pitch)
            envelope = _sigmoid(raw[self.config.model.n_bands + 1] * 1.4)
            envelope *= min(2.0, max(0.05, prosody.energy))
            rows.append([*bands, f0, envelope])
            marker = phone.marker
            if phone.kind in {"cjk", "latin", "digit"} and prosody.energy >= 1.08:
                marker = f"{marker}+ACCENT"
            markers.append(marker)
        return rows, markers

    def prepare_embeddings(
        self,
        *,
        phones: Iterable[PhonemeToken],
        speaker: int,
        workers: int,
        depth: int,
    ) -> dict[tuple[int, int, str, int, int], list[float]]:
        """Warm fixed-hash features using the scheduler's worker/depth decision.

        Multiple depth passes intentionally repeat identical work; depth models
        optional front-end prefetch levels while never changing frame values.
        The result is assembled in submission order, so even a multi-worker
        warmup cannot alter floating-point evaluation order.
        """

        tasks: list[tuple[tuple[int, int, str, int, int], int]] = []
        for phone in phones:
            for frame_in_phone in range(phone.frames):
                key = _embedding_key(
                    seed=self.config.model.seed,
                    speaker=speaker,
                    phone=phone.phone,
                    global_index=phone.global_index,
                    frame_in_phone=frame_in_phone,
                )
                tasks.append((key, frame_in_phone))
        if not tasks:
            return {}
        passes = max(0, int(depth))
        if passes == 0:
            return {}
        cache: dict[tuple[int, int, str, int, int], list[float]] = {}
        worker_count = max(1, min(int(workers), len(tasks)))

        def compute(item: tuple[tuple[int, int, str, int, int], int]) -> tuple[tuple[int, int, str, int, int], list[float]]:
            key, frame_in_phone = item
            return (
                key,
                _hash_embedding(
                    seed=self.config.model.seed,
                    speaker=speaker,
                    phone=key[2],
                    global_index=key[3],
                    frame_in_phone=frame_in_phone,
                    dim=self.config.model.text_embedding_dim,
                ),
            )

        for _ in range(passes):
            if worker_count == 1:
                pairs = [compute(item) for item in tasks]
            else:
                with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="tts-preprocess") as pool:
                    pairs = list(pool.map(compute, tasks))
            cache = dict(pairs)
        return cache

    def predict_phones(
        self,
        *,
        utt_id: str,
        phones: Iterable[PhonemeToken],
        speaker: int,
        prosody: Prosody,
        frame_start: int = 0,
        embedding_cache: dict[tuple[int, int, str, int, int], list[float]] | None = None,
    ) -> AcousticResult:
        rows: list[list[float]] = []
        markers: list[str] = []
        for phone in phones:
            phone_rows, phone_markers = self._predict_phone(
                phone=phone,
                speaker=speaker,
                prosody=prosody,
                embedding_cache=embedding_cache,
            )
            rows.extend(phone_rows)
            markers.extend(phone_markers)
        values = np.ascontiguousarray(np.asarray(rows, dtype="<f8"), dtype="<f8")
        if values.ndim != 2 or values.shape[1] != self.config.model.output_dim:
            raise ValueError("acoustic output shape invariant failed")
        return AcousticResult(
            utt_id=utt_id,
            frame_start=frame_start,
            frame_end=frame_start + values.shape[0],
            values=values,
            markers=tuple(markers),
        )

    def predict_chunk(
        self, *, chunk: TextChunk, speaker: int, prosody: Prosody, frame_start: int
    ) -> AcousticResult:
        return self.predict_phones(
            utt_id="",
            phones=chunk.phonemes,
            speaker=speaker,
            prosody=prosody,
            frame_start=frame_start,
        )

    def predict_all(self, *, frontend: FrontendResult, speaker: int, prosody: Prosody) -> AcousticResult:
        return self.predict_phones(
            utt_id=frontend.utt_id,
            phones=frontend.phonemes,
            speaker=speaker,
            prosody=prosody,
            frame_start=0,
        )


def assert_finite(*, utt_id: str, values: np.ndarray, frame_offset: int = 0) -> None:
    if values.ndim != 2:
        raise ValueError("acoustic values must be a 2-D array")
    finite = np.isfinite(values)
    if bool(finite.all()):
        return
    local_frame, parameter_index = np.argwhere(~finite)[0]
    names = tuple(f"band_{index}" for index in range(values.shape[1] - 2)) + PARAMETER_NAMES
    raise numeric_error(
        utt_id=utt_id,
        frame_index=int(frame_offset + local_frame),
        parameter=names[int(parameter_index)],
        value=float(values[local_frame, parameter_index]),
        local_frame=int(local_frame),
    )


def acoustic_summary(*, values: np.ndarray, frame_start: int, markers: Sequence[str]) -> dict[str, Any]:
    bands = values.shape[1] - 2
    params: dict[str, Any] = {}
    for index in range(values.shape[1]):
        name = f"band_{index}" if index < bands else PARAMETER_NAMES[index - bands]
        column = values[:, index]
        params[name] = {
            "min": float(column.min()) if column.size else None,
            "max": float(column.max()) if column.size else None,
            "mean": float(column.mean()) if column.size else None,
        }
    return {
        "frame_start": frame_start,
        "frame_count": int(values.shape[0]),
        "shape": [int(x) for x in values.shape],
        "numeric_digest": sha256_bytes(np.ascontiguousarray(values, dtype="<f8").tobytes()),
        "parameters": params,
        "markers_digest": sha256_bytes("\n".join(markers).encode("utf-8")),
    }


def benchmark_unit_ms(
    config: EngineConfig,
    predictor: AcousticPredictor,
    *,
    repeats: int = 5,
    benchmark_frames: int = 96,
) -> dict[str, Any]:
    """Measure local forward-pass throughput for the frozen budget cost model."""

    sample_phones = [
        PhonemeToken(
            global_index=index,
            char_start=index,
            char_end=index + 1,
            char="测",
            phone=f"bench_{index % 7}",
            frames=4,
            kind="cjk" if index % 3 else "latin",
            marker="BENCH",
        )
        for index in range(max(1, math.ceil(benchmark_frames / 4)))
    ]
    prosody = Prosody()
    samples_ms: list[float] = []
    measured_frames = sum(phone.frames for phone in sample_phones)
    for _ in range(repeats):
        start = time.perf_counter_ns()
        predictor.predict_phones(
            utt_id="calibration",
            phones=sample_phones,
            speaker=1,
            prosody=prosody,
            frame_start=0,
        )
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        samples_ms.append(elapsed_ms / measured_frames)
    samples_ms.sort()
    median = samples_ms[len(samples_ms) // 2]
    quantized = max(0.0001, round(median, 5))
    return {
        "schema_version": 1,
        "benchmark_key": predictor.weight_digest + ":" + config.model_structure_digest(),
        "method": "median_of_repeats_forward_pass_per_frame",
        "repeats": repeats,
        "benchmark_frames": measured_frames,
        "samples_ms_per_frame": samples_ms,
        "unit_ms": quantized,
    }
