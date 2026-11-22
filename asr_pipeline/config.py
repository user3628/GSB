"""Frozen pipeline configuration with explicit ms -> sample conversion."""

from dataclasses import asdict, dataclass, field

from .errors import InvalidFrameError
from .hashing import digest_json

STAGES = ("frontend", "decode", "align", "summary")
TARGET_SAMPLE_RATE = 16000


@dataclass(frozen=True)
class FrameConfig:
    frame_ms: float = 25.0
    hop_ms: float = 10.0
    sample_rate: int = TARGET_SAMPLE_RATE
    bands: int = 8
    marker_bins: int = 20

    def validate(self):
        if self.frame_ms <= 0 or self.hop_ms <= 0:
            raise InvalidFrameError(
                "frame length and hop must be positive",
                {"frame_ms": self.frame_ms, "hop_ms": self.hop_ms},
            )
        if self.hop_ms > self.frame_ms:
            raise InvalidFrameError(
                "frame hop must not exceed frame length",
                {"frame_ms": self.frame_ms, "hop_ms": self.hop_ms},
            )
        length_samples = self.frame_ms * self.sample_rate / 1000.0
        hop_samples = self.hop_ms * self.sample_rate / 1000.0
        if abs(length_samples - round(length_samples)) > 1e-9:
            raise InvalidFrameError(
                "frame length does not convert to an integer sample count",
                {"frame_ms": self.frame_ms, "length_samples": length_samples},
            )
        if abs(hop_samples - round(hop_samples)) > 1e-9:
            raise InvalidFrameError(
                "frame hop does not convert to an integer sample count",
                {"hop_ms": self.hop_ms, "hop_samples": hop_samples},
            )
        if self.bands < 1:
            raise InvalidFrameError("band count must be positive", {"bands": self.bands})
        if self.marker_bins < len(__import__("asr_pipeline.vocabulary", fromlist=["VOCABULARY"]).VOCABULARY):
            raise InvalidFrameError(
                "marker bins must be at least the vocabulary size",
                {"marker_bins": self.marker_bins},
            )

    @property
    def frame_length(self):
        self.validate()
        return int(round(self.frame_ms * self.sample_rate / 1000.0))

    @property
    def hop_length(self):
        self.validate()
        return int(round(self.hop_ms * self.sample_rate / 1000.0))

    def to_dict(self):
        data = asdict(self)
        data["frame_length_samples"] = self.frame_length
        data["hop_length_samples"] = self.hop_length
        return data

    @property
    def feature_columns(self):
        return self.bands * 2 + self.marker_bins + 1


@dataclass(frozen=True)
class DecoderConfig:
    min_silence_frames: int = 5
    min_word_frames: int = 3
    voiced_energy_db: float = 45.0
    energy_scale: float = 6.0
    vocab_id: str = "default-v4"
    silence_marker_baseline: tuple = (618.707, 910.865, 873.64, 444.283, 904.863, 1208.8, 717.479, 610.494, 739.146, 1037.179, 732.425, 1062.447, 618.267, 750.34, 682.086, 980.181, 1024.111, 747.948, 1406.116, 1077.584)


@dataclass(frozen=True)
class AlignConfig:
    tolerance_ms: float = 40.0


@dataclass(frozen=True)
class BudgetConfig:
    global_frame_budget: int = 100000
    worker_frame_quota: int = 60000
    shard_frame_latency: int = 20000
    workers: int = 2


@dataclass(frozen=True)
class PipelineConfig:
    frame: FrameConfig = field(default_factory=FrameConfig)
    decoder: DecoderConfig = field(default_factory=DecoderConfig)
    align: AlignConfig = field(default_factory=AlignConfig)
    budget: BudgetConfig = field(default_factory=BudgetConfig)

    def to_dict(self):
        return {
            "frame": self.frame.to_dict(),
            "decoder": asdict(self.decoder),
            "align": asdict(self.align),
            "budget": asdict(self.budget),
        }

    def fingerprint(self):
        return digest_json(self.to_dict())


def estimate_frame_count(declared_samples, frame_config):
    """Pre-admission frame estimate derived from declared sample total."""
    frame_config.validate()
    if declared_samples <= 0:
        return 0
    hop = frame_config.hop_length
    length = frame_config.frame_length
    return 1 + max(0, (declared_samples - length + hop - 1) // hop)
