"""Pipeline configuration with a stable configuration version fingerprint."""

from dataclasses import dataclass, field, asdict

from .errors import BusinessError, E_INVALID_CONFIG
from .hashing import digest

DIMENSIONS = ("sharpness", "color", "structure", "semantic")


@dataclass(frozen=True)
class Config:
    weights: dict = field(
        default_factory=lambda: {
            "sharpness": 0.3,
            "color": 0.25,
            "structure": 0.2,
            "semantic": 0.25,
        }
    )
    thresholds: dict = field(default_factory=lambda: {"excellent": 0.75, "good": 0.45})
    fusion_domain: str = "log"
    epsilon: float = 1e-9

    def validate(self):
        for dimension in DIMENSIONS:
            if dimension not in self.weights:
                raise BusinessError(
                    E_INVALID_CONFIG,
                    f"missing weight for dimension {dimension}",
                    {"dimension": dimension},
                )
            if not isinstance(self.weights[dimension], (int, float)):
                raise BusinessError(
                    E_INVALID_CONFIG,
                    f"weight for {dimension} must be numeric",
                    {"dimension": dimension, "value": str(self.weights[dimension])},
                )
            if self.weights[dimension] < 0:
                raise BusinessError(
                    E_INVALID_CONFIG,
                    f"negative weight for dimension {dimension}",
                    {"dimension": dimension, "value": float(self.weights[dimension])},
                )
        if self.fusion_domain not in ("log", "linear"):
            raise BusinessError(
                E_INVALID_CONFIG,
                "fusion_domain must be 'log' or 'linear'",
                {"fusion_domain": self.fusion_domain},
            )
        excellent = self.thresholds.get("excellent")
        good = self.thresholds.get("good")
        if excellent is None or good is None or not (0 < good < excellent <= 1):
            raise BusinessError(
                E_INVALID_CONFIG,
                "thresholds must satisfy 0 < good < excellent <= 1",
                {"thresholds": self.thresholds},
            )
        return self

    def version(self):
        """Configuration fingerprint used inside every checkpoint record."""
        return digest(asdict(self))[0:16]
