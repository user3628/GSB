"""Workbench configuration objects and the three strategy tiers.

Configurations are plain, JSON-serializable dataclasses.  Their canonical
digest feeds the stage version fingerprints, so changing any parameter (binary
threshold, merge rule, budget, ...) triggers the correct invalidation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .hashing import digest_json

# Four ordered pipeline stages.
STAGES = ("detect", "decision", "recognize", "diagnose")

# Detection strategy tiers (light -> heavy).
DETECT_STRATEGIES = ("detect_fast", "detect_balanced", "detect_deep")
# Recognition strategy tiers (light -> heavy).
RECOGNIZE_STRATEGIES = ("recog_fast", "recog_balanced", "recog_deep")

# Heavy strategies are designed for the medium/large groups only.
DETECT_HEAVY = {"detect_deep"}
RECOGNIZE_HEAVY = {"recog_deep"}

# Deterministic configuration schema version (bumped on structural changes).
CONFIG_SCHEMA_VERSION = "ocrwb-config-v1"


@dataclass(frozen=True)
class BinarizeConfig:
    """Grayscale + fixed-threshold binarization parameters (foreground=1)."""

    threshold: int = 160
    invert_background: bool = True


@dataclass(frozen=True)
class MergeConfig:
    """Character connected-component -> line-box merge parameters."""

    min_component_area: int = 4
    min_component_height: int = 4
    row_band_tol: int = 4
    gap_ratio: float = 1.4
    pad_x: int = 1
    pad_y: int = 1


@dataclass(frozen=True)
class BudgetConfig:
    """Per-shard compute budgets (abstract deterministic cost units)."""

    detect_small: int = 260
    detect_medium: int = 900
    detect_large: int = 2600
    recognize_small: int = 240
    recognize_medium: int = 900
    recognize_large: int = 2600

    def detect_budget(self, group: str) -> int:
        return {
            "small": self.detect_small,
            "medium": self.detect_medium,
            "large": self.detect_large,
        }[group]

    def recognize_budget(self, group: str) -> int:
        return {
            "small": self.recognize_small,
            "medium": self.recognize_medium,
            "large": self.recognize_large,
        }[group]


@dataclass(frozen=True)
class WorkbenchConfig:
    binarize: BinarizeConfig = field(default_factory=BinarizeConfig)
    merge: MergeConfig = field(default_factory=MergeConfig)
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    iou_match_threshold: float = 0.5
    confidence_decimals: int = 6

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "WorkbenchConfig":
        return cls(
            binarize=BinarizeConfig(**data["binarize"]),
            merge=MergeConfig(**data["merge"]),
            budget=BudgetConfig(**data["budget"]),
            iou_match_threshold=data["iou_match_threshold"],
            confidence_decimals=data["confidence_decimals"],
        )

    def version(self) -> str:
        """Configuration version fingerprint including the schema version."""
        return digest_json({"schema": CONFIG_SCHEMA_VERSION, "config": self.to_dict()})[:16]


# Strategy-specific parameter overrides applied on top of MergeConfig.
# Each tier grows more permissive / more expensive.
DETECT_STRATEGY_PARAMS: dict[str, dict] = {
    "detect_fast": {
        "min_component_area": 3,
        "min_component_height": 4,
        "row_band_tol": 2,
        "gap_ratio": 1.15,
        "pad_x": 1,
        "pad_y": 1,
    },
    "detect_balanced": {
        "min_component_area": 4,
        "min_component_height": 4,
        "row_band_tol": 3,
        "gap_ratio": 1.3,
        "pad_x": 1,
        "pad_y": 1,
    },
    "detect_deep": {
        "min_component_area": 2,
        "min_component_height": 3,
        "row_band_tol": 4,
        "gap_ratio": 1.5,
        "pad_x": 2,
        "pad_y": 2,
    },
}
