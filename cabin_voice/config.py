from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .canonical import digest_obj
from .errors import invalid_argument

ZONE_ORDER = ("driver", "passenger", "rear_left", "rear_right")
ZONE_LABELS = {
    "driver": "主驾",
    "passenger": "副驾",
    "rear_left": "后排左",
    "rear_right": "后排右",
}


@dataclass(frozen=True)
class VadConfig:
    frame_ms: float = 32.0
    hop_ms: float = 10.0
    energy_floor_db: float = -55.0
    energy_ceiling_db: float = -15.0
    energy_weight: float = 0.65
    flatness_weight: float = 0.35
    enter_threshold: float = 0.62
    exit_threshold: float = 0.46
    min_speech_ms: int = 50
    min_silence_ms: int = 60
    frame_epsilon: float = 1e-12

    def validate(self) -> None:
        if self.frame_ms <= 0 or self.hop_ms <= 0:
            raise invalid_argument("frame_ms and hop_ms must be positive")
        if self.hop_ms > self.frame_ms:
            raise invalid_argument("hop_ms must not exceed frame_ms")
        if self.energy_ceiling_db <= self.energy_floor_db:
            raise invalid_argument("energy_ceiling_db must exceed energy_floor_db")
        if self.energy_weight < 0 or self.flatness_weight < 0:
            raise invalid_argument("feature weights must be non-negative")
        if self.energy_weight + self.flatness_weight <= 0:
            raise invalid_argument("at least one feature weight must be positive")
        if not 0 <= self.exit_threshold <= 1:
            raise invalid_argument("exit_threshold must be within [0, 1]")
        if not 0 <= self.enter_threshold <= 1:
            raise invalid_argument("enter_threshold must be within [0, 1]")
        if self.enter_threshold <= self.exit_threshold:
            raise invalid_argument("enter_threshold must exceed exit_threshold")
        if self.min_speech_ms < 0 or self.min_silence_ms < 0:
            raise invalid_argument("minimum segment durations must be non-negative")


@dataclass(frozen=True)
class ArbitrationConfig:
    speed_of_sound_m_s: float = 343.0
    geometry_m: dict[str, tuple[float, float, float]] = field(
        default_factory=lambda: {
            "driver": (0.0, 0.0, 0.0),
            "passenger": (2.5, 0.0, 0.0),
            "rear_left": (0.0, 2.5, 0.0),
            "rear_right": (2.5, 2.5, 0.0),
        }
    )
    priority_weight: float = 0.20
    hit_weight: float = 0.48
    tdoa_weight: float = 0.32
    tdoa_tolerance_ms: float = 12.0
    review_margin: float = 0.07
    cluster_window_ms: int = 350
    hit_search_window_ms: int = 300
    neutral_tdoa_score: float = 0.5

    def validate(self) -> None:
        if self.speed_of_sound_m_s <= 0:
            raise invalid_argument("speed_of_sound_m_s must be positive")
        if set(self.geometry_m) != set(ZONE_ORDER):
            raise invalid_argument(
                "geometry_m must define exactly the four cabin zones",
                expected=list(ZONE_ORDER),
                actual=sorted(self.geometry_m),
            )
        if self.priority_weight < 0 or self.hit_weight < 0 or self.tdoa_weight < 0:
            raise invalid_argument("arbitration weights must be non-negative")
        total = self.priority_weight + self.hit_weight + self.tdoa_weight
        if abs(total - 1.0) > 1e-9:
            raise invalid_argument("arbitration weights must sum to 1")
        if self.tdoa_tolerance_ms <= 0:
            raise invalid_argument("tdoa_tolerance_ms must be positive")
        if self.review_margin < 0:
            raise invalid_argument("review_margin must be non-negative")
        if self.cluster_window_ms < 0 or self.hit_search_window_ms < 0:
            raise invalid_argument("search windows must be non-negative")


@dataclass(frozen=True)
class AppConfig:
    vad: VadConfig = field(default_factory=VadConfig)
    arbitration: ArbitrationConfig = field(default_factory=ArbitrationConfig)
    query_budget_ms: int = 20

    def validate(self) -> None:
        self.vad.validate()
        self.arbitration.validate()
        if self.query_budget_ms < 0:
            raise invalid_argument("query_budget_ms must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def phase_digest(self, phase: str) -> str:
        if phase == "vad":
            return digest_obj({"vad": asdict(self.vad)})
        if phase == "arbitrate":
            return digest_obj(
                {
                    "arbitration": asdict(self.arbitration),
                }
            )
        return digest_obj(asdict(self))


PHASE_PATCH_PATHS: dict[str, set[str]] = {
    "vad": {
        "frame_ms",
        "hop_ms",
        "energy_floor_db",
        "energy_ceiling_db",
        "energy_weight",
        "flatness_weight",
        "enter_threshold",
        "exit_threshold",
        "min_speech_ms",
        "min_silence_ms",
    },
    "arbitration": {
        "speed_of_sound_m_s",
        "geometry_m",
        "priority_weight",
        "hit_weight",
        "tdoa_weight",
        "tdoa_tolerance_ms",
        "review_margin",
        "cluster_window_ms",
        "hit_search_window_ms",
        "neutral_tdoa_score",
    },
    "query": {"query_budget_ms"},
}


def config_from_dict(value: dict[str, Any] | None) -> AppConfig:
    if not value:
        config = AppConfig()
        config.validate()
        return config
    if not isinstance(value, dict):
        raise invalid_argument("configuration must be an object")
    vad_raw = dict(value.get("vad", {}))
    arb_raw = dict(value.get("arbitration", {}))
    try:
        query_budget = int(value.get("query_budget_ms", 20))
    except (TypeError, ValueError) as exc:
        raise invalid_argument("query_budget_ms must be an integer") from exc
    vad_fields = set(VadConfig.__dataclass_fields__)
    arb_fields = set(ArbitrationConfig.__dataclass_fields__)
    unknown_vad = set(vad_raw) - vad_fields
    unknown_arb = set(arb_raw) - arb_fields
    if unknown_vad or unknown_arb:
        raise invalid_argument(
            "unknown configuration fields",
            unknown_vad=sorted(unknown_vad),
            unknown_arbitration=sorted(unknown_arb),
        )
    if "geometry_m" in arb_raw:
        arb_raw["geometry_m"] = {
            key: tuple(float(part) for part in position)
            for key, position in arb_raw["geometry_m"].items()
        }
    try:
        config = AppConfig(
            vad=VadConfig(**vad_raw),
            arbitration=ArbitrationConfig(**arb_raw),
            query_budget_ms=query_budget,
        )
    except (TypeError, ValueError) as exc:
        raise invalid_argument("invalid configuration value", reason=str(exc)) from exc
    config.validate()
    return config


def patch_config(config: AppConfig, patch: dict[str, Any]) -> AppConfig:
    raw = config.to_dict()
    if "query_budget_ms" in patch:
        raw["query_budget_ms"] = patch["query_budget_ms"]
    for section in ("vad", "arbitration"):
        if section not in patch:
            continue
        if not isinstance(patch[section], dict):
            raise invalid_argument(f"{section} patch must be an object")
        unknown = set(patch[section]) - PHASE_PATCH_PATHS[section]
        if unknown:
            raise invalid_argument(
                f"unsupported {section} configuration fields",
                fields=sorted(unknown),
            )
        raw[section].update(patch[section])
    unknown_top = set(patch) - {"vad", "arbitration", "query_budget_ms"}
    if unknown_top:
        raise invalid_argument("unsupported configuration sections", fields=sorted(unknown_top))
    return config_from_dict(raw)
