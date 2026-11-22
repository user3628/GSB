"""Internal evaluation model and lossless v1/v2 configuration codecs."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

from .canonical import finite_float, fingerprint, read_json
from .errors import AudioQError


METRIC_NAMES = ("snr", "spectral", "clipping")


def _check_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AudioQError("E_CONFIG", f"{name} must be a positive integer", observed={name: value})
    return value


def _check_finite(value: Any, name: str, *, minimum: float | None = None) -> float:
    try:
        number = finite_float(value, name=name)
    except AudioQError as exc:
        raise AudioQError("E_CONFIG", f"{name} must be finite", observed={name: repr(value)}) from exc
    if minimum is not None and number < minimum:
        raise AudioQError("E_CONFIG", f"{name} must be >= {minimum}", observed={name: number})
    return number


@dataclass(frozen=True)
class FrameConfig:
    length: int = 400
    hop: int = 160
    window: str = "hann"

    def __post_init__(self) -> None:
        _check_positive_int(self.length, "frame.length")
        _check_positive_int(self.hop, "frame.hop")
        if self.window != "hann":
            raise AudioQError("E_CONFIG", "only the hann frame window is supported", observed={"window": self.window})

    def to_dict(self) -> dict[str, Any]:
        return {"length": self.length, "hop": self.hop, "window": self.window}


@dataclass(frozen=True)
class MetricsConfig:
    snr_threshold_db: float = 8.0
    spectral_epsilon: float = 1e-8
    spectral_scale_db: float = 20.0
    clipping_level: float = 0.98
    clipping_ratio_limit: float = 0.001
    clipping_scale_ratio: float = 0.08

    def __post_init__(self) -> None:
        _check_finite(self.snr_threshold_db, "metrics.snr_threshold_db")
        for name, value, minimum in (
            ("metrics.spectral_epsilon", self.spectral_epsilon, 1e-15),
            ("metrics.spectral_scale_db", self.spectral_scale_db, 1e-9),
            ("metrics.clipping_level", self.clipping_level, 0.0),
            ("metrics.clipping_ratio_limit", self.clipping_ratio_limit, 0.0),
            ("metrics.clipping_scale_ratio", self.clipping_scale_ratio, 1e-9),
        ):
            _check_finite(value, name, minimum=minimum)
        if self.clipping_level > 1.0:
            raise AudioQError("E_CONFIG", "metrics.clipping_level must be <= 1", observed={"value": self.clipping_level})

    def to_dict(self) -> dict[str, Any]:
        return {
            "snr_threshold_db": self.snr_threshold_db,
            "spectral_epsilon": self.spectral_epsilon,
            "spectral_scale_db": self.spectral_scale_db,
            "clipping_level": self.clipping_level,
            "clipping_ratio_limit": self.clipping_ratio_limit,
            "clipping_scale_ratio": self.clipping_scale_ratio,
        }


@dataclass(frozen=True)
class FusionConfig:
    domain: str = "log"
    epsilon: float = 1e-6
    decimals: int = 12
    high_precision: int = 50

    def __post_init__(self) -> None:
        if self.domain not in {"log", "normalized"}:
            raise AudioQError("E_CONFIG", "fusion.domain must be 'log' or 'normalized'", observed={"domain": self.domain})
        _check_finite(self.epsilon, "fusion.epsilon", minimum=1e-15)
        if isinstance(self.epsilon, bool) or not isinstance(self.epsilon, (int, float)):
            raise AudioQError("E_CONFIG", "fusion.epsilon must be numeric")
        if isinstance(self.decimals, bool) or not isinstance(self.decimals, int) or not 0 <= self.decimals <= 15:
            raise AudioQError("E_CONFIG", "fusion.decimals must be an integer in [0, 15]")
        if isinstance(self.high_precision, bool) or not isinstance(self.high_precision, int) or self.high_precision < 20:
            raise AudioQError("E_CONFIG", "fusion.high_precision must be an integer >= 20")

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "epsilon": self.epsilon,
            "decimals": self.decimals,
            "high_precision": self.high_precision,
        }


@dataclass(frozen=True)
class ReferenceConfig:
    min_mos: float = 0.0
    max_mos: float = 5.0
    target_mos: float = 4.0

    def __post_init__(self) -> None:
        low = _check_finite(self.min_mos, "reference.min_mos")
        high = _check_finite(self.max_mos, "reference.max_mos")
        target = _check_finite(self.target_mos, "reference.target_mos")
        if low > high:
            raise AudioQError("E_CONFIG", "reference minimum exceeds maximum", observed={"min": low, "max": high})
        if not low <= target <= high:
            raise AudioQError(
                "E_CONFIG",
                "reference target lies outside its interval",
                observed={"min": low, "max": high, "target": target},
            )

    def to_dict(self) -> dict[str, Any]:
        return {"min_mos": self.min_mos, "max_mos": self.max_mos, "target_mos": self.target_mos}


def _normalise_weights(raw: Mapping[str, Any], *, source: str) -> dict[str, float]:
    if not isinstance(raw, Mapping):
        raise AudioQError("E_INVALID_WEIGHT", "weights must be an object", observed={"source": source})
    unknown = sorted(set(raw) - set(METRIC_NAMES))
    if unknown:
        raise AudioQError("E_INVALID_WEIGHT", "unknown metric names in weights", observed={"unknown": unknown})
    weights: dict[str, float] = {}
    for name in METRIC_NAMES:
        if name not in raw:
            raise AudioQError("E_INVALID_WEIGHT", "a metric weight is missing", observed={"metric": name})
        value = raw[name]
        if isinstance(value, bool):
            raise AudioQError("E_INVALID_WEIGHT", "weight must be numeric", observed={"metric": name, "value": value})
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise AudioQError("E_INVALID_WEIGHT", "weight must be numeric", observed={"metric": name, "value": value}) from exc
        if not math.isfinite(number) or number < 0:
            raise AudioQError(
                "E_INVALID_WEIGHT",
                "weight must be finite and non-negative",
                observed={"metric": name, "value": repr(value)},
            )
        weights[name] = number
    total = sum(weights.values())
    if not math.isfinite(total) or total <= 0:
        raise AudioQError("E_INVALID_WEIGHT", "weight sum must be positive", observed={"sum": total})
    return {name: weights[name] / total for name in METRIC_NAMES}


def _normalise_percentages(
    raw: Mapping[str, Any],
    *,
    policy: str = "normalize",
) -> tuple[dict[str, float], int, list[str]]:
    if policy not in {"normalize", "strict_100", "divide_100"}:
        raise AudioQError("E_CONFIG", "unknown percentage normalization policy", observed={"policy": policy})
    if not isinstance(raw, Mapping):
        raise AudioQError("E_INVALID_WEIGHT", "weights_percent must be an object")
    unknown = sorted(set(raw) - set(METRIC_NAMES))
    if unknown:
        raise AudioQError("E_INVALID_WEIGHT", "unknown metric names in weights_percent", observed={"unknown": unknown})
    percentages: dict[str, int] = {}
    for name in METRIC_NAMES:
        if name not in raw:
            raise AudioQError("E_INVALID_WEIGHT", "a percentage weight is missing", observed={"metric": name})
        value = raw[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AudioQError(
                "E_INVALID_WEIGHT",
                "v1 weights must be non-negative integers",
                observed={"metric": name, "value": value},
            )
        percentages[name] = value
    total = sum(percentages.values())
    if total <= 0:
        raise AudioQError("E_INVALID_WEIGHT", "percentage weight sum must be positive", observed={"sum": total})
    if policy == "strict_100" and total != 100:
        raise AudioQError("E_INVALID_WEIGHT", "v1 weights must sum to 100", observed={"sum": total})
    divisor = 100.0 if policy == "divide_100" else float(total)
    weights = {name: percentages[name] / divisor for name in METRIC_NAMES}
    if abs(sum(weights.values()) - 1.0) > 1e-12:
        raise AudioQError("E_INVALID_WEIGHT", "normalized percentage weights are invalid", observed={"sum": sum(weights.values())})
    return weights, total, sorted(unknown)


def _derive_snr_threshold(weights: Mapping[str, float], rule: str, fixed: float) -> float:
    if rule == "fixed":
        return _check_finite(fixed, "compat.default_snr_threshold_db")
    if rule == "weight_scaled":
        return 4.0 + 10.0 * float(weights["snr"])
    raise AudioQError("E_CONFIG", "unknown SNR threshold derivation rule", observed={"rule": rule})


def _reference_from_mapping(value: Mapping[str, Any], *, context: str) -> ReferenceConfig:
    try:
        low = float(value.get("min_mos", value.get("min", 0.0)))
        high = float(value.get("max_mos", value.get("max", 5.0)))
        target_raw = value.get("target_mos", value.get("target"))
        target = (low + high) / 2.0 if target_raw is None else float(target_raw)
        return ReferenceConfig(min_mos=low, max_mos=high, target_mos=target)
    except (TypeError, ValueError) as exc:
        raise AudioQError(
            "E_CONFIG",
            f"{context} reference interval contains a non-numeric value",
            observed={"reference": dict(value), "reason": str(exc)},
        ) from exc


@dataclass
class EvaluationConfig:
    """The internal semantic model used by every stage.

    It deliberately retains ``schema_version`` so a decoded v1 document can be
    exported back to v1 without pretending it originated as v2.
    """

    evaluation_id: str
    frame: FrameConfig = field(default_factory=FrameConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    weights: Mapping[str, float] = field(default_factory=lambda: {"snr": 0.4, "spectral": 0.35, "clipping": 0.25})
    fusion: FusionConfig = field(default_factory=FusionConfig)
    default_reference: ReferenceConfig = field(default_factory=ReferenceConfig)
    schema_version: int = 2
    compatibility: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version not in {1, 2}:
            raise AudioQError("E_CONFIG", "schema_version must be 1 or 2", observed={"schema_version": self.schema_version})
        if not isinstance(self.evaluation_id, str) or not self.evaluation_id.strip():
            raise AudioQError("E_CONFIG", "evaluation_id must be a non-empty string")
        self.weights = _normalise_weights(self.weights, source=f"v{self.schema_version}")
        self.compatibility = dict(self.compatibility)

    def semantic_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "evaluation_id": self.evaluation_id,
            "frame": self.frame.to_dict(),
            "metrics": self.metrics.to_dict(),
            "weights": {name: self.weights[name] for name in METRIC_NAMES},
            "fusion": self.fusion.to_dict(),
            "default_reference": self.default_reference.to_dict(),
        }

    @property
    def version_fingerprint(self) -> str:
        return fingerprint(self.semantic_dict())

    def for_schema(self, schema_version: int) -> "EvaluationConfig":
        if schema_version not in {1, 2}:
            raise AudioQError("E_CONFIG", "target schema version must be 1 or 2")
        return replace(self, schema_version=schema_version)

    def to_v2_dict(self) -> dict[str, Any]:
        payload = self.semantic_dict()
        payload["schema_version"] = 2
        return payload

    def to_v1_dict(self) -> dict[str, Any]:
        percentages, weights_restored = _largest_remainder_percentages(self.weights)
        restore: dict[str, Any] = {"metrics.snr_threshold_db": self.metrics.snr_threshold_db}
        downgrade_items = ["metrics.snr_threshold_db"]
        if weights_restored:
            restore["weights"] = {name: self.weights[name] for name in METRIC_NAMES}
            downgrade_items.append("weights.integer_percent_precision")
        payload: dict[str, Any] = {
            "schema_version": 1,
            "evaluation_id": self.evaluation_id,
            "frame": self.frame.to_dict(),
            "metrics": {
                "spectral_epsilon": self.metrics.spectral_epsilon,
                "spectral_scale_db": self.metrics.spectral_scale_db,
                "clipping_level": self.metrics.clipping_level,
                "clipping_ratio_limit": self.metrics.clipping_ratio_limit,
                "clipping_scale_ratio": self.metrics.clipping_scale_ratio,
            },
            "weights_percent": percentages,
            "fusion": self.fusion.to_dict(),
            "default_reference": self.default_reference.to_dict(),
            "downgrade_items": downgrade_items,
            "downgrade_restore": restore,
        }
        return payload


def _largest_remainder_percentages(weights: Mapping[str, float]) -> tuple[dict[str, int], bool]:
    raw = {name: float(weights[name]) * 100.0 for name in METRIC_NAMES}
    floors = {name: int(math.floor(raw[name])) for name in METRIC_NAMES}
    remaining = 100 - sum(floors.values())
    order = sorted(METRIC_NAMES, key=lambda name: (-(raw[name] - floors[name]), METRIC_NAMES.index(name)))
    for name in order[:remaining]:
        floors[name] += 1
    restored = any(abs(floors[name] / 100.0 - float(weights[name])) > 1e-15 for name in METRIC_NAMES)
    return floors, restored


def decode_config_v2(value: Mapping[str, Any]) -> EvaluationConfig:
    if not isinstance(value, Mapping):
        raise AudioQError("E_CONFIG", "v2 configuration must be a JSON object")
    version = value.get("schema_version", value.get("version", 2))
    if version != 2:
        raise AudioQError("E_CONFIG", "not a version 2 configuration", observed={"schema_version": version})
    evaluation_id = value.get("evaluation_id", value.get("id", "default"))
    frame_raw = value.get("frame", {})
    metrics_raw = value.get("metrics", {})
    fusion_raw = value.get("fusion", {})
    reference_raw = value.get("default_reference", {"min_mos": 0.0, "max_mos": 5.0, "target_mos": 4.0})
    if not isinstance(frame_raw, Mapping) or not isinstance(metrics_raw, Mapping):
        raise AudioQError("E_CONFIG", "frame and metrics must be JSON objects")
    if not isinstance(fusion_raw, Mapping) or not isinstance(reference_raw, Mapping):
        raise AudioQError("E_CONFIG", "fusion and default_reference must be JSON objects")
    try:
        frame = FrameConfig(
            length=int(frame_raw.get("length", frame_raw.get("frame_length", 400))),
            hop=int(frame_raw.get("hop", frame_raw.get("hop_length", frame_raw.get("frame_shift", 160)))),
            window=str(frame_raw.get("window", "hann")),
        )
        metrics = MetricsConfig(
            snr_threshold_db=float(metrics_raw.get("snr_threshold_db", metrics_raw.get("snr_threshold", 8.0))),
            spectral_epsilon=float(metrics_raw.get("spectral_epsilon", 1e-8)),
            spectral_scale_db=float(metrics_raw.get("spectral_scale_db", 20.0)),
            clipping_level=float(metrics_raw.get("clipping_level", 0.98)),
            clipping_ratio_limit=float(metrics_raw.get("clipping_ratio_limit", 0.001)),
            clipping_scale_ratio=float(metrics_raw.get("clipping_scale_ratio", 0.08)),
        )
        fusion = FusionConfig(
            domain=str(fusion_raw.get("domain", "log")),
            epsilon=float(fusion_raw.get("epsilon", 1e-6)),
            decimals=int(fusion_raw.get("decimals", 12)),
            high_precision=int(fusion_raw.get("high_precision", 50)),
        )
        reference = _reference_from_mapping(reference_raw, context="v2")
    except (TypeError, ValueError) as exc:
        raise AudioQError("E_CONFIG", "v2 configuration contains a non-numeric field", observed={"reason": str(exc)}) from exc
    weights_raw = value.get("weights", {"snr": 0.4, "spectral": 0.35, "clipping": 0.25})
    if not isinstance(weights_raw, Mapping):
        raise AudioQError("E_INVALID_WEIGHT", "weights must be an object")
    return EvaluationConfig(
        evaluation_id=str(evaluation_id),
        frame=frame,
        metrics=metrics,
        weights=weights_raw,
        fusion=fusion,
        default_reference=reference,
        schema_version=2,
        compatibility={"source_schema_version": 2},
    )


def decode_config_v1(
    value: Mapping[str, Any],
    *,
    normalize_policy: str = "normalize",
    snr_threshold_rule: str = "fixed",
    default_snr_threshold_db: float = 8.0,
) -> EvaluationConfig:
    if not isinstance(value, Mapping):
        raise AudioQError("E_CONFIG", "v1 configuration must be a JSON object")
    version = value.get("schema_version", value.get("version", 1))
    if version != 1:
        raise AudioQError("E_CONFIG", "not a version 1 configuration", observed={"schema_version": version})
    raw_percent = value.get("weights_percent", value.get("weights_pct", value.get("weights")))
    if raw_percent is None:
        raise AudioQError("E_INVALID_WEIGHT", "v1 configuration has no percentage weights")
    weights, percent_total, _ = _normalise_percentages(raw_percent, policy=normalize_policy)

    restore = value.get("downgrade_restore", {})
    if not isinstance(restore, Mapping):
        restore = {}
    restored_weights = restore.get("weights")
    if isinstance(restored_weights, Mapping):
        weights = _normalise_weights(restored_weights, source="v1 downgrade restore")

    frame_raw = value.get("frame", {})
    metrics_raw = value.get("metrics", {})
    fusion_raw = value.get("fusion", {})
    reference_raw = value.get("default_reference", {"min_mos": 0.0, "max_mos": 5.0, "target_mos": 4.0})
    if not isinstance(frame_raw, Mapping) or not isinstance(metrics_raw, Mapping):
        raise AudioQError("E_CONFIG", "v1 frame and metrics must be JSON objects")
    if not isinstance(fusion_raw, Mapping) or not isinstance(reference_raw, Mapping):
        raise AudioQError("E_CONFIG", "v1 fusion and default_reference must be JSON objects")
    try:
        frame = FrameConfig(
            length=int(frame_raw.get("length", frame_raw.get("frame_length", value.get("frame_length", 400)))),
            hop=int(
                frame_raw.get(
                    "hop",
                    frame_raw.get("hop_length", frame_raw.get("frame_shift", value.get("frame_shift", 160))),
                )
            ),
            window=str(frame_raw.get("window", value.get("window", "hann"))),
        )
        threshold = restore.get("metrics.snr_threshold_db")
        threshold_source = "downgrade_restore"
        if threshold is None:
            flat_threshold = value.get("snr_threshold_db", metrics_raw.get("snr_threshold_db"))
            if flat_threshold is not None:
                threshold = flat_threshold
                threshold_source = "legacy_field"
        if threshold is None:
            threshold = _derive_snr_threshold(weights, snr_threshold_rule, default_snr_threshold_db)
            threshold_source = snr_threshold_rule
        metrics = MetricsConfig(
            snr_threshold_db=float(threshold),
            spectral_epsilon=float(metrics_raw.get("spectral_epsilon", 1e-8)),
            spectral_scale_db=float(metrics_raw.get("spectral_scale_db", 20.0)),
            clipping_level=float(metrics_raw.get("clipping_level", 0.98)),
            clipping_ratio_limit=float(metrics_raw.get("clipping_ratio_limit", 0.001)),
            clipping_scale_ratio=float(metrics_raw.get("clipping_scale_ratio", 0.08)),
        )
        fusion = FusionConfig(
            domain=str(fusion_raw.get("domain", "log")),
            epsilon=float(fusion_raw.get("epsilon", 1e-6)),
            decimals=int(fusion_raw.get("decimals", 12)),
            high_precision=int(fusion_raw.get("high_precision", 50)),
        )
        reference = _reference_from_mapping(reference_raw, context="v1")
    except (TypeError, ValueError) as exc:
        raise AudioQError("E_CONFIG", "v1 configuration contains a non-numeric field", observed={"reason": str(exc)}) from exc
    return EvaluationConfig(
        evaluation_id=str(value.get("evaluation_id", value.get("config_id", value.get("id", "legacy")))),
        frame=frame,
        metrics=metrics,
        weights=weights,
        fusion=fusion,
        default_reference=reference,
        schema_version=1,
        compatibility={
            "source_schema_version": 1,
            "normalization_policy": normalize_policy,
            "percentage_sum": percent_total,
            "snr_threshold_rule": snr_threshold_rule,
            "snr_threshold_source": threshold_source,
            "downgrade_items": list(value.get("downgrade_items", [])),
        },
    )


def decode_config(
    value: Mapping[str, Any],
    *,
    normalize_policy: str = "normalize",
    snr_threshold_rule: str = "fixed",
    default_snr_threshold_db: float = 8.0,
) -> EvaluationConfig:
    if not isinstance(value, Mapping):
        raise AudioQError("E_CONFIG", "configuration must be a JSON object")
    version = value.get("schema_version", value.get("version"))
    if version == 1:
        return decode_config_v1(
            value,
            normalize_policy=normalize_policy,
            snr_threshold_rule=snr_threshold_rule,
            default_snr_threshold_db=default_snr_threshold_db,
        )
    if version == 2:
        return decode_config_v2(value)
    raise AudioQError("E_CONFIG", "unsupported configuration schema version", observed={"schema_version": version})


def encode_config(config: EvaluationConfig, target_version: int = 2) -> dict[str, Any]:
    if target_version == 2:
        return config.to_v2_dict()
    if target_version == 1:
        return config.to_v1_dict()
    raise AudioQError("E_CONFIG", "target version must be 1 or 2", observed={"target_version": target_version})


def convert_config(
    value: Mapping[str, Any],
    target_version: int,
    *,
    normalize_policy: str = "normalize",
    snr_threshold_rule: str = "fixed",
    default_snr_threshold_db: float = 8.0,
) -> tuple[EvaluationConfig, dict[str, Any]]:
    """Import any supported structure version and export the requested version."""

    internal = decode_config(
        value,
        normalize_policy=normalize_policy,
        snr_threshold_rule=snr_threshold_rule,
        default_snr_threshold_db=default_snr_threshold_db,
    )
    return internal, encode_config(internal, target_version)


def load_config_file(
    path: Path | str,
    *,
    normalize_policy: str = "normalize",
    snr_threshold_rule: str = "fixed",
    default_snr_threshold_db: float = 8.0,
) -> EvaluationConfig:
    value = read_json(Path(path))
    return decode_config(
        value,
        normalize_policy=normalize_policy,
        snr_threshold_rule=snr_threshold_rule,
        default_snr_threshold_db=default_snr_threshold_db,
    )


@dataclass
class EvaluationModel:
    """One assessed shard in the shared internal representation."""

    shard_id: str
    sample_count: int
    sample_rate: int
    metric_scores: Mapping[str, float | None]
    fused_score: float | None
    schema_version: int
    version_fingerprint: str
    errors: list[Mapping[str, Any]] = field(default_factory=list)
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.sample_count < 0 or self.sample_rate <= 0:
            raise AudioQError("E_NUMERIC", "invalid sample count or sample rate")
        self.metric_scores = {name: self.metric_scores.get(name) for name in METRIC_NAMES}

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "shard_id": self.shard_id,
            "sample_count": self.sample_count,
            "sample_rate": self.sample_rate,
            "metric_scores": dict(self.metric_scores),
            "fused_score": self.fused_score,
            "schema_version": self.schema_version,
            "version_fingerprint": self.version_fingerprint,
            "errors": [dict(item) for item in self.errors],
        }
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EvaluationModel":
        return cls(
            shard_id=str(value["shard_id"]),
            sample_count=int(value["sample_count"]),
            sample_rate=int(value["sample_rate"]),
            metric_scores={name: value.get("metric_scores", {}).get(name) for name in METRIC_NAMES},
            fused_score=value.get("fused_score"),
            schema_version=int(value["schema_version"]),
            version_fingerprint=str(value["version_fingerprint"]),
            errors=list(value.get("errors", [])),
            extra=dict(value.get("extra", {})),
        )


InternalEvaluationModel = EvaluationModel
