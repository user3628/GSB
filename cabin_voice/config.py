"""Engine configuration defaults and strict validation."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Iterable

from .errors import CabinVoiceError
from .util import deep_merge, sha256_json


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "detection": {
        "frame_ms": 25.0,
        "hop_ms": 10.0,
        "window": "hann",
        "energy_floor_db": -100.0,
        "flatness_weight": 0.55,
        "enter_threshold_db": -43.0,
        "exit_threshold_db": -51.0,
        "min_speech_ms": 70,
        "min_silence_ms": 110,
    },
    "arrival": {
        "speed_of_sound_mps": 343.0,
        "delay_sigma_ms": 18.0,
    },
    "attribution": {
        "review_gap_threshold": 0.20,
        "priority_weight": 0.02,
        "lexicon_floor": 1e-9,
    },
}


def default_config() -> dict[str, Any]:
    return deepcopy(DEFAULT_CONFIG)


def merge_config(base: dict[str, Any], patch: dict[str, Any] | None) -> dict[str, Any]:
    return deep_merge(base, patch or {})


def config_version(config: dict[str, Any]) -> str:
    return sha256_json(config)


def _finite(
    value: Any,
    parameter: str,
    zone_id: str,
    *,
    minimum: float | None = None,
    strictly_positive: bool = False,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CabinVoiceError(
            "E_INVALID_CONFIG",
            "configuration value must be numeric",
            {"zone_id": zone_id, "parameter": parameter, "value": value},
        ) from exc
    invalid = not math.isfinite(number)
    if minimum is not None and number < minimum:
        invalid = True
    if strictly_positive and number <= 0:
        invalid = True
    if invalid:
        raise CabinVoiceError(
            "E_INVALID_CONFIG",
            "configuration value is outside the allowed range",
            {"zone_id": zone_id, "parameter": parameter, "value": value},
        )
    return number


def validate_config(config: dict[str, Any], zone_ids: Iterable[str]) -> None:
    zones = list(zone_ids) or ["<unknown>"]
    for zone_id in zones:
        detection = config.get("detection")
        if not isinstance(detection, dict):
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "detection configuration is missing",
                {"zone_id": zone_id, "parameter": "detection"},
            )
        frame_ms = _finite(
            detection.get("frame_ms"), "detection.frame_ms", zone_id, strictly_positive=True
        )
        hop_ms = _finite(
            detection.get("hop_ms"), "detection.hop_ms", zone_id, strictly_positive=True
        )
        if hop_ms == 0:
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "frame hop must be greater than zero",
                {"zone_id": zone_id, "parameter": "detection.hop_ms"},
            )
        if frame_ms <= hop_ms:
            # Overlap is allowed only when the window is meaningfully longer than the hop.
            # Equal values are accepted, but frame shorter than hop would create holes.
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "frame length must be greater than or equal to the frame hop",
                {"zone_id": zone_id, "parameter": "detection.frame_ms"},
            )
        window = detection.get("window")
        if window != "hann":
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "only the Hann window is supported",
                {"zone_id": zone_id, "parameter": "detection.window", "value": window},
            )
        _finite(
            detection.get("energy_floor_db"),
            "detection.energy_floor_db",
            zone_id,
        )
        _finite(
            detection.get("flatness_weight"),
            "detection.flatness_weight",
            zone_id,
            minimum=0.0,
        )
        enter = _finite(
            detection.get("enter_threshold_db"),
            "detection.enter_threshold_db",
            zone_id,
        )
        exit_ = _finite(
            detection.get("exit_threshold_db"),
            "detection.exit_threshold_db",
            zone_id,
        )
        if enter <= exit_:
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "enter threshold must be greater than exit threshold",
                {
                    "zone_id": zone_id,
                    "parameter": "detection.enter_threshold_db",
                    "enter_threshold_db": enter,
                    "exit_threshold_db": exit_,
                },
            )
        _finite(
            detection.get("min_speech_ms"),
            "detection.min_speech_ms",
            zone_id,
            minimum=0.0,
        )
        _finite(
            detection.get("min_silence_ms"),
            "detection.min_silence_ms",
            zone_id,
            minimum=0.0,
        )

        arrival = config.get("arrival")
        if not isinstance(arrival, dict):
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "arrival configuration is missing",
                {"zone_id": zone_id, "parameter": "arrival"},
            )
        _finite(
            arrival.get("speed_of_sound_mps"),
            "arrival.speed_of_sound_mps",
            zone_id,
            strictly_positive=True,
        )
        _finite(
            arrival.get("delay_sigma_ms"),
            "arrival.delay_sigma_ms",
            zone_id,
            strictly_positive=True,
        )

        attribution = config.get("attribution")
        if not isinstance(attribution, dict):
            raise CabinVoiceError(
                "E_INVALID_CONFIG",
                "attribution configuration is missing",
                {"zone_id": zone_id, "parameter": "attribution"},
            )
        _finite(
            attribution.get("review_gap_threshold"),
            "attribution.review_gap_threshold",
            zone_id,
            minimum=0.0,
        )
        _finite(
            attribution.get("priority_weight"),
            "attribution.priority_weight",
            zone_id,
            minimum=0.0,
        )
        _finite(
            attribution.get("lexicon_floor"),
            "attribution.lexicon_floor",
            zone_id,
            strictly_positive=True,
        )

    # Validate unknown top-level keys too, so typo'd config cannot silently look valid.
    allowed = {"schema_version", "detection", "arrival", "attribution"}
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise CabinVoiceError(
            "E_INVALID_CONFIG",
            "unknown top-level configuration keys",
            {"zone_id": zones[0], "parameter": unknown[0], "unknown": unknown},
        )
