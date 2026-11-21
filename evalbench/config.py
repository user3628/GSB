"""Calibration configuration: v2 native model + v1 legacy upgrade path.

v2 config (native):
    {"config_version": 2, "n_bins": 10, "binning": "equal_width",
     "calibration": {"method": "platt"}, "reliability_min_samples": 5,
     "expected_positive_rate": [0.2, 0.8], "rounding": "half_up",
     "id_delimiter": ":"}

v1 config (legacy):
    {"config_version": 1, "method": "platt_scaling",   # string alias
     "bins_pct": 10,                                    # integer percent
     "reliability_min_samples": 5}
    v1 lacks expected_positive_rate; it is backfilled with the default.

Every alias resolution, percent conversion and backfilled default is
recorded as an upgrade note so diagnostics can cite the exact basis.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from .codec import DEFAULT_ID_DELIMITER, DEFAULT_ROUNDING, ROUNDING_RULES
from .errors import ConfigInvalidError
from .fingerprint import fingerprint_of_obj

METHOD_PLATT = "platt"
METHOD_ISOTONIC = "isotonic"
METHODS = (METHOD_PLATT, METHOD_ISOTONIC)

BINNING_EQUAL_WIDTH = "equal_width"
BINNING_EQUAL_FREQ = "equal_freq"
BINNINGS = (BINNING_EQUAL_WIDTH, BINNING_EQUAL_FREQ)

# Configurable alias map: legacy v1 method strings -> method enum.
DEFAULT_METHOD_ALIASES = {
    "platt": METHOD_PLATT,
    "platt_scaling": METHOD_PLATT,
    "platt-scaling": METHOD_PLATT,
    "sigmoid": METHOD_PLATT,
    "isotonic": METHOD_ISOTONIC,
    "isotonic_regression": METHOD_ISOTONIC,
    "iso_reg": METHOD_ISOTONIC,
}

DEFAULT_EXPECTED_POSITIVE_RATE = (0.0, 1.0)
DEFAULT_RELIABILITY_MIN_SAMPLES = 1
MAX_BINS = 1000

# Which config fields each pipeline stage depends on. A change to a field
# invalidates the earliest stage listing it and everything downstream.
STAGE_CONFIG_FIELDS = {
    "parse": ("rounding", "id_delimiter", "n_bins"),
    "calibrate": ("n_bins", "binning", "method"),
    "diagnose": ("reliability_min_samples", "expected_positive_rate"),
}
STAGES = ("parse", "calibrate", "diagnose")


@dataclass(frozen=True)
class UpgradeNote:
    field: str
    source: str
    target: str
    reason: str

    def to_dict(self) -> dict:
        return {"field": self.field, "from": self.source, "to": self.target,
                "reason": self.reason}


@dataclass(frozen=True)
class CalibrationConfig:
    config_version: int
    n_bins: int
    binning: str
    method: str
    reliability_min_samples: int
    expected_positive_rate: tuple[float, float]
    rounding: str = DEFAULT_ROUNDING
    id_delimiter: str = DEFAULT_ID_DELIMITER
    upgrade_notes: tuple[UpgradeNote, ...] = field(default_factory=tuple)

    # -- serialization -----------------------------------------------------
    def stage_view(self, stage: str) -> dict:
        """The config subset a stage depends on (drives its fingerprint)."""
        view = {
            "rounding": self.rounding,
            "id_delimiter": self.id_delimiter,
            "n_bins": self.n_bins,
            "binning": self.binning,
            "method": self.method,
            "reliability_min_samples": self.reliability_min_samples,
            "expected_positive_rate": list(self.expected_positive_rate),
        }
        return {k: view[k] for k in STAGE_CONFIG_FIELDS[stage]}

    def stage_fingerprint(self, stage: str) -> str:
        return fingerprint_of_obj(self.stage_view(stage))

    def to_v2_dict(self) -> dict:
        return {
            "config_version": 2,
            "n_bins": self.n_bins,
            "binning": self.binning,
            "calibration": {"method": self.method},
            "reliability_min_samples": self.reliability_min_samples,
            "expected_positive_rate": list(self.expected_positive_rate),
            "rounding": self.rounding,
            "id_delimiter": self.id_delimiter,
        }

    def upgrade_notes_dicts(self) -> list[dict]:
        return [n.to_dict() for n in self.upgrade_notes]


def _fail(message: str, **details):
    raise ConfigInvalidError(message, **details)


def _validate(cfg: CalibrationConfig) -> CalibrationConfig:
    if not (1 <= cfg.n_bins <= MAX_BINS):
        _fail("n_bins out of range", n_bins=cfg.n_bins, allowed=[1, MAX_BINS])
    if cfg.binning not in BINNINGS:
        _fail("unknown binning strategy", binning=cfg.binning, allowed=list(BINNINGS))
    if cfg.method not in METHODS:
        _fail("unknown calibration method", method=cfg.method, allowed=list(METHODS))
    if cfg.reliability_min_samples < 1:
        _fail("reliability_min_samples must be >= 1",
              reliability_min_samples=cfg.reliability_min_samples)
    lo, hi = cfg.expected_positive_rate
    if not (0.0 <= lo <= hi <= 1.0):
        _fail("expected_positive_rate must satisfy 0 <= lo <= hi <= 1",
              expected_positive_rate=list(cfg.expected_positive_rate))
    if cfg.rounding not in ROUNDING_RULES:
        _fail("unknown rounding rule", rounding=cfg.rounding,
              allowed=sorted(ROUNDING_RULES))
    if not cfg.id_delimiter:
        _fail("id_delimiter must be a non-empty string")
    return cfg


def parse_v2_config(obj: dict) -> CalibrationConfig:
    try:
        method = obj["calibration"]["method"]
    except (KeyError, TypeError):
        _fail("v2 config requires calibration.method")
    try:
        rate = obj["expected_positive_rate"]
        rate = (float(rate[0]), float(rate[1]))
    except (KeyError, TypeError, IndexError, ValueError):
        _fail("v2 config requires expected_positive_rate: [lo, hi]")
    cfg = CalibrationConfig(
        config_version=2,
        n_bins=int(obj.get("n_bins", 0)),
        binning=str(obj.get("binning", BINNING_EQUAL_WIDTH)),
        method=str(method),
        reliability_min_samples=int(obj.get("reliability_min_samples",
                                            DEFAULT_RELIABILITY_MIN_SAMPLES)),
        expected_positive_rate=rate,
        rounding=str(obj.get("rounding", DEFAULT_ROUNDING)),
        id_delimiter=str(obj.get("id_delimiter", DEFAULT_ID_DELIMITER)),
    )
    return _validate(cfg)


def parse_v1_config(
    obj: dict,
    *,
    method_aliases: dict | None = None,
    default_expected_rate: tuple[float, float] = DEFAULT_EXPECTED_POSITIVE_RATE,
) -> CalibrationConfig:
    """Parse a legacy v1 config and explicitly upgrade it to the v2 model."""
    aliases = dict(DEFAULT_METHOD_ALIASES)
    if method_aliases:
        aliases.update(method_aliases)
    notes: list[UpgradeNote] = []

    alias = obj.get("method")
    if not isinstance(alias, str):
        _fail("v1 config requires string field 'method'")
    if alias not in aliases:
        _fail("unknown v1 method alias", method_alias=alias,
              known_aliases=sorted(aliases))
    method = aliases[alias]
    notes.append(UpgradeNote(
        field="method", source=alias, target=method,
        reason=f"alias resolved via configurable method alias map ('{alias}' -> '{method}')",
    ))

    bins_pct = obj.get("bins_pct")
    if isinstance(bins_pct, bool) or not isinstance(bins_pct, int):
        _fail("v1 config requires integer field 'bins_pct'", bins_pct=bins_pct)
    if bins_pct <= 0 or 100 % bins_pct != 0:
        _fail("bins_pct must be a positive divisor of 100", bins_pct=bins_pct)
    n_bins = 100 // bins_pct
    notes.append(UpgradeNote(
        field="n_bins", source=f"bins_pct={bins_pct}", target=str(n_bins),
        reason=f"integer percent converted: n_bins = 100 / bins_pct = 100 / {bins_pct}",
    ))

    if "expected_positive_rate" in obj:
        rate = obj["expected_positive_rate"]
        rate = (float(rate[0]), float(rate[1]))
    else:
        rate = (float(default_expected_rate[0]), float(default_expected_rate[1]))
        notes.append(UpgradeNote(
            field="expected_positive_rate", source="<missing>",
            target=f"[{rate[0]}, {rate[1]}]",
            reason="field absent in v1; backfilled with configured default interval",
        ))

    min_samples = int(obj.get("reliability_min_samples", DEFAULT_RELIABILITY_MIN_SAMPLES))
    if "reliability_min_samples" not in obj:
        notes.append(UpgradeNote(
            field="reliability_min_samples", source="<missing>",
            target=str(min_samples),
            reason="field absent in v1; backfilled with default",
        ))

    cfg = CalibrationConfig(
        config_version=2,
        n_bins=n_bins,
        binning=BINNING_EQUAL_WIDTH,
        method=method,
        reliability_min_samples=min_samples,
        expected_positive_rate=rate,
        rounding=str(obj.get("rounding", DEFAULT_ROUNDING)),
        id_delimiter=str(obj.get("id_delimiter", DEFAULT_ID_DELIMITER)),
        upgrade_notes=tuple(notes),
    )
    notes.append(UpgradeNote(
        field="binning", source="<missing>", target=BINNING_EQUAL_WIDTH,
        reason="v1 has no binning field; equal_width is the v1 semantics",
    ))
    cfg = CalibrationConfig(**{**cfg.__dict__, "upgrade_notes": tuple(notes)})
    return _validate(cfg)


def load_config(obj_or_path, *, method_aliases: dict | None = None) -> CalibrationConfig:
    """Load a config from a dict or JSON file path; auto-detects version."""
    if isinstance(obj_or_path, (str, bytes)) or hasattr(obj_or_path, "read"):
        if hasattr(obj_or_path, "read"):
            obj = json.load(obj_or_path)
        else:
            with open(obj_or_path, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
    else:
        obj = obj_or_path
    if not isinstance(obj, dict):
        _fail("config root must be a JSON object")
    version = obj.get("config_version")
    if version == 2:
        return parse_v2_config(obj)
    if version == 1:
        return parse_v1_config(obj, method_aliases=method_aliases)
    _fail("missing or unsupported 'config_version' (expected 1 or 2)",
          config_version=version)
