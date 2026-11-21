"""Configuration parsing, validation and legacy v1 manifest compatibility."""

from __future__ import annotations

import copy
import hashlib
import json
import os

import yaml

from ..errors import InvalidConfigError, InvalidParamError, SeqTooLongError

SCHEME_TYPES = ("rope", "ntk_rope", "linear_rope", "alibi")
PADDING_RULES = ("zero_pad", "replicate_last")
DEFAULT_TOLERANCE_KEYS = (
    "mean_bias", "bias_peak", "diff_abs_mean",
    "monotonic_violations", "change_rate",
)


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def content_fingerprint(obj) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def stable_scheme_id(kind: str, params: dict) -> str:
    digest = hashlib.sha256(
        canonical_json({"kind": kind, "params": params}).encode("utf-8")
    ).hexdigest()[:10]
    return f"{kind}-{digest}"


class AppConfig:
    def __init__(self, data: dict, *, source_path=None, provenance=None):
        self.data = copy.deepcopy(data)
        self.source_path = source_path
        self.provenance = provenance or {}

    # ------------------------------------------------------------------ io
    @classmethod
    def load(cls, path):
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        if not isinstance(data, dict):
            raise InvalidConfigError("top-level YAML must be a mapping",
                                     stage="config")
        version = data.get("config_version")
        if version == 1:
            cfg, provenance = convert_legacy_v1(data)
            cfg.source_path = os.path.abspath(path)
            cfg.provenance = provenance
            return cfg
        if version != 2:
            raise InvalidConfigError(
                f"unsupported config_version: {version!r}", stage="config",
                context={"config_version": version})
        cfg = cls(data, source_path=os.path.abspath(path))
        cfg.validate()
        return cfg

    def dump(self, path):
        with open(path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(self.data, handle, sort_keys=True,
                           allow_unicode=True, default_flow_style=False)

    # ------------------------------------------------------------ accessors
    @property
    def version(self):
        return self.data["config_version"]

    @property
    def seed(self):
        return int(self.data.get("seed", 20261121))

    @property
    def max_seq_len(self):
        return int(self.data["limits"]["max_seq_len"])

    @property
    def head_dim(self):
        return int(self.data["head_dim"]["value"])

    @property
    def padding_rule(self):
        return self.data["head_dim"].get("padding_rule", "zero_pad")

    @property
    def train_length(self):
        return int(self.data["train_length"])

    @property
    def schemes(self):
        return self.data["schemes"]

    @property
    def bins(self):
        return self.data["bins"]

    @property
    def tolerance(self):
        return self.data["tolerance"]

    def fingerprint(self):
        return content_fingerprint(self.data)

    # ------------------------------------------------------------- validate
    def validate(self):
        data = self.data
        stage = "config"
        if data.get("config_version") != 2:
            raise InvalidConfigError("config_version must be 2", stage=stage)
        limits = data.get("limits")
        if not isinstance(limits, dict) or \
                not isinstance(limits.get("max_seq_len"), int) or \
                limits["max_seq_len"] <= 0:
            raise InvalidConfigError("limits.max_seq_len must be positive int",
                                     stage=stage)
        head = data.get("head_dim")
        if not isinstance(head, dict) or not isinstance(head.get("value"), int) \
                or head["value"] <= 0:
            raise InvalidConfigError("head_dim.value must be positive int",
                                     stage=stage)
        if head.get("padding_rule", "zero_pad") not in PADDING_RULES:
            raise InvalidConfigError(
                f"unsupported padding_rule: {head.get('padding_rule')}",
                stage=stage)
        if not isinstance(data.get("train_length"), int) or \
                data["train_length"] <= 0:
            raise InvalidConfigError("train_length must be positive int",
                                     stage=stage)
        schemes = data.get("schemes")
        if not isinstance(schemes, list) or not schemes:
            raise InvalidConfigError("at least one scheme is required",
                                     stage=stage)
        seen_ids = set()
        for scheme in schemes:
            self._validate_scheme(scheme, seen_ids)
        bins = data.get("bins")
        if not isinstance(bins, list) or len(bins) == 0:
            raise InvalidConfigError(
                "number of extrapolation bins must not be zero",
                stage=stage, context={"bins": bins})
        prev_high = None
        for bin_cfg in bins:
            self._validate_bin(bin_cfg, prev_high)
            prev_high = float(bin_cfg["ratio_high"])
        tolerance = data.get("tolerance")
        if not isinstance(tolerance, dict) or not tolerance:
            raise InvalidConfigError("tolerance mapping is required",
                                     stage=stage)
        for key, value in tolerance.items():
            if not isinstance(value, (int, float)) or value < 0:
                raise InvalidConfigError(
                    f"tolerance.{key} must be a non-negative number",
                    stage=stage)
        return True

    def _validate_scheme(self, scheme, seen_ids):
        stage = "config"
        sid = scheme.get("id")
        if not isinstance(sid, str) or not sid:
            raise InvalidConfigError("scheme id is required", stage=stage)
        if sid in seen_ids:
            raise InvalidConfigError(f"duplicate scheme id: {sid}",
                                     stage=stage)
        seen_ids.add(sid)
        kind = scheme.get("type")
        if kind not in SCHEME_TYPES:
            raise InvalidConfigError(
                f"unsupported scheme type: {kind!r}", stage=stage,
                context={"scheme": sid})
        if kind == "alibi":
            slopes = scheme.get("slopes")
            if slopes is not None:
                if not isinstance(slopes, list) or not slopes:
                    raise InvalidConfigError(
                        "alibi slopes must be a non-empty list",
                        stage=stage, context={"scheme": sid})
                for slope in slopes:
                    if not isinstance(slope, (int, float)) or slope <= 0:
                        raise InvalidParamError(
                            "alibi slope must be positive",
                            stage=stage, scheme=sid,
                            context={"slope": slope})
            return
        base_freq = scheme.get("base_freq")
        if not isinstance(base_freq, (int, float)):
            raise InvalidConfigError("base_freq must be numeric",
                                     stage=stage, scheme=sid)
        scale = scheme.get("scale")
        if not isinstance(scale, (int, float)):
            raise InvalidConfigError("scale must be numeric",
                                     stage=stage, scheme=sid)
        if float(scale) < 0.0:
            raise InvalidParamError(
                "scaling coefficient must not be negative",
                stage=stage, scheme=sid, context={"scale": scale})

    def _validate_bin(self, bin_cfg, prev_high):
        stage = "config"
        for field in ("id", "ratio_low", "ratio_high"):
            if field not in bin_cfg:
                raise InvalidConfigError(
                    f"bin missing field: {field}", stage=stage)
        low = float(bin_cfg["ratio_low"])
        high = float(bin_cfg["ratio_high"])
        if low < 0 or high <= low:
            raise InvalidConfigError(
                "bin must satisfy 0 <= ratio_low < ratio_high",
                stage=stage, context={"bin": bin_cfg["id"]})
        if prev_high is not None and low != prev_high:
            raise InvalidConfigError(
                "bin boundaries must be contiguous with previous bin",
                stage=stage, context={"bin": bin_cfg["id"]})

    def check_sequence_length(self, seq_len):
        if int(seq_len) > self.max_seq_len:
            raise SeqTooLongError(
                f"sequence length {seq_len} exceeds limit "
                f"{self.max_seq_len}",
                stage="config", context={"seq_len": int(seq_len),
                                         "max_seq_len": self.max_seq_len})


# ------------------------------------------------------------- legacy v1
def default_alibi_slopes(n_heads: int):
    """Geometric power-law slopes 2^(-8/n) as used by the canonical ALiBi."""
    base = 2.0 ** (-8.0 / float(n_heads))
    return [float(base ** (i + 1)) for i in range(n_heads)]


def convert_legacy_v1(legacy: dict):
    """Convert a structure-version-1 manifest into a v2 config.

    The conversion is reversible: integer percentage scales, default ALiBi
    slopes and integer extrapolation ratios are recorded in the provenance
    block so the v1 manifest can be reconstructed.
    """
    if not isinstance(legacy, dict) or legacy.get("config_version") != 1:
        raise InvalidConfigError("not a version-1 manifest", stage="config")
    divisor = int(legacy.get("legacy_conversion", {})
                  .get("percentage_divisor", 100))
    if divisor <= 0:
        raise InvalidConfigError("percentage_divisor must be positive",
                                 stage="config")
    n_heads = int(legacy.get("n_heads", 4))
    schemes_v2 = []
    provenance = {"source_version": 1, "percentage_divisor": divisor,
                  "schemes": {}}
    for item in legacy.get("schemes", []):
        kind = item["kind"]
        name = item.get("name", kind)
        record = {"name": name, "kind": kind}
        if kind == "alibi":
            slopes = item.get("slopes")
            if slopes is None:
                slopes = default_alibi_slopes(n_heads)
                record["slopes_source"] = "default_power_law"
            else:
                record["slopes_source"] = "explicit"
            params = {"slopes": [float(s) for s in slopes]}
            record["slopes"] = params["slopes"]
        else:
            scale_pct = int(item["scale_pct"])
            record["scale_pct"] = scale_pct
            params = {
                "base_freq": float(item["base_freq"]),
                "scale": scale_pct / float(divisor),
            }
        sid = stable_scheme_id(kind, params)
        record["stable_id"] = sid
        scheme_v2 = {"id": sid, "type": kind, **params}
        schemes_v2.append(scheme_v2)
        provenance["schemes"][sid] = record

    ratios = [int(r) for r in legacy.get("extrap_ratios", [1, 2, 4])]
    if not ratios:
        raise InvalidConfigError(
            "number of extrapolation bins must not be zero", stage="config")
    boundaries = [0] + ratios
    bin_ids = ["train", "near", "far", "farther", "extreme"]
    bins_v2 = []
    for idx, high in enumerate(ratios):
        bins_v2.append({
            "id": bin_ids[idx] if idx < len(bin_ids) else f"bin_{idx}",
            "ratio_low": float(boundaries[idx]),
            "ratio_high": float(high),
        })
    provenance["extrap_ratios"] = ratios

    tol_pct = int(legacy.get("tolerance_pct", 5))
    tol_fraction = tol_pct / float(divisor)
    tolerance_v2 = {key: tol_fraction for key in DEFAULT_TOLERANCE_KEYS
                    if key != "monotonic_violations"}
    tolerance_v2["monotonic_violations"] = 0
    provenance["tolerance_pct"] = tol_pct

    data = {
        "config_version": 2,
        "name": legacy.get("name", "legacy-converted"),
        "seed": int(legacy.get("seed", 20261121)),
        "limits": {
            "max_seq_len": int(legacy.get("max_seq_len", 4096)),
        },
        "head_dim": {
            "value": int(legacy.get("head_dim", 16)),
            "padding_rule": legacy.get("padding_rule", "zero_pad"),
        },
        "train_length": int(legacy.get("train_length", 8)),
        "schemes": schemes_v2,
        "bins": bins_v2,
        "tolerance": tolerance_v2,
    }
    cfg = AppConfig(data)
    cfg.validate()
    return cfg, provenance


def export_legacy_v1(cfg: AppConfig) -> dict:
    """Inverse of :func:`convert_legacy_v1` for round-trip verification."""
    divisor = cfg.provenance.get("percentage_divisor", 100)
    legacy_schemes = []
    for scheme in cfg.schemes:
        kind = scheme["type"]
        if kind == "alibi":
            entry = {"name": scheme["id"], "kind": "alibi",
                     "slopes": [float(s) for s in scheme["slopes"]]}
        else:
            entry = {
                "name": scheme["id"],
                "kind": kind,
                "base_freq": float(scheme["base_freq"]),
                "scale_pct": int(round(float(scheme["scale"]) * divisor)),
            }
        legacy_schemes.append(entry)
    return {
        "config_version": 1,
        "name": cfg.data.get("name", "legacy-converted"),
        "seed": cfg.seed,
        "max_seq_len": cfg.max_seq_len,
        "n_heads": len(cfg.schemes),
        "head_dim": cfg.head_dim,
        "padding_rule": cfg.padding_rule,
        "train_length": cfg.train_length,
        "schemes": legacy_schemes,
        "extrap_ratios": [int(b["ratio_high"]) for b in cfg.bins],
        "tolerance_pct": int(round(
            float(cfg.tolerance["mean_bias"]) * divisor)),
        "legacy_conversion": {"percentage_divisor": divisor},
    }
