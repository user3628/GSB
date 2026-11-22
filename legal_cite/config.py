"""Versioned resolution configuration.

The configuration object is the single source of truth for unit conversion,
language weights, channel priors, freshness parameters and exact-comparison
precision.  Its fingerprint feeds the pipeline version fingerprint, so any
change invalidates the affected downstream stages.
"""

from copy import deepcopy

from .serde import fingerprint

CONFIG_VERSION = 3

# Default unit conversion dictionary.  Each factor converts a unit into the
# canonical unit (months for duration, basis points for percentage scale).
DEFAULT_UNIT_CONVERSIONS = {
    "duration": {"month": 1, "months": 1, "day": 1.0 / 30.0, "days": 1.0 / 30.0,
                 "year": 12, "years": 12},
    "penalty_scale": {"percent": 1, "pct": 1, "%": 1, "bp": 0.01, "bps": 0.01,
                      "basis_point": 0.01, "basis_points": 0.01},
}

DEFAULT_LANGUAGE_WEIGHTS = {"en": 1.0, "zh": 0.95}

DEFAULT_CHANNEL_PRIORS = {
    # Prior probability that a citation from the channel is trustworthy.
    "official_filing": 0.92,
    "counsel_review": 0.85,
    "counterparty_draft": 0.6,
    "ocr_extract": 0.45,
}

DEFAULT_FRESHNESS = {
    # log-odds adjustment per 30 days of age; newer citations score higher.
    "half_life_days": 365.0,
    "decay_per_30d": -0.05,
    "reference_date": "2026-01-01",
}

DEFAULT_PRECISION = {
    # Exact comparison precision: penalty kept to this many decimal places of
    # a percent; durations rounded to whole months after conversion.
    "penalty_decimal_places": 4,
    "duration_decimal_places": 0,
    "max_penalty_percent": 100.0,
    "min_penalty_percent": 0.0,
}

DEFAULT_REVIEW = {
    # A group whose best log-score is below this threshold is sent to manual
    # review instead of producing an adopted citation.
    "min_trust_log_score": -0.5,
}


def default_config():
    return {
        "config_version": CONFIG_VERSION,
        "unit_conversions": deepcopy(DEFAULT_UNIT_CONVERSIONS),
        "language_weights": dict(DEFAULT_LANGUAGE_WEIGHTS),
        "channel_priors": dict(DEFAULT_CHANNEL_PRIORS),
        "freshness": dict(DEFAULT_FRESHNESS),
        "precision": dict(DEFAULT_PRECISION),
        "review": dict(DEFAULT_REVIEW),
    }


def config_fingerprint(cfg):
    """Fingerprint over every value that can change resolution outcomes."""
    material = {
        "config_version": cfg["config_version"],
        "unit_conversions": cfg["unit_conversions"],
        "language_weights": cfg["language_weights"],
        "channel_priors": cfg["channel_priors"],
        "freshness": cfg["freshness"],
        "precision": cfg["precision"],
        "review": cfg["review"],
    }
    return fingerprint(material)
