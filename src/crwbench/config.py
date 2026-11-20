"""YAML configuration loading, validation and version fingerprinting."""
from __future__ import annotations

import os

import yaml

from .errors import E_CONFIG, raise_error
from .util import digest_obj

CATEGORY_KEYS = ["obligation", "disclaimer", "breach", "payment", "termination"]


class Config:
    def __init__(self, raw: dict, path: str | None = None):
        self.raw = raw
        self.path = path
        self.config_version = str(raw.get("config_version", "unknown"))
        self.default_language = raw.get("default_language", "zh")
        self.categories = raw["categories"]
        self.category_keys = [c["key"] for c in self.categories]
        self.negation = raw["negation"]
        self.degree = raw["degree"]
        self.scoring = raw["scoring"]
        self.budget = raw["budget"]
        self.level_thresholds = list(self.scoring["level_thresholds"])

    @property
    def fingerprint(self) -> str:
        return digest_obj(self.raw)

    def category(self, key: str) -> dict | None:
        for cat in self.categories:
            if cat["key"] == key:
                return cat
        return None

    def keyword_table(self, key: str, lang: str) -> dict | None:
        """Explicit None return signals the unknown-table fallback."""
        cat = self.category(key)
        if cat is None:
            return None
        table = cat.get("keywords") or {}
        return table.get(lang)

    def negation_terms(self, lang: str) -> list[str]:
        return list(self.negation.get("terms", {}).get(lang, []))

    def degree_terms(self, lang: str) -> dict:
        return dict(self.degree.get("terms", {}).get(lang, {}))

    def quota(self, key: str) -> int:
        return int(self.budget["category_quotas"].get(key, 0))


def load_config(path: str) -> Config:
    if not os.path.exists(path):
        raise_error(E_CONFIG, f"config file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise_error(E_CONFIG, f"invalid YAML: {exc}")
    validate_config(raw)
    return Config(raw, path)


def validate_config(raw: dict) -> None:
    if not isinstance(raw, dict):
        raise_error(E_CONFIG, "config root must be a mapping")
    for field in ("categories", "negation", "degree", "scoring", "budget"):
        if field not in raw:
            raise_error(E_CONFIG, f"missing config section: {field}")
    keys = [c.get("key") for c in raw["categories"]]
    if keys != CATEGORY_KEYS:
        raise_error(
            E_CONFIG,
            "category table must contain exactly the five fixed categories in order",
            {"expected": CATEGORY_KEYS, "found": keys},
        )
    thresholds = raw["scoring"].get("level_thresholds")
    if not isinstance(thresholds, list) or len(thresholds) != 3:
        raise_error(E_CONFIG, "scoring.level_thresholds must contain 3 values")
    if list(thresholds) != sorted(thresholds):
        raise_error(E_CONFIG, "scoring.level_thresholds must be ascending")
    budget = raw["budget"]
    for field in ("global_max_judgments", "category_quotas"):
        if field not in budget:
            raise_error(E_CONFIG, f"budget.{field} is required")
    quotas = budget["category_quotas"]
    if any(quotas.get(k) is None for k in CATEGORY_KEYS):
        raise_error(E_CONFIG, "every category needs a quota", {"quotas": quotas})
    neg_terms = raw["negation"].get("terms", {})
    for lang, terms in neg_terms.items():
        if not all(isinstance(term, str) for term in terms):
            raise_error(
                E_CONFIG,
                "negation terms must all be strings (quote YAML scalars)",
                {"lang": lang, "terms": terms},
            )
    for cat in raw["categories"]:
        for lang, table in (cat.get("keywords") or {}).items():
            if not all(isinstance(term, str) for term in table):
                raise_error(
                    E_CONFIG,
                    "keyword terms must all be strings (quote YAML scalars)",
                    {"category": cat["key"], "lang": lang},
                )
