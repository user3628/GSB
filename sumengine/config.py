"""A dependency-free YAML subset loader and configuration normalization.

The project intentionally depends only on NumPy (and the Python standard
library).  Configuration files use a deliberately small YAML subset:
block mappings/lists, quoted or plain scalars, and standalone comments.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

from .errors import EngineError
from .utils import digest


DEFAULTS: dict[str, Any] = {
    "seed": 20261122,
    "runtime": {
        "min_body_chars": 40,
        "min_sentence_chars": 4,
    },
    "budget": {
        "global_sentence_evaluations": 260,
        "per_worker_sentence_evaluations": 95,
        "max_workers": 3,
        "latency_budget_ms": 12000,
        "estimated_overhead_ms": 60,
        "estimated_ms_per_evaluation": 0.7,
        "poll_interval_ms": 10,
    },
    "targets": {
        "short": {"min_chars": 70, "max_chars": 150, "candidate_count": 3},
        "medium": {"min_chars": 130, "max_chars": 270, "candidate_count": 3},
        "long": {"min_chars": 210, "max_chars": 430, "candidate_count": 3},
    },
    "extractor": {
        "max_sentences": 10,
        "min_sentences": 2,
        "mmr_lambda": 0.72,
        "position_decay": 0.72,
        "weights": {
            "term_frequency": 2.0,
            "position": 1.0,
            "title_overlap": 1.25,
            "length_fit": 0.55,
        },
    },
    "compression": {
        "min_coverage": 0.62,
        "max_length_ratio": 1.02,
        "removable_clause_patterns": [
            "，[^，。；!?！？]{0,48}?(?:表示|认为|指出|强调|据悉|据了解|分析人士称)[^，。；!?！？]{0,48}",
            ",?\\s+(?:officials? said|analysts? said|according to reports?)[^,.;!?]{0,80}",
        ],
        "synonym_replacements": {
            "迅速": "快速",
            "表示": "称",
            "目前": "当前",
            "高度重视": "重点关注",
            "召开": "举行",
            "approximately": "about",
            "announced": "said",
            "rapidly": "quickly",
            "in order to": "to",
        },
        "required_replacement_terms": ["记者", "officials said"],
        "max_removed_ratio": 0.45,
    },
    "template": {
        "min_coverage": 0.58,
        "max_points": 4,
        "templates": {
            "zh": "要点：{who}；{what}；{when_where}。",
            "en": "Key points: {who}; {what}; {when_where}.",
            "mixed": "要点 / Key points: {who}; {what}; {when_where}.",
        },
    },
    "fusion": {
        "weights": {
            "coverage": 1.55,
            "length_fit": 0.75,
            "language": 0.5,
            "source_prior": 0.35,
        },
        "route_priority": ["extractive", "compressed", "template"],
        "source_priors": {"wire": 1.0, "desk": 0.94, "social": 0.82},
        "epsilon": 1e-9,
        "fusion_candidate_evaluations": 3,
    },
    "checkpoint": {
        "publish_epoch": "local",
    },
}


class _YamlSubsetParser:
    def __init__(self, text: str, source: str = "<yaml>") -> None:
        self.source = source
        self.lines: list[tuple[int, str, int]] = []
        self._tokenize(text)

    def _tokenize(self, text: str) -> None:
        for number, raw in enumerate(text.replace("\r\n", "\n").replace("\r", "\n").split("\n"), 1):
            if "\t" in raw[: len(raw) - len(raw.lstrip())]:
                self._fail(number, "tabs are not allowed for indentation")
            content = _strip_yaml_comment(raw).rstrip()
            if not content.strip():
                continue
            indent = len(content) - len(content.lstrip(" "))
            self.lines.append((indent, content[indent:], number))

    def _fail(self, line: int, message: str) -> None:
        raise EngineError(
            "E_CONFIG",
            f"{self.source}:{line}: {message}",
        )

    def parse(self) -> Any:
        if not self.lines:
            return {}
        indent, _, line = self.lines[0]
        value, index = self._parse_block(0, indent)
        if index != len(self.lines):
            self._fail(self.lines[index][2], "unexpected content")
        return value

    def _parse_block(self, index: int, indent: int) -> tuple[Any, int]:
        if index >= len(self.lines):
            return {}, index
        if self.lines[index][0] != indent:
            self._fail(self.lines[index][2], "inconsistent indentation")
        if self.lines[index][1].startswith("-"):
            return self._parse_list(index, indent)
        return self._parse_mapping(index, indent)

    def _parse_mapping(self, index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(self.lines):
            current_indent, content, line = self.lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                self._fail(line, "unexpected indentation")
            if content.startswith("-"):
                self._fail(line, "list item found where mapping key was expected")
            key, rest = _split_mapping_line(content, line, self._fail)
            if key in result:
                self._fail(line, f"duplicate key {key!r}")
            index += 1
            if rest == "":
                if index < len(self.lines) and self.lines[index][0] > indent:
                    value, index = self._parse_block(index, self.lines[index][0])
                else:
                    value = None
            else:
                value = _parse_scalar(rest, self.source, line)
            result[key] = value
        return result, index

    def _parse_list(self, index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(self.lines):
            current_indent, content, line = self.lines[index]
            if current_indent < indent:
                break
            if current_indent != indent or not content.startswith("-"):
                if current_indent > indent:
                    self._fail(line, "unexpected indentation in list")
                break
            rest = content[1:].strip()
            index += 1
            if not rest:
                if index < len(self.lines) and self.lines[index][0] > indent:
                    value, index = self._parse_block(index, self.lines[index][0])
                else:
                    value = None
            elif _mapping_colon(rest) >= 0 and not rest.startswith(("'", '"')):
                # A small but useful extension: "- key: value" mapping items.
                key, first_value = _split_mapping_line(rest, line, self._fail)
                item: dict[str, Any] = {}
                if first_value:
                    item[key] = _parse_scalar(first_value, self.source, line)
                elif index < len(self.lines) and self.lines[index][0] > indent:
                    child, index = self._parse_block(index, self.lines[index][0])
                    item[key] = child
                else:
                    item[key] = None
                if index < len(self.lines) and self.lines[index][0] > indent:
                    child_indent = self.lines[index][0]
                    extra, index = self._parse_mapping(index, child_indent)
                    for extra_key, extra_value in extra.items():
                        if extra_key in item:
                            self._fail(line, f"duplicate key {extra_key!r}")
                        item[extra_key] = extra_value
                value = item
            else:
                value = _parse_scalar(rest, self.source, line)
            result.append(value)
        return result, index


def _strip_yaml_comment(line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_double:
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == "#" and not in_single and not in_double:
            if index == 0 or line[index - 1].isspace():
                return line[:index]
    return line


def _mapping_colon(content: str) -> int:
    in_single = False
    in_double = False
    depth = 0
    escaped = False
    for index, char in enumerate(content):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_double:
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue
        if char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char == ":" and depth == 0:
            return index
    return -1


def _split_mapping_line(content: str, line: int, fail: Any) -> tuple[str, str]:
    colon = _mapping_colon(content)
    if colon <= 0:
        fail(line, "expected 'key: value'")
    key = content[:colon].strip()
    if not key:
        fail(line, "empty mapping key")
    if key.startswith(("'", '"')):
        key = _parse_scalar(key, "<inline>", line)
        if not isinstance(key, str):
            fail(line, "mapping key must be a string")
    return str(key), content[colon + 1 :].strip()


def _parse_scalar(value: str, source: str, line: int) -> Any:
    if value in {"null", "Null", "NULL", "~"}:
        return None
    if value in {"true", "True", "TRUE"}:
        return True
    if value in {"false", "False", "FALSE"}:
        return False
    if value.startswith('"'):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise EngineError("E_CONFIG", f"{source}:{line}: invalid double-quoted string: {exc}") from exc
    if value.startswith("'"):
        if not value.endswith("'") or len(value) < 2:
            raise EngineError("E_CONFIG", f"{source}:{line}: unterminated single-quoted string")
        return value[1:-1].replace("''", "'")
    if value.startswith("[") or value.startswith("{"):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise EngineError(
                "E_CONFIG",
                f"{source}:{line}: inline collections must use JSON syntax: {exc}",
            ) from exc
    if re.fullmatch(r"[-+]?(?:0|[1-9]\d*)", value):
        return int(value)
    if re.fullmatch(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?", value):
        return float(value)
    return value


def parse_yaml(text: str, source: str = "<yaml>") -> Any:
    return _YamlSubsetParser(text, source).parse()


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def normalize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise EngineError("E_CONFIG", "Configuration root must be a mapping")
    config = _deep_merge(DEFAULTS, raw)
    _validate_config(config)
    return config


def _require_number(value: Any, path: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EngineError("E_CONFIG", f"{path} must be a number")
    if minimum is not None and value < minimum:
        raise EngineError("E_CONFIG", f"{path} must be >= {minimum}")
    return float(value)


def _require_int(value: Any, path: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EngineError("E_CONFIG", f"{path} must be an integer")
    if value < minimum:
        raise EngineError("E_CONFIG", f"{path} must be >= {minimum}")
    return value


def _validate_config(config: dict[str, Any]) -> None:
    _require_int(config.get("seed"), "seed")
    for path in (
        ("runtime", "min_body_chars"),
        ("runtime", "min_sentence_chars"),
        ("budget", "global_sentence_evaluations"),
        ("budget", "per_worker_sentence_evaluations"),
        ("budget", "max_workers"),
        ("budget", "latency_budget_ms"),
        ("budget", "poll_interval_ms"),
    ):
        _require_int(config[path[0]][path[1]], ".".join(path), minimum=1)
    _require_number(config["budget"]["estimated_ms_per_evaluation"], "budget.estimated_ms_per_evaluation", minimum=0)
    _require_number(config["budget"]["estimated_overhead_ms"], "budget.estimated_overhead_ms", minimum=0)
    if config["budget"]["per_worker_sentence_evaluations"] > config["budget"]["global_sentence_evaluations"]:
        raise EngineError("E_CONFIG", "per-worker quota cannot exceed the global evaluation budget")

    targets = config.get("targets")
    if not isinstance(targets, dict) or not targets:
        raise EngineError("E_CONFIG", "targets must be a non-empty mapping")
    for name, band in targets.items():
        if not isinstance(band, dict):
            raise EngineError("E_CONFIG", f"targets.{name} must be a mapping")
        low = _require_int(band.get("min_chars"), f"targets.{name}.min_chars", minimum=1)
        high = _require_int(band.get("max_chars"), f"targets.{name}.max_chars", minimum=1)
        if low > high:
            raise EngineError("E_CONFIG", f"targets.{name}.min_chars exceeds max_chars")
        _require_int(band.get("candidate_count", 3), f"targets.{name}.candidate_count", minimum=1)

    extractor = config["extractor"]
    _require_number(extractor["mmr_lambda"], "extractor.mmr_lambda", minimum=0)
    if extractor["mmr_lambda"] > 1:
        raise EngineError("E_CONFIG", "extractor.mmr_lambda must be <= 1")
    _require_int(extractor["max_sentences"], "extractor.max_sentences", minimum=1)
    _require_int(extractor["min_sentences"], "extractor.min_sentences", minimum=1)
    for key, value in extractor["weights"].items():
        _require_number(value, f"extractor.weights.{key}", minimum=0)

    compression = config["compression"]
    coverage = _require_number(compression["min_coverage"], "compression.min_coverage", minimum=0)
    if coverage > 1:
        raise EngineError("E_CONFIG", "compression.min_coverage must be <= 1")
    if not isinstance(compression.get("synonym_replacements"), dict):
        raise EngineError("E_CONFIG", "compression.synonym_replacements must be a mapping")
    if not isinstance(compression.get("removable_clause_patterns"), list):
        raise EngineError("E_CONFIG", "compression.removable_clause_patterns must be a list")
    for pattern in compression["removable_clause_patterns"]:
        if not isinstance(pattern, str):
            raise EngineError("E_CONFIG", "compression patterns must be strings")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise EngineError("E_CONFIG", f"invalid compression regex {pattern!r}: {exc}") from exc
    if not isinstance(compression.get("required_replacement_terms", []), list):
        raise EngineError("E_CONFIG", "compression.required_replacement_terms must be a list")

    template = config["template"]
    template_coverage = _require_number(template["min_coverage"], "template.min_coverage", minimum=0)
    if template_coverage > 1:
        raise EngineError("E_CONFIG", "template.min_coverage must be <= 1")
    if not isinstance(template.get("templates"), dict) or not template["templates"]:
        raise EngineError("E_CONFIG", "template.templates must be a non-empty mapping")

    fusion = config["fusion"]
    for key, value in fusion["weights"].items():
        _require_number(value, f"fusion.weights.{key}", minimum=0)
    if not isinstance(fusion.get("route_priority"), list) or not fusion["route_priority"]:
        raise EngineError("E_CONFIG", "fusion.route_priority must be a non-empty list")
    if not isinstance(fusion.get("source_priors"), dict):
        raise EngineError("E_CONFIG", "fusion.source_priors must be a mapping")
    eps = _require_number(fusion["epsilon"], "fusion.epsilon", minimum=0)
    if eps <= 0:
        raise EngineError("E_CONFIG", "fusion.epsilon must be > 0")


def config_from_file(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EngineError("E_CONFIG", f"Cannot read configuration {file_path}: {exc}") from exc
    raw = parse_yaml(text, str(file_path))
    return normalize_config(raw)


def config_version(config: dict[str, Any]) -> str:
    return digest(config)
