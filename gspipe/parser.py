"""Mixed Chinese/Latin instruction tokenizer and operator-chain parser."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .canonical import normalize_instruction, sha256_json
from .errors import invalid_chain


SUPPORTED_OPERATORS = ("crop", "resize", "rotate", "color_shift", "blur")

OPERATOR_PARAMS: dict[str, tuple[str, ...]] = {
    "crop": ("x", "y", "w", "h"),
    "resize": ("w", "h", "mode"),
    "rotate": ("angle",),
    "color_shift": ("dr", "dg", "db"),
    "blur": ("radius",),
}

REQUIRED_PARAMS: dict[str, tuple[str, ...]] = {
    "crop": ("x", "y", "w", "h"),
    "resize": ("w", "h"),
    "rotate": ("angle",),
    "color_shift": ("dr", "dg", "db"),
    "blur": ("radius",),
}

# Chinese aliases are intentionally strict: a recognised alias still requires
# explicit numeric parameters in the same clause.  This avoids guessing sizes
# or coordinates, which would make edits non-reproducible.
CHINESE_ALIASES: dict[str, str] = {
    "高斯模糊": "blur",
    "调整尺寸": "resize",
    "裁剪": "crop",
    "裁切": "crop",
    "缩放": "resize",
    "旋转": "rotate",
    "调色": "color_shift",
    "调暖": "color_shift",
    "模糊": "blur",
}

_ALIAS_RE = re.compile("|".join(sorted(map(re.escape, CHINESE_ALIASES), key=len, reverse=True)))
_TOKEN_RE = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*"
    r"|[-+]?(?:\d+(?:\.\d*)?|\.\d+)"
    r"|[\u3400-\u9fff]"
    r"|\S"
)
_NUMBER_RE = re.compile(r"^[-+]?\d+$")
_PUNCTUATION = {",", ";", "，", "；", "。", ".", "、", "："}
_KNOWN_PARAM_KEYS = {key for keys in OPERATOR_PARAMS.values() for key in keys}


@dataclass(frozen=True)
class Token:
    text: str
    start: int
    end: int
    kind: str


@dataclass(frozen=True)
class Operator:
    name: str
    params: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"operator": self.name, "params": dict(sorted(self.params.items()))}


@dataclass(frozen=True)
class ParsedChain:
    task_id: str
    normalized_instruction: str
    allowed_ops: tuple[str, ...]
    operators: tuple[Operator, ...]
    chain_summary: str
    input_size: tuple[int, int]
    output_size: tuple[int, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "gspipe-parsed-chain-v1",
            "task_id": self.task_id,
            "normalized_instruction": self.normalized_instruction,
            "allowed_ops": list(self.allowed_ops),
            "operators": [operator.to_dict() for operator in self.operators],
            "chain_summary": self.chain_summary,
            "input_size": list(self.input_size),
            "output_size": list(self.output_size),
        }

    @property
    def digest(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class _Marker:
    operator: str
    start: int
    end: int
    source: str


def tokenize_mixed(text: str) -> list[Token]:
    """Split normalized text into Chinese single characters and Latin runs."""

    tokens: list[Token] = []
    for match in _TOKEN_RE.finditer(text):
        value = match.group(0)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            kind = "latin"
        elif re.fullmatch(r"[\u3400-\u9fff]", value):
            kind = "han"
        elif re.fullmatch(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)", value):
            kind = "number"
        else:
            kind = "punct"
        tokens.append(Token(value, match.start(), match.end(), kind))
    return tokens


def _next_significant(tokens: Sequence[Token], index: int) -> int | None:
    index += 1
    while index < len(tokens):
        if tokens[index].text not in _PUNCTUATION:
            return index
        index += 1
    return None


def _previous_significant(tokens: Sequence[Token], index: int) -> int | None:
    index -= 1
    while index >= 0:
        if tokens[index].text not in _PUNCTUATION:
            return index
        index -= 1
    return None


def _detect_unknown_operator(
    normalized: str,
    tokens: Sequence[Token],
    *,
    task_id: str,
) -> None:
    known_values = {"nearest", "bilinear"}
    for index, token in enumerate(tokens):
        if token.kind != "latin":
            continue
        lowered = token.text.lower()
        if lowered in SUPPORTED_OPERATORS:
            continue
        next_index = _next_significant(tokens, index)
        if lowered in _KNOWN_PARAM_KEYS and next_index is not None and tokens[next_index].text in {"=", ":"}:
            continue
        if next_index is not None and tokens[next_index].text in {"=", ":"}:
            # An unknown key=value pair is reported by the operator parameter
            # validator as unknown_parameter.
            continue
        if lowered in known_values:
            previous_index = _previous_significant(tokens, index)
            if previous_index is not None and tokens[previous_index].text in {"=", ":"}:
                continue
        # In the strict mixed-language grammar, every bare Latin run is an
        # operator tag.  Parameter keys and enum values are the only other
        # permitted Latin tokens.  This guarantees unknown operator names
        # cannot be silently ignored.
        raise invalid_chain(
            task_id,
            token.text,
            "unsupported_operator",
            f"Unsupported operator or bare Latin token {token.text!r}",
            normalized_instruction=normalized,
        )


def _find_markers(normalized: str, tokens: Sequence[Token], task_id: str) -> list[_Marker]:
    explicit: list[_Marker] = []
    explicit_ops: set[str] = set()
    for token in tokens:
        if token.kind != "latin":
            continue
        lowered = token.text.lower()
        if lowered in SUPPORTED_OPERATORS:
            explicit.append(_Marker(lowered, token.start, token.end, "latin"))
            explicit_ops.add(lowered)

    aliases: list[_Marker] = []
    for match in _ALIAS_RE.finditer(normalized):
        operator = CHINESE_ALIASES[match.group(0)]
        # When an explicit Latin tag for the same operation is present, the
        # Chinese word is descriptive rather than a second operator occurrence.
        if operator in explicit_ops:
            continue
        aliases.append(_Marker(operator, match.start(), match.end(), "zh"))

    markers = sorted(explicit + aliases, key=lambda marker: (marker.start, marker.end))
    if not markers:
        raise invalid_chain(
            task_id,
            None,
            "no_operator",
            "Instruction does not contain a supported operator tag",
            normalized_instruction=normalized,
        )
    return markers


def _parse_raw_params(
    task_id: str,
    operator: str,
    region: str,
    *,
    normalized: str,
) -> dict[str, str]:
    tokens = tokenize_mixed(region)
    values: dict[str, str] = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.kind != "latin":
            index += 1
            continue
        key = token.text.lower()
        if key not in OPERATOR_PARAMS[operator]:
            equals_index = _next_significant(tokens, index)
            if equals_index is not None and tokens[equals_index].text in {"=", ":"}:
                raise invalid_chain(
                    task_id,
                    operator,
                    "unknown_parameter",
                    f"Unknown parameter {key!r} for operator {operator}",
                    normalized_instruction=normalized,
                    parameter=key,
                )
            # Descriptive English words in a clause are ignored; fully
            # qualified unknown operator-like tokens are caught above.
            index += 1
            continue
        equals_index = _next_significant(tokens, index)
        if equals_index is None or tokens[equals_index].text not in {"=", ":"}:
            raise invalid_chain(
                task_id,
                operator,
                "malformed_parameter",
                f"Parameter {key!r} for {operator} must use key=value syntax",
                normalized_instruction=normalized,
                parameter=key,
            )
        value_index = _next_significant(tokens, equals_index)
        if value_index is None or tokens[value_index].text in _PUNCTUATION:
            raise invalid_chain(
                task_id,
                operator,
                "missing_parameter_value",
                f"Parameter {key!r} for {operator} has no value",
                normalized_instruction=normalized,
                parameter=key,
            )
        value_token = tokens[value_index]
        if value_token.kind == "punct" and value_token.text not in {"+", "-"}:
            raise invalid_chain(
                task_id,
                operator,
                "missing_parameter_value",
                f"Parameter {key!r} for {operator} has no scalar value",
                normalized_instruction=normalized,
                parameter=key,
            )
        value = value_token.text
        if key in values:
            raise invalid_chain(
                task_id,
                operator,
                "duplicate_parameter",
                f"Parameter {key!r} appears more than once for {operator}",
                normalized_instruction=normalized,
                parameter=key,
            )
        values[key] = value
        index = value_index + 1
    return values


def _int_param(
    task_id: str,
    operator: str,
    raw: Mapping[str, str],
    key: str,
    *,
    normalized: str,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if key not in raw:
        raise invalid_chain(
            task_id,
            operator,
            "missing_parameter",
            f"Operator {operator} requires parameter {key!r}",
            normalized_instruction=normalized,
            parameter=key,
        )
    value = raw[key]
    if not _NUMBER_RE.fullmatch(value):
        raise invalid_chain(
            task_id,
            operator,
            "invalid_parameter_type",
            f"Parameter {key!r} for {operator} must be an integer",
            normalized_instruction=normalized,
            parameter=key,
            value=value,
        )
    integer = int(value)
    if integer < minimum or (maximum is not None and integer > maximum):
        raise invalid_chain(
            task_id,
            operator,
            "parameter_out_of_range",
            f"Parameter {key!r} for {operator} is outside [{minimum}, {maximum}]",
            normalized_instruction=normalized,
            parameter=key,
            value=integer,
            minimum=minimum,
            maximum=maximum,
        )
    return integer


def _params_for_operator(
    task_id: str,
    operator: str,
    raw: Mapping[str, str],
    *,
    normalized: str,
    max_image_side: int,
    max_image_pixels: int,
) -> dict[str, Any]:
    for key in raw:
        if key not in OPERATOR_PARAMS[operator]:
            raise invalid_chain(
                task_id,
                operator,
                "unknown_parameter",
                f"Unknown parameter {key!r} for operator {operator}",
                normalized_instruction=normalized,
                parameter=key,
            )
    for key in REQUIRED_PARAMS[operator]:
        if key not in raw:
            raise invalid_chain(
                task_id,
                operator,
                "missing_parameter",
                f"Operator {operator} requires parameter {key!r}",
                normalized_instruction=normalized,
                parameter=key,
            )

    if operator == "crop":
        return {
            "x": _int_param(task_id, operator, raw, "x", normalized=normalized, minimum=0),
            "y": _int_param(task_id, operator, raw, "y", normalized=normalized, minimum=0),
            "w": _int_param(task_id, operator, raw, "w", normalized=normalized, minimum=1),
            "h": _int_param(task_id, operator, raw, "h", normalized=normalized, minimum=1),
        }
    if operator == "resize":
        width = _int_param(
            task_id,
            operator,
            raw,
            "w",
            normalized=normalized,
            minimum=1,
            maximum=max_image_side,
        )
        height = _int_param(
            task_id,
            operator,
            raw,
            "h",
            normalized=normalized,
            minimum=1,
            maximum=max_image_side,
        )
        if width * height > max_image_pixels:
            raise invalid_chain(
                task_id,
                operator,
                "output_too_large",
                "Resize output exceeds configured pixel limit",
                normalized_instruction=normalized,
                width=width,
                height=height,
                max_image_pixels=max_image_pixels,
            )
        mode = raw.get("mode", "nearest")
        if mode not in {"nearest", "bilinear"}:
            raise invalid_chain(
                task_id,
                operator,
                "invalid_resize_mode",
                "resize mode must be 'nearest' or 'bilinear'",
                normalized_instruction=normalized,
                value=mode,
            )
        return {"w": width, "h": height, "mode": mode}
    if operator == "rotate":
        return {
            "angle": _int_param(
                task_id,
                operator,
                raw,
                "angle",
                normalized=normalized,
                minimum=-360,
                maximum=360,
            )
        }
    if operator == "color_shift":
        return {
            "dr": _int_param(
                task_id, operator, raw, "dr", normalized=normalized, minimum=-255, maximum=255
            ),
            "dg": _int_param(
                task_id, operator, raw, "dg", normalized=normalized, minimum=-255, maximum=255
            ),
            "db": _int_param(
                task_id, operator, raw, "db", normalized=normalized, minimum=-255, maximum=255
            ),
        }
    if operator == "blur":
        return {
            "radius": _int_param(
                task_id, operator, raw, "radius", normalized=normalized, minimum=0, maximum=32
            )
        }
    raise invalid_chain(
        task_id,
        operator,
        "unsupported_operator",
        f"Unsupported operator {operator!r}",
        normalized_instruction=normalized,
    )


def chain_summary(operators: Iterable[Operator]) -> str:
    parts: list[str] = []
    for operator in operators:
        params = operator.params
        if operator.name == "crop":
            body = ",".join(f"{key}={params[key]}" for key in ("x", "y", "w", "h"))
        elif operator.name == "resize":
            body = f"w={params['w']},h={params['h']},mode={params['mode']}"
        elif operator.name == "rotate":
            body = f"angle={params['angle']}"
        elif operator.name == "color_shift":
            body = ",".join(f"{key}={params[key]}" for key in ("dr", "dg", "db"))
        elif operator.name == "blur":
            body = f"radius={params['radius']}"
        else:
            body = ",".join(f"{key}={params[key]}" for key in sorted(params))
        parts.append(f"{operator.name}({body})")
    return ">".join(parts)


def _advance_size(
    task_id: str,
    operator: Operator,
    size: tuple[int, int],
    *,
    normalized: str,
    max_image_side: int,
    max_image_pixels: int,
) -> tuple[int, int]:
    width, height = size
    params = operator.params
    if operator.name == "crop":
        x, y, crop_w, crop_h = params["x"], params["y"], params["w"], params["h"]
        if x + crop_w > width or y + crop_h > height:
            raise invalid_chain(
                task_id,
                operator.name,
                "crop_out_of_bounds",
                "crop rectangle exceeds the upstream image",
                normalized_instruction=normalized,
                upstream_size=[width, height],
                rectangle=[x, y, crop_w, crop_h],
            )
        return crop_w, crop_h
    if operator.name == "resize":
        new_width, new_height = params["w"], params["h"]
        if new_width > max_image_side or new_height > max_image_side or new_width * new_height > max_image_pixels:
            raise invalid_chain(
                task_id,
                operator.name,
                "output_too_large",
                "Operator output exceeds configured image limits",
                normalized_instruction=normalized,
                output_size=[new_width, new_height],
            )
        return new_width, new_height
    if size[0] > max_image_side or size[1] > max_image_side or size[0] * size[1] > max_image_pixels:
        raise invalid_chain(
            task_id,
            operator.name,
            "upstream_too_large",
            "Upstream image exceeds configured limits",
            normalized_instruction=normalized,
            upstream_size=list(size),
        )
    return size


def parse_instruction(
    task_id: str,
    instruction: str,
    allowed_ops: Sequence[str],
    input_size: tuple[int, int],
    *,
    max_chain_length: int = 5,
    max_image_side: int = 2048,
    max_image_pixels: int = 4194304,
) -> ParsedChain:
    """Parse, validate, and canonicalize one instruction into an operator chain."""

    normalized = normalize_instruction(instruction)
    if len(input_size) != 2 or input_size[0] <= 0 or input_size[1] <= 0:
        raise invalid_chain(task_id, None, "invalid_input_size", "Input image size must be positive")

    normalized_allowed: list[str] = []
    for raw_allowed in allowed_ops:
        label = str(raw_allowed).strip().lower()
        if label not in SUPPORTED_OPERATORS:
            raise invalid_chain(
                task_id,
                str(raw_allowed),
                "unsupported_allowed_operator",
                f"allowed_ops contains unsupported operator {raw_allowed!r}",
                normalized_instruction=normalized,
            )
        if label not in normalized_allowed:
            normalized_allowed.append(label)

    tokens = tokenize_mixed(normalized)
    _detect_unknown_operator(normalized, tokens, task_id=task_id)
    markers = _find_markers(normalized, tokens, task_id)
    if len(markers) > max_chain_length:
        offending = markers[max_chain_length]
        raise invalid_chain(
            task_id,
            offending.operator,
            "chain_too_long",
            f"Operator chain length {len(markers)} exceeds limit {max_chain_length}",
            normalized_instruction=normalized,
            max_chain_length=max_chain_length,
        )

    operators: list[Operator] = []
    for index, marker in enumerate(markers):
        if marker.operator not in normalized_allowed:
            raise invalid_chain(
                task_id,
                marker.operator,
                "operator_not_allowed",
                f"Operator {marker.operator!r} is not in allowed_ops",
                normalized_instruction=normalized,
                allowed_ops=normalized_allowed,
            )
        region_end = markers[index + 1].start if index + 1 < len(markers) else len(normalized)
        region = normalized[marker.end : region_end]
        raw_params = _parse_raw_params(task_id, marker.operator, region, normalized=normalized)
        params = _params_for_operator(
            task_id,
            marker.operator,
            raw_params,
            normalized=normalized,
            max_image_side=max_image_side,
            max_image_pixels=max_image_pixels,
        )
        operators.append(Operator(marker.operator, params))

    size = (int(input_size[0]), int(input_size[1]))
    for operator in operators:
        size = _advance_size(
            task_id,
            operator,
            size,
            normalized=normalized,
            max_image_side=max_image_side,
            max_image_pixels=max_image_pixels,
        )

    return ParsedChain(
        task_id=task_id,
        normalized_instruction=normalized,
        allowed_ops=tuple(normalized_allowed),
        operators=tuple(operators),
        chain_summary=chain_summary(operators),
        input_size=(int(input_size[0]), int(input_size[1])),
        output_size=size,
    )
