from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable

from .errors import BusinessError, E_INPUT_INVALID, E_STAT_INVALID
from .fixed import (
    MAX_ROWS,
    RATIO_SCALE,
    clamp_int,
    decimal_from,
    format_decimal,
    quantize_ratio,
)


RELATION_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.-]{0,127}$")
MAX_SEARCH_RELATIONS = 9


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RelationStats:
    name: str
    row_count: int
    ndv_ratio_fixed: int
    correlation_fixed: int
    stale_keys: tuple[str, ...] = ()
    source_precision_places: int = 3
    rounding_place: int = 9
    rounding_error_units: str = "0"
    source_kind: str = "permille"

    @property
    def ndv_ratio(self) -> str:
        from .fixed import format_fixed

        return format_fixed(self.ndv_ratio_fixed, RATIO_SCALE, 9)

    @property
    def correlation_coefficient(self) -> str:
        from .fixed import format_fixed

        return format_fixed(self.correlation_fixed, RATIO_SCALE, 9)

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "ndv_ratio_fixed": self.ndv_ratio_fixed,
            "correlation_fixed": self.correlation_fixed,
            "stale_keys": list(self.stale_keys),
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            "relation": self.name,
            "row_count": self.row_count,
            "ndv_ratio": self.ndv_ratio,
            "ndv_ratio_fixed": str(self.ndv_ratio_fixed),
            "correlation_coefficient": self.correlation_coefficient,
            "correlation_fixed": str(self.correlation_fixed),
            "stale_keys": list(self.stale_keys),
            "normalization": {
                "source_kind": self.source_kind,
                "source_precision_places": self.source_precision_places,
                "target_scale": str(RATIO_SCALE),
                "rounding_place": self.rounding_place,
                "rounding_error_units": self.rounding_error_units,
            },
        }

@dataclass(frozen=True)
class StatsSnapshot:
    version: int
    relations: tuple[RelationStats, ...]
    digest: str
    format_name: str
    format_version: int
    decision: str
    degradations: tuple[dict[str, Any], ...] = ()
    normalization: dict[str, Any] = field(default_factory=dict)

    def by_name(self) -> dict[str, RelationStats]:
        return {relation.name: relation for relation in self.relations}

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "relations": [relation.canonical_dict() for relation in self.relations],
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            "snapshot_version": self.version,
            "digest": self.digest,
            "format": self.format_name,
            "format_version": self.format_version,
            "decision": self.decision,
            "degradations": list(self.degradations),
            "normalization": self.normalization,
            "relations": [relation.public_dict() for relation in self.relations],
        }

    def storage_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "digest": self.digest,
            "format_name": self.format_name,
            "format_version": self.format_version,
            "decision": self.decision,
            "degradations": list(self.degradations),
            "normalization": self.normalization,
            "relations": [
                {
                    "name": relation.name,
                    "row_count": relation.row_count,
                    "ndv_ratio_fixed": relation.ndv_ratio_fixed,
                    "correlation_fixed": relation.correlation_fixed,
                    "stale_keys": list(relation.stale_keys),
                    "source_precision_places": relation.source_precision_places,
                    "rounding_place": relation.rounding_place,
                    "rounding_error_units": relation.rounding_error_units,
                    "source_kind": relation.source_kind,
                }
                for relation in self.relations
            ],
        }


def normalize_relation(
    *,
    name: str,
    row_count: int,
    ndv_ratio_value: Any,
    correlation_value: Any,
    stale_keys: Iterable[str] = (),
    source_kind: str,
    source_precision_places: int,
    default_correlation: bool = False,
) -> RelationStats:
    if not isinstance(name, str) or not RELATION_NAME_RE.match(name):
        raise BusinessError(E_STAT_INVALID, "invalid relation name", {"relation": name})
    if isinstance(row_count, bool) or not isinstance(row_count, int):
        raise BusinessError(
            E_STAT_INVALID,
            "row_count must be an integer",
            {"relation": name, "row_count": row_count},
        )
    if row_count < 0 or row_count > MAX_ROWS:
        raise BusinessError(
            E_STAT_INVALID,
            "row_count is outside the supported non-negative range",
            {"relation": name, "row_count": row_count, "max": MAX_ROWS},
        )
    try:
        ndv_fixed, ndv_error, ndv_places = quantize_ratio(ndv_ratio_value)
        corr_fixed, corr_error, corr_places = quantize_ratio(correlation_value)
    except Exception as exc:
        raise BusinessError(
            E_STAT_INVALID,
            "statistic is not a finite decimal number",
            {"relation": name, "reason": str(exc)},
        ) from exc
    if ndv_fixed < 0 or ndv_fixed > RATIO_SCALE:
        raise BusinessError(
            E_STAT_INVALID,
            "distinct-value ratio must be in [0, 1]",
            {"relation": name, "value": str(ndv_ratio_value)},
        )
    if row_count == 0 and ndv_fixed != 0:
        raise BusinessError(
            E_STAT_INVALID,
            "zero-row relation must have a zero distinct-value ratio",
            {"relation": name},
        )
    if corr_fixed < 0 or corr_fixed > RATIO_SCALE:
        raise BusinessError(
            E_STAT_INVALID,
            "correlation coefficient must be in [0, 1]",
            {"relation": name, "value": str(correlation_value)},
        )
    if isinstance(stale_keys, bool):
        normalized_stale = ("*",) if stale_keys else ()
    else:
        normalized_stale = tuple(sorted(str(item) for item in stale_keys))
        if any(not item for item in normalized_stale):
            raise BusinessError(
                E_STAT_INVALID,
                "stale key markers must be non-empty strings",
                {"relation": name},
            )
    error_total = ndv_error + corr_error
    return RelationStats(
        name=name,
        row_count=row_count,
        ndv_ratio_fixed=ndv_fixed,
        correlation_fixed=corr_fixed,
        stale_keys=normalized_stale,
        source_precision_places=max(ndv_places, corr_places, source_precision_places),
        rounding_place=9,
        rounding_error_units=format_decimal(error_total, 12),
        source_kind=source_kind,
    )


def finalize_snapshot(
    *,
    version: int,
    relations: list[RelationStats],
    format_name: str,
    format_version: int,
    decision: str,
    degradations: list[dict[str, Any]],
) -> StatsSnapshot:
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < 0
        or version > MAX_ROWS
    ):
        raise BusinessError(
            E_STAT_INVALID,
            "snapshot version must be an integer in the signed 64-bit non-negative range",
        )
    if not relations:
        raise BusinessError(E_STAT_INVALID, "snapshot must contain at least one relation")
    names = [relation.name for relation in relations]
    if len(names) != len(set(names)):
        raise BusinessError(E_STAT_INVALID, "duplicate relation in snapshot", {"names": names})
    ordered = tuple(sorted(relations, key=lambda item: item.name))
    payload = {
        "version": version,
        "relations": [relation.canonical_dict() for relation in ordered],
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    total_error = sum(Decimal(r.rounding_error_units) for r in ordered)
    normalization = {
        "ratio_scale": str(RATIO_SCALE),
        "per_record": [
            {
                "relation": relation.name,
                "source_kind": relation.source_kind,
                "source_precision_places": relation.source_precision_places,
                "rounding_place": relation.rounding_place,
                "rounding_error_units": relation.rounding_error_units,
            }
            for relation in ordered
        ],
        "cumulative_rounding_error_units": format_decimal(total_error, 12),
        "rounding_note": (
            "All ratios are quantized half-even to 9 decimal places; the error is "
            "reported in ratio units at 1e-12 precision."
        ),
    }
    return StatsSnapshot(
        version=version,
        relations=ordered,
        digest=digest,
        format_name=format_name,
        format_version=format_version,
        decision=decision,
        degradations=tuple(degradations),
        normalization=normalization,
    )


@dataclass(frozen=True)
class Predicate:
    id: str
    kind: str
    relation_a: str
    column_a: str
    relation_b: str | None = None
    column_b: str | None = None
    range_fraction_permille: int | None = None
    correlation_fixed: int | None = None

    @property
    def relations(self) -> tuple[str, ...]:
        if self.relation_b and self.relation_b != self.relation_a:
            return (self.relation_a, self.relation_b)
        return (self.relation_a,)

    def public_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "relation_a": self.relation_a,
            "column_a": self.column_a,
        }
        if self.relation_b is not None:
            result["relation_b"] = self.relation_b
        if self.column_b is not None:
            result["column_b"] = self.column_b
        if self.range_fraction_permille is not None:
            result["range_fraction_permille"] = self.range_fraction_permille
        if self.correlation_fixed is not None:
            from .fixed import format_fixed

            result["correlation_coefficient"] = format_fixed(
                self.correlation_fixed, RATIO_SCALE, 9
            )
        return result


def _pick(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def parse_predicate(raw: dict[str, Any], index: int) -> Predicate:
    if not isinstance(raw, dict):
        raise BusinessError(E_INPUT_INVALID, "predicate must be an object")
    predicate_id = str(raw.get("id") or f"p{index + 1}")
    kind = str(raw.get("kind") or raw.get("type") or "").lower()
    kind = {"eq": "equality", "range_join": "range", "self": "self_join"}.get(kind, kind)
    left = raw.get("left") if isinstance(raw.get("left"), dict) else {}
    right = raw.get("right") if isinstance(raw.get("right"), dict) else {}
    relation_a = _pick(raw, "relation_a", "relation", "left_relation")
    column_a = _pick(raw, "column_a", "column", "left_column")
    relation_b = _pick(raw, "relation_b", "join_relation", "right_relation")
    column_b = _pick(raw, "column_b", "join_column", "right_column")
    if relation_a is None:
        relation_a = left.get("relation")
    if column_a is None:
        column_a = left.get("column")
    if relation_b is None:
        relation_b = right.get("relation")
    if column_b is None:
        column_b = right.get("column")
    if not isinstance(relation_a, str) or not isinstance(column_a, str):
        raise BusinessError(
            E_INPUT_INVALID,
            "predicate is missing relation_a/column_a",
            {"predicate": predicate_id},
        )
    if kind not in {"equality", "range", "self_join"}:
        raise BusinessError(
            E_INPUT_INVALID,
            "predicate kind must be equality, range, or self_join",
            {"predicate": predicate_id, "kind": kind},
        )
    if kind == "equality":
        if not isinstance(relation_b, str) or not isinstance(column_b, str):
            raise BusinessError(
                E_INPUT_INVALID,
                "equality predicate requires relation_b/column_b",
                {"predicate": predicate_id},
            )
    if kind == "self_join":
        if not isinstance(column_b, str):
            raise BusinessError(
                E_INPUT_INVALID,
                "self_join predicate requires column_b",
                {"predicate": predicate_id},
            )
        relation_b = relation_a
    range_fraction = raw.get("range_fraction_permille")
    if kind == "range":
        if range_fraction is None:
            range_fraction = 333
        if (
            isinstance(range_fraction, bool)
            or not isinstance(range_fraction, int)
            or range_fraction <= 0
            or range_fraction > 1000
        ):
            raise BusinessError(
                E_STAT_INVALID,
                "range_fraction_permille must be an integer in [1, 1000]",
                {"predicate": predicate_id, "value": range_fraction},
            )
    else:
        range_fraction = None
    correlation_value = raw.get("correlation_coefficient")
    correlation_fixed = None
    if correlation_value is not None:
        try:
            correlation_fixed, _, _ = quantize_ratio(correlation_value)
        except Exception as exc:
            raise BusinessError(
                E_STAT_INVALID,
                "predicate correlation coefficient is invalid",
                {"predicate": predicate_id, "reason": str(exc)},
            ) from exc
        if correlation_fixed < 0 or correlation_fixed > RATIO_SCALE:
            raise BusinessError(
                E_STAT_INVALID,
                "predicate correlation coefficient must be in [0, 1]",
                {"predicate": predicate_id},
            )
    if relation_b is not None and not isinstance(relation_b, str):
        raise BusinessError(E_INPUT_INVALID, "relation_b must be a string")
    return Predicate(
        id=predicate_id,
        kind=kind,
        relation_a=relation_a,
        column_a=column_a,
        relation_b=relation_b if relation_b != relation_a else None,
        column_b=column_b,
        range_fraction_permille=range_fraction,
        correlation_fixed=correlation_fixed,
    )


@dataclass(frozen=True)
class PlanOptions:
    max_relations: int = 8
    tolerance_ppm: int = 1_000
    row_cost_micros: int = 1_000_000

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> "PlanOptions":
        if not raw:
            return cls()
        options = raw.get("options", raw)
        max_relations = options.get("max_relations", 8)
        tolerance_ppm = options.get("tolerance_ppm", 1_000)
        if (
            isinstance(max_relations, bool)
            or not isinstance(max_relations, int)
            or max_relations < 1
            or max_relations > MAX_SEARCH_RELATIONS
        ):
            raise BusinessError(
                E_INPUT_INVALID,
                f"max_relations must be an integer in [1, {MAX_SEARCH_RELATIONS}]",
            )
        if (
            isinstance(tolerance_ppm, bool)
            or not isinstance(tolerance_ppm, int)
            or tolerance_ppm < 0
            or tolerance_ppm > 1_000_000
        ):
            raise BusinessError(
                E_INPUT_INVALID, "tolerance_ppm must be an integer in [0, 1000000]"
            )
        return cls(max_relations=max_relations, tolerance_ppm=tolerance_ppm)


def normalize_version(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BusinessError(E_INPUT_INVALID, "snapshot_version must be a non-negative integer")
    return value


def coerce_snapshot_from_dict(raw: dict[str, Any]) -> StatsSnapshot:
    try:
        relations = [
            RelationStats(
                name=item["name"],
                row_count=int(item["row_count"]),
                ndv_ratio_fixed=int(item["ndv_ratio_fixed"]),
                correlation_fixed=int(item["correlation_fixed"]),
                stale_keys=tuple(item.get("stale_keys", ())),
                source_precision_places=int(item.get("source_precision_places", 3)),
                rounding_place=int(item.get("rounding_place", 9)),
                rounding_error_units=str(item.get("rounding_error_units", "0")),
                source_kind=str(item.get("source_kind", "stored")),
            )
            for item in raw["relations"]
        ]
        return StatsSnapshot(
            version=int(raw["version"]),
            relations=tuple(relations),
            digest=str(raw["digest"]),
            format_name=str(raw.get("format_name", "sqlite")),
            format_version=int(raw.get("format_version", 0)),
            decision=str(raw.get("decision", "ACCEPT")),
            degradations=tuple(raw.get("degradations", ())),
            normalization=dict(raw.get("normalization", {})),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BusinessError(E_INTERNAL, "stored snapshot is corrupt", {"reason": str(exc)})


def parse_fraction_to_fixed(value: Any, field_name: str) -> int:
    try:
        decimal = decimal_from(value)
    except Exception as exc:
        raise BusinessError(E_STAT_INVALID, f"{field_name} must be numeric") from exc
    if decimal < 0 or decimal > 1:
        raise BusinessError(E_STAT_INVALID, f"{field_name} must be in [0, 1]")
    fixed, _, _ = quantize_ratio(decimal)
    return clamp_int(fixed, 0, RATIO_SCALE)

