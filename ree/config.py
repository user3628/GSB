from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import REEError
from .models import sha256_value


class RelationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    head: list[str]
    tail: list[str]


class EventSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    roles: dict[str, list[str]]


class AlignmentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["strict", "expand"] = "strict"


class LegacySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alignment: AlignmentSpec = Field(default_factory=AlignmentSpec)
    name_disambiguation: Literal["first", "longest"] = "first"


class Features(BaseModel):
    model_config = ConfigDict(extra="forbid")

    use_entity_lexicon: bool = True
    use_relation_cues: bool = True
    use_argument_proximity: bool = True
    use_transition_scores: bool = True


class ExtractionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_entity_tokens: int = 6
    max_trigger_tokens: int = 4
    max_relation_distance: int = 16
    max_argument_distance: int = 12
    max_arguments_per_role: int = 3


class REEConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = 2
    entity_types: list[str]
    relation_types: dict[str, RelationSpec]
    event_types: dict[str, EventSpec]
    triggers: dict[str, list[str]]
    features: Features = Field(default_factory=Features)
    smoothing: float = Field(default=0.5, ge=0.0)
    confidence_threshold: float = Field(default=0.2, ge=0.0, le=1.0)
    legacy: LegacySpec = Field(default_factory=LegacySpec)
    extraction: ExtractionSpec = Field(default_factory=ExtractionSpec)

    @model_validator(mode="after")
    def validate_schema(self) -> "REEConfig":
        entity_types = set(self.entity_types)
        if not entity_types:
            raise ValueError("entity_types must not be empty")
        for relation_type, spec in self.relation_types.items():
            if not spec.head or not spec.tail:
                raise ValueError(f"relation {relation_type} needs non-empty head/tail")
            unknown = set(spec.head + spec.tail) - entity_types
            if unknown:
                raise ValueError(
                    f"relation {relation_type} references unknown entity types: {sorted(unknown)}"
                )
        for event_type, spec in self.event_types.items():
            if not spec.roles:
                raise ValueError(f"event {event_type} needs at least one role")
            for role, allowed in spec.roles.items():
                unknown = set(allowed) - entity_types
                if unknown:
                    raise ValueError(
                        f"event {event_type}.{role} references unknown entity types: {sorted(unknown)}"
                    )
        unknown_triggers = set(self.triggers) - set(self.event_types)
        if unknown_triggers:
            raise ValueError(f"triggers reference unknown event types: {sorted(unknown_triggers)}")
        for event_type, trigger_words in self.triggers.items():
            if not trigger_words:
                raise ValueError(f"event {event_type} needs at least one trigger")
        return self

    def digest(self) -> str:
        return sha256_value(self.model_dump(mode="json"))

    def stage_components(self, stage: str) -> dict[str, object]:
        schema = {
            "entity_types": sorted(self.entity_types),
            "relation_types": {
                key: {
                    "head": sorted(value.head),
                    "tail": sorted(value.tail),
                }
                for key, value in sorted(self.relation_types.items())
            },
            "event_types": {
                key: {role: sorted(types) for role, types in sorted(value.roles.items())}
                for key, value in sorted(self.event_types.items())
            },
        }
        if stage == "parse":
            return {"legacy": self.legacy.model_dump(mode="json")}
        if stage == "estimate":
            return {"schema": schema, "smoothing": self.smoothing}
        if stage == "extract":
            return {
                "schema": schema,
                "triggers": {
                    key: sorted(words) for key, words in sorted(self.triggers.items())
                },
                "features": self.features.model_dump(mode="json"),
                "confidence_threshold": self.confidence_threshold,
                "extraction": self.extraction.model_dump(mode="json"),
            }
        if stage == "validate":
            return {
                "supported_versions": [1, 2],
                "roundtrip_formats": ["BIO", "JSON_INTERVALS", "LEGACY_FLAT_V1"],
            }
        raise ValueError(f"unknown stage: {stage}")


def load_config(path: str | Path) -> REEConfig:
    path = Path(path)
    if not path.exists():
        raise REEError(
            "E_MISSING_CONFIG",
            "config",
            f"configuration file not found: {path.as_posix()}",
        )
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return REEConfig.model_validate(raw)
    except REEError:
        raise
    except Exception as exc:
        raise REEError(
            "E_INVALID_CONFIG",
            "config",
            f"invalid extraction config: {exc}",
        ) from exc
