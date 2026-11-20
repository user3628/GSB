from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

CURRENT_SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = (1, 2)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_value(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


class Entity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    start: int
    end: int

    @model_validator(mode="after")
    def validate_span(self) -> "Entity":
        if self.start < 0 or self.end <= self.start:
            raise ValueError(f"invalid entity span [{self.start}, {self.end})")
        if not self.id or not self.type:
            raise ValueError("entity id and type must be non-empty")
        return self


class Relation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    head: str
    tail: str

    @model_validator(mode="after")
    def validate_refs(self) -> "Relation":
        if not self.id or not self.type or not self.head or not self.tail:
            raise ValueError("relation id, type, head and tail must be non-empty")
        return self


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    trigger_start: int
    trigger_end: int
    arguments: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_event(self) -> "Event":
        if self.trigger_start < 0 or self.trigger_end <= self.trigger_start:
            raise ValueError(
                f"invalid trigger span [{self.trigger_start}, {self.trigger_end})"
            )
        if not self.id or not self.type:
            raise ValueError("event id and type must be non-empty")
        for role, members in self.arguments.items():
            if not role or not isinstance(members, list) or not members:
                raise ValueError("event argument roles must map to non-empty lists")
        return self


class DocumentIR(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str
    tokens: list[str]
    text: str
    source: str
    entities: list[Entity] = Field(default_factory=list)
    relations: list[Relation] = Field(default_factory=list)
    events: list[Event] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_document(self) -> "DocumentIR":
        if not self.doc_id or not self.tokens:
            raise ValueError("document requires doc_id and tokens")
        entity_ids = {entity.id for entity in self.entities}
        if len(entity_ids) != len(self.entities):
            raise ValueError("entity ids must be unique within a document")
        for entity in self.entities:
            if entity.end > len(self.tokens):
                raise ValueError(
                    f"entity {entity.id} end {entity.end} exceeds token count {len(self.tokens)}"
                )
        relation_ids = {relation.id for relation in self.relations}
        if len(relation_ids) != len(self.relations):
            raise ValueError("relation ids must be unique within a document")
        for relation in self.relations:
            if relation.head not in entity_ids or relation.tail not in entity_ids:
                raise ValueError(
                    f"relation {relation.id} references a missing entity"
                )
        event_ids = {event.id for event in self.events}
        if len(event_ids) != len(self.events):
            raise ValueError("event ids must be unique within a document")
        for event in self.events:
            if event.trigger_end > len(self.tokens):
                raise ValueError(f"event {event.id} trigger exceeds token count")
            for members in event.arguments.values():
                for entity_id in members:
                    if entity_id not in entity_ids:
                        raise ValueError(
                            f"event {event.id} references missing entity {entity_id}"
                        )
        return self


def normalize_document(document: DocumentIR) -> DocumentIR:
    """Return a deterministic, sorted copy without changing annotation IDs."""

    entities = sorted(
        document.entities,
        key=lambda entity: (entity.start, entity.end, entity.type, entity.id),
    )
    relations = sorted(
        document.relations,
        key=lambda relation: (relation.type, relation.head, relation.tail, relation.id),
    )
    events: list[Event] = []
    for event in document.events:
        arguments = {
            role: sorted(dict.fromkeys(members))
            for role, members in sorted(event.arguments.items())
            if members
        }
        events.append(
            event.model_copy(
                update={
                    "arguments": arguments,
                    "trigger_start": event.trigger_start,
                    "trigger_end": event.trigger_end,
                }
            )
        )
    events.sort(
        key=lambda event: (
            event.trigger_start,
            event.trigger_end,
            event.type,
            event.id,
        )
    )
    return document.model_copy(
        update={"entities": entities, "relations": relations, "events": events}
    )


def assign_ids(document: DocumentIR) -> DocumentIR:
    """Assign canonical IDs based on sorted annotation geometry."""

    sorted_entities = sorted(
        document.entities,
        key=lambda entity: (entity.start, entity.end, entity.type, entity.id),
    )
    entity_id_map = {
        entity.id: f"e{index}" for index, entity in enumerate(sorted_entities, start=1)
    }
    entities = [
        entity.model_copy(update={"id": entity_id_map[entity.id]})
        for entity in sorted_entities
    ]
    sorted_relations = sorted(
        document.relations,
        key=lambda relation: (
            entity_id_map[relation.head],
            entity_id_map[relation.tail],
            relation.type,
            relation.id,
        ),
    )
    relations = [
        relation.model_copy(
            update={
                "id": f"r{index}",
                "head": entity_id_map[relation.head],
                "tail": entity_id_map[relation.tail],
            }
        )
        for index, relation in enumerate(sorted_relations, start=1)
    ]
    sorted_events = sorted(
        document.events,
        key=lambda event: (
            event.trigger_start,
            event.trigger_end,
            event.type,
            event.id,
        ),
    )
    events = [
        event.model_copy(
            update={
                "id": f"ev{index}",
                "arguments": {
                    role: [entity_id_map[entity_id] for entity_id in members]
                    for role, members in sorted(event.arguments.items())
                },
            }
        )
        for index, event in enumerate(sorted_events, start=1)
    ]
    return DocumentIR(
        doc_id=document.doc_id,
        tokens=list(document.tokens),
        text=document.text,
        source=document.source,
        entities=entities,
        relations=relations,
        events=events,
        provenance=dict(document.provenance),
    )


def core_payload(document: DocumentIR) -> dict[str, Any]:
    normalized = normalize_document(document)
    return {
        "doc_id": normalized.doc_id,
        "tokens": normalized.tokens,
        "entities": [entity.model_dump(mode="json") for entity in normalized.entities],
        "relations": [
            relation.model_dump(mode="json") for relation in normalized.relations
        ],
        "events": [event.model_dump(mode="json") for event in normalized.events],
    }


def core_digest(document: DocumentIR) -> str:
    return sha256_value(core_payload(document))


def documents_core_digest(documents: list[DocumentIR]) -> str:
    return sha256_value([core_payload(document) for document in documents])
