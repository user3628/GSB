from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .alignment import align_char_span, token_char_spans, token_span_to_chars
from .config import REEConfig
from .errors import REEError, unsupported_version
from .models import (
    CURRENT_SCHEMA_VERSION,
    DocumentIR,
    Entity,
    Event,
    Relation,
    assign_ids,
    normalize_document,
)

BIO_LABEL_RE = re.compile(r"^([BIES])-(.+)$")
FORMAT_ALIASES = {
    "BIO": "BIO",
    "BIOES": "BIO",
    "JSON": "JSON_INTERVALS",
    "JSON_INTERVALS": "JSON_INTERVALS",
    "JSON_INTERVAL": "JSON_INTERVALS",
    "LEGACY": "LEGACY_FLAT_V1",
    "LEGACY_FLAT": "LEGACY_FLAT_V1",
    "LEGACY_FLAT_V1": "LEGACY_FLAT_V1",
    "V1": "LEGACY_FLAT_V1",
}


@dataclass
class DecodedFormat:
    document: DocumentIR
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    upgrades: list[str] = field(default_factory=list)
    source_version: int = CURRENT_SCHEMA_VERSION


def canonical_format_name(name: str) -> str:
    normalized = name.strip().upper().replace("-", "_")
    if normalized not in FORMAT_ALIASES:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            f"unknown annotation format: {name}",
            details={"allowed_formats": sorted(FORMAT_ALIASES)},
        )
    return FORMAT_ALIASES[normalized]


def _error_diagnostic(error: REEError, *, stage: str | None = None) -> dict[str, Any]:
    payload = error.as_dict()
    payload["stage"] = stage or error.stage
    return payload


def _assign_bio_layers(entities: list[Entity]) -> list[list[Entity]]:
    layers: list[list[Entity]] = []
    for entity in sorted(
        entities, key=lambda item: (item.start, -item.end, item.type, item.id)
    ):
        for layer in layers:
            if all(
                entity.end <= existing.start or entity.start >= existing.end
                for existing in layer
            ):
                layer.append(entity)
                break
        else:
            layers.append([entity])
    return layers or [[]]


def _bio_tags_for_layer(entities: list[Entity], token_count: int) -> list[str]:
    tags = ["O"] * token_count
    for entity in sorted(entities, key=lambda item: (item.start, item.end, item.id)):
        length = entity.end - entity.start
        if length == 1:
            tags[entity.start] = f"S-{entity.type}"
        else:
            tags[entity.start] = f"B-{entity.type}"
            for index in range(entity.start + 1, entity.end - 1):
                tags[index] = f"I-{entity.type}"
            tags[entity.end - 1] = f"E-{entity.type}"
    return tags


def bio_layers_for(document: DocumentIR) -> list[list[str]]:
    normalized = normalize_document(document)
    return [
        _bio_tags_for_layer(layer, len(normalized.tokens))
        for layer in _assign_bio_layers(normalized.entities)
    ]


def encode_bio(document: DocumentIR) -> dict[str, Any]:
    document = normalize_document(document)
    layer_entities = _assign_bio_layers(document.entities)
    layer_payloads = [
        {
            "name": f"layer_{index + 1}",
            "tags": _bio_tags_for_layer(layer, len(document.tokens)),
        }
        for index, layer in enumerate(layer_entities)
    ]
    return {
        "format": "BIO",
        "schema_version": CURRENT_SCHEMA_VERSION,
        "doc_id": document.doc_id,
        "tokens": document.tokens,
        "text": document.text,
        "source": document.source,
        "tags": layer_payloads[0]["tags"],
        "bio_layers": layer_payloads,
        "companion": {
            "entities": [entity.model_dump(mode="json") for entity in document.entities],
            "relations": [
                relation.model_dump(mode="json") for relation in document.relations
            ],
            "events": [event.model_dump(mode="json") for event in document.events],
        },
    }


def _decode_bio_tags(tags: list[str], token_count: int) -> list[tuple[int, int, str]]:
    if len(tags) != token_count:
        raise REEError(
            "E_BIO_INVALID",
            "format",
            "BIO tag count does not match token count",
            details={"tag_count": len(tags), "token_count": token_count},
        )
    spans: list[tuple[int, int, str]] = []
    open_start: int | None = None
    open_type: str | None = None
    for index, tag in enumerate(tags):
        if tag == "O":
            if open_start is not None:
                raise REEError(
                    "E_BIO_INVALID",
                    "format",
                    "open BIO span interrupted by O",
                    details={"token_index": index},
                )
            continue
        match = BIO_LABEL_RE.match(tag)
        if not match:
            raise REEError(
                "E_BIO_INVALID",
                "format",
                f"invalid BIO label: {tag}",
                details={"token_index": index},
            )
        prefix, entity_type = match.groups()
        if prefix == "S":
            if open_start is not None:
                raise REEError(
                    "E_BIO_INVALID",
                    "format",
                    "S label found while another span is open",
                    details={"token_index": index},
                )
            spans.append((index, index + 1, entity_type))
            continue
        if prefix == "B":
            if open_start is not None:
                raise REEError(
                    "E_BIO_INVALID",
                    "format",
                    "B label found while another span is open",
                    details={"token_index": index},
                )
            open_start = index
            open_type = entity_type
            continue
        if prefix == "I":
            if open_start is None or open_type != entity_type:
                raise REEError(
                    "E_BIO_INVALID",
                    "format",
                    "I label without matching B label",
                    details={"token_index": index, "label": tag},
                )
            continue
        if prefix == "E":
            if open_start is None or open_type != entity_type:
                raise REEError(
                    "E_BIO_INVALID",
                    "format",
                    "E label without matching B label",
                    details={"token_index": index, "label": tag},
                )
            spans.append((open_start, index + 1, entity_type))
            open_start = None
            open_type = None
    if open_start is not None:
        raise REEError(
            "E_BIO_INVALID",
            "format",
            "unterminated BIO span",
            details={"start": open_start, "type": open_type},
        )
    return spans


def decode_bio(payload: dict[str, Any], config: REEConfig | None = None) -> DecodedFormat:
    del config
    version = int(payload.get("schema_version", CURRENT_SCHEMA_VERSION))
    if version > CURRENT_SCHEMA_VERSION:
        raise unsupported_version(
            "format", version, CURRENT_SCHEMA_VERSION, doc_id=payload.get("doc_id")
        )
    if version != CURRENT_SCHEMA_VERSION:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            f"BIO payload cannot declare schema version {version}",
            doc_id=payload.get("doc_id"),
        )
    tokens = payload.get("tokens")
    if not isinstance(tokens, list) or not all(isinstance(token, str) for token in tokens):
        raise REEError("E_INVALID_FORMAT", "format", "BIO payload requires string tokens")
    doc_id = str(payload.get("doc_id", ""))
    if not doc_id:
        raise REEError("E_INVALID_FORMAT", "format", "BIO payload requires doc_id")
    raw_layers = payload.get("bio_layers")
    if raw_layers is None:
        if "tags" not in payload:
            raise REEError("E_INVALID_FORMAT", "format", "BIO payload requires tags")
        raw_layers = [{"name": "layer_1", "tags": payload["tags"]}]
    if not isinstance(raw_layers, list) or not raw_layers:
        raise REEError("E_INVALID_FORMAT", "format", "bio_layers must be a non-empty list")

    tag_spans: list[tuple[int, int, str]] = []
    for layer in raw_layers:
        tags = layer.get("tags") if isinstance(layer, dict) else None
        if not isinstance(tags, list):
            raise REEError("E_INVALID_FORMAT", "format", "each BIO layer needs tags")
        tag_spans.extend(_decode_bio_tags(tags, len(tokens)))
    if len(set(tag_spans)) != len(tag_spans):
        raise REEError("E_BIO_INVALID", "format", "duplicate spans across BIO layers")

    companion = payload.get("companion") or {}
    try:
        companion_entities = [
            Entity.model_validate(item) for item in companion.get("entities", [])
        ]
        companion_relations = [
            Relation.model_validate(item) for item in companion.get("relations", [])
        ]
        companion_events = [
            Event.model_validate(item) for item in companion.get("events", [])
        ]
    except Exception as exc:
        raise REEError(
            "E_INVALID_FORMAT", "format", f"invalid BIO companion record: {exc}"
        ) from exc

    if companion_entities:
        by_span: dict[tuple[int, int, str], list[Entity]] = {}
        for entity in companion_entities:
            by_span.setdefault((entity.start, entity.end, entity.type), []).append(entity)
        entities: list[Entity] = []
        for span in tag_spans:
            matches = by_span.get(span, [])
            if len(matches) != 1:
                raise REEError(
                    "E_BIO_COMPANION_MISMATCH",
                    "format",
                    "BIO tags and companion entities disagree",
                    details={"span": list(span), "matches": len(matches)},
                )
            entities.append(matches[0])
        if len(companion_entities) != len(tag_spans):
            raise REEError(
                "E_BIO_COMPANION_MISMATCH",
                "format",
                "companion entity count does not match BIO spans",
                details={
                    "companion_entities": len(companion_entities),
                    "tag_spans": len(tag_spans),
                },
            )
    else:
        entities = [
            Entity(id=f"tmp{index}", type=entity_type, start=start, end=end)
            for index, (start, end, entity_type) in enumerate(sorted(tag_spans), start=1)
        ]

    try:
        document = DocumentIR(
            doc_id=doc_id,
            tokens=tokens,
            text=str(payload.get("text", " ".join(tokens))),
            source=str(payload.get("source", "unknown")),
            entities=entities,
            relations=companion_relations,
            events=companion_events,
        )
    except Exception as exc:
        raise REEError("E_INVALID_FORMAT", "format", f"invalid BIO document: {exc}") from exc
    if not companion_entities:
        document = assign_ids(document)
    return DecodedFormat(document=normalize_document(document))


def encode_json_intervals(document: DocumentIR) -> dict[str, Any]:
    document = normalize_document(document)
    return {
        "format": "JSON_INTERVALS",
        "schema_version": CURRENT_SCHEMA_VERSION,
        "doc_id": document.doc_id,
        "tokens": document.tokens,
        "text": document.text,
        "source": document.source,
        "entities": [entity.model_dump(mode="json") for entity in document.entities],
        "relations": [
            relation.model_dump(mode="json") for relation in document.relations
        ],
        "events": [event.model_dump(mode="json") for event in document.events],
    }


def decode_json_intervals(
    payload: dict[str, Any], config: REEConfig | None = None
) -> DecodedFormat:
    doc_id = payload.get("doc_id")
    version = int(payload.get("schema_version", CURRENT_SCHEMA_VERSION))
    if version > CURRENT_SCHEMA_VERSION:
        raise unsupported_version(
            "format", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
        )
    if version < 1:
        raise unsupported_version(
            "format", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
        )
    if version == 1:
        if config is None:
            raise REEError(
                "E_COMPAT_CONFIG_REQUIRED",
                "format",
                "legacy schema upgrade requires extraction configuration",
                doc_id=doc_id,
            )
        return decode_legacy_flat_v1(payload, config)
    try:
        tokens = payload["tokens"]
        if not isinstance(tokens, list) or not all(isinstance(item, str) for item in tokens):
            raise ValueError("tokens must be a list of strings")
        entities_raw = payload.get("entities", [])
        entities: list[Entity] = []
        for index, item in enumerate(entities_raw, start=1):
            data = dict(item)
            data.setdefault("id", f"tmp{index}")
            entities.append(Entity.model_validate(data))
        relations = [Relation.model_validate(item) for item in payload.get("relations", [])]
        events: list[Event] = []
        for item in payload.get("events", []):
            data = dict(item)
            trigger = data.pop("trigger", None)
            if trigger is not None:
                data.setdefault("trigger_start", trigger.get("start"))
                data.setdefault("trigger_end", trigger.get("end"))
            arguments = data.get("arguments", {})
            normalized_arguments: dict[str, list[str]] = {}
            for role, value in arguments.items():
                if isinstance(value, str):
                    normalized_arguments[role] = [value]
                else:
                    normalized_arguments[role] = list(value)
            data["arguments"] = normalized_arguments
            events.append(Event.model_validate(data))
        document = DocumentIR(
            doc_id=str(payload["doc_id"]),
            tokens=tokens,
            text=str(payload.get("text", " ".join(tokens))),
            source=str(payload.get("source", "unknown")),
            entities=entities,
            relations=relations,
            events=events,
        )
    except REEError:
        raise
    except Exception as exc:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            f"invalid JSON interval payload: {exc}",
            doc_id=str(doc_id) if doc_id is not None else None,
        ) from exc
    generated_ids = [entity.id.startswith("tmp") for entity in document.entities]
    if any(generated_ids) and not all(generated_ids):
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            "mixed explicit and generated entity IDs are ambiguous",
        )
    if any(generated_ids):
        document = assign_ids(document)
    return DecodedFormat(document=normalize_document(document))


def _legacy_names(document: DocumentIR) -> tuple[dict[str, str], dict[str, str]]:
    used: dict[str, int] = {}
    entity_name: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    prefixes: dict[str, str] = {}
    for entity in document.entities:
        prefix = " ".join(document.tokens[entity.start : entity.end]).strip() or entity.type
        seen = used.get(prefix, 0) + 1
        used[prefix] = seen
        name = prefix if seen == 1 else f"{prefix}#{seen}"
        prefixes[entity.id] = prefix
        entity_name[entity.id] = name
        name_to_id[name] = entity.id
    return entity_name, name_to_id


def encode_legacy_flat_v1(document: DocumentIR) -> dict[str, Any]:
    document = normalize_document(document)
    entity_name, _ = _legacy_names(document)
    entity_by_id = {entity.id: entity for entity in document.entities}
    token_spans = token_char_spans(document.tokens, document.text, doc_id=document.doc_id)

    entities_payload: list[dict[str, Any]] = []
    for entity in document.entities:
        char_start, char_end = token_spans[entity.start][0], token_spans[entity.end - 1][1]
        entities_payload.append(
            {
                "name": entity_name[entity.id],
                "type": entity.type,
                "char_start": char_start,
                "char_end": char_end,
            }
        )

    relations_payload = [
        {
            "type": relation.type,
            "head": entity_name[relation.head],
            "tail": entity_name[relation.tail],
        }
        for relation in document.relations
    ]

    events_payload: list[dict[str, Any]] = []
    for event in document.events:
        char_start, char_end = token_span_to_chars(
            document.tokens,
            document.text,
            event.trigger_start,
            event.trigger_end,
            doc_id=document.doc_id,
        )
        trigger = " ".join(document.tokens[event.trigger_start : event.trigger_end])
        roles = sorted(event.arguments)
        max_members = max((len(event.arguments[role]) for role in roles), default=0)
        record_count = max(1, max_members)
        for record_index in range(record_count):
            arguments: dict[str, str] = {}
            for role in roles:
                members = event.arguments[role]
                if record_index < len(members):
                    entity_id = members[record_index]
                    if entity_id not in entity_by_id:
                        raise REEError(
                            "E_INVALID_FORMAT",
                            "format",
                            f"event {event.id} references unknown entity {entity_id}",
                            doc_id=document.doc_id,
                        )
                    arguments[role] = entity_name[entity_id]
            events_payload.append(
                {
                    "event_id": event.id,
                    "type": event.type,
                    "trigger": trigger,
                    "trigger_char_start": char_start,
                    "trigger_char_end": char_end,
                    "arguments": arguments,
                }
            )

    return {
        "format": "LEGACY_FLAT_V1",
        "schema_version": 1,
        "doc_id": document.doc_id,
        "tokens": document.tokens,
        "text": document.text,
        "source": document.source,
        "entities": entities_payload,
        "relations": relations_payload,
        "events": events_payload,
    }


def _legacy_trigger_offsets(
    record: dict[str, Any], text: str, doc_id: str
) -> tuple[int, int]:
    if "trigger_char_start" in record and "trigger_char_end" in record:
        return int(record["trigger_char_start"]), int(record["trigger_char_end"])
    trigger = record.get("trigger")
    if not isinstance(trigger, str) or not trigger:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            "legacy event requires trigger offsets or trigger text",
            doc_id=doc_id,
        )
    index = text.find(trigger)
    if index < 0:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            f"legacy trigger {trigger!r} is not present in text",
            doc_id=doc_id,
        )
    return index, index + len(trigger)


def decode_legacy_flat_v1(
    payload: dict[str, Any], config: REEConfig
) -> DecodedFormat:
    doc_id = str(payload.get("doc_id", ""))
    version = int(payload.get("schema_version", 1))
    if version > CURRENT_SCHEMA_VERSION:
        raise unsupported_version(
            "format", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id or None
        )
    if version != 1:
        raise REEError(
            "E_INVALID_FORMAT",
            "format",
            f"legacy decoder requires schema version 1, got {version}",
            doc_id=doc_id or None,
        )
    if not doc_id:
        raise REEError("E_INVALID_FORMAT", "format", "legacy payload requires doc_id")
    tokens = payload.get("tokens")
    text = payload.get("text")
    if not isinstance(tokens, list) or not all(isinstance(item, str) for item in tokens):
        raise REEError(
            "E_INVALID_FORMAT", "format", "legacy payload requires string tokens", doc_id=doc_id
        )
    if not isinstance(text, str):
        raise REEError(
            "E_INVALID_FORMAT", "format", "legacy payload requires text", doc_id=doc_id
        )

    diagnostics: list[dict[str, Any]] = []
    name_records: list[tuple[str, str, int, int, str]] = []
    for index, item in enumerate(payload.get("entities", []), start=1):
        try:
            char_start = int(item["char_start"])
            char_end = int(item["char_end"])
            alignment = align_char_span(
                tokens,
                text,
                char_start,
                char_end,
                mode=config.legacy.alignment.mode,
                doc_id=doc_id,
            )
            if alignment.adjusted:
                diagnostics.append(
                    {
                        "code": "W_ALIGN_EXPANDED",
                        "stage": "format",
                        "doc_id": doc_id,
                        "offset": char_start,
                        "message": "character interval expanded to token boundaries",
                        "details": {
                            "char_start": char_start,
                            "char_end": char_end,
                            "aligned_char_start": alignment.char_start,
                            "aligned_char_end": alignment.char_end,
                        },
                    }
                )
            name = str(item.get("name", "")).strip()
            entity_type = str(item.get("type", ""))
            if not name or not entity_type:
                raise ValueError("legacy entity requires name and type")
            name_records.append(
                (f"tmp{index}", name, alignment.start, alignment.end, entity_type)
            )
        except REEError as exc:
            diagnostics.append(_error_diagnostic(exc, stage="format"))
        except Exception as exc:
            diagnostics.append(
                {
                    "code": "E_INVALID_FORMAT",
                    "stage": "format",
                    "doc_id": doc_id,
                    "message": f"invalid legacy entity: {exc}",
                }
            )

    seed_entities = [
        Entity(id=temp_id, type=entity_type, start=start, end=end)
        for temp_id, _name, start, end, entity_type in name_records
    ]
    ordered_entities = sorted(
        seed_entities,
        key=lambda entity: (entity.start, entity.end, entity.type, entity.id),
    )
    id_map = {
        entity.id: f"e{index}"
        for index, entity in enumerate(ordered_entities, start=1)
    }
    canonical_entities = [
        entity.model_copy(update={"id": id_map[entity.id]}) for entity in seed_entities
    ]
    base_document = DocumentIR(
        doc_id=doc_id,
        tokens=tokens,
        text=text,
        source=str(payload.get("source", "unknown")),
        entities=canonical_entities,
    )
    base_document = assign_ids(base_document)

    name_to_ids: dict[str, list[str]] = {}
    for temp_id, name, _start, _end, _entity_type in name_records:
        name_to_ids.setdefault(name, []).append(id_map[temp_id])
    entity_by_id = {entity.id: entity for entity in base_document.entities}
    for ids in name_to_ids.values():
        ids.sort(key=lambda entity_id: (entity_by_id[entity_id].start, entity_id))

    def resolve_name(name: str) -> str | None:
        candidates = name_to_ids.get(name, [])
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        diagnostics.append(
            {
                "code": "E_AMBIGUOUS_NAME",
                "stage": "format",
                "doc_id": doc_id,
                "message": f"legacy entity name {name!r} is ambiguous",
                "details": {"name": name, "candidates": candidates},
            }
        )
        if config.legacy.name_disambiguation == "longest":
            candidates = sorted(
                candidates,
                key=lambda entity_id: (
                    -(entity_by_id[entity_id].end - entity_by_id[entity_id].start),
                    entity_by_id[entity_id].start,
                    entity_id,
                ),
            )
        return candidates[0]

    relations: list[Relation] = []
    for item in payload.get("relations", []):
        head = resolve_name(str(item.get("head", "")))
        tail = resolve_name(str(item.get("tail", "")))
        if head is None or tail is None:
            diagnostics.append(
                {
                    "code": "E_MISSING_ENTITY_REF",
                    "stage": "format",
                    "doc_id": doc_id,
                    "message": "legacy relation references an unavailable entity name",
                    "details": {"head": item.get("head"), "tail": item.get("tail")},
                }
            )
            continue
        try:
            relations.append(
                Relation(
                    id=f"tmpr{len(relations) + 1}",
                    type=str(item["type"]),
                    head=head,
                    tail=tail,
                )
            )
        except Exception as exc:
            diagnostics.append(
                {
                    "code": "E_INVALID_FORMAT",
                    "stage": "format",
                    "doc_id": doc_id,
                    "message": f"invalid legacy relation: {exc}",
                }
            )

    event_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for item in payload.get("events", []):
        try:
            char_start, char_end = _legacy_trigger_offsets(item, text, doc_id)
            alignment = align_char_span(
                tokens,
                text,
                char_start,
                char_end,
                mode=config.legacy.alignment.mode,
                doc_id=doc_id,
            )
            if alignment.adjusted:
                diagnostics.append(
                    {
                        "code": "W_ALIGN_EXPANDED",
                        "stage": "format",
                        "doc_id": doc_id,
                        "offset": char_start,
                        "message": "trigger interval expanded to token boundaries",
                    }
                )
            event_type = str(item["type"])
            event_id = item.get("event_id")
            group_key = (
                ("event_id", str(event_id))
                if event_id is not None
                else ("trigger", event_type, alignment.start, alignment.end)
            )
            group = event_groups.setdefault(
                group_key,
                {
                    "type": event_type,
                    "trigger_start": alignment.start,
                    "trigger_end": alignment.end,
                    "arguments": {},
                },
            )
            arguments = item.get("arguments", {})
            for role, value in arguments.items():
                values = [value] if isinstance(value, str) else list(value)
                for name in values:
                    entity_id = resolve_name(str(name))
                    if entity_id is None:
                        diagnostics.append(
                            {
                                "code": "E_MISSING_ENTITY_REF",
                                "stage": "format",
                                "doc_id": doc_id,
                                "message": "legacy event argument references an unavailable entity name",
                                "details": {"role": role, "name": name},
                            }
                        )
                        continue
                    group["arguments"].setdefault(str(role), []).append(entity_id)
        except REEError as exc:
            diagnostics.append(_error_diagnostic(exc, stage="format"))
        except Exception as exc:
            diagnostics.append(
                {
                    "code": "E_INVALID_FORMAT",
                    "stage": "format",
                    "doc_id": doc_id,
                    "message": f"invalid legacy event: {exc}",
                }
            )

    events = [
        Event(
            id=f"tmpev{index}",
            type=group["type"],
            trigger_start=group["trigger_start"],
            trigger_end=group["trigger_end"],
            arguments={
                role: sorted(dict.fromkeys(members))
                for role, members in sorted(group["arguments"].items())
            },
        )
        for index, group in enumerate(event_groups.values(), start=1)
    ]
    document = DocumentIR(
        doc_id=doc_id,
        tokens=tokens,
        text=text,
        source=str(payload.get("source", "unknown")),
        entities=canonical_entities,
        relations=relations,
        events=events,
    )
    document = assign_ids(document)
    upgrades = [
        "char_offsets_to_token_indices",
        "entity_names_to_ids",
        "single_argument_to_list",
    ]
    document = document.model_copy(
        update={
            "provenance": {
                "source_annotation_version": 1,
                "target_schema_version": CURRENT_SCHEMA_VERSION,
                "source_format": "LEGACY_FLAT_V1",
                "upgrades": upgrades,
            }
        }
    )
    diagnostics.sort(
        key=lambda item: (
            str(item.get("doc_id", "")),
            str(item.get("code", "")),
            int(item.get("offset", -1)),
            str(item.get("message", "")),
        )
    )
    return DecodedFormat(
        document=normalize_document(document),
        diagnostics=diagnostics,
        upgrades=upgrades,
        source_version=1,
    )


def decode_format(
    format_name: str, payload: dict[str, Any], config: REEConfig | None = None
) -> DecodedFormat:
    canonical = canonical_format_name(format_name)
    if canonical == "BIO":
        return decode_bio(payload, config)
    if canonical == "JSON_INTERVALS":
        return decode_json_intervals(payload, config)
    if config is None:
        raise REEError(
            "E_COMPAT_CONFIG_REQUIRED",
            "format",
            "legacy decoding requires extraction configuration",
        )
    return decode_legacy_flat_v1(payload, config)


def encode_format(format_name: str, document: DocumentIR) -> dict[str, Any]:
    canonical = canonical_format_name(format_name)
    if canonical == "BIO":
        return encode_bio(document)
    if canonical == "JSON_INTERVALS":
        return encode_json_intervals(document)
    return encode_legacy_flat_v1(document)
