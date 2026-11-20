from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable

from .config import REEConfig
from .formats import bio_layers_for
from .models import DocumentIR, normalize_document


def surface_key(tokens: list[str], start: int, end: int) -> str:
    return "".join(tokens[start:end]).casefold()


def _increment(nested: dict[str, Any], keys: Iterable[str]) -> None:
    current = nested
    keys = list(keys)
    for key in keys[:-1]:
        current = current.setdefault(key, {})
    current[keys[-1]] = current.get(keys[-1], 0) + 1


def estimate_counts(seed_documents: list[DocumentIR], config: REEConfig) -> dict[str, Any]:
    entity_type_counts: Counter[str] = Counter()
    entity_surface_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    relation_type_counts: Counter[str] = Counter()
    relation_surface_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    relation_cue_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    event_type_counts: Counter[str] = Counter()
    role_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    argument_type_counts: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    transition_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for raw_document in sorted(seed_documents, key=lambda document: document.doc_id):
        document = normalize_document(raw_document)
        entities = {entity.id: entity for entity in document.entities}
        for entity in document.entities:
            entity_type_counts[entity.type] += 1
            entity_surface_counts[surface_key(document.tokens, entity.start, entity.end)][
                entity.type
            ] += 1
        for relation in document.relations:
            relation_type_counts[relation.type] += 1
            head = entities[relation.head]
            tail = entities[relation.tail]
            head_surface = surface_key(document.tokens, head.start, head.end)
            tail_surface = surface_key(document.tokens, tail.start, tail.end)
            relation_surface_counts[head_surface + "\u0001" + tail_surface][
                relation.type
            ] += 1
            if head.end <= tail.start:
                gap_tokens = document.tokens[head.end : tail.start]
            elif tail.end <= head.start:
                gap_tokens = document.tokens[tail.end : head.start]
            else:
                gap_tokens = []
            for token in gap_tokens:
                relation_cue_counts[relation.type][token.casefold()] += 1
        for event in document.events:
            event_type_counts[event.type] += 1
            for role, members in event.arguments.items():
                role_counts[event.type][role] += len(members)
                for entity_id in members:
                    entity = entities[entity_id]
                    argument_type_counts[event.type][role][entity.type] += 1
        for layer in bio_layers_for(document):
            previous = "<START>"
            for label in layer:
                transition_counts[previous][label] += 1
                previous = label
            transition_counts[previous]["<END>"] += 1

    def sorted_nested(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: sorted_nested(value[key]) for key in sorted(value)}
        return value

    counts = {
        "entity_type_counts": dict(sorted(entity_type_counts.items())),
        "entity_surface_counts": sorted_nested(
            {key: dict(value) for key, value in entity_surface_counts.items()}
        ),
        "relation_type_counts": dict(sorted(relation_type_counts.items())),
        "relation_surface_counts": sorted_nested(
            {key: dict(value) for key, value in relation_surface_counts.items()}
        ),
        "relation_cue_counts": sorted_nested(
            {key: dict(value) for key, value in relation_cue_counts.items()}
        ),
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "role_counts": sorted_nested(
            {key: dict(value) for key, value in role_counts.items()}
        ),
        "argument_type_counts": sorted_nested(
            {
                event_type: {
                    role: dict(type_counts)
                    for role, type_counts in roles.items()
                }
                for event_type, roles in argument_type_counts.items()
            }
        ),
        "transition_counts": sorted_nested(
            {key: dict(value) for key, value in transition_counts.items()}
        ),
    }
    return {
        "model": "local-count-v1",
        "smoothing": config.smoothing,
        "unknown_label": "<UNK>",
        "seed_document_count": len(seed_documents),
        "entity_annotation_count": sum(entity_type_counts.values()),
        "relation_annotation_count": sum(relation_type_counts.values()),
        "event_annotation_count": sum(event_type_counts.values()),
        "counts": counts,
    }


def smoothed_probability(
    count: int,
    total: int,
    label_count: int,
    smoothing: float,
) -> float:
    return (count + smoothing) / (total + smoothing * (label_count + 1))
