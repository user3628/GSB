from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable

from .config import REEConfig
from .estimator import smoothed_probability, surface_key
from .models import DocumentIR, Entity, Event, Relation, normalize_document


def _normalize_trigger(value: str) -> str:
    return "".join(value.split()).casefold()


def _distance(left_start: int, left_end: int, right_start: int, right_end: int) -> int:
    if left_end <= right_start:
        return right_start - left_end
    if right_end <= left_start:
        return left_start - right_end
    return 0


def _relation_gap_tokens(
    tokens: list[str], head: Entity, tail: Entity
) -> list[str]:
    if head.end <= tail.start:
        return tokens[head.end : tail.start]
    if tail.end <= head.start:
        return tokens[tail.end : head.start]
    return []


def _extract_entities(
    document: DocumentIR,
    counts: dict[str, Any],
    config: REEConfig,
) -> tuple[list[Entity], list[dict[str, Any]]]:
    surfaces = counts["entity_surface_counts"]
    type_count = len(config.entity_types)
    candidates: list[tuple[int, int, str, float]] = []
    for start in range(len(document.tokens)):
        max_end = min(
            len(document.tokens), start + config.extraction.max_entity_tokens
        )
        for end in range(start + 1, max_end + 1):
            key = surface_key(document.tokens, start, end)
            observed = surfaces.get(key)
            if not observed:
                continue
            total = sum(observed.values())
            for entity_type, count in observed.items():
                if entity_type not in config.entity_types:
                    continue
                probability = smoothed_probability(
                    count, total, type_count, config.smoothing
                )
                if probability < config.confidence_threshold:
                    continue
                score = math.log(probability)
                if config.features.use_transition_scores:
                    start_label = (
                        f"S-{entity_type}" if end - start == 1 else f"B-{entity_type}"
                    )
                    transitions = counts["transition_counts"].get("<START>", {})
                    transition_total = sum(transitions.values())
                    transition_probability = smoothed_probability(
                        transitions.get(start_label, 0),
                        transition_total,
                        max(1, len(transitions)),
                        config.smoothing,
                    )
                    score += 0.05 * math.log(transition_probability)
                candidates.append((start, end, entity_type, score))
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    entities = [
        Entity(id=f"e{index}", type=entity_type, start=start, end=end)
        for index, (start, end, entity_type, _score) in enumerate(candidates, start=1)
    ]
    audit = [
        {
            "id": f"e{index}",
            "start": start,
            "end": end,
            "type": entity_type,
            "log_score": round(score, 9),
        }
        for index, (start, end, entity_type, score) in enumerate(candidates, start=1)
    ]
    return entities, audit


def _extract_relations(
    document: DocumentIR,
    entities: list[Entity],
    counts: dict[str, Any],
    config: REEConfig,
) -> tuple[list[Relation], list[dict[str, Any]]]:
    relation_totals = sum(counts["relation_type_counts"].values())
    relation_types = sorted(config.relation_types)
    best_by_pair: dict[tuple[str, str], tuple[float, str, dict[str, Any]]] = {}
    for head in entities:
        for tail in entities:
            if head.id == tail.id:
                continue
            distance = _distance(head.start, head.end, tail.start, tail.end)
            if distance > config.extraction.max_relation_distance:
                continue
            gap_tokens = _relation_gap_tokens(document.tokens, head, tail)
            for relation_type in relation_types:
                spec = config.relation_types[relation_type]
                if head.type not in spec.head or tail.type not in spec.tail:
                    continue
                count = counts["relation_type_counts"].get(relation_type, 0)
                probability = smoothed_probability(
                    count, relation_totals, len(relation_types), config.smoothing
                )
                head_surface = surface_key(document.tokens, head.start, head.end)
                tail_surface = surface_key(document.tokens, tail.start, tail.end)
                pair_counts = counts["relation_surface_counts"].get(
                    head_surface + "\u0001" + tail_surface, {}
                )
                pair_observed = pair_counts.get(relation_type, 0)
                cue_counts = counts["relation_cue_counts"].get(relation_type, {})
                cue_strength = sum(cue_counts.get(token.casefold(), 0) for token in gap_tokens)
                if config.features.use_relation_cues:
                    evidence = pair_observed > 0 or cue_strength > 0
                else:
                    evidence = pair_observed > 0
                if not evidence:
                    continue
                confidence = probability
                if pair_observed:
                    confidence = min(1.0, confidence + 0.55)
                if config.features.use_relation_cues and cue_strength:
                    confidence = min(1.0, confidence + 0.08 * min(cue_strength, 3))
                confidence = min(1.0, confidence + 0.02 * max(0, 4 - distance))
                if confidence < config.confidence_threshold:
                    continue
                score = math.log(max(confidence, 1e-12))
                candidate = {
                    "id": "",
                    "type": relation_type,
                    "head": head.id,
                    "tail": tail.id,
                    "confidence": round(confidence, 9),
                    "log_score": round(score, 9),
                    "distance": distance,
                    "pair_observed": pair_observed,
                    "cue_strength": cue_strength,
                }
                key = (head.id, tail.id)
                previous = best_by_pair.get(key)
                if previous is None or (
                    confidence,
                    relation_type,
                ) > (previous[0], previous[1]):
                    best_by_pair[key] = (confidence, relation_type, candidate)
    selected = [item[2] for item in best_by_pair.values()]
    selected.sort(key=lambda item: (item["head"], item["tail"], item["type"]))
    relations: list[Relation] = []
    audit: list[dict[str, Any]] = []
    for index, item in enumerate(selected, start=1):
        relation = Relation(
            id=f"r{index}",
            type=item["type"],
            head=item["head"],
            tail=item["tail"],
        )
        relations.append(relation)
        audit.append({**item, "id": relation.id})
    return relations, audit


def _extract_events(
    document: DocumentIR,
    entities: list[Entity],
    counts: dict[str, Any],
    config: REEConfig,
) -> tuple[list[Event], list[dict[str, Any]]]:
    trigger_lookup: dict[str, set[str]] = defaultdict(set)
    for event_type, trigger_words in config.triggers.items():
        for trigger in trigger_words:
            trigger_lookup[_normalize_trigger(trigger)].add(event_type)

    trigger_matches: set[tuple[int, int, str]] = set()
    for start in range(len(document.tokens)):
        max_end = min(len(document.tokens), start + config.extraction.max_trigger_tokens)
        for end in range(start + 1, max_end + 1):
            key = "".join(document.tokens[start:end]).casefold()
            for event_type in trigger_lookup.get(key, set()):
                trigger_matches.add((start, end, event_type))

    event_records: list[tuple[int, int, str, dict[str, list[str]], list[dict[str, Any]]]] = []
    for trigger_start, trigger_end, event_type in sorted(
        trigger_matches, key=lambda item: (item[0], item[1], item[2])
    ):
        event_spec = config.event_types[event_type]
        arguments: dict[str, list[str]] = {}
        audits: list[dict[str, Any]] = []
        for role in sorted(event_spec.roles):
            allowed_types = set(event_spec.roles[role])
            type_counts = counts["argument_type_counts"].get(event_type, {}).get(role, {})
            total = sum(type_counts.values())
            role_candidates: list[tuple[float, int, Entity]] = []
            for entity in entities:
                if entity.type not in allowed_types:
                    continue
                distance = _distance(trigger_start, trigger_end, entity.start, entity.end)
                if (
                    config.features.use_argument_proximity
                    and distance > config.extraction.max_argument_distance
                ):
                    continue
                probability = smoothed_probability(
                    type_counts.get(entity.type, 0),
                    total,
                    len(config.entity_types),
                    config.smoothing,
                )
                confidence = probability
                if config.features.use_argument_proximity:
                    confidence *= max(0.5, 1.0 - 0.02 * distance)
                if confidence < config.confidence_threshold:
                    continue
                log_score = math.log(max(confidence, 1e-12))
                role_candidates.append((confidence, distance, entity))
            role_candidates.sort(
                key=lambda item: (
                    -item[0],
                    item[1],
                    item[2].start,
                    item[2].end,
                    item[2].id,
                )
            )
            selected_entities = role_candidates[
                : config.extraction.max_arguments_per_role
            ]
            if selected_entities:
                arguments[role] = [item[2].id for item in selected_entities]
                for confidence, distance, entity in selected_entities:
                    audits.append(
                        {
                            "role": role,
                            "entity": entity.id,
                            "type": entity.type,
                            "confidence": round(confidence, 9),
                            "log_score": round(math.log(max(confidence, 1e-12)), 9),
                            "distance": distance,
                        }
                    )
        event_records.append(
            (trigger_start, trigger_end, event_type, arguments, audits)
        )

    events: list[Event] = []
    audit: list[dict[str, Any]] = []
    for index, (start, end, event_type, arguments, arg_audit) in enumerate(
        event_records, start=1
    ):
        event = Event(
            id=f"ev{index}",
            type=event_type,
            trigger_start=start,
            trigger_end=end,
            arguments={role: sorted(members) for role, members in sorted(arguments.items())},
        )
        events.append(event)
        audit.append(
            {
                "id": event.id,
                "type": event_type,
                "trigger_start": start,
                "trigger_end": end,
                "arguments": arg_audit,
            }
        )
    return events, audit


def extract_documents(
    documents: list[DocumentIR],
    estimate_artifact: dict[str, Any],
    config: REEConfig,
    *,
    document_hook: Callable[[str], bool] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    counts = estimate_artifact["counts"]
    extracted: list[DocumentIR] = []
    audit_by_doc: dict[str, Any] = {}
    for raw_document in sorted(documents, key=lambda document: document.doc_id):
        document = normalize_document(raw_document)
        entities, entity_audit = _extract_entities(document, counts, config)
        relations, relation_audit = _extract_relations(
            document, entities, counts, config
        )
        events, event_audit = _extract_events(document, entities, counts, config)
        result = DocumentIR(
            doc_id=document.doc_id,
            tokens=document.tokens,
            text=document.text,
            source=document.source,
            entities=entities,
            relations=relations,
            events=events,
            provenance={
                "extractor": "local-count-v1",
                "source_annotation_version": document.provenance.get(
                    "source_annotation_version", 2
                ),
                "source_upgrades": document.provenance.get("upgrades", []),
                "target_schema_version": 2,
            },
        )
        extracted.append(normalize_document(result))
        audit_by_doc[document.doc_id] = {
            "entities": entity_audit,
            "relations": relation_audit,
            "events": event_audit,
        }
        if document_hook is not None and document_hook(document.doc_id):
            return None, {
                "status": "stopped",
                "stage": "extract",
                "doc_id": document.doc_id,
            }
    summary = {
        "document_count": len(extracted),
        "entity_count": sum(len(document.entities) for document in extracted),
        "relation_count": sum(len(document.relations) for document in extracted),
        "event_count": sum(len(document.events) for document in extracted),
        "event_argument_count": sum(
            len(members)
            for document in extracted
            for event in document.events
            for members in event.arguments.values()
        ),
    }
    artifact = {
        "schema_version": 2,
        "model": "local-count-v1",
        "documents": [document.model_dump(mode="json") for document in extracted],
        "summary": summary,
        "audit": audit_by_doc,
    }
    return artifact, None
