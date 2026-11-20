from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from .alignment import token_char_spans
from .config import load_config
from .inputs import write_jsonl
from .models import DocumentIR, Entity, Event, Relation, canonical_json, normalize_document

FIXED_SEED = 20261121


def _make_document(
    doc_id: str,
    tokens: list[str],
    source: str,
    entity_specs: list[tuple[str, int, int]],
    relation_specs: list[tuple[str, int, int]],
    event_specs: list[tuple[str, int, int, dict[str, list[int]]]],
) -> DocumentIR:
    indexed_entities = sorted(
        enumerate(entity_specs),
        key=lambda item: (item[1][1], item[1][2], item[1][0], item[0]),
    )
    entity_ids: dict[int, str] = {}
    entities: list[Entity] = []
    for canonical_index, (original_index, spec) in enumerate(indexed_entities, start=1):
        entity_type, start, end = spec
        entity_id = f"e{canonical_index}"
        entity_ids[original_index] = entity_id
        entities.append(Entity(id=entity_id, type=entity_type, start=start, end=end))
    relations = [
        Relation(
            id=f"r{index}",
            type=relation_type,
            head=entity_ids[head_index],
            tail=entity_ids[tail_index],
        )
        for index, (relation_type, head_index, tail_index) in enumerate(
            sorted(
                relation_specs,
                key=lambda item: (
                    entity_ids[item[1]],
                    entity_ids[item[2]],
                    item[0],
                ),
            ),
            start=1,
        )
    ]
    events = [
        Event(
            id=f"ev{index}",
            type=event_type,
            trigger_start=trigger_start,
            trigger_end=trigger_end,
            arguments={
                role: sorted(entity_ids[index] for index in members)
                for role, members in sorted(arguments.items())
            },
        )
        for index, (event_type, trigger_start, trigger_end, arguments) in enumerate(
            sorted(event_specs, key=lambda item: (item[1], item[2], item[0])),
            start=1,
        )
    ]
    return normalize_document(
        DocumentIR(
            doc_id=doc_id,
            tokens=tokens,
            text=" ".join(tokens),
            source=source,
            entities=entities,
            relations=relations,
            events=events,
        )
    )


def _ordinary_documents(rng: random.Random) -> list[dict[str, Any]]:
    specs: list[tuple[list[str], list[tuple[str, int, int]], list[tuple[str, int, int]], list[tuple[str, int, int, dict[str, list[int]]]]]] = [
        (
            ["OpenAI", "researchers", "Alice", "Chen", "met", "with", "Acme", "Labs", "in", "Paris", "for", "a", "conference", "."],
            [("ORG", 0, 1), ("PERSON", 2, 4), ("ORG", 6, 8), ("LOC", 9, 10), ("EVENT", 12, 13)],
            [("WORKS_AT", 1, 0), ("LOCATED_IN", 2, 3), ("PARTICIPATED_IN", 1, 4), ("SPONSORS", 2, 4)],
            [
                ("MEETING", 4, 5, {"participant": [1, 2], "location": [3]}),
                ("CONFERENCE", 12, 13, {"speaker": [1], "organizer": [2], "venue": [3]}),
            ],
        ),
        (
            ["New", "York", "University", "and", "New", "York", "City", "signed", "an", "agreement", "in", "上海", "."],
            [("LOC", 0, 2), ("ORG", 0, 3), ("LOC", 4, 7), ("WORK", 8, 10), ("LOC", 11, 12)],
            [("LOCATED_IN", 1, 0), ("LOCATED_IN", 2, 0)],
            [
                ("SIGNING", 7, 8, {"signer": [1], "agreement": [3], "location": [4]}),
                ("SIGNING", 7, 10, {"signer": [1], "agreement": [3]}),
                ("SIGNING", 8, 10, {"agreement": [3]}),
            ],
        ),
        (
            ["华为", "与", "Acme", "Labs", "在", "深圳", "举行", "会谈", "并", "签署", "协议", "."],
            [("ORG", 0, 1), ("ORG", 2, 4), ("LOC", 5, 6), ("WORK", 10, 11)],
            [("LOCATED_IN", 0, 2), ("LOCATED_IN", 1, 2)],
            [
                ("MEETING", 6, 8, {"participant": [0, 1], "location": [2]}),
                ("MEETING", 7, 8, {"participant": [0, 1]}),
                ("SIGNING", 9, 10, {"signer": [0, 1], "agreement": [3], "location": [2]}),
            ],
        ),
        (
            ["Google", "launched", "Gemini", "Workspace", "in", "Tokyo", "during", "the", "AI", "Summit", "."],
            [("ORG", 0, 1), ("WORK", 2, 3), ("WORK", 2, 4), ("LOC", 5, 6), ("EVENT", 8, 10)],
            [("CREATED", 0, 2), ("SPONSORS", 0, 4), ("LOCATED_IN", 0, 3)],
            [
                ("LAUNCH", 1, 2, {"founder": [0], "product": [2], "location": [3]}),
                ("CONFERENCE", 9, 10, {"organizer": [0], "venue": [3]}),
                ("CONFERENCE", 8, 10, {"organizer": [0]}),
            ],
        ),
        (
            ["Microsoft", "在", "北京", "发布", "Bing", "Chat", "并", "举办", "AI", "大会", "."],
            [("ORG", 0, 1), ("LOC", 2, 3), ("WORK", 4, 6), ("EVENT", 8, 10)],
            [("LOCATED_IN", 0, 1), ("CREATED", 0, 2), ("SPONSORS", 0, 3)],
            [
                ("LAUNCH", 3, 4, {"founder": [0], "product": [2], "location": [1]}),
                ("CONFERENCE", 9, 10, {"organizer": [0], "venue": [1]}),
            ],
        ),
        (
            ["Dr", "Li", "Wei", "met", "Acme", "Labs", "in", "Singapore", "and", "signed", "a", "contract", "."],
            [("PERSON", 0, 3), ("ORG", 4, 6), ("LOC", 7, 8), ("WORK", 10, 12)],
            [("WORKS_AT", 0, 1), ("LOCATED_IN", 1, 2)],
            [
                ("MEETING", 3, 4, {"participant": [0, 1], "location": [2]}),
                ("SIGNING", 9, 10, {"signer": [0, 1], "agreement": [3], "location": [2]}),
            ],
        ),
        (
            ["The", "Paris", "Office", "of", "Acme", "Labs", "launched", "Orion", "in", "Paris", "."],
            [("LOC", 1, 2), ("ORG", 1, 3), ("ORG", 4, 6), ("WORK", 7, 8), ("LOC", 9, 10)],
            [("LOCATED_IN", 1, 0), ("LOCATED_IN", 2, 4), ("CREATED", 2, 3)],
            [("LAUNCH", 6, 7, {"founder": [2], "product": [3], "location": [4]})],
        ),
        (
            ["华为", "与", "Google", "在", "上海", "举行", "峰会", "并", "发布", "Harmony", "OS", "."],
            [("ORG", 0, 1), ("ORG", 2, 3), ("LOC", 4, 5), ("WORK", 9, 11)],
            [("LOCATED_IN", 0, 2), ("LOCATED_IN", 1, 2), ("CREATED", 0, 3)],
            [
                ("MEETING", 5, 7, {"participant": [0, 1], "location": [2]}),
                ("CONFERENCE", 6, 7, {"organizer": [0], "venue": [2]}),
                ("LAUNCH", 8, 9, {"founder": [0], "product": [3], "location": [2]}),
            ],
        ),
    ]
    sources = ["multilingual_demo", "local_seed", "interop_demo", "mixed_corpus"]
    documents: list[dict[str, Any]] = []
    for index, (tokens, entities, relations, events) in enumerate(specs, start=1):
        document = _make_document(
            f"d{index:03d}",
            tokens,
            rng.choice(sources),
            entities,
            relations,
            events,
        )
        documents.append(document.model_dump(mode="json"))
    rng.shuffle(documents)
    return documents


def _legacy_payload(document: DocumentIR, *, inject_bad_offset: bool = False) -> dict[str, Any]:
    token_spans = token_char_spans(document.tokens, document.text, doc_id=document.doc_id)
    entity_names: dict[str, str] = {}
    entities: list[dict[str, Any]] = []
    for entity in document.entities:
        name = " ".join(document.tokens[entity.start : entity.end])
        entity_names[entity.id] = name
        char_start, char_end = token_spans[entity.start][0], token_spans[entity.end - 1][1]
        entities.append(
            {
                "name": name,
                "type": entity.type,
                "char_start": char_start,
                "char_end": char_end,
            }
        )
    if inject_bad_offset and entities:
        bad_char_start = entities[0]["char_start"] + 1
        entities.append(
            {
                "name": "Ghost Entity",
                "type": "ORG",
                "char_start": bad_char_start,
                "char_end": entities[0]["char_end"],
            }
        )
    relations = [
        {
            "type": relation.type,
            "head": entity_names[relation.head],
            "tail": entity_names[relation.tail],
        }
        for relation in document.relations
    ]
    events: list[dict[str, Any]] = []
    for event in document.events:
        char_start, char_end = token_spans[event.trigger_start][0], token_spans[event.trigger_end - 1][1]
        arguments: dict[str, str] = {}
        for role, members in sorted(event.arguments.items()):
            if members:
                arguments[role] = entity_names[members[0]]
        events.append(
            {
                "type": event.type,
                "trigger": " ".join(document.tokens[event.trigger_start : event.trigger_end]),
                "trigger_char_start": char_start,
                "trigger_char_end": char_end,
                "arguments": arguments,
            }
        )
    return {
        "schema_version": 1,
        "doc_id": document.doc_id,
        "entities": entities,
        "relations": relations,
        "events": events,
    }


def generate_samples(root: str | Path) -> dict[str, Any]:
    root = Path(root).resolve()
    config = load_config(root / "config" / "extraction.yaml")
    rng = random.Random(FIXED_SEED)
    documents = _ordinary_documents(rng)
    for item in documents:
        DocumentIR.model_validate(item)

    document_dir = root / "data" / "documents"
    seed_dir = root / "data" / "seeds"
    legacy_dir = root / "data" / "legacy"
    for directory in (document_dir, seed_dir, legacy_dir):
        directory.mkdir(parents=True, exist_ok=True)
        for stale in directory.glob("*.jsonl"):
            stale.unlink()

    shard_paths: list[str] = []
    seed_paths: list[str] = []
    for shard_index in range(4):
        shard_documents = documents[shard_index * 2 : shard_index * 2 + 2]
        document_records = []
        seed_records = []
        for document_payload in shard_documents:
            document = DocumentIR.model_validate(document_payload)
            document_records.append(
                {
                    "doc_id": document.doc_id,
                    "tokens": document.tokens,
                    "text": document.text,
                    "source": document.source,
                    "annotation_version": 2,
                }
            )
            seed_records.append(
                {
                    "doc_id": document.doc_id,
                    "schema_version": 2,
                    "entities": [entity.model_dump(mode="json") for entity in document.entities],
                    "relations": [
                        relation.model_dump(mode="json") for relation in document.relations
                    ],
                    "events": [event.model_dump(mode="json") for event in document.events],
                }
            )
        document_path = document_dir / f"shard_{shard_index + 1:02d}.jsonl"
        seed_path = seed_dir / f"seed_{shard_index + 1:02d}.jsonl"
        write_jsonl(document_path, document_records)
        write_jsonl(seed_path, seed_records)
        shard_paths.append(document_path.relative_to(root).as_posix())
        seed_paths.append(seed_path.relative_to(root).as_posix())

    legacy_one = _make_document(
        "L001",
        ["Ada", "Lovelace", "met", "Acme", "Labs", "in", "London", "."],
        "legacy_archive",
        [("PERSON", 0, 3), ("ORG", 3, 5), ("LOC", 6, 7)],
        [("WORKS_AT", 0, 1), ("LOCATED_IN", 1, 2)],
        [("MEETING", 2, 3, {"participant": [0], "location": [2]})],
    )
    legacy_two = _make_document(
        "L002",
        ["Paris", "Team", "met", "Paris", "Office", "in", "Paris", "."],
        "legacy_ambiguous",
        [("ORG", 0, 2), ("LOC", 3, 4), ("ORG", 4, 6), ("LOC", 6, 7)],
        [("LOCATED_IN", 0, 1), ("LOCATED_IN", 2, 3)],
        [("MEETING", 2, 3, {"participant": [0], "location": [1]})],
    )
    legacy_records = []
    for document, inject_bad in ((legacy_one, False), (legacy_two, True)):
        legacy_records.append(
            {
                "doc_id": document.doc_id,
                "tokens": document.tokens,
                "text": document.text,
                "source": document.source,
                "annotation_version": 1,
                "annotation": _legacy_payload(document, inject_bad_offset=inject_bad),
            }
        )
    legacy_path = legacy_dir / "legacy_v1.jsonl"
    write_jsonl(legacy_path, legacy_records)

    manifest = {
        "schema_version": 1,
        "documents": shard_paths,
        "seeds": seed_paths,
        "legacy": [legacy_path.relative_to(root).as_posix()],
    }
    (root / "data").mkdir(parents=True, exist_ok=True)
    (root / "data" / "manifest.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8")

    summary = {
        "seed": FIXED_SEED,
        "generator": "ree.sample_data",
        "document_shards": len(shard_paths),
        "seed_shards": len(seed_paths),
        "legacy_shards": 1,
        "document_count": len(documents) + len(legacy_records),
        "ordinary_document_count": len(documents),
        "legacy_document_count": len(legacy_records),
        "overlapping_entity_documents": ["d002", "d004", "d007"],
        "nested_event_documents": ["d002", "d003", "d004", "d008"],
        "align_fail_injected_doc_id": "L002",
        "entity_types": config.entity_types,
        "relation_types": sorted(config.relation_types),
        "event_types": sorted(config.event_types),
        "manifest": manifest,
    }
    (root / "artifacts").mkdir(exist_ok=True)
    (root / "artifacts" / "sample_manifest.json").write_text(
        canonical_json(summary) + "\n", encoding="utf-8"
    )
    return summary
