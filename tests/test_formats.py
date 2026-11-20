from __future__ import annotations

import json
from pathlib import Path

import pytest

from ree.config import load_config
from ree.errors import REEError
from ree.formats import (
    decode_bio,
    decode_json_intervals,
    decode_legacy_flat_v1,
    encode_bio,
    encode_json_intervals,
    encode_legacy_flat_v1,
)
from ree.inputs import read_jsonl
from ree.models import core_digest
from ree.pipeline import Pipeline


def test_all_three_formats_preserve_core_fields(workspace: Path) -> None:
    pipeline = Pipeline(workspace)
    pipeline.run()
    config = pipeline.load_config()
    documents = pipeline.get_documents()
    assert len(documents) == 10

    for document in documents:
        bio_payload = encode_bio(document)
        assert all(
            tag == "O" or tag[0] in "BIES" and tag[1] == "-"
            for layer in bio_payload["bio_layers"]
            for tag in layer["tags"]
        )
        bio_decoded = decode_bio(bio_payload)
        assert core_digest(bio_decoded.document) == core_digest(document)

        json_payload = encode_json_intervals(document)
        json_decoded = decode_json_intervals(json_payload, config)
        assert core_digest(json_decoded.document) == core_digest(document)

        legacy_payload = encode_legacy_flat_v1(document)
        legacy_decoded = decode_legacy_flat_v1(legacy_payload, config)
        assert core_digest(legacy_decoded.document) == core_digest(document)
        assert legacy_decoded.upgrades == [
            "char_offsets_to_token_indices",
            "entity_names_to_ids",
            "single_argument_to_list",
        ]


def test_bio_multilayer_supports_overlapping_entities(workspace: Path) -> None:
    pipeline = Pipeline(workspace)
    pipeline.run()
    document = pipeline.get_document("d002")
    payload = encode_bio(document)
    assert len(payload["bio_layers"]) >= 2
    assert any(
        tag.startswith(("B-", "I-", "E-", "S-"))
        for layer in payload["bio_layers"]
        for tag in layer["tags"]
    )
    decoded = decode_bio(payload)
    assert core_digest(decoded.document) == core_digest(document)


def test_legacy_upgrade_and_align_failure(workspace: Path) -> None:
    pipeline = Pipeline(workspace)
    pipeline.run()
    config = load_config(workspace / "config" / "extraction.yaml")
    records = read_jsonl(workspace / "data" / "legacy" / "legacy_v1.jsonl")
    assert {record["doc_id"] for record in records} == {"L001", "L002"}

    decoded = {
        record["doc_id"]: decode_legacy_flat_v1(
            {
                **record["annotation"],
                "doc_id": record["doc_id"],
                "tokens": record["tokens"],
                "text": record["text"],
                "source": record["source"],
            },
            config,
        )
        for record in records
    }
    assert decoded["L001"].source_version == 1
    assert all(
        isinstance(members, list) and members
        for event in decoded["L001"].document.events
        for members in event.arguments.values()
    )
    align_failure = next(
        item for item in decoded["L002"].diagnostics if item["code"] == "E_ALIGN_FAIL"
    )
    assert align_failure["doc_id"] == "L002"
    assert "offset" in align_failure
    assert any(
        item["code"] == "E_AMBIGUOUS_NAME" for item in decoded["L002"].diagnostics
    )

    parse_artifact = pipeline.get_stage_artifact("parse")
    align_failures = [
        item
        for item in parse_artifact["diagnostics"]
        if item["code"] == "E_ALIGN_FAIL"
    ]
    assert len(align_failures) == 1
    assert align_failures[0]["doc_id"] == "L002"
    assert align_failures[0]["stage"] == "parse"


def test_unsupported_version_is_stable_error(workspace: Path) -> None:
    pipeline = Pipeline(workspace)
    pipeline.run()
    payload = encode_json_intervals(pipeline.get_document("d001"))
    payload["schema_version"] = 99
    with pytest.raises(REEError) as captured:
        decode_json_intervals(payload, pipeline.load_config())
    assert captured.value.code == "E_UNSUPPORTED_VERSION"
    assert captured.value.doc_id == "d001"
    assert captured.value.as_dict()["details"]["supported_version"] == 2


def test_legacy_v1_routed_through_json_decoder(workspace: Path) -> None:
    config = load_config(workspace / "config" / "extraction.yaml")
    record = json.loads(
        (workspace / "data" / "legacy" / "legacy_v1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    decoded = decode_json_intervals(
        {
            **record["annotation"],
            "doc_id": record["doc_id"],
            "tokens": record["tokens"],
            "text": record["text"],
            "source": record["source"],
        },
        config,
    )
    assert decoded.source_version == 1
    assert decoded.document.provenance["source_annotation_version"] == 1
    assert "entity_names_to_ids" in decoded.upgrades


def test_legacy_alignment_mode_is_configurable(workspace: Path) -> None:
    config = load_config(workspace / "config" / "extraction.yaml")
    record = json.loads(
        (workspace / "data" / "legacy" / "legacy_v1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[1]
    )
    payload = {
        **record["annotation"],
        "doc_id": record["doc_id"],
        "tokens": record["tokens"],
        "text": record["text"],
        "source": record["source"],
    }
    config.legacy.alignment.mode = "expand"
    decoded = decode_legacy_flat_v1(payload, config)
    assert not any(item["code"] == "E_ALIGN_FAIL" for item in decoded.diagnostics)
    assert any(item["code"] == "W_ALIGN_EXPANDED" for item in decoded.diagnostics)
