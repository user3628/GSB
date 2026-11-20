from __future__ import annotations

import copy
import json

import pytest

from transcript_workbench.errors import WorkbenchError
from transcript_workbench.formats import encode_json
from transcript_workbench.models import Word
from transcript_workbench.workspace import Workspace


def _single_chunk_payload(workspace: Workspace, words):
    state = workspace.load_state()
    chunk = state.chunks["chunk-01"]
    return encode_json(
        {chunk.chunk_id: chunk},
        [Word.from_dict(word) if isinstance(word, dict) else word for word in words],
    )


def _import(workspace: Workspace, payload: str, expected: int):
    return workspace.import_text(
        content=payload,
        fmt="json",
        expected_version=expected,
        chunk_id="chunk-01",
    )


@pytest.mark.parametrize(
    ("mutation", "violation"),
    [
        (lambda w: w[0].__setitem__("start_ms", -5), "negative_start"),
        (
            lambda w: w[1].__setitem__("end_ms", w[1]["start_ms"] - 1),
            "end_before_start",
        ),
        (
            lambda w: w[1].__setitem__("end_ms", w[1]["start_ms"]),
            "zero_length",
        ),
        (
            lambda w: w[0].__setitem__("end_ms", 99_999),
            "end_beyond_chunk",
        ),
        (
            lambda w: w[1].__setitem__("start_ms", w[0]["end_ms"] - 3),
            "overlap",
        ),
        (
            lambda w: w[1].__setitem__(
                "start_ms", w[0]["end_ms"] + 1500
            ),
            "gap_exceeds_tolerance",
        ),
    ],
)
def test_illegal_timeline_returns_business_error_without_partial_version(
    boot_workspace, mutation, violation
):
    state = boot_workspace.load_state()
    original_words = [
        word.to_dict() for word in state.words if word.chunk_id == "chunk-01"
    ][:2]
    words = copy.deepcopy(original_words)
    mutation(words)
    if violation == "gap_exceeds_tolerance":
        words[1]["end_ms"] = words[1]["start_ms"] + 30
    before = state.version
    with pytest.raises(WorkbenchError) as raised:
        _import(boot_workspace, _single_chunk_payload(boot_workspace, words), before)
    error = raised.value
    assert error.code == "E_TIMELINE_ERROR"
    assert error.details["chunk_id"] == "chunk-01"
    assert error.details["entry_index"] is not None
    assert error.details["violation"] == violation
    assert boot_workspace.load_state().version == before
    assert not (boot_workspace.versions_dir / f"v{before + 1:08d}.json").exists()


def test_out_of_order_import_is_rejected(boot_workspace):
    state = boot_workspace.load_state()
    words = [
        word.to_dict() for word in state.words if word.chunk_id == "chunk-01"
    ][:2]
    words.reverse()
    payload = json.loads(_single_chunk_payload(boot_workspace, words))
    payload["words"].reverse()
    before = state.version
    with pytest.raises(WorkbenchError) as raised:
        _import(boot_workspace, json.dumps(payload), before)
    assert raised.value.code == "E_TIMELINE_ERROR"
    assert raised.value.details["violation"] == "out_of_order"
    assert boot_workspace.load_state().version == before


def test_legacy_json_migration_records_rules_and_provenance(
    boot_workspace, demo_data
):
    legacy = demo_data["legacy_path"].read_text(encoding="utf-8")
    result = boot_workspace.import_text(
        content=legacy,
        fmt="json",
        expected_version=1,
        chunk_id="chunk-01",
    )
    assert result["version"] == 2
    assert result["kind"] == "migration"
    migration = result["migrations"][-1]
    assert migration["from_schema_version"] == 0
    assert migration["to_schema_version"] == 2
    assert "seconds_to_milliseconds" in migration["rules"]
    assert "missing_confidence_default_0.5" in migration["rules"]
    state = boot_workspace.load_state()
    assert state.words[0].source_version == 2
    assert state.words[0].source_entry_ids
    assert state.words[0].language in {"zh-CN", "en-US"}
    assert state.chunks["chunk-01"].duration_ms == 2200


@pytest.mark.parametrize(
    "raw",
    [
        "{not-json",
        json.dumps(
            {
                "schema_version": 99,
                "time_unit": "ms",
                "chunks": [{"chunk_id": "chunk-01", "duration_ms": 2200}],
                "words": [],
            }
        ),
        json.dumps(
            {
                "schema_version": 0,
                "time_unit": "video-frames",
                "audio": "chunk-01",
                "segments": [{"start": 0, "end": 1, "word": "x"}],
            }
        ),
    ],
)
def test_unmigratable_json_returns_e_incompat_format(boot_workspace, raw):
    before = boot_workspace.load_state().version
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.import_text(
            content=raw,
            fmt="json",
            expected_version=before,
            chunk_id="chunk-01",
        )
    assert raised.value.code == "E_INCOMPAT_FORMAT"
    assert boot_workspace.load_state().version == before


def test_import_duration_mismatch_is_incompatible(boot_workspace):
    state = boot_workspace.load_state()
    payload = json.loads(
        encode_json(
            {state.chunks["chunk-01"].chunk_id: state.chunks["chunk-01"]},
            [word for word in state.words if word.chunk_id == "chunk-01"],
        )
    )
    payload["chunks"][0]["duration_ms"] = 9999
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.import_text(
            content=json.dumps(payload),
            fmt="json",
            expected_version=1,
            chunk_id="chunk-01",
        )
    assert raised.value.code == "E_INCOMPAT_FORMAT"


def test_seconds_unit_is_migrated_even_when_schema_number_is_current(boot_workspace):
    state = boot_workspace.load_state()
    chunk = state.chunks["chunk-01"]
    word = next(word for word in state.words if word.chunk_id == chunk.chunk_id)
    payload = {
        "schema_version": 2,
        "time_unit": "seconds",
        "chunks": [
            {
                "chunk_id": chunk.chunk_id,
                "order": chunk.order,
                "duration_ms": chunk.duration_ms,
                "language": chunk.language,
            }
        ],
        "words": [
            {
                "entry_id": word.entry_id,
                "chunk_id": word.chunk_id,
                "start": word.start_ms / 1000,
                "end": word.end_ms / 1000,
                "text": word.text,
                "confidence": word.confidence,
                "language": word.language,
            }
        ],
    }
    result = boot_workspace.import_text(
        content=json.dumps(payload),
        fmt="json",
        expected_version=1,
        chunk_id=chunk.chunk_id,
    )
    assert result["kind"] == "migration"
    assert result["migrations"][-1]["rules"] == ["seconds_to_milliseconds"]
    imported = [item for item in boot_workspace.load_state().words if item.chunk_id == chunk.chunk_id]
    assert len(imported) == 1
    assert imported[0].start_ms == int(round(word.start_ms / 1000 * 1000))
