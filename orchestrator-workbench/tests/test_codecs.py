from __future__ import annotations

import zlib

import pytest

from app.codecs import binary, jsonl, yaml_like
from app.errors import WorkbenchError
from app.models import DecodedPayload, FormatName, RawRecord, SnapshotHeader


def _payload(fmt: FormatName, version: int = 1) -> DecodedPayload:
    return DecodedPayload(
        header=SnapshotHeader(
            format=fmt, format_version=version, snapshot_seq=1, generated_at="2026-11-22T00:00:00Z"
        ),
        records=[
            RawRecord(resource_id="a", parents=[], budget=1, weight_permille=500,
                      weight_float=0.5, expected_version=1),
            RawRecord(resource_id="b", parents=["a"], budget=2, weight_permille=250,
                      weight_float=0.25, expected_version=1),
        ],
    )


def test_binary_roundtrip_preserves_all_fields():
    blob = binary.encode(_payload(FormatName.BINARY))
    assert blob[:4] == binary.MAGIC
    assert blob[4] == 1
    decoded = binary.decode(blob)
    assert decoded.header.snapshot_seq == 1
    assert [r.resource_id for r in decoded.records] == ["a", "b"]
    assert decoded.records[1].parents == ["a"]
    assert decoded.records[0].weight_permille == 500
    assert decoded.records[0].budget == 1


def test_binary_record_crc_detection():
    blob = bytearray(binary.encode(_payload(FormatName.BINARY)))
    blob[-1] ^= 0xFF
    with pytest.raises(WorkbenchError) as exc:
        binary.decode(bytes(blob))
    assert exc.value.code == "E_FORMAT_INVALID"


def test_binary_bad_magic_and_truncation():
    with pytest.raises(WorkbenchError) as exc:
        binary.decode(b"XXXX" + b"\x00" * 40)
    assert exc.value.code == "E_FORMAT_INVALID"
    with pytest.raises(WorkbenchError):
        binary.decode(b"ROW")


def test_jsonl_roundtrip_and_missing_fields():
    payload = _payload(FormatName.JSONL)
    blob = jsonl.encode(payload)
    decoded = jsonl.decode(blob)
    assert decoded.records[0].weight_float == pytest.approx(0.5)
    malformed = blob.decode().splitlines()[0] + "\n{not-json}\n"
    with pytest.raises(WorkbenchError) as exc:
        jsonl.decode(malformed.encode())
    assert exc.value.code == "E_FORMAT_INVALID"


def test_yaml_subset_has_no_weight():
    blob = yaml_like.encode(_payload(FormatName.YAML))
    decoded = yaml_like.decode(blob)
    assert all(r.weight_float is None and r.weight_permille is None for r in decoded.records)
    assert decoded.records[1].parents == ["a"]
    with pytest.raises(WorkbenchError) as exc:
        yaml_like.decode(b"a -> [b]\n")
    assert exc.value.code == "E_FORMAT_INVALID"
