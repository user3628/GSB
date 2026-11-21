from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from slowquery.errors import BusinessError
from slowquery.service import SlowQueryService
from slowquery.util import sha256_canonical
from slowquery.validation import normalize_sample_batch, normalize_snapshot


def test_sample_normalization_accepts_all_fixture_samples(config: Path) -> None:
    raw = json.loads((Path(__file__).resolve().parent.parent / "data" / "samples.json").read_text())
    samples = normalize_sample_batch(raw)
    assert len(samples) == 7
    assert samples[0]["duration_ms"] == 860
    assert samples[0]["occurred_at"] == "2026-01-15T08:00:00.000Z"
    assert samples[0]["payload_sha256"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("duration_ms", -1),
        ("duration_ms", 1.5),
        ("duration_ms", True),
        ("duration_ms", 2**63),
        ("scanned_rows", -9),
        ("scanned_rows", 2**63),
        ("lock_wait_ms", -1),
    ],
)
def test_invalid_numeric_sample_is_rejected(field: str, value, tmp_path: Path) -> None:
    raw = {
        "sample_id": "bad-numeric",
        "sql": "SELECT 1",
        "duration_ms": 1,
        "scanned_rows": 1,
        "lock_wait_ms": 0,
        "occurred_at": "2026-01-01T00:00:00Z",
    }
    raw[field] = value
    with pytest.raises(BusinessError) as caught:
        normalize_sample_batch([raw])
    assert caught.value.code == "E_SAMPLE_INVALID"


def test_invalid_sql_and_timestamp_are_rejected() -> None:
    base = {
        "sample_id": "bad-sql",
        "sql": "DELETE FROM orders",
        "duration_ms": 1,
        "scanned_rows": 1,
        "lock_wait_ms": 0,
        "occurred_at": "2026-01-01T00:00:00Z",
    }
    with pytest.raises(BusinessError) as caught:
        normalize_sample_batch([base])
    assert caught.value.code == "E_SAMPLE_INVALID"
    base["sql"] = "SELECT 1"
    base["occurred_at"] = "yesterday"
    with pytest.raises(BusinessError) as caught:
        normalize_sample_batch([base])
    assert caught.value.code == "E_SAMPLE_INVALID"


def test_sample_batch_is_all_or_nothing(service: SlowQueryService) -> None:
    valid = {
        "sample_id": "valid-one",
        "sql": "SELECT 1",
        "duration_ms": 1,
        "scanned_rows": 1,
        "lock_wait_ms": 0,
        "occurred_at": "2026-01-01T00:00:00Z",
    }
    invalid = dict(valid, sample_id="invalid-one", duration_ms=-2)
    with pytest.raises(BusinessError) as caught:
        service.submit_samples([valid, invalid])
    assert caught.value.code == "E_SAMPLE_INVALID"
    assert service.repo.list_samples() == []


def test_snapshot_digest_validation() -> None:
    data_dir = Path(__file__).resolve().parent.parent / "data"
    raw = json.loads((data_dir / "snapshot_v1.json").read_text())
    tampered = copy.deepcopy(raw)
    tampered["tables"]["orders"]["row_count"] = 999
    with pytest.raises(BusinessError) as caught:
        normalize_snapshot(tampered)
    assert caught.value.code == "E_SNAPSHOT_INVALID"


def test_snapshot_summary_validation_with_recomputed_digest() -> None:
    data_dir = Path(__file__).resolve().parent.parent / "data"
    raw = json.loads((data_dir / "snapshot_v1.json").read_text())
    raw["summary"]["total_rows"] = 1
    raw.pop("digest")
    raw["digest"] = "sha256:" + sha256_canonical(raw)
    with pytest.raises(BusinessError) as caught:
        normalize_snapshot(raw)
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    assert "summary.total_rows" in caught.value.message


def test_snapshot_version_must_be_monotonic(seeded_service: SlowQueryService) -> None:
    data_dir = Path(__file__).resolve().parent.parent / "data"
    raw = json.loads((data_dir / "snapshot_v1.json").read_text())
    with pytest.raises(BusinessError) as caught:
        seeded_service.load_snapshot(raw)
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    assert seeded_service.repo.active_snapshot_version() == 1
