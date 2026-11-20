from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from replication_guard.errors import AppError
from replication_guard.formats import encode_jsonl
from replication_guard.models import normalize_policy
from replication_guard.service import ReplicationService


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def _import_samples(service: ReplicationService, batch_id: str = "base") -> dict:
    return service.import_policies(
        (SAMPLES / "policies.jsonl").read_bytes(),
        format_hint="jsonl",
        batch_id=batch_id,
    )


def _import_objects(service: ReplicationService, objects: list[dict], batch_id: str) -> dict:
    text = encode_jsonl([normalize_policy(item) for item in objects])
    return service.import_policies(text, format_hint="jsonl", batch_id=batch_id)


def test_conflict_outcomes_overwrite_skip_reject(service: ReplicationService) -> None:
    first = _import_samples(service)
    assert first["datasetVersion"] == 2
    current = service.get_policy("replica-east-01")["policy"]

    lower = dict(current)
    lower["policy_version"] = 1
    lower["updated_at_ms"] = 1763000000000
    lower["delay_threshold_ms"] = 111
    skipped = _import_objects(service, [lower], "conflict-lower")
    assert skipped["importReport"]["counts"]["SKIP"] == 1
    assert skipped["status"] == "NO_CHANGES"

    higher = dict(current)
    higher["policy_version"] = 8
    higher["delay_threshold_ms"] = 777
    overwritten = _import_objects(service, [higher], "conflict-higher")
    assert overwritten["importReport"]["counts"]["OVERWRITE"] == 1
    assert service.get_policy("replica-east-01")["policy"]["delay_threshold_ms"] == 777

    current = service.get_policy("replica-east-01")["policy"]
    ambiguous = dict(current)
    ambiguous["replication_group"] = "different-group"
    new_policy = {
        "slave_id": "replica-new-atomic",
        "replication_group": "new",
        "delay_threshold_ms": 10,
        "tolerable_delay_ms": 20,
        "expected_version": "8.0.1",
        "degradation_action": "MONITOR",
        "priority": 1,
        "policy_version": 1,
        "updated_at_ms": 1763000099999,
    }
    version_before = service.list_policies()["datasetVersion"]
    with pytest.raises(AppError) as captured:
        _import_objects(service, [new_policy, ambiguous], "conflict-reject")
    assert captured.value.code == "E_IMPORT_CONFLICT"
    assert captured.value.details["atomicRollback"] is True
    report = captured.value.details["importReport"]
    assert report["counts"]["ADDED"] == 1
    assert report["counts"]["REJECT"] == 1
    assert service.list_policies()["datasetVersion"] == version_before
    with pytest.raises(AppError) as missing:
        service.get_policy("replica-new-atomic")
    assert missing.value.code == "E_NOT_FOUND"


def test_failed_high_version_batch_does_not_change_dataset(service: ReplicationService) -> None:
    before = service.list_policies()
    with pytest.raises(AppError) as captured:
        service.import_policies(
            (SAMPLES / "policies_future_v2.jsonl").read_bytes(),
            format_hint="jsonl",
            batch_id="future-batch",
        )
    assert captured.value.code == "E_FORMAT_INCOMPATIBLE"
    after = service.list_policies()
    assert after["datasetVersion"] == before["datasetVersion"] == 1
    assert after["policies"] == []
    with pytest.raises(AppError) as retry:
        service.import_policies(
            (SAMPLES / "policies_future_v2.jsonl").read_bytes(),
            format_hint="jsonl",
            batch_id="future-batch",
        )
    assert retry.value.code == "E_FORMAT_INCOMPATIBLE"
    assert retry.value.details["deduplicated"] is True


def test_base_version_guard(service: ReplicationService, sample_policy_raw: list[dict]) -> None:
    _import_samples(service)
    with pytest.raises(AppError) as captured:
        service.upsert_policy(
            sample_policy_raw[0],
            slave_id="replica-east-01",
            base_dataset_version=1,
            batch_id="stale-edit",
        )
    assert captured.value.code == "E_STALE_BASE"
    assert captured.value.details["currentDatasetVersion"] == 2


def test_same_batch_concurrent_submissions_wait_and_reuse(service: ReplicationService) -> None:
    text = (SAMPLES / "policies.jsonl").read_text("utf-8")

    def submit() -> dict:
        return service.import_policies(text, format_hint="jsonl", batch_id="concurrent-same-batch")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: submit(), range(2)))
    assert {result["batchId"] for result in results} == {"concurrent-same-batch"}
    assert service.list_policies()["datasetVersion"] == 2
    assert sum(1 for result in results if result.get("deduplicated")) == 1
    with service.db.reader() as connection:
        count = connection.execute(
            "SELECT COUNT(*) AS n FROM applied_batches WHERE batch_id = 'concurrent-same-batch'"
        ).fetchone()["n"]
    assert count == 1

