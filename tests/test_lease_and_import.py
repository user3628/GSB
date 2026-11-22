from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from svc_registry.db import Database
from svc_registry.errors import E_CONFLICTING_BATCH, E_STALE_LEASE, BusinessError
from svc_registry.formats import encode_jsonl
from svc_registry.importer import ImportInput, SnapshotImporter
from svc_registry.models import NormalizedRegistration, RegistrationRecord, Snapshot
from svc_registry.policies import TimePolicy, WeightPolicy
from svc_registry.service import RegistryService


def normalized(
    instance_id: str = "i",
    *,
    weight: int = 500,
    version: int = 1,
    endpoint: str = "http://127.0.0.1:28765/health/ok",
) -> NormalizedRegistration:
    return NormalizedRegistration(
        service_name="svc",
        instance_id=instance_id,
        endpoint=endpoint,
        weight_permille=weight,
        lease_ttl_ms=30_000,
        probe_method="http",
        probe_params={"timeout_ms": 500, "expected_status": 200, "expected_body_contains": "OK"},
        registered_at_ms=1_700_000_000_000,
        lease_version=version,
        probe_window_ms=400,
        last_heartbeat_at_ms=1_700_000_000_000,
    )


def test_duplicate_registration_updates_and_renewal_is_exactly_once() -> None:
    path = tempfile.mktemp(suffix=".db")
    service = RegistryService(path)

    async def scenario() -> None:
        first = await service.register(normalized())
        assert first["action"] == "created"
        assert first["lease"]["lease_version"] == 1
        second = await service.register(normalized(weight=700))
        assert second["action"] == "updated"
        assert second["lease"]["lease_version"] == 2
        before = service.get_instance("svc", "i")
        stale_version = before["lease_version"] - 1
        with pytest.raises(BusinessError) as stale:
            await service.renew_lease("svc", "i", stale_version)
        assert stale.value.code == E_STALE_LEASE
        after_stale = service.get_instance("svc", "i")
        assert after_stale["lease_expires_at_ms"] == before["lease_expires_at_ms"]

        current_version = before["lease_version"]
        results = await asyncio.gather(
            *[service.renew_lease("svc", "i", current_version) for _ in range(6)]
        )
        assert {result.instance["lease_version"] for result in results} == {current_version + 1}
        assert sum(not result.reused_concurrent_result for result in results) == 1
        assert sum(result.reused_concurrent_result for result in results) == 5
        with service.db.read() as connection:
            renew_events = connection.execute(
                "SELECT COUNT(*) FROM lease_events WHERE service_name='svc' AND instance_id='i' AND event_type='RENEW'"
            ).fetchone()[0]
        assert renew_events == 1
        with pytest.raises(BusinessError) as stale_retry:
            await service.renew_lease("svc", "i", current_version)
        assert stale_retry.value.code == E_STALE_LEASE

    asyncio.run(scenario())


def _record(
    *,
    instance_id: str,
    version: int,
    registered_at_ms: int,
    weight: int,
    endpoint: str = "http://127.0.0.1:28765/health/ok",
) -> RegistrationRecord:
    return RegistrationRecord(
        service_name="svc",
        instance_id=instance_id,
        endpoint=endpoint,
        weight=weight,
        weight_encoding="permille",
        lease_ttl_ms=30_000,
        probe_method="http",
        probe_params={"timeout_ms": 500, "expected_status": 200, "expected_body_contains": "OK"},
        registered_at=registered_at_ms,
        time_unit="ms",
        lease_version=version,
        probe_window_ms=400,
        last_heartbeat_at=registered_at_ms,
        source_version=2,
    )


def _payload(records: list[RegistrationRecord]) -> list[bytes]:
    return encode_jsonl(Snapshot("registration", 2, tuple(records)), shards=2)


def test_import_conflicts_are_atomic_and_batch_idempotent() -> None:
    path = tempfile.mktemp(suffix=".db")
    service = RegistryService(path)
    importer = SnapshotImporter(service.db)
    policies = {
        "weight_policy": WeightPolicy(),
        "time_policy": TimePolicy(declared=True, truncation_rule="floor"),
    }

    async def scenario() -> None:
        await service.register(normalized("keep", weight=500, version=1))
        current = service.get_instance("svc", "keep")

        older = _record(
            instance_id="keep",
            version=1,
            registered_at_ms=1,
            weight=600,
        )
        report = await importer.import_snapshot(
            ImportInput(batch_id="skip-old", data=_payload([older]), **policies)
        )
        assert report["status"] == "committed"
        assert report["outcomes"][0]["outcome"] == "skip"
        assert report["outcomes"][0]["basis"] == "last_registered_at_older"
        assert service.get_instance("svc", "keep")["weight_permille"] == 500

        newer = _record(
            instance_id="keep",
            version=2,
            registered_at_ms=current["last_registered_at_ms"] + 1000,
            weight=650,
        )
        report = await importer.import_snapshot(
            ImportInput(batch_id="override-new", data=_payload([newer]), **policies)
        )
        assert report["outcomes"][0]["outcome"] == "override"
        assert service.get_instance("svc", "keep")["weight_permille"] == 650
        assert service.get_instance("svc", "keep")["lease_version"] == 2

        replayed = await importer.import_snapshot(
            ImportInput(batch_id="override-new", data=_payload([newer]), **policies)
        )
        assert replayed["replayed"] is True
        assert replayed["executed"] is False

        with pytest.raises(BusinessError) as conflict:
            await importer.import_snapshot(
                ImportInput(
                    batch_id="override-new",
                    data=_payload([_record(instance_id="keep", version=9, registered_at_ms=9, weight=900)]),
                    **policies,
                )
            )
        assert conflict.value.code == E_CONFLICTING_BATCH

        tied = service.get_instance("svc", "keep")
        ambiguous = _record(
            instance_id="keep",
            version=tied["lease_version"],
            registered_at_ms=tied["last_registered_at_ms"],
            weight=tied["weight_permille"],
            endpoint="http://127.0.0.1:28765/health/wrong-marker",
        )
        brand_new = _record(instance_id="must-not-appear", version=1, registered_at_ms=1, weight=500)
        rejected = await importer.import_snapshot(
            ImportInput(batch_id="ambiguous", data=_payload([brand_new, ambiguous]), **policies)
        )
        assert rejected["status"] == "rejected"
        assert rejected["outcomes"][1]["outcome"] == "reject"
        assert rejected["outcomes"][1]["basis"] == "ambiguous_complete_tie"
        assert rejected["atomic_rollback"] is True
        assert service.db.get_instance("svc", "must-not-appear") is None

    asyncio.run(scenario())
