from __future__ import annotations

from leasefair.errors import (
    E_CONFLICTING_REQUEST,
    E_LOCK_HELD,
    E_NOT_HOLDER,
    E_STALE_LEASE,
    OK,
)
from leasefair.service import LeaseService


def test_grant_conflict_renew_release_and_monotonic_versions(service: LeaseService) -> None:
    granted = service.acquire(
        "alpha", request_id="a1", holder_id="holder-a", ttl_ms=10_000, weight="0.6"
    )
    assert granted["code"] == OK
    assert granted["lease"]["version"] == 1
    assert granted["lease"]["expires_at_ms"] == granted["lease"]["granted_at_ms"] + 10_000

    held = service.acquire(
        "alpha", request_id="a2", holder_id="holder-b", ttl_ms=1000, weight="0.4"
    )
    assert held["code"] == E_LOCK_HELD
    assert held["remaining_ttl_ms"] == 10_000
    assert held["current"]["holder_id"] == "holder-a"
    assert held["current"]["granted_at_ms"] == granted["lease"]["granted_at_ms"]

    stale_holder = service.renew(
        "alpha", request_id="a3", holder_id="holder-b", version=1, ttl_ms=20_000
    )
    assert stale_holder["code"] == E_NOT_HOLDER

    renewed = service.renew(
        "alpha", request_id="a4", holder_id="holder-a", version=1, ttl_ms=20_000
    )
    assert renewed["code"] == OK
    assert renewed["lease"]["renew_count"] == 1
    assert renewed["lease"]["expires_at_ms"] == renewed["now_ms"] + 20_000

    released = service.release(
        "alpha", request_id="a5", holder_id="holder-a", version=1
    )
    assert released["code"] == OK
    regranted = service.acquire(
        "alpha", request_id="a6", holder_id="holder-b", ttl_ms=1000, weight="0.4"
    )
    assert regranted["lease"]["version"] == 2


def test_stale_version_never_extends_expiry(service: LeaseService) -> None:
    first = service.acquire(
        "alpha", request_id="v1", holder_id="holder-a", ttl_ms=1000, weight="0.5"
    )
    first_version = first["lease"]["version"]
    service.release("alpha", request_id="release", holder_id="holder-a", version=first_version)
    second = service.acquire(
        "alpha", request_id="v2", holder_id="holder-b", ttl_ms=5000, weight="0.5"
    )
    expiry_before = second["lease"]["expires_at_ms"]
    metrics_before = service.metrics()
    response = service.renew(
        "alpha",
        request_id="stale-renew",
        holder_id="holder-a",
        version=first_version,
        ttl_ms=99_000,
    )
    assert response["code"] == E_STALE_LEASE
    assert service.metrics() == metrics_before
    snapshot = service.get_lock("alpha")
    assert snapshot["lease"]["expires_at_ms"] == expiry_before
    assert snapshot["lease"]["version"] == second["lease"]["version"]


def test_expired_lease_can_be_reacquired_and_old_renew_is_rejected(
    service: LeaseService, clock
) -> None:
    service.acquire("alpha", request_id="old", holder_id="a", ttl_ms=300, weight="0.5")
    clock.advance(301)
    expired_renew = service.renew(
        "alpha", request_id="old-renew", holder_id="a", version=1, ttl_ms=300
    )
    assert expired_renew["code"] == "E_LEASE_EXPIRED"
    replacement = service.acquire(
        "alpha", request_id="new", holder_id="b", ttl_ms=1000, weight="0.5"
    )
    assert replacement["code"] == OK
    assert replacement["lease"]["version"] == 2
    assert replacement["lease"]["holder_id"] == "b"
    metrics = service.metrics()
    assert metrics["cluster"]["occupied_keys"] == 1
    assert [lease["holder_id"] for lease in metrics["leases"]] == ["b"]


def test_idempotent_replay_and_conflicting_payload(service: LeaseService) -> None:
    payload = dict(request_id="same", holder_id="a", ttl_ms=1000, weight="0.5")
    first = service.acquire("alpha", **payload)
    replay = service.acquire("alpha", **payload)
    assert replay == first
    conflict = service.acquire(
        "alpha", request_id="same", holder_id="b", ttl_ms=1000, weight="0.5"
    )
    assert conflict["code"] == E_CONFLICTING_REQUEST


def test_registered_denominator_and_release_occupancy(service: LeaseService) -> None:
    for key in ("alpha", "beta", "gamma", "delta"):
        service.register_key(key)
    service.acquire("alpha", request_id="a", holder_id="holder", ttl_ms=5000, weight="1")
    metrics = service.metrics()
    assert metrics["cluster"]["occupied_keys"] == 1
    assert metrics["cluster"]["registered_keys"] == 4
    assert metrics["cluster"]["occupancy"]["q"] == 250_000_000

