from __future__ import annotations

import sqlite3

from gateway_middleware.auth import authorize
from gateway_middleware.errors import E_AUTH_DENIED, E_TOKEN_EXPIRED
from gateway_middleware.models import AuthRegistry, EventInput


def _registry(dataset, settings) -> AuthRegistry:
    return AuthRegistry(
        tokens={token.token_id: token for token in dataset.tokens},
        client_groups=dict(dataset.client_groups),
        quota_groups={group.name: group for group in dataset.quota_groups},
        token_version=1,
        local_secret=settings.local_secret,
    )


def test_expiry_boundary_and_auth_reason_priority(small_service, settings):
    _, dataset = small_service
    registry = _registry(dataset, settings)
    boundary = next(event for event in dataset.events if event.request_id.startswith("boundary-expiry"))
    expired = next(event for event in dataset.events if event.request_id.startswith("expired-"))

    accepted = authorize(boundary, registry, token_version=1)
    rejected = authorize(expired, registry, token_version=1)

    assert accepted.ok is True
    assert accepted.reason == "TOKEN_VALID"
    assert rejected.ok is False
    assert rejected.code == E_TOKEN_EXPIRED
    assert rejected.reason == "TOKEN_EXPIRED"

    one_ms_early = boundary.model_copy(update={"request_id": "early", "timestamp_ms": boundary.timestamp_ms - 1})
    assert authorize(one_ms_early, registry, token_version=1).ok is True


def test_signature_group_and_token_state_failures(small_service, settings):
    _, dataset = small_service
    registry = _registry(dataset, settings)
    template = next(event for event in dataset.events if event.request_id.startswith("small-"))

    tampered = template.model_copy(update={"request_id": "tampered", "token_digest": "0" * 64})
    unknown = template.model_copy(update={"request_id": "unknown", "token_id": "missing"})
    revoked = template.model_copy(
        update={"request_id": "revoked", "token_id": "tok-revoked-free", "client_id": "c-free-2"}
    )
    group_mismatch = template.model_copy(
        update={
            "request_id": "group-mismatch",
            "token_id": "tok-mismatch-standard",
            "client_id": "c-free-group-mismatch",
            "token_digest": next(
                token.digest_hex for token in dataset.tokens if token.token_id == "tok-mismatch-standard"
            ),
        }
    )

    assert authorize(tampered, registry, token_version=1).reason == "SIGNATURE_MISMATCH"
    assert authorize(unknown, registry, token_version=1).reason == "TOKEN_NOT_FOUND"
    assert authorize(revoked, registry, token_version=1).reason == "TOKEN_INACTIVE"
    assert authorize(group_mismatch, registry, token_version=1).reason == "QUOTA_GROUP_MISMATCH"
    assert all(
        authorize(event, registry, token_version=1).code == E_AUTH_DENIED
        for event in (tampered, unknown, revoked, group_mismatch)
    )


def test_auth_failures_never_enter_window_events(small_service):
    service, dataset = small_service
    result = service.submit_batch(dataset.batch("auth-window-batch"))

    auth_failures = [decision for decision in result.decisions if not decision.auth_ok]
    auth_successes = [decision for decision in result.decisions if decision.auth_ok]
    assert {decision.decision_code for decision in auth_failures} == {
        E_AUTH_DENIED,
        E_TOKEN_EXPIRED,
    }
    assert all(decision.rate_evaluated is False for decision in auth_failures)
    assert all(decision.short is None and decision.long is None for decision in auth_failures)

    with sqlite3.connect(service.store.db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM window_events").fetchone()[0]
        failed_ids = {decision.request_id for decision in auth_failures}
        persisted_failed = connection.execute(
            "SELECT COUNT(*) FROM window_events WHERE request_id IN (%s)"
            % ",".join("?" for _ in failed_ids),
            tuple(failed_ids),
        ).fetchone()[0]

    assert count == len(auth_successes)
    assert persisted_failed == 0
