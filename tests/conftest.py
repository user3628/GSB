from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from gateway_middleware.auth import make_token_digest
from gateway_middleware.config import Settings
from gateway_middleware.models import EventInput, QuotaGroup, TokenState
from gateway_middleware.sample_data import generate_small, seed_dataset
from gateway_middleware.service import GatewayService


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return replace(Settings(), db_path=tmp_path / "gateway.sqlite3")


@pytest.fixture
def small_service(settings: Settings):
    service = GatewayService(settings)
    dataset = generate_small(settings)
    seed_dataset(service, dataset)
    return service, dataset


def event(
    *,
    seq: int = 0,
    request_id: str | None = None,
    timestamp_ms: int = 1_700_000_000_000,
    route_key: str = "/api/test",
    client_id: str = "c1",
    token_id: str = "tok-c1",
    token_digest: str = "digest",
    weight: int = 1,
) -> EventInput:
    return EventInput(
        event_seq=seq,
        request_id=request_id or f"req-{seq}",
        timestamp_ms=timestamp_ms,
        route_key=route_key,
        client_id=client_id,
        token_id=token_id,
        token_digest=token_digest,
        weight=weight,
    )


def seeded_tiny_service(
    tmp_path: Path,
    *,
    short_limit: int = 5,
    long_limit: int = 20,
    **setting_overrides,
):
    settings = replace(
        Settings(),
        db_path=tmp_path / "tiny.sqlite3",
        short_exact_limit=4,
        long_exact_limit=8,
        **setting_overrides,
    )
    group = QuotaGroup(
        "tiny",
        short_limit_weight=short_limit,
        long_limit_weight=long_limit,
        target_weight_per_sec=short_limit,
    )
    expires = 1_700_000_100_000
    digest = make_token_digest(
        settings.local_secret,
        token_id="tok-c1",
        client_id="c1",
        quota_group="tiny",
        expires_at_ms=expires,
        active=True,
        issued_at_ms=1_699_999_000_000,
    )
    token = TokenState(
        token_id="tok-c1",
        client_id="c1",
        quota_group="tiny",
        expires_at_ms=expires,
        digest_hex=digest,
        active=True,
        issued_at_ms=1_699_999_000_000,
    )
    service = GatewayService(settings)
    seed_dataset_version = service.store.seed_local_registry(
        quota_groups=(group,),
        client_groups={"c1": "tiny"},
        tokens=(token,),
    )
    return service, token, seed_dataset_version


def event_for_token(token: TokenState, *, seq: int, timestamp_ms: int, route: str = "/api/test", weight: int = 1):
    return EventInput(
        event_seq=seq,
        request_id=f"tiny-{seq:06d}",
        timestamp_ms=timestamp_ms,
        route_key=route,
        client_id=token.client_id,
        token_id=token.token_id,
        token_digest=token.digest_hex,
        weight=weight,
    )
