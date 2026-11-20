from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from .auth import make_token_digest
from .config import Settings
from .models import BatchInput, EventInput, QuotaGroup, TokenState

BASE_TIMESTAMP_MS = 1_700_000_000_000

GROUP_DEFINITIONS: tuple[QuotaGroup, ...] = (
    QuotaGroup("free", short_limit_weight=8, long_limit_weight=40, target_weight_per_sec=8),
    QuotaGroup("standard", short_limit_weight=40, long_limit_weight=320, target_weight_per_sec=40),
    QuotaGroup("priority", short_limit_weight=150, long_limit_weight=1_200, target_weight_per_sec=150),
)

CLIENT_GROUPS: dict[str, str] = {
    "c-free-1": "free",
    "c-free-2": "free",
    "c-free-group-mismatch": "free",
    "c-standard-1": "standard",
    "c-standard-2": "standard",
    "c-priority-1": "priority",
    "c-priority-2": "priority",
}

ROUTES: tuple[str, ...] = (
    "/api/orders",
    "/api/search",
    "/api/profile",
    "/api/pay",
    "/api/export",
)


@dataclass(slots=True)
class SampleDataset:
    name: str
    seed: int
    base_timestamp_ms: int
    quota_groups: tuple[QuotaGroup, ...]
    client_groups: dict[str, str]
    tokens: tuple[TokenState, ...]
    events: list[EventInput]

    def batch(self, batch_id: str) -> BatchInput:
        return BatchInput(batch_id=batch_id, events=self.events)


def _token(
    secret: str,
    *,
    token_id: str,
    client_id: str,
    quota_group: str,
    expires_at_ms: int,
    active: bool,
    issued_at_ms: int,
) -> TokenState:
    digest = make_token_digest(
        secret,
        token_id=token_id,
        client_id=client_id,
        quota_group=quota_group,
        expires_at_ms=expires_at_ms,
        active=active,
        issued_at_ms=issued_at_ms,
    )
    return TokenState(
        token_id=token_id,
        client_id=client_id,
        quota_group=quota_group,
        expires_at_ms=expires_at_ms,
        digest_hex=digest,
        active=active,
        issued_at_ms=issued_at_ms,
    )


def _normal_token(secret: str, client_id: str, base_ts: int) -> TokenState:
    group = CLIENT_GROUPS[client_id]
    return _token(
        secret,
        token_id=f"tok-normal-{client_id}",
        client_id=client_id,
        quota_group=group,
        expires_at_ms=base_ts + 300_000,
        active=True,
        issued_at_ms=base_ts - 1_000,
    )


def _token_registry(secret: str, base_ts: int) -> tuple[TokenState, ...]:
    tokens = [_normal_token(secret, client_id, base_ts) for client_id in CLIENT_GROUPS]
    tokens.extend(
        [
            _token(
                secret,
                token_id="tok-boundary-standard",
                client_id="c-standard-1",
                quota_group="standard",
                expires_at_ms=base_ts + 5_000,
                active=True,
                issued_at_ms=base_ts - 1_000,
            ),
            _token(
                secret,
                token_id="tok-expired-free",
                client_id="c-free-1",
                quota_group="free",
                expires_at_ms=base_ts + 4_000,
                active=True,
                issued_at_ms=base_ts - 2_000,
            ),
            _token(
                secret,
                token_id="tok-revoked-free",
                client_id="c-free-2",
                quota_group="free",
                expires_at_ms=base_ts + 300_000,
                active=False,
                issued_at_ms=base_ts - 1_000,
            ),
            _token(
                secret,
                token_id="tok-mismatch-standard",
                client_id="c-free-group-mismatch",
                quota_group="standard",
                expires_at_ms=base_ts + 300_000,
                active=True,
                issued_at_ms=base_ts - 1_000,
            ),
        ]
    )
    return tuple(tokens)


def _event(
    *,
    seq: int,
    name: str,
    timestamp_ms: int,
    route_key: str,
    client_id: str,
    token_id: str,
    token_digest: str,
    weight: int = 1,
) -> EventInput:
    return EventInput(
        event_seq=seq,
        request_id=f"{name}-{seq:08d}",
        timestamp_ms=timestamp_ms,
        route_key=route_key,
        client_id=client_id,
        token_id=token_id,
        token_digest=token_digest,
        weight=weight,
    )


def generate_small(settings: Settings, seed: int = 20261121) -> SampleDataset:
    """A few hundred deterministic requests with auth and ordering edge cases."""
    base = BASE_TIMESTAMP_MS
    tokens = _token_registry(settings.local_secret, base)
    token_map = {token.token_id: token for token in tokens}
    rng = np.random.default_rng(seed)
    clients = list(CLIENT_GROUPS)
    routes = list(ROUTES)
    events: list[EventInput] = []
    seq = 0
    for _ in range(280):
        client = clients[int(rng.integers(0, len(clients)))]
        route = routes[int(rng.integers(0, len(routes)))]
        token = token_map[f"tok-normal-{client}"]
        events.append(
            _event(
                seq=seq,
                name="small",
                timestamp_ms=base + int(rng.integers(0, 3_500)),
                route_key=route,
                client_id=client,
                token_id=token.token_id,
                token_digest=token.digest_hex,
                weight=1 + int(rng.integers(0, 2)),
            )
        )
        seq += 1

    edge_events = [
        # Expiry boundary: timestamp == expires_at_ms is accepted.
        _event(
            seq=seq,
            name="boundary-expiry",
            timestamp_ms=token_map["tok-boundary-standard"].expires_at_ms,
            route_key="/api/orders",
            client_id="c-standard-1",
            token_id="tok-boundary-standard",
            token_digest=token_map["tok-boundary-standard"].digest_hex,
        ),
        # One millisecond after expiry is E_TOKEN_EXPIRED.
        _event(
            seq=seq + 1,
            name="expired",
            timestamp_ms=token_map["tok-expired-free"].expires_at_ms + 1,
            route_key="/api/orders",
            client_id="c-free-1",
            token_id="tok-expired-free",
            token_digest=token_map["tok-expired-free"].digest_hex,
        ),
        _event(
            seq=seq + 2,
            name="revoked",
            timestamp_ms=base + 1_000,
            route_key="/api/search",
            client_id="c-free-2",
            token_id="tok-revoked-free",
            token_digest=token_map["tok-revoked-free"].digest_hex,
        ),
        _event(
            seq=seq + 3,
            name="bad-signature",
            timestamp_ms=base + 1_001,
            route_key="/api/search",
            client_id="c-standard-2",
            token_id="tok-normal-c-standard-2",
            token_digest="0" * 64,
        ),
        _event(
            seq=seq + 4,
            name="unknown-token",
            timestamp_ms=base + 1_002,
            route_key="/api/pay",
            client_id="c-standard-2",
            token_id="tok-does-not-exist",
            token_digest="f" * 64,
        ),
        _event(
            seq=seq + 5,
            name="group-mismatch",
            timestamp_ms=base + 1_003,
            route_key="/api/pay",
            client_id="c-free-group-mismatch",
            token_id="tok-mismatch-standard",
            token_digest=token_map["tok-mismatch-standard"].digest_hex,
        ),
        # Deliberately appended out of chronological order: a late event.
        _event(
            seq=seq + 6,
            name="late-event",
            timestamp_ms=base + 17,
            route_key="/api/export",
            client_id="c-priority-1",
            token_id="tok-normal-c-priority-1",
            token_digest=token_map["tok-normal-c-priority-1"].digest_hex,
        ),
    ]
    events.extend(edge_events)
    return SampleDataset(
        name="small",
        seed=seed,
        base_timestamp_ms=base,
        quota_groups=GROUP_DEFINITIONS,
        client_groups=dict(CLIENT_GROUPS),
        tokens=tokens,
        events=events,
    )


def generate_large(
    settings: Settings,
    *,
    event_count: int = 100_000,
    seed: int = 20261122,
) -> SampleDataset:
    """Deterministic large workload with a hot route/client burst for sampling."""
    if event_count < 10_000:
        raise ValueError("large sample requires at least 10,000 events")
    base = BASE_TIMESTAMP_MS
    tokens = _token_registry(settings.local_secret, base)
    token_map = {token.token_id: token for token in tokens}
    rng = np.random.default_rng(seed)

    normal_clients = list(CLIENT_GROUPS)
    normal_tokens = [token_map[f"tok-normal-{client}"] for client in normal_clients]
    auth_failure_count = max(9, event_count // 10_000)
    expired_count = auth_failure_count
    revoked_count = max(3, auth_failure_count // 2)
    tampered_count = max(3, auth_failure_count // 2)
    random_count = event_count - expired_count - revoked_count - tampered_count
    if random_count <= 0:
        raise ValueError("event_count is too small")

    timestamps = rng.integers(0, 120_000, size=random_count, dtype=np.int64)
    client_indices = rng.integers(0, len(normal_clients), size=random_count, dtype=np.int64)
    route_indices = rng.integers(0, len(ROUTES), size=random_count, dtype=np.int64)
    weights = 1 + rng.integers(0, 3, size=random_count, dtype=np.int64)

    # Force a dense 1-second burst on one priority route/client pair. This
    # exceeds both the short exact limit and the short base quota.
    burst_size = min(7_500, max(1_500, event_count // 18))
    burst_start = 10_000
    hot_client_index = normal_clients.index("c-priority-1")
    hot_route_index = ROUTES.index("/api/search")
    burst_floor = max(0, random_count - burst_size)
    for offset in range(random_count - burst_size, random_count):
        timestamps[offset] = burst_start + (offset - (random_count - burst_size)) % 1_000
        client_indices[offset] = hot_client_index
        route_indices[offset] = hot_route_index
        weights[offset] = 1

    events: list[EventInput] = []
    seq = 0
    for index in range(random_count):
        client = normal_clients[int(client_indices[index])]
        token = token_map[f"tok-normal-{client}"]
        events.append(
            _event(
                seq=seq,
                name="large",
                timestamp_ms=base + int(timestamps[index]),
                route_key=ROUTES[int(route_indices[index])],
                client_id=client,
                token_id=token.token_id,
                token_digest=token.digest_hex,
                weight=int(weights[index]),
            )
        )
        seq += 1

    expired_token = token_map["tok-expired-free"]
    for index in range(expired_count):
        events.append(
            _event(
                seq=seq,
                name="large-expired",
                timestamp_ms=base + 5_000 + index,
                route_key="/api/orders",
                client_id="c-free-1",
                token_id=expired_token.token_id,
                token_digest=expired_token.digest_hex,
            )
        )
        seq += 1
    revoked_token = token_map["tok-revoked-free"]
    for index in range(revoked_count):
        events.append(
            _event(
                seq=seq,
                name="large-revoked",
                timestamp_ms=base + 20_000 + index,
                route_key="/api/profile",
                client_id="c-free-2",
                token_id=revoked_token.token_id,
                token_digest=revoked_token.digest_hex,
            )
        )
        seq += 1
    tampered_token = token_map["tok-normal-c-standard-1"]
    for index in range(tampered_count):
        events.append(
            _event(
                seq=seq,
                name="large-tampered",
                timestamp_ms=base + 30_000 + index,
                route_key="/api/pay",
                client_id="c-standard-1",
                token_id=tampered_token.token_id,
                token_digest=("a" * 63) + str(index % 10),
            )
        )
        seq += 1

    return SampleDataset(
        name="large",
        seed=seed,
        base_timestamp_ms=base,
        quota_groups=GROUP_DEFINITIONS,
        client_groups=dict(CLIENT_GROUPS),
        tokens=tokens,
        events=events,
    )


def seed_dataset(service, dataset: SampleDataset, *, allow_existing: bool = False) -> int:
    return service.store.seed_local_registry(
        quota_groups=dataset.quota_groups,
        client_groups=dataset.client_groups,
        tokens=dataset.tokens,
        allow_existing=allow_existing,
    )


def decision_distribution(decisions: Sequence) -> dict[str, int]:
    counts: dict[str, int] = {}
    for decision in decisions:
        counts[decision.decision_code] = counts.get(decision.decision_code, 0) + 1
    return dict(sorted(counts.items()))
