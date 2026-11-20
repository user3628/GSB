from __future__ import annotations

import hashlib
import hmac

from .errors import E_AUTH_DENIED, E_TOKEN_EXPIRED
from .models import AuthRegistry, AuthResult, EventInput, TokenState, canonical_json


def token_claims(
    *,
    token_id: str,
    client_id: str,
    quota_group: str,
    expires_at_ms: int,
    active: bool,
    issued_at_ms: int,
) -> dict[str, object]:
    return {
        "token_id": token_id,
        "client_id": client_id,
        "quota_group": quota_group,
        "expires_at_ms": expires_at_ms,
        "active": active,
        "issued_at_ms": issued_at_ms,
    }


def make_token_digest(
    secret: str,
    *,
    token_id: str,
    client_id: str,
    quota_group: str,
    expires_at_ms: int,
    active: bool,
    issued_at_ms: int,
) -> str:
    claims = token_claims(
        token_id=token_id,
        client_id=client_id,
        quota_group=quota_group,
        expires_at_ms=expires_at_ms,
        active=active,
        issued_at_ms=issued_at_ms,
    )
    return hmac.new(
        secret.encode("utf-8"),
        canonical_json(claims).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def authorize(
    event: EventInput,
    registry: AuthRegistry,
    *,
    token_version: int,
) -> AuthResult:
    """Authorize one request against a single pinned token-registry snapshot.

    Expiry boundary: an event is valid when timestamp_ms <= expires_at_ms;
    timestamp_ms == expires_at_ms + 1 is expired.
    """

    if token_version != registry.token_version:
        raise ValueError("token registry version mismatch")

    token: TokenState | None = registry.tokens.get(event.token_id)
    if token is None:
        return AuthResult(False, E_AUTH_DENIED, "TOKEN_NOT_FOUND")
    if not token.active:
        return AuthResult(False, E_AUTH_DENIED, "TOKEN_INACTIVE")
    if token.client_id != event.client_id:
        return AuthResult(False, E_AUTH_DENIED, "TOKEN_CLIENT_MISMATCH")

    expected = make_token_digest(
        registry.local_secret,
        token_id=token.token_id,
        client_id=token.client_id,
        quota_group=token.quota_group,
        expires_at_ms=token.expires_at_ms,
        active=token.active,
        issued_at_ms=token.issued_at_ms,
    )
    if not hmac.compare_digest(expected, event.token_digest):
        return AuthResult(False, E_AUTH_DENIED, "SIGNATURE_MISMATCH")

    client_group = registry.client_groups.get(event.client_id)
    if client_group is None:
        return AuthResult(False, E_AUTH_DENIED, "CLIENT_GROUP_NOT_FOUND")
    if client_group != token.quota_group:
        return AuthResult(False, E_AUTH_DENIED, "QUOTA_GROUP_MISMATCH")

    if event.timestamp_ms > token.expires_at_ms:
        return AuthResult(False, E_TOKEN_EXPIRED, "TOKEN_EXPIRED", token.quota_group)

    return AuthResult(True, "AUTH_OK", "TOKEN_VALID", token.quota_group)

