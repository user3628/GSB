"""TLS handshake stage state machine."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import Codes, WorkbenchError

CLIENT_HELLO = "CLIENT_HELLO"
SERVER_HELLO = "SERVER_HELLO"
CERTIFICATE = "CERTIFICATE"
CERT_VERIFY = "CERT_VERIFY"
FINISHED = "FINISHED"

STAGES: tuple[str, ...] = (
    CLIENT_HELLO,
    SERVER_HELLO,
    CERTIFICATE,
    CERT_VERIFY,
    FINISHED,
)

# Legal forward migrations. No backward jump and no skipping a stage.
LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    "INIT": frozenset({CLIENT_HELLO}),
    CLIENT_HELLO: frozenset({SERVER_HELLO}),
    SERVER_HELLO: frozenset({CERTIFICATE}),
    CERTIFICATE: frozenset({CERT_VERIFY}),
    CERT_VERIFY: frozenset({FINISHED}),
    FINISHED: frozenset(),
}


@dataclass(frozen=True)
class Transition:
    source: str
    target: str
    legal: bool


def can_transition(source: str, target: str) -> bool:
    return target in LEGAL_TRANSITIONS.get(source, frozenset())


def assert_transition(source: str, target: str) -> Transition:
    if not can_transition(source, target):
        raise WorkbenchError(
            Codes.INVALID_TRANSITION,
            f"illegal handshake transition {source} -> {target}",
            {
                "source": source,
                "target": target,
                "legal_targets": sorted(LEGAL_TRANSITIONS.get(source, frozenset())),
            },
        )
    return Transition(source=source, target=target, legal=True)
