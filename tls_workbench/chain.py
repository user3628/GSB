"""Certificate path construction and whole-chain validation."""

from __future__ import annotations

from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature

from . import timeutil
from .certificates import verify_signature
from .errors import Codes, WorkbenchError


@dataclass(frozen=True)
class ChainPath:
    chain_id: str
    ordered_roles: tuple[str, ...]
    edges: tuple[dict, ...]
    margins: tuple[dict, ...]
    signature_checks: tuple[dict, ...]
    root_self_signed: bool


def _index_by_subject(certificates: dict) -> dict:
    index: dict[str, list[str]] = {}
    for role, info in certificates.items():
        index.setdefault(info["subject"], []).append(role)
    return index


def build_path(chain: dict) -> list[str]:
    """Build leaf -> intermediate -> root from issuer/subject relationships.

    Raises ``E_PATH_LOOP`` on a cycle and ``E_CHAIN_INVALID`` if the chain
    cannot terminate at a self-issued root.
    """
    certificates = chain["certificates"]
    if "leaf" not in certificates:
        raise WorkbenchError(Codes.CHAIN_INVALID, "chain is missing leaf")
    if "root" not in certificates:
        raise WorkbenchError(Codes.CHAIN_INVALID, "chain is missing root")
    by_subject = _index_by_subject(certificates)
    current = "leaf"
    ordered: list[str] = []
    seen: set[str] = set()
    while True:
        if current in seen:
            raise WorkbenchError(
                Codes.PATH_LOOP,
                f"certificate path contains a cycle at role {current}",
                {"visited": ordered + [current]},
            )
        seen.add(current)
        ordered.append(current)
        info = certificates[current]
        if info["subject"] == info["issuer"]:
            if current != "root":
                raise WorkbenchError(
                    Codes.CHAIN_INVALID,
                    f"self-issued certificate at non-root role {current}",
                )
            return ordered
        parents = by_subject.get(info["issuer"], [])
        if not parents:
            raise WorkbenchError(
                Codes.CHAIN_INVALID,
                f"no issuer found for role {current} (issuer={info['issuer']})",
            )
        if len(parents) > 1:
            raise WorkbenchError(
                Codes.CHAIN_INVALID,
                f"ambiguous issuer match for role {current}: {parents}",
            )
        current = parents[0]


def validate_chain(chain: dict, instant: timeutil.VerificationInstant) -> ChainPath:
    objects = chain["_objects"]
    ordered = build_path(chain)
    edges: list[dict] = []
    margins: list[dict] = []
    signature_checks: list[dict] = []
    for position, role in enumerate(ordered):
        info = chain["certificates"][role]
        margin = timeutil.evaluate_validity(
            info["not_before_ms"], info["not_after_ms"], instant
        )
        margins.append(
            {
                "role": role,
                "serial_number": info["serial_number"],
                **margin.__dict__,
            }
        )
        if not margin.is_valid:
            raise WorkbenchError(
                margin.status,
                f"certificate {role} validity check failed: {margin.status}",
                {
                    "role": role,
                    "margin_before_ms": margin.margin_before_ms,
                    "margin_after_ms": margin.margin_after_ms,
                    "effective_verify_ms": margin.effective_verify_ms,
                },
            )
    for child_role, issuer_role in zip(ordered, ordered[1:]):
        child = objects[child_role]
        issuer = objects[issuer_role]
        try:
            verify_signature(child, issuer.public_key())
            ok = True
            error = None
        except InvalidSignature:
            ok = False
            error = "signature does not verify against issuer public key"
        except WorkbenchError as exc:
            ok = False
            error = exc.message
        signature_checks.append(
            {
                "child_role": child_role,
                "issuer_role": issuer_role,
                "verified": ok,
                "error": error,
            }
        )
        edges.append(
            {
                "child_role": child_role,
                "issuer_role": issuer_role,
                "child_subject": chain["certificates"][child_role]["subject"],
                "issuer_subject": chain["certificates"][issuer_role]["subject"],
            }
        )
        if not ok:
            raise WorkbenchError(
                Codes.SIGNATURE_INVALID,
                f"{child_role} signature verification failed",
                {"child_role": child_role, "issuer_role": issuer_role},
            )
    root = objects["root"]
    root_self_signed = root.subject == root.issuer
    if not root_self_signed:
        raise WorkbenchError(
            Codes.ROOT_NOT_SELF_SIGNED, "root certificate is not self-issued"
        )
    try:
        verify_signature(root, root.public_key())
        root_signature_ok = True
        root_signature_error = None
    except InvalidSignature:
        root_signature_ok = False
        root_signature_error = "root self-signature failed"
    signature_checks.append(
        {
            "child_role": "root",
            "issuer_role": "root",
            "verified": root_signature_ok,
            "error": root_signature_error,
        }
    )
    if not root_signature_ok:
        raise WorkbenchError(
            Codes.SIGNATURE_INVALID,
            "root self-signature verification failed",
        )
    return ChainPath(
        chain_id=chain["chain_id"],
        ordered_roles=tuple(ordered),
        edges=tuple(edges),
        margins=tuple(margins),
        signature_checks=tuple(signature_checks),
        root_self_signed=root_self_signed,
    )
