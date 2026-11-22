from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import business_error


INJECTION_KINDS = {
    "projection_dim": "projection_alignment",
    "invalid_operator": "fusion_scoring",
    "text_too_long": "text_encoding",
    "similarity_nan": "projection_alignment",
    "missing_ref": "projection_alignment",
}


@dataclass(frozen=True)
class Injection:
    kind: str
    sample_id: str | None = None

    @property
    def stage(self) -> str:
        return INJECTION_KINDS[self.kind]

    def to_dict(self) -> dict[str, str]:
        result = {"kind": self.kind}
        if self.sample_id is not None:
            result["sample_id"] = self.sample_id
        return result


def parse_injections(value: Any) -> tuple[Injection, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = [{"kind": value}]
    if not isinstance(value, list):
        raise business_error(
            "E_INVALID_INPUT",
            "injection",
            "injections must be an array",
            field="injections",
            http_status=400,
        )
    result: list[Injection] = []
    for item in value:
        if isinstance(item, str):
            kind = item
            sample_id = None
        elif isinstance(item, dict):
            kind = item.get("kind")
            sample_id = item.get("sample_id")
        else:
            raise business_error(
                "E_INVALID_INPUT",
                "injection",
                "injection entry must be an object or string",
                field="injections",
                http_status=400,
            )
        if kind not in INJECTION_KINDS:
            raise business_error(
                "E_INVALID_INPUT",
                "injection",
                "unsupported injection kind",
                diagnostics={"kind": kind, "supported": sorted(INJECTION_KINDS)},
                field="kind",
                http_status=400,
            )
        if sample_id is not None and not isinstance(sample_id, str):
            raise business_error(
                "E_INVALID_INPUT",
                "injection",
                "sample_id must be a string",
                field="sample_id",
                http_status=400,
            )
        result.append(Injection(kind=str(kind), sample_id=sample_id))
    return tuple(result)


def injection_map(injections: tuple[Injection, ...]) -> dict[str, Injection]:
    return {injection.kind: injection for injection in injections}
