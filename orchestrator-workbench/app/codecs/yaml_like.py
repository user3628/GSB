"""Legacy YAML-subset line codec.

Each resource is exactly one line in a deliberately small YAML subset:

    resource_id: [parent_a, parent_b]
    resource_id: []
    # comment lines and blank lines are ignored

Only the identifier and the parent list exist in this format: there is no
weight field, no budget and no expected version. Snapshot metadata travels in
comment header lines of the form ``# @snapshot_seq 1``.
"""
from __future__ import annotations

from app.errors import ErrorCode, WorkbenchError
from app.models import DecodedPayload, FormatName, RawRecord, SnapshotHeader

SUPPORTED_VERSION = 1
DEFAULT_GENERATED_AT = "1970-01-01T00:00:00Z"


def encode(payload: DecodedPayload) -> bytes:
    if payload.header.format is not FormatName.YAML:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "yaml codec requires yaml format")
    if payload.header.format_version != SUPPORTED_VERSION:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"writer only emits yaml v{SUPPORTED_VERSION}, got v{payload.header.format_version}",
        )
    lines = [
        "# @format yaml",
        f"# @format_version {payload.header.format_version}",
        f"# @snapshot_seq {payload.header.snapshot_seq}",
        f"# @generated_at {payload.header.generated_at}",
    ]
    for record in payload.records:
        parents = ", ".join(record.parents)
        lines.append(f"{record.resource_id}: [{parents}]")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _parse_parent_list(raw: str, line_no: int) -> list[str]:
    raw = raw.strip()
    if not (raw.startswith("[") and raw.endswith("]")):
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {line_no}: parent list must use [..] syntax")
    inner = raw[1:-1].strip()
    if not inner:
        return []
    parents = []
    for item in inner.split(","):
        name = item.strip()
        if not name or any(ch.isspace() for ch in name):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {line_no}: invalid parent entry {item!r}")
        parents.append(name)
    return parents


def decode(data: bytes) -> DecodedPayload:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"invalid utf-8: {exc}") from exc

    snapshot_seq = 0
    format_version = SUPPORTED_VERSION
    generated_at = DEFAULT_GENERATED_AT
    records: list[RawRecord] = []
    seen_ids: set[str] = set()

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            body = line[1:].strip()
            if body.startswith("@snapshot_seq"):
                try:
                    snapshot_seq = int(body.split()[1])
                except (IndexError, ValueError) as exc:
                    raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "bad @snapshot_seq header") from exc
            elif body.startswith("@format_version"):
                try:
                    format_version = int(body.split()[1])
                except (IndexError, ValueError) as exc:
                    raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "bad @format_version header") from exc
            elif body.startswith("@generated_at"):
                generated_at = body[len("@generated_at") :].strip() or DEFAULT_GENERATED_AT
            continue
        if ":" not in line:
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {line_no}: expected 'id: [parents]'")
        resource_id, parent_part = line.split(":", 1)
        resource_id = resource_id.strip()
        if not resource_id or any(ch.isspace() for ch in resource_id):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {line_no}: invalid resource id")
        if resource_id in seen_ids:
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {line_no}: duplicate resource {resource_id}")
        seen_ids.add(resource_id)
        records.append(
            RawRecord(
                resource_id=resource_id,
                parents=_parse_parent_list(parent_part, line_no),
                present_fields=["parents"] if parent_part.strip() not in ("[]", "") else [],
            )
        )

    if not records:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "yaml snapshot contains no records")

    header = SnapshotHeader(
        format=FormatName.YAML,
        format_version=format_version,
        snapshot_seq=snapshot_seq,
        generated_at=generated_at,
    )
    return DecodedPayload(header=header, records=records)
