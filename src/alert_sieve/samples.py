from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .domain import Alert, alert_snapshot_checksum, parse_alert_snapshot
from .errors import snapshot_invalid
from .util import normalize_checksum


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_alerts_jsonl(path: str | Path) -> dict[str, Any]:
    """Load a header-plus-alert JSONL file and return an alert_snapshot body.

    The JSONL format is intentionally simple:
    line 1: {"record_type":"header","kind":"alert_snapshot",...}
    line n: {"record_type":"alert", "timestamp_ms":...}
    """
    file_path = Path(path)
    lines = [line for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        raise snapshot_invalid("alerts JSONL file is empty", path=str(file_path))
    try:
        header = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise snapshot_invalid("invalid JSONL header", path=str(file_path), reason=str(exc)) from exc
    if header.get("record_type") != "header" or header.get("kind") != "alert_snapshot":
        raise snapshot_invalid("JSONL first line must be an alert_snapshot header")
    try:
        version = int(header["version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise snapshot_invalid("JSONL header.version must be an integer") from exc
    batch_id = header.get("batch_id") or f"snapshot-v{version}"
    if not isinstance(batch_id, str) or not batch_id:
        raise snapshot_invalid("JSONL header.batch_id must be a non-empty string")
    checksum = header.get("checksum")
    if not isinstance(checksum, str):
        raise snapshot_invalid("JSONL header.checksum is required")
    try:
        checksum = normalize_checksum(checksum)
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    alerts = []
    for index, line in enumerate(lines[1:], start=1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise snapshot_invalid("invalid alert JSONL record", line_number=index + 1, reason=str(exc)) from exc
        if record.get("record_type") not in (None, "alert"):
            raise snapshot_invalid("JSONL alert record has an unexpected record_type", line_number=index + 1)
        if "batch_id" in record and record["batch_id"] != batch_id:
            raise snapshot_invalid("JSONL alert batch_id does not match the header", line_number=index + 1)
        if "line_no" in record:
            raise snapshot_invalid("JSONL line_no is implicit and must not be supplied", line_number=index + 1)
        payload = {key: value for key, value in record.items() if key != "record_type"}
        try:
            alert = Alert(
                batch_id=batch_id,
                line_no=index,
                timestamp_ms=int(payload["timestamp_ms"]),
                fingerprint=str(payload["fingerprint"]),
                service=str(payload["service"]),
                resource=str(payload["resource"]),
                severity=str(payload["severity"]).upper(),
                labels=dict(payload.get("labels", {})),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise snapshot_invalid("invalid alert JSONL record", line_number=index + 1, reason=str(exc)) from exc
        alerts.append(alert)
    expected = alert_snapshot_checksum(version, alerts)
    if expected != checksum:
        raise snapshot_invalid("JSONL header checksum mismatch", expected=expected, actual=checksum)
    body = {
        "kind": "alert_snapshot",
        "version": version,
        "batch_id": batch_id,
        "checksum": checksum,
        "alerts": [{"line_no": alert.line_no, **alert.content_payload()} for alert in alerts],
    }
    parse_alert_snapshot(body)
    return body
