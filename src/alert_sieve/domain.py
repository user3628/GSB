from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .errors import BusinessError, alert_invalid, snapshot_invalid
from .util import digest, normalize_checksum

# Public, documented constants. All score arithmetic is integer-only.
SCORE_SCALE = 1_000_000
HOP_BASE = 600_000
DEFAULT_TIME_WINDOW_MS = 300_000
SCORE_TOLERANCE = 1_000
MAX_HOPS = 31
MAX_TS_MS = 2**63 - 1
MAX_WINDOW_MS = 2**63 - 1

ALLOWED_SEVERITIES = {
    "INFO",
    "WARNING",
    "WARN",
    "ERROR",
    "CRITICAL",
    "FATAL",
    "DEBUG",
    "P0",
    "P1",
    "P2",
    "P3",
    "P4",
}


def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise snapshot_invalid(f"{where} must be a JSON object")
    return value


def _require_nonempty_string(value: Any, field: str, *, max_len: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    text = value.strip()
    if len(text) > max_len:
        raise ValueError(f"{field} must be at most {max_len} characters")
    return text


def _require_int(value: Any, field: str, *, minimum: int = 0, maximum: int = MAX_TS_MS) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return value


def _require_labels(value: Any, field: str = "labels") -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object of string values")
    labels: dict[str, str] = {}
    for key, raw in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field} keys must be non-empty strings")
        if not isinstance(raw, str):
            raise ValueError(f"{field}.{key} must be a string")
        labels[key.strip()] = raw
    return dict(sorted(labels.items()))


def _normalize_severity(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("severity must be a string")
    severity = value.strip().upper()
    if severity not in ALLOWED_SEVERITIES:
        raise ValueError(f"severity must be one of {sorted(ALLOWED_SEVERITIES)}")
    return "WARNING" if severity == "WARN" else severity


def _checksum_from_body(body: Mapping[str, Any], *, where: str) -> str:
    raw = body.get("checksum")
    if not isinstance(raw, str) or not raw:
        raise snapshot_invalid(f"{where}.checksum is required")
    try:
        return normalize_checksum(raw)
    except ValueError as exc:
        raise snapshot_invalid(str(exc), field=f"{where}.checksum") from exc


def verify_checksum(expected: str, actual: str, *, where: str) -> None:
    if expected != actual:
        raise snapshot_invalid(
            f"{where} checksum mismatch",
            expected=expected,
            actual=actual,
        )


@dataclass(frozen=True)
class Alert:
    batch_id: str
    line_no: int
    timestamp_ms: int
    fingerprint: str
    service: str
    resource: str
    severity: str
    labels: dict[str, str]

    @property
    def alert_uid(self) -> str:
        return f"{self.batch_id}:{self.line_no}"

    @property
    def content_hash(self) -> str:
        return digest(self.content_payload())

    def content_payload(self) -> dict[str, Any]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "fingerprint": self.fingerprint,
            "service": self.service,
            "resource": self.resource,
            "severity": self.severity,
            "labels": self.labels,
        }

    def persisted_payload(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "line_no": self.line_no,
            **self.content_payload(),
        }


@dataclass(frozen=True)
class FingerprintMeta:
    fingerprint: str
    service: str
    labels: dict[str, str]

    @property
    def content_hash(self) -> str:
        return digest({"fingerprint": self.fingerprint, "service": self.service, "labels": self.labels})

    def payload(self) -> dict[str, Any]:
        return {"fingerprint": self.fingerprint, "service": self.service, "labels": self.labels}


@dataclass(frozen=True)
class AlertSnapshot:
    version: int
    batch_id: str | None
    checksum: str
    alerts: tuple[Alert, ...]
    fingerprints: dict[str, FingerprintMeta]


@dataclass(frozen=True)
class AlertIncrement:
    batch_id: str
    base_version: int
    version: int
    checksum: str
    alerts: tuple[Alert, ...]
    affected_fingerprints: tuple[FingerprintMeta, ...]


def _parse_alert(
    raw: Any,
    *,
    index: int,
    default_batch_id: str | None,
    default_line_no: int | None,
) -> Alert:
    try:
        obj = _require_mapping(raw, f"alerts[{index}]")
        batch_id = obj.get("batch_id", default_batch_id)
        batch_id = _require_nonempty_string(batch_id, "batch_id", max_len=256)
        line_raw = obj.get("line_no", default_line_no)
        line_no = _require_int(line_raw, "line_no", minimum=1, maximum=2**31 - 1)
        timestamp_ms = _require_int(obj.get("timestamp_ms"), "timestamp_ms")
        fingerprint = _require_nonempty_string(obj.get("fingerprint"), "fingerprint", max_len=256)
        service = _require_nonempty_string(obj.get("service"), "service", max_len=256)
        resource = _require_nonempty_string(obj.get("resource"), "resource", max_len=1024)
        severity = _normalize_severity(obj.get("severity"))
        labels = _require_labels(obj.get("labels", {}))
        return Alert(
            batch_id=batch_id,
            line_no=line_no,
            timestamp_ms=timestamp_ms,
            fingerprint=fingerprint,
            service=service,
            resource=resource,
            severity=severity,
            labels=labels,
        )
    except BusinessError:
        raise
    except ValueError as exc:
        raise alert_invalid(str(exc), alert_index=index) from exc


def parse_fingerprint_meta(raw: Any, *, index: int) -> FingerprintMeta:
    try:
        obj = _require_mapping(raw, f"affected_fingerprints[{index}]")
        return FingerprintMeta(
            fingerprint=_require_nonempty_string(obj.get("fingerprint"), "fingerprint", max_len=256),
            service=_require_nonempty_string(obj.get("service"), "service", max_len=256),
            labels=_require_labels(obj.get("labels", {})),
        )
    except ValueError as exc:
        raise alert_invalid(str(exc), affected_fingerprint_index=index) from exc


def _merge_fingerprints(
    alerts: Iterable[Alert],
    declarations: Iterable[FingerprintMeta] = (),
) -> dict[str, FingerprintMeta]:
    result: dict[str, FingerprintMeta] = {}
    for meta in declarations:
        prior = result.get(meta.fingerprint)
        if prior is not None and prior != meta:
            raise alert_invalid(
                "conflicting fingerprint metadata in one payload",
                fingerprint=meta.fingerprint,
            )
        result[meta.fingerprint] = meta
    for alert in alerts:
        candidate = FingerprintMeta(alert.fingerprint, alert.service, alert.labels)
        prior = result.get(alert.fingerprint)
        if prior is not None and prior != candidate:
            raise alert_invalid(
                "alerts for one fingerprint must use one service and one label set",
                fingerprint=alert.fingerprint,
                expected=prior.payload(),
                actual=candidate.payload(),
            )
        result[alert.fingerprint] = candidate
    return result


def alert_record_canonical(alert: Alert) -> dict[str, Any]:
    return alert.persisted_payload()


def alert_snapshot_checksum(version: int, alerts: Iterable[Alert]) -> str:
    records = sorted((alert_record_canonical(a) for a in alerts), key=lambda x: (x["batch_id"], x["line_no"]))
    return digest({"kind": "alert_snapshot", "version": version, "alerts": records})


def alert_increment_checksum(
    batch_id: str,
    base_version: int,
    version: int,
    alerts: Iterable[Alert],
    affected_fingerprints: Iterable[FingerprintMeta],
) -> str:
    records = sorted((alert_record_canonical(a) for a in alerts), key=lambda x: (x["batch_id"], x["line_no"]))
    fps = sorted((m.payload() for m in affected_fingerprints), key=lambda x: x["fingerprint"])
    return digest(
        {
            "kind": "alert_increment",
            "batch_id": batch_id,
            "base_version": base_version,
            "version": version,
            "alerts": records,
            "affected_fingerprints": fps,
        }
    )


def parse_alert_snapshot(body: Any) -> AlertSnapshot:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "alert_snapshot")
    if kind != "alert_snapshot":
        raise snapshot_invalid("kind must be alert_snapshot", kind=kind)
    try:
        version = _require_int(obj.get("version"), "version", minimum=1)
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="alert snapshot")
    batch_default = obj.get("batch_id")
    if batch_default is not None:
        try:
            batch_default = _require_nonempty_string(batch_default, "batch_id", max_len=256)
        except ValueError as exc:
            raise snapshot_invalid(str(exc)) from exc
    raw_alerts = obj.get("alerts")
    if not isinstance(raw_alerts, list):
        raise snapshot_invalid("alerts must be an array")
    default_batch = batch_default or f"snapshot-v{version}"
    alerts: list[Alert] = []
    seen: dict[str, Alert] = {}
    for index, raw in enumerate(raw_alerts):
        alert = _parse_alert(
            raw,
            index=index,
            default_batch_id=default_batch,
            default_line_no=index + 1,
        )
        prior = seen.get(alert.alert_uid)
        if prior is not None:
            raise alert_invalid("duplicate batch_id/line_no in alert snapshot", alert_uid=alert.alert_uid)
        seen[alert.alert_uid] = alert
        alerts.append(alert)
    fingerprints = _merge_fingerprints(alerts)
    expected = alert_snapshot_checksum(version, alerts)
    verify_checksum(expected, checksum, where="alert snapshot")
    return AlertSnapshot(version, batch_default, checksum, tuple(alerts), fingerprints)


def parse_alert_increment(body: Any, *, verify_checksum_value: bool = True) -> AlertIncrement:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "alert_increment")
    if kind != "alert_increment":
        raise snapshot_invalid("kind must be alert_increment", kind=kind)
    try:
        batch_id = _require_nonempty_string(obj.get("batch_id"), "batch_id", max_len=256)
        base_version = _require_int(obj.get("base_version"), "base_version", minimum=0)
        version = _require_int(obj.get("version"), "version", minimum=1)
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="alert increment")
    raw_alerts = obj.get("alerts", [])
    raw_fps = obj.get("affected_fingerprints", [])
    if not isinstance(raw_alerts, list):
        raise snapshot_invalid("alerts must be an array")
    if not isinstance(raw_fps, list):
        raise snapshot_invalid("affected_fingerprints must be an array")
    if not raw_alerts and not raw_fps:
        raise snapshot_invalid("an alert increment must include alerts or affected_fingerprints")
    alerts: list[Alert] = []
    for index, raw in enumerate(raw_alerts):
        if isinstance(raw, Mapping) and "line_no" in raw:
            raise alert_invalid("increment line_no is assigned by array order and must not be supplied", alert_index=index)
        if isinstance(raw, Mapping) and "batch_id" in raw and raw.get("batch_id") != batch_id:
            raise alert_invalid("increment alert batch_id must match the envelope batch_id", alert_index=index)
        alerts.append(
            _parse_alert(
                raw,
                index=index,
                default_batch_id=batch_id,
                default_line_no=index + 1,
            )
        )
    fps = tuple(parse_fingerprint_meta(raw, index=i) for i, raw in enumerate(raw_fps))
    fingerprints = _merge_fingerprints(alerts, fps)
    expected = alert_increment_checksum(batch_id, base_version, version, alerts, fps)
    if verify_checksum_value:
        verify_checksum(expected, checksum, where="alert increment")
    return AlertIncrement(batch_id, base_version, version, checksum, tuple(alerts), fps)


@dataclass(frozen=True)
class TopologySnapshot:
    version: int
    checksum: str
    nodes: frozenset[str]
    edges: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class TopologyIncrement:
    base_version: int
    version: int
    checksum: str
    upsert_edges: frozenset[tuple[str, str]]
    remove_edges: frozenset[tuple[str, str]]
    upsert_nodes: frozenset[str]
    remove_nodes: frozenset[str]


def _parse_edge(raw: Any, *, where: str) -> tuple[str, str]:
    try:
        if isinstance(raw, Mapping):
            source = raw.get("source", raw.get("from"))
            target = raw.get("target", raw.get("to"))
        elif isinstance(raw, (list, tuple)) and len(raw) == 2:
            source, target = raw
        else:
            raise ValueError("edge must be {source,target} or a two-element array")
        source_s = _require_nonempty_string(source, "edge.source", max_len=256)
        target_s = _require_nonempty_string(target, "edge.target", max_len=256)
        if source_s == target_s:
            raise ValueError("self edges are not permitted")
        return source_s, target_s
    except ValueError as exc:
        raise snapshot_invalid(str(exc), field=where) from exc


def _parse_edges(raw: Any, *, where: str) -> frozenset[tuple[str, str]]:
    if raw is None:
        return frozenset()
    if not isinstance(raw, list):
        raise snapshot_invalid(f"{where} must be an array")
    edges: set[tuple[str, str]] = set()
    for index, item in enumerate(raw):
        edge = _parse_edge(item, where=f"{where}[{index}]")
        if edge in edges:
            raise snapshot_invalid("duplicate edge", edge=list(edge), field=where)
        edges.add(edge)
    return frozenset(edges)


def _parse_nodes(raw: Any, *, where: str) -> frozenset[str]:
    if raw is None:
        return frozenset()
    if not isinstance(raw, list):
        raise snapshot_invalid(f"{where} must be an array")
    nodes: set[str] = set()
    for index, item in enumerate(raw):
        try:
            node = _require_nonempty_string(item, f"{where}[{index}]", max_len=256)
        except ValueError as exc:
            raise snapshot_invalid(str(exc)) from exc
        if node in nodes:
            raise snapshot_invalid("duplicate node", node=node, field=where)
        nodes.add(node)
    return frozenset(nodes)


def _edge_records(edges: Iterable[tuple[str, str]]) -> list[list[str]]:
    return [[source, target] for source, target in sorted(edges)]


def topology_snapshot_checksum(version: int, nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> str:
    return digest(
        {
            "kind": "topology_snapshot",
            "version": version,
            "nodes": sorted(set(nodes)),
            "edges": _edge_records(edges),
        }
    )


def topology_increment_checksum(
    base_version: int,
    version: int,
    upsert_edges: Iterable[tuple[str, str]],
    remove_edges: Iterable[tuple[str, str]],
    upsert_nodes: Iterable[str],
    remove_nodes: Iterable[str],
) -> str:
    return digest(
        {
            "kind": "topology_increment",
            "base_version": base_version,
            "version": version,
            "upsert_edges": _edge_records(upsert_edges),
            "remove_edges": _edge_records(remove_edges),
            "upsert_nodes": sorted(set(upsert_nodes)),
            "remove_nodes": sorted(set(remove_nodes)),
        }
    )


def parse_topology_snapshot(body: Any) -> TopologySnapshot:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "topology_snapshot")
    if kind != "topology_snapshot":
        raise snapshot_invalid("kind must be topology_snapshot", kind=kind)
    try:
        version = _require_int(obj.get("version"), "version", minimum=1)
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="topology snapshot")
    edges = _parse_edges(obj.get("edges", []), where="edges")
    nodes = _parse_nodes(obj.get("nodes", []), where="nodes")
    nodes = frozenset(nodes | {s for s, _ in edges} | {t for _, t in edges})
    expected = topology_snapshot_checksum(version, nodes, edges)
    verify_checksum(expected, checksum, where="topology snapshot")
    return TopologySnapshot(version, checksum, nodes, edges)


def parse_topology_increment(body: Any) -> TopologyIncrement:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "topology_increment")
    if kind != "topology_increment":
        raise snapshot_invalid("kind must be topology_increment", kind=kind)
    try:
        base_version = _require_int(obj.get("base_version"), "base_version", minimum=0)
        version = _require_int(obj.get("version"), "version", minimum=1)
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="topology increment")
    upsert_edges = _parse_edges(obj.get("upsert_edges", []), where="upsert_edges")
    remove_edges = _parse_edges(obj.get("remove_edges", []), where="remove_edges")
    if upsert_edges & remove_edges:
        raise snapshot_invalid("the same edge cannot be both upserted and removed", edges=_edge_records(upsert_edges & remove_edges))
    upsert_nodes = _parse_nodes(obj.get("upsert_nodes", []), where="upsert_nodes")
    remove_nodes = _parse_nodes(obj.get("remove_nodes", []), where="remove_nodes")
    if upsert_nodes & remove_nodes:
        raise snapshot_invalid("the same node cannot be both upserted and removed", nodes=sorted(upsert_nodes & remove_nodes))
    expected = topology_increment_checksum(
        base_version,
        version,
        upsert_edges,
        remove_edges,
        upsert_nodes,
        remove_nodes,
    )
    verify_checksum(expected, checksum, where="topology increment")
    return TopologyIncrement(
        base_version,
        version,
        checksum,
        upsert_edges,
        remove_edges,
        upsert_nodes,
        remove_nodes,
    )


@dataclass(frozen=True)
class SuppressionRule:
    rule_id: str
    scope_service: str
    scope_severity: str | None
    match_labels: dict[str, str]
    window_ms: int
    enabled: bool

    def payload(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "scope": {"service": self.scope_service, "severity": self.scope_severity},
            "match_labels": self.match_labels,
            "window_ms": self.window_ms,
            "enabled": self.enabled,
        }

    def matches(self, service: str, severity: str, labels: Mapping[str, str]) -> bool:
        if not self.enabled:
            return False
        if self.scope_service not in ("*", service):
            return False
        if self.scope_severity is not None and self.scope_severity not in ("*", severity):
            return False
        for key, expected in self.match_labels.items():
            if labels.get(key) != expected:
                return False
        return True


@dataclass(frozen=True)
class RulesSnapshot:
    version: int
    checksum: str
    default_window_ms: int
    rules: dict[str, SuppressionRule]


@dataclass(frozen=True)
class RulesIncrement:
    base_version: int
    version: int
    checksum: str
    default_window_ms: int | None
    upsert_rules: dict[str, SuppressionRule]
    remove_rules: frozenset[str]


def parse_rule(raw: Any, *, index: int) -> SuppressionRule:
    try:
        obj = _require_mapping(raw, f"rules[{index}]")
        rule_id = _require_nonempty_string(obj.get("rule_id"), "rule_id", max_len=256)
        scope = _require_mapping(obj.get("scope", {}), "scope")
        scope_service = scope.get("service", "*")
        if not isinstance(scope_service, str) or not scope_service.strip():
            raise ValueError("scope.service must be a non-empty string")
        severity_raw = scope.get("severity")
        if severity_raw is None or severity_raw == "*":
            severity = None
        else:
            severity = _normalize_severity(severity_raw)
        labels = _require_labels(obj.get("match_labels", {}), "match_labels")
        window_ms = _require_int(
            obj.get("window_ms"),
            "window_ms",
            minimum=0,
            maximum=MAX_WINDOW_MS,
        )
        enabled = obj.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        return SuppressionRule(
            rule_id=rule_id,
            scope_service=scope_service.strip(),
            scope_severity=severity,
            match_labels=labels,
            window_ms=window_ms,
            enabled=enabled,
        )
    except ValueError as exc:
        raise snapshot_invalid(str(exc), rule_index=index) from exc


def rules_snapshot_checksum(version: int, default_window_ms: int, rules: Iterable[SuppressionRule]) -> str:
    return digest(
        {
            "kind": "rules_snapshot",
            "version": version,
            "default_window_ms": default_window_ms,
            "rules": sorted((rule.payload() for rule in rules), key=lambda x: x["rule_id"]),
        }
    )


def rules_increment_checksum(
    base_version: int,
    version: int,
    default_window_ms: int | None,
    upsert_rules: Iterable[SuppressionRule],
    remove_rules: Iterable[str],
) -> str:
    return digest(
        {
            "kind": "rules_increment",
            "base_version": base_version,
            "version": version,
            "default_window_ms": default_window_ms,
            "upsert_rules": sorted((rule.payload() for rule in upsert_rules), key=lambda x: x["rule_id"]),
            "remove_rules": sorted(set(remove_rules)),
        }
    )


def _parse_unique_rules(raw: Any, *, where: str) -> dict[str, SuppressionRule]:
    if not isinstance(raw, list):
        raise snapshot_invalid(f"{where} must be an array")
    rules: dict[str, SuppressionRule] = {}
    for index, item in enumerate(raw):
        rule = parse_rule(item, index=index)
        if rule.rule_id in rules:
            raise snapshot_invalid("duplicate rule_id", rule_id=rule.rule_id, field=where)
        rules[rule.rule_id] = rule
    return rules


def parse_rules_snapshot(body: Any) -> RulesSnapshot:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "rules_snapshot")
    if kind != "rules_snapshot":
        raise snapshot_invalid("kind must be rules_snapshot", kind=kind)
    try:
        version = _require_int(obj.get("version"), "version", minimum=1)
        default_window_ms = _require_int(
            obj.get("default_window_ms", 60_000),
            "default_window_ms",
            minimum=0,
            maximum=MAX_WINDOW_MS,
        )
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="rules snapshot")
    rules = _parse_unique_rules(obj.get("rules", []), where="rules")
    expected = rules_snapshot_checksum(version, default_window_ms, rules.values())
    verify_checksum(expected, checksum, where="rules snapshot")
    return RulesSnapshot(version, checksum, default_window_ms, rules)


def parse_rules_increment(body: Any) -> RulesIncrement:
    obj = _require_mapping(body, "body")
    kind = obj.get("kind", "rules_increment")
    if kind != "rules_increment":
        raise snapshot_invalid("kind must be rules_increment", kind=kind)
    try:
        base_version = _require_int(obj.get("base_version"), "base_version", minimum=0)
        version = _require_int(obj.get("version"), "version", minimum=1)
        default_raw = obj.get("default_window_ms")
        default_window_ms = (
            None
            if default_raw is None
            else _require_int(default_raw, "default_window_ms", minimum=0, maximum=MAX_WINDOW_MS)
        )
    except ValueError as exc:
        raise snapshot_invalid(str(exc)) from exc
    checksum = _checksum_from_body(obj, where="rules increment")
    upserts = _parse_unique_rules(obj.get("upsert_rules", []), where="upsert_rules")
    raw_removes = obj.get("remove_rules", [])
    if not isinstance(raw_removes, list):
        raise snapshot_invalid("remove_rules must be an array")
    removes: set[str] = set()
    for index, item in enumerate(raw_removes):
        try:
            rule_id = _require_nonempty_string(item, f"remove_rules[{index}]", max_len=256)
        except ValueError as exc:
            raise snapshot_invalid(str(exc)) from exc
        if rule_id in removes:
            raise snapshot_invalid("duplicate remove_rules entry", rule_id=rule_id)
        removes.add(rule_id)
    if set(upserts) & removes:
        raise snapshot_invalid("a rule cannot be both upserted and removed")
    expected = rules_increment_checksum(base_version, version, default_window_ms, upserts.values(), removes)
    verify_checksum(expected, checksum, where="rules increment")
    return RulesIncrement(base_version, version, checksum, default_window_ms, upserts, frozenset(removes))
