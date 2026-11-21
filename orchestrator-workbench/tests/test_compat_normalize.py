from __future__ import annotations

import pytest

from app.compat import negotiate
from app.errors import WorkbenchError
from app.models import DecodedPayload, FormatName, NegotiationDecision, RawRecord, RoundingRule, SnapshotHeader
from app.normalize import normalize_records


def _records() -> list[RawRecord]:
    return [
        RawRecord(resource_id="a", parents=[], weight_float=0.1236),
        RawRecord(resource_id="b", parents=["a"], weight_permille=250),
        RawRecord(resource_id="c", parents=[]),
    ]


def test_negotiation_matrix_verdicts():
    binary_payload = DecodedPayload(
        header=SnapshotHeader(format=FormatName.BINARY, format_version=1, snapshot_seq=1,
                              generated_at="t"),
        records=_records(),
    )
    report = negotiate(binary_payload)
    assert report.decision is NegotiationDecision.ACCEPT
    assert report.matrix_rule == "BIN-V1-NATIVE"

    yaml_payload = DecodedPayload(
        header=SnapshotHeader(format=FormatName.YAML, format_version=1, snapshot_seq=1,
                              generated_at="t"),
        records=_records(),
    )
    yaml_report = negotiate(yaml_payload)
    assert yaml_report.decision is NegotiationDecision.DEGRADED_ACCEPT
    assert any(item.field == "weight" for item in yaml_report.degradations)


def test_high_version_with_unknown_required_is_rejected():
    payload = DecodedPayload(
        header=SnapshotHeader(format=FormatName.BINARY, format_version=2, snapshot_seq=1,
                              generated_at="t", unknown_required_fields=["required_x"]),
        records=_records(),
    )
    with pytest.raises(WorkbenchError) as exc:
        negotiate(payload)
    assert exc.value.code == "E_VERSION_UNSUPPORTED"


def test_rounding_rules_and_cumulative_error():
    result = normalize_records(_records(), RoundingRule.HALF_UP)
    by_id = {item.resource_id: item for item in result.resources}
    assert by_id["a"].weight_permille == 124
    assert by_id["b"].weight_permille == 250
    assert by_id["c"].weight_permille == 0
    assert result.rounding[0].discarded_fraction == pytest.approx(-0.0004)
    assert result.cumulative_error == pytest.approx(-0.0004)


def test_half_even_differs_from_half_up():
    records = [RawRecord(resource_id="a", parents=[], weight_float=0.1235)]
    up = normalize_records(records, RoundingRule.HALF_UP).resources[0].weight_permille
    even = normalize_records(records, RoundingRule.HALF_EVEN).resources[0].weight_permille
    assert up == 124
    assert even == 124  # 123.5 -> banker's rounds to even 124
    records2 = [RawRecord(resource_id="a", parents=[], weight_float=0.1245)]
    up2 = normalize_records(records2, RoundingRule.HALF_UP).resources[0].weight_permille
    even2 = normalize_records(records2, RoundingRule.HALF_EVEN).resources[0].weight_permille
    assert up2 == 125
    assert even2 == 124


def test_budget_overflow_and_negative_rejection():
    records = [
        RawRecord(resource_id="ok", parents=[], budget=1),
        RawRecord(resource_id="big", parents=[], budget=0xFFFFFFFF + 1),
        RawRecord(resource_id="neg", parents=[], budget=-1),
    ]
    result = normalize_records(records, RoundingRule.HALF_EVEN)
    rejected_ids = {entry.resource_id for entry in result.rejected}
    assert rejected_ids == {"big", "neg"}
    assert [item.resource_id for item in result.resources] == ["ok"]


def test_duplicate_identifiers_rejected():
    records = [RawRecord(resource_id="dup", parents=[]), RawRecord(resource_id="dup", parents=[])]
    result = normalize_records(records, RoundingRule.HALF_EVEN)
    assert len(result.resources) == 1
    assert result.rejected[0].reason == "duplicate resource_id"
