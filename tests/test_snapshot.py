from __future__ import annotations

import copy
from decimal import Decimal
from pathlib import Path

import pytest

from app.errors import BusinessError
from app.snapshot import attach_checksum, parse_snapshot, parse_snapshot_text


def test_valid_snapshot_digest_and_components(baseline_payload: dict) -> None:
    snapshot = parse_snapshot(baseline_payload)
    assert snapshot.snapshot_version == 1
    assert snapshot.checksum.startswith("sha256:")
    assert tuple(snapshot.components) == ("api", "cache", "worker")
    assert [component.component_id for component in snapshot.ordered_components] == ["api", "cache", "worker"]


def test_digest_detects_tampering(root: Path) -> None:
    raw = (root / "data" / "baseline.json").read_text(encoding="utf-8")
    tampered_text = raw.replace('"quota": 400', '"quota": 401', 1)
    with pytest.raises(BusinessError) as caught:
        parse_snapshot_text(tampered_text)
    assert caught.value.code == "E_SNAPSHOT_INVALID"


def test_duplicate_json_keys_are_rejected(baseline_payload: dict) -> None:
    text = '{"schema_version":"1.0","schema_version":"1.0"}'
    with pytest.raises(BusinessError) as caught:
        parse_snapshot_text(text)
    assert caught.value.code == "E_SNAPSHOT_INVALID"


def test_component_order_does_not_change_digest(baseline_payload: dict) -> None:
    reordered = dict(baseline_payload)
    reordered["components"] = {key: baseline_payload["components"][key] for key in reversed(list(baseline_payload["components"]))}
    assert attach_checksum(reordered)["checksum"] == baseline_payload["checksum"]


def test_invalid_numeric_contract_is_rejected(baseline_payload: dict) -> None:
    changed = copy.deepcopy(baseline_payload)
    changed["components"]["api"]["ratio"] = Decimal("1.500000")
    changed = attach_checksum(changed)
    with pytest.raises(BusinessError) as caught:
        parse_snapshot(changed)
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    assert "components.api.ratio" in caught.value.message
