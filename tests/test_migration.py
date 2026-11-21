from __future__ import annotations

import copy

import pytest

from factmerge.errors import FactMergeError
from factmerge.evidence import export_manifest_bytes, validate_manifest
from factmerge.migration import export_legacy_v1_bytes, migrate_legacy_v1_file


def test_v1_migration_fills_factors_marks_downgrades_and_roundtrips(root, config):
    source = root / "samples" / "legacy_evidence_v1.jsonl"
    manifest = migrate_legacy_v1_file(source, config)
    assert validate_manifest(manifest)["ok"] is True
    assert export_legacy_v1_bytes(manifest) == source.read_bytes()
    assert export_manifest_bytes(manifest).endswith(b"\n")

    claims = {claim["claim_id"]: claim for entity in manifest["entities"] for claim in entity["claims"]}
    assert claims["L001"]["stage2"]["conversion"]["factor"] == "1"
    assert claims["L002"]["stage2"]["conversion"]["factor"] == "100000000"
    assert claims["L003"]["stage2"]["conversion"]["factor"] == "2.589988110336"
    assert claims["L003"]["stage2"]["conversion"]["factor_precision"] == 12
    assert "V1_SINGLE_MONTH_EXPANDED" in claims["L005"]["stage3"]["migration_downgrades"]
    assert claims["L005"]["stage2"]["canonical_value"]["value"] == "2021-04..2021-04[closed,closed]"


def test_v1_unmappable_unit_is_invalid_format(root, config):
    source = root / "samples" / "legacy_evidence_v1_invalid.jsonl"
    with pytest.raises(FactMergeError) as excinfo:
        migrate_legacy_v1_file(source, config)
    error = excinfo.value
    assert error.code == "E_INVALID_FORMAT"
    assert error.claim_id == "L900"
    assert error.details["unit_text"] == "furlongs"


def test_v1_unit_mapping_is_configurable(root, config):
    source = root / "samples" / "legacy_evidence_v1_invalid.jsonl"
    customized = copy.deepcopy(config)
    customized["legacy_v1"]["unit_text_mapping"]["furlongs"] = "sq_mi"
    manifest = migrate_legacy_v1_file(source, customized)
    assert validate_manifest(manifest)["ok"] is True
    claim = manifest["entities"][0]["claims"][0]
    assert claim["claim_id"] == "L900"
    assert claim["stage2"]["conversion"]["factor"] == "2.589988110336"


def test_v1_number_policy_is_configurable(root, config):
    source = root / "samples" / "legacy_evidence_v1.jsonl"
    customized = copy.deepcopy(config)
    customized["legacy_v1"]["number_policy"] = "decimal"
    with pytest.raises(FactMergeError) as excinfo:
        migrate_legacy_v1_file(source, customized)
    assert excinfo.value.code == "E_INVALID_FORMAT"
    assert "comma grouping" in excinfo.value.message
