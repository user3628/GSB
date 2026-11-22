import pytest

from legal_cite.config import config_fingerprint, default_config
from legal_cite.errors import (E_EVIDENCE_BROKEN, E_VERSION_SHAPE_MISMATCH,
                               LegalCiteError)
from legal_cite.evidence import (MANIFEST_VERSION, import_manifest,
                                 verify_manifest)
from legal_cite.legacy_v1_generator import build_legacy_v1
from legal_cite.pipeline import run_pipeline
from legal_cite.serde import canonical_text


def test_full_chain_structure_and_verification(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    assert verify_manifest(state["manifest"]) is True
    for entry in state["manifest"]["evidence_chains"]:
        links = [link["link"] for link in entry["chain"]]
        assert links == ["raw_anchor", "normalization", "locator_parse",
                         "element_normalization", "resolution"]


def test_tampered_chain_is_broken(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    manifest = state["manifest"]
    manifest["evidence_chains"][0]["chain"][3]["elements"] = {"tampered": True}
    with pytest.raises(LegalCiteError) as ei:
        verify_manifest(manifest)
    assert ei.value.code == E_EVIDENCE_BROKEN
    assert ei.value.details["broken_link"] == "element_normalization"


def test_missing_link_is_broken(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    manifest = state["manifest"]
    manifest["evidence_chains"][0]["chain"].pop(2)
    with pytest.raises(LegalCiteError) as ei:
        verify_manifest(manifest)
    assert ei.value.code == E_EVIDENCE_BROKEN
    assert ei.value.details["broken_link"] == "locator_parse"


def test_v1_migration_and_round_trip():
    cfg = default_config()
    cfp = config_fingerprint(cfg)
    v2 = import_manifest(build_legacy_v1(), cfg, cfp)
    assert v2["manifest_version"] == MANIFEST_VERSION
    assert v2["migrated_from_version"] == 1
    verify_manifest(v2)
    again = import_manifest(v2, cfg, cfp)
    assert canonical_text(again) == canonical_text(v2)
    notes = v2["evidence_chains"][0]["chain"][3]["migration_notes"]
    assert {n["step"] for n in notes} == {
        "locator_parse", "penalty_fixed_point",
        "conversion_factor_backfill"}
    # thousands separator parsed to fixed point percent
    penalty = v2["evidence_chains"][0]["chain"][3]["elements"]["penalty"]
    assert penalty["value"] == 1234.56


def test_version_shape_mismatch():
    cfg = default_config()
    with pytest.raises(LegalCiteError) as ei:
        import_manifest({"manifest_version": 99}, cfg, config_fingerprint(cfg))
    assert ei.value.code == E_VERSION_SHAPE_MISMATCH
    with pytest.raises(LegalCiteError) as ei:
        import_manifest({"manifest_version": 1, "legacy_decisions": []},
                        cfg, config_fingerprint(cfg))
    assert ei.value.code == E_VERSION_SHAPE_MISMATCH
    with pytest.raises(LegalCiteError) as ei:
        import_manifest({"manifest_version": 2, "decisions": []},
                        cfg, config_fingerprint(cfg))
    assert ei.value.code == E_VERSION_SHAPE_MISMATCH
