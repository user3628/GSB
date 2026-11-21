from __future__ import annotations

import copy

import pytest

from factmerge import FactMergeEngine
from factmerge.errors import FactMergeError
from factmerge.evidence import export_manifest_bytes, finalize_manifest, rebuild_manifest, validate_manifest
from factmerge.util import digest_obj


@pytest.fixture()
def run_result(config, rows, expected):
    return FactMergeEngine(config, rows, expected=expected).run(use_state=False)


def test_manifest_rebuild_recreates_same_decision_and_bytes(run_result):
    manifest = run_result["manifest"]
    report = validate_manifest(manifest)
    assert report["ok"] is True
    assert manifest["version"]["content_fingerprint"] == digest_obj(
        {"stage_content_digests": manifest["version"]["stage_content_digests"]}
    )
    rebuilt = rebuild_manifest(manifest)
    assert export_manifest_bytes(rebuilt) == export_manifest_bytes(manifest)
    assert rebuilt["manifest_id"] == manifest["manifest_id"]
    assert [entity["chain_digest"] for entity in rebuilt["entities"]] == [
        entity["chain_digest"] for entity in manifest["entities"]
    ]


def test_missing_stage_reports_claim_and_breakpoint(run_result):
    broken = copy.deepcopy(run_result["manifest"])
    entity = next(item for item in broken["entities"] if item["entity_id"] == "ENT-ATLAS")
    claim = entity["claims"][0]
    claim_id = claim["claim_id"]
    del claim["stage2"]
    with pytest.raises(FactMergeError) as excinfo:
        validate_manifest(broken)
    error = excinfo.value
    assert error.code == "E_EVIDENCE_BROKEN"
    assert error.claim_id == claim_id
    assert error.breakpoint == "stage2"


def test_tampered_link_is_detected(run_result):
    broken = copy.deepcopy(run_result["manifest"])
    claim = broken["entities"][0]["claims"][0]
    claim["links"][1]["target_digest"] = "0" * 64
    with pytest.raises(FactMergeError) as excinfo:
        validate_manifest(broken)
    assert excinfo.value.code == "E_EVIDENCE_BROKEN"
    assert excinfo.value.breakpoint == "links"


def test_claim_evidence_contains_offsets_factor_and_reason(run_result):
    manifest = run_result["manifest"]
    claim = next(
        claim
        for entity in manifest["entities"]
        for claim in entity["claims"]
        if claim["claim_id"] == "C-POP-ATLAS-ZH"
    )
    assert claim["stage1"]["numeric"]["span"]["source_start"] == 0
    assert claim["stage1"]["parse_source"].startswith("raw_value")
    assert claim["stage2"]["conversion"]["factor"] == "100000000"
    assert claim["stage2"]["conversion"]["recompute"]["exact_decimal"] is True
    assert claim["stage3"]["reason_code"] == "ACCEPTED_HIGHEST_CONFIDENCE"
    assert len(claim["links"]) == 3


def test_manifest_id_requires_finalized_manifest(run_result):
    manifest = copy.deepcopy(run_result["manifest"])
    del manifest["manifest_id"]
    with pytest.raises(FactMergeError) as excinfo:
        validate_manifest(manifest)
    assert excinfo.value.code == "E_EVIDENCE_BROKEN"
    finalized = finalize_manifest(manifest)
    assert validate_manifest(finalized)["ok"] is True
