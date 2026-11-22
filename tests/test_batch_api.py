import pytest

from fastapi.testclient import TestClient

from legal_cite.api import app
from legal_cite.batch import batch_vs_per_contract, recompute_summary
from legal_cite.config import default_config
from legal_cite.errors import E_INCREMENTAL_MISMATCH, LegalCiteError
from legal_cite.legacy_v1_generator import build_legacy_v1
from legal_cite.serde import canonical_text


def test_metrics_and_per_contract_consistency(all_records, gold):
    cfg = default_config()
    summary, state = recompute_summary(all_records, gold, cfg)
    metrics = summary["metrics"]
    assert metrics["conflict_identification_rate"] == 1.0
    assert metrics["resolution_accuracy"] == 1.0
    assert metrics["evidence_rebuild_success_rate"] == 1.0
    consistent = batch_vs_per_contract(all_records, gold, cfg)
    assert canonical_text(consistent) == canonical_text(summary)


def test_incremental_mismatch_is_detected(all_records, gold, monkeypatch):
    cfg = default_config()
    # Corrupt a per-contract outcome to prove the guard fires.
    from legal_cite import batch as batch_mod
    original = batch_mod.run_pipeline

    calls = {"n": 0}

    def flaky(records, *a, **kw):
        state = original(records, *a, **kw)
        calls["n"] += 1
        if calls["n"] == 2 and state["manifest"]["decisions"]:
            target = state["manifest"]["decisions"][0]
            target["adopted_citation_id"] = "CITE-FORCED-DIFF"
        return state

    monkeypatch.setattr(batch_mod, "run_pipeline", flaky)
    with pytest.raises(LegalCiteError) as ei:
        batch_vs_per_contract(all_records, gold, cfg)
    assert ei.value.code == E_INCREMENTAL_MISMATCH
    assert ei.value.details["first_difference_contract"]


def test_http_resolve_and_health(all_records):
    client = TestClient(app)
    assert client.get("/health").json()["bind"] == "127.0.0.1"
    resp = client.post("/resolve", json={"records": all_records})
    assert resp.status_code == 200
    body = resp.json()
    assert body["pipeline_fingerprint"]
    assert body["decisions"]
    # contract filter
    one = client.post("/resolve",
                      json={"records": all_records, "contract_id": "CTR-001"})
    assert one.status_code == 200
    assert all(d["contract_id"] == "CTR-001" for d in one.json()["decisions"])


def test_http_evidence_migration_endpoint():
    client = TestClient(app)
    resp = client.post("/evidence/verify",
                       json={"manifest": build_legacy_v1()})
    assert resp.status_code == 200
    assert resp.json()["migrated"] == 1


def test_http_error_code_serialisation(all_records):
    client = TestClient(app)
    bad = dict(all_records[0])
    bad["citation_span"] = [0, 9999]
    resp = client.post("/resolve", json={"records": [bad]})
    # parse error is captured as a diagnostic, not a 500
    assert resp.status_code == 200
    codes = {d["code"] for d in resp.json()["diagnostics"]}
    assert "E_CITATION_UNPARSED" in codes
