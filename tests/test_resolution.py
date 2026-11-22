from legal_cite.config import default_config
from legal_cite.pipeline import run_pipeline
from legal_cite.serde import read_jsonl


def test_pipeline_determinism_and_single_adoption(all_records):
    cfg = default_config()
    first = run_pipeline(all_records, cfg=cfg)
    second = run_pipeline([dict(r) for r in all_records], cfg=cfg)
    from legal_cite.serde import canonical_text
    assert canonical_text(first["manifest"]) == canonical_text(second["manifest"])
    for decision in first["manifest"]["decisions"]:
        if not decision["needs_manual_review"]:
            adopted = [r for r in decision["resolutions"]
                       if r["status"] == "ADOPTED"]
            assert len(adopted) == 1


def test_diagnostics_cover_both_error_codes(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    codes = {d["code"] for d in state["diagnostics"]}
    assert "E_CITATION_UNPARSED" in codes
    assert "E_ELEMENT_INVALID" in codes
    # invalid citations are retained as diagnostics, never silently dropped
    assert len(state["diagnostics"]) >= 5


def test_manual_review_group(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    review = [d for d in state["manifest"]["decisions"]
              if d["needs_manual_review"]]
    assert len(review) == 1
    decision = review[0]
    assert decision["adopted_citation_id"] is None
    assert decision["review_basis"]["best_log_score"] < \
        default_config()["review"]["min_trust_log_score"]


def test_cross_shard_duplicates_folded(all_records):
    state = run_pipeline(all_records, cfg=default_config())
    assert state["folded_duplicates"]
    folded = state["folded_duplicates"][0]
    assert folded["reason"] == "DUPLICATE_LOWER_SCORE"
    assert folded["kept_shard_id"] != folded["dropped_shard_id"]


def test_tie_break_is_deterministic():
    from legal_cite.resolution import resolve_group
    cfg = default_config()
    base = {
        "contract_id": "C", "location": {"article": 1, "section": 0,
                                         "locator": "A1.S0"},
        "channel": "official_filing", "lang": "en", "issued_date": "2025-12-01",
        "elements_normalized": {"penalty": {"value": 5.0}},
    }
    members = []
    for cid in ("CITE-B", "CITE-A"):
        rec = dict(base)
        rec["citation_id"] = cid
        members.append(rec)
    group = {"contract_id": "C", "locator": "A1.S0", "article": 1,
             "section": 0, "has_conflict": False,
             "distinct_value_signatures": 1, "members": members}
    out = resolve_group(group, cfg)
    assert out["adopted_citation_id"] == "CITE-A"
    assert any(r["reason"] == "TIE_BROKEN_BY_CITATION_ID"
               for r in out["resolutions"])
