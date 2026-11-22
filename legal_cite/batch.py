"""Batch recomputation, metrics and incremental consistency checks."""

from pathlib import Path

from .config import config_fingerprint
from .errors import E_INCREMENTAL_MISMATCH, LegalCiteError
from .pipeline import run_pipeline
from .serde import (canonical_text, fingerprint, read_jsonl,
                    write_json_atomic)


def _decision_index(state):
    index = {}
    for decision in state["manifest"]["decisions"]:
        index[(decision["contract_id"], decision["locator"])] = decision
    return index


def evaluate(state, gold_records):
    """Compute conflict identification, resolution and evidence metrics."""
    decisions = _decision_index(state)
    gold_conflicts = [g for g in gold_records if g.get("conflict")]
    identified = 0
    correct = 0
    for gold in gold_conflicts:
        locator = f"A{gold['locator_article']}.S{gold['locator_section']}"
        decision = decisions.get((gold["contract_id"], locator))
        if decision is None:
            continue
        if decision["has_conflict"]:
            identified += 1
        if gold.get("expected_manual_review"):
            if decision["needs_manual_review"]:
                correct += 1
        elif decision["adopted_citation_id"] == \
                gold["expected_adopted_citation_id"]:
            correct += 1
    chain_total = len(state["manifest"]["evidence_chains"])
    # A successful rebuild means every chain re-hashes and reconciles.
    from .evidence import verify_manifest
    verify_manifest(state["manifest"])
    rebuilt_ok = chain_total
    metrics = {
        "conflict_identification_rate": _ratio(identified, len(gold_conflicts)),
        "resolution_accuracy": _ratio(correct, len(gold_conflicts)),
        "evidence_rebuild_success_rate": _ratio(rebuilt_ok, chain_total),
        "gold_conflict_groups": len(gold_conflicts),
        "evidence_chains": chain_total,
        "diagnostic_count": len(state["diagnostics"]),
    }
    return metrics


def _ratio(num, den):
    if not den:
        return 0.0
    return round(num / den, 6)


def recompute_summary(records, gold_records, cfg=None):
    cfg = cfg or __import__("legal_cite.config", fromlist=["default_config"]).default_config()
    state = run_pipeline(records, cfg=cfg)
    metrics = evaluate(state, gold_records)
    summary = {
        "config_fingerprint": config_fingerprint(cfg),
        "pipeline_fingerprint": state["pipeline_fingerprint"],
        "metrics": metrics,
    }
    summary["summary_fingerprint"] = fingerprint(summary)
    return summary, state


def batch_vs_per_contract(records, gold_records, cfg=None):
    """Batch run must equal the byte-identical aggregation of per-contract runs."""
    records, _ = __import__("legal_cite.pipeline", fromlist=["_load_shards"])._load_shards(records)
    batch_summary, batch_state = recompute_summary(records, gold_records, cfg)

    contract_ids = sorted({r["contract_id"] for r in records})
    merged_decisions = {}
    merged_chains = {}
    merged_diagnostics = []
    for contract_id in contract_ids:
        subset = [r for r in records if r["contract_id"] == contract_id]
        state = run_pipeline(subset, cfg=cfg)
        for decision in state["manifest"]["decisions"]:
            merged_decisions[(decision["contract_id"],
                              decision["locator"])] = decision
        for chain in state["manifest"]["evidence_chains"]:
            merged_chains[chain["citation_id"]] = chain
        merged_diagnostics.extend(state["diagnostics"])

    batch_decisions = _decision_index(batch_state)
    for key in sorted(set(batch_decisions) | set(merged_decisions)):
        left = canonical_text(batch_decisions.get(key))
        right = canonical_text(merged_decisions.get(key))
        if left != right:
            raise LegalCiteError(
                E_INCREMENTAL_MISMATCH,
                "per-contract aggregation differs from batch decision",
                {"first_difference_contract": key[0], "locator": key[1]},
            )
    for citation_id in sorted(
            set(merged_chains) |
            {c["citation_id"] for c in
             batch_state["manifest"]["evidence_chains"]}):
        left_chain = next((c for c in
                           batch_state["manifest"]["evidence_chains"]
                           if c["citation_id"] == citation_id), None)
        if canonical_text(left_chain) != canonical_text(
                merged_chains.get(citation_id)):
            contract_id = next((r["contract_id"] for r in records
                                if r["citation_id"] == citation_id), None)
            raise LegalCiteError(
                E_INCREMENTAL_MISMATCH,
                "per-contract evidence chain differs from batch chain",
                {"first_difference_contract": contract_id,
                 "citation_id": citation_id},
            )
    # Determinism: two identical runs give byte-identical summaries.
    rerun_summary, _ = recompute_summary(records, gold_records, cfg)
    if canonical_text(rerun_summary) != canonical_text(batch_summary):
        raise LegalCiteError(
            E_INCREMENTAL_MISMATCH,
            "repeated run produced a different summary", {},
        )
    return batch_summary


def write_summary(summary, path):
    write_json_atomic(Path(path), summary, indent=2)
