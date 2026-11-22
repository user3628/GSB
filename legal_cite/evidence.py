"""Stage 4: evidence-chain construction, versioned export/import and migration.

Manifest versions
-----------------
v2: structured integer locator, fixed-point percent, explicit conversion
    factors, full ordered evidence chain with hashed links.
v1: legacy free-text locator, percent strings with thousands separators, no
    conversion factors.  Imported through configurable rules and upgraded to
    v2; every conversion is recorded in the evidence chain.
"""

from .errors import (E_EVIDENCE_BROKEN, E_VERSION_SHAPE_MISMATCH,
                     LegalCiteError)
from .normalize import canonical_locator, cn_to_int, parse_locator, normalize_text
from .serde import fingerprint
from decimal import Decimal

MANIFEST_VERSION = 2
REQUIRED_CHAIN_LINKS = (
    "raw_anchor", "normalization", "locator_parse",
    "element_normalization", "resolution",
)


def _chain_for_citation(parsed, resolution, cfg_fp):
    location = parsed["location"]
    chain = [
        {
            "link": "raw_anchor",
            "raw_text": parsed["raw_text"],
            "citation_span": parsed["citation_span"],
            "citation_text": parsed["raw_text"][
                parsed["citation_span"][0]:parsed["citation_span"][1]],
        },
        {
            "link": "normalization",
            "normalized_text": parsed["normalized_text"],
            "nfkc": True,
            "whitespace_folded": True,
        },
        {
            "link": "locator_parse",
            "locator": location["locator"],
            "article": location["article"],
            "section": location["section"],
            "pattern": location["pattern"],
            "matched_text": location["matched_text"],
            "raw_span": location["raw_span"],
        },
        {
            "link": "element_normalization",
            "config_fingerprint": cfg_fp,
            "elements": parsed["elements_normalized"],
        },
        {
            "link": "resolution",
            "status": resolution["status"],
            "reason": resolution["reason"],
            "scores": resolution["scores"],
        },
    ]
    prev = None
    for link in chain:
        link["prev_hash"] = prev
        link["link_hash"] = fingerprint(
            {k: v for k, v in link.items() if k != "link_hash"})
        prev = link["link_hash"]
    return chain


def build_manifest(pipeline_state, cfg, cfg_fp):
    """Assemble the version-2 evidence manifest from pipeline outputs."""
    parsed_by_id = {p["citation_id"]: p for p in pipeline_state["normalized"]}
    chains = []
    decisions = []
    for group in pipeline_state["resolutions"]:
        for resolution in group["resolutions"]:
            citation_id = resolution["citation_id"]
            parsed = parsed_by_id[citation_id]
            chain = _chain_for_citation(parsed, resolution, cfg_fp)
            chains.append({"citation_id": citation_id,
                           "locator": group["locator"],
                           "chain": chain})
        decisions.append({
            "contract_id": group["contract_id"],
            "locator": group["locator"],
            "has_conflict": group["has_conflict"],
            "distinct_value_signatures": group["distinct_value_signatures"],
            "adopted_citation_id": group["adopted_citation_id"],
            "needs_manual_review": group["needs_manual_review"],
            "review_basis": group["review_basis"],
            "resolutions": group["resolutions"],
        })
    return {
        "manifest_version": MANIFEST_VERSION,
        "config_fingerprint": cfg_fp,
        "pipeline_fingerprint": pipeline_state["pipeline_fingerprint"],
        "diagnostics": pipeline_state["diagnostics"],
        "folded_duplicates": pipeline_state["folded_duplicates"],
        "decisions": decisions,
        "evidence_chains": chains,
    }


def verify_manifest(manifest):
    """Re-hash every chain and cross-check conclusions.

    Raises ``E_EVIDENCE_BROKEN`` naming the first broken link/check.
    """
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise LegalCiteError(
            E_VERSION_SHAPE_MISMATCH,
            "verify_manifest expects a version-2 manifest",
            {"manifest_version": manifest.get("manifest_version")},
        )
    decisions = {(d["contract_id"], d["locator"]): d
                 for d in manifest["decisions"]}
    for entry in manifest["evidence_chains"]:
        chain = entry["chain"]
        present = [link["link"] for link in chain]
        for required in REQUIRED_CHAIN_LINKS:
            if required not in present:
                raise LegalCiteError(
                    E_EVIDENCE_BROKEN,
                    "evidence chain missing intermediate link",
                    {"citation_id": entry["citation_id"],
                     "broken_link": required, "present_links": present},
                )
        prev = None
        for link in chain:
            payload = {k: v for k, v in link.items() if k != "link_hash"}
            if link.get("prev_hash") != prev:
                raise LegalCiteError(
                    E_EVIDENCE_BROKEN, "chain prev_hash discontinuity",
                    {"citation_id": entry["citation_id"],
                     "broken_link": link["link"]})
            actual = fingerprint(payload)
            if actual != link["link_hash"]:
                raise LegalCiteError(
                    E_EVIDENCE_BROKEN, "chain link hash mismatch",
                    {"citation_id": entry["citation_id"],
                     "broken_link": link["link"]})
            prev = actual
        # Conclusion consistency: resolution link must match the decision.
        resolution_link = next(l for l in chain if l["link"] == "resolution")
        locator = entry["locator"]
        matching = [d for d in manifest["decisions"] if d["locator"] == locator]
        if not matching:
            raise LegalCiteError(
                E_EVIDENCE_BROKEN, "no decision for evidence chain locator",
                {"citation_id": entry["citation_id"], "broken_link": "resolution"})
        decision = matching[0]
        res = next((r for r in decision["resolutions"]
                    if r["citation_id"] == entry["citation_id"]), None)
        if res is None:
            raise LegalCiteError(
                E_EVIDENCE_BROKEN, "citation absent from group decision",
                {"citation_id": entry["citation_id"], "broken_link": "resolution"})
        if res["status"] != resolution_link["status"] \
                or res["reason"] != resolution_link["reason"]:
            raise LegalCiteError(
                E_EVIDENCE_BROKEN,
                "resolution link inconsistent with group conclusion",
                {"citation_id": entry["citation_id"], "broken_link": "resolution"})
        if not decision["needs_manual_review"]:
            adopted = decision["adopted_citation_id"]
            statuses = {r["citation_id"]: r["status"]
                        for r in decision["resolutions"]}
            adopted_count = sum(1 for s in statuses.values() if s == "ADOPTED")
            if adopted_count != 1 or statuses.get(adopted) != "ADOPTED":
                raise LegalCiteError(
                    E_EVIDENCE_BROKEN,
                    "group does not contain exactly one adopted citation",
                    {"locator": locator, "broken_link": "resolution"})
    return True


# ---------------------------------------------------------------------------
# v1 -> v2 migration
# ---------------------------------------------------------------------------

def _parse_free_locator(free_text):
    normalized, index_map = normalize_text(free_text)
    return parse_locator(normalized, index_map)


def _fixed_point_percent(text, places):
    cleaned = str(text).replace(",", "").strip()
    dec = Decimal(cleaned)
    quantum = Decimal(1).scaleb(-places)
    return float(dec.quantize(quantum))


def _migrated_status(item, legacy):
    decision = next(d for d in legacy["legacy_decisions"]
                    if item["citation_id"] in d["citation_ids"])
    return "ADOPTED" if item["citation_id"] == decision["legacy_adopted_id"] \
        else "DOWNGRADED"


def _migrated_reason(item, legacy):
    decision = next(d for d in legacy["legacy_decisions"]
                    if item["citation_id"] in d["citation_ids"])
    return "TOP_LOG_SCORE" if item["citation_id"] == decision["legacy_adopted_id"] \
        else "MIGRATED_FROM_V1"


def migrate_v1_to_v2(legacy, cfg, cfg_fp):
    """Import a structure-v1 manifest and return a v2 manifest."""
    if legacy.get("manifest_version") != 1:
        raise LegalCiteError(
            E_VERSION_SHAPE_MISMATCH,
            "migration source must declare manifest_version 1",
            {"manifest_version": legacy.get("manifest_version")},
        )
    required_v1 = {"legacy_decisions", "legacy_evidence"}
    if not required_v1.issubset(legacy):
        raise LegalCiteError(
            E_VERSION_SHAPE_MISMATCH,
            "v1 manifest missing required legacy fields",
            {"missing": sorted(required_v1 - set(legacy))},
        )
    if "evidence_chains" in legacy or "decisions" in legacy:
        raise LegalCiteError(
            E_VERSION_SHAPE_MISMATCH,
            "declared v1 manifest contains v2-shaped fields",
            {"unexpected": [k for k in ("evidence_chains", "decisions")
                            if k in legacy]},
        )
    places = cfg["precision"]["penalty_decimal_places"]
    chains = []
    for item in legacy["legacy_evidence"]:
        locator_info = _parse_free_locator(item["free_locator_text"])
        raw_percent = item["penalty_text"]
        fixed = _fixed_point_percent(raw_percent, places)
        migration_notes = [
            {"step": "locator_parse", "from": item["free_locator_text"],
             "to": locator_info["locator"], "rule": "configured_locator_parser",
             "pattern": locator_info["pattern"]},
            {"step": "penalty_fixed_point", "from": raw_percent,
             "to": fixed, "rule": "strip_thousands_separators_then_quantize",
             "factor": 1.0, "decimal_places": places},
            {"step": "conversion_factor_backfill", "factor": 1.0,
             "rule": "legacy_percent_assumed_canonical_percent"},
        ]
        chain = [
            {"link": "raw_anchor", "raw_text": item.get("raw_text", ""),
             "citation_span": item.get("citation_span", [0, 0]),
             "citation_text": item.get("citation_text", "")},
            {"link": "normalization", "normalized_text":
             normalize_text(item.get("raw_text", ""))[0],
             "nfkc": True, "whitespace_folded": True},
            {"link": "locator_parse", "locator": locator_info["locator"],
             "article": locator_info["article"], "section": locator_info["section"],
             "pattern": locator_info["pattern"],
             "matched_text": locator_info["matched_text"],
             "raw_span": locator_info["raw_span"],
             "migrated_from_free_text": item["free_locator_text"]},
            {"link": "element_normalization", "config_fingerprint": cfg_fp,
             "migration_notes": migration_notes,
             "elements": {"penalty": {"value": fixed, "unit": "percent",
                                      "conversion": {"from_unit": "percent",
                                                     "factor": 1.0,
                                                     "decimal_places": places,
                                                     "raw_value": raw_percent}}}},
            {"link": "resolution",
             "status": _migrated_status(item, legacy),
             "reason": _migrated_reason(item, legacy),
             "scores": item.get("legacy_scores", {})},
        ]
        prev = None
        for link in chain:
            link["prev_hash"] = prev
            link["link_hash"] = fingerprint(
                {k: v for k, v in link.items() if k != "link_hash"})
            prev = link["link_hash"]
        chains.append({"citation_id": item["citation_id"],
                       "locator": locator_info["locator"], "chain": chain})
    decisions = []
    for dec in legacy["legacy_decisions"]:
        free = dec["free_locator_text"]
        locator_info = _parse_free_locator(free)
        resolutions = [
            {"citation_id": cid,
             "status": "ADOPTED" if cid == dec["legacy_adopted_id"]
             else "DOWNGRADED",
             "reason": "MIGRATED_FROM_V1" if cid != dec["legacy_adopted_id"]
             else "TOP_LOG_SCORE",
             "scores": {}, "value_signature": []}
            for cid in dec["citation_ids"]
        ]
        decisions.append({
            "contract_id": dec["contract_id"],
            "locator": locator_info["locator"],
            "adopted_citation_id": dec["legacy_adopted_id"],
            "needs_manual_review": False,
            "review_basis": None,
            "resolutions": resolutions,
        })
    return {
        "manifest_version": MANIFEST_VERSION,
        "config_fingerprint": cfg_fp,
        "pipeline_fingerprint": legacy.get("pipeline_fingerprint",
                                           "migrated:" + fingerprint(legacy)),
        "diagnostics": legacy.get("diagnostics", []),
        "folded_duplicates": legacy.get("folded_duplicates", []),
        "decisions": decisions,
        "evidence_chains": chains,
        "migrated_from_version": 1,
    }


def import_manifest(blob, cfg, cfg_fp):
    """Dispatch import by declared shape and enforce version/shape agreement."""
    version = blob.get("manifest_version")
    if version == MANIFEST_VERSION:
        if "evidence_chains" not in blob or "decisions" not in blob:
            raise LegalCiteError(
                E_VERSION_SHAPE_MISMATCH,
                "declared v2 manifest lacks v2 fields",
                {"manifest_version": version},
            )
        verify_manifest(blob)
        return blob
    if version == 1:
        return migrate_v1_to_v2(blob, cfg, cfg_fp)
    raise LegalCiteError(
        E_VERSION_SHAPE_MISMATCH, "unsupported manifest version",
        {"manifest_version": version},
    )
