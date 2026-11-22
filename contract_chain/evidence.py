"""Stage 5: versioned evidence-list construction and reconstructable re-import."""
from __future__ import annotations

from .codec import digest, read_json, stable_sorted, write_json
from .errors import EvidenceBrokenError

REQUIRED_LINKS = (
    "region_coordinate",
    "raw_text_span",
    "location_parse",
    "term_normalization",
    "resolution",
)


def build_evidence(parse_product, normalize_product, resolve_product) -> dict:
    """Assemble a self-contained, versioned manifest covering every adjudicated ref."""
    norm_by_ref = {item["ref_id"]: item for item in normalize_product["results"]}
    group_by_winner = {}
    group_of_ref = {}
    demotion_by_ref = {item["ref_id"]: item for item in resolve_product["demotions"]}
    for group in resolve_product["groups"]:
        group_by_winner[group["winner_ref_id"]] = group
        for ref_id in group["ref_ids"]:
            group_of_ref[ref_id] = group

    records = []
    for entry in parse_product["entries"]:
        ref_id = entry["ref_id"]
        group = group_of_ref.get(ref_id)
        if group is None:
            verdict = "excluded_parse_error"
        elif ref_id == group["winner_ref_id"]:
            verdict = "adopted"
        else:
            verdict = "demoted"
        normalized = norm_by_ref.get(ref_id)
        term_factors = {}
        for term_name, term in sorted((normalized or {}).get("normalized_terms", {}).items()):
            if term["kind"] == "duration_interval":
                term_factors[term_name] = {
                    "lower": term["lower"],
                    "upper": term["upper"],
                    "lower_open": term["lower_open"],
                    "upper_open": term["upper_open"],
                }
            else:
                term_factors[term_name] = {"point": term["point"]}
        record = {
            "ref_id": ref_id,
            "contract_id": entry["contract_id"],
            "verdict": verdict,
            "group_id": group["group_id"] if group else None,
            "links": {
                "region_coordinate": {
                    "page_id": entry["page_id"],
                    "region_id": entry["region_id"],
                    "gt_xywh": entry["region_binding"]["gt_xywh"],
                    "page_width": entry["region_binding"]["page_width"],
                    "page_height": entry["region_binding"]["page_height"],
                    "matched": entry["region_binding"]["matched"],
                    "match": entry["region_binding"]["match"],
                },
                "raw_text_span": {
                    "raw_text": entry["raw_text"],
                    "normalized_text": entry["normalized_text"],
                    "location_candidates": entry["location_candidates"],
                    "parse_error": entry.get("parse_error"),
                },
                "location_parse": {
                    "preferred_location": entry["preferred_location"],
                    "candidate_count": len(entry["location_candidates"]),
                    "sources": [candidate["source"] for candidate in entry["location_candidates"]],
                },
                "term_normalization": {
                    "normalized_terms": term_factors,
                    "term_errors": (normalized or {}).get("term_errors", []),
                },
                "resolution": {
                    "group": group,
                    "demotion": demotion_by_ref.get(ref_id),
                    "score": group["scores"].get(ref_id) if group else None,
                },
            },
        }
        records.append(record)

    records = stable_sorted(records, key=lambda item: item["ref_id"])
    manifest = {
        "manifest_version": 1,
        "kind": "contract_chain_evidence_manifest",
        "record_count": len(records),
        "records": records,
    }
    manifest["manifest_digest"] = digest(
        {key: value for key, value in manifest.items() if key != "manifest_digest"}
    )
    return manifest


def verify_links(manifest: dict) -> None:
    """Ensure every evidence link is physically present; no inferred facts allowed."""
    missing = []
    for record in manifest.get("records", []):
        links = record.get("links")
        if not isinstance(links, dict):
            missing.append({"ref_id": record.get("ref_id"), "link": "links"})
            continue
        for name in REQUIRED_LINKS:
            link = links.get(name)
            if link is None:
                missing.append({"ref_id": record.get("ref_id"), "link": name})
                continue
            if not link:
                missing.append({"ref_id": record.get("ref_id"), "link": name})
        coordinate = links.get("region_coordinate") or {}
        if coordinate and not coordinate.get("gt_xywh"):
            missing.append({"ref_id": record.get("ref_id"), "link": "region_coordinate.gt_xywh"})
    if missing:
        raise EvidenceBrokenError(
            "evidence chain is missing required links",
            {"missing": missing, "missing_link_names": sorted({item["link"] for item in missing})},
        )


def export_manifest(manifest: dict, path) -> str:
    verify_links(manifest)
    write_json(path, manifest)
    return path


def import_manifest(path) -> dict:
    manifest = read_json(path)
    verify_links(manifest)
    stored = manifest.get("manifest_digest")
    actual = digest({key: value for key, value in manifest.items() if key != "manifest_digest"})
    if stored != actual:
        raise EvidenceBrokenError(
            "manifest digest mismatch", {"stored": stored, "actual": actual}
        )
    return manifest


def reconstruct_decisions(manifest: dict) -> dict:
    """Rebuild the resolution verdict set from the imported evidence list only."""
    verify_links(manifest)
    verdicts = {}
    for record in manifest["records"]:
        group = record["links"]["resolution"].get("group")
        verdicts[record["ref_id"]] = {
            "verdict": record["verdict"],
            "group_id": record["group_id"],
            "winner_ref_id": group["winner_ref_id"] if group else None,
            "conflicting_terms": group["conflicting_terms"] if group else [],
            "demotion_reason": (record["links"]["resolution"].get("demotion") or {}).get("reason"),
            "region_gt_xywh": record["links"]["region_coordinate"]["gt_xywh"],
            "page_id": record["links"]["region_coordinate"]["page_id"],
            "region_id": record["links"]["region_coordinate"]["region_id"],
        }
    return {
        ref_id: verdicts[ref_id] for ref_id in sorted(verdicts)
    }


def compare_reconstruction(manifest: dict, resolve_product: dict) -> bool:
    rebuilt = reconstruct_decisions(manifest)
    winners = {group["winner_ref_id"]: group for group in resolve_product["groups"]}
    demoted = {item["ref_id"] for item in resolve_product["demotions"]}
    for ref_id, verdict in rebuilt.items():
        if ref_id in winners:
            expected = "adopted"
        elif ref_id in demoted:
            expected = "demoted"
        else:
            expected = "excluded_parse_error"
        if verdict["verdict"] != expected:
            return False
    return set(rebuilt) == {record["ref_id"] for record in manifest["records"]}
