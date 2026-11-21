"""Stage 4: evidence-chain construction, versioned export and re-import checks."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .config import config_fingerprint, stage_fingerprints
from .errors import FactMergeError
from .util import (
    atomic_write_bytes,
    canonical_json,
    digest_obj,
    digest_without,
    read_json,
)


EVIDENCE_SCHEMA = "2.0"


def stage_content_digests(entities: list[dict[str, Any]]) -> dict[str, str]:
    rows: dict[str, list[list[str]]] = {"stage1": [], "stage2": [], "stage3": [], "stage4": []}
    for entity in entities:
        for claim in entity["claims"]:
            rows["stage1"].append([claim["claim_id"], claim["stage1"]["digest"]])
            rows["stage2"].append([claim["claim_id"], claim["stage2"]["digest"]])
            rows["stage3"].append([claim["claim_id"], claim["stage3"]["digest"]])
        rows["stage4"].append([entity["entity_id"], entity["chain_digest"]])
    return {stage: digest_obj(sorted(values)) for stage, values in rows.items()}


def _ensure_digest(value: dict[str, Any], key: str = "digest") -> dict[str, Any]:
    if key not in value:
        value[key] = digest_without(value, key)
    return value


def _link(from_name: str, to_name: str, source_digest: str, target_digest: str) -> dict[str, Any]:
    link = {
        "from": from_name,
        "to": to_name,
        "source_digest": source_digest,
        "target_digest": target_digest,
    }
    link["link_digest"] = digest_without(link, "link_digest")
    return link


def build_claim_evidence(
    source_claim: dict[str, Any],
    stage1: dict[str, Any],
    stage2: dict[str, Any],
    stage3: dict[str, Any],
) -> dict[str, Any]:
    source = copy.deepcopy(source_claim)
    source["digest"] = digest_without(source, "digest")
    stage1 = _ensure_digest(copy.deepcopy(stage1))
    stage2 = _ensure_digest(copy.deepcopy(stage2))
    stage3 = _ensure_digest(copy.deepcopy(stage3))
    links = [
        _link("source_claim", "stage1", source["digest"], stage1["digest"]),
        _link("stage1", "stage2", stage1["digest"], stage2["digest"]),
        _link("stage2", "stage3", stage2["digest"], stage3["digest"]),
    ]
    evidence_core = {
        "claim_id": source_claim.get("claim_id"),
        "entity_id": source_claim.get("entity_id"),
        "category": stage3.get("category"),
        "source_claim": source,
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3,
        "links": links,
    }
    evidence_core["chain_digest"] = digest_obj(evidence_core)
    return evidence_core


def build_entity_evidence(
    resolution: dict[str, Any],
    stage1_by_claim: dict[str, dict[str, Any]],
    stage2_by_claim: dict[str, dict[str, Any]],
    source_by_claim: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    claims: list[dict[str, Any]] = []
    for decision in resolution["decisions"]:
        claim_id = decision["claim_id"]
        if claim_id not in stage1_by_claim or claim_id not in stage2_by_claim:
            raise FactMergeError(
                "E_EVIDENCE_BROKEN",
                "cannot build evidence chain: a stage payload is missing",
                claim_id=claim_id,
                entity_id=resolution["entity_id"],
                breakpoint="stage1" if claim_id not in stage1_by_claim else "stage2",
            )
        claims.append(
            build_claim_evidence(
                source_by_claim[claim_id],
                stage1_by_claim[claim_id],
                stage2_by_claim[claim_id],
                decision,
            )
        )
    entity = {
        "entity_id": resolution["entity_id"],
        "status": resolution["status"],
        "conflict_detected": resolution["conflict_detected"],
        "decision": {
            "stage3_digest": resolution["digest"],
            "accepted": resolution["accepted"],
            "demoted": resolution["demoted"],
            "categories": resolution["categories"],
        },
        "claims": sorted(claims, key=lambda item: item["claim_id"]),
    }
    entity["chain_digest"] = digest_without(entity, "chain_digest")
    return entity


def finalize_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(manifest)
    result["manifest_id"] = digest_without(result, "manifest_id")
    return result


def export_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return (canonical_json(finalize_manifest(manifest)) + "\n").encode("utf-8")


def export_manifest(manifest: dict[str, Any], path: str | Path) -> None:
    atomic_write_bytes(path, export_manifest_bytes(manifest))


def load_manifest(path: str | Path) -> dict[str, Any]:
    try:
        manifest = read_json(path)
    except (OSError, json.JSONDecodeError) as exc:
        raise FactMergeError("E_INVALID_FORMAT", f"cannot read evidence manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise FactMergeError("E_INVALID_FORMAT", "evidence manifest root must be an object")
    return manifest


def _broken(
    message: str,
    claim: dict[str, Any] | None = None,
    breakpoint: str | None = None,
    **details: Any,
) -> None:
    raise FactMergeError(
        "E_EVIDENCE_BROKEN",
        message,
        claim_id=str(claim.get("claim_id")) if claim and claim.get("claim_id") is not None else None,
        entity_id=str(claim.get("entity_id")) if claim and claim.get("entity_id") is not None else None,
        breakpoint=breakpoint,
        details=details,
    )


def _verify_digest(
    value: dict[str, Any],
    key: str,
    claim: dict[str, Any],
    breakpoint: str,
) -> str:
    if not isinstance(value, dict) or key not in value:
        _broken(f"missing digest in {breakpoint}", claim, breakpoint)
    actual = digest_without(value, key)
    if actual != value[key]:
        _broken(f"digest mismatch in {breakpoint}", claim, breakpoint)
    return actual


def _rebuild_entity_decision(entity: dict[str, Any]) -> dict[str, Any]:
    accepted: list[dict[str, Any]] = []
    demoted: list[dict[str, Any]] = []
    for claim in entity.get("claims", []):
        stage3 = claim.get("stage3")
        if not isinstance(stage3, dict):
            _broken("evidence chain has no stage3 decision", claim, "stage3")
        if stage3.get("decision") == "accepted":
            accepted.append(
                {
                    "category": stage3.get("category"),
                    "claim_id": stage3.get("claim_id"),
                    "value": stage3.get("comparison_value"),
                    "confidence": stage3.get("confidence"),
                    "supporting_claim_ids": stage3.get("supporting_claim_ids", []),
                }
            )
        elif stage3.get("decision") == "demoted":
            demoted.append(
                {
                    "category": stage3.get("category"),
                    "claim_id": stage3.get("claim_id"),
                    "reason_code": stage3.get("reason_code"),
                    "reason": stage3.get("reason"),
                    "winner_claim_id": stage3.get("winner_claim_id"),
                }
            )
        else:
            _broken("unknown stage3 decision", claim, "stage3")
    accepted.sort(key=lambda item: (str(item["category"]), str(item["claim_id"])))
    demoted.sort(key=lambda item: (str(item["category"]), str(item["claim_id"])))
    return {"accepted": accepted, "demoted": demoted}


def validate_manifest(
    manifest: dict[str, Any],
    *,
    expected_config_digest: str | None = None,
) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        _broken("manifest root is not an object", breakpoint="manifest")
    schema = manifest.get("schema_version")
    if schema != EVIDENCE_SCHEMA:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            f"unsupported evidence schema version: {schema!r}; expected {EVIDENCE_SCHEMA!r}",
        )
    config = manifest.get("config")
    if not isinstance(config, dict):
        _broken("manifest has no configuration snapshot", breakpoint="config")
    config_digest = digest_obj(config)
    if manifest.get("config_digest") != config_digest:
        _broken("configuration digest mismatch", breakpoint="config")
    if expected_config_digest is not None and config_digest != expected_config_digest:
        _broken("manifest was built with a different configuration", breakpoint="config")

    version = manifest.get("version", {})
    corpus_digest = version.get("corpus_digest")
    if not corpus_digest:
        _broken("manifest has no corpus digest", breakpoint="version")
    recomputed_fps = stage_fingerprints(config, str(corpus_digest))
    if version.get("stage_fingerprints") != recomputed_fps:
        _broken("stage fingerprints cannot be reproduced from the manifest", breakpoint="version")
    if version.get("config_fingerprint") != config_fingerprint(recomputed_fps):
        _broken("configuration fingerprint mismatch", breakpoint="version")
    content_digests = version.get("stage_content_digests")
    if not isinstance(content_digests, dict) or set(content_digests) != {"stage1", "stage2", "stage3", "stage4"}:
        _broken("stage content digests are incomplete", breakpoint="version")
    if version.get("content_fingerprint") != digest_obj({"stage_content_digests": content_digests}):
        _broken("stage content fingerprint mismatch", breakpoint="version")
    if manifest.get("stage_content_digests") != content_digests:
        _broken("root and versioned stage content digests differ", breakpoint="version")

    entity_reports: list[dict[str, Any]] = []
    for entity in manifest.get("entities", []):
        entity_id = entity.get("entity_id")
        if not entity_id:
            _broken("entity evidence has no entity_id", breakpoint="entity")
        claims = entity.get("claims")
        if not isinstance(claims, list) or not claims:
            _broken("entity evidence has no claims", {"entity_id": entity_id}, "claims")
        seen: set[str] = set()
        for claim in claims:
            claim_id = claim.get("claim_id")
            if not claim_id:
                _broken("claim evidence has no claim_id", {"entity_id": entity_id}, "claim")
            if claim_id in seen:
                _broken("duplicate claim evidence", claim, "claim")
            seen.add(claim_id)
            if claim.get("entity_id") != entity_id:
                _broken("claim belongs to a different entity", claim, "claim")
            for required_stage in ("source_claim", "stage1", "stage2", "stage3"):
                if required_stage not in claim:
                    _broken(f"evidence chain is missing {required_stage}", claim, required_stage)
            source_digest = _verify_digest(claim["source_claim"], "digest", claim, "source_claim")
            stage1_digest = _verify_digest(claim["stage1"], "digest", claim, "stage1")
            stage2_digest = _verify_digest(claim["stage2"], "digest", claim, "stage2")
            stage3_digest = _verify_digest(claim["stage3"], "digest", claim, "stage3")
            if claim["stage2"].get("stage1_digest") != stage1_digest:
                _broken("stage2 does not reference the stage1 digest", claim, "stage2")
            if claim["stage3"].get("stage2_digest") != stage2_digest:
                _broken("stage3 does not reference the stage2 digest", claim, "stage3")
            links = claim.get("links")
            if not isinstance(links, list) or len(links) != 3:
                _broken("evidence links are incomplete", claim, "links")
            expected_targets = [stage1_digest, stage2_digest, stage3_digest]
            expected_sources = [source_digest, stage1_digest, stage2_digest]
            for index, link in enumerate(links):
                if not isinstance(link, dict) or "link_digest" not in link:
                    _broken("link digest is missing", claim, "links")
                if digest_without(link, "link_digest") != link["link_digest"]:
                    _broken("link digest mismatch", claim, "links")
                if link.get("source_digest") != expected_sources[index] or link.get("target_digest") != expected_targets[index]:
                    _broken("evidence link does not match adjacent stage digests", claim, "links")
            if "chain_digest" not in claim or digest_without(claim, "chain_digest") != claim["chain_digest"]:
                _broken("claim evidence chain digest mismatch", claim, "chain")

        rebuilt = _rebuild_entity_decision(entity)
        stored_decision = entity.get("decision", {})
        if sorted(rebuilt["accepted"], key=lambda item: canonical_json(item)) != sorted(
            stored_decision.get("accepted", []), key=lambda item: canonical_json(item)
        ):
            _broken("accepted decisions cannot be rebuilt from evidence", {"entity_id": entity_id}, "stage3")
        if sorted(rebuilt["demoted"], key=lambda item: canonical_json(item)) != sorted(
            stored_decision.get("demoted", []), key=lambda item: canonical_json(item)
        ):
            _broken("demoted decisions cannot be rebuilt from evidence", {"entity_id": entity_id}, "stage3")
        if "chain_digest" not in entity or digest_without(entity, "chain_digest") != entity["chain_digest"]:
            _broken("entity evidence digest mismatch", {"entity_id": entity_id}, "entity")
        entity_reports.append(
            {
                "entity_id": entity_id,
                "claim_count": len(claims),
                "chain_digest": entity["chain_digest"],
            }
        )

    if "manifest_id" not in manifest or digest_without(manifest, "manifest_id") != manifest["manifest_id"]:
        _broken("manifest digest mismatch", breakpoint="manifest")
    return {
        "ok": True,
        "schema_version": EVIDENCE_SCHEMA,
        "manifest_id": manifest["manifest_id"],
        "config_digest": config_digest,
        "entity_count": len(entity_reports),
        "entities": entity_reports,
    }


def rebuild_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    validate_manifest(manifest)
    return finalize_manifest(copy.deepcopy(manifest))


def rebuild_manifest_file(path: str | Path) -> dict[str, Any]:
    return rebuild_manifest(load_manifest(path))
