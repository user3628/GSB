from __future__ import annotations

from typing import Any, Callable

from .config import REEConfig
from .formats import (
    DecodedFormat,
    decode_bio,
    decode_json_intervals,
    decode_legacy_flat_v1,
    encode_bio,
    encode_json_intervals,
    encode_legacy_flat_v1,
)
from .models import DocumentIR, canonical_json, core_digest, normalize_document


def _step(
    name: str,
    before: DocumentIR,
    decoded: DecodedFormat,
) -> dict[str, Any]:
    before_digest = core_digest(before)
    after_digest = core_digest(decoded.document)
    return {
        "step": name,
        "ok": before_digest == after_digest,
        "before_digest": before_digest,
        "after_digest": after_digest,
        "diagnostics": decoded.diagnostics,
        "upgrades": decoded.upgrades,
    }


def _bio_chain(document: DocumentIR) -> dict[str, Any]:
    current = normalize_document(document)
    steps: list[dict[str, Any]] = []

    bio_payload = encode_bio(current)
    decoded = decode_bio(bio_payload)
    steps.append(_step("BIO->IR", current, decoded))
    current = decoded.document

    json_payload = encode_json_intervals(current)
    json_decoded = decode_json_intervals(json_payload)
    steps.append(_step("IR->JSON->IR", current, json_decoded))
    current = json_decoded.document

    bio_payload_2 = encode_bio(current)
    final_decoded = decode_bio(bio_payload_2)
    steps.append(_step("IR->BIO->IR", current, final_decoded))
    current = final_decoded.document

    original = normalize_document(document)
    return {
        "name": "BIO->IR->JSON->IR->BIO",
        "ok": all(step["ok"] for step in steps)
        and core_digest(original) == core_digest(current),
        "original_digest": core_digest(original),
        "final_digest": core_digest(current),
        "final_bio_digest": core_digest(final_decoded.document),
        "final_payload_sha256": __import__("hashlib").sha256(
            canonical_json(bio_payload_2).encode("utf-8")
        ).hexdigest(),
        "steps": steps,
    }


def _legacy_chain(document: DocumentIR, config: REEConfig) -> dict[str, Any]:
    current = normalize_document(document)
    steps: list[dict[str, Any]] = []

    legacy_payload = encode_legacy_flat_v1(current)
    decoded = decode_legacy_flat_v1(legacy_payload, config)
    steps.append(_step("LEGACY_V1->IR", current, decoded))
    current = decoded.document

    json_payload = encode_json_intervals(current)
    json_decoded = decode_json_intervals(json_payload, config)
    steps.append(_step("IR->JSON->IR", current, json_decoded))
    current = json_decoded.document

    legacy_payload_2 = encode_legacy_flat_v1(current)
    final_decoded = decode_legacy_flat_v1(legacy_payload_2, config)
    steps.append(_step("IR->LEGACY_V1->IR", current, final_decoded))
    current = final_decoded.document

    original = normalize_document(document)
    upgrades = sorted(
        {
            upgrade
            for step in steps
            for upgrade in step["upgrades"]
        }
    )
    return {
        "name": "LEGACY_V1->IR->JSON->IR->LEGACY_V1",
        "ok": all(step["ok"] for step in steps)
        and core_digest(original) == core_digest(current),
        "original_digest": core_digest(original),
        "final_digest": core_digest(current),
        "upgrades": upgrades,
        "steps": steps,
    }


def validate_roundtrips(
    documents: list[DocumentIR],
    config: REEConfig,
    *,
    document_hook: Callable[[str], bool] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    checks: list[dict[str, Any]] = []
    for raw_document in sorted(documents, key=lambda document: document.doc_id):
        document = normalize_document(raw_document)
        source_upgrade = document.provenance
        source_upgrades = source_upgrade.get(
            "source_upgrades", source_upgrade.get("upgrades", [])
        )
        doc_checks = [_bio_chain(document), _legacy_chain(document, config)]
        checks.append(
            {
                "doc_id": document.doc_id,
                "source_annotation_version": source_upgrade.get(
                    "source_annotation_version", 2
                ),
                "source_upgrades": source_upgrades,
                "chains": doc_checks,
                "ok": all(chain["ok"] for chain in doc_checks),
            }
        )
        if document_hook is not None and document_hook(document.doc_id):
            return None, {
                "status": "stopped",
                "stage": "validate",
                "doc_id": document.doc_id,
            }
    failure_count = sum(1 for check in checks if not check["ok"])
    chain_count = sum(len(check["chains"]) for check in checks)
    passed_chain_count = sum(
        1 for check in checks for chain in check["chains"] if chain["ok"]
    )
    return {
        "schema_version": 2,
        "ok": failure_count == 0,
        "summary": {
            "document_count": len(checks),
            "chain_count": chain_count,
            "passed_chain_count": passed_chain_count,
            "failure_count": failure_count,
            "upgraded_document_count": sum(
                1 for check in checks if check["source_annotation_version"] == 1
            ),
        },
        "checks": checks,
    }, None
