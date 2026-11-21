"""Process-pool entry point and per-document staged pipeline."""

from __future__ import annotations

from typing import Any

from .candidates import (
    generate_compressed,
    generate_extractive,
    generate_template,
)
from .errors import EngineError
from .fusion import fuse_candidates
from .text import key_points, prepare_sentences
from .utils import digest


ROUTE_STAGES = ("extract", "compress", "template")
ALL_STAGES = ("extract", "compress", "template", "fusion", "final")


def build_final_product(
    doc: dict[str, Any],
    fusion: dict[str, Any],
    edit: dict[str, Any] | None,
    *,
    fusion_fingerprint: str,
    final_fingerprint: str,
) -> dict[str, Any]:
    winner = fusion.get("winner")
    if winner is None:
        raise EngineError(
            "E_NO_AVAILABLE_CANDIDATE",
            "All three candidates are unavailable for this document",
            doc_id=doc["doc_id"],
            details={"candidates": fusion.get("candidates", [])},
        )
    edit_version = int(edit.get("version", 0)) if edit else 0
    needs_review = bool(edit and edit.get("base_fusion_fingerprint") != fusion_fingerprint)
    summary = str(edit["text"]) if edit else str(winner["text"])
    return {
        "doc_id": doc["doc_id"],
        "summary": summary,
        "summary_digest": digest(summary),
        "summary_source": "human_edit" if edit else winner["route"],
        "candidate_route": winner["route"],
        "candidate_id": winner["candidate_id"],
        "candidate_summary": winner["text"],
        "fusion_score": winner["fusion_score"],
        "human_edited": bool(edit),
        "edit_version": edit_version,
        "needs_review": needs_review,
        "review_reason": "INPUT_OR_CONFIG_CHANGED_SINCE_EDIT" if needs_review else None,
        "fusion_fingerprint": fusion_fingerprint,
        "stage_fingerprint": final_fingerprint,
    }


def _bundle_result(
    doc: dict[str, Any],
    job: dict[str, Any],
    products: dict[str, dict[str, Any]],
    *,
    reused_stages: list[str],
    evaluation_count: int,
) -> dict[str, Any]:
    changed = [stage for stage in ALL_STAGES if stage not in reused_stages]
    final = products["final"]
    fusion = products["fusion"]
    return {
        "ok": True,
        "doc_id": doc["doc_id"],
        "input_fingerprint": job["input_fingerprint"],
        "config_version": job["config_version"],
        "schedule_fingerprint": job["schedule_fingerprint"],
        "edit_version": int(job.get("edit", {}).get("version", 0)) if job.get("edit") else 0,
        "stage_fingerprints": dict(job["stage_fingerprints"]),
        "products": products,
        "reused_stages": [stage for stage in ALL_STAGES if stage in reused_stages],
        "changed_stages": changed,
        "evaluation_count": int(evaluation_count),
        "final": final,
        "fusion": fusion,
    }


def run_document_job(job: dict[str, Any]) -> dict[str, Any]:
    """Compute one document as an indivisible scheduling unit."""
    doc = job["doc"]
    config = job["config"]
    stage_fingerprints = job["stage_fingerprints"]
    cached: dict[str, dict[str, Any]] = {
        str(name): product for name, product in job.get("cached_stages", {}).items() if name in ALL_STAGES
    }
    products: dict[str, dict[str, Any]] = dict(cached)
    reused_stages = [stage for stage in ALL_STAGES if stage in cached]
    evaluation_count = 0
    sentences: list[dict[str, Any]] | None = None
    points: list[dict[str, Any]] | None = None

    def context() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        nonlocal sentences, points
        if sentences is None or points is None:
            sentences = prepare_sentences(doc, config)
            points = key_points(doc, sentences)
        return sentences, points

    if "extract" not in products:
        sentence_list, key_point_list = context()
        extract = generate_extractive(doc, sentence_list, key_point_list, config)
        extract["key_points"] = key_point_list
        extract["sentence_count"] = len(sentence_list)
        products["extract"] = extract
        evaluation_count += len(sentence_list)

    extract_product = products["extract"]
    if "compress" not in products:
        sentence_list, key_point_list = context()
        compressed = generate_compressed(doc, sentence_list, key_point_list, config, extract_product)
        compressed["key_points"] = key_point_list
        products["compress"] = compressed
        evaluation_count += len(sentence_list)

    if "template" not in products:
        sentence_list, key_point_list = context()
        template = generate_template(doc, sentence_list, key_point_list, config, extract_product)
        template["key_points"] = key_point_list
        products["template"] = template
        evaluation_count += len(sentence_list)

    if "fusion" not in products:
        candidate_bundle = {
            "doc_id": doc["doc_id"],
            "candidates": [products[name] for name in ROUTE_STAGES],
            "key_points": products["extract"].get("key_points", []),
        }
        products["fusion"] = fuse_candidates(doc, candidate_bundle, config)
        evaluation_count += len(candidate_bundle["candidates"])

    if "final" not in products:
        products["final"] = build_final_product(
            doc,
            products["fusion"],
            job.get("edit"),
            fusion_fingerprint=stage_fingerprints["fusion"],
            final_fingerprint=stage_fingerprints["final"],
        )

    return _bundle_result(
        doc,
        job,
        products,
        reused_stages=reused_stages,
        evaluation_count=evaluation_count,
    )


def safe_worker_entry(job: dict[str, Any]) -> dict[str, Any]:
    """Never let a single document failure escape as a pool-killing exception."""
    doc_id = str(job.get("doc", {}).get("doc_id", ""))
    try:
        return run_document_job(job)
    except EngineError as exc:
        return {
            "ok": False,
            "doc_id": doc_id,
            "error": exc.to_dict()["error"],
            "error_type": "EngineError",
        }
    except BaseException as exc:  # pragma: no cover - exercised with injected failures
        return {
            "ok": False,
            "doc_id": doc_id,
            "error": {
                "code": "E_WORKER_FAILURE",
                "message": f"{type(exc).__name__}: {exc}",
                "doc_id": doc_id,
            },
            "error_type": type(exc).__name__,
        }
