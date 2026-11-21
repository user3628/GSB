"""Deterministic, budget-aware document admission planning."""

from __future__ import annotations

from typing import Any, Iterable

from .errors import EngineError
from .text import split_sentences
from .utils import digest


REQUIRED_DOC_FIELDS = ("doc_id", "title", "body", "language", "source", "target_length")


def document_semantic_view(doc: dict[str, Any]) -> dict[str, Any]:
    return {field: doc.get(field) for field in REQUIRED_DOC_FIELDS}


def document_input_fingerprint(doc: dict[str, Any]) -> str:
    return digest(document_semantic_view(doc))


def validate_documents(documents: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(documents, list):
        raise EngineError("E_INVALID_INPUT", "Documents must be a list")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for index, doc in enumerate(documents):
        if not isinstance(doc, dict):
            raise EngineError("E_INVALID_INPUT", f"Document at index {index} must be an object")
        missing = [field for field in REQUIRED_DOC_FIELDS if field not in doc]
        if missing:
            raise EngineError("E_INVALID_INPUT", f"Document at index {index} missing fields: {', '.join(missing)}")
        doc_id = doc.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id.strip():
            raise EngineError("E_INVALID_INPUT", f"Document at index {index} has an invalid doc_id")
        if doc_id in seen:
            raise EngineError("E_INVALID_INPUT", f"Duplicate doc_id: {doc_id}", doc_id=doc_id)
        seen.add(doc_id)
        if not isinstance(doc.get("body"), str):
            raise EngineError("E_INVALID_INPUT", "Document body must be a string", doc_id=doc_id)
        if not isinstance(doc.get("title"), str):
            raise EngineError("E_INVALID_INPUT", "Document title must be a string", doc_id=doc_id)
        language = doc.get("language")
        if language not in {"zh", "en", "mixed"}:
            raise EngineError(
                "E_INVALID_INPUT",
                "Document language must be one of zh, en, mixed",
                doc_id=doc_id,
            )
        if not isinstance(doc.get("source"), str) or not doc["source"].strip():
            raise EngineError("E_INVALID_INPUT", "Document source must be a non-empty string", doc_id=doc_id)
        target = doc.get("target_length")
        if target not in config["targets"]:
            raise EngineError(
                "E_INVALID_INPUT",
                f"Unknown target_length: {target}",
                doc_id=doc_id,
            )
        normalized.append(document_semantic_view(doc))
    normalized.sort(key=lambda item: item["doc_id"])
    return normalized


def estimate_document(doc: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    sentence_count = len(split_sentences(doc["body"]))
    fusion_evaluations = int(config["fusion"]["fusion_candidate_evaluations"])
    evaluations = max(1, sentence_count) * 3 + fusion_evaluations
    estimated_ms = (
        float(config["budget"]["estimated_overhead_ms"])
        + evaluations * float(config["budget"]["estimated_ms_per_evaluation"])
    )
    return {
        "doc_id": doc["doc_id"],
        "body_chars": len(doc["body"]),
        "sentence_count": sentence_count,
        "target_length": doc["target_length"],
        "estimated_sentence_evaluations": evaluations,
        "estimated_latency_ms": round(estimated_ms, 6),
        "input_fingerprint": document_input_fingerprint(doc),
    }


def _unprocessed(row: dict[str, Any], code: str, reason: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "doc_id": row["doc_id"],
        "code": code,
        "reason": reason,
        "details": details or {},
        "estimated_sentence_evaluations": row["estimated_sentence_evaluations"],
        "estimated_latency_ms": row["estimated_latency_ms"],
        "body_chars": row["body_chars"],
        "sentence_count": row["sentence_count"],
        "target_length": row["target_length"],
        "input_fingerprint": row["input_fingerprint"],
    }


def schedule_documents(documents: Iterable[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    docs = list(documents)
    budget = config["budget"]
    global_budget = int(budget["global_sentence_evaluations"])
    per_worker = int(budget["per_worker_sentence_evaluations"])
    max_workers = int(budget["max_workers"])
    latency_budget = float(budget["latency_budget_ms"])
    overhead = float(budget["estimated_overhead_ms"])
    ms_per_eval = float(budget["estimated_ms_per_evaluation"])
    rows = [estimate_document(doc, config) for doc in docs]
    rows.sort(key=lambda row: (row["estimated_sentence_evaluations"], row["doc_id"]))
    worker_loads = [0 for _ in range(max_workers)]
    worker_docs: list[list[str]] = [[] for _ in range(max_workers)]
    admission_order: list[str] = []
    accepted: list[dict[str, Any]] = []
    unprocessed: list[dict[str, Any]] = []
    used = 0
    for row in rows:
        estimate = int(row["estimated_sentence_evaluations"])
        if estimate > global_budget:
            unprocessed.append(
                _unprocessed(
                    row,
                    "E_GLOBAL_BUDGET",
                    "Document estimate exceeds the global evaluation budget",
                    {"global_budget": global_budget},
                )
            )
            continue
        if estimate > per_worker:
            unprocessed.append(
                _unprocessed(
                    row,
                    "E_WORKER_QUOTA",
                    "Document estimate exceeds a single worker quota",
                    {"per_worker_quota": per_worker},
                )
            )
            continue
        if used + estimate > global_budget:
            unprocessed.append(
                _unprocessed(
                    row,
                    "E_GLOBAL_BUDGET",
                    "Global evaluation budget is exhausted",
                    {"remaining": global_budget - used},
                )
            )
            continue
        available_workers = [
            (worker_loads[index], index)
            for index in range(max_workers)
            if worker_loads[index] + estimate <= per_worker
        ]
        if not available_workers:
            unprocessed.append(
                _unprocessed(
                    row,
                    "E_WORKER_QUOTA",
                    "No worker has enough remaining quota",
                    {"worker_loads": list(worker_loads), "per_worker_quota": per_worker},
                )
            )
            continue
        _, worker_index = min(available_workers)
        projected_loads = list(worker_loads)
        projected_loads[worker_index] += estimate
        projected_latency = overhead + max(projected_loads) * ms_per_eval
        if projected_latency > latency_budget + 1e-12:
            unprocessed.append(
                _unprocessed(
                    row,
                    "E_LATENCY_BUDGET",
                    "Projected admission would exceed the request latency budget",
                    {
                        "projected_latency_ms": round(projected_latency, 6),
                        "latency_budget_ms": latency_budget,
                    },
                )
            )
            continue
        worker_loads[worker_index] = projected_loads[worker_index]
        worker_docs[worker_index].append(row["doc_id"])
        used += estimate
        admission_order.append(row["doc_id"])
        accepted.append(row)

    accepted_ids = sorted(row["doc_id"] for row in accepted)
    grouped = [
        {
            "worker": index,
            "doc_ids": sorted(worker_docs[index]),
            "estimated_evaluations": worker_loads[index],
        }
        for index in range(max_workers)
    ]
    actual_workers = sum(1 for load in worker_loads if load > 0)
    estimated_latency = overhead + max(worker_loads, default=0) * ms_per_eval
    unprocessed.sort(key=lambda item: item["doc_id"])
    return {
        "accepted_doc_ids": accepted_ids,
        "admission_order": admission_order,
        "unprocessed": unprocessed,
        "worker_groups": grouped,
        "worker_count_planned": max(1, actual_workers),
        "global_budget": global_budget,
        "per_worker_quota": per_worker,
        "max_workers": max_workers,
        "accepted_evaluations": used,
        "remaining_evaluations": global_budget - used,
        "estimated_latency_ms": round(estimated_latency, 6),
        "latency_budget_ms": latency_budget,
        "accepted": [dict(row) for row in sorted(accepted, key=lambda item: item["doc_id"])],
    }
