"""Four-stage deterministic orchestration engine."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .assembly import run_assembly
from .budget import plan_budget
from .checkpoints import STAGES, STAGE_INDEX, CheckpointStore
from .config import config_section_hashes
from .errors import (
    E_BUDGET_EXHAUSTED,
    E_INTERNAL,
    E_INVALID_STAGE,
    E_LATENCY_BUDGET,
    E_NUMERIC,
    E_QUERY_INVALID,
    E_WORKER,
    EngineError,
)
from .evidence import run_rerank
from .fingerprint import (
    documents_fingerprint,
    normalize_query_input_fingerprint,
    stage_components,
    stage_fingerprint,
    version_descriptor,
)
from .normalize import run_normalization
from .retrieval import prepare_documents, retrieve_subquery
from .util import canonical_json_bytes, sha256_json


STABLE_KEYS = (
    "query_id",
    "status",
    "error",
    "subqueries",
    "candidates",
    "reranked",
    "evidence",
    "discarded_evidence",
    "answer",
    "budget",
    "degradations",
    "version",
    "invalidation",
    "stopped_after_stage",
    "next_stage",
    "completed_stages",
    "last_checkpoint",
    "partial",
)


def stable_projection(result: dict[str, Any]) -> dict[str, Any]:
    projected = {key: result[key] for key in STABLE_KEYS if key in result}
    if "invalidation" in projected and isinstance(projected["invalidation"], dict):
        projected["invalidation"] = {
            "earliest_stage": projected["invalidation"].get("earliest_stage"),
            "reasons": projected["invalidation"].get("reasons", []),
        }
    return projected


def stable_bytes(result: dict[str, Any]) -> bytes:
    return canonical_json_bytes(stable_projection(result))


class Orchestrator:
    def __init__(
        self,
        config: dict[str, Any],
        documents: list[dict[str, Any]],
        workspace: str,
        *,
        mode: str = "parallel",
        stop_event: Any | None = None,
        stop_after_stage: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        force_rebuild: bool = False,
    ) -> None:
        if mode not in {"parallel", "serial"}:
            raise EngineError(E_INTERNAL, "mode must be 'parallel' or 'serial'")
        if stop_after_stage is not None and stop_after_stage not in STAGE_INDEX:
            raise EngineError(E_INVALID_STAGE, "unknown stop_after_stage", details={"stage": stop_after_stage})
        self.config = config
        self.documents = documents
        self.prepared_documents = prepare_documents(documents)
        self.workspace = workspace
        self.mode = mode
        self.stop_event = stop_event
        self.stop_after_stage = stop_after_stage
        self.clock = clock
        self.force_rebuild = force_rebuild
        self.store = CheckpointStore(workspace)
        self.config_hashes = config_section_hashes(config)
        self.documents_hash = documents_fingerprint(documents)
        self.budget_version = self.config_hashes["budget"][:16]

    def _elapsed_ms(self, started: float) -> float:
        return round(max(0.0, (self.clock() - started) * 1000.0), 6)

    def _check_latency(
        self,
        *,
        started: float,
        query_id: str,
        stage: str,
        accepted_count: int,
        timing: dict[str, float],
        partial: dict[str, Any] | None = None,
    ) -> None:
        elapsed = self._elapsed_ms(started)
        timing[stage] = elapsed
        budget = float(self.config["budget"]["latency_budget_ms"])
        if elapsed > budget:
            raise EngineError(
                E_LATENCY_BUDGET,
                "request latency budget exceeded after an indivisible stage",
                query_id=query_id,
                details={
                    "stage": stage,
                    "elapsed_ms": elapsed,
                    "latency_budget_ms": budget,
                    "accepted_subquery_count": accepted_count,
                },
                partial=partial or {},
            )

    def _stop_requested(self, stage: str) -> bool:
        if self.stop_after_stage == stage:
            return True
        return bool(self.stop_event is not None and self.stop_event.is_set())

    def _can_reuse_stage(self, ledger: dict[str, Any] | None, stage: str) -> bool:
        if self.force_rebuild or ledger is None:
            return False
        return STAGE_INDEX[stage] < int(ledger.get("cursor", 0))

    def _invalidation_for(
        self,
        *,
        stage: str,
        old_record: dict[str, Any] | None,
        current_components: dict[str, str],
        current_fingerprint: str,
    ) -> dict[str, Any] | None:
        if old_record is None:
            return {
                "stage": stage,
                "reason": "missing_checkpoint",
                "changed_components": [],
                "previous_stage_fingerprint": None,
                "current_stage_fingerprint": current_fingerprint,
            }
        previous_components = old_record.get("component_fingerprints", {})
        changed = sorted(
            key
            for key in set(previous_components) | set(current_components)
            if previous_components.get(key) != current_components.get(key)
        )
        if old_record.get("stage_fingerprint") == current_fingerprint and not changed:
            return None
        return {
            "stage": stage,
            "reason": "fingerprint_mismatch" if changed else "checkpoint_fingerprint_mismatch",
            "changed_components": changed,
            "previous_stage_fingerprint": old_record.get("stage_fingerprint"),
            "current_stage_fingerprint": current_fingerprint,
        }

    def _publish(
        self,
        *,
        query_id: str,
        stage: str,
        input_fingerprint: str,
        components: dict[str, str],
        fingerprint: str,
        product_summary: dict[str, Any],
        product: dict[str, Any],
        invalidation: dict[str, Any],
        epoch_started: bool,
    ) -> tuple[dict[str, Any], bool]:
        stage_index = STAGE_INDEX[stage]
        ledger = self.store.read_ledger(query_id)
        if ledger is not None and stage_index < int(ledger["cursor"]):
            if not epoch_started:
                reasons = invalidation.get("reasons", [])
                if not reasons:
                    reasons = [
                        {
                            "stage": stage,
                            "reason": "checkpoint_cursor_rewind",
                            "changed_components": [],
                        }
                    ]
                self.store.begin_epoch(query_id, stage, reasons)
                epoch_started = True
        record = self.store.append(
            query_id=query_id,
            stage=stage,
            input_fingerprint=input_fingerprint,
            component_fingerprints=components,
            stage_fingerprint=fingerprint,
            budget_config_version=self.budget_version,
            product_summary=product_summary,
            product=product,
        )
        return record, epoch_started

    def _run_channel(
        self,
        query_id: str,
        channel: int,
        subqueries: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for subquery in subqueries:
            try:
                retrieved = retrieve_subquery(
                    subquery,
                    self.prepared_documents,
                    self.config["retrieval"],
                    query_id,
                )
                results.append(
                    {
                        "subquery_index": subquery["index"],
                        "subquery_text": subquery["text"],
                        "priority": subquery["priority"],
                        "status": "accepted",
                        "channel": channel,
                        "candidate_count": retrieved["candidate_count"],
                        "candidates": retrieved["candidates"],
                    }
                )
            except EngineError as exc:
                error_record = exc.to_dict()
                error_record["subquery_index"] = subquery["index"]
                errors.append(error_record)
                results.append(
                    {
                        "subquery_index": subquery["index"],
                        "subquery_text": subquery["text"],
                        "priority": subquery["priority"],
                        "status": "error",
                        "channel": channel,
                        "candidate_count": 0,
                        "candidates": [],
                        "error": error_record,
                    }
                )
            except Exception as exc:  # noqa: BLE001 - converted to structured error
                error_record = {
                    "code": E_WORKER,
                    "message": "retrieval worker raised an unexpected exception",
                    "query_id": query_id,
                    "subquery_index": subquery["index"],
                    "details": {"exception_type": type(exc).__name__, "exception": str(exc)},
                }
                errors.append(error_record)
                results.append(
                    {
                        "subquery_index": subquery["index"],
                        "subquery_text": subquery["text"],
                        "priority": subquery["priority"],
                        "status": "error",
                        "channel": channel,
                        "candidate_count": 0,
                        "candidates": [],
                        "error": error_record,
                    }
                )
        return results, errors

    def _execute_dispatch(
        self,
        *,
        query_id: str,
        normalized: dict[str, Any],
        budget: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        subqueries_by_index = {item["index"]: item for item in normalized["subqueries"]}
        accepted_by_channel: dict[int, list[dict[str, Any]]] = {}
        for accepted in budget["accepted"]:
            subquery = subqueries_by_index[accepted["subquery_index"]]
            accepted_by_channel.setdefault(accepted["channel"], []).append(subquery)
        for items in accepted_by_channel.values():
            items.sort(key=lambda item: (item["priority"], item["index"]))

        all_results: list[dict[str, Any]] = []
        all_errors: list[dict[str, Any]] = []
        channel_items = sorted(accepted_by_channel.items())
        if self.mode == "serial":
            for channel, items in channel_items:
                results, errors = self._run_channel(query_id, channel, items)
                all_results.extend(results)
                all_errors.extend(errors)
        else:
            with ThreadPoolExecutor(max_workers=max(1, len(channel_items))) as executor:
                futures = [
                    executor.submit(self._run_channel, query_id, channel, items)
                    for channel, items in channel_items
                ]
                for future in futures:
                    results, errors = future.result()
                    all_results.extend(results)
                    all_errors.extend(errors)

        result_by_index = {item["subquery_index"]: item for item in all_results}
        for rejected in budget["rejected"]:
            subquery = subqueries_by_index[rejected["subquery_index"]]
            result_by_index[subquery["index"]] = {
                "subquery_index": subquery["index"],
                "subquery_text": subquery["text"],
                "priority": subquery["priority"],
                "status": "rejected",
                "channel": None,
                "reason": rejected["reason"],
                "candidate_count": 0,
                "candidates": [],
            }
        ordered_results = sorted(
            result_by_index.values(), key=lambda item: (item["priority"], item["subquery_index"])
        )
        all_errors.sort(key=lambda item: (int(item.get("subquery_index", 0)), item.get("code", "")))
        return ordered_results, all_errors

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        started = self.clock()
        query_id = request.get("query_id")
        diagnostics: dict[str, Any] = {
            "mode": self.mode,
            "timing_ms": {},
            "cache_usage": {},
            "checkpoint_writes": [],
        }
        stage_fps: dict[str, str] = {}
        invalidation: dict[str, Any] = {
            "earliest_stage": None,
            "reasons": [],
            "reused_stages": [],
            "rebuilt_stages": [],
        }
        normalized: dict[str, Any] | None = None
        dispatch_product: dict[str, Any] | None = None
        rerank_product: dict[str, Any] | None = None
        assembly_product: dict[str, Any] | None = None
        accepted_count = 0
        epoch_started = False

        if not isinstance(query_id, str) or not query_id:
            error = EngineError(
                E_QUERY_INVALID,
                "query_id is required",
                query_id=query_id if isinstance(query_id, str) else None,
                subquery_index=0,
                details={"reason": "missing_query_id"},
            )
            return self._error_response(error, diagnostics, stage_fps, invalidation)

        ledger_snapshot = self.store.read_ledger(query_id)

        try:
            normalize_input = normalize_query_input_fingerprint(request)
            normalize_components = stage_components(
                "normalize",
                config_hashes=self.config_hashes,
                input_fingerprint=normalize_input,
            )
            normalize_fp = stage_fingerprint(normalize_components)
            stage_fps["normalize"] = normalize_fp
            normalize_record = (
                self.store.find_matching(query_id, "normalize", normalize_fp)
                if self._can_reuse_stage(ledger_snapshot, "normalize")
                else None
            )
            if normalize_record is not None:
                normalized = normalize_record["product"]
                diagnostics["cache_usage"]["normalize"] = {
                    "status": "reused",
                    "seq": normalize_record["seq"],
                    "stage_seq": normalize_record["stage_seq"],
                }
                invalidation["reused_stages"].append("normalize")
            else:
                normalized = run_normalization(request, self.config)
                if ledger_snapshot is not None and STAGE_INDEX["normalize"] < int(ledger_snapshot["cursor"]):
                    old = self.store.latest_record(query_id, "normalize")
                    reason = self._invalidation_for(
                        stage="normalize",
                        old_record=old,
                        current_components=normalize_components,
                        current_fingerprint=normalize_fp,
                    )
                    if reason is not None:
                        invalidation["reasons"].append(reason)
                        if invalidation["earliest_stage"] is None:
                            invalidation["earliest_stage"] = "normalize"
                diagnostics["cache_usage"]["normalize"] = {"status": "rebuilt"}
            self._check_latency(
                started=started,
                query_id=query_id,
                stage="normalize",
                accepted_count=accepted_count,
                timing=diagnostics["timing_ms"],
                partial={"completed_stage": "normalize", "subqueries": normalized["subqueries"]},
            )
            if diagnostics["cache_usage"]["normalize"]["status"] == "rebuilt":
                if (
                    ledger_snapshot is not None
                    and STAGE_INDEX["normalize"] >= int(ledger_snapshot["cursor"])
                    and not invalidation["reasons"]
                ):
                    invalidation["rebuilt_stages"].append("normalize")
                record, epoch_started = self._publish(
                    query_id=query_id,
                    stage="normalize",
                    input_fingerprint=normalize_input,
                    components=normalize_components,
                    fingerprint=normalize_fp,
                    product_summary={
                        "normalized_query": normalized["normalized_query"],
                        "subquery_count": len(normalized["subqueries"]),
                    },
                    product=normalized,
                    invalidation=invalidation,
                    epoch_started=epoch_started,
                )
                diagnostics["checkpoint_writes"].append({"stage": "normalize", "seq": record["seq"]})
                if "normalize" not in invalidation["rebuilt_stages"]:
                    invalidation["rebuilt_stages"].append("normalize")
            if self._stop_requested("normalize"):
                return self._stopped_response(
                    query_id, "normalize", diagnostics, stage_fps, invalidation, normalized=normalized
                )

            dispatch_input = sha256_json(
                {
                    "upstream_stage_fingerprint": normalize_fp,
                    "product_sha256": sha256_json(normalized),
                }
            )
            dispatch_components = stage_components(
                "dispatch",
                config_hashes=self.config_hashes,
                input_fingerprint=dispatch_input,
                documents_hash=self.documents_hash,
            )
            dispatch_fp = stage_fingerprint(dispatch_components)
            stage_fps["dispatch"] = dispatch_fp
            dispatch_record = (
                self.store.find_matching(query_id, "dispatch", dispatch_fp)
                if self._can_reuse_stage(ledger_snapshot, "dispatch")
                else None
            )
            if dispatch_record is not None:
                dispatch_product = dispatch_record["product"]
                diagnostics["cache_usage"]["dispatch"] = {
                    "status": "reused",
                    "seq": dispatch_record["seq"],
                    "stage_seq": dispatch_record["stage_seq"],
                }
                invalidation["reused_stages"].append("dispatch")
            else:
                budget = plan_budget(
                    normalized["subqueries"],
                    len(self.documents),
                    self.config["budget"],
                )
                accepted_count = budget["accepted_count"]
                if budget["accepted_count"] == 0:
                    raise EngineError(
                        E_BUDGET_EXHAUSTED,
                        "no subquery was admitted under the three-part budget",
                        query_id=query_id,
                        details={
                            "accepted_subquery_count": 0,
                            "global_quota": budget["global_quota"],
                            "per_channel_quota": budget["per_channel_quota"],
                        },
                        partial={"budget": budget, "rejected": budget["rejected"]},
                    )
                subquery_results, worker_errors = self._execute_dispatch(
                    query_id=query_id,
                    normalized=normalized,
                    budget=budget,
                )
                dispatch_product = {
                    "query_id": query_id,
                    "budget": budget,
                    "subquery_results": subquery_results,
                    "worker_errors": worker_errors,
                }
                if worker_errors:
                    first = worker_errors[0]
                    raise EngineError(
                        first.get("code", E_WORKER),
                        first.get("message", "retrieval worker failed"),
                        query_id=query_id,
                        subquery_index=first.get("subquery_index"),
                        doc_id=first.get("doc_id"),
                        details={
                            "worker_errors": worker_errors,
                            "completed_subqueries": [
                                {
                                    "subquery_index": item["subquery_index"],
                                    "candidate_count": item["candidate_count"],
                                }
                                for item in subquery_results
                                if item["status"] == "accepted"
                            ],
                        },
                        partial={
                            "budget": budget,
                            "subquery_results": subquery_results,
                            "worker_errors": worker_errors,
                        },
                    )
                if ledger_snapshot is not None and STAGE_INDEX["dispatch"] < int(ledger_snapshot["cursor"]):
                    old = self.store.latest_record(query_id, "dispatch")
                    reason = self._invalidation_for(
                        stage="dispatch",
                        old_record=old,
                        current_components=dispatch_components,
                        current_fingerprint=dispatch_fp,
                    )
                    if reason is not None:
                        invalidation["reasons"].append(reason)
                        if invalidation["earliest_stage"] is None:
                            invalidation["earliest_stage"] = "dispatch"
                diagnostics["cache_usage"]["dispatch"] = {"status": "rebuilt"}

            accepted_count = int(dispatch_product["budget"]["accepted_count"])
            self._check_latency(
                started=started,
                query_id=query_id,
                stage="dispatch",
                accepted_count=accepted_count,
                timing=diagnostics["timing_ms"],
                partial={
                    "budget": dispatch_product["budget"],
                    "subquery_results": dispatch_product["subquery_results"],
                },
            )
            if diagnostics["cache_usage"]["dispatch"]["status"] == "rebuilt":
                record, epoch_started = self._publish(
                    query_id=query_id,
                    stage="dispatch",
                    input_fingerprint=dispatch_input,
                    components=dispatch_components,
                    fingerprint=dispatch_fp,
                    product_summary={
                        "accepted_count": dispatch_product["budget"]["accepted_count"],
                        "rejected_count": dispatch_product["budget"]["rejected_count"],
                        "global_used": dispatch_product["budget"]["global_used"],
                        "candidate_count": sum(
                            item["candidate_count"] for item in dispatch_product["subquery_results"]
                        ),
                    },
                    product=dispatch_product,
                    invalidation=invalidation,
                    epoch_started=epoch_started,
                )
                diagnostics["checkpoint_writes"].append({"stage": "dispatch", "seq": record["seq"]})
                if "dispatch" not in invalidation["rebuilt_stages"]:
                    invalidation["rebuilt_stages"].append("dispatch")
            if self._stop_requested("dispatch"):
                return self._stopped_response(
                    query_id,
                    "dispatch",
                    diagnostics,
                    stage_fps,
                    invalidation,
                    normalized=normalized,
                    dispatch_product=dispatch_product,
                )

            rerank_input = sha256_json(
                {
                    "upstream_stage_fingerprint": dispatch_fp,
                    "product_sha256": sha256_json(dispatch_product),
                }
            )
            rerank_components = stage_components(
                "rerank",
                config_hashes=self.config_hashes,
                input_fingerprint=rerank_input,
            )
            rerank_fp = stage_fingerprint(rerank_components)
            stage_fps["rerank"] = rerank_fp
            rerank_record = (
                self.store.find_matching(query_id, "rerank", rerank_fp)
                if self._can_reuse_stage(ledger_snapshot, "rerank")
                else None
            )
            if rerank_record is not None:
                rerank_product = rerank_record["product"]
                diagnostics["cache_usage"]["rerank"] = {
                    "status": "reused",
                    "seq": rerank_record["seq"],
                    "stage_seq": rerank_record["stage_seq"],
                }
                invalidation["reused_stages"].append("rerank")
            else:
                rerank_product = run_rerank(normalized, dispatch_product, self.config)
                if ledger_snapshot is not None and STAGE_INDEX["rerank"] < int(ledger_snapshot["cursor"]):
                    old = self.store.latest_record(query_id, "rerank")
                    reason = self._invalidation_for(
                        stage="rerank",
                        old_record=old,
                        current_components=rerank_components,
                        current_fingerprint=rerank_fp,
                    )
                    if reason is not None:
                        invalidation["reasons"].append(reason)
                        if invalidation["earliest_stage"] is None:
                            invalidation["earliest_stage"] = "rerank"
                diagnostics["cache_usage"]["rerank"] = {"status": "rebuilt"}
            self._check_latency(
                started=started,
                query_id=query_id,
                stage="rerank",
                accepted_count=accepted_count,
                timing=diagnostics["timing_ms"],
                partial={
                    "budget": dispatch_product["budget"],
                    "evidence_count": len(rerank_product["evidence"]),
                },
            )
            if diagnostics["cache_usage"]["rerank"]["status"] == "rebuilt":
                record, epoch_started = self._publish(
                    query_id=query_id,
                    stage="rerank",
                    input_fingerprint=rerank_input,
                    components=rerank_components,
                    fingerprint=rerank_fp,
                    product_summary={
                        "evidence_count": len(rerank_product["evidence"]),
                        "used_chars": rerank_product["evidence_budget"]["used_chars"],
                        "discarded_count": len(rerank_product["discarded_evidence"]),
                    },
                    product=rerank_product,
                    invalidation=invalidation,
                    epoch_started=epoch_started,
                )
                diagnostics["checkpoint_writes"].append({"stage": "rerank", "seq": record["seq"]})
                if "rerank" not in invalidation["rebuilt_stages"]:
                    invalidation["rebuilt_stages"].append("rerank")
            if self._stop_requested("rerank"):
                return self._stopped_response(
                    query_id,
                    "rerank",
                    diagnostics,
                    stage_fps,
                    invalidation,
                    normalized=normalized,
                    dispatch_product=dispatch_product,
                    rerank_product=rerank_product,
                )

            assembly_input = sha256_json(
                {
                    "upstream_stage_fingerprint": rerank_fp,
                    "product_sha256": sha256_json(rerank_product),
                }
            )
            assembly_components = stage_components(
                "assemble",
                config_hashes=self.config_hashes,
                input_fingerprint=assembly_input,
            )
            assembly_fp = stage_fingerprint(assembly_components)
            stage_fps["assemble"] = assembly_fp
            assembly_record = (
                self.store.find_matching(query_id, "assemble", assembly_fp)
                if self._can_reuse_stage(ledger_snapshot, "assemble")
                else None
            )
            if assembly_record is not None:
                assembly_product = assembly_record["product"]
                diagnostics["cache_usage"]["assemble"] = {
                    "status": "reused",
                    "seq": assembly_record["seq"],
                    "stage_seq": assembly_record["stage_seq"],
                }
                invalidation["reused_stages"].append("assemble")
            else:
                assembly_product = run_assembly(normalized, rerank_product, self.config)
                if ledger_snapshot is not None and STAGE_INDEX["assemble"] < int(ledger_snapshot["cursor"]):
                    old = self.store.latest_record(query_id, "assemble")
                    reason = self._invalidation_for(
                        stage="assemble",
                        old_record=old,
                        current_components=assembly_components,
                        current_fingerprint=assembly_fp,
                    )
                    if reason is not None:
                        invalidation["reasons"].append(reason)
                        if invalidation["earliest_stage"] is None:
                            invalidation["earliest_stage"] = "assemble"
                diagnostics["cache_usage"]["assemble"] = {"status": "rebuilt"}
            self._check_latency(
                started=started,
                query_id=query_id,
                stage="assemble",
                accepted_count=accepted_count,
                timing=diagnostics["timing_ms"],
                partial={
                    "budget": dispatch_product["budget"],
                    "answer_sha256": assembly_product["answer"]["answer_sha256"],
                },
            )
            if diagnostics["cache_usage"]["assemble"]["status"] == "rebuilt":
                record, epoch_started = self._publish(
                    query_id=query_id,
                    stage="assemble",
                    input_fingerprint=assembly_input,
                    components=assembly_components,
                    fingerprint=assembly_fp,
                    product_summary={
                        "answer_sha256": assembly_product["answer"]["answer_sha256"],
                        "citation_count": len(assembly_product["answer"]["citations"]),
                        "evidence_count": assembly_product["answer"]["evidence_count"],
                    },
                    product=assembly_product,
                    invalidation=invalidation,
                    epoch_started=epoch_started,
                )
                diagnostics["checkpoint_writes"].append({"stage": "assemble", "seq": record["seq"]})
                if "assemble" not in invalidation["rebuilt_stages"]:
                    invalidation["rebuilt_stages"].append("assemble")

            status = "degraded" if dispatch_product["budget"]["degraded"] else "ok"
            result = self._success_response(
                query_id=query_id,
                status=status,
                normalized=normalized,
                dispatch_product=dispatch_product,
                rerank_product=rerank_product,
                assembly_product=assembly_product,
                stage_fps=stage_fps,
                invalidation=invalidation,
                diagnostics=diagnostics,
            )
            self._finalize_result(result)
            return result
        except EngineError as exc:
            if exc.query_id is None:
                exc.query_id = query_id
            return self._error_response(exc, diagnostics, stage_fps, invalidation)
        except Exception as exc:  # noqa: BLE001 - stable business envelope
            error = EngineError(
                E_INTERNAL,
                "unexpected orchestrator failure",
                query_id=query_id,
                details={"exception_type": type(exc).__name__, "exception": str(exc)},
            )
            return self._error_response(error, diagnostics, stage_fps, invalidation)

    def _version(self, stage_fps: dict[str, str]) -> dict[str, Any]:
        return version_descriptor(self.config, stage_fps)

    def _success_response(
        self,
        *,
        query_id: str,
        status: str,
        normalized: dict[str, Any],
        dispatch_product: dict[str, Any],
        rerank_product: dict[str, Any],
        assembly_product: dict[str, Any],
        stage_fps: dict[str, str],
        invalidation: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "query_id": query_id,
            "status": status,
            "subqueries": normalized["subqueries"],
            "normalization": {
                "raw_query": normalized["raw_query"],
                "normalized_query": normalized["normalized_query"],
                "query_tokens": normalized["query_tokens"],
                "expanded_query_terms": normalized["expanded_query_terms"],
                "synonym_matches": normalized["synonym_matches"],
            },
            "candidates": dispatch_product["subquery_results"],
            "reranked": rerank_product["subquery_results"],
            "evidence": rerank_product["evidence"],
            "discarded_evidence": rerank_product["discarded_evidence"],
            "answer": assembly_product["answer"],
            "budget": dispatch_product["budget"],
            "degradations": dispatch_product["budget"]["rejected"],
            "version": self._version(stage_fps),
            "invalidation": invalidation,
            "diagnostics": diagnostics,
        }

    def _stopped_response(
        self,
        query_id: str,
        stage: str,
        diagnostics: dict[str, Any],
        stage_fps: dict[str, str],
        invalidation: dict[str, Any],
        *,
        normalized: dict[str, Any] | None = None,
        dispatch_product: dict[str, Any] | None = None,
        rerank_product: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        index = STAGE_INDEX[stage]
        ledger = self.store.read_ledger(query_id)
        result: dict[str, Any] = {
            "query_id": query_id,
            "status": "stopped",
            "stopped_after_stage": stage,
            "next_stage": STAGES[index + 1] if index + 1 < len(STAGES) else None,
            "completed_stages": list(STAGES[: index + 1]),
            "last_checkpoint": ledger,
            "version": self._version(stage_fps),
            "invalidation": invalidation,
            "diagnostics": diagnostics,
        }
        partial: dict[str, Any] = {"completed_stage": stage}
        if normalized is not None:
            partial["subqueries"] = normalized["subqueries"]
        if dispatch_product is not None:
            partial["budget"] = dispatch_product["budget"]
            partial["candidates"] = dispatch_product["subquery_results"]
        if rerank_product is not None:
            partial["evidence"] = rerank_product["evidence"]
        result["partial"] = partial
        self._finalize_result(result)
        return result

    def _error_response(
        self,
        error: EngineError,
        diagnostics: dict[str, Any],
        stage_fps: dict[str, str],
        invalidation: dict[str, Any],
    ) -> dict[str, Any]:
        diagnostics["timing_ms"].setdefault("error", diagnostics["timing_ms"].get("last_stage", 0.0))
        result = {
            "query_id": error.query_id,
            "status": "error",
            "error": error.to_dict(),
            "partial": error.partial,
            "version": self._version(stage_fps),
            "invalidation": invalidation,
            "diagnostics": diagnostics,
        }
        self._finalize_result(result)
        return result

    def _finalize_result(self, result: dict[str, Any]) -> None:
        stable = stable_projection(result)
        digest = sha256_json(stable)
        result["diagnostics"]["stable_sha256"] = digest
        query_id = result.get("query_id")
        if isinstance(query_id, str) and query_id:
            try:
                self.store.write_version_snapshot(query_id, result.get("version", {}))
                self.store.write_output(query_id, stable, result.get("diagnostics", {}))
            except OSError:
                pass

    def run_all(self, requests: list[dict[str, Any]]) -> dict[str, Any]:
        ordered = sorted(requests, key=lambda item: str(item.get("query_id", "")))
        results = [self.run(request) for request in ordered]
        projections = [stable_projection(result) for result in results]
        summary_sha256 = sha256_json(projections)
        return {
            "status": "ok" if all(result["status"] in {"ok", "degraded", "stopped"} for result in results) else "partial",
            "mode": self.mode,
            "request_count": len(results),
            "results": results,
            "stable_projections": projections,
            "summary_sha256": summary_sha256,
        }
