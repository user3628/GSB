"""Orchestration: scheduling, parallel execution, checkpoints and incrementality."""

from __future__ import annotations

import copy
import json
import multiprocessing
import time
from pathlib import Path
from typing import Any

from .config import config_from_file, config_version as compute_config_version, normalize_config
from .errors import EngineError
from .samples import load_documents as load_documents_file
from .scheduler import (
    document_input_fingerprint,
    schedule_documents,
    validate_documents,
)
from .store import StateStore
from .utils import atomic_write_bytes, atomic_write_json, canonical_bytes, digest, read_json, sha256_bytes
from .worker import ALL_STAGES, ROUTE_STAGES, build_final_product, safe_worker_entry


STAGE_ORDER = ALL_STAGES
ROUTE_CLOSURE: dict[str, set[str]] = {
    "extract": {"extract", "compress", "template", "fusion", "final"},
    "compress": {"compress", "fusion", "final"},
    "template": {"template", "fusion", "final"},
    "fusion": {"fusion", "final"},
    "final": {"final"},
}


def _load_config_file(path: Path) -> dict[str, Any]:
    try:
        if path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8") as handle:
                return normalize_config(json.load(handle))
        return config_from_file(path)
    except EngineError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise EngineError("E_CONFIG", f"Cannot load configuration {path}: {exc}") from exc


def _read_documents_file(path: Path) -> list[dict[str, Any]]:
    try:
        return load_documents_file(path)
    except FileNotFoundError as exc:
        raise EngineError("E_INPUT_NOT_FOUND", f"Document shard file not found: {path}") from exc
    except EngineError:
        raise
    except OSError as exc:
        raise EngineError("E_INPUT_NOT_FOUND", f"Cannot read document shard file {path}: {exc}") from exc


class SummaryEngine:
    def __init__(
        self,
        workspace: str | Path,
        *,
        documents_path: str | Path | None = None,
        config_path: str | Path | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.documents_path = Path(documents_path).resolve() if documents_path else self.workspace / "samples" / "documents.jsonl"
        if config_path is not None:
            self.config_path = Path(config_path).resolve()
        elif config is None:
            pointer = read_json(self.workspace / "state" / "active_config.json", None)
            pointed = Path(str(pointer.get("config_path"))) if isinstance(pointer, dict) and pointer.get("config_path") else None
            self.config_path = pointed if pointed and pointed.exists() else self.workspace / "samples" / "config.yaml"
        else:
            self.config_path = self.workspace / "samples" / "config.yaml"
        self._config_override = normalize_config(config) if config is not None else None
        self.store = StateStore(self.workspace)
        self.store.initialize()

    @classmethod
    def with_samples(
        cls,
        workspace: str | Path,
        *,
        documents_path: str | Path | None = None,
        config_path: str | Path | None = None,
    ) -> "SummaryEngine":
        workspace = Path(workspace).resolve()
        return cls(
            workspace,
            documents_path=Path(documents_path).resolve() if documents_path else None,
            config_path=Path(config_path).resolve() if config_path else None,
        )

    def _load_config(self) -> dict[str, Any]:
        if self._config_override is not None:
            return copy.deepcopy(self._config_override)
        return _load_config_file(self.config_path)

    def get_config(self) -> dict[str, Any]:
        return self._load_config()

    def _load_documents(self) -> list[dict[str, Any]]:
        return _read_documents_file(self.documents_path)

    def load_inputs(self, config: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        configuration = normalize_config(config) if config is not None else self._load_config()
        documents = validate_documents(self._load_documents(), configuration)
        return documents, configuration

    def save_config(self, raw_config: dict[str, Any]) -> dict[str, Any]:
        configuration = normalize_config(raw_config)
        target = self.workspace / "samples" / "config.json"
        atomic_write_json(target, configuration)
        atomic_write_json(self.workspace / "state" / "active_config.json", {"config_path": str(target)})
        self.config_path = target
        self._config_override = None
        return configuration

    def import_documents(self, documents: list[dict[str, Any]], *, path: str | Path | None = None) -> dict[str, Any]:
        configuration = self._load_config()
        normalized = validate_documents(documents, configuration)
        target = Path(path).resolve() if path else self.documents_path
        payload = b"".join(canonical_bytes(doc) + b"\n" for doc in normalized)
        atomic_write_bytes(target, payload)
        self.documents_path = target
        return {
            "documents_path": str(target),
            "document_count": len(normalized),
            "digest": sha256_bytes(payload),
        }

    def compute_stage_fingerprints(
        self,
        doc: dict[str, Any],
        config: dict[str, Any],
        edit: dict[str, Any] | None,
    ) -> dict[str, str]:
        input_fp = document_input_fingerprint(doc)
        target_cfg = config["targets"][doc["target_length"]]
        schedule_fp = digest(
            {
                "schema": "schedule.v1",
                "input": input_fp,
                "budget": config["budget"],
                "runtime": config["runtime"],
                "fusion_candidate_evaluations": config["fusion"]["fusion_candidate_evaluations"],
            }
        )
        extract_fp = digest(
            {
                "schema": "extract.v1",
                "schedule": schedule_fp,
                "input": input_fp,
                "extractor": config["extractor"],
                "target": target_cfg,
            }
        )
        compress_fp = digest(
            {
                "schema": "compress.v1",
                "schedule": schedule_fp,
                "input": input_fp,
                "compression": config["compression"],
                "target": target_cfg,
                "extract": extract_fp,
            }
        )
        template_fp = digest(
            {
                "schema": "template.v1",
                "schedule": schedule_fp,
                "input": input_fp,
                "template": config["template"],
                "target": target_cfg,
                "extract": extract_fp,
            }
        )
        fusion_fp = digest(
            {
                "schema": "fusion.v1",
                "schedule": schedule_fp,
                "input": input_fp,
                "target": target_cfg,
                "extract": extract_fp,
                "compress": compress_fp,
                "template": template_fp,
                "fusion": config["fusion"],
            }
        )
        edit_version = int(edit.get("version", 0)) if edit else 0
        edit_text_digest = digest(edit["text"]) if edit else digest(None)
        final_fp = digest(
            {
                "schema": "final.v1",
                "fusion": fusion_fp,
                "edit_version": edit_version,
                "edit_text_digest": edit_text_digest,
            }
        )
        return {
            "schedule": schedule_fp,
            "extract": extract_fp,
            "compress": compress_fp,
            "template": template_fp,
            "fusion": fusion_fp,
            "final": final_fp,
        }

    def version_fingerprint(
        self,
        doc: dict[str, Any],
        config: dict[str, Any],
        edit: dict[str, Any] | None,
    ) -> str:
        fingerprints = self.compute_stage_fingerprints(doc, config, edit)
        return digest(
            {
                "schema": "version.v1",
                "config": compute_config_version(config),
                "input": document_input_fingerprint(doc),
                "stages": fingerprints,
            }
        )

    def _latest_checkpoints(self) -> dict[tuple[str, str], dict[str, Any]]:
        return self.store.latest_checkpoints()

    def cache_info(
        self,
        doc: dict[str, Any],
        config: dict[str, Any],
        edit: dict[str, Any] | None,
        latest: dict[tuple[str, str], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        fingerprints = self.compute_stage_fingerprints(doc, config, edit)
        if latest is None:
            latest = self._latest_checkpoints()
        product_state = self.store.load_products(doc["doc_id"])
        cached: dict[str, Any] = {}
        previous: dict[str, str] = {}
        previous_schedule = None
        if product_state:
            previous_schedule = product_state.get("schedule_fingerprint")
            for stage in STAGE_ORDER:
                entry = product_state.get("stages", {}).get(stage)
                record = latest.get((doc["doc_id"], stage))
                if entry:
                    previous[stage] = str(entry.get("fingerprint"))
                    if (
                        record
                        and str(record.get("stage_fingerprint")) == str(entry.get("fingerprint"))
                        and str(entry.get("fingerprint")) == fingerprints[stage]
                        and str(record.get("product_digest")) == str(entry.get("product_digest"))
                        and digest(entry.get("product")) == str(entry.get("product_digest"))
                    ):
                        cached[stage] = entry["product"]
        for stage in STAGE_ORDER:
            record = latest.get((doc["doc_id"], stage))
            if record and stage not in previous:
                previous[stage] = str(record.get("stage_fingerprint"))
        affected = self.affected_stages(previous, fingerprints, previous_schedule, fingerprints["schedule"])
        earliest_affected = next((stage for stage in STAGE_ORDER if stage in affected), None)
        return {
            "fingerprints": fingerprints,
            "cached_stages": cached,
            "previous_fingerprints": previous,
            "affected_stages": affected,
            "earliest_affected_stage": earliest_affected,
            "previous_schedule_fingerprint": previous_schedule,
            "product_state": product_state,
        }

    @staticmethod
    def affected_stages(
        previous: dict[str, str],
        current: dict[str, str],
        previous_schedule: str | None = None,
        current_schedule: str | None = None,
    ) -> list[str]:
        if not previous:
            return list(STAGE_ORDER)
        if previous_schedule is not None and current_schedule is not None and previous_schedule != current_schedule:
            return list(STAGE_ORDER)
        affected: set[str] = set()
        for stage in ("extract", "compress", "template", "fusion", "final"):
            if previous.get(stage) != current.get(stage):
                affected.update(ROUTE_CLOSURE[stage])
        return [stage for stage in STAGE_ORDER if stage in affected]

    def _make_job(
        self,
        doc: dict[str, Any],
        config: dict[str, Any],
        edit: dict[str, Any] | None,
        info: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "doc": doc,
            "config": config,
            "input_fingerprint": document_input_fingerprint(doc),
            "config_version": compute_config_version(config),
            "schedule_fingerprint": info["fingerprints"]["schedule"],
            "stage_fingerprints": {
                stage: info["fingerprints"][stage] for stage in STAGE_ORDER
            },
            "edit": (
                {
                    "version": int(edit["version"]),
                    "text": edit["text"],
                    "base_fusion_fingerprint": edit["base_fusion_fingerprint"],
                }
                if edit
                else None
            ),
            "cached_stages": info["cached_stages"],
        }

    def _result_view(self, result: dict[str, Any]) -> dict[str, Any]:
        final = result["final"]
        fusion = result["fusion"]
        fingerprints = result["stage_fingerprints"]
        return {
            "doc_id": result["doc_id"],
            "ok": True,
            "input_fingerprint": result["input_fingerprint"],
            "config_version": result["config_version"],
            "edit_version": result["edit_version"],
            "version_fingerprint": digest(
                {
                    "input": result["input_fingerprint"],
                    "config_version": result["config_version"],
                    "stages": fingerprints,
                }
            ),
            "stage_fingerprints": fingerprints,
            "reused_stages": result["reused_stages"],
            "changed_stages": result["changed_stages"],
            "evaluations_used": result["evaluation_count"],
            "summary": final["summary"],
            "final": final,
            "fusion": fusion,
            "error": None,
        }

    def _persist_result(self, result: dict[str, Any]) -> None:
        doc_id = result["doc_id"]
        if not result.get("ok"):
            return
        if not result.get("changed_stages"):
            return
        fingerprints = result["stage_fingerprints"]
        stage_entries: dict[str, dict[str, Any]] = {}
        for stage in STAGE_ORDER:
            product = result["products"][stage]
            stage_entries[stage] = {
                "fingerprint": fingerprints[stage],
                "edit_version": int(result["edit_version"]),
                "product": product,
            }
        self.store.save_products(
            doc_id,
            input_fingerprint=result["input_fingerprint"],
            config_version=result["config_version"],
            schedule_fingerprint=result["schedule_fingerprint"],
            stages=stage_entries,
        )
        records: list[dict[str, Any]] = []
        for stage in STAGE_ORDER:
            if stage not in result["changed_stages"]:
                continue
            product = result["products"][stage]
            records.append(
                {
                    "stage": stage,
                    "doc_id": doc_id,
                    "input_fingerprint": result["input_fingerprint"],
                    "stage_fingerprint": fingerprints[stage],
                    "config_version": result["config_version"],
                    "schedule_fingerprint": result["schedule_fingerprint"],
                    "edit_version": int(result["edit_version"]),
                    "product_digest": digest(product),
                    "needs_review": bool(result["final"].get("needs_review")),
                }
            )
        if records:
            self.store.publish_records(records, expected_doc_id=doc_id)

    @staticmethod
    def _worker_exception_result(doc_id: str, exc: BaseException) -> dict[str, Any]:
        return {
            "ok": False,
            "doc_id": doc_id,
            "error": {
                "code": "E_WORKER_FAILURE",
                "message": f"{type(exc).__name__}: {exc}",
                "doc_id": doc_id,
            },
        }

    def _serial_execute(
        self,
        jobs: list[dict[str, Any]],
        deadline: float,
        stop_after_docs: int | None,
        initial_completed: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        results: dict[str, dict[str, Any]] = {}
        latency_error = False
        for job in sorted(jobs, key=lambda item: item["doc"]["doc_id"]):
            if self.store.stop_requested():
                break
            if time.monotonic() > deadline:
                latency_error = True
                break
            try:
                result = safe_worker_entry(job)
            except BaseException as exc:  # defensive; safe_worker_entry normally absorbs
                result = self._worker_exception_result(job["doc"]["doc_id"], exc)
            results[result["doc_id"]] = result
            if result.get("ok"):
                try:
                    self._persist_result(result)
                except EngineError as exc:
                    result = {"ok": False, "doc_id": job["doc"]["doc_id"], "error": exc.to_dict()["error"]}
                    results[result["doc_id"]] = result
            if stop_after_docs is not None and initial_completed + len(results) >= stop_after_docs:
                self.store.request_stop("stop_after_docs")
            if time.monotonic() > deadline:
                latency_error = True
                break
        return results, latency_error

    def _parallel_execute(
        self,
        jobs: list[dict[str, Any]],
        deadline: float,
        stop_after_docs: int | None,
        worker_count: int,
        initial_completed: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        if not jobs:
            return {}, False
        results: dict[str, dict[str, Any]] = {}
        latency_error = False
        context = multiprocessing.get_context("spawn")
        try:
            pool = context.Pool(processes=max(1, min(worker_count, len(jobs))))
        except BaseException as exc:
            doc_results = {
                job["doc"]["doc_id"]: self._worker_exception_result(job["doc"]["doc_id"], exc)
                for job in jobs
            }
            return doc_results, False
        pending: dict[Any, str] = {}
        job_by_id = {job["doc"]["doc_id"]: job for job in jobs}
        queue = sorted(job_by_id)
        cursor = 0

        def submit_next() -> None:
            nonlocal cursor
            while (
                cursor < len(queue)
                and len(pending) < max(1, min(worker_count, len(jobs)))
                and not self.store.stop_requested()
            ):
                doc_id = queue[cursor]
                cursor += 1
                pending[pool.apply_async(safe_worker_entry, (job_by_id[doc_id],))] = doc_id

        submit_next()
        try:
            while pending or cursor < len(queue):
                ready = sorted(
                    [future for future in pending if future.ready()],
                    key=lambda future: pending[future],
                )
                for future in ready:
                    doc_id = pending.pop(future)
                    try:
                        result = future.get()
                    except BaseException as exc:
                        result = self._worker_exception_result(doc_id, exc)
                    results[doc_id] = result
                    if result.get("ok"):
                        try:
                            self._persist_result(result)
                        except EngineError as exc:
                            result = {"ok": False, "doc_id": doc_id, "error": exc.to_dict()["error"]}
                            results[doc_id] = result
                    if stop_after_docs is not None and initial_completed + len(results) >= stop_after_docs:
                        self.store.request_stop("stop_after_docs")
                if time.monotonic() > deadline:
                    latency_error = True
                    break
                if not self.store.stop_requested():
                    submit_next()
                if not pending and cursor >= len(queue):
                    break
                if self.store.stop_requested() and not pending:
                    break
                time.sleep(max(0.001, float(job_by_id[queue[0]]["config"]["budget"]["poll_interval_ms"]) / 1000.0))
        finally:
            if latency_error and pending:
                pool.terminate()
            else:
                pool.close()
            pool.join()
        return results, latency_error

    def run(
        self,
        *,
        mode: str = "parallel",
        stop_after_docs: int | None = None,
        resume: bool = False,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if mode not in {"serial", "parallel"}:
            raise EngineError("E_INVALID_INPUT", "mode must be 'serial' or 'parallel'")
        if stop_after_docs is not None and (not isinstance(stop_after_docs, int) or stop_after_docs < 1):
            raise EngineError("E_INVALID_INPUT", "stop_after_docs must be a positive integer")
        self.store.initialize()
        if resume:
            self.store.clear_stop()
        documents, configuration = self.load_inputs(config)
        schedule = schedule_documents(documents, configuration)
        latest = self._latest_checkpoints()
        edits = self.store.load_edits()["documents"]
        accepted_by_id = {doc["doc_id"]: doc for doc in documents if doc["doc_id"] in set(schedule["accepted_doc_ids"])}
        jobs: list[dict[str, Any]] = []
        precomputed_results: dict[str, dict[str, Any]] = {}
        for doc_id in schedule["accepted_doc_ids"]:
            doc = accepted_by_id[doc_id]
            edit = edits.get(doc_id)
            info = self.cache_info(doc, configuration, edit, latest)
            job = self._make_job(doc, configuration, edit, info)
            if len(info["cached_stages"]) == len(STAGE_ORDER):
                result = safe_worker_entry(job)
                precomputed_results[doc_id] = result
            else:
                jobs.append(job)

        jobs.sort(key=lambda item: item["doc"]["doc_id"])
        results: dict[str, dict[str, Any]] = {}
        deadline = time.monotonic() + float(configuration["budget"]["latency_budget_ms"]) / 1000.0
        stop_hit = False
        latency_error = False
        for doc_id in sorted(precomputed_results):
            if stop_after_docs is not None and len(results) >= stop_after_docs:
                self.store.request_stop("stop_after_docs")
                stop_hit = True
                break
            if self.store.stop_requested():
                stop_hit = True
                break
            results[doc_id] = precomputed_results[doc_id]
        if not stop_hit and not self.store.stop_requested() and len(results) < len(schedule["accepted_doc_ids"]):
            if mode == "serial":
                computed, latency_error = self._serial_execute(
                    jobs,
                    deadline,
                    stop_after_docs,
                    initial_completed=len(results),
                )
            else:
                computed, latency_error = self._parallel_execute(
                    jobs,
                    deadline,
                    stop_after_docs,
                    int(schedule["worker_count_planned"]),
                    initial_completed=len(results),
                )
            results.update(computed)
        if self.store.stop_requested():
            stop_hit = True

        completed_ids = set(results)
        accepted_ids = list(schedule["accepted_doc_ids"])
        deferred = sorted(doc_id for doc_id in accepted_ids if doc_id not in completed_ids)
        successful = [results[doc_id] for doc_id in sorted(results) if results[doc_id].get("ok")]
        failures = [
            results[doc_id]["error"]
            for doc_id in sorted(results)
            if not results[doc_id].get("ok")
        ]
        global_error = None
        latency_rejections = [
            item for item in schedule["unprocessed"] if item.get("code") == "E_LATENCY_BUDGET"
        ]
        if latency_error:
            global_error = {
                "code": "E_LATENCY_BUDGET",
                "message": "Execution exceeded the request latency budget",
                "details": {"latency_budget_ms": schedule["latency_budget_ms"]},
            }
        elif not accepted_ids and latency_rejections:
            global_error = {
                "code": "E_LATENCY_BUDGET",
                "message": "No document fitted the request latency budget",
                "details": {"latency_budget_ms": schedule["latency_budget_ms"]},
            }
        if global_error is not None:
            status = "error"
        elif stop_hit:
            status = "stopped"
        elif failures:
            status = "complete_with_errors"
        else:
            status = "complete"
        documents_out = [self._result_view(result) for result in successful]
        budget_usage = {
            "global_budget": schedule["global_budget"],
            "per_worker_quota": schedule["per_worker_quota"],
            "max_workers": schedule["max_workers"],
            "planned_evaluations": schedule["accepted_evaluations"],
            "evaluations_used": sum(int(result.get("evaluation_count", 0)) for result in successful),
            "remaining_planned_evaluations": schedule["remaining_evaluations"],
            "accepted_documents": len(accepted_ids),
            "completed_documents": len(completed_ids),
            "successful_documents": len(successful),
            "failed_documents": len(failures),
            "deferred_documents": len(deferred),
            "worker_groups": schedule["worker_groups"],
            "estimated_latency_ms": schedule["estimated_latency_ms"],
            "latency_budget_ms": schedule["latency_budget_ms"],
        }
        output = {
            "ok": global_error is None,
            "status": status,
            "config_version": compute_config_version(configuration),
            "input_document_count": len(documents),
            "schedule": schedule,
            "documents": documents_out,
            "documents_with_errors": failures,
            "deferred": deferred,
            "budget_usage": budget_usage,
            "error": global_error,
        }
        self.store.save_last_run(output)
        return output

    def resume(
        self,
        *,
        mode: str = "parallel",
        stop_after_docs: int | None = None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.run(mode=mode, stop_after_docs=stop_after_docs, resume=True, config=config)

    def request_stop(self, reason: str = "requested") -> dict[str, Any]:
        self.store.request_stop(reason)
        return {"ok": True, "stop_requested": True, "reason": reason}

    def stop_status(self) -> dict[str, Any]:
        return {"stop_requested": self.store.stop_requested()}

    def _doc_by_id(self, doc_id: str, config: dict[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        configuration = normalize_config(config) if config is not None else self._load_config()
        documents = validate_documents(self._load_documents(), configuration)
        for doc in documents:
            if doc["doc_id"] == doc_id:
                return doc, configuration
        raise EngineError("E_INPUT_NOT_FOUND", f"Unknown doc_id: {doc_id}", doc_id=doc_id)

    def edit_summary(self, doc_id: str, text: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(text, str) or not text.strip():
            raise EngineError("E_INVALID_INPUT", "Edited summary must be a non-empty string", doc_id=doc_id)
        doc, configuration = self._doc_by_id(doc_id, config)
        latest = self._latest_checkpoints()
        edit_current = self.store.get_edit(doc_id)
        info = self.cache_info(doc, configuration, edit_current, latest)
        if "fusion" not in info["cached_stages"]:
            raise EngineError(
                "E_NOT_READY",
                "A current fusion result is required before editing the summary",
                doc_id=doc_id,
                details={"affected_stages": info["affected_stages"]},
            )
        fusion_fingerprint = info["fingerprints"]["fusion"]
        saved = self.store.append_edit(
            doc_id,
            text,
            base_fusion_fingerprint=fusion_fingerprint,
        )
        fingerprints = self.compute_stage_fingerprints(doc, configuration, saved)
        final_product = build_final_product(
            doc,
            info["cached_stages"]["fusion"],
            saved,
            fusion_fingerprint=fusion_fingerprint,
            final_fingerprint=fingerprints["final"],
        )
        cached = {stage: product for stage, product in info["cached_stages"].items() if stage != "final"}
        cached["final"] = final_product
        stage_entries = {
            stage: {
                "fingerprint": fingerprints[stage],
                "edit_version": int(saved["version"]),
                "product": product,
            }
            for stage, product in cached.items()
            if stage in STAGE_ORDER
        }
        self.store.save_products(
            doc_id,
            input_fingerprint=document_input_fingerprint(doc),
            config_version=compute_config_version(configuration),
            schedule_fingerprint=fingerprints["schedule"],
            stages=stage_entries,
        )
        record = {
            "stage": "final",
            "doc_id": doc_id,
            "input_fingerprint": document_input_fingerprint(doc),
            "stage_fingerprint": fingerprints["final"],
            "config_version": compute_config_version(configuration),
            "schedule_fingerprint": fingerprints["schedule"],
            "edit_version": int(saved["version"]),
            "product_digest": digest(final_product),
            "needs_review": bool(final_product["needs_review"]),
        }
        published = self.store.publish_records([record], expected_doc_id=doc_id)[0]
        return {
            "ok": True,
            "doc_id": doc_id,
            "edit_version": int(saved["version"]),
            "text": saved["text"],
            "text_digest": saved["text_digest"],
            "needs_review": bool(final_product["needs_review"]),
            "checkpoint_sequence": int(published["sequence"]),
            "stage_fingerprint": fingerprints["final"],
        }

    def status(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        documents, configuration = self.load_inputs(config)
        schedule = schedule_documents(documents, configuration)
        latest = self._latest_checkpoints()
        edits = self.store.load_edits()["documents"]
        accepted = set(schedule["accepted_doc_ids"])
        unprocessed = {item["doc_id"]: item for item in schedule["unprocessed"]}
        checkpoint_records = self.store.checkpoint_records()
        docs_out: list[dict[str, Any]] = []
        for doc in documents:
            doc_id = doc["doc_id"]
            edit = edits.get(doc_id)
            info = self.cache_info(doc, configuration, edit, latest)
            fingerprints = info["fingerprints"]
            cached = info["cached_stages"]
            stage_status: dict[str, Any] = {}
            for stage in STAGE_ORDER:
                latest_record = latest.get((doc_id, stage))
                stage_status[stage] = {
                    "current_fingerprint": fingerprints[stage],
                    "checkpointed": latest_record is not None,
                    "checkpoint_sequence": int(latest_record["sequence"]) if latest_record else None,
                    "checkpoint_matches": bool(
                        latest_record and latest_record.get("stage_fingerprint") == fingerprints[stage]
                    ),
                    "cached": stage in cached,
                }
            final = cached.get("final")
            docs_out.append(
                {
                    "doc_id": doc_id,
                    "accepted": doc_id in accepted,
                    "unprocessed": unprocessed.get(doc_id),
                    "input_fingerprint": document_input_fingerprint(doc),
                    "edit_version": int(edit.get("version", 0)) if edit else 0,
                    "human_edited": edit is not None,
                    "version_fingerprint": self.version_fingerprint(doc, configuration, edit),
                    "stage_fingerprints": fingerprints,
                    "stage_status": stage_status,
                    "cached_stages": [stage for stage in STAGE_ORDER if stage in cached],
                    "affected_stages": info["affected_stages"],
                    "earliest_affected_stage": info["earliest_affected_stage"],
                    "summary": final.get("summary") if final else None,
                    "needs_review": bool(final.get("needs_review")) if final else False,
                    "ready": final is not None,
                }
            )
        docs_out.sort(key=lambda item: item["doc_id"])
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "documents_path": str(self.documents_path),
            "config_path": str(self.config_path),
            "config_version": compute_config_version(configuration),
            "checkpoint_count": len(checkpoint_records),
            "last_checkpoint_sequence": max(
                (int(record.get("sequence", 0)) for record in checkpoint_records),
                default=0,
            ),
            "schedule": schedule,
            "documents": docs_out,
        }

    def budget_status(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        last_run = self.store.load_last_run()
        if last_run is not None and config is None:
            return {
                "ok": True,
                "source": "last_run",
                "budget_usage": last_run.get("budget_usage"),
                "unprocessed": last_run.get("schedule", {}).get("unprocessed", []),
                "deferred": last_run.get("deferred", []),
                "status": last_run.get("status"),
            }
        documents, configuration = self.load_inputs(config)
        schedule = schedule_documents(documents, configuration)
        return {
            "ok": True,
            "source": "current_schedule",
            "budget_usage": {
                "global_budget": schedule["global_budget"],
                "per_worker_quota": schedule["per_worker_quota"],
                "max_workers": schedule["max_workers"],
                "planned_evaluations": schedule["accepted_evaluations"],
                "remaining_planned_evaluations": schedule["remaining_evaluations"],
                "worker_groups": schedule["worker_groups"],
                "estimated_latency_ms": schedule["estimated_latency_ms"],
                "latency_budget_ms": schedule["latency_budget_ms"],
            },
            "unprocessed": schedule["unprocessed"],
            "deferred": [],
            "status": "planned",
        }

    def candidates(self, doc_id: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        doc, configuration = self._doc_by_id(doc_id, config)
        edit = self.store.get_edit(doc_id)
        info = self.cache_info(doc, configuration, edit)
        state = self.store.load_products(doc_id)
        if state is None:
            raise EngineError("E_NOT_READY", "No products have been generated for this document", doc_id=doc_id)
        stages = state.get("stages", {})
        return {
            "ok": True,
            "doc_id": doc_id,
            "current": len(info["cached_stages"]) == len(STAGE_ORDER),
            "affected_stages": info["affected_stages"],
            "earliest_affected_stage": info["earliest_affected_stage"],
            "stage_fingerprints": info["fingerprints"],
            "key_points": (stages.get("extract") or {}).get("product", {}).get("key_points", []),
            "sentences": (stages.get("extract") or {}).get("product", {}).get("sentences", []),
            "candidates": {
                stage: (stages.get(stage) or {}).get("product")
                for stage in ROUTE_STAGES
                if stage in stages
            },
            "fusion": (stages.get("fusion") or {}).get("product"),
            "final": (stages.get("final") or {}).get("product"),
        }

    def export_set(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        documents, configuration = self.load_inputs(config)
        schedule = schedule_documents(documents, configuration)
        last_run = self.store.load_last_run()
        latest = self._latest_checkpoints()
        edits = self.store.load_edits()["documents"]
        processed: set[str]
        if last_run is not None:
            processed = {item["doc_id"] for item in last_run.get("documents", [])}
            if last_run.get("deferred"):
                processed = processed - set(last_run["deferred"])
            unprocessed = last_run.get("schedule", {}).get("unprocessed", schedule["unprocessed"])
        else:
            processed = set(schedule["accepted_doc_ids"])
            unprocessed = schedule["unprocessed"]
        docs_by_id = {doc["doc_id"]: doc for doc in documents}
        entries: list[dict[str, Any]] = []
        for doc_id in sorted(processed):
            doc = docs_by_id.get(doc_id)
            if doc is None:
                continue
            edit = edits.get(doc_id)
            info = self.cache_info(doc, configuration, edit, latest)
            final = info["cached_stages"].get("final")
            entries.append(
                {
                    "doc_id": doc_id,
                    "valid": final is not None,
                    "summary": final.get("summary") if final else None,
                    "summary_source": final.get("summary_source") if final else None,
                    "route": final.get("candidate_route") if final else None,
                    "human_edited": bool(final.get("human_edited")) if final else bool(edit),
                    "edit_version": int(edit.get("version", 0)) if edit else 0,
                    "needs_review": bool(final.get("needs_review")) if final else False,
                    "fusion_score": final.get("fusion_score") if final else None,
                    "version_fingerprint": self.version_fingerprint(doc, configuration, edit),
                    "stage_fingerprints": info["fingerprints"],
                }
            )
        return {
            "schema": "sumengine.final_summaries.v1",
            "ok": True,
            "config_version": compute_config_version(configuration),
            "document_count": len(entries),
            "documents": entries,
            "unprocessed": sorted(unprocessed, key=lambda item: item["doc_id"]),
        }

    def export(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        value = self.export_set(config)
        files = self.store.write_export(value)
        return {"ok": True, **files, "document_count": value["document_count"]}
