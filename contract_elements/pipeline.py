from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .arbitration import parse_effective_date, resolve_contract
from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_jsonl,
    canonical_line,
    read_json,
    read_jsonl,
    sha256_bytes,
    sha256_value,
    short_hash,
)
from .config import config_fingerprint
from .errors import BusinessError, error_dict
from .experiment import compute_metrics, experiment_identity
from .extraction import DictionaryStore, ElementExtractor, SeedEstimator
from .interop import V1Interop
from .normalization import ClauseNormalizer
from .versioning import (
    ContentCache,
    build_dependency_components,
    build_version_state,
    load_previous_state,
    write_version_state,
)


@dataclass(frozen=True)
class DatasetPaths:
    input_path: Path = Path("data/contracts.jsonl")
    seed_path: Path = Path("data/seeds.jsonl")
    dictionary_path: Path = Path("data/elements.json")
    v1_path: Path = Path("data/annotations_v1.jsonl")
    gold_path: Path = Path("data/gold_conflicts.json")
    output_dir: Path = Path("output/default")
    runtime_dir: Path = Path("runtime/default")

    def with_output(self, output_dir: str | Path, runtime_dir: str | Path) -> "DatasetPaths":
        return DatasetPaths(
            input_path=self.input_path,
            seed_path=self.seed_path,
            dictionary_path=self.dictionary_path,
            v1_path=self.v1_path,
            gold_path=self.gold_path,
            output_dir=Path(output_dir),
            runtime_dir=Path(runtime_dir),
        )


def _load_dictionary(path: str | Path) -> list[dict[str, Any]]:
    value = read_json(path)
    if isinstance(value, dict):
        entries = value.get("entries")
    else:
        entries = value
    if not isinstance(entries, list):
        raise ValueError("dictionary must be a list or contain an entries list")
    return entries


def _matching_rows(rows: list[dict[str, Any]], injection: dict[str, Any]) -> list[dict[str, Any]]:
    contract_id = injection.get("contract_id")
    clause_no = injection.get("clause_no")
    source_index = injection.get("source_index")
    matched = []
    for index, row in enumerate(rows):
        if contract_id is not None and str(row.get("contract_id")) != str(contract_id):
            continue
        if clause_no is not None and str(row.get("clause_no")) != str(clause_no):
            continue
        if source_index is not None and index != int(source_index):
            continue
        matched.append(row)
    return matched


def apply_injections(
    rows: list[dict[str, Any]],
    injections: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    result = deepcopy(rows)
    for injection in injections:
        matched = _matching_rows(result, injection)
        if not matched:
            raise ValueError(f"injection target did not match any row: {injection}")
        kind = injection.get("kind", injection.get("type"))
        value = injection.get("value")
        for row in matched:
            if kind == "invalid_clause_no":
                row["clause_no"] = value if value is not None else "第X条"
            elif kind == "text_too_long":
                max_chars = int(config["normalization"]["max_text_chars"])
                suffix = str(value) if value is not None else "超长条款内容"
                repeats = max(1, max_chars // max(1, len(suffix)) + 2)
                row["clause_text"] = f"{row.get('clause_text', '')} {suffix * repeats}"
            elif kind == "unknown_element":
                annotations = list(row.get("annotations") or [])
                annotations.append(
                    {
                        "element_type": value if value is not None else "payment_method",
                        "start": 0,
                        "end": 1,
                        "value": "UNKNOWN",
                    }
                )
                row["annotations"] = annotations
            elif kind == "invalid_effective_date":
                row["effective_date"] = value if value is not None else "2025/13/40"
            else:
                raise ValueError(f"unknown injection kind: {kind}")
    return result


class BatchEngine:
    def __init__(
        self,
        config: dict[str, Any],
        paths: DatasetPaths,
        *,
        injection_path: str | Path | None = None,
    ) -> None:
        self.config = deepcopy(config)
        self.paths = paths
        self.injections: list[dict[str, Any]] = []
        if injection_path is not None:
            payload = read_json(injection_path)
            self.injections = payload.get("injections", []) if isinstance(payload, dict) else payload
        original_rows = read_jsonl(paths.input_path)
        self.input_rows = apply_injections(original_rows, self.injections, self.config)
        self.seed_rows = read_jsonl(paths.seed_path)
        self.dictionary_entries = _load_dictionary(paths.dictionary_path)
        self.dictionary = DictionaryStore(self.dictionary_entries)
        self.normalizer = ClauseNormalizer(self.config)
        estimator = SeedEstimator(self.normalizer, self.dictionary, self.config["extraction"])
        self.model = estimator.estimate(self.seed_rows)
        self.extractor = ElementExtractor(self.model, self.dictionary, self.config["extraction"])
        self.dependency_components = build_dependency_components(
            self.config,
            self.input_rows,
            self.seed_rows,
            self.dictionary_entries,
            self.model,
        )
        self.run_id = sha256_value(
            {
                "config": self.config,
                "inputs": self.input_rows,
                "seeds": self.seed_rows,
                "dictionary": self.dictionary_entries,
            }
        )
        self.cache = ContentCache(paths.runtime_dir / "cache")
        self.version_state_path = paths.runtime_dir / "version_state.json"
        self.checkpoint_path = paths.runtime_dir / "checkpoint.json"
        self.progress_path = paths.runtime_dir / "progress.json"
        self.stop_file = paths.runtime_dir / "STOP"
        self.previous_version_state = load_previous_state(self.version_state_path)
        self.cache_stats: dict[str, dict[str, int]] = {
            "normalize": {"hit": 0, "miss": 0},
            "extract": {"hit": 0, "miss": 0},
            "arbitrate": {"hit": 0, "miss": 0},
        }
        self._processed_this_run = 0
        self._stop_after_units: int | None = None

    def _cache_get(self, stage: str, key: str) -> Any | None:
        value = self.cache.get(stage, key)
        self.cache_stats[stage]["hit" if value is not None else "miss"] += 1
        return value

    def _cache_put(self, stage: str, key: str, payload: Any) -> None:
        self.cache.put(stage, key, payload)

    def _normalize_key(self, row: dict[str, Any], index: int) -> str:
        return sha256_value(
            {
                "source_index": index,
                "contract_id": row.get("contract_id"),
                "clause_text": row.get("clause_text"),
                "clause_no": row.get("clause_no"),
                "template_id": row.get("template_id"),
                "annotations": row.get("annotations") or [],
                "config": self.config["normalization"],
            }
        )

    def _extract_key(self, unit: dict[str, Any]) -> str:
        return sha256_value(
            {
                "unit": unit,
                "model": self.model,
                "config": self.config["extraction"],
            }
        )

    def _arbitrate_key(
        self,
        contract_id: str,
        candidates: list[dict[str, Any]],
    ) -> str:
        return sha256_value(
            {
                "contract_id": contract_id,
                "candidates": candidates,
                "config": self.config["arbitration"],
            }
        )

    def _normalize_one(self, row: dict[str, Any], index: int) -> dict[str, Any]:
        key = self._normalize_key(row, index)
        cached = self._cache_get("normalize", key)
        if cached is not None:
            return cached
        try:
            result = {"ok": True, "result": self.normalizer.normalize_record(row, index)}
        except BusinessError as error:
            result = {"ok": False, "error": error.to_dict()}
        self._cache_put("normalize", key, result)
        return result

    def _extract_one(self, unit: dict[str, Any]) -> dict[str, Any]:
        key = self._extract_key(unit)
        cached = self._cache_get("extract", key)
        if cached is not None:
            return cached
        try:
            payload = {"ok": True, "result": self.extractor.extract_unit(unit)}
        except BusinessError as error:
            payload = {"ok": False, "error": error.to_dict()}
        self._cache_put("extract", key, payload)
        return payload

    def _arbitrate_one(
        self,
        contract_id: str,
        candidates: list[dict[str, Any]],
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        key = sha256_value(
            {
                "contract": self._arbitrate_key(contract_id, candidates),
                "effective_dates": [row.get("effective_date") for row in rows],
            }
        )
        cached = self._cache_get("arbitrate", key)
        if cached is not None:
            return cached
        errors: list[dict[str, Any]] = []
        try:
            for row in rows:
                parse_effective_date(row.get("effective_date"), contract_id, self.config["arbitration"])
            resolved = resolve_contract(contract_id, candidates, self.config["arbitration"])
            payload = {
                "type": "arbitrated_contract",
                "contract_id": contract_id,
                "resolved": [resolved[key] for key in sorted(resolved)],
                "errors": errors,
            }
        except BusinessError as error:
            errors.append(error.to_dict())
            payload = {
                "type": "arbitrated_contract",
                "contract_id": contract_id,
                "resolved": [],
                "errors": errors,
            }
        self._cache_put("arbitrate", key, payload)
        return payload

    def _initial_state(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "seq": 0,
            "stage": "normalize",
            "normalize_done": 0,
            "extract_done": 0,
            "arbitrate_done": 0,
            "normalized": [],
            "extracted": [],
            "arbitrated": [],
            "completed": False,
        }

    def _write_checkpoint(self, state: dict[str, Any], stage: str) -> None:
        state["stage"] = stage
        state["seq"] = int(state["seq"]) + 1
        atomic_write_json(self.checkpoint_path, state)

    def _load_checkpoint(self) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            raise BusinessError(
                "E_CHECKPOINT_MISMATCH",
                "",
                "checkpoint",
                "no checkpoint exists for resume",
                {"path": str(self.checkpoint_path)},
            )
        state = read_json(self.checkpoint_path)
        if state.get("run_id") != self.run_id:
            raise BusinessError(
                "E_CHECKPOINT_MISMATCH",
                "",
                "checkpoint",
                "checkpoint does not match current input and configuration",
                {
                    "checkpoint_run_id": state.get("run_id"),
                    "current_run_id": self.run_id,
                },
            )
        return state

    def _contract_rows(self) -> tuple[list[str], dict[str, list[dict[str, Any]]]]:
        order: list[str] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in self.input_rows:
            contract_id = str(row.get("contract_id", ""))
            if contract_id not in grouped:
                grouped[contract_id] = []
                order.append(contract_id)
            grouped[contract_id].append(row)
        return order, grouped

    def _units_with_dates(self, state: dict[str, Any]) -> list[tuple[dict[str, Any], str | None]]:
        result: list[tuple[dict[str, Any], str | None]] = []
        for normalized in state["normalized"]:
            if not normalized.get("ok"):
                continue
            record = normalized["result"]
            effective_date = self.input_rows[record["source_index"]].get("effective_date")
            for unit in record["units"]:
                result.append((unit, effective_date))
        return result

    def _should_stop(self) -> bool:
        if self.stop_file.exists():
            return True
        return self._stop_after_units is not None and self._processed_this_run >= self._stop_after_units

    def _after_unit(self, state: dict[str, Any], stage: str) -> bool:
        self._processed_this_run += 1
        self._write_checkpoint(state, stage)
        return self._should_stop()

    def _write_progress(self, state: dict[str, Any], status: str) -> None:
        atomic_write_json(
            self.progress_path,
            {
                "schema_version": 1,
                "status": status,
                "stage": state.get("stage"),
                "seq": state.get("seq", 0),
                "normalize_done": state.get("normalize_done", 0),
                "extract_done": state.get("extract_done", 0),
                "arbitrate_done": state.get("arbitrate_done", 0),
                "normalize_total": len(self.input_rows),
                "extract_total": len(self._units_with_dates(state)) if state.get("normalize_done") == len(self.input_rows) else None,
                "arbitrate_total": len(self._contract_rows()[0]),
            },
        )

    def _clear_stop(self) -> None:
        if self.stop_file.exists():
            self.stop_file.unlink()

    def run(
        self,
        *,
        resume: bool = False,
        stop_after_units: int | None = None,
        clear_stop: bool = False,
    ) -> dict[str, Any]:
        self._processed_this_run = 0
        self._stop_after_units = stop_after_units
        if resume:
            state = self._load_checkpoint()
            if clear_stop:
                self._clear_stop()
        else:
            state = self._initial_state()
            if self.stop_file.exists():
                self._clear_stop()
            self.paths.output_dir.mkdir(parents=True, exist_ok=True)
            self.paths.runtime_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.checkpoint_path, state)

        if state.get("completed"):
            return self._finalize(state)

        while state["normalize_done"] < len(self.input_rows):
            if self._should_stop():
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}
            index = int(state["normalize_done"])
            payload = self._normalize_one(self.input_rows[index], index)
            if payload.get("ok"):
                state["normalized"].append(
                    {
                        "ok": True,
                        "source_index": index,
                        "contract_id": payload["result"]["contract_id"],
                        "result": payload["result"],
                    }
                )
            else:
                state["normalized"].append(
                    {
                        "ok": False,
                        "source_index": index,
                        "contract_id": str(self.input_rows[index].get("contract_id", "")),
                        "error": payload["error"],
                    }
                )
            state["normalize_done"] = index + 1
            if self._after_unit(state, "normalize"):
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}

        units_with_dates = self._units_with_dates(state)
        while state["extract_done"] < len(units_with_dates):
            if self._should_stop():
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}
            index = int(state["extract_done"])
            unit, _effective_date = units_with_dates[index]
            payload = self._extract_one(unit)
            state["extracted"].append(
                {
                    "ok": bool(payload.get("ok")),
                    "clause_id": unit["clause_id"],
                    "contract_id": unit["contract_id"],
                    "result": payload.get("result"),
                    "error": payload.get("error"),
                }
            )
            state["extract_done"] = index + 1
            if self._after_unit(state, "extract"):
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}

        contract_order, contract_rows = self._contract_rows()
        extracted_by_contract: dict[str, list[dict[str, Any]]] = {contract_id: [] for contract_id in contract_order}
        effective_by_clause = {unit["clause_id"]: date for unit, date in units_with_dates}
        for extracted in state["extracted"]:
            if extracted.get("ok"):
                extracted_by_contract.setdefault(extracted["contract_id"], []).append(extracted["result"])
        while state["arbitrate_done"] < len(contract_order):
            if self._should_stop():
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}
            index = int(state["arbitrate_done"])
            contract_id = contract_order[index]
            clauses = extracted_by_contract.get(contract_id, [])
            candidates = []
            for clause in clauses:
                for candidate in clause.get("candidates", []):
                    enriched = dict(candidate)
                    enriched["effective_date"] = effective_by_clause.get(candidate["clause_id"])
                    candidates.append(enriched)
            payload = self._arbitrate_one(contract_id, candidates, contract_rows[contract_id])
            state["arbitrated"].append(payload)
            state["arbitrate_done"] = index + 1
            if self._after_unit(state, "arbitrate"):
                self._write_progress(state, "stopped")
                return {"status": "stopped", **self._progress_summary(state)}

        state["completed"] = True
        self._write_checkpoint(state, "complete")
        return self._finalize(state)

    def _progress_summary(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "stage": state.get("stage"),
            "seq": state.get("seq"),
            "normalize_done": state.get("normalize_done", 0),
            "normalize_total": len(self.input_rows),
            "extract_done": state.get("extract_done", 0),
            "arbitrate_done": state.get("arbitrate_done", 0),
            "checkpoint": str(self.checkpoint_path),
        }

    def _collect_contract_artifacts(self, state: dict[str, Any]) -> dict[str, Any]:
        contract_order, contract_rows = self._contract_rows()
        extracted_by_contract: dict[str, list[dict[str, Any]]] = {contract_id: [] for contract_id in contract_order}
        errors_by_contract: dict[str, list[dict[str, Any]]] = {contract_id: [] for contract_id in contract_order}
        all_errors: list[dict[str, Any]] = []

        for normalized in state["normalized"]:
            contract_id = normalized.get("contract_id", "")
            if not normalized.get("ok"):
                error = normalized["error"]
                errors_by_contract.setdefault(contract_id, []).append(error)
                all_errors.append(error)
        for extracted in state["extracted"]:
            contract_id = extracted.get("contract_id", "")
            if not extracted.get("ok"):
                error = extracted.get("error")
                if error:
                    errors_by_contract.setdefault(contract_id, []).append(error)
                    all_errors.append(error)
                continue
            result = extracted["result"]
            extracted_by_contract.setdefault(contract_id, []).append(result)
            for error in result.get("errors", []):
                errors_by_contract.setdefault(contract_id, []).append(error)
                all_errors.append(error)

        arbitrated_by_contract = {
            item["contract_id"]: item for item in state["arbitrated"] if item.get("type") == "arbitrated_contract"
        }
        for arbitration in state["arbitrated"]:
            contract_id = arbitration["contract_id"]
            for error in arbitration.get("errors", []):
                errors_by_contract.setdefault(contract_id, []).append(error)
                all_errors.append(error)

        def dedupe(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
            unique = {sha256_value(error): error for error in errors}
            return [unique[key] for key in sorted(unique)]

        results: list[dict[str, Any]] = []
        candidate_rows: list[dict[str, Any]] = []
        conflict_rows: list[dict[str, Any]] = []
        for contract_id in contract_order:
            clauses = sorted(
                extracted_by_contract.get(contract_id, []),
                key=lambda item: (item["clause_no"], item["unit_index"], item["clause_id"]),
            )
            arbitration = arbitrated_by_contract.get(
                contract_id,
                {"resolved": [], "errors": []},
            )
            resolved = arbitration.get("resolved", [])
            conflicts = [item for item in resolved if item.get("conflict")]
            rows = contract_rows.get(contract_id, [])
            first = rows[0] if rows else {}
            for clause in clauses:
                candidate_rows.extend(clause.get("candidates", []))
            conflict_rows.extend(conflicts)
            results.append(
                {
                    "type": "contract_result",
                    "contract_id": contract_id,
                    "template_id": first.get("template_id"),
                    "effective_date": first.get("effective_date"),
                    "clauses": clauses,
                    "resolved_elements": resolved,
                    "conflicts": conflicts,
                    "errors": dedupe(errors_by_contract.get(contract_id, [])),
                }
            )

        candidate_rows.sort(
            key=lambda item: (
                item["contract_id"],
                item["clause_no"],
                item["unit_index"],
                item["element_type"],
                item["start"],
                item["end"],
                item["candidate_id"],
            )
        )
        conflict_rows.sort(key=lambda item: (item["contract_id"], item["element_type"]))
        all_errors = dedupe(all_errors)
        all_errors.sort(
            key=lambda item: (
                item.get("contract_id", ""),
                item.get("stage", ""),
                item.get("code", ""),
                sha256_value(item),
            )
        )
        return {
            "results": results,
            "candidates": candidate_rows,
            "conflicts": conflict_rows,
            "errors": all_errors,
        }

    def _jsonl_bytes(self, rows: list[dict[str, Any]]) -> bytes:
        return b"".join(canonical_line(row) for row in rows)

    def _finalize(self, state: dict[str, Any]) -> dict[str, Any]:
        artifacts = self._collect_contract_artifacts(state)
        result_bytes = self._jsonl_bytes(artifacts["results"])
        candidate_bytes = self._jsonl_bytes(artifacts["candidates"])
        conflict_bytes = self._jsonl_bytes(artifacts["conflicts"])
        error_bytes = self._jsonl_bytes(artifacts["errors"])
        output_dir = self.paths.output_dir
        atomic_write_bytes(output_dir / "results.jsonl", result_bytes)
        atomic_write_bytes(output_dir / "candidates.jsonl", candidate_bytes)
        atomic_write_bytes(output_dir / "conflicts.jsonl", conflict_bytes)
        atomic_write_bytes(output_dir / "errors.jsonl", error_bytes)

        extracted = [entry["result"] for entry in state["extracted"] if entry.get("ok")]
        resolved = [item for arbitration in state["arbitrated"] for item in arbitration.get("resolved", [])]
        gold_conflicts: list[dict[str, Any]] = []
        if self.paths.gold_path.exists():
            payload = read_json(self.paths.gold_path)
            gold_conflicts = payload.get("conflicts", []) if isinstance(payload, dict) else payload
        v1_rows = read_jsonl(self.paths.v1_path) if self.paths.v1_path.exists() else []
        interop = V1Interop(self.config, self.normalizer)
        try:
            compatibility = interop.compatibility(v1_rows) if v1_rows else {
                "success": True,
                "input_records": 0,
                "successful_records": 0,
                "converted_records": 0,
                "first_export_sha256": None,
                "second_export_sha256": None,
            }
        except Exception as exc:
            compatibility = {
                "success": False,
                "input_records": len(v1_rows),
                "successful_records": 0,
                "converted_records": 0,
                "first_export_sha256": None,
                "second_export_sha256": None,
                "failure": str(exc),
            }
        metrics = compute_metrics(extracted, resolved, gold_conflicts, compatibility, self.config)
        error_codes: dict[str, int] = {}
        for error in artifacts["errors"]:
            code = error.get("code", "UNKNOWN")
            error_codes[code] = error_codes.get(code, 0) + 1
        valid_candidates = sum(1 for candidate in artifacts["candidates"] if candidate.get("valid"))
        run_summary = {
            "config_fingerprint": config_fingerprint(self.config),
            "experiment_identity": experiment_identity(
                self.config,
                self.input_rows,
                self.seed_rows,
                self.dictionary_entries,
                gold_conflicts,
                v1_rows,
            ),
            "counts": {
                "input_records": len(self.input_rows),
                "normalized_records": sum(1 for item in state["normalized"] if item.get("ok")),
                "clause_units": len(extracted),
                "contracts": len(self._contract_rows()[0]),
                "candidates": len(artifacts["candidates"]),
                "valid_candidates": valid_candidates,
                "resolved_elements": len(resolved),
                "conflicts": len(artifacts["conflicts"]),
                "errors": len(artifacts["errors"]),
            },
            "error_codes": {code: error_codes[code] for code in sorted(error_codes)},
        }
        version_state = build_version_state(
            self.previous_version_state,
            self.dependency_components,
            {
                "normalize": state["normalized"],
                "extract": state["extracted"],
                "arbitrate": state["arbitrated"],
            },
            self.cache_stats,
            run_summary,
        )
        write_version_state(self.version_state_path, version_state)
        summary = {
            "schema_version": 1,
            "status": "completed",
            **run_summary,
            "version_fingerprint": version_state["version_fingerprint"],
            "metrics": metrics,
            "artifacts": {
                "results.jsonl": sha256_bytes(result_bytes),
                "candidates.jsonl": sha256_bytes(candidate_bytes),
                "conflicts.jsonl": sha256_bytes(conflict_bytes),
                "errors.jsonl": sha256_bytes(error_bytes),
            },
        }
        atomic_write_json(output_dir / "summary.json", summary)
        atomic_write_json(output_dir / "experiment_summary.json", summary)
        self._write_progress(state, "completed")
        return {"status": "completed", "summary": summary, **self._progress_summary(state)}

    def parse_contract(self, contract_id: str) -> dict[str, Any]:
        matching = [row for row in self.input_rows if str(row.get("contract_id", "")) == contract_id]
        if not matching:
            raise BusinessError(
                "E_INTEROP",
                contract_id,
                "normalize",
                "contract_id was not found in the local input",
                {},
            )
        errors: list[dict[str, Any]] = []
        clauses: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        effective_by_clause: dict[str, str | None] = {}
        for index, row in enumerate(self.input_rows):
            if str(row.get("contract_id", "")) != contract_id:
                continue
            payload = self._normalize_one(row, index)
            if not payload.get("ok"):
                errors.append(payload["error"])
                continue
            normalized = payload["result"]
            for unit in normalized["units"]:
                effective_by_clause[unit["clause_id"]] = row.get("effective_date")
                extracted = self._extract_one(unit)
                if not extracted.get("ok"):
                    if extracted.get("error"):
                        errors.append(extracted["error"])
                    continue
                clause = extracted["result"]
                clauses.append(clause)
                candidates.extend(clause.get("candidates", []))
                errors.extend(clause.get("errors", []))
        clauses.sort(key=lambda item: (item["clause_no"], item["unit_index"], item["clause_id"]))
        arbitration_errors: list[dict[str, Any]] = []
        try:
            for row in matching:
                parse_effective_date(row.get("effective_date"), contract_id, self.config["arbitration"])
            enriched_candidates = []
            for candidate in candidates:
                enriched = dict(candidate)
                # Clause payloads do not carry effective dates; resolution reads
                # them from normalized input metadata by clause_id.
                enriched["effective_date"] = effective_by_clause.get(candidate["clause_id"])
                enriched_candidates.append(enriched)
            resolved_map = resolve_contract(contract_id, enriched_candidates, self.config["arbitration"])
            resolved = [resolved_map[key] for key in sorted(resolved_map)]
        except BusinessError as error:
            resolved = []
            arbitration_errors.append(error.to_dict())
            errors.append(error.to_dict())
        unique_errors = {sha256_value(error): error for error in errors}
        errors = [unique_errors[key] for key in sorted(unique_errors)]
        candidates.sort(key=lambda item: (item["clause_no"], item["unit_index"], item["element_type"], item["start"], item["candidate_id"]))
        first = matching[0]
        return {
            "type": "contract_result",
            "contract_id": contract_id,
            "template_id": first.get("template_id"),
            "effective_date": first.get("effective_date"),
            "clauses": clauses,
            "candidates": candidates,
            "resolved_elements": resolved,
            "conflicts": [item for item in resolved if item.get("conflict")],
            "errors": errors,
            "arbitration_errors": arbitration_errors,
            "version_fingerprint": (
                None
                if self.previous_version_state is None
                else self.previous_version_state.get("version_fingerprint")
            ),
        }



