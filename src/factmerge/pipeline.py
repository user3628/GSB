"""End-to-end four-stage engine and deterministic batch execution."""

from __future__ import annotations

import copy
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .config import (
    config_fingerprint,
    compute_corpus_digest,
    detect_invalidation,
    load_config,
    stage_component_digests,
    stage_fingerprints,
    validate_config,
)
from .errors import FactMergeError
from .evidence import (
    EVIDENCE_SCHEMA,
    build_entity_evidence,
    export_manifest_bytes,
    finalize_manifest,
    stage_content_digests,
    validate_manifest,
)
from .parser import infer_category, parse_entity_claims
from .resolver import resolve_entity
from .state import STAGES, StateStore
from .units import normalize_claim
from .util import (
    atomic_write_json,
    canonical_json,
    digest_obj,
    digest_without,
    quantize,
    read_json,
    read_jsonl,
)


def load_corpus(source: str | Path | Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FactMergeError("E_INVALID_FORMAT", f"corpus path does not exist: {path}")
        files: list[Path]
        if path.is_dir():
            files = sorted(path.glob("*.jsonl"))
            if not files:
                raise FactMergeError("E_INVALID_FORMAT", f"no JSONL shards found in {path}")
        else:
            files = [path]
        rows: list[dict[str, Any]] = []
        for file_path in files:
            stripped = file_path.read_text(encoding="utf-8").lstrip()
            if stripped.startswith("["):
                try:
                    value = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise FactMergeError(
                        "E_INVALID_FORMAT", f"invalid corpus JSON in {file_path}: {exc.msg}"
                    ) from exc
                if not isinstance(value, list):
                    raise FactMergeError("E_INVALID_FORMAT", f"corpus array expected in {file_path}")
                rows.extend(value)
            else:
                try:
                    rows.extend(read_jsonl(file_path))
                except ValueError as exc:
                    raise FactMergeError("E_INVALID_FORMAT", str(exc)) from exc
    else:
        rows = [copy.deepcopy(row) for row in source]

    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise FactMergeError("E_INVALID_FORMAT", f"corpus row {index} is not an object")
        claim_id = str(row.get("claim_id") or "")
        entity_id = str(row.get("entity_id") or "")
        if not claim_id or not entity_id:
            raise FactMergeError(
                "E_INVALID_FORMAT",
                f"corpus row {index} requires claim_id and entity_id",
                claim_id=claim_id or None,
                entity_id=entity_id or None,
            )
        if claim_id in seen:
            raise FactMergeError(
                "E_INVALID_FORMAT",
                f"duplicate claim_id: {claim_id}",
                claim_id=claim_id,
                entity_id=entity_id,
            )
        seen.add(claim_id)
        if isinstance(row.get("raw_value"), float):
            raise FactMergeError(
                "E_INVALID_FORMAT",
                "raw_value must be a string or integer; binary floats are not accepted",
                claim_id=claim_id,
                entity_id=entity_id,
            )
    return rows


def load_expected(source: str | Path | dict[str, Any] | None) -> dict[str, Any] | None:
    if source is None:
        return None
    if isinstance(source, (str, Path)):
        try:
            value = read_json(source)
        except (OSError, ValueError) as exc:
            raise FactMergeError("E_INVALID_FORMAT", f"cannot read expected-value file: {exc}") from exc
    else:
        value = copy.deepcopy(source)
    if not isinstance(value, dict):
        raise FactMergeError("E_INVALID_FORMAT", "expected values must be an object")
    return value


def _finalize_parse(entity_id: str, parsed: dict[str, Any], raw_by_id: dict[str, dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    for claim in parsed["claims"]:
        claim["digest"] = digest_without(claim, "digest")
    for error in parsed["errors"]:
        claim_id = str(error.get("claim_id") or "")
        raw = raw_by_id.get(claim_id, {})
        category = raw.get("category")
        if not category:
            try:
                category = infer_category(
                    raw,
                    [str(raw.get("raw_value", "")), str(raw.get("sentence", "")), str(raw.get("unit", ""))],
                    config,
                )
            except FactMergeError:
                category = None
        error["category"] = category or "unknown"
        error["entity_id"] = error.get("entity_id") or entity_id
        error["lang"] = str(raw.get("lang") or "und")
        error["digest"] = digest_without(error, "digest")
    payload = {
        "entity_id": entity_id,
        "claims": sorted(parsed["claims"], key=lambda item: item["claim_id"]),
        "errors": sorted(parsed["errors"], key=lambda item: str(item.get("claim_id", ""))),
    }
    payload["digest"] = digest_without(payload, "digest")
    return payload


def _build_stage2(stage1: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    normalized = [normalize_claim(claim, config) for claim in stage1["claims"]]
    errors: list[dict[str, Any]] = []
    for error in stage1["errors"]:
        code = str(error.get("code", "E_PARSE_AMBIGUOUS"))
        stage2_error = {
            "claim_id": error.get("claim_id"),
            "entity_id": error.get("entity_id"),
            "lang": error.get("lang", "und"),
            "category": error.get("category", "unknown"),
            "kind": "error",
            "status": "error",
            "review_issues": [
                {
                    "code": code,
                    "message": error.get("message", "parse error"),
                    "strategy": "error",
                    "disposition": "demoted",
                }
            ],
            "stage1_digest": error["digest"],
            "parse_error": error,
        }
        stage2_error["digest"] = digest_without(stage2_error, "digest")
        errors.append(stage2_error)
    payload = {
        "entity_id": stage1["entity_id"],
        "claims": sorted(normalized, key=lambda item: item["claim_id"]),
        "errors": sorted(errors, key=lambda item: str(item.get("claim_id", ""))),
    }
    payload["digest"] = digest_without(payload, "digest")
    return payload


def _build_stage3(
    entity_id: str,
    stage2: dict[str, Any],
    raw_by_id: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    resolution = resolve_entity(entity_id, stage2["claims"], stage2["errors"], raw_by_id, config)
    stage2_digests = {
        claim["claim_id"]: claim["digest"] for claim in list(stage2["claims"]) + list(stage2["errors"])
    }
    for decision in resolution["decisions"]:
        claim_id = decision["claim_id"]
        if claim_id not in stage2_digests:
            raise FactMergeError(
                "E_EVIDENCE_BROKEN",
                "stage3 decision has no matching stage2 payload",
                claim_id=claim_id,
                entity_id=entity_id,
                breakpoint="stage2",
            )
        decision["stage2_digest"] = stage2_digests[claim_id]
        decision["digest"] = digest_without(decision, "digest")
    resolution["digest"] = digest_without(resolution, "digest")
    return resolution


def _expected_value_matches(expected: dict[str, Any], accepted: dict[str, Any] | None) -> bool:
    if accepted is None:
        return False
    value = accepted.get("value")
    if not isinstance(value, dict):
        return False
    if expected.get("type") == "month_interval" or "start" in expected:
        expected_start = expected.get("start")
        expected_end = expected.get("end")
        return (
            value.get("type") == "month_interval"
            and value.get("start", {}).get("iso") == expected_start
            and value.get("end", {}).get("iso") == expected_end
        )
    expected_value = expected.get("value")
    expected_unit = expected.get("canonical_unit")
    return (
        str(value.get("value")) == str(expected_value)
        and (expected_unit is None or value.get("canonical_unit") == expected_unit)
    )


def _rate(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    value = Decimal(numerator) / Decimal(denominator)
    return format(quantize(value, "0.000001"), "f")


def _summary(
    entities: list[dict[str, Any]],
    *,
    expected: dict[str, Any] | None,
    version: dict[str, Any],
    evidence_ok: bool,
) -> dict[str, Any]:
    claim_count = sum(len(entity["claims"]) for entity in entities)
    parse_error_count = 0
    accepted_count = 0
    demoted_count = 0
    conflict_groups = 0
    resolved_groups = 0
    expected_conflicts = 0
    identified_conflicts = 0
    decision_expected = 0
    decision_correct = 0
    non_conflict_expected = 0
    non_conflict_correct = 0

    for entity in entities:
        entity_id = entity["entity_id"]
        category_results = entity["decision"].get("categories", [])
        accepted_by_category = {
            item["category"]: item for item in entity["decision"].get("accepted", [])
        }
        for claim in entity["claims"]:
            if (
                claim["stage1"].get("status") == "error"
                or claim["stage2"].get("status") == "error"
                or "code" in claim["stage1"]
            ):
                parse_error_count += 1
            if claim["stage3"].get("decision") == "accepted":
                accepted_count += 1
            else:
                demoted_count += 1
        for category_result in category_results:
            if category_result.get("conflict_detected"):
                conflict_groups += 1
            if category_result.get("accepted_claim_id"):
                resolved_groups += 1

        if expected and entity_id in expected:
            entity_expected = expected[entity_id]
            for category, exp in sorted(entity_expected.items()):
                if not isinstance(exp, dict):
                    continue
                actual_conflict = any(
                    item.get("category") == category and item.get("conflict_detected")
                    for item in category_results
                )
                if exp.get("conflict", False):
                    expected_conflicts += 1
                    if actual_conflict:
                        identified_conflicts += 1
                else:
                    non_conflict_expected += 1
                    if not actual_conflict:
                        non_conflict_correct += 1
                decision_expected += 1
                if _expected_value_matches(exp, accepted_by_category.get(category)):
                    decision_correct += 1

    summary = {
        "schema_version": "1.0",
        "config_fingerprint": version["config_fingerprint"],
        "stage_fingerprints": version["stage_fingerprints"],
        "corpus_digest": version["corpus_digest"],
        "entity_count": len(entities),
        "claim_count": claim_count,
        "parse_error_count": parse_error_count,
        "accepted_claim_count": accepted_count,
        "demoted_claim_count": demoted_count,
        "conflict_group_count": conflict_groups,
        "resolved_group_count": resolved_groups,
        "expected_conflict_group_count": expected_conflicts if expected is not None else None,
        "identified_conflict_group_count": identified_conflicts if expected is not None else None,
        "conflict_identification_rate": _rate(identified_conflicts, expected_conflicts)
        if expected is not None
        else None,
        "expected_decision_count": decision_expected if expected is not None else None,
        "correct_decision_count": decision_correct if expected is not None else None,
        "decision_accuracy": _rate(decision_correct, decision_expected) if expected is not None else None,
        "non_conflict_expected_count": non_conflict_expected if expected is not None else None,
        "non_conflict_correct_count": non_conflict_correct if expected is not None else None,
        "evidence_rebuild_success_count": len(entities) if evidence_ok else 0,
        "evidence_rebuild_rate": _rate(len(entities) if evidence_ok else 0, len(entities)),
        "expected_digest": digest_obj(expected) if expected is not None else None,
        "results_digest": digest_obj(entities),
    }
    return summary


def _build_manifest(
    config: dict[str, Any],
    version: dict[str, Any],
    entities: list[dict[str, Any]],
    expected: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    stage_digests = stage_content_digests(entities)
    version = copy.deepcopy(version)
    version["stage_content_digests"] = stage_digests
    version["content_fingerprint"] = digest_obj({"stage_content_digests": stage_digests})
    provisional = {
        "schema_version": EVIDENCE_SCHEMA,
        "manifest_schema": "factmerge.evidence",
        "config": copy.deepcopy(config),
        "config_digest": digest_obj(config),
        "version": version,
        "stage_content_digests": stage_digests,
        "entities": entities,
        "summary": {},
    }
    provisional = finalize_manifest(provisional)
    validate_manifest(provisional)
    summary = _summary(entities, expected=expected, version=version, evidence_ok=True)
    manifest = {
        **{key: provisional[key] for key in provisional if key != "summary" and key != "manifest_id"},
        "summary": summary,
    }
    manifest = finalize_manifest(manifest)
    validate_manifest(manifest)
    manifest_bytes = export_manifest_bytes(manifest)
    summary = copy.deepcopy(summary)
    summary["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    return manifest, summary, manifest_bytes


class FactMergeEngine:
    def __init__(
        self,
        config: dict[str, Any] | str | Path | None = None,
        corpus: str | Path | Iterable[dict[str, Any]] | None = None,
        *,
        state_dir: str | Path | None = None,
        expected: str | Path | dict[str, Any] | None = None,
    ):
        if isinstance(config, dict):
            self.config = copy.deepcopy(config)
            validate_config(self.config)
        else:
            self.config = load_config(config)
        if corpus is None:
            raise FactMergeError("E_INVALID_FORMAT", "a corpus is required")
        self.rows = load_corpus(corpus)
        self.expected = load_expected(expected)
        self.corpus_digest = compute_corpus_digest(self.rows)
        self.state = StateStore(state_dir) if state_dir is not None else None
        self._last_manifest: dict[str, Any] | None = None
        self._last_summary: dict[str, Any] | None = None

    def _version(self) -> dict[str, Any]:
        components = stage_component_digests(self.config)
        fps = stage_fingerprints(self.config, self.corpus_digest)
        return {
            "corpus_digest": self.corpus_digest,
            "component_digests": components,
            "stage_fingerprints": fps,
            "config_fingerprint": config_fingerprint(fps),
        }

    @property
    def entity_ids(self) -> list[str]:
        return sorted({str(row["entity_id"]) for row in self.rows})

    def _raw_by_entity(self) -> dict[str, dict[str, dict[str, Any]]]:
        grouped: dict[str, dict[str, dict[str, Any]]] = {}
        for row in sorted(self.rows, key=lambda item: str(item["claim_id"])):
            grouped.setdefault(str(row["entity_id"]), {})[str(row["claim_id"])] = row
        return grouped

    def _compute_entity(
        self,
        entity_id: str,
        raw_by_id: dict[str, dict[str, Any]],
        version: dict[str, Any],
    ) -> dict[str, Any]:
        fps = version["stage_fingerprints"]
        cached: dict[str, Any] = {}
        if self.state is not None:
            for stage in STAGES:
                payload = self.state.load_checkpoint(entity_id, stage, fps[stage])
                if payload is not None:
                    cached[stage] = payload
                    self.state.record_reuse(stage)

        stage1 = cached.get("stage1")
        if stage1 is None:
            parsed = parse_entity_claims(raw_by_id.values(), self.config)
            stage1 = _finalize_parse(entity_id, parsed, raw_by_id, self.config)
            if self.state is not None:
                self.state.write_checkpoint(entity_id, "stage1", fps["stage1"], stage1)

        stage2 = cached.get("stage2")
        if stage2 is None:
            stage2 = _build_stage2(stage1, self.config)
            if self.state is not None:
                self.state.write_checkpoint(entity_id, "stage2", fps["stage2"], stage2)

        stage3 = cached.get("stage3")
        if stage3 is None:
            stage3 = _build_stage3(entity_id, stage2, raw_by_id, self.config)
            if self.state is not None:
                self.state.write_checkpoint(entity_id, "stage3", fps["stage3"], stage3)

        stage4 = cached.get("stage4")
        if stage4 is None:
            stage4 = build_entity_evidence(stage3, {
                claim["claim_id"]: claim for claim in list(stage1["claims"]) + list(stage1["errors"])
            }, {
                claim["claim_id"]: claim for claim in list(stage2["claims"]) + list(stage2["errors"])
            }, raw_by_id)
            if self.state is not None:
                self.state.write_checkpoint(entity_id, "stage4", fps["stage4"], stage4)
        return stage4

    def run(
        self,
        *,
        entity_ids: Iterable[str] | None = None,
        resume: bool = False,
        stop_after_entities: int | None = None,
        use_state: bool = True,
    ) -> dict[str, Any]:
        version = self._version()
        self._last_manifest = None
        self._last_summary = None
        selected = self.entity_ids if entity_ids is None else sorted(set(str(item) for item in entity_ids))
        missing = sorted(set(selected).difference(self.entity_ids))
        if missing:
            raise FactMergeError("E_NOT_FOUND", f"unknown entity_id(s): {', '.join(missing)}")
        if stop_after_entities is not None and stop_after_entities < 0:
            raise FactMergeError("E_INVALID_FORMAT", "stop_after_entities must be non-negative")

        previous = self.state.state.get("version") if self.state is not None else None
        invalidation = detect_invalidation(
            previous,
            version["component_digests"],
            version["corpus_digest"],
            version["stage_fingerprints"],
        )
        if self.state is not None:
            if resume:
                self.state.clear_stop_request()
            self.state.begin_run(version, invalidation)
            self.state.mark_invalidations(
                version["stage_fingerprints"],
                invalidation["invalidated_stages"] if invalidation else [],
            )

        raw_grouped = self._raw_by_entity()
        entity_results: list[dict[str, Any]] = []
        processed = 0
        stopped = False
        next_entity: str | None = None

        if stop_after_entities == 0:
            stopped = True
            next_entity = selected[0] if selected else None

        for index, entity_id in enumerate([] if stopped else selected):
            if self.state is not None and use_state and self.state.stop_requested():
                stopped = True
                next_entity = entity_id
                break
            entity_results.append(
                self._compute_entity(
                    entity_id,
                    raw_grouped.get(entity_id, {}),
                    version,
                )
            )
            processed += 1
            if stop_after_entities is not None and processed >= stop_after_entities:
                if index + 1 < len(selected):
                    stopped = True
                    next_entity = selected[index + 1]
                break

        if stopped:
            if self.state is not None:
                self.state.set_run_status(
                    "stopped",
                    [entity["entity_id"] for entity in entity_results],
                    next_entity,
                )
            return {
                "status": "stopped",
                "manifest": None,
                "summary": None,
                "manifest_bytes": None,
                "manifest_sha256": None,
                "entity_results": entity_results,
                "next_entity_id": next_entity,
                "version_status": self.version_status(),
            }

        manifest, summary, manifest_bytes = _build_manifest(
            self.config, version, entity_results, self.expected
        )
        self._last_manifest = manifest
        self._last_summary = summary
        if self.state is not None:
            self.state.update_content_version(manifest["version"])
            self.state.set_run_status(
                "complete",
                [entity["entity_id"] for entity in entity_results],
                None,
            )
        return {
            "status": "complete",
            "manifest": manifest,
            "summary": summary,
            "manifest_bytes": manifest_bytes,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "entity_results": entity_results,
            "next_entity_id": None,
            "version_status": self.version_status(),
        }

    def verify_entity(self, entity_id: str, *, use_state: bool = True) -> dict[str, Any]:
        return self.run(entity_ids=[entity_id], use_state=use_state)

    def request_stop(self) -> None:
        if self.state is None:
            raise FactMergeError("E_INVALID_FORMAT", "request_stop requires a state directory")
        self.state.request_stop()

    def clear_stop(self) -> None:
        if self.state is not None:
            self.state.clear_stop_request()

    def version_status(self) -> dict[str, Any]:
        if self.state is None:
            version = self._version()
            return {
                "state_dir": None,
                "current_version": version,
                "earliest_affected_stage": 1,
                "invalidations": {
                    "earliest_stage": 1,
                    "reason_codes": ["STATELESS_ENGINE"],
                    "invalidated_stages": [1, 2, 3, 4],
                },
                "checkpoint_sequence": None,
            }
        status = self.state.version_status()
        status["stage_content_digests"] = status.get("version", {}).get("stage_content_digests", {})
        current = self._version()
        previous = self.state.state.get("version")
        planned = detect_invalidation(
            previous,
            current["component_digests"],
            current["corpus_digest"],
            current["stage_fingerprints"],
        )
        status["current_version"] = current
        status["planned_invalidation"] = planned
        return status

    def _require_manifest(self) -> dict[str, Any]:
        if self._last_manifest is None:
            raise FactMergeError("E_NOT_FOUND", "no completed run is loaded in this engine instance")
        return self._last_manifest

    def query_entity(self, entity_id: str) -> dict[str, Any]:
        manifest = self._require_manifest()
        for entity in manifest["entities"]:
            if entity["entity_id"] == entity_id:
                return entity
        raise FactMergeError("E_NOT_FOUND", f"entity not found in last run: {entity_id}", entity_id=entity_id)

    def query_claim(self, claim_id: str) -> dict[str, Any]:
        manifest = self._require_manifest()
        for entity in manifest["entities"]:
            for claim in entity["claims"]:
                if claim["claim_id"] == claim_id:
                    return claim
        raise FactMergeError("E_NOT_FOUND", f"claim not found in last run: {claim_id}", claim_id=claim_id)

    def consistency_check(self) -> dict[str, Any]:
        full = self.run(use_state=False)
        repeat = self.run(use_state=False)
        per_entity_results: list[dict[str, Any]] = []
        for entity_id in self.entity_ids:
            result = FactMergeEngine(
                self.config,
                self.rows,
                state_dir=None,
                expected=self.expected,
            ).run(entity_ids=[entity_id], use_state=False)
            per_entity_results.extend(result["entity_results"])
        per_entity_results.sort(key=lambda item: item["entity_id"])
        return {
            "repeat_manifest_byte_equal": full["manifest_bytes"] == repeat["manifest_bytes"],
            "repeat_summary_byte_equal": canonical_json(full["summary"]) == canonical_json(repeat["summary"]),
            "batch_vs_single_entity_equal": canonical_json(full["entity_results"])
            == canonical_json(per_entity_results),
            "manifest_sha256": full["manifest_sha256"],
            "summary": full["summary"],
        }


def write_run_outputs(result: dict[str, Any], out_dir: str | Path) -> None:
    if result["status"] != "complete":
        raise FactMergeError("E_STOP_REQUESTED", "run is stopped; no final manifest to export")
    out = Path(out_dir)
    from .util import atomic_write_bytes

    atomic_write_bytes(out / "evidence_manifest.json", result["manifest_bytes"])
    atomic_write_json(out / "batch_summary.json", result["summary"])
