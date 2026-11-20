from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from .config import load_config
from .errors import REEError
from .formats import decode_json_intervals
from .models import canonical_bytes, sha256_bytes
from .pipeline import Pipeline
from .sample_data import FIXED_SEED, generate_samples


def _clean_runtime(root: Path) -> None:
    runtime = root / ".ree"
    if runtime.exists():
        shutil.rmtree(runtime)


def _error_code(callable_object) -> str:
    try:
        callable_object()
    except REEError as exc:
        return exc.code
    return "NO_ERROR"


def _constraint_violations(pipeline: Pipeline, config) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for document in pipeline.get_documents():
        entities = {entity.id: entity for entity in document.entities}
        for relation in document.relations:
            spec = config.relation_types.get(relation.type)
            if spec is None:
                violations.append(
                    {"doc_id": document.doc_id, "kind": "unknown_relation", "id": relation.id}
                )
                continue
            if (
                entities[relation.head].type not in spec.head
                or entities[relation.tail].type not in spec.tail
            ):
                violations.append(
                    {"doc_id": document.doc_id, "kind": "relation_domain", "id": relation.id}
                )
        for event in document.events:
            spec = config.event_types.get(event.type)
            if spec is None:
                violations.append(
                    {"doc_id": document.doc_id, "kind": "unknown_event", "id": event.id}
                )
                continue
            for role, members in event.arguments.items():
                allowed = set(spec.roles.get(role, []))
                for entity_id in members:
                    if entities[entity_id].type not in allowed:
                        violations.append(
                            {
                                "doc_id": document.doc_id,
                                "kind": "event_argument_type",
                                "event_id": event.id,
                                "role": role,
                                "entity_id": entity_id,
                            }
                        )
    return violations


def run_demo(root: str | Path, *, regenerate: bool = True) -> dict[str, Any]:
    root = Path(root).resolve()
    if regenerate:
        generate_samples(root)
    manifest_path = root / "data" / "manifest.json"
    if not manifest_path.exists():
        generate_samples(root)
    _clean_runtime(root)

    pipeline = Pipeline(root)
    stopped = pipeline.run(stop_after_stage="estimate")
    seq_after_stop = [record.seq for record in pipeline.checkpoints.records()]
    resumed = pipeline.resume()
    records = pipeline.checkpoints.records()
    checkpoint_sequence = [record.seq for record in records]
    checkpoint_stages = [record.stage for record in records]
    extract_artifact = pipeline.get_stage_artifact("extract")
    validate_artifact = pipeline.get_stage_artifact("validate")
    parse_artifact = pipeline.get_stage_artifact("parse")
    config = load_config(pipeline.config_path)

    extraction_sha256 = hashlib.sha256(canonical_bytes(extract_artifact)).hexdigest()
    validation_sha256 = hashlib.sha256(canonical_bytes(validate_artifact)).hexdigest()
    run_digest = sha256_bytes(
        canonical_bytes(
            {
                "extract": extract_artifact,
                "validate": validate_artifact,
                "checkpoint_stages": checkpoint_stages,
            }
        )
    )
    violations = _constraint_violations(pipeline, config)

    unsupported_code = _error_code(
        lambda: decode_json_intervals(
            {
                "format": "JSON_INTERVALS",
                "schema_version": 99,
                "doc_id": "unsupported_probe",
                "tokens": ["probe"],
                "text": "probe",
                "source": "demo",
            },
            config,
        )
    )

    missing_workspace = Pipeline(root, runtime_dir=".ree/demo_missing_artifact")
    missing_code = _error_code(
        lambda: missing_workspace.get_stage_artifact("extract")
    )

    checkpoint_workspace = Pipeline(root, runtime_dir=".ree/demo_stale_checkpoint")
    stale_checkpoint_code = _error_code(
        lambda: checkpoint_workspace.checkpoints.publish(
            stage="estimate",
            expected_seq=1,
            input_digest=sha256_bytes(b"input"),
            config_version=config.digest(),
            artifact_digest=sha256_bytes(b"artifact"),
            artifact_path=".ree/demo_stale_checkpoint/missing.json",
            input_components={"probe": True},
        )
    )

    original_config = pipeline.config_path.read_bytes()
    invalidation_demo: dict[str, Any] = {}
    stale_artifact_code = "NO_ERROR"
    try:
        modified = original_config.replace(b"smoothing: 0.5", b"smoothing: 0.6")
        if modified == original_config:
            raise RuntimeError("demo could not modify smoothing configuration")
        pipeline.workspace.atomic_write_bytes(pipeline.config_path, modified)
        stale_artifact_code = _error_code(
            lambda: pipeline.get_stage_artifact("extract")
        )
        stale_state = pipeline.status()
        matching = [
            record
            for record in stale_state["invalidations"]
            if record.get("earliest_stage") == "estimate"
            and "smoothing" in record.get("changed_components", [])
        ]
        if matching:
            latest = matching[-1]
            invalidation_demo = {
                "earliest_stage": latest["earliest_stage"],
                "affected_stages": latest["affected_stages"],
                "changed_components": latest["changed_components"],
                "modified_smoothing": 0.6,
                "restored_smoothing": 0.5,
            }
    finally:
        pipeline.workspace.atomic_write_bytes(pipeline.config_path, original_config)

    final_state = pipeline.status()
    invalidation_history = final_state["invalidations"]
    upgraded_checks = [
        check
        for check in validate_artifact["checks"]
        if check["source_annotation_version"] == 1
    ]
    diagnostics = parse_artifact["diagnostics"]

    summary: dict[str, Any] = {
        "status": "ok",
        "seed": FIXED_SEED,
        "sample": {
            "document_shards": 4,
            "legacy_shards": 1,
            "document_count": parse_artifact["summary"]["document_count"],
            "seed_document_count": parse_artifact["summary"]["seed_document_count"],
        },
        "stop_resume": {
            "stopped_stage": stopped["stage"],
            "checkpoint_sequence_after_stop": seq_after_stop,
            "resumed_status": resumed["status"],
            "final_checkpoint_sequence": checkpoint_sequence,
            "final_checkpoint_stages": checkpoint_stages,
            "continuous": checkpoint_sequence
            == list(range(1, len(checkpoint_sequence) + 1)),
        },
        "extraction": extract_artifact["summary"],
        "validation": validate_artifact["summary"],
        "constraint_violation_count": len(violations),
        "constraint_violations": violations,
        "digests": {
            "extraction_sha256": extraction_sha256,
            "validation_sha256": validation_sha256,
            "run_sha256": run_digest,
            "config_version": config.digest(),
        },
        "diagnostics": {
            "codes": parse_artifact["summary"]["diagnostic_codes"],
            "align_fail": [
                item for item in diagnostics if item.get("code") == "E_ALIGN_FAIL"
            ],
        },
        "legacy_upgrade": {
            "upgraded_document_count": validate_artifact["summary"][
                "upgraded_document_count"
            ],
            "documents": [
                {
                    "doc_id": check["doc_id"],
                    "source_annotation_version": check["source_annotation_version"],
                    "source_upgrades": check["source_upgrades"],
                    "roundtrip_upgrades": sorted(
                        {
                            upgrade
                            for chain in check["chains"]
                            for upgrade in chain.get("upgrades", [])
                        }
                    ),
                }
                for check in upgraded_checks
            ],
        },
        "version_invalidation": {
            **invalidation_demo,
            "persisted_history_count": len(invalidation_history),
            "all_fresh_after_restore": final_state["all_fresh"],
        },
        "error_injections": {
            "E_UNSUPPORTED_VERSION": unsupported_code,
            "E_MISSING_ARTIFACT": missing_code,
            "E_STALE_CHECKPOINT": stale_checkpoint_code,
            "E_STALE_ARTIFACT": stale_artifact_code,
        },
        "final_stage_states": {
            stage: item["state"] for stage, item in final_state["stages"].items()
        },
    }
    expected_path = root / "artifacts" / "expected_demo_summary.json"
    if expected_path.exists():
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
        summary["fixed_digest_match"] = expected.get("run_sha256") == run_digest
        summary["expected_run_sha256"] = expected.get("run_sha256")
    else:
        summary["fixed_digest_match"] = None
        summary["expected_run_sha256"] = None

    output_path = root / "artifacts" / "demo_summary.json"
    pipeline.workspace.atomic_write_bytes(output_path, canonical_bytes(summary))
    return summary
