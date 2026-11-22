from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Callable

from .alignment_stage import alignment_input_descriptor_from_records, run_alignment
from .checkpoints import CheckpointStore
from .config import STAGE_ORDER, TIERS, ExperimentConfig
from .errors import GracefulStop, MtlError
from .evaluation_stage import evaluation_input_descriptor, run_evaluation
from .policy_stage import (
    load_policy,
    make_policy_decision,
    policy_input_descriptor,
    run_policy,
)
from .split_stage import run_split, split_input_descriptor
from .translation import decode_input_descriptor, run_decode
from .util import (
    canonical_json,
    read_json,
    sha256_file,
    sha256_json,
    write_json_atomic,
)


class StopController:
    def __init__(self, event: threading.Event, stop_after_tier: str | None = None):
        self.event = event
        self.stop_after_tier = stop_after_tier
        self.tier = ""
        self.stage = ""

    def begin(self, tier: str, stage: str) -> None:
        self.tier = tier
        self.stage = stage
        self.check()

    def check(self) -> None:
        if self.event.is_set():
            raise GracefulStop(tier=self.tier, stage=self.stage)


def resolve_dataset(root: Path, data_dir: str | Path) -> tuple[Path, dict[str, Any]]:
    data_path = Path(data_dir)
    if not data_path.is_absolute():
        data_path = (root / data_path).resolve()
    manifest_path = data_path / "manifest.json"
    if not manifest_path.exists():
        raise MtlError(
            code="E_INVALID_STAGE",
            message="dataset manifest is missing; run the generator first",
            details={"manifest_path": str(manifest_path)},
        )
    manifest = read_json(manifest_path)
    resolved = json.loads(canonical_json(manifest))
    for tier in TIERS:
        if tier not in resolved["tiers"]:
            continue
        tier_entry = resolved["tiers"][tier]
        corpus = data_path / "corpora" / f"{tier}.jsonl"
        references = data_path / "references" / f"{tier}.jsonl"
        if not corpus.exists() and (data_path / tier_entry["corpus_file"]).exists():
            corpus = data_path / tier_entry["corpus_file"]
        if not references.exists() and (data_path / tier_entry["reference_file"]).exists():
            references = data_path / tier_entry["reference_file"]
        tier_entry["corpus_file"] = str(corpus.resolve())
        tier_entry["reference_file"] = str(references.resolve())
    return data_path, resolved


def _record_by_stage(store: CheckpointStore, tier: str) -> dict[str, dict[str, Any]]:
    return {record["stage"]: record for record in store.records(tier)}


def _split_descriptor(
    root: Path, tier: str, config: ExperimentConfig, manifest: dict[str, Any]
) -> dict[str, Any]:
    return split_input_descriptor(root, tier, config, manifest)


def _align_descriptor(
    tier: str,
    config: ExperimentConfig,
    records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    split_record = records.get("split")
    if not split_record:
        return None
    return alignment_input_descriptor_from_records(
        tier, config, split_record["artifact_digest"]
    )


def _policy_descriptor(
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    alignment_record = records.get("align")
    if not alignment_record:
        return None
    return policy_input_descriptor(
        tier, config, manifest, alignment_record["artifact_digest"]
    )


def _decode_descriptor(
    root: Path,
    tier: str,
    config: ExperimentConfig,
    store: CheckpointStore,
    records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    split_record = records.get("split")
    alignment_record = records.get("align")
    policy_record = records.get("policy")
    if not split_record or not alignment_record or not policy_record:
        return None
    policy_artifact = load_policy(store.artifact_path(policy_record))
    selected = policy_artifact["decision"]["selected"]
    return decode_input_descriptor(
        tier,
        config,
        split_digest=split_record["artifact_digest"],
        alignment_digest=alignment_record["artifact_digest"],
        policy_digest=policy_record["artifact_digest"],
        alignment_mode=selected["alignment_mode"],
        phrase_limit=int(selected["phrase_limit"]),
        beam_width=int(selected["beam_width"]),
    )


def _evaluation_descriptor(
    root: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    decode_record = records.get("decode")
    if not decode_record:
        return None
    reference_path = root / manifest["reference_file"]
    return evaluation_input_descriptor(
        tier,
        config,
        decode_digest=decode_record["artifact_digest"],
        reference_sha256=sha256_file(reference_path),
    )


def _descriptor_for_stage(
    stage: str,
    root: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    store: CheckpointStore,
    records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if stage == "split":
        return _split_descriptor(root, tier, config, manifest)
    if stage == "align":
        return _align_descriptor(tier, config, records)
    if stage == "policy":
        return _policy_descriptor(tier, config, manifest, records)
    if stage == "decode":
        return _decode_descriptor(root, tier, config, store, records)
    if stage == "evaluate":
        return _evaluation_descriptor(root, tier, config, manifest, records)
    raise MtlError(
        code="E_INVALID_STAGE",
        message="unknown pipeline stage",
        details={"stage": stage},
    )


_FIELD_REASONS = {
    "corpus_sha256": "corpus_content_changed",
    "bpe_merges": "bpe_rules_config_changed",
    "effective_bpe_limit": "bpe_rules_config_changed",
    "bpe_min_count": "bpe_rules_config_changed",
    "normalizer": "normalization_config_changed",
    "tokenizer": "normalization_config_changed",
    "smoothing": "smoothing_changed",
    "discard_threshold": "discard_threshold_changed",
    "em_iterations": "alignment_iterations_changed",
    "budget": "policy_budget_changed",
    "beam_width_override": "decode_width_changed",
    "phrase_limit_override": "phrase_limit_changed",
    "manifest_feature_digest": "scale_manifest_changed",
    "beam_width": "decode_width_changed",
    "phrase_limit": "phrase_limit_changed",
    "phrase_max_len": "phrase_max_len_changed",
    "reference_sha256": "reference_changed",
    "metric_max_order": "metric_config_changed",
    "confidence_level": "metric_config_changed",
}


def _changed_fields(
    old: dict[str, Any] | None, new: dict[str, Any] | None
) -> list[str]:
    if old is None or new is None:
        return []
    keys = sorted(set(old) | set(new))
    return [key for key in keys if canonical_json(old.get(key)) != canonical_json(new.get(key))]


def _reasons_for_changed_fields(
    stage: str, fields: list[str], *, upstream: bool = False
) -> list[str]:
    if upstream:
        return ["upstream_stage_invalid"]
    if not fields:
        return [f"{stage}_checkpoint_invalid"]
    reasons: list[str] = []
    for field in fields:
        reasons.append(_FIELD_REASONS.get(field, f"{stage}_{field}_changed"))
    return sorted(set(reasons))


def _validate_record(
    *,
    record: dict[str, Any] | None,
    stage: str,
    expected_sequence: int,
    descriptor: dict[str, Any] | None,
    store: CheckpointStore,
) -> tuple[bool, list[str], list[str]]:
    if record is None:
        return False, [], ["missing_checkpoint"]
    if record.get("stage") != stage:
        return False, [], ["stage_record_mismatch"]
    if int(record.get("sequence", -1)) != expected_sequence:
        return False, [], ["non_contiguous_sequence"]
    if descriptor is None:
        return False, [], ["missing_prerequisite_stage"]
    old_descriptor = record.get("input_descriptor")
    if old_descriptor != descriptor:
        fields = _changed_fields(old_descriptor, descriptor)
        return False, fields, _reasons_for_changed_fields(stage, fields)
    if record.get("input_fingerprint") != sha256_json(descriptor):
        return False, [], ["record_fingerprint_mismatch"]
    if not store.is_record_file_valid(record):
        return False, [], ["checkpoint_record_missing_or_corrupt"]
    if not store.is_record_artifact_valid(record):
        return False, [], ["artifact_digest_mismatch"]
    return True, [], []


def query_status(
    root: Path,
    data_dir: str | Path,
    config: ExperimentConfig,
) -> dict[str, Any]:
    root = root.resolve()
    _, manifest = resolve_dataset(root, data_dir)
    store = CheckpointStore(root)
    tiers_status: dict[str, Any] = {}
    for tier in config.tiers:
        tier_manifest = manifest["tiers"][tier]
        records = list(store.records(tier))
        by_stage = {record["stage"]: record for record in records}
        reused: list[str] = []
        affected: list[str] = []
        earliest: str | None = None
        reasons: list[str] = []
        changed_fields: list[str] = []
        invalidated = False
        descriptor_digests: dict[str, str | None] = {}
        for index, stage in enumerate(STAGE_ORDER):
            record = by_stage.get(stage)
            descriptor = (
                None
                if invalidated
                else _descriptor_for_stage(
                    stage, root, tier, config, tier_manifest, store, by_stage
                )
            )
            descriptor_digests[stage] = sha256_json(descriptor) if descriptor is not None else None
            if not invalidated:
                valid, fields, stage_reasons = _validate_record(
                    record=record,
                    stage=stage,
                    expected_sequence=index + 1,
                    descriptor=descriptor,
                    store=store,
                )
                if valid:
                    reused.append(stage)
                    continue
                invalidated = True
                earliest = stage
                reasons = stage_reasons
                changed_fields = fields
            affected.append(stage)
        tiers_status[tier] = {
            "checkpoint_records": [
                {
                    "stage": record["stage"],
                    "sequence": record["sequence"],
                    "input_fingerprint": record["input_fingerprint"],
                    "decision_fingerprint": record["decision_fingerprint"],
                    "artifact_digest": record["artifact_digest"],
                }
                for record in records
            ],
            "current_input_fingerprints": descriptor_digests,
            "would_invalidate": {
                "earliest_stage": earliest,
                "changed_fields": changed_fields,
                "reasons": reasons,
                "reused_stages": reused,
                "invalidated_stages": affected,
                "next_sequence": len(reused) + 1 if earliest else None,
            },
            "is_current": earliest is None and len(reused) == len(STAGE_ORDER),
        }
    return {
        "schema_version": 1,
        "engine_version": config.engine_version,
        "config": config.to_dict(),
        "tiers": tiers_status,
        "latest_invalidation": store.latest_invalidation(),
    }


def _build_stage(
    *,
    stage: str,
    root: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    store: CheckpointStore,
    records: dict[str, dict[str, Any]],
    decision_fingerprint: str,
    stop_check: Callable[[], None],
) -> dict[str, Any]:
    staged_dir = store.make_staging_dir(tier, stage)
    try:
        if stage == "split":
            descriptor = _split_descriptor(root, tier, config, manifest)
            run_split(
                root,
                tier,
                config,
                manifest,
                staged_dir,
                stop_check=stop_check,
            )
        elif stage == "align":
            descriptor = _align_descriptor(tier, config, records)
            split_record = records.get("split")
            if descriptor is None or split_record is None:
                raise MtlError(
                    code="E_INVALID_STAGE",
                    message="align stage requires a completed split checkpoint",
                    details={"tier": tier, "stage": stage},
                )
            run_alignment(
                store.artifact_path(split_record),
                tier,
                config,
                staged_dir,
                split_artifact_digest=split_record["artifact_digest"],
                stop_check=stop_check,
            )
        elif stage == "policy":
            descriptor = _policy_descriptor(tier, config, manifest, records)
            alignment_record = records.get("align")
            if descriptor is None or alignment_record is None:
                raise MtlError(
                    code="E_INVALID_STAGE",
                    message="policy stage requires a completed align checkpoint",
                    details={"tier": tier, "stage": stage},
                )
            run_policy(
                tier,
                manifest,
                config,
                alignment_record["artifact_digest"],
                staged_dir,
            )
            stop_check()
        elif stage == "decode":
            descriptor = _decode_descriptor(root, tier, config, store, records)
            split_record = records.get("split")
            alignment_record = records.get("align")
            policy_record = records.get("policy")
            if (
                descriptor is None
                or split_record is None
                or alignment_record is None
                or policy_record is None
            ):
                raise MtlError(
                    code="E_INVALID_STAGE",
                    message="decode stage requires split, align and policy checkpoints",
                    details={"tier": tier, "stage": stage},
                )
            run_decode(
                store.artifact_path(split_record),
                store.artifact_path(alignment_record),
                store.artifact_path(policy_record),
                tier,
                config,
                staged_dir,
                split_artifact_digest=split_record["artifact_digest"],
                alignment_artifact_digest=alignment_record["artifact_digest"],
                policy_artifact_digest=policy_record["artifact_digest"],
                stop_check=stop_check,
            )
        elif stage == "evaluate":
            descriptor = _evaluation_descriptor(root, tier, config, manifest, records)
            decode_record = records.get("decode")
            split_record = records.get("split")
            if descriptor is None or decode_record is None or split_record is None:
                raise MtlError(
                    code="E_INVALID_STAGE",
                    message="evaluate stage requires split and decode checkpoints",
                    details={"tier": tier, "stage": stage},
                )
            reference_path = root / manifest["reference_file"]
            run_evaluation(
                store.artifact_path(split_record),
                store.artifact_path(decode_record),
                reference_path,
                tier,
                config,
                manifest,
                staged_dir,
                decode_artifact_digest=decode_record["artifact_digest"],
                reference_sha256=sha256_file(reference_path),
                stop_check=stop_check,
            )
            stop_check()
        else:
            raise MtlError(
                code="E_INVALID_STAGE",
                message="unknown stage",
                details={"stage": stage},
            )
        assert descriptor is not None
        record = store.publish_stage(
            tier=tier,
            stage=stage,
            staged_dir=staged_dir,
            input_descriptor=descriptor,
            decision_fingerprint=decision_fingerprint,
        )
        records[stage] = record
        return record
    except Exception:
        store.discard_staging_dir(staged_dir)
        raise


def _tier_summary(
    root: Path,
    tier: str,
    manifest: dict[str, Any],
    store: CheckpointStore,
) -> dict[str, Any]:
    records = store.records(tier)
    by_stage = {record["stage"]: record for record in records}
    split_summary = read_json(store.artifact_path(by_stage["split"]) / "summary.json")
    bpe = read_json(store.artifact_path(by_stage["split"]) / "bpe.json")
    policy_artifact = load_policy(store.artifact_path(by_stage["policy"]))
    decode_summary = read_json(store.artifact_path(by_stage["decode"]) / "summary.json")
    metrics = read_json(store.artifact_path(by_stage["evaluate"]) / "metrics.json")
    selected = policy_artifact["decision"]["selected"]
    manifest_core = {
        key: value
        for key, value in manifest.items()
        if key not in {"corpus_file", "reference_file"}
    }
    return {
        "tier": tier,
        "manifest": manifest_core,
        "split": split_summary,
        "bpe": {
            "merge_limit": bpe["merge_limit"],
            "learned_rule_count": bpe["learned_rule_count"],
            "rule_digest": bpe["rule_digest"],
        },
        "policy": {
            "alignment_mode": selected["alignment_mode"],
            "phrase_limit": selected["phrase_limit"],
            "beam_width": selected["beam_width"],
            "score": selected["score"],
            "estimated_work": selected["estimated_work"],
            "decision_fingerprint": policy_artifact["decision_fingerprint"],
            "uncertainty": policy_artifact["decision"]["uncertainty"],
            "selection_reason": policy_artifact["decision"]["selection_reason"],
        },
        "decode": decode_summary,
        "metrics": metrics,
        "checkpoints": [
            {
                "stage": record["stage"],
                "sequence": record["sequence"],
                "input_fingerprint": record["input_fingerprint"],
                "decision_fingerprint": record["decision_fingerprint"],
                "artifact_digest": record["artifact_digest"],
            }
            for record in records
        ],
    }


def build_summary(
    root: Path,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    store: CheckpointStore,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema_version": 1,
        "engine_version": config.engine_version,
        "generator": {
            "version": manifest.get("generator_version"),
            "seed": manifest.get("generator_seed"),
        },
        "config": config.to_dict(),
        "tiers": {},
    }
    for tier in config.tiers:
        body["tiers"][tier] = _tier_summary(root, tier, manifest["tiers"][tier], store)
    digest = sha256_json(body)
    body["summary_digest"] = digest
    return body


def _write_run_report(
    root: Path,
    widget: dict[str, Any],
) -> None:
    write_json_atomic(root / "artifacts" / "last_run_report.json", widget)


def _checkpoint_counts(store: CheckpointStore, tiers: tuple[str, ...]) -> dict[str, int]:
    return {tier: len(store.records(tier)) for tier in tiers}


def run_experiment(
    root: Path,
    data_dir: str | Path,
    config: ExperimentConfig,
    *,
    stop_event: threading.Event | None = None,
    stop_after_tier: str | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    data_path, manifest = resolve_dataset(root, data_dir)
    store = CheckpointStore(root)
    controller = StopController(stop_event or threading.Event(), stop_after_tier)
    reused_by_tier: dict[str, list[str]] = {tier: [] for tier in config.tiers}
    invalidation_events: list[dict[str, Any]] = []
    try:
        for tier in config.tiers:
            tier_manifest = manifest["tiers"][tier]
            planned_decision = make_policy_decision(tier, tier_manifest, config)
            decision_fingerprint = planned_decision["decision_fingerprint"]
            records = _record_by_stage(store, tier)
            restart_index = len(STAGE_ORDER)
            for index, stage in enumerate(STAGE_ORDER):
                controller.begin(tier, stage)
                descriptor = _descriptor_for_stage(
                    stage, root, tier, config, tier_manifest, store, records
                )
                record = records.get(stage)
                valid, fields, reasons = _validate_record(
                    record=record,
                    stage=stage,
                    expected_sequence=index + 1,
                    descriptor=descriptor,
                    store=store,
                )
                if valid:
                    reused_by_tier[tier].append(stage)
                    continue
                restart_index = index
                event = store.invalidate_from(
                    tier=tier,
                    stage=stage,
                    reasons=reasons,
                    detail={
                        "changed_fields": fields,
                        "record_sequence": record.get("sequence") if record else None,
                        "expected_sequence": index + 1,
                        "current_input_fingerprint": sha256_json(descriptor)
                        if descriptor is not None
                        else None,
                    },
                )
                if event.get("earliest_stage"):
                    invalidation_events.append(event)
                records = _record_by_stage(store, tier)
                break
            if restart_index < len(STAGE_ORDER):
                for stage in STAGE_ORDER[restart_index:]:
                    controller.begin(tier, stage)
                    record = _build_stage(
                        stage=stage,
                        root=root,
                        tier=tier,
                        config=config,
                        manifest=tier_manifest,
                        store=store,
                        records=records,
                        decision_fingerprint=decision_fingerprint,
                        stop_check=controller.check,
                    )
                    records[stage] = record
            if stop_after_tier == tier and tier != config.tiers[-1]:
                controller.stage = "tier_boundary"
                raise GracefulStop(tier=tier, stage="tier_boundary")
        summary = build_summary(root, config, manifest, store)
        summary_path = root / "artifacts" / "run_summary.json"
        write_json_atomic(summary_path, summary)
        _write_run_report(
            root,
            {
                "status": "completed",
                "summary_digest": summary["summary_digest"],
                "reused_stages": reused_by_tier,
                "invalidations": invalidation_events,
                "checkpoint_counts": _checkpoint_counts(store, config.tiers),
            },
        )
        return summary
    except GracefulStop:
        _write_run_report(
            root,
            {
                "status": "stopped",
                "tier": controller.tier,
                "stage": controller.stage,
                "reused_stages": reused_by_tier,
                "invalidations": invalidation_events,
                "checkpoint_counts": _checkpoint_counts(store, config.tiers),
                "resume": "rerun the same command without --stop-after-tier",
            },
        )
        raise

