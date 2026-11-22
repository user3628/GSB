from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .aggregate import reject_result, summarize_results, threshold_reject_result
from .calibration import calibrate
from .config import Config
from .data import BatchSpec, load_batch_documents
from .errors import E_INVALID_CONFIG, E_INVALID_LABEL, PipelineError
from .features import extract_features
from .fingerprints import (
    artifact_digest,
    calibration_fingerprint,
    feature_fingerprint,
    summarize_fingerprint,
    train_score_fingerprint,
)
from .model import is_holdout, train_and_score
from .util import (
    atomic_write_json,
    atomic_write_jsonl,
    read_json,
    read_jsonl,
    sha256_file,
)


@dataclass
class CachePlan:
    batch: BatchSpec
    labels: tuple[str, ...]
    data_digest: str
    feature_fp: str
    earliest_index: int
    current_next_index: int
    cached_records: dict[int, dict[str, Any]]

    @property
    def fully_cached(self) -> bool:
        return self.earliest_index >= 4

    @property
    def requires_invalidation(self) -> bool:
        return self.earliest_index < self.current_next_index


@dataclass
class BatchExecution:
    plan: CachePlan
    stage_payloads: list[dict[str, Any]]
    temp_dir: Path


def _artifacts_valid(workspace: Path, record: dict[str, Any]) -> bool:
    for artifact in record.get("artifacts", []):
        path = workspace / artifact["path"]
        if not path.is_file() or sha256_file(path) != artifact["digest"]:
            return False
    return True


def build_cache_plan(
    batch: BatchSpec,
    config: Config,
    workspace: Path,
    checkpoint_state: dict[str, Any],
) -> CachePlan:
    labels = tuple(sorted(batch.declared_labels))
    if not labels:
        raise PipelineError(E_INVALID_CONFIG, "batch must declare at least one label", {"batch_id": batch.batch_id})
    unknown_labels = sorted(set(labels) - set(config.labels))
    if unknown_labels:
        raise PipelineError(
            E_INVALID_LABEL,
            "batch declares labels absent from configuration",
            {"batch_id": batch.batch_id, "labels": unknown_labels},
        )
    data_digest = sha256_file(batch.path)
    fp_features = feature_fingerprint(batch.batch_id, data_digest, batch.row_count, config)
    info = checkpoint_state["batches"].get(
        batch.batch_id,
        {"next_stage_index": 0, "stage_records": {}},
    )
    records = info.get("stage_records", {})
    cached: dict[int, dict[str, Any]] = {}
    current_next = int(info.get("next_stage_index", 0))
    earliest = 4
    for index in range(4):
        record = records.get(index)
        if index == 0:
            expected = fp_features
        elif index == 1:
            previous = records.get(0)
            if previous is None:
                earliest = 1
                break
            expected = train_score_fingerprint(
                fp_features, previous["artifact_digest"], labels, config
            )
        elif index == 2:
            previous = records.get(1)
            if previous is None:
                earliest = 2
                break
            expected = calibration_fingerprint(
                records[1]["input_fingerprint"], previous["artifact_digest"], labels, config
            )
        else:
            previous = records.get(2)
            if previous is None:
                earliest = 3
                break
            expected = summarize_fingerprint(
                records[2]["input_fingerprint"], previous["artifact_digest"], labels, config
            )
        if record is None:
            earliest = index
            break
        if record.get("input_fingerprint") != expected or not _artifacts_valid(workspace, record):
            earliest = index
            break
        cached[index] = record
    return CachePlan(
        batch=batch,
        labels=labels,
        data_digest=data_digest,
        feature_fp=fp_features,
        earliest_index=earliest,
        current_next_index=current_next,
        cached_records=cached,
    )


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, value)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(path, rows)


def _artifact_ref(temp_dir: Path, relative: str, summary: dict[str, Any]) -> dict[str, Any]:
    path = temp_dir / relative
    return {
        "path": relative,
        "digest": sha256_file(path),
        "summary": summary,
    }


def _cached_path(workspace: Path, record: dict[str, Any], suffix: str = "") -> str:
    candidates = [
        artifact["path"]
        for artifact in record["artifacts"]
        if artifact["path"].endswith(suffix)
    ]
    if not candidates:
        raise PipelineError(
            E_INVALID_CONFIG,
            "cached checkpoint artifact is missing an expected suffix",
            {"suffix": suffix, "artifacts": [item["path"] for item in record["artifacts"]]},
        )
    return candidates[0]


def _payload_artifacts(
    stage_payloads: list[dict[str, Any]],
    cached_records: dict[int, dict[str, Any]],
    stage_index: int,
) -> list[dict[str, Any]]:
    for payload in stage_payloads:
        if payload["stage_index"] == stage_index:
            return payload["artifacts"]
    if stage_index in cached_records:
        return cached_records[stage_index]["artifacts"]
    raise PipelineError(
        E_INVALID_CONFIG,
        "required stage artifacts are unavailable",
        {"stage_index": stage_index},
    )


def process_batch(
    plan: CachePlan,
    config: Config,
    workspace: Path,
    temp_root: Path,
) -> BatchExecution:
    batch = plan.batch
    temp_dir = temp_root / batch.batch_id
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    documents_list = load_batch_documents(batch)
    documents = {document["doc_id"]: document for document in documents_list}
    observed_labels = {document.get("label") for document in documents_list}
    invalid_labels = sorted(label for label in observed_labels if label not in plan.labels)
    if invalid_labels:
        raise PipelineError(
            E_INVALID_LABEL,
            "batch data contains labels not declared in metadata",
            {"batch_id": batch.batch_id, "labels": invalid_labels},
        )
    labels = plan.labels
    stage_payloads: list[dict[str, Any]] = []
    feature_records: list[dict[str, Any]] = []
    if plan.earliest_index <= 0:
        feature_records = [extract_features(document, config.hash_dim) for document in documents_list]
        feature_records.sort(key=lambda row: row["doc_id"])
        feature_rel = f"features/{batch.batch_id}.jsonl"
        diagnostic_rel = f"diagnostics/features/{batch.batch_id}.jsonl"
        _write_jsonl(temp_dir / feature_rel, feature_records)
        diagnostics = [
            {
                "batch_id": batch.batch_id,
                "doc_id": feature["doc_id"],
                "error_code": feature["error_code"],
            }
            for feature in feature_records
            if feature.get("error_code")
        ]
        _write_jsonl(temp_dir / diagnostic_rel, diagnostics)
        feature_artifacts = [
            _artifact_ref(
                temp_dir,
                feature_rel,
                {
                    "doc_count": len(feature_records),
                    "token_count": sum(row["token_count"] for row in feature_records),
                    "collision_count": sum(row["hash_collision_count"] for row in feature_records),
                },
            ),
            _artifact_ref(
                temp_dir,
                diagnostic_rel,
                {"diagnostic_count": len(diagnostics)},
            ),
        ]
        stage_payloads.append(
            {
                "stage": "features",
                "stage_index": 0,
                "input_fingerprint": plan.feature_fp,
                "artifacts": feature_artifacts,
            }
        )
        feature_fp = plan.feature_fp
        feature_artifact_digest = artifact_digest(feature_artifacts)
    else:
        feature_record = plan.cached_records[0]
        feature_rel = _cached_path(workspace, feature_record, f"features/{batch.batch_id}.jsonl")
        feature_records = read_jsonl(workspace / feature_rel)
        feature_records.sort(key=lambda row: row["doc_id"])
        feature_fp = plan.feature_fp
        feature_artifact_digest = feature_record["artifact_digest"]
    if plan.earliest_index <= 1:
        holdout_ids = {
            doc_id
            for doc_id in documents
            if is_holdout(
                doc_id,
                config.calibration.fold_modulus,
                config.calibration.fold_remainder,
            )
        }
        train_ids = set(documents) - holdout_ids
        full_model, full_scores = train_and_score(
            batch_id=batch.batch_id,
            features=feature_records,
            documents=documents,
            labels=labels,
            smoothing=config.smoothing,
            unknown_token=config.unknown_token,
            hash_dim=config.hash_dim,
            training_scope="full",
        )
        holdout_model, holdout_scores = train_and_score(
            batch_id=batch.batch_id,
            features=feature_records,
            documents=documents,
            labels=labels,
            smoothing=config.smoothing,
            unknown_token=config.unknown_token,
            hash_dim=config.hash_dim,
            training_scope="holdout",
            training_doc_ids=train_ids,
            scoring_doc_ids=holdout_ids,
        )
        model_rel = f"models/{batch.batch_id}.json"
        holdout_model_rel = f"models/{batch.batch_id}.holdout.json"
        scores_rel = f"scores/{batch.batch_id}.jsonl"
        holdout_scores_rel = f"scores/{batch.batch_id}.holdout.jsonl"
        _write_json(temp_dir / model_rel, full_model)
        _write_json(temp_dir / holdout_model_rel, holdout_model)
        _write_jsonl(temp_dir / scores_rel, full_scores)
        _write_jsonl(temp_dir / holdout_scores_rel, holdout_scores)
        train_artifacts = [
            _artifact_ref(
                temp_dir,
                scores_rel,
                {"score_count": len(full_scores), "model_digest": full_model["model_digest"]},
            ),
            _artifact_ref(
                temp_dir,
                model_rel,
                {
                    "model_digest": full_model["model_digest"],
                    "vocabulary_size": full_model["training"]["vocabulary_size"],
                },
            ),
            _artifact_ref(
                temp_dir,
                holdout_scores_rel,
                {"score_count": len(holdout_scores), "model_digest": holdout_model["model_digest"]},
            ),
            _artifact_ref(
                temp_dir,
                holdout_model_rel,
                {"model_digest": holdout_model["model_digest"]},
            ),
        ]
        train_fp = train_score_fingerprint(
            feature_fp, feature_artifact_digest, labels, config
        )
        stage_payloads.append(
            {
                "stage": "train_score",
                "stage_index": 1,
                "input_fingerprint": train_fp,
                "artifacts": train_artifacts,
            }
        )
        train_artifact_digest = artifact_digest(train_artifacts)
    else:
        train_record = plan.cached_records[1]
        train_fp = train_record["input_fingerprint"]
        train_artifact_digest = train_record["artifact_digest"]
        holdout_scores_rel = _cached_path(
            workspace, train_record, f"scores/{batch.batch_id}.holdout.jsonl"
        )
        holdout_scores = read_jsonl(workspace / holdout_scores_rel)
        full_scores_rel = _cached_path(workspace, train_record, f"scores/{batch.batch_id}.jsonl")
        full_scores = read_jsonl(workspace / full_scores_rel)
        holdout_ids = {
            doc_id
            for doc_id in documents
            if is_holdout(
                doc_id,
                config.calibration.fold_modulus,
                config.calibration.fold_remainder,
            )
        }
        train_ids = set(documents) - holdout_ids
    if plan.earliest_index <= 2:
        holdout_features = [
            feature for feature in feature_records if feature["doc_id"] in holdout_ids
        ]
        label_counts: dict[str, int] = {label: 0 for label in labels}
        for document in documents.values():
            label = document.get("label")
            if label in label_counts:
                label_counts[label] += 1
        calibration = calibrate(
            batch_id=batch.batch_id,
            labels=labels,
            documents=documents,
            holdout_features=holdout_features,
            holdout_scores=holdout_scores,
            train_doc_ids=train_ids,
            holdout_doc_ids=holdout_ids,
            thresholds=config.thresholds,
            target_fpr=config.target_fpr,
            threshold_max=config.threshold_max,
            min_samples=config.calibration.min_samples_per_class,
            fold_modulus=config.calibration.fold_modulus,
            fold_remainder=config.calibration.fold_remainder,
            batch_label_counts=label_counts,
        )
        calibration_rel = f"calibration/{batch.batch_id}.json"
        _write_json(temp_dir / calibration_rel, calibration)
        calibration_artifacts = [
            _artifact_ref(
                temp_dir,
                calibration_rel,
                {
                    "review_class_count": sum(
                        1 for item in calibration["classes"].values() if item["review_required"]
                    ),
                    "calibration_digest": calibration["calibration_digest"],
                },
            )
        ]
        calibration_fp = calibration_fingerprint(
            train_fp, train_artifact_digest, labels, config
        )
        stage_payloads.append(
            {
                "stage": "calibration",
                "stage_index": 2,
                "input_fingerprint": calibration_fp,
                "artifacts": calibration_artifacts,
            }
        )
        calibration_artifact_digest = artifact_digest(calibration_artifacts)
    else:
        calibration_record = plan.cached_records[2]
        calibration_fp = calibration_record["input_fingerprint"]
        calibration_artifact_digest = calibration_record["artifact_digest"]
        calibration_rel = _cached_path(
            workspace, calibration_record, f"calibration/{batch.batch_id}.json"
        )
        calibration = read_json(workspace / calibration_rel)
    if plan.earliest_index <= 3:
        score_by_doc = {row["doc_id"]: row for row in full_scores}
        score_lines = {row["doc_id"]: index for index, row in enumerate(full_scores, 1)}
        result_rows: list[dict[str, Any]] = []
        feature_by_doc = {row["doc_id"]: row for row in feature_records}
        calibration_ref = {
            "path": f"calibration/{batch.batch_id}.json",
            "digest": calibration_artifact_digest,
            "calibration_digest": calibration["calibration_digest"],
        }
        for document in documents_list:
            doc_id = document["doc_id"]
            feature = feature_by_doc[doc_id]
            score_row = score_by_doc[doc_id]
            train_artifacts = _payload_artifacts(stage_payloads, plan.cached_records, 1)
            score_ref = {
                "path": f"scores/{batch.batch_id}.jsonl",
                "line": score_lines[doc_id],
                "digest": next(
                    artifact["digest"]
                    for artifact in train_artifacts
                    if artifact["path"].endswith(f"scores/{batch.batch_id}.jsonl")
                ),
            }
            if feature.get("error_code"):
                result_rows.append(
                    reject_result(
                        document, feature["error_code"], calibration_ref, score_ref
                    )
                )
                continue
            if score_row["score_error_codes"]:
                result_rows.append(
                    reject_result(
                        document,
                        score_row["score_error_codes"][0],
                        calibration_ref,
                        score_ref,
                    )
                )
                continue
            class_decisions = {
                label: {
                    "probability": score_row["probabilities"][label],
                    "threshold": calibration["classes"][label]["threshold"],
                    "accepted": score_row["probabilities"][label]
                    >= calibration["classes"][label]["threshold"],
                    "calibration_review_required": calibration["classes"][label]["review_required"],
                }
                for label in labels
            }
            accepted_labels = [label for label in labels if class_decisions[label]["accepted"]]
            if not accepted_labels:
                result_rows.append(
                    threshold_reject_result(
                        document, score_row, calibration, calibration_ref, score_ref
                    )
                )
                continue
            predicted = sorted(
                accepted_labels,
                key=lambda label: (-score_row["probabilities"][label], label),
            )[0]
            review_reasons = list(calibration["classes"][predicted]["review_reasons"])
            review_required = bool(review_reasons)
            result_rows.append(
                {
                    "batch_id": batch.batch_id,
                    "batch_no": batch.batch_no,
                    "doc_id": doc_id,
                    "true_label": document.get("label"),
                    "language": document.get("lang"),
                    "channel": document.get("channel"),
                    "risk_level": document.get("risk_level"),
                    "error_code": None,
                    "predicted_label": predicted,
                    "decision": "REVIEW" if review_required else "ACCEPT",
                    "decision_reasons": review_reasons,
                    "review_required": review_required,
                    "metric_eligible": True,
                    "selected_probability": score_row["probabilities"][predicted],
                    "probabilities": score_row["probabilities"],
                    "log_scores": score_row["log_scores"],
                    "class_decisions": class_decisions,
                    "calibration_ref": calibration_ref,
                    "score_evidence": score_ref,
                }
            )
        result_rows.sort(key=lambda row: row["doc_id"])
        for row in result_rows:
            row["batch_no"] = batch.batch_no
        languages = tuple(sorted(set(batch.languages) | {doc.get("lang") for doc in documents_list if doc.get("lang")}))
        channels = tuple(sorted(set(batch.channels) | {doc.get("channel") for doc in documents_list if doc.get("channel")}))
        batch_summary = summarize_results(
            result_rows,
            labels=labels,
            languages=languages,
            channels=channels,
            class_weights={label: config.class_weights[label] for label in labels},
        )
        batch_summary["batch_id"] = batch.batch_id
        batch_summary["batch_no"] = batch.batch_no
        results_rel = f"results/{batch.batch_id}.jsonl"
        summary_rel = f"summaries/{batch.batch_id}.json"
        _write_jsonl(temp_dir / results_rel, result_rows)
        _write_json(temp_dir / summary_rel, batch_summary)
        summary_artifacts = [
            _artifact_ref(
                temp_dir,
                results_rel,
                {
                    "doc_count": len(result_rows),
                    "review_count": sum(1 for row in result_rows if row.get("review_required")),
                    "error_count": sum(1 for row in result_rows if row.get("error_code")),
                },
            ),
            _artifact_ref(
                temp_dir,
                summary_rel,
                {"summary_digest": batch_summary["summary_digest"]},
            ),
        ]
        summary_fp = summarize_fingerprint(
            calibration_fp, calibration_artifact_digest, labels, config
        )
        stage_payloads.append(
            {
                "stage": "summarize",
                "stage_index": 3,
                "input_fingerprint": summary_fp,
                "artifacts": summary_artifacts,
            }
        )
    return BatchExecution(plan=plan, stage_payloads=stage_payloads, temp_dir=temp_dir)
