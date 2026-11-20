from __future__ import annotations

import contextlib
import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import STAGE_INDEX, STAGE_NAMES, WorkbenchConfig, load_config
from .manifest import Dataset, ManifestSample, load_dataset, scores_digest, truth_digest
from .metrics import aggregate_metrics
from .stages import metric_contribution, preprocess_artifact, report_artifact, score_artifact
from .store import CheckpointStore
from .util import (
    WorkbenchError,
    atomic_write_bytes,
    atomic_write_json,
    digest,
    ensure_dir,
    exclusive_lock,
    file_sha256,
    pretty_json,
    read_json,
    relpath,
)


STOP_PATH = Path("checkpoints/control/stop.request")
RUN_STATE_PATH = Path("checkpoints/control/run_state.json")
RUN_LOCK_PATH = Path("checkpoints/control/run.lock")


@dataclass
class SamplePlan:
    sample: ManifestSample
    base_parts: dict[str, dict[str, Any]]
    current_parts: dict[str, dict[str, Any]] = field(default_factory=dict)
    current_fingerprints: dict[str, str] = field(default_factory=dict)
    existing: dict[str, dict[str, Any] | None] = field(default_factory=dict)
    earliest_stage: str | None = None

    @property
    def earliest_index(self) -> int | None:
        return STAGE_INDEX[self.earliest_stage] if self.earliest_stage else None


@dataclass
class PipelineResult:
    status: str
    published: int
    reused: int
    stop_reason: str | None
    report_path: str | None
    report_sha256: str | None
    metrics: dict[str, Any] | None
    failures: int
    conflicts: int
    invalidations: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "published": self.published,
            "reused": self.reused,
            "stop_reason": self.stop_reason,
            "report_path": self.report_path,
            "report_sha256": self.report_sha256,
            "metrics": self.metrics,
            "failures": self.failures,
            "conflicts": self.conflicts,
            "invalidations": self.invalidations,
        }


class StopRequested(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def run_pipeline(
    root: Path,
    *,
    config_path: str | Path = "configs/default.json",
    mode: str = "run",
    clear_stop_flag: bool = True,
    stop_after_checkpoints: int | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> PipelineResult:
    root = root.resolve()
    prepare_directories(root)
    if clear_stop_flag:
        clear_stop(root)
    with exclusive_lock(root / RUN_LOCK_PATH, blocking=False):
        config = load_config(root, config_path)
        dataset = load_dataset(root, config)
        store = CheckpointStore(root)
        run_id = new_run_id()
        started_at = utc_now()
        state: dict[str, Any] = {
            "status": "running",
            "mode": mode,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": None,
            "pid": os.getpid(),
            "stage": "planning",
            "sample_key": None,
            "published": 0,
            "reused": 0,
            "changed_samples": 0,
            "stop_reason": None,
            "report_path": None,
            "report_sha256": None,
            "config_digest": config.digest,
            "manifest_sha256": dataset.manifest_sha256,
            "injection_sha256": dataset.injection_sha256,
        }
        _write_run_state(root, state)
        published = 0
        reused = 0
        try:
            existing = {stage: store.all(stage) for stage in STAGE_NAMES}
            image_hashes = {
                sample.key: _image_hash(dataset, sample) for sample in dataset.samples
            }
            plans = [
                plan_sample(sample, dataset, config, existing, image_hashes[sample.key])
                for sample in dataset.samples
            ]
            invalidation_entries = _invalidate_plans(store, plans, existing)
            state["changed_samples"] = sum(1 for plan in plans if plan.earliest_stage)
            state["stage"] = "preprocess"
            _write_run_state(root, state)

            def stop_point(stage_name: str, sample: ManifestSample | None = None) -> None:
                nonlocal published
                state["stage"] = stage_name
                state["sample_key"] = sample.key if sample else None
                state["published"] = published
                state["reused"] = reused
                _write_run_state(root, state)
                reason = _stop_reason(root, published, stop_after_checkpoints)
                if reason:
                    raise StopRequested(reason)

            # Stage 1: decode, geometry validation, deterministic profile.
            for sample in dataset.samples:
                stop_point("preprocess", sample)
                parts = plan_stage1_parts(sample, dataset, config, image_hashes[sample.key])
                fingerprint = fingerprint_parts(parts)
                current = store.get("preprocess", sample.key)
                if current and current["input_fingerprint"] == fingerprint:
                    reused += 1
                    continue
                artifact = preprocess_artifact(sample, dataset, config)
                store.publish(
                    stage="preprocess",
                    artifact=artifact,
                    sample_key=sample.key,
                    image_id=sample.image_id,
                    input_fingerprint=fingerprint,
                    input_parts=parts,
                    config_version=config.config_version,
                    expected_sample_key=sample.key,
                )
                published += 1

            # Stage 2: per-label decisions and exclusivity diagnostics.
            for sample in dataset.samples:
                stop_point("score", sample)
                pre = store.get("preprocess", sample.key)
                if not pre or pre["artifact"]["status"] == "error":
                    continue
                parts = plan_stage2_parts(sample, dataset, config, pre["artifact_digest"])
                fingerprint = fingerprint_parts(parts)
                current = store.get("score", sample.key)
                if current and current["input_fingerprint"] == fingerprint:
                    reused += 1
                    continue
                artifact = score_artifact(sample, dataset, config)
                store.publish(
                    stage="score",
                    artifact=artifact,
                    sample_key=sample.key,
                    image_id=sample.image_id,
                    input_fingerprint=fingerprint,
                    input_parts=parts,
                    config_version=config.config_version,
                    upstream_digests={"preprocess": pre["artifact_digest"]},
                    expected_sample_key=sample.key,
                )
                published += 1

            # Stage 3: one deterministic metric contribution per scoring success.
            for sample in dataset.samples:
                stop_point("metrics", sample)
                pre = store.get("preprocess", sample.key)
                score = store.get("score", sample.key)
                if not pre or pre["artifact"]["status"] == "error":
                    continue
                if not score or score["artifact"]["status"] not in {"ok", "warning"}:
                    continue
                parts = plan_stage3_parts(sample, config, score["artifact_digest"])
                fingerprint = fingerprint_parts(parts)
                current = store.get("metrics", sample.key)
                if current and current["input_fingerprint"] == fingerprint:
                    reused += 1
                    continue
                artifact = metric_contribution(score["artifact"])
                store.publish(
                    stage="metrics",
                    artifact=artifact,
                    sample_key=sample.key,
                    image_id=sample.image_id,
                    input_fingerprint=fingerprint,
                    input_parts=parts,
                    config_version=config.config_version,
                    upstream_digests={"score": score["artifact_digest"]},
                    expected_sample_key=sample.key,
                )
                published += 1

            # Stage 4: per-sample failure location and report facts.
            for sample in dataset.samples:
                stop_point("report", sample)
                pre = store.get("preprocess", sample.key)
                score = store.get("score", sample.key)
                metrics = store.get("metrics", sample.key)
                parts = plan_stage4_parts(sample, config, pre, score, metrics)
                fingerprint = fingerprint_parts(parts)
                current = store.get("report", sample.key)
                if current and current["input_fingerprint"] == fingerprint:
                    reused += 1
                    continue
                artifact = report_artifact(
                    sample,
                    dataset,
                    config,
                    pre,
                    score,
                    metrics,
                )
                upstream = {
                    stage: record["artifact_digest"]
                    for stage, record in (("preprocess", pre), ("score", score), ("metrics", metrics))
                    if record is not None
                }
                store.publish(
                    stage="report",
                    artifact=artifact,
                    sample_key=sample.key,
                    image_id=sample.image_id,
                    input_fingerprint=fingerprint,
                    input_parts=parts,
                    config_version=config.config_version,
                    upstream_digests=upstream,
                    expected_sample_key=sample.key,
                )
                published += 1

            stop_point("finalize", None)
            report, report_sha, failures, conflicts = build_and_write_report(root, config, dataset, store)
            state.update(
                {
                    "status": "completed",
                    "stage": "completed",
                    "sample_key": None,
                    "finished_at": utc_now(),
                    "published": published,
                    "reused": reused,
                    "report_path": "reports/report.json",
                    "report_sha256": report_sha,
                    "metrics": report["metrics"],
                    "failure_count": len(failures),
                    "conflict_count": len(conflicts),
                }
            )
            _write_run_state(root, state)
            return PipelineResult(
                status="completed",
                published=published,
                reused=reused,
                stop_reason=None,
                report_path="reports/report.json",
                report_sha256=report_sha,
                metrics=report["metrics"],
                failures=len(failures),
                conflicts=len(conflicts),
                invalidations=len(invalidation_entries),
            )
        except StopRequested as exc:
            state.update(
                {
                    "status": "stopped",
                    "finished_at": utc_now(),
                    "published": published,
                    "reused": reused,
                    "stop_reason": exc.reason,
                    "stage": "stopped",
                    "sample_key": None,
                }
            )
            _write_run_state(root, state)
            return PipelineResult(
                status="stopped",
                published=published,
                reused=reused,
                stop_reason=exc.reason,
                report_path=None,
                report_sha256=None,
                metrics=None,
                failures=0,
                conflicts=0,
                invalidations=len(invalidation_entries),
            )
        except Exception as exc:
            state.update(
                {
                    "status": "failed",
                    "finished_at": utc_now(),
                    "published": published,
                    "reused": reused,
                    "stage": "failed",
                    "sample_key": None,
                    "error": exc.as_dict() if isinstance(exc, WorkbenchError) else {"message": str(exc)},
                }
            )
            _write_run_state(root, state)
            raise


def plan_sample(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
    existing: dict[str, dict[str, dict[str, Any]]],
    image_sha256: str,
) -> SamplePlan:
    base_parts = {
        "preprocess": plan_stage1_parts(sample, dataset, config, image_sha256),
        "score": plan_stage2_base_parts(sample, dataset, config),
        "metrics": plan_stage3_base_parts(sample, config),
        "report": plan_stage4_base_parts(sample, config),
    }
    plan = SamplePlan(sample=sample, base_parts=base_parts)
    for stage in STAGE_NAMES:
        plan.existing[stage] = existing.get(stage, {}).get(sample.key)

    rec1 = plan.existing["preprocess"]
    parts1 = base_parts["preprocess"]
    fp1 = fingerprint_parts(parts1)
    plan.current_parts["preprocess"] = parts1
    plan.current_fingerprints["preprocess"] = fp1
    if rec1 is None or rec1["input_fingerprint"] != fp1:
        plan.earliest_stage = "preprocess"
        return plan

    if rec1["artifact"].get("status") == "error":
        if plan.existing["score"] is not None:
            plan.earliest_stage = "score"
            return plan
        if plan.existing["metrics"] is not None:
            plan.earliest_stage = "metrics"
            return plan
        parts4 = {
            **base_parts["report"],
            "preprocess_artifact": rec1["artifact_digest"],
            "score_artifact": None,
            "metrics_artifact": None,
        }
        fp4 = fingerprint_parts(parts4)
        plan.current_parts["report"] = parts4
        plan.current_fingerprints["report"] = fp4
        rec4 = plan.existing["report"]
        if rec4 is None or rec4["input_fingerprint"] != fp4:
            plan.earliest_stage = "report"
        return plan

    parts2 = {
        **base_parts["score"],
        "preprocess_artifact": rec1["artifact_digest"],
    }
    fp2 = fingerprint_parts(parts2)
    plan.current_parts["score"] = parts2
    plan.current_fingerprints["score"] = fp2
    rec2 = plan.existing["score"]
    if rec2 is None or rec2["input_fingerprint"] != fp2:
        plan.earliest_stage = "score"
        return plan

    if rec2["artifact"].get("status") == "error":
        if plan.existing["metrics"] is not None:
            plan.earliest_stage = "metrics"
            return plan
        parts4 = {
            **base_parts["report"],
            "preprocess_artifact": rec1["artifact_digest"],
            "score_artifact": rec2["artifact_digest"],
            "metrics_artifact": None,
        }
        fp4 = fingerprint_parts(parts4)
        plan.current_parts["report"] = parts4
        plan.current_fingerprints["report"] = fp4
        rec4 = plan.existing["report"]
        if rec4 is None or rec4["input_fingerprint"] != fp4:
            plan.earliest_stage = "report"
        return plan

    parts3 = {
        **base_parts["metrics"],
        "score_artifact": rec2["artifact_digest"],
    }
    fp3 = fingerprint_parts(parts3)
    plan.current_parts["metrics"] = parts3
    plan.current_fingerprints["metrics"] = fp3
    rec3 = plan.existing["metrics"]
    if rec3 is None or rec3["input_fingerprint"] != fp3:
        plan.earliest_stage = "metrics"
        return plan

    parts4 = {
        **base_parts["report"],
        "preprocess_artifact": rec1["artifact_digest"],
        "score_artifact": rec2["artifact_digest"],
        "metrics_artifact": rec3["artifact_digest"],
    }
    fp4 = fingerprint_parts(parts4)
    plan.current_parts["report"] = parts4
    plan.current_fingerprints["report"] = fp4
    rec4 = plan.existing["report"]
    if rec4 is None or rec4["input_fingerprint"] != fp4:
        plan.earliest_stage = "report"
    return plan


def plan_stage1_parts(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
    image_sha256: str,
) -> dict[str, Any]:
    return {
        "identity": digest({"sample_key": sample.key, "image_id": sample.image_id}),
        "image_sha256": image_sha256,
        "declared_geometry": digest(
            {"width": sample.raw.get("width"), "height": sample.raw.get("height")}
        ),
        "preprocess_config": config.stage_digest("preprocess"),
        "manifest_source": digest(
            {
                "manifest_path": relpath(dataset.root, sample.manifest_path),
                "image_path": sample.relative_image_path,
            }
        ),
    }


def plan_stage2_base_parts(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
) -> dict[str, Any]:
    return {
        "identity": digest({"sample_key": sample.key, "image_id": sample.image_id}),
        "truth": truth_digest(sample),
        "scores": scores_digest(sample),
        "classes": digest(dataset.classes),
        "scoring_config": config.stage_digest("score"),
    }


def plan_stage2_parts(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
    preprocess_artifact_digest: str,
) -> dict[str, Any]:
    return {
        **plan_stage2_base_parts(sample, dataset, config),
        "preprocess_artifact": preprocess_artifact_digest,
    }


def plan_stage3_base_parts(sample: ManifestSample, config: WorkbenchConfig) -> dict[str, Any]:
    return {
        "identity": digest({"sample_key": sample.key, "image_id": sample.image_id}),
        "metrics_config": config.stage_digest("metrics"),
    }


def plan_stage3_parts(
    sample: ManifestSample,
    config: WorkbenchConfig,
    score_artifact_digest: str,
) -> dict[str, Any]:
    return {
        **plan_stage3_base_parts(sample, config),
        "score_artifact": score_artifact_digest,
    }


def plan_stage4_base_parts(sample: ManifestSample, config: WorkbenchConfig) -> dict[str, Any]:
    return {
        "identity": digest({"sample_key": sample.key, "image_id": sample.image_id}),
        "report_config": config.stage_digest("report"),
        "manifest_source": digest(
            {
                "manifest_path": sample.manifest_path.as_posix(),
                "record_index": sample.source_index,
                "image_path": sample.relative_image_path,
            }
        ),
    }


def plan_stage4_parts(
    sample: ManifestSample,
    config: WorkbenchConfig,
    preprocess_record: dict[str, Any] | None,
    score_record: dict[str, Any] | None,
    metrics_record: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        **plan_stage4_base_parts(sample, config),
        "preprocess_artifact": preprocess_record["artifact_digest"] if preprocess_record else None,
        "score_artifact": score_record["artifact_digest"] if score_record else None,
        "metrics_artifact": metrics_record["artifact_digest"] if metrics_record else None,
    }


def fingerprint_parts(parts: dict[str, Any]) -> str:
    return digest(parts)


def build_and_write_report(
    root: Path,
    config: WorkbenchConfig,
    dataset: Dataset,
    store: CheckpointStore,
) -> tuple[dict[str, Any], str, list[dict[str, Any]], list[dict[str, Any]]]:
    stage_records = {stage: store.all(stage) for stage in STAGE_NAMES}
    metrics = aggregate_metrics(stage_records["metrics"], dataset.classes)
    sample_artifacts = [
        stage_records["report"][sample.key]["artifact"]
        for sample in dataset.samples
        if sample.key in stage_records["report"]
    ]
    failures: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for artifact in sample_artifacts:
        failures.extend(artifact.get("failures", []))
        conflicts.extend(artifact.get("conflicts", []))
    failures.sort(key=lambda item: (item.get("sample_key", ""), item.get("stage_index", 0), item.get("code", "")))
    conflicts.sort(
        key=lambda item: (
            item.get("sample_key", ""),
            item.get("label_pair", ["", ""])[0],
            item.get("label_pair", ["", ""])[1],
        )
    )
    stage_summary: dict[str, Any] = {}
    for stage in STAGE_NAMES:
        artifacts = [record["artifact"] for record in stage_records[stage].values()]
        counts: dict[str, int] = {}
        for artifact in artifacts:
            status = artifact.get("status", "unknown")
            counts[status] = counts.get(status, 0) + 1
        stage_summary[stage] = {
            "stage_index": STAGE_INDEX[stage],
            "record_count": len(artifacts),
            "status_counts": dict(sorted(counts.items())),
            "missing_samples": sorted(
                sample.key for sample in dataset.samples if sample.key not in stage_records[stage]
            ),
        }
    artifact_digests = {
        stage: {
            key: record["artifact_digest"] for key, record in sorted(stage_records[stage].items())
        }
        for stage in STAGE_NAMES
    }
    report = {
        "schema_version": 1,
        "report_type": "deterministic-multilabel-validation",
        "dataset": dataset.source_summary(),
        "config": {
            "path": relpath(root, config.path),
            "version": config.config_version,
            "stage_versions": {
                stage: f"sha256:{config.stage_digest(stage)}" for stage in STAGE_NAMES
            },
        },
        "pipeline": {
            "stages": list(STAGE_NAMES),
            "stage_summary": stage_summary,
            "artifact_digests": artifact_digests,
        },
        "metrics": metrics,
        "failures": failures,
        "conflicts": conflicts,
        "samples": sample_artifacts if config.data["report"]["include_sample_records"] else None,
        "definitions": {
            "truth_source": "samples/manifest.json truth arrays after optional declarative injection",
            "score_source": "samples/manifest.json score tables after optional declarative injection",
            "hit": "truth label score >= its declared lower_bound",
            "miss": "truth label score < its declared lower_bound",
            "false_positive": "non-truth label score >= scoring.default_threshold",
            "conflict": "configured exclusive label pair simultaneously present in truth or predicted sets",
        },
    }
    report_bytes = pretty_json(report).encode("utf-8")
    report_sha = hashlib.sha256(report_bytes).hexdigest()
    reports_dir = ensure_dir(root / "reports")
    atomic_write_bytes(reports_dir / "report.json", report_bytes)
    atomic_write_bytes(reports_dir / "report.sha256", f"{report_sha}\n".encode("ascii"))
    atomic_write_bytes(reports_dir / "failures.json", pretty_json(failures).encode("utf-8"))
    atomic_write_bytes(reports_dir / "conflicts.json", pretty_json(conflicts).encode("utf-8"))
    return report, report_sha, failures, conflicts


def workspace_status(root: Path, *, config_path: str | Path = "configs/default.json") -> dict[str, Any]:
    root = root.resolve()
    store = CheckpointStore(root)
    result: dict[str, Any] = {
        "workspace": str(root),
        "run_state": read_run_state(root),
        "stop_requested": stop_requested(root),
        "stages": store.verify_all(),
        "invalidations": store.invalidations(20),
    }
    try:
        config = load_config(root, config_path)
        dataset = load_dataset(root, config)
        existing = {stage: store.all(stage) for stage in STAGE_NAMES}
        image_hashes = {sample.key: _image_hash(dataset, sample) for sample in dataset.samples}
        samples: list[dict[str, Any]] = []
        for sample in dataset.samples:
            plan = plan_sample(sample, dataset, config, existing, image_hashes[sample.key])
            sample_status: dict[str, Any] = {
                "sample_key": sample.key,
                "image_id": sample.image_id,
                "manifest_record_index": sample.source_index,
                "image_path": sample.relative_image_path,
                "earliest_stale_stage": plan.earliest_stage,
                "stages": {},
            }
            for stage in STAGE_NAMES:
                record = existing[stage].get(sample.key)
                current_fp = plan.current_fingerprints.get(stage)
                if record is None:
                    state = "missing"
                elif current_fp is None:
                    state = "blocked_by_upstream"
                elif record["input_fingerprint"] == current_fp:
                    state = "fresh"
                else:
                    state = "stale"
                sample_status["stages"][stage] = {
                    "state": state,
                    "status": record["artifact"].get("status") if record else None,
                    "sequence": record["sequence"] if record else None,
                    "input_fingerprint": record["input_fingerprint"] if record else None,
                    "current_fingerprint": current_fp,
                    "artifact_digest": record["artifact_digest"] if record else None,
                    "config_version": record["config_version"] if record else None,
                }
            samples.append(sample_status)
        result["dataset"] = dataset.source_summary()
        result["config"] = {
            "path": relpath(root, config.path),
            "version": config.config_version,
            "stage_versions": {
                stage: f"sha256:{config.stage_digest(stage)}" for stage in STAGE_NAMES
            },
        }
        result["samples"] = samples
        result["integrity"] = "ok"
    except WorkbenchError as exc:
        result["integrity"] = "error"
        result["error"] = exc.as_dict()
    return result


def read_failures(root: Path) -> list[dict[str, Any]]:
    path = root / "reports" / "failures.json"
    if not path.exists():
        return []
    return read_json(path)


def read_conflicts(root: Path) -> list[dict[str, Any]]:
    path = root / "reports" / "conflicts.json"
    if not path.exists():
        return []
    return read_json(path)


def read_report(root: Path) -> dict[str, Any] | None:
    path = root / "reports" / "report.json"
    return read_json(path) if path.exists() else None


def sample_detail(
    root: Path,
    sample_key: str,
    *,
    config_path: str | Path = "configs/default.json",
) -> dict[str, Any]:
    config = load_config(root, config_path)
    dataset = load_dataset(root, config)
    matched = next((sample for sample in dataset.samples if sample.key == sample_key), None)
    if matched is None:
        raise WorkbenchError("E_NOT_FOUND", "sample key was not found", {"sample_key": sample_key})
    store = CheckpointStore(root)
    records = {stage: store.get(stage, sample_key) for stage in STAGE_NAMES}
    return {
        "source": matched.source_summary(root.resolve()),
        "raw_manifest_record": matched.raw,
        "checkpoints": records,
    }


def request_stop(root: Path, reason: str = "http_stop_request") -> dict[str, Any]:
    payload = {"requested": True, "reason": reason, "requested_at": utc_now()}
    atomic_write_json(root.resolve() / STOP_PATH, payload, pretty=True)
    return payload


def clear_stop(root: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        (root.resolve() / STOP_PATH).unlink()


def stop_requested(root: Path) -> bool:
    return (root.resolve() / STOP_PATH).exists()


def read_run_state(root: Path) -> dict[str, Any] | None:
    path = root.resolve() / RUN_STATE_PATH
    return read_json(path) if path.exists() else None


def prepare_directories(root: Path) -> None:
    for directory in ("samples", "configs", "checkpoints", "reports", "checkpoints/control"):
        ensure_dir(root.resolve() / directory)


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _write_run_state(root: Path, state: dict[str, Any]) -> None:
    atomic_write_json(root.resolve() / RUN_STATE_PATH, state, pretty=True)


def _stop_reason(root: Path, published: int, stop_after: int | None) -> str | None:
    if stop_requested(root):
        return "stop_request"
    if stop_after is not None and published >= stop_after:
        return "stop_after_checkpoints"
    return None


def _image_hash(dataset: Dataset, sample: ManifestSample) -> str:
    if sample.image_path is not None and sample.image_path.exists():
        return file_sha256(sample.image_path)
    return digest(
        {
            "missing_image": True,
            "path": sample.relative_image_path,
            "manifest": relpath(dataset.root, sample.manifest_path),
            "record_index": sample.source_index,
        }
    )


def _invalidate_plans(
    store: CheckpointStore,
    plans: list[SamplePlan],
    existing: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    keys_by_stage: dict[str, list[str]] = {stage: [] for stage in STAGE_NAMES}
    entries: list[dict[str, Any]] = []
    for plan in plans:
        if plan.earliest_stage is None:
            continue
        start = plan.earliest_index or 1
        invalidated_stages = list(STAGE_NAMES[start - 1 :])
        for stage in invalidated_stages:
            keys_by_stage[stage].append(plan.sample.key)
        old_records = {
            stage: plan.existing[stage]
            for stage in invalidated_stages
            if plan.existing[stage] is not None
        }
        if not old_records:
            continue
        changed: list[dict[str, Any]] = []
        for stage in invalidated_stages:
            old = plan.existing[stage]
            if old is None:
                changed.append({"stage": stage, "reason": "checkpoint_missing"})
                continue
            available = plan.current_parts.get(stage, plan.base_parts[stage])
            old_parts = old.get("input_parts", {})
            changed_parts = sorted(
                key
                for key in set(old_parts) | set(available)
                if old_parts.get(key) != available.get(key)
            )
            changed.append(
                {
                    "stage": stage,
                    "changed_parts": changed_parts,
                    "old_fingerprint": old.get("input_fingerprint"),
                    "new_fingerprint": plan.current_fingerprints.get(stage),
                    "reason": "input_changed"
                    if changed_parts
                    else "upstream_invalidated_or_checkpoint_missing",
                }
            )
        entries.append(
            {
                "sample_key": plan.sample.key,
                "image_id": plan.sample.image_id,
                "earliest_stage": plan.earliest_stage,
                "earliest_stage_index": plan.earliest_index,
                "invalidated_stages": invalidated_stages,
                "changes": changed,
            }
        )
    store.invalidate(keys_by_stage)
    return store.append_invalidations(entries)


__all__ = [
    "PipelineResult",
    "build_and_write_report",
    "clear_stop",
    "fingerprint_parts",
    "prepare_directories",
    "read_conflicts",
    "read_failures",
    "read_report",
    "read_run_state",
    "request_stop",
    "run_pipeline",
    "sample_detail",
    "stop_requested",
    "workspace_status",
]
