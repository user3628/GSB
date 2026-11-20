from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .budget import decide_recall
from .checkpoint import CheckpointState, CheckpointStore, STAGES, STAGE_INDEX
from .config import LabConfig, load_config
from .data import InputBundle, Manifest, load_input_bundle
from .errors import BusinessError, ErrorCode
from .fusion import fuse_recall
from .recall import RecallEngine
from .summary import build_summary
from .util import atomic_write_bytes, canonical_json_bytes, digest_bytes, digest_file, file_lock, quantize
from .workspace import init_workspace, load_workspace_manifest, read_meta


RECALL_ALGORITHM_VERSION = "recall-routes-v1"
FUSION_ALGORITHM_VERSION = "fusion-v1"
SUMMARY_ALGORITHM_VERSION = "summary-v1"


@dataclass(frozen=True)
class StageView:
    stage: str
    status: str
    expected_input_fingerprint: str | None
    record: dict[str, Any] | None
    reason: str
    reason_text: str

    @property
    def valid(self) -> bool:
        return self.status == "valid"

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "status": self.status,
            "valid": self.valid,
            "expected_input_fingerprint": self.expected_input_fingerprint,
            "checkpoint": self.record,
            "reason": self.reason,
            "reason_text": self.reason_text,
        }


def _fingerprint(payload: dict[str, Any]) -> str:
    return digest_bytes(canonical_json_bytes(payload))


def recall_input_fingerprint(bundle: InputBundle, config: LabConfig) -> str:
    return _fingerprint(
        {
            "format": "recall-input-v1",
            "algorithm_version": RECALL_ALGORITHM_VERSION,
            "scale": bundle.entry.scale,
            "items_digest": bundle.items_digest,
            "queries_digest": bundle.queries_digest,
            "item_count": len(bundle.items),
            "query_count": len(bundle.queries),
            "recall_config": config.recall_payload,
            "budget_config": config.budget_payload,
        }
    )


def fusion_input_fingerprint(recall_fp: str, recall_artifact_digest: str, config: LabConfig) -> str:
    return _fingerprint(
        {
            "format": "fusion-input-v1",
            "algorithm_version": FUSION_ALGORITHM_VERSION,
            "recall_input_fingerprint": recall_fp,
            "recall_artifact_digest": recall_artifact_digest,
            "fusion_config": config.fusion_payload,
        }
    )


def summary_input_fingerprint(fusion_fp: str, fusion_artifact_digest: str, seed: int) -> str:
    return _fingerprint(
        {
            "format": "summary-input-v1",
            "algorithm_version": SUMMARY_ALGORITHM_VERSION,
            "fusion_input_fingerprint": fusion_fp,
            "fusion_artifact_digest": fusion_artifact_digest,
            "seed": seed,
        }
    )


class Lab:
    def __init__(self, root: str | Path, *, config_path: str | Path | None = None):
        self.root = Path(root).resolve()
        self.meta = read_meta(self.root)
        self.workspace_id = self.meta["workspace_id"]
        self.manifest: Manifest = load_workspace_manifest(self.root)
        self.config_path = Path(config_path).resolve() if config_path else (self.root / "config" / "fusion.yaml")
        self.config = load_config(self.config_path)
        self.store = CheckpointStore(self.root, self.workspace_id)
        self._bundle_cache: dict[str, InputBundle] = {}

    @classmethod
    def initialize(
        cls,
        root: str | Path,
        manifest_path: str | Path,
        config_path: str | Path,
        *,
        force: bool = False,
    ) -> "Lab":
        init_workspace(root, manifest_path, config_path, force=force)
        return cls(root)

    def bundle(self, scale: str) -> InputBundle:
        if scale not in self._bundle_cache:
            entry = self.manifest.by_scale().get(scale)
            if entry is None:
                raise BusinessError(
                    ErrorCode.NOT_FOUND,
                    "scale is not present in manifest",
                    {"scale": scale, "available": [entry.scale for entry in self.manifest.entries]},
                )
            self._bundle_cache[scale] = load_input_bundle(self.manifest, entry)
        return self._bundle_cache[scale]

    def _entry(self, scale: str):
        entry = self.manifest.by_scale().get(scale)
        if entry is None:
            raise BusinessError(
                ErrorCode.NOT_FOUND,
                "scale is not present in manifest",
                {"scale": scale, "available": [item.scale for item in self.manifest.entries]},
            )
        return entry

    def config_fingerprint(self) -> str:
        return self.config.fingerprint

    def _verify_record_artifact(self, scale: str, stage: str, record: dict[str, Any]) -> Path:
        path = self.root / record["artifact_path"]
        if not path.exists():
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "published stage artifact is missing",
                {"scale": scale, "stage": stage, "path": record["artifact_path"], "reason": "MISSING_ARTIFACT"},
            )
        actual = digest_file(path)
        if actual != record["artifact_digest"]:
            raise BusinessError(
                ErrorCode.STALE_CHECKPOINT,
                "published stage artifact digest mismatch",
                {
                    "scale": scale,
                    "stage": stage,
                    "expected": record["artifact_digest"],
                    "actual": actual,
                    "reason": "ARTIFACT_DIGEST_MISMATCH",
                },
            )
        return path

    def _artifact_exists_with_digest(self, record: dict[str, Any]) -> tuple[bool, bool]:
        path = self.root / record["artifact_path"]
        if not path.exists():
            return False, False
        return True, digest_file(path) == record["artifact_digest"]

    def _build_stage_views(self, scale: str, state: CheckpointState) -> dict[str, StageView]:
        bundle = self.bundle(scale)
        recall_fp = recall_input_fingerprint(bundle, self.config)
        views: dict[str, StageView] = {}

        recall_record = state.records.get("recall")
        if recall_record is None:
            views["recall"] = StageView(
                "recall", "missing", recall_fp, None, "MISSING_CHECKPOINT", "尚无召回检查点"
            )
        elif recall_record["input_fingerprint"] != recall_fp:
            views["recall"] = StageView(
                "recall",
                "stale_input",
                recall_fp,
                recall_record,
                "INPUT_MISMATCH",
                "物品、查询、召回参数或预算已变化，召回阶段需重建",
            )
        else:
            exists, digest_ok = self._artifact_exists_with_digest(recall_record)
            if not digest_ok:
                # A mismatching digest is tampering/staleness, but a missing artifact
                # is a missing product and is recovered only by an explicit rebuild.
                if exists:
                    self._verify_record_artifact(scale, "recall", recall_record)
                views["recall"] = StageView(
                    "recall",
                    "artifact_missing",
                    recall_fp,
                    recall_record,
                    "MISSING_ARTIFACT",
                    "召回检查点存在但产物文件缺失",
                )
            else:
                views["recall"] = StageView(
                    "recall", "valid", recall_fp, recall_record, "VALID", "召回检查点与当前输入一致"
                )

        if not views["recall"].valid:
            views["fusion"] = StageView(
                "fusion",
                "invalid_prerequisite",
                None,
                state.records.get("fusion"),
                "INVALID_PREREQUISITE",
                "召回阶段无效，融合阶段不可用",
            )
            views["summary"] = StageView(
                "summary",
                "invalid_prerequisite",
                None,
                state.records.get("summary"),
                "INVALID_PREREQUISITE",
                "融合阶段无效，汇总阶段不可用",
            )
            return views

        fusion_fp = fusion_input_fingerprint(recall_fp, recall_record["artifact_digest"], self.config)
        fusion_record = state.records.get("fusion")
        if fusion_record is None:
            views["fusion"] = StageView(
                "fusion", "missing", fusion_fp, None, "MISSING_CHECKPOINT", "尚无融合检查点"
            )
        elif fusion_record["input_fingerprint"] != fusion_fp:
            views["fusion"] = StageView(
                "fusion",
                "stale_input",
                fusion_fp,
                fusion_record,
                "INPUT_MISMATCH",
                "融合权重、策略、截断位或上游产物已变化，融合阶段需重建",
            )
        else:
            exists, digest_ok = self._artifact_exists_with_digest(fusion_record)
            if not digest_ok:
                if exists:
                    self._verify_record_artifact(scale, "fusion", fusion_record)
                views["fusion"] = StageView(
                    "fusion",
                    "artifact_missing",
                    fusion_fp,
                    fusion_record,
                    "MISSING_ARTIFACT",
                    "融合检查点存在但产物文件缺失",
                )
            else:
                views["fusion"] = StageView(
                    "fusion", "valid", fusion_fp, fusion_record, "VALID", "融合检查点与当前输入一致"
                )

        if not views["fusion"].valid:
            views["summary"] = StageView(
                "summary",
                "invalid_prerequisite",
                None,
                state.records.get("summary"),
                "INVALID_PREREQUISITE",
                "融合阶段无效，汇总阶段不可用",
            )
            return views

        entry = self._entry(scale)
        summary_fp = summary_input_fingerprint(fusion_fp, fusion_record["artifact_digest"], entry.seed)
        summary_record = state.records.get("summary")
        if summary_record is None:
            views["summary"] = StageView(
                "summary", "missing", summary_fp, None, "MISSING_CHECKPOINT", "尚无汇总检查点"
            )
        elif summary_record["input_fingerprint"] != summary_fp:
            views["summary"] = StageView(
                "summary",
                "stale_input",
                summary_fp,
                summary_record,
                "INPUT_MISMATCH",
                "随机种子或上游产物已变化，汇总阶段需重建",
            )
        else:
            exists, digest_ok = self._artifact_exists_with_digest(summary_record)
            if not digest_ok:
                if exists:
                    self._verify_record_artifact(scale, "summary", summary_record)
                views["summary"] = StageView(
                    "summary",
                    "artifact_missing",
                    summary_fp,
                    summary_record,
                    "MISSING_ARTIFACT",
                    "汇总检查点存在但产物文件缺失",
                )
            else:
                views["summary"] = StageView(
                    "summary", "valid", summary_fp, summary_record, "VALID", "汇总检查点与当前输入一致"
                )
        return views

    def _stage_views(self, scale: str) -> tuple[CheckpointState, dict[str, StageView]]:
        state = self.store.load_state(scale)
        return state, self._build_stage_views(scale, state)

    def _read_published_artifact(
        self,
        scale: str,
        stage: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        path = self._verify_record_artifact(scale, stage, record)
        try:
            raw = path.read_bytes()
            artifact = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError(
                ErrorCode.STALE_CHECKPOINT,
                "stage artifact cannot be decoded",
                {"scale": scale, "stage": stage, "reason": "ARTIFACT_DECODE_ERROR", "detail": str(exc)},
            ) from exc
        if not isinstance(artifact, dict):
            raise BusinessError(
                ErrorCode.STALE_CHECKPOINT,
                "stage artifact is not an object",
                {"scale": scale, "stage": stage, "reason": "ARTIFACT_SHAPE_ERROR"},
            )
        if artifact.get("scale") != scale or artifact.get("input_fingerprint") != record["input_fingerprint"]:
            raise BusinessError(
                ErrorCode.STALE_CHECKPOINT,
                "stage artifact metadata does not match its checkpoint",
                {"scale": scale, "stage": stage, "reason": "ARTIFACT_METADATA_MISMATCH"},
            )
        return artifact

    def _write_artifact(self, scale: str, stage: str, artifact: dict[str, Any]) -> tuple[str, str]:
        data = canonical_json_bytes(artifact)
        digest = digest_bytes(data)
        path = self.store.artifact_path(scale, stage, digest)
        atomic_write_bytes(path, data)
        return digest, artifact["input_fingerprint"]

    def _compute_recall(self, scale: str, input_fp: str) -> dict[str, Any]:
        bundle = self.bundle(scale)
        decision = decide_recall(
            self.config,
            scale=scale,
            item_count=len(bundle.items),
            query_count=len(bundle.queries),
        )
        engine = RecallEngine(bundle)
        artifact = engine.recall_all(decision)
        artifact["config_version"] = self.config.fingerprint
        artifact["input_fingerprint"] = input_fp
        artifact["algorithm_version"] = RECALL_ALGORITHM_VERSION
        return artifact

    def _compute_fusion(self, scale: str, input_fp: str, recall_record: dict[str, Any]) -> dict[str, Any]:
        recall_artifact = self._read_published_artifact(scale, "recall", recall_record)
        return fuse_recall(
            recall_artifact,
            config=self.config,
            recall_digest=recall_record["artifact_digest"],
            input_fingerprint=input_fp,
        )

    def _compute_summary(
        self,
        scale: str,
        input_fp: str,
        recall_record: dict[str, Any],
        fusion_record: dict[str, Any],
    ) -> dict[str, Any]:
        recall_artifact = self._read_published_artifact(scale, "recall", recall_record)
        fusion_artifact = self._read_published_artifact(scale, "fusion", fusion_record)
        bundle = self.bundle(scale)
        return build_summary(
            recall_artifact,
            fusion_artifact,
            queries=bundle.queries,
            config=self.config,
            input_fingerprint=input_fp,
            fusion_digest=fusion_record["artifact_digest"],
        )

    def run_stage(self, scale: str, stage: str) -> dict[str, Any]:
        if stage not in STAGES:
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "unknown stage",
                {"stage": stage, "allowed": list(STAGES)},
            )
        state, views = self._stage_views(scale)
        view = views[stage]
        if view.valid:
            return {
                "scale": scale,
                "stage": stage,
                "status": "reused",
                "sequence": view.record["sequence"] if view.record else None,
                "input_fingerprint": view.expected_input_fingerprint,
            }
        if view.reason == "INVALID_PREREQUISITE":
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "stage prerequisite is not valid",
                {"scale": scale, "stage": stage, "reason": "INVALID_PREREQUISITE"},
            )
        if view.reason == "MISSING_ARTIFACT":
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "published artifact is missing; use a controlled rebuild workflow",
                {"scale": scale, "stage": stage, "reason": "MISSING_ARTIFACT"},
            )

        input_fp = view.expected_input_fingerprint
        assert input_fp is not None
        if stage == "recall":
            artifact = self._compute_recall(scale, input_fp)
            upstream: dict[str, str] = {}
        elif stage == "fusion":
            recall_record = state.records["recall"]
            artifact = self._compute_fusion(scale, input_fp, recall_record)
            recall_fp = views["recall"].expected_input_fingerprint
            assert recall_fp is not None
            upstream = {"recall": recall_fp}
        else:
            recall_record = state.records["recall"]
            fusion_record = state.records["fusion"]
            artifact = self._compute_summary(scale, input_fp, recall_record, fusion_record)
            recall_fp = views["recall"].expected_input_fingerprint
            fusion_fp = views["fusion"].expected_input_fingerprint
            assert recall_fp is not None and fusion_fp is not None
            upstream = {
                "recall": recall_fp,
                "fusion": fusion_fp,
            }
        artifact_digest, _ = self._write_artifact(scale, stage, artifact)
        record = self.store.publish(
            scale,
            stage,
            input_fingerprint=input_fp,
            config_version=self.config.fingerprint,
            artifact_digest=artifact_digest,
            upstream_fingerprints=upstream,
            basis_sequence=state.sequence,
        )
        return {
            "scale": scale,
            "stage": stage,
            "status": "published",
            "sequence": record["sequence"],
            "input_fingerprint": input_fp,
            "artifact_digest": artifact_digest,
            "artifact_path": record["artifact_path"],
        }

    def _run_lock(self):
        return file_lock(self.root / "run.lock", exclusive=True, blocking=False)

    def _stop_path(self) -> Path:
        return self.root / "control" / "stop.request"

    def request_stop(self, *, reason: str = "explicit_request") -> dict[str, Any]:
        payload = {"format_version": 1, "reason": reason}
        atomic_write_bytes(self._stop_path(), canonical_json_bytes(payload))
        return payload

    def stop_requested(self) -> bool:
        return self._stop_path().exists()

    def clear_stop(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            self._stop_path().unlink()

    def _write_stop_report(self, *, last: dict[str, Any] | None, completed: list[dict[str, Any]]) -> None:
        payload = {
            "format": "stop-report-v1",
            "status": "stopped_gracefully",
            "completed": completed,
            "last": last,
            "checkpoints": {
                entry.scale: self.store.load_state(entry.scale).sequence for entry in self.manifest.entries
            },
        }
        atomic_write_bytes(self.root / "control" / "stop-report.json", canonical_json_bytes(payload))

    def run(
        self,
        *,
        scales: Iterable[str] | None = None,
        until: str = "summary",
        stop_after: str | None = None,
        export: bool = True,
    ) -> dict[str, Any]:
        if until not in STAGES:
            raise BusinessError(ErrorCode.INVALID_STAGE, "unknown target stage", {"stage": until})
        target_index = STAGE_INDEX[until]
        scale_names = list(scales) if scales is not None else [entry.scale for entry in self.manifest.entries]
        if not scale_names:
            raise BusinessError(ErrorCode.INVALID_INPUT, "at least one scale is required")
        for scale in scale_names:
            self._entry(scale)
        if stop_after is not None:
            parts = stop_after.split(":", 1)
            if len(parts) != 2 or parts[0] not in scale_names or parts[1] not in STAGES:
                raise BusinessError(
                    ErrorCode.INVALID_INPUT,
                    "stop_after must be <scale>:<stage>",
                    {"value": stop_after},
                )

        completed: list[dict[str, Any]] = []
        last: dict[str, Any] | None = None
        with self._run_lock():
            for scale in scale_names:
                for stage in STAGES[: target_index + 1]:
                    result = self.run_stage(scale, stage)
                    last = result
                    completed.append(result)
                    if stop_after == f"{scale}:{stage}" or self.stop_requested():
                        self._write_stop_report(last=last, completed=completed)
                        return {
                            "status": "stopped",
                            "reason": "stop_requested_after_indivisible_stage",
                            "last": last,
                            "completed": completed,
                        }
        export_result = None
        if export and target_index == STAGE_INDEX["summary"]:
            export_result = self.export_summary(scales=scale_names)
        return {
            "status": "completed",
            "completed": completed,
            "export": export_result,
        }

    def resume(
        self,
        *,
        scales: Iterable[str] | None = None,
        until: str = "summary",
        export: bool = True,
    ) -> dict[str, Any]:
        self.clear_stop()
        return self.run(scales=scales, until=until, export=export)

    def status(self, *, scales: Iterable[str] | None = None) -> dict[str, Any]:
        scale_names = list(scales) if scales is not None else [entry.scale for entry in self.manifest.entries]
        scale_statuses: list[dict[str, Any]] = []
        for scale in scale_names:
            entry = self._entry(scale)
            bundle = self.bundle(scale)
            state, views = self._stage_views(scale)
            decision = decide_recall(
                self.config,
                scale=scale,
                item_count=len(bundle.items),
                query_count=len(bundle.queries),
            )
            invalid = [stage for stage in STAGES if not views[stage].valid]
            scale_statuses.append(
                {
                    "scale": scale,
                    "item_count": len(bundle.items),
                    "query_count": len(bundle.queries),
                    "manifest_expected_candidate_depth": entry.expected_candidate_depth,
                    "seed": entry.seed,
                    "sequence": state.sequence,
                    "earliest_invalid_stage": invalid[0] if invalid else None,
                    "all_stages_valid": not invalid,
                    "decision": decision.as_dict(),
                    "stages": {stage: views[stage].as_dict() for stage in STAGES},
                    "version": {
                        "content_digest": bundle.content_digest,
                        "items_digest": bundle.items_digest,
                        "queries_digest": bundle.queries_digest,
                        "config_version": self.config.fingerprint,
                        "recall_config_version": _fingerprint(self.config.recall_payload),
                        "budget_config_version": _fingerprint(self.config.budget_payload),
                        "fusion_config_version": _fingerprint(self.config.fusion_payload),
                        "expected_input_fingerprints": {
                            stage: views[stage].expected_input_fingerprint for stage in STAGES
                        },
                    },
                }
            )
        return {
            "workspace_id": self.workspace_id,
            "workspace_root": str(self.root),
            "config_path": str(self.config_path),
            "config_version": self.config.fingerprint,
            "stop_requested": self.stop_requested(),
            "scales": scale_statuses,
        }

    def version_info(self, scale: str) -> dict[str, Any]:
        status = self.status(scales=[scale])
        return status["scales"][0]["version"]

    def decision(self, scale: str) -> dict[str, Any]:
        bundle = self.bundle(scale)
        return decide_recall(
            self.config,
            scale=scale,
            item_count=len(bundle.items),
            query_count=len(bundle.queries),
        ).as_dict()

    def _require_valid_stage(self, scale: str, stage: str) -> tuple[CheckpointState, dict[str, Any]]:
        state, views = self._stage_views(scale)
        view = views[stage]
        if not view.valid or view.record is None:
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "stage artifact is not available",
                {
                    "scale": scale,
                    "stage": stage,
                    "reason": view.reason,
                    "status": view.status,
                },
            )
        return state, view.record

    def artifact(self, scale: str, stage: str) -> dict[str, Any]:
        if stage not in STAGES:
            raise BusinessError(ErrorCode.INVALID_STAGE, "unknown stage", {"stage": stage})
        _, record = self._require_valid_stage(scale, stage)
        return self._read_published_artifact(scale, stage, record)

    def fusion_detail(self, scale: str, query_id: str | None = None) -> dict[str, Any]:
        artifact = self.artifact(scale, "fusion")
        if query_id is None:
            return artifact
        for query in artifact["queries"]:
            if query["query_id"] == query_id:
                return query
        raise BusinessError(
            ErrorCode.NOT_FOUND,
            "query_id is not present in fusion artifact",
            {"scale": scale, "query_id": query_id},
        )

    def export_summary(self, *, scales: Iterable[str] | None = None) -> dict[str, Any]:
        scale_names = list(scales) if scales is not None else [entry.scale for entry in self.manifest.entries]
        scale_summaries: list[dict[str, Any]] = []
        artifact_digests: dict[str, str] = {}
        total_items = 0
        total_queries = 0
        for scale in scale_names:
            _, record = self._require_valid_stage(scale, "summary")
            summary = self._read_published_artifact(scale, "summary", record)
            if summary.get("config_version") != self.config.fingerprint:
                raise BusinessError(
                    ErrorCode.INVALID_STAGE,
                    "summary was produced by a different config version",
                    {"scale": scale, "summary_config": summary.get("config_version")},
                )
            scale_summaries.append(summary)
            artifact_digests[scale] = record["artifact_digest"]
            total_items += int(summary["item_count"])
            total_queries += int(summary["query_count"])

        precision_values = [float(item["fusion"]["precision_at_k"]) for item in scale_summaries]
        recall_values = [float(item["fusion"]["recall_at_k"]) for item in scale_summaries]
        coverage_values = [float(item["candidate_coverage"]) for item in scale_summaries]
        aggregate = {
            "scale_count": len(scale_summaries),
            "total_items": total_items,
            "total_queries": total_queries,
            "mean_scale_precision_at_k": quantize(sum(precision_values) / len(precision_values), 10)
            if precision_values
            else 0.0,
            "mean_scale_recall_at_k": quantize(sum(recall_values) / len(recall_values), 10)
            if recall_values
            else 0.0,
            "mean_scale_candidate_coverage": quantize(sum(coverage_values) / len(coverage_values), 10)
            if coverage_values
            else 0.0,
        }
        payload = {
            "format": "experiment-summary-v1",
            "config_version": self.config.fingerprint,
            "seed_by_scale": {entry.scale: entry.seed for entry in self.manifest.entries if entry.scale in scale_names},
            "artifact_digests": artifact_digests,
            "scales": scale_summaries,
            "aggregate": aggregate,
        }
        data = canonical_json_bytes(payload)
        output_path = self.root / "exports" / "experiment-summary.json"
        atomic_write_bytes(output_path, data)
        return {
            "path": str(output_path),
            "digest": digest_bytes(data),
            "bytes": len(data),
            "summary": payload,
        }

    def export_summary_bytes(self, *, scales: Iterable[str] | None = None) -> bytes:
        result = self.export_summary(scales=scales)
        return (self.root / "exports" / "experiment-summary.json").read_bytes()
