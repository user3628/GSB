from __future__ import annotations

import copy
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .clustering import (
    cluster_input_fingerprint,
    compute_clustering,
    load_cluster_artifacts,
)
from .config import (
    config_fingerprint,
    load_config,
    resolve_manifest_path,
    stage_config_fingerprint,
    validate_config,
)
from .diagnostics import (
    compute_diagnosis,
    compute_report,
    diagnose_input_fingerprint,
    load_diagnosis_artifact,
)
from .errors import WorkbenchError, incremental_mismatch, invalid_stage
from .preprocessing import (
    compute_preprocess,
    get_tier_item,
    load_manifest,
    load_preprocess_artifacts,
    preprocess_input_fingerprint,
)
from .reduction import (
    compute_reduction,
    load_reduction_artifacts,
    reduce_input_fingerprint,
)
from .samples import verify_samples
from .state import STAGE_ORDER, StateStore
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    ensure_workspace,
    read_json,
    sha256_bytes,
    sha256_file,
)


class Workbench:
    def __init__(
        self,
        root: Path,
        config: dict[str, Any] | None = None,
        *,
        state_dir: Path | None = None,
        output_dir: Path | None = None,
        manifest_path: Path | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.paths = ensure_workspace(self.root)
        self.config = config if config is not None else load_config()
        validate_config(self.config)
        self.config_sha256 = config_fingerprint(self.config)
        self.state_dir = (
            Path(state_dir).resolve() if state_dir is not None else self.paths["state"]
        )
        self.output_dir = (
            Path(output_dir).resolve()
            if output_dir is not None
            else self.paths["outputs"]
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = (
            Path(manifest_path).resolve()
            if manifest_path is not None
            else resolve_manifest_path(self.config, self.root)
        )
        self.state = StateStore(self.state_dir, root=self.root)

    def import_samples(self, source_manifest: Path | None = None) -> dict[str, Any]:
        source = (
            Path(source_manifest).resolve()
            if source_manifest is not None
            else self.manifest_path
        )
        verification = verify_samples(source)
        target_manifest = self.manifest_path
        copied: list[str] = []
        if source != target_manifest:
            source_dir = source.parent
            target_dir = target_manifest.parent
            target_dir.mkdir(parents=True, exist_ok=True)
            manifest_bytes = source.read_bytes()
            manifest = read_json(source)
            for item in manifest.get("files", []):
                source_csv = source_dir / item["file"]
                if not source_csv.exists():
                    raise invalid_stage(
                        "Imported sample file is missing", {"path": str(source_csv)}
                    )
                atomic_write_bytes(target_dir / item["file"], source_csv.read_bytes())
                copied.append(item["file"])
            atomic_write_bytes(target_manifest, manifest_bytes)
            summary_path = source_dir / "generation_summary.json"
            if summary_path.exists():
                atomic_write_bytes(
                    target_dir / "generation_summary.json", summary_path.read_bytes()
                )
        audit = {
            "schema_version": "sample-import-v1",
            "source_manifest": str(source),
            "target_manifest": str(target_manifest),
            "manifest_sha256": sha256_file(target_manifest),
            "copied_files": copied,
            "verification": verification,
        }
        atomic_write_json(self.output_dir / "last_import.json", audit, pretty=True)
        return audit

    def _stage_config_fp(self, stage: str) -> str:
        return stage_config_fingerprint(self.config, stage)

    def _record_predecessor(self, record: dict[str, Any]) -> dict[str, str]:
        return {
            "stage": record["stage"],
            "tier": record["tier"],
            "artifact_digest": record["artifact_digest"],
            "sequence": int(record["sequence"]),
        }

    def _load_existing(self, stage: str, tier: str) -> dict[str, Any] | None:
        return self.state.load_latest(stage, tier, verify_artifacts=True)

    def _publish_bundle(
        self,
        *,
        stage: str,
        tier: str,
        input_fingerprint: str,
        config_fingerprint_value: str,
        bundle: Any,
        predecessors: list[dict[str, str]],
        existing: dict[str, Any] | None,
    ) -> dict[str, Any]:
        record = self.state.publish(
            stage=stage,
            tier=tier,
            input_fingerprint=input_fingerprint,
            config_fingerprint=config_fingerprint_value,
            config_version=self.config_sha256,
            files=bundle.artifacts,
            predecessors=predecessors,
            expected_current_digest=(
                existing["artifact_digest"] if existing is not None else None
            ),
            expected_current_sequence=(
                int(existing["sequence"]) if existing is not None else None
            ),
        )
        record["artifact_paths"] = {
            name: self.state.state_dir / info["path"]
            for name, info in record["artifacts"].items()
        }
        return record

    def _run_tier(
        self,
        tier: str,
        manifest: dict[str, Any],
        actions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        manifest_item = get_tier_item(manifest, tier)
        pre_fp = preprocess_input_fingerprint(
            self.manifest_path, manifest, tier, self.config
        )
        pre_config_fp = self._stage_config_fp("preprocess")
        pre_record = self._load_existing("preprocess", tier)
        if (
            pre_record is not None
            and pre_record["input_fingerprint"] == pre_fp
            and pre_record["config_fingerprint"] == pre_config_fp
        ):
            action = "reuse"
            pre_record = pre_record
        else:
            bundle = compute_preprocess(self.manifest_path, tier, self.config)
            pre_record = self._publish_bundle(
                stage="preprocess",
                tier=tier,
                input_fingerprint=pre_fp,
                config_fingerprint_value=pre_config_fp,
                bundle=bundle,
                predecessors=[],
                existing=pre_record,
            )
            action = "recompute"
        actions.append(self._action(stage="preprocess", tier=tier, action=action, record=pre_record))
        pre_data = load_preprocess_artifacts(pre_record["artifact_paths"])

        red_fp = reduce_input_fingerprint(
            pre_record["artifact_digest"], pre_data.metadata, self.config
        )
        red_config_fp = self._stage_config_fp("reduce")
        red_record = self._load_existing("reduce", tier)
        if (
            red_record is not None
            and red_record["input_fingerprint"] == red_fp
            and red_record["config_fingerprint"] == red_config_fp
        ):
            action = "reuse"
        else:
            bundle = compute_reduction(
                pre_data, pre_record["artifact_digest"], tier, self.config
            )
            red_record = self._publish_bundle(
                stage="reduce",
                tier=tier,
                input_fingerprint=red_fp,
                config_fingerprint_value=red_config_fp,
                bundle=bundle,
                predecessors=[self._record_predecessor(pre_record)],
                existing=red_record,
            )
            action = "recompute"
        actions.append(self._action(stage="reduce", tier=tier, action=action, record=red_record))
        red_data = load_reduction_artifacts(red_record["artifact_paths"])

        clu_fp = cluster_input_fingerprint(
            red_record["artifact_digest"], red_data["metadata"], self.config
        )
        clu_config_fp = self._stage_config_fp("cluster")
        clu_record = self._load_existing("cluster", tier)
        if (
            clu_record is not None
            and clu_record["input_fingerprint"] == clu_fp
            and clu_record["config_fingerprint"] == clu_config_fp
        ):
            action = "reuse"
        else:
            bundle = compute_clustering(
                red_data["reduced"],
                red_record["artifact_digest"],
                red_data["metadata"],
                tier,
                self.config,
            )
            clu_record = self._publish_bundle(
                stage="cluster",
                tier=tier,
                input_fingerprint=clu_fp,
                config_fingerprint_value=clu_config_fp,
                bundle=bundle,
                predecessors=[self._record_predecessor(red_record)],
                existing=clu_record,
            )
            action = "recompute"
        actions.append(self._action(stage="cluster", tier=tier, action=action, record=clu_record))
        clu_data = load_cluster_artifacts(clu_record["artifact_paths"])

        diag_fp = diagnose_input_fingerprint(
            clu_record["artifact_digest"],
            clu_data["metadata"],
            red_data["metadata"],
            self.config,
        )
        diag_config_fp = self._stage_config_fp("diagnose")
        diag_record = self._load_existing("diagnose", tier)
        if (
            diag_record is not None
            and diag_record["input_fingerprint"] == diag_fp
            and diag_record["config_fingerprint"] == diag_config_fp
        ):
            action = "reuse"
        else:
            diagnosis = compute_diagnosis(
                red_data["reduced"],
                clu_data["labels"],
                clu_data["centers"],
                red_data["metadata"],
                clu_data["metadata"],
                tier,
                self.config,
                red_data["reconstruction_error"],
            )
            from .stage import ArtifactBundle

            bundle = ArtifactBundle(
                artifacts={"diagnosis.json": canonical_json_bytes(diagnosis)},
                metadata=diagnosis,
            )
            diag_record = self._publish_bundle(
                stage="diagnose",
                tier=tier,
                input_fingerprint=diag_fp,
                config_fingerprint_value=diag_config_fp,
                bundle=bundle,
                predecessors=[
                    self._record_predecessor(red_record),
                    self._record_predecessor(clu_record),
                ],
                existing=diag_record,
            )
            action = "recompute"
        actions.append(self._action(stage="diagnose", tier=tier, action=action, record=diag_record))
        diagnosis = load_diagnosis_artifact(diag_record["artifact_paths"]["diagnosis.json"])
        return {
            "tier": tier,
            "manifest_item": manifest_item,
            "preprocess": pre_record,
            "reduce": red_record,
            "cluster": clu_record,
            "diagnose": diag_record,
            "diagnosis": diagnosis,
        }

    def _action(
        self,
        *,
        stage: str,
        tier: str,
        action: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "stage": stage,
            "tier": tier,
            "action": action,
            "sequence": int(record["sequence"]),
            "input_fingerprint": record["input_fingerprint"],
            "config_fingerprint": record["config_fingerprint"],
            "artifact_digest": record["artifact_digest"],
        }

    def _report_input_fingerprint(self, diag_records: list[dict[str, Any]]) -> str:
        payload = {
            "diagnoses": sorted(
                [
                    {
                        "tier": record["tier"],
                        "artifact_digest": record["artifact_digest"],
                        "sequence": int(record["sequence"]),
                    }
                    for record in diag_records
                ],
                key=lambda item: item["tier"],
            ),
            "report_config": self._stage_config_fp("report"),
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def _run_report(
        self,
        tier_results: dict[str, dict[str, Any]],
        actions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        diag_records = [tier_results[tier]["diagnose"] for tier in self.config["tiers"]]
        report_fp = self._report_input_fingerprint(diag_records)
        report_config_fp = self._stage_config_fp("report")
        existing = self._load_existing("report", "all")
        if (
            existing is not None
            and existing["input_fingerprint"] == report_fp
            and existing["config_fingerprint"] == report_config_fp
        ):
            record = existing
            action = "reuse"
            report_bytes = record["artifact_paths"]["report.json"].read_bytes()
        else:
            diagnoses = {
                tier: tier_results[tier]["diagnosis"] for tier in self.config["tiers"]
            }
            report, report_bytes = compute_report(
                diagnoses, self.config, self.config_sha256
            )
            from .stage import ArtifactBundle

            bundle = ArtifactBundle(
                artifacts={"report.json": report_bytes}, metadata=report
            )
            record = self._publish_bundle(
                stage="report",
                tier="all",
                input_fingerprint=report_fp,
                config_fingerprint_value=report_config_fp,
                bundle=bundle,
                predecessors=[self._record_predecessor(item) for item in diag_records],
                existing=existing,
            )
            action = "recompute"
        actions.append(self._action(stage="report", tier="all", action=action, record=record))
        atomic_write_bytes(self.output_dir / "diagnosis_report.json", report_bytes)
        report = json.loads(report_bytes.decode("utf-8"))
        return {
            "record": record,
            "report": report,
            "report_bytes": report_bytes,
            "action": action,
        }

    def run(
        self,
        *,
        mode: str = "auto",
        reset: bool = False,
        verify_full: bool = False,
        stop_after_tier: str | None = None,
        after_tier_hook: Callable[[str, StateStore], None] | None = None,
    ) -> dict[str, Any]:
        if mode not in {"auto", "full"}:
            raise WorkbenchError("E_INVALID_CONFIG", "mode must be auto or full")
        validate_config(self.config)
        self.import_samples(self.manifest_path)
        if reset or mode == "full":
            self.state.reset()
            for stale in (
                self.output_dir / "diagnosis_report.json",
                self.output_dir / "incremental_verification.json",
            ):
                try:
                    stale.unlink()
                except FileNotFoundError:
                    pass
        self.state.save_active_config(self.config)
        self.state.clear_stop()
        self.state.reconcile()
        if stop_after_tier is not None and stop_after_tier not in self.config["tiers"]:
            raise WorkbenchError(
                "E_INVALID_CONFIG",
                "stop_after_tier must be one of the configured tiers",
                {"stop_after_tier": stop_after_tier, "tiers": self.config["tiers"]},
            )
        manifest = load_manifest(self.manifest_path)
        actions: list[dict[str, Any]] = []
        tier_results: dict[str, dict[str, Any]] = {}
        stopped = False
        stopped_after_tier: str | None = None
        for tier in self.config["tiers"]:
            if self.state.stop_requested():
                stopped = True
                break
            tier_results[tier] = self._run_tier(tier, manifest, actions)
            if after_tier_hook is not None:
                after_tier_hook(tier, self.state)
            if stop_after_tier == tier:
                self.state.request_stop(f"demo_stop_after_tier:{tier}")
            if self.state.stop_requested():
                stopped = True
                stopped_after_tier = tier
                break

        report_result: dict[str, Any] | None = None
        if self.state.stop_requested() and not stopped:
            stopped = True
            stopped_after_tier = self.config["tiers"][-1] if self.config["tiers"] else None
        if not stopped and len(tier_results) == len(self.config["tiers"]):
            report_result = self._run_report(tier_results, actions)

        recomputed = [
            action for action in actions if action["action"] == "recompute"
        ]
        reused = [action for action in actions if action["action"] == "reuse"]
        earliest_recomputed: dict[str, Any] | None = None
        if recomputed:
            earliest_index = min(STAGE_ORDER.index(item["stage"]) for item in recomputed)
            earliest_recomputed = {
                "stage": STAGE_ORDER[earliest_index],
                "actions": [
                    item for item in recomputed if item["stage"] == STAGE_ORDER[earliest_index]
                ],
            }
        result: dict[str, Any] = {
            "ok": True,
            "mode": mode,
            "config_sha256": self.config_sha256,
            "stopped": stopped,
            "stopped_after_tier": stopped_after_tier,
            "completed_tiers": [tier for tier in self.config["tiers"] if tier in tier_results],
            "report_written": report_result is not None,
            "reused_stage_count": len(reused),
            "recomputed_stage_count": len(recomputed),
            "earliest_recomputed_stage": earliest_recomputed,
            "actions": actions,
            "note": (
                "A graceful stop finishes the current indivisible tier unit and does not "
                "claim that an exit report is produced."
            ),
        }
        if report_result is not None:
            result["report_sha256"] = report_result["report"]["report_sha256"]
            result["report_path"] = str(self.output_dir / "diagnosis_report.json")
            if verify_full:
                if mode == "full":
                    result["incremental_verification"] = {
                        "ok": True,
                        "skipped": True,
                        "reason": "full mode has no incremental result to compare",
                    }
                else:
                    result["incremental_verification"] = self._verify_against_full(
                        report_result["report_bytes"],
                        tier_results,
                    )
        atomic_write_json(self.output_dir / "last_run.json", result, pretty=True)
        return result

    def _verify_against_full(
        self,
        incremental_report_bytes: bytes,
        tier_results: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        verification_root = Path(
            tempfile.mkdtemp(prefix="full-verify-", dir=self.output_dir)
        )
        try:
            temp_state = verification_root / "state"
            temp_output = verification_root / "outputs"
            verifier = Workbench(
                verification_root,
                copy.deepcopy(self.config),
                state_dir=temp_state,
                output_dir=temp_output,
                manifest_path=self.manifest_path,
            )
            full_result = verifier.run(mode="full", verify_full=False)
            if not full_result.get("report_written"):
                raise incremental_mismatch(
                    "Full verification run did not produce a report",
                    {"full_result": full_result},
                )
            full_record = verifier.state.load_latest("report", "all")
            full_report_bytes = full_record["artifact_paths"]["report.json"].read_bytes()
            differences: list[dict[str, Any]] = []
            if incremental_report_bytes != full_report_bytes:
                incremental_report = json.loads(incremental_report_bytes.decode("utf-8"))
                full_report = json.loads(full_report_bytes.decode("utf-8"))
                differences.extend(
                    _json_differences(incremental_report, full_report, limit=50)
                )
                if not differences:
                    differences.append(
                        {
                            "path": "$bytes",
                            "message": "canonical JSON bytes differ despite equal parsed values",
                        }
                    )
            for tier in self.config["tiers"]:
                if tier not in tier_results:
                    differences.append(
                        {"path": f"tiers.{tier}", "message": "missing incremental tier"}
                    )
                    continue
                incremental_record = tier_results[tier]["cluster"]
                full_cluster_record = verifier.state.load_latest("cluster", tier)
                if full_cluster_record is None:
                    differences.append(
                        {"path": f"tiers.{tier}.cluster", "message": "missing full cluster"}
                    )
                    continue
                incremental_labels = np.load(
                    incremental_record["artifact_paths"]["labels.npy"],
                    allow_pickle=False,
                )
                full_labels = np.load(
                    full_cluster_record["artifact_paths"]["labels.npy"],
                    allow_pickle=False,
                )
                if not _partitions_equivalent(incremental_labels, full_labels):
                    differences.append(
                        {
                            "path": f"tiers.{tier}.cluster.labels",
                            "message": "cluster partitions differ beyond label permutation",
                        }
                    )
            details = {
                "incremental_report_sha256": sha256_bytes(incremental_report_bytes),
                "full_report_sha256": sha256_bytes(full_report_bytes),
                "differences": differences[:50],
                "difference_count": len(differences),
            }
            if differences:
                raise incremental_mismatch(
                    "Incremental result does not match a clean full build",
                    details,
                )
            result = {
                "ok": True,
                "incremental_report_sha256": sha256_bytes(incremental_report_bytes),
                "full_report_sha256": sha256_bytes(full_report_bytes),
                "report_byte_identical": True,
                "labels_equivalent_under_permutation": True,
            }
            atomic_write_json(
                self.output_dir / "incremental_verification.json", result, pretty=True
            )
            return result
        finally:
            shutil.rmtree(verification_root, ignore_errors=True)

    def _report_record_for_current_config(self) -> dict[str, Any] | None:
        record = self.state.load_latest("report", "all")
        if record is None:
            return None
        if (
            record["config_version"] != self.config_sha256
            or record["config_fingerprint"] != self._stage_config_fp("report")
        ):
            raise invalid_stage(
                "Available diagnosis report belongs to a different configuration",
                {
                    "report_config_version": record["config_version"],
                    "current_config_version": self.config_sha256,
                },
            )
        return record

    def report(self) -> dict[str, Any]:
        record = self._report_record_for_current_config()
        if record is not None:
            report_path = record["artifact_paths"]["report.json"]
        else:
            report_path = self.output_dir / "diagnosis_report.json"
        if not report_path.exists():
            raise invalid_stage("Diagnosis report is not available; run the pipeline first")
        value = read_json(report_path)
        if value.get("config_sha256") != self.config_sha256:
            raise invalid_stage(
                "Diagnosis report file belongs to a different configuration",
                {
                    "report_config_sha256": value.get("config_sha256"),
                    "current_config_sha256": self.config_sha256,
                },
            )
        return value

    def report_bytes(self) -> bytes:
        record = self._report_record_for_current_config()
        if record is not None:
            return record["artifact_paths"]["report.json"].read_bytes()
        path = self.output_dir / "diagnosis_report.json"
        if not path.exists():
            raise invalid_stage("Diagnosis report is not available; run the pipeline first")
        value = read_json(path)
        if value.get("config_sha256") != self.config_sha256:
            raise invalid_stage(
                "Diagnosis report file belongs to a different configuration",
                {
                    "report_config_sha256": value.get("config_sha256"),
                    "current_config_sha256": self.config_sha256,
                },
            )
        return path.read_bytes()

    def rationale(self, tier: str) -> dict[str, Any]:
        red_record = self.state.load_latest("reduce", tier)
        clu_record = self.state.load_latest("cluster", tier)
        diag_record = self.state.load_latest("diagnose", tier)
        if red_record is None or clu_record is None or diag_record is None:
            raise invalid_stage(
                "Tier rationale is unavailable until reduce/cluster/diagnose complete",
                {"tier": tier},
            )
        for record in (red_record, clu_record, diag_record):
            if record["config_version"] != self.config_sha256:
                raise invalid_stage(
                    "Tier rationale contains records from a different configuration",
                    {
                        "tier": tier,
                        "record_stage": record["stage"],
                        "record_config_version": record["config_version"],
                        "current_config_version": self.config_sha256,
                    },
                )
        red = load_reduction_artifacts(red_record["artifact_paths"])["metadata"]
        clu = load_cluster_artifacts(clu_record["artifact_paths"])["metadata"]
        diag = load_diagnosis_artifact(diag_record["artifact_paths"]["diagnosis.json"])
        return {
            "ok": True,
            "tier": tier,
            "reduction": {
                "selected_candidate": red["candidate"],
                "target_dimension": red["target_dimension"],
                "reconstruction_error": red["reconstruction_error"],
                "cumulative_explained_variance": red[
                    "cumulative_explained_variance"
                ],
                "selection": red["selection"],
            },
            "clustering": {
                "algorithm": clu["algorithm"],
                "n_clusters": clu["n_clusters"],
                "k_reason": clu["k_reason"],
                "k_search": clu["k_search"],
                "iterations": clu["iterations"],
                "empty_cluster_policy": clu["empty_cluster_policy"],
                "empty_clusters_handled": clu["empty_clusters_handled"],
            },
            "diagnosis": diag,
        }

    def fingerprints(self) -> dict[str, Any]:
        manifest = load_manifest(self.manifest_path)
        tiers: dict[str, Any] = {}
        for tier in self.config["tiers"]:
            stages: dict[str, Any] = {}
            for stage in ("preprocess", "reduce", "cluster", "diagnose"):
                record = self.state.load_latest(stage, tier)
                stages[stage] = {
                    "stage_config_fingerprint": self._stage_config_fp(stage),
                    "checkpoint": (
                        None
                        if record is None
                        else {
                            "sequence": record["sequence"],
                            "input_fingerprint": record["input_fingerprint"],
                            "config_fingerprint": record["config_fingerprint"],
                            "config_version": record["config_version"],
                            "artifact_digest": record["artifact_digest"],
                        }
                    ),
                }
            pre_fp = preprocess_input_fingerprint(
                self.manifest_path, manifest, tier, self.config
            )
            stages["preprocess"]["current_input_fingerprint"] = pre_fp
            tiers[tier] = stages
        report_record = self.state.load_latest("report", "all")
        report = {
            "stage_config_fingerprint": self._stage_config_fp("report"),
            "checkpoint": (
                None
                if report_record is None
                else {
                    "sequence": report_record["sequence"],
                    "input_fingerprint": report_record["input_fingerprint"],
                    "config_fingerprint": report_record["config_fingerprint"],
                    "config_version": report_record["config_version"],
                    "artifact_digest": report_record["artifact_digest"],
                }
            ),
        }
        return {
            "ok": True,
            "config_sha256": self.config_sha256,
            "sample_manifest": str(self.manifest_path),
            "sample_manifest_sha256": sha256_file(self.manifest_path),
            "tiers": tiers,
            "report": report,
        }


def _partitions_equivalent(left: np.ndarray, right: np.ndarray) -> bool:
    left = np.asarray(left)
    right = np.asarray(right)
    if left.shape != right.shape:
        return False
    mapping: dict[int, int] = {}
    reverse: dict[int, int] = {}
    for left_label, right_label in zip(left.tolist(), right.tolist()):
        existing_right = mapping.get(left_label)
        if existing_right is None:
            mapping[left_label] = right_label
        elif existing_right != right_label:
            return False
        existing_left = reverse.get(right_label)
        if existing_left is None:
            reverse[right_label] = left_label
        elif existing_left != left_label:
            return False
    return True


def _json_differences(
    left: Any,
    right: Any,
    *,
    path: str = "$",
    limit: int = 50,
) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []

    def visit(a: Any, b: Any, current: str) -> None:
        if len(differences) >= limit:
            return
        if type(a) is not type(b):
            differences.append(
                {"path": current, "left_type": type(a).__name__, "right_type": type(b).__name__}
            )
            return
        if isinstance(a, dict):
            for key in sorted(set(a) | set(b)):
                child = f"{current}.{key}"
                if key not in a:
                    differences.append({"path": child, "message": "missing on incremental side"})
                elif key not in b:
                    differences.append({"path": child, "message": "missing on full side"})
                else:
                    visit(a[key], b[key], child)
            return
        if isinstance(a, list):
            if len(a) != len(b):
                differences.append(
                    {"path": current, "left_length": len(a), "right_length": len(b)}
                )
                return
            for index, (left_item, right_item) in enumerate(zip(a, b)):
                visit(left_item, right_item, f"{current}[{index}]")
            return
        if a != b:
            differences.append({"path": current, "left": a, "right": b})

    visit(left, right, path)
    return differences
