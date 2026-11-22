from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes, read_json, sha256_bytes, sha256_file
from .config import Config
from .errors import E_INVALID_STAGE, E_STALE_VERSION, business_error
from .manifest import Manifest, load_batch, load_manifest
from .numerics import evaluate_batch_density
from .reference import ReferenceTable, adjudicate_drift, load_reference
from .state import DensityState, merge_batch_state
from .workspace import Workspace, WorkspaceHistory


def import_inputs(
    workspace_path: str | Path,
    manifest_path: str | Path,
    config_path: str | Path,
    reference_path: str | Path,
) -> dict[str, Any]:
    workspace = Workspace(workspace_path).initialize()
    manifest_path = Path(manifest_path).resolve()
    config_path = Path(config_path).resolve()
    reference_path = Path(reference_path).resolve()
    manifest = load_manifest(manifest_path)
    Config.from_dict(read_json(config_path))
    reference = load_reference(reference_path)
    batch_inputs = [
        {"batch_id": spec.batch_id, "input_digest": manifest.input_digest(spec)}
        for spec in manifest.batches
    ]
    snapshot = {
        "schema_version": 1,
        "manifest_path": str(manifest_path),
        "config_path": str(config_path),
        "reference_path": str(reference_path),
        "manifest_digest": sha256_file(manifest_path),
        "config_digest": sha256_file(config_path),
        "reference_digest": reference.digest,
        "batch_input_digests": batch_inputs,
    }
    workspace.write_inputs(snapshot)
    return snapshot


class Engine:
    def __init__(
        self,
        workspace: Workspace,
        manifest: Manifest,
        config: Config,
        reference: ReferenceTable,
        inputs: dict[str, Any],
    ) -> None:
        self.workspace = workspace
        self.manifest = manifest
        self.config = config
        self.reference = reference
        self.inputs = inputs
        self.config_fingerprint = config.config_fingerprint(reference.digest)
        self.density_stage_fingerprint = config.density_stage_fingerprint()
        self.adjudication_stage_fingerprint = config.adjudication_stage_fingerprint(
            reference.digest
        )

    @classmethod
    def from_workspace(cls, workspace: str | Path) -> Engine:
        workspace_obj = Workspace(workspace).initialize()
        snapshot = workspace_obj.load_inputs()
        manifest = load_manifest(snapshot["manifest_path"])
        config = Config.from_dict(read_json(snapshot["config_path"]))
        reference = load_reference(snapshot["reference_path"])
        return cls(workspace_obj, manifest, config, reference, snapshot)

    def batch_input_digests(self) -> list[dict[str, Any]]:
        return [
            {
                "batch_id": spec.batch_id,
                "input_digest": self.manifest.input_digest(spec),
            }
            for spec in self.manifest.batches
        ]

    def planned_version_fingerprint(self) -> str:
        payload = {
            "config_fingerprint": self.config_fingerprint,
            "manifest_digest": sha256_file(self.manifest.path),
            "batch_inputs": self.batch_input_digests(),
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def _history(self) -> WorkspaceHistory:
        return self.workspace.history()

    def _state_from_history(self, history: WorkspaceHistory) -> DensityState:
        head = history.head
        if head is None:
            return DensityState.empty(self.config)
        return DensityState.from_record(head["state"])

    def inspect_versions(self) -> dict[str, Any]:
        history = self._history()
        records = list(history.records)
        reasons: set[str] = set()
        earliest: int | None = None
        current_inputs = {
            item["batch_id"]: item["input_digest"]
            for item in self.batch_input_digests()
        }
        for record in records:
            batch_id = int(record["batch_id"])
            candidate: int | None = None
            if batch_id >= len(self.manifest.batches):
                reasons.add("batch-removed-from-manifest")
                candidate = len(self.manifest.batches)
            else:
                input_digest = current_inputs[batch_id]
                if record.get("input_digest") != input_digest:
                    reasons.add(f"batch-content:{batch_id}")
                    candidate = batch_id
                if (
                    record.get("stage_fingerprints", {}).get("density")
                    != self.density_stage_fingerprint
                ):
                    reasons.add("bandwidth/chunk/sum/grid-density-config")
                    candidate = (
                        batch_id if candidate is None else min(candidate, batch_id)
                    )
                if (
                    record.get("stage_fingerprints", {}).get("adjudication")
                    != self.adjudication_stage_fingerprint
                ):
                    reasons.add("drift-threshold/reference-quantiles")
                    candidate = (
                        batch_id if candidate is None else min(candidate, batch_id)
                    )
                if record.get("config_fingerprint") != self.config_fingerprint:
                    reasons.add("config-fingerprint")
                    candidate = (
                        batch_id if candidate is None else min(candidate, batch_id)
                    )
            if candidate is not None:
                earliest = candidate if earliest is None else min(earliest, candidate)
        last_existing_batch = max(
            (int(record["batch_id"]) for record in records), default=-1
        )
        affected_end = max(last_existing_batch, len(self.manifest.batches) - 1)
        head = history.head
        return {
            "requires_invalidation": earliest is not None,
            "earliest_affected_batch": earliest,
            "affected_batch_start": earliest,
            "affected_batch_end": affected_end if earliest is not None else None,
            "reasons": sorted(reasons),
            "reused_batch_range": [0, earliest - 1]
            if earliest is not None and earliest > 0
            else ([] if earliest == 0 else None),
            "old_version_fingerprint": head.get("version_fingerprint")
            if head
            else None,
            "new_version_fingerprint": self.planned_version_fingerprint(),
            "based_on_generation": history.generation,
            "current_config_fingerprint": self.config_fingerprint,
            "current_density_stage_fingerprint": self.density_stage_fingerprint,
            "current_adjudication_stage_fingerprint": self.adjudication_stage_fingerprint,
        }

    def reconcile(self) -> dict[str, Any]:
        plan = self.inspect_versions()
        if not plan["requires_invalidation"]:
            return {"changed": False, "plan": plan, "event": None}
        history = self._history()
        if not history.records:
            return {"changed": False, "plan": plan, "event": None}
        event = self.workspace.apply_invalidation(
            reason=",".join(plan["reasons"]),
            affected_batch_start=int(plan["affected_batch_start"]),
            affected_batch_end=int(plan["affected_batch_end"]),
            old_version_fingerprint=plan["old_version_fingerprint"],
            new_version_fingerprint=plan["new_version_fingerprint"],
            based_on_generation=int(plan["based_on_generation"]),
        )
        return {"changed": True, "plan": plan, "event": event}

    def version_info(self) -> dict[str, Any]:
        history = self._history()
        head = history.head
        return {
            "config_version": self.config.config_version,
            "config_fingerprint": self.config_fingerprint,
            "density_stage_fingerprint": self.density_stage_fingerprint,
            "adjudication_stage_fingerprint": self.adjudication_stage_fingerprint,
            "manifest_digest": sha256_file(self.manifest.path),
            "reference_digest": self.reference.digest,
            "planned_version_fingerprint": self.planned_version_fingerprint(),
            "generation": history.generation,
            "batch_input_digests": self.batch_input_digests(),
            "checkpoint_head": (
                {
                    "sequence": int(head["sequence"]),
                    "batch_id": int(head["batch_id"]),
                    "state_version": int(head["state_summary"]["state_version"]),
                    "version_fingerprint": head["version_fingerprint"],
                    "record_sha256": sha256_bytes(canonical_json_bytes(head) + b"\n"),
                }
                if head
                else None
            ),
            "record_count": len(history.records),
            "next_sequence": history.next_sequence,
            "next_batch_id": history.next_batch_id,
            "latest_invalidation": history.events[-1] if history.events else None,
            "invalidations": list(history.events),
            "ignored_checkpoints": list(history.ignored_records),
            "version_change_plan": self.inspect_versions(),
        }

    def _check_expected_version(
        self, expected_version: int | None, actual_version: int
    ) -> None:
        if expected_version is not None and int(expected_version) != int(
            actual_version
        ):
            raise business_error(
                E_STALE_VERSION,
                "asserted state version does not match the current active version",
                expected_version=int(expected_version),
                actual_version=int(actual_version),
            )

    def _load_parent_state(self, history: WorkspaceHistory) -> DensityState:
        return self._state_from_history(history)

    def prepare_batch_record(
        self,
        batch_id: int,
        expected_version: int | None = None,
        reconcile: bool = True,
    ) -> dict[str, Any]:
        if reconcile:
            self.reconcile()
        history = self._history()
        expected_batch = history.next_batch_id
        self._check_expected_version(expected_version, history.state_version)
        if batch_id != expected_batch:
            raise business_error(
                E_INVALID_STAGE,
                "batch is not the next contiguous stage",
                requested_batch_id=batch_id,
                expected_batch_id=expected_batch,
                predecessor_missing=batch_id > expected_batch,
            )
        if batch_id < 0 or batch_id >= len(self.manifest.batches):
            raise business_error(
                E_INVALID_STAGE,
                "batch is outside the manifest",
                requested_batch_id=batch_id,
                batch_count=len(self.manifest.batches),
            )
        parent_state = self._load_parent_state(history)
        loaded = load_batch(self.manifest, batch_id)
        density = evaluate_batch_density(loaded, self.config, batch_id=batch_id)
        new_state = merge_batch_state(
            parent_state,
            density,
            self.config,
            batch_id,
            self.config_fingerprint,
        )
        adjudication = adjudicate_drift(
            new_state, self.reference, self.config, batch_id
        )
        state_summary = new_state.summary(self.config_fingerprint)
        if (
            adjudication["state_version_fingerprint"]
            != state_summary["version_fingerprint"]
        ):
            raise business_error(
                E_STALE_VERSION,
                "adjudication does not reference the active density state version",
                adjudication_version_fingerprint=adjudication[
                    "state_version_fingerprint"
                ],
                state_version_fingerprint=state_summary["version_fingerprint"],
            )
        record = {
            "schema_version": 1,
            "sequence": history.next_sequence,
            "batch_id": batch_id,
            "generation": history.generation,
            "input_digest": loaded.input_digest,
            "input_file": loaded.file_path.name,
            "manifest_digest": sha256_file(self.manifest.path),
            "config_version": self.config.config_version,
            "config_fingerprint": self.config_fingerprint,
            "stage_fingerprints": {
                "density": self.density_stage_fingerprint,
                "adjudication": self.adjudication_stage_fingerprint,
            },
            "version_fingerprint": state_summary["version_fingerprint"],
            "parent_state_version": parent_state.version,
            "parent_state_fingerprint": (
                parent_state.version_fingerprint(self.config_fingerprint)
                if parent_state.version
                else None
            ),
            "state_summary": state_summary,
            "state": new_state.to_record(),
            "density": {
                "batch_id": batch_id,
                "raw_shape": list(loaded.raw_shape),
                "effective_samples": density.effective_samples,
                "dropped_rows": loaded.dropped_rows,
                "valid_columns": list(loaded.spec.valid_columns),
                "source_labels": list(loaded.source_labels),
                "bandwidth": list(density.bandwidth),
                "chunk_size": self.config.density.chunk_size,
                "sum_strategy": self.config.density.sum_strategy,
                "scale": self.config.density.density_scale,
                "local_curve_sha256": density.local_curve_sha256,
                "blocks": list(density.blocks),
                "error_report": density.error_report,
            },
            "adjudication": adjudication,
            "checkpoint_sequence": history.next_sequence,
        }
        return record

    def publish_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.workspace.publish_record(record)

    def run_next(
        self,
        expected_version: int | None = None,
        reconcile: bool = True,
        on_batch: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        stop = self.workspace.consume_stop()
        if stop is not None:
            return {"status": "stopped", "stop_request": stop, "published_batch": None}
        if reconcile:
            self.reconcile()
        history = self._history()
        self._check_expected_version(expected_version, history.state_version)
        batch_id = history.next_batch_id
        if batch_id >= len(self.manifest.batches):
            return {"status": "complete", "published_batch": None}
        record = self.prepare_batch_record(
            batch_id, expected_version=expected_version, reconcile=False
        )
        published = self.publish_record(record)
        if on_batch is not None:
            on_batch(published)
        stop = self.workspace.consume_stop()
        if stop is not None:
            return {
                "status": "stopped",
                "stop_request": stop,
                "published_batch": int(published["batch_id"]),
            }
        return {
            "status": "published",
            "published_batch": int(published["batch_id"]),
            "record": published,
        }

    def run(
        self,
        batch_id: int | None = None,
        max_batches: int | None = None,
        expected_version: int | None = None,
        reconcile: bool = True,
        on_batch: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        if batch_id is not None:
            if reconcile:
                self.reconcile()
            history = self._history()
            self._check_expected_version(expected_version, history.state_version)
            if batch_id != history.next_batch_id:
                raise business_error(
                    E_INVALID_STAGE,
                    "requested batch is not the next stage",
                    requested_batch_id=batch_id,
                    expected_batch_id=history.next_batch_id,
                )
            stop = self.workspace.consume_stop()
            if stop is not None:
                return {
                    "status": "stopped",
                    "stop_request": stop,
                    "published_batch": None,
                }
            record = self.prepare_batch_record(
                batch_id, expected_version=expected_version, reconcile=False
            )
            published = self.publish_record(record)
            if on_batch is not None:
                on_batch(published)
            stop = self.workspace.consume_stop()
            return {
                "status": "stopped" if stop is not None else "published",
                "stop_request": stop,
                "published_batch": int(published["batch_id"]),
                "record": published,
            }
        published_batches: list[int] = []
        while True:
            if max_batches is not None and len(published_batches) >= max_batches:
                return {"status": "bounded", "published_batches": published_batches}
            result = self.run_next(
                expected_version=expected_version,
                reconcile=reconcile,
                on_batch=on_batch,
            )
            if result["status"] == "published":
                published_batches.append(int(result["published_batch"]))
                expected_version = int(
                    result["record"]["state_summary"]["state_version"]
                )
                reconcile = False
                continue
            result["published_batches"] = published_batches
            return result

    def resume(
        self,
        expected_version: int | None = None,
        max_batches: int | None = None,
        on_batch: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        return self.run(
            batch_id=None,
            max_batches=max_batches,
            expected_version=expected_version,
            reconcile=True,
            on_batch=on_batch,
        )

    def request_stop(self, details: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "based_on_state_version": self._history().state_version,
            **(details or {}),
        }
        return self.workspace.request_stop(payload)

    def _record_for_batch(
        self, batch_id: int, expected_version: int | None = None
    ) -> dict[str, Any]:
        history = self._history()
        self._check_expected_version(expected_version, history.state_version)
        if batch_id < 0 or batch_id >= len(self.manifest.batches):
            raise business_error(
                E_INVALID_STAGE,
                "batch is outside the manifest",
                requested_batch_id=batch_id,
                batch_count=len(self.manifest.batches),
            )
        records = history.records
        if batch_id >= len(records):
            raise business_error(
                E_INVALID_STAGE,
                "requested batch has no committed state and adjudication",
                requested_batch_id=batch_id,
                available_batch_count=len(records),
                missing_predecessor=True,
            )
        record = records[batch_id]
        if int(record["batch_id"]) != batch_id:
            raise business_error(
                E_INVALID_STAGE,
                "checkpoint prefix is not contiguous for the requested batch",
                requested_batch_id=batch_id,
            )
        if batch_id >= len(self.manifest.batches):
            raise business_error(
                E_STALE_VERSION,
                "record belongs to a batch removed from the current manifest",
                batch_id=batch_id,
            )
        current_input_digest = self.manifest.input_digest(self.manifest.by_id(batch_id))
        stale_reasons: list[str] = []
        if record.get("input_digest") != current_input_digest:
            stale_reasons.append("batch-content")
        if (
            record.get("stage_fingerprints", {}).get("density")
            != self.density_stage_fingerprint
        ):
            stale_reasons.append("density-config")
        if (
            record.get("stage_fingerprints", {}).get("adjudication")
            != self.adjudication_stage_fingerprint
        ):
            stale_reasons.append("adjudication-config-or-reference")
        if record.get("config_fingerprint") != self.config_fingerprint:
            stale_reasons.append("config-fingerprint")
        if stale_reasons:
            raise business_error(
                E_STALE_VERSION,
                "committed record is invalid under the current version; reconcile and rebuild before querying",
                batch_id=batch_id,
                stale_reasons=stale_reasons,
                record_version_fingerprint=record.get("version_fingerprint"),
                current_config_fingerprint=self.config_fingerprint,
            )
        return record

    def get_state(
        self, batch_id: int, expected_version: int | None = None
    ) -> dict[str, Any]:
        record = self._record_for_batch(batch_id, expected_version=expected_version)
        state = DensityState.from_record(record["state"])
        curve = state.curve()
        return {
            "batch_id": batch_id,
            "sequence": int(record["sequence"]),
            "state_summary": record["state_summary"],
            "state": state.to_record(),
            "curve_sha256": state.curve_sha256(),
            "curve": curve.tolist(),
            "curve_bytes_sha256": sha256_bytes(
                curve.astype("<f8", copy=False).tobytes()
            ),
        }

    def get_curve(
        self, batch_id: int, expected_version: int | None = None
    ) -> dict[str, Any]:
        record = self._record_for_batch(batch_id, expected_version=expected_version)
        state = DensityState.from_record(record["state"])
        curve = state.curve()
        return {
            "batch_id": batch_id,
            "state_summary": record["state_summary"],
            "curve": curve.tolist(),
            "curve_sha256": state.curve_sha256(),
        }

    def get_verdict(
        self, batch_id: int, expected_version: int | None = None
    ) -> dict[str, Any]:
        record = self._record_for_batch(batch_id, expected_version=expected_version)
        return dict(record["adjudication"])

    def get_error_report(
        self, batch_id: int, expected_version: int | None = None
    ) -> dict[str, Any]:
        record = self._record_for_batch(batch_id, expected_version=expected_version)
        report = dict(record["density"]["error_report"])
        report["blocks"] = list(record["density"]["blocks"])
        report["local_curve_sha256"] = record["density"]["local_curve_sha256"]
        return report

    def final_summary(self) -> dict[str, Any]:
        history = self._history()
        head = history.head
        if head is None:
            return {
                "status": "empty",
                "record_count": 0,
                "state_version": 0,
                "generation": history.generation,
            }
        verdicts = [record["adjudication"]["decision"] for record in history.records]
        errors = [record["density"]["error_report"] for record in history.records]
        return {
            "status": "complete"
            if len(history.records) == len(self.manifest.batches)
            else "partial",
            "record_count": len(history.records),
            "batch_count": len(self.manifest.batches),
            "final_state_summary": head["state_summary"],
            "final_version_fingerprint": head["version_fingerprint"],
            "final_curve_sha256": head["state_summary"]["curve_sha256"],
            "verdicts": verdicts,
            "max_error_bound_density": max(
                float(item["error_bound_density"]) for item in errors
            ),
            "min_significant_digits": min(
                int(item["significant_digits"]) for item in errors
            ),
            "record_chain_sha256": self.workspace.record_chain_digest(),
            "generation": history.generation,
            "version_change_plan": self.inspect_versions(),
        }
