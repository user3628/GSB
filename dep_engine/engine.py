"""Incremental orchestration, invalidation planning, recovery and verification."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    read_json,
    sha256_bytes,
    sha256_obj,
)
from .config import AnalysisConfig
from .constants import (
    AGGREGATION,
    DECODING,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    FEATUREIZATION,
    SCHEMA_VERSION,
    STAGE_CONFIG_FIELDS,
    STAGE_SCHEMA_VERSIONS,
    STAGES,
    VALIDATION,
)
from .errors import EngineError
from .manifest import Manifest, ShardEntry
from .pipeline import (
    aggregate_stage,
    decode_artifact,
    featureize,
    validate_stage,
)
from .store import CheckpointStore, RecoveredState


def _stage_inputs(
    *,
    stage: str,
    shard_entry: ShardEntry,
    shard_content_digest: str,
    scoped_config: dict[str, Any],
    upstream_digests: dict[str, str],
) -> dict[str, Any]:
    components: dict[str, Any] = {
        "stage_schema_version": STAGE_SCHEMA_VERSIONS[stage],
    }
    if stage == FEATUREIZATION:
        components.update(
            {
                "shard_content_sha256": shard_content_digest,
                "manifest_entry_sha256": sha256_obj(shard_entry.as_dict()),
            }
        )
    else:
        previous = STAGES[STAGES.index(stage) - 1]
        components["upstream_output_digests"] = {
            previous: upstream_digests[previous]
        }
    for field in STAGE_CONFIG_FIELDS[stage]:
        components[field] = scoped_config[field]
    return components


class Engine:
    def __init__(
        self,
        root: Path | str = ".",
        *,
        state_dir: str = "runtime/state",
        views_dir: str = "runtime/views",
        config_path: str = "config/analysis.yaml",
        manifest_path: str = "data/manifest.json",
    ):
        self.root = Path(root).resolve()
        self.state_dir = (self.root / state_dir).resolve()
        self.views_dir = (self.root / views_dir).resolve()
        self.config_path = (self.root / config_path).resolve()
        self.config_relative_path = config_path
        self.manifest_relative_path = manifest_path
        self.manifest_path = (self.root / manifest_path).resolve()
        self.checkpoints_dir = self.state_dir / "checkpoints"
        self.stop_marker = self.state_dir / "STOP"

    # ------------------------------------------------------------------
    # Inputs and workspace queries
    # ------------------------------------------------------------------
    def load_config(self) -> AnalysisConfig:
        return AnalysisConfig.from_yaml(self.config_path)

    def load_manifest(self) -> Manifest:
        return Manifest.load(self.root, self.manifest_relative_path)

    def store(self, manifest: Manifest | None = None) -> CheckpointStore:
        manifest = manifest or self.load_manifest()
        return CheckpointStore(self.checkpoints_dir, manifest.shard_names())

    def checkpoint_summary(self) -> dict[str, Any]:
        state = self.store().scan()
        return {
            "last_sequence": state.last_sequence,
            "next_sequence": state.next_sequence,
            "last_shard_sequence": {
                str(key): value for key, value in sorted(state.last_shard_sequence.items())
            },
            "record_count": len(state.records),
            "ignored": state.ignored,
        }

    def _stage_components(
        self,
        *,
        stage: str,
        shard_entry: ShardEntry,
        config: AnalysisConfig,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return config.stage_components(stage), config.stage_fingerprint(stage)

    # ------------------------------------------------------------------
    # Version fingerprints and invalidation planning
    # ------------------------------------------------------------------
    def _classify_invalidation(
        self,
        stage: str,
        record: dict[str, Any] | None,
        expected_components: dict[str, Any],
    ) -> tuple[str, list[str]]:
        if record is None:
            return "missing_checkpoint", []
        old = record.get("input_components", {})
        changed = sorted(
            key
            for key in set(old) | set(expected_components)
            if old.get(key) != expected_components.get(key)
        )
        if stage == FEATUREIZATION and "shard_content_sha256" in changed:
            return "input_content_changed", changed
        if stage == FEATUREIZATION and "manifest_entry_sha256" in changed:
            return "manifest_entry_changed", changed
        config_fields = set(STAGE_CONFIG_FIELDS[stage])
        if any(key in config_fields for key in changed):
            return "config_changed", changed
        if "upstream_output_digests" in changed:
            return "upstream_artifact_changed", changed
        if "stage_schema_version" in changed:
            return "stage_schema_changed", changed
        return "input_fingerprint_changed", changed

    def plan(
        self,
        *,
        config: AnalysisConfig | None = None,
        manifest: Manifest | None = None,
        state: RecoveredState | None = None,
    ) -> dict[str, Any]:
        config = config or self.load_config()
        manifest = manifest or self.load_manifest()
        state = state or self.store(manifest).scan()
        shard_plans: list[dict[str, Any]] = []

        for entry in manifest.shards:
            content_digest = manifest.shard_content_digest(entry.index)
            outputs: dict[str, str] = {}
            earliest: str | None = None
            reused: list[str] = []
            invalidated: list[str] = []
            stage_reports: dict[str, Any] = {}

            for stage in STAGES:
                config_components, config_fp = self._stage_components(
                    stage=stage,
                    shard_entry=entry,
                    config=config,
                )
                record = state.latest(entry.index, stage)

                if earliest is not None:
                    invalidated.append(stage)
                    stage_reports[stage] = {
                        "status": "invalidated",
                        "reason": "upstream_invalidated",
                        "caused_by_stage": earliest,
                        "stored_input_fingerprint": (
                            record["input_fingerprint"] if record else None
                        ),
                        "expected_input_fingerprint": None,
                        "stage_config_fingerprint": config_fp,
                    }
                    continue

                try:
                    components = _stage_inputs(
                        stage=stage,
                        shard_entry=entry,
                        shard_content_digest=content_digest,
                        scoped_config=config_components,
                        upstream_digests=outputs,
                    )
                except KeyError:
                    # This cannot normally happen because earlier stages are
                    # checked first, but it makes missing prerequisites explicit.
                    stage_reports[stage] = {
                        "status": "invalidated",
                        "reason": "missing_prerequisite",
                        "stored_input_fingerprint": (
                            record["input_fingerprint"] if record else None
                        ),
                        "expected_input_fingerprint": None,
                        "stage_config_fingerprint": config_fp,
                    }
                    invalidated.append(stage)
                    earliest = stage
                    continue

                expected_fp = sha256_obj(components)
                if record is not None and record["input_fingerprint"] == expected_fp:
                    reused.append(stage)
                    outputs[stage] = record["output_digest"]
                    stage_reports[stage] = {
                        "status": "reusable",
                        "reason": "input_fingerprint_match",
                        "stored_input_fingerprint": record["input_fingerprint"],
                        "expected_input_fingerprint": expected_fp,
                        "stored_output_digest": record["output_digest"],
                        "shard_sequence": record["shard_sequence"],
                        "sequence": record["sequence"],
                        "stage_config_fingerprint": config_fp,
                    }
                else:
                    reason, changed = self._classify_invalidation(
                        stage, record, components
                    )
                    earliest = stage
                    invalidated.append(stage)
                    stage_reports[stage] = {
                        "status": "invalidated",
                        "reason": reason,
                        "changed_components": changed,
                        "stored_input_fingerprint": (
                            record["input_fingerprint"] if record else None
                        ),
                        "expected_input_fingerprint": expected_fp,
                        "stored_stage_config_fingerprint": (
                            record["stage_config_fingerprint"] if record else None
                        ),
                        "expected_stage_config_fingerprint": config_fp,
                        "stored_output_digest": (
                            record["output_digest"] if record else None
                        ),
                    }

            shard_plans.append(
                {
                    "shard_index": entry.index,
                    "shard_name": entry.file,
                    "content_sha256": content_digest,
                    "earliest_invalid_stage": earliest,
                    "reused_stages": reused,
                    "invalidated_stages": invalidated,
                    "stages": stage_reports,
                }
            )

        globally_invalid = [
            plan["earliest_invalid_stage"]
            for plan in shard_plans
            if plan["earliest_invalid_stage"] is not None
        ]
        global_earliest = (
            min(globally_invalid, key=lambda stage: STAGES.index(stage))
            if globally_invalid
            else None
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "config_fingerprint": config.fingerprint(),
            "stage_config_fingerprints": config.stage_fingerprints(),
            "manifest_fingerprint": manifest.manifest_fingerprint(),
            "shard_count": manifest.shard_count,
            "global_earliest_invalid_stage": global_earliest,
            "all_reusable": global_earliest is None and all(
                len(plan["reused_stages"]) == len(STAGES) for plan in shard_plans
            ),
            "shards": shard_plans,
        }

    # ------------------------------------------------------------------
    # Build and resilient resume
    # ------------------------------------------------------------------
    def _stop_requested(self) -> bool:
        return self.stop_marker.exists()

    def request_stop(self) -> None:
        atomic_write_bytes(self.stop_marker, b"stop\n")

    def clear_stop(self) -> None:
        try:
            self.stop_marker.unlink()
        except FileNotFoundError:
            pass

    def _consume_stop(self) -> None:
        self.clear_stop()

    def _expected_stage_output(
        self,
        *,
        stage: str,
        entry: ShardEntry,
        manifest: Manifest,
        config: AnalysisConfig,
        outputs: dict[str, str],
        state: RecoveredState,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, str], str, str, dict[str, Any]]:
        config_components, config_fp = self._stage_components(
            stage=stage, shard_entry=entry, config=config
        )
        content_digest = manifest.shard_content_digest(entry.index)
        components = _stage_inputs(
            stage=stage,
            shard_entry=entry,
            shard_content_digest=content_digest,
            scoped_config=config_components,
            upstream_digests=outputs,
        )
        input_fp = sha256_obj(components)
        previous = STAGES[STAGES.index(stage) - 1] if stage != FEATUREIZATION else None
        upstream = {previous: outputs[previous]} if previous else {}
        return (
            components,
            config_components,
            upstream,
            input_fp,
            config_fp,
            {"content_digest": content_digest},
        )

    def _execute_stage(
        self,
        *,
        stage: str,
        entry: ShardEntry,
        manifest: Manifest,
        config: AnalysisConfig,
        outputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        if stage == FEATUREIZATION:
            return featureize(
                entry=entry,
                text=manifest.shard_text(entry.index),
                content_digest=manifest.shard_content_digest(entry.index),
                config=config,
            )
        if stage == DECODING:
            return decode_artifact(
                feature_artifact=outputs[FEATUREIZATION],
                config=config,
            )
        if stage == VALIDATION:
            return validate_stage(
                decoded_artifact=outputs[DECODING],
                config=config,
            )
        if stage == AGGREGATION:
            return aggregate_stage(outputs[VALIDATION])
        raise AssertionError(f"unknown stage {stage}")

    def build(
        self,
        *,
        stop_after_shards: int | None = None,
    ) -> dict[str, Any]:
        if stop_after_shards is not None and stop_after_shards < 0:
            raise EngineError(
                "E_INVALID_STAGE",
                "stop_after_shards must be non-negative",
                {"valid_stages": list(STAGES)},
            )

        config = self.load_config()
        manifest = self.load_manifest()
        store = self.store(manifest)
        state = store.scan()
        plan = self.plan(config=config, manifest=manifest, state=state)
        computed: list[dict[str, Any]] = []
        reused: list[dict[str, Any]] = []
        processed_shards = 0
        stopped = False

        for entry in manifest.shards:
            if stop_after_shards is not None and processed_shards >= stop_after_shards:
                stopped = True
                break
            if self._stop_requested():
                self._consume_stop()
                stopped = True
                break

            state = store.scan()
            outputs: dict[str, dict[str, Any]] = {}
            output_digests: dict[str, str] = {}
            for stage in STAGES:
                state = store.scan()
                record = state.latest(entry.index, stage)
                try:
                    (
                        components,
                        config_components,
                        upstream_digests,
                        input_fp,
                        config_fp,
                        _extra,
                    ) = self._expected_stage_output(
                        stage=stage,
                        entry=entry,
                        manifest=manifest,
                        config=config,
                        outputs=output_digests,
                        state=state,
                    )
                except KeyError:
                    raise EngineError(
                        "E_INVALID_STAGE",
                        "a prerequisite stage artifact is missing",
                        {
                            "shard_index": entry.index,
                            "stage": stage,
                            "valid_stages": list(STAGES),
                            "available_stages": state.available_stages(entry.index),
                        },
                    )

                if record is not None and record["input_fingerprint"] == input_fp:
                    outputs[stage] = record["output"]
                    output_digests[stage] = record["output_digest"]
                    reused.append(
                        {
                            "shard_index": entry.index,
                            "stage": stage,
                            "shard_sequence": record["shard_sequence"],
                            "sequence": record["sequence"],
                        }
                    )
                    continue

                # Recompute only this invalidated stage.  Real upstream output
                # objects are used directly; stale artifacts are never silently
                # substituted.
                output = self._execute_stage(
                    stage=stage,
                    entry=entry,
                    manifest=manifest,
                    config=config,
                    outputs=outputs,
                )
                published = None
                last_error: EngineError | None = None
                for _attempt in range(3):
                    state = store.scan()
                    record = state.latest(entry.index, stage)
                    if record is not None and record["input_fingerprint"] == input_fp:
                        published = record
                        break
                    expected_sequence = state.last_shard_sequence.get(entry.index, 0) + 1
                    try:
                        published = store.publish(
                            shard_index=entry.index,
                            shard_name=entry.file,
                            stage=stage,
                            input_fingerprint=input_fp,
                            input_components=components,
                            config_fingerprint=config.fingerprint(),
                            stage_config_fingerprint=config_fp,
                            stage_config_components=config_components,
                            upstream_output_digests=upstream_digests,
                            output=output,
                            expected_shard_sequence=expected_sequence,
                        )
                        break
                    except EngineError as exc:
                        if exc.code != "E_STALE_CHECKPOINT":
                            raise
                        last_error = exc
                        # Another worker may have published this exact input.
                        state = store.scan()
                        record = state.latest(entry.index, stage)
                        if record is not None and record["input_fingerprint"] == input_fp:
                            published = record
                            break
                if published is None:
                    if last_error is not None:
                        raise last_error
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "unable to publish stage after concurrent updates",
                        {
                            "shard_index": entry.index,
                            "stage": stage,
                            "valid_stages": list(STAGES),
                        },
                    )
                outputs[stage] = published["output"]
                output_digests[stage] = published["output_digest"]
                computed.append(
                    {
                        "shard_index": entry.index,
                        "stage": stage,
                        "shard_sequence": published["shard_sequence"],
                        "sequence": published["sequence"],
                        "input_fingerprint": published["input_fingerprint"],
                        "output_digest": published["output_digest"],
                    }
                )
            processed_shards += 1

        complete = processed_shards == manifest.shard_count
        summary = None
        if complete and not stopped:
            final_plan = self.plan(config=config, manifest=manifest)
            if not final_plan["all_reusable"]:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "workspace changed while the build was running",
                    {
                        "reason": "workspace_changed_during_build",
                        "global_earliest_invalid_stage": final_plan[
                            "global_earliest_invalid_stage"
                        ],
                        "valid_stages": list(STAGES),
                    },
                )
            summary = self.materialize_views()

        return {
            "ok": True,
            "status": "complete" if complete and not stopped else "stopped",
            "stopped": stopped,
            "processed_shards": processed_shards,
            "shard_count": manifest.shard_count,
            "computed": computed,
            "reused": reused,
            "checkpoint": self.checkpoint_summary(),
            "plan": plan,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Public per-stage execution and stage queries
    # ------------------------------------------------------------------
    def _validate_stage_name(self, stage: str) -> None:
        if stage not in STAGES:
            raise EngineError(
                "E_INVALID_STAGE",
                "unknown stage",
                {"stage": stage, "valid_stages": list(STAGES), "available_stages": []},
            )

    def get_stage_artifact(self, shard_index: int, stage: str) -> dict[str, Any]:
        self._validate_stage_name(stage)
        manifest = self.load_manifest()
        manifest.entry(shard_index)
        state = self.store(manifest).scan()
        record = state.latest(shard_index, stage)
        if record is None:
            raise EngineError(
                "E_INVALID_STAGE",
                "stage artifact is missing",
                {
                    "shard_index": shard_index,
                    "stage": stage,
                    "valid_stages": list(STAGES),
                    "available_stages": state.available_stages(shard_index),
                },
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "shard_index": shard_index,
            "stage": stage,
            "sequence": record["sequence"],
            "shard_sequence": record["shard_sequence"],
            "input_fingerprint": record["input_fingerprint"],
            "config_fingerprint": record["config_fingerprint"],
            "stage_config_fingerprint": record["stage_config_fingerprint"],
            "output_digest": record["output_digest"],
            "upstream_output_digests": record["upstream_output_digests"],
            "output": record["output"],
        }

    def run_stage(self, shard_index: int, stage: str) -> dict[str, Any]:
        """Execute exactly one stage after enforcing the entire prerequisite chain.

        Prerequisites must not merely exist; they must be current under the
        loaded configuration and inputs.  This prevents an explicit stage jump
        from publishing a downstream artifact on a stale upstream tree.
        """

        self._validate_stage_name(stage)
        config = self.load_config()
        manifest = self.load_manifest()
        manifest.entry(shard_index)
        store = self.store(manifest)
        state = store.scan()
        plan = self.plan(config=config, manifest=manifest, state=state)
        shard_plan = plan["shards"][shard_index]
        available = state.available_stages(shard_index)

        stage_position = STAGES.index(stage)
        for prerequisite in STAGES[:stage_position]:
            prerequisite_record = state.latest(shard_index, prerequisite)
            if prerequisite_record is None:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "a prerequisite stage artifact is missing",
                    {
                        "reason": "missing_prerequisite",
                        "shard_index": shard_index,
                        "stage": stage,
                        "missing_stage": prerequisite,
                        "valid_stages": list(STAGES),
                        "available_stages": available,
                    },
                )
            stage_report = shard_plan["stages"][prerequisite]
            if stage_report["status"] != "reusable":
                raise EngineError(
                    "E_INVALID_STAGE",
                    "a prerequisite stage artifact is stale",
                    {
                        "reason": "stale_prerequisite",
                        "shard_index": shard_index,
                        "stage": stage,
                        "stale_stage": prerequisite,
                        "prerequisite_invalidation": stage_report,
                        "valid_stages": list(STAGES),
                        "available_stages": available,
                    },
                )

        outputs: dict[str, dict[str, Any]] = {}
        output_digests: dict[str, str] = {}
        for prerequisite in STAGES[:stage_position]:
            record = state.latest(shard_index, prerequisite)
            assert record is not None
            outputs[prerequisite] = record["output"]
            output_digests[prerequisite] = record["output_digest"]

        entry = manifest.entry(shard_index)
        components, config_components, upstream_digests, input_fp, config_fp, _extra = (
            self._expected_stage_output(
                stage=stage,
                entry=entry,
                manifest=manifest,
                config=config,
                outputs=output_digests,
                state=state,
            )
        )
        existing = state.latest(shard_index, stage)
        if existing is not None and existing["input_fingerprint"] == input_fp:
            return {
                "ok": True,
                "status": "reused",
                "shard_index": shard_index,
                "stage": stage,
                "sequence": existing["sequence"],
                "shard_sequence": existing["shard_sequence"],
                "input_fingerprint": existing["input_fingerprint"],
                "output_digest": existing["output_digest"],
            }

        output = self._execute_stage(
            stage=stage,
            entry=entry,
            manifest=manifest,
            config=config,
            outputs=outputs,
        )
        expected_sequence = state.last_shard_sequence.get(shard_index, 0) + 1
        published = store.publish(
            shard_index=shard_index,
            shard_name=entry.file,
            stage=stage,
            input_fingerprint=input_fp,
            input_components=components,
            config_fingerprint=config.fingerprint(),
            stage_config_fingerprint=config_fp,
            stage_config_components=config_components,
            upstream_output_digests=upstream_digests,
            output=output,
            expected_shard_sequence=expected_sequence,
        )
        return {
            "ok": True,
            "status": "computed",
            "shard_index": shard_index,
            "stage": stage,
            "sequence": published["sequence"],
            "shard_sequence": published["shard_sequence"],
            "input_fingerprint": published["input_fingerprint"],
            "output_digest": published["output_digest"],
        }

    # ------------------------------------------------------------------
    # Deterministic public views and queries
    # ------------------------------------------------------------------
    def _views_state(self) -> tuple[AnalysisConfig, Manifest, RecoveredState, dict[str, Any]]:
        config = self.load_config()
        manifest = self.load_manifest()
        state = self.store(manifest).scan()
        plan = self.plan(config=config, manifest=manifest, state=state)
        return config, manifest, state, plan

    def _build_view_documents(
        self,
        *,
        config: AnalysisConfig,
        manifest: Manifest,
        state: RecoveredState,
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        if not plan["all_reusable"]:
            raise EngineError(
                "E_INVALID_STAGE",
                "checkpoint chain is incomplete or stale",
                {
                    "global_earliest_invalid_stage": plan[
                        "global_earliest_invalid_stage"
                    ],
                    "valid_stages": list(STAGES),
                    "available_stages": {
                        str(entry.index): state.available_stages(entry.index)
                        for entry in manifest.shards
                    },
                },
            )

        trees: list[dict[str, Any]] = []
        diagnostics: list[dict[str, Any]] = []
        aggregates: list[dict[str, Any]] = []
        error_counts: dict[str, int] = {}
        violation_counts: dict[str, int] = {}
        total_sentences = 0
        total_tokens = 0
        tree_count = 0
        low_confidence_steps = 0

        for entry in manifest.shards:
            validation_record = state.latest(entry.index, VALIDATION)
            aggregate_record = state.latest(entry.index, AGGREGATION)
            if validation_record is None or aggregate_record is None:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "a required stage artifact is missing",
                    {
                        "shard_index": entry.index,
                        "missing_stage": (
                            VALIDATION if validation_record is None else AGGREGATION
                        ),
                        "valid_stages": list(STAGES),
                        "available_stages": state.available_stages(entry.index),
                    },
                )
            validation_output = validation_record["output"]
            aggregate_output = aggregate_record["output"]
            trees.extend(validation_output.get("sentences", []))
            diagnostics.extend(validation_output.get("diagnostics", []))
            aggregates.append(
                {
                    **aggregate_output,
                    "pass_output_digest": aggregate_record["output_digest"],
                }
            )
            total_sentences += int(aggregate_output["sentence_count"])
            total_tokens += int(aggregate_output["token_count"])
            tree_count += int(aggregate_output["tree_count"])
            low_confidence_steps += int(aggregate_output["low_confidence_step_count"])
            for code, count in aggregate_output.get("error_counts", {}).items():
                error_counts[code] = error_counts.get(code, 0) + int(count)
            for violation, count in aggregate_output.get(
                "constraint_violation_counts", {}
            ).items():
                violation_counts[violation] = (
                    violation_counts.get(violation, 0) + int(count)
                )

        tree_digest = sha256_obj(trees)
        diagnostic_digest = sha256_obj(diagnostics)
        summary: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "config_fingerprint": config.fingerprint(),
            "stage_config_fingerprints": config.stage_fingerprints(),
            "manifest_fingerprint": manifest.manifest_fingerprint(),
            "shard_count": manifest.shard_count,
            "sentence_count": total_sentences,
            "token_count": total_tokens,
            "tree_count": tree_count,
            "error_count": sum(error_counts.values()),
            "error_counts": dict(sorted(error_counts.items())),
            "constraint_violation_count": sum(violation_counts.values()),
            "constraint_violation_counts": dict(sorted(violation_counts.items())),
            "low_confidence_step_count": low_confidence_steps,
            "tree_digest": tree_digest,
            "diagnostic_digest": diagnostic_digest,
            "shards": aggregates,
        }
        summary["summary_digest"] = sha256_obj(summary)

        return {
            "trees": {
                "schema_version": SCHEMA_VERSION,
                "tree_digest": tree_digest,
                "sentences": trees,
            },
            "diagnostics": {
                "schema_version": SCHEMA_VERSION,
                "diagnostic_digest": diagnostic_digest,
                "diagnostics": diagnostics,
            },
            "summary": summary,
        }

    def materialize_views(self) -> dict[str, Any]:
        config, manifest, state, plan = self._views_state()
        documents = self._build_view_documents(
            config=config,
            manifest=manifest,
            state=state,
            plan=plan,
        )
        atomic_write_json(self.views_dir / "trees.json", documents["trees"])
        atomic_write_json(
            self.views_dir / "diagnostics.json", documents["diagnostics"]
        )
        atomic_write_json(self.views_dir / "summary.json", documents["summary"])
        return documents["summary"]

    def get_trees(self, shard_index: int | None = None) -> dict[str, Any]:
        config, manifest, state, plan = self._views_state()
        if shard_index is not None:
            manifest.entry(shard_index)
            record = state.latest(shard_index, VALIDATION)
            if record is None:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "validation artifact is missing for shard",
                    {
                        "shard_index": shard_index,
                        "missing_stage": VALIDATION,
                        "valid_stages": list(STAGES),
                        "available_stages": state.available_stages(shard_index),
                    },
                )
            shard_report = plan["shards"][shard_index]
            if shard_report["stages"][VALIDATION]["status"] != "reusable":
                raise EngineError(
                    "E_INVALID_STAGE",
                    "validation artifact is stale for shard",
                    {
                        "shard_index": shard_index,
                        "stage": VALIDATION,
                        "earliest_invalid_stage": shard_report[
                            "earliest_invalid_stage"
                        ],
                        "validation_status": shard_report["stages"][VALIDATION],
                        "valid_stages": list(STAGES),
                        "available_stages": state.available_stages(shard_index),
                    },
                )
            sentences = record["output"].get("sentences", [])
            return {
                "schema_version": SCHEMA_VERSION,
                "shard_index": shard_index,
                "tree_digest": sha256_obj(sentences),
                "sentences": sentences,
            }
        documents = self._build_view_documents(
            config=config, manifest=manifest, state=state, plan=plan
        )
        return documents["trees"]

    def get_diagnostics(self, shard_index: int | None = None) -> dict[str, Any]:
        config, manifest, state, plan = self._views_state()
        if shard_index is not None:
            manifest.entry(shard_index)
            record = state.latest(shard_index, VALIDATION)
            if record is None:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "validation artifact is missing for shard",
                    {
                        "shard_index": shard_index,
                        "missing_stage": VALIDATION,
                        "valid_stages": list(STAGES),
                        "available_stages": state.available_stages(shard_index),
                    },
                )
            shard_report = plan["shards"][shard_index]
            if shard_report["stages"][VALIDATION]["status"] != "reusable":
                raise EngineError(
                    "E_INVALID_STAGE",
                    "validation artifact is stale for shard",
                    {
                        "shard_index": shard_index,
                        "stage": VALIDATION,
                        "earliest_invalid_stage": shard_report[
                            "earliest_invalid_stage"
                        ],
                        "validation_status": shard_report["stages"][VALIDATION],
                        "valid_stages": list(STAGES),
                        "available_stages": state.available_stages(shard_index),
                    },
                )
            diagnostics = record["output"].get("diagnostics", [])
            return {
                "schema_version": SCHEMA_VERSION,
                "shard_index": shard_index,
                "diagnostic_digest": sha256_obj(diagnostics),
                "diagnostics": diagnostics,
            }
        documents = self._build_view_documents(
            config=config, manifest=manifest, state=state, plan=plan
        )
        return documents["diagnostics"]

    def get_summary(self) -> dict[str, Any]:
        config, manifest, state, plan = self._views_state()
        documents = self._build_view_documents(
            config=config, manifest=manifest, state=state, plan=plan
        )
        return documents["summary"]

    def get_fingerprints(self) -> dict[str, Any]:
        config = self.load_config()
        manifest = self.load_manifest()
        state = self.store(manifest).scan()
        plan = self.plan(config=config, manifest=manifest, state=state)
        shards = []
        for entry in manifest.shards:
            stages: dict[str, Any] = {}
            for stage in STAGES:
                record = state.latest(entry.index, stage)
                stages[stage] = (
                    None
                    if record is None
                    else {
                        "sequence": record["sequence"],
                        "shard_sequence": record["shard_sequence"],
                        "input_fingerprint": record["input_fingerprint"],
                        "config_fingerprint": record["config_fingerprint"],
                        "stage_config_fingerprint": record[
                            "stage_config_fingerprint"
                        ],
                        "output_digest": record["output_digest"],
                    }
                )
            shards.append(
                {
                    "shard_index": entry.index,
                    "shard_name": entry.file,
                    "content_sha256": manifest.shard_content_digest(entry.index),
                    "stages": stages,
                }
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "config_fingerprint": config.fingerprint(),
            "stage_config_fingerprints": config.stage_fingerprints(),
            "manifest_fingerprint": manifest.manifest_fingerprint(),
            "shards": shards,
            "invalidation": plan,
        }

    def get_invalidations(self) -> dict[str, Any]:
        return self.plan()

    # ------------------------------------------------------------------
    # Explicit incremental mutation entrypoints
    # ------------------------------------------------------------------
    def replace_shard(
        self, shard_index: int, text: str, *, build: bool = True
    ) -> dict[str, Any]:
        manifest = self.load_manifest()
        entry = manifest.entry(shard_index)
        updated = manifest.replace_shard_text(shard_index, text)
        plan = self.plan(manifest=manifest)
        result: dict[str, Any] = {
            "ok": True,
            "replaced_shard": entry.index,
            "manifest_entry": updated,
            "plan": plan,
        }
        if build:
            result["build"] = self.build()
        return result

    def update_config(
        self,
        config: AnalysisConfig | dict[str, Any],
        *,
        build: bool = True,
    ) -> dict[str, Any]:
        parsed = (
            config
            if isinstance(config, AnalysisConfig)
            else AnalysisConfig.from_mapping(config)
        )
        atomic_write_bytes(self.config_path, parsed.to_yaml_bytes())
        plan = self.plan(config=parsed)
        result: dict[str, Any] = {
            "ok": True,
            "config_fingerprint": parsed.fingerprint(),
            "plan": plan,
        }
        if build:
            result["build"] = self.build()
        return result

    # ------------------------------------------------------------------
    # Full-build equivalence verification
    # ------------------------------------------------------------------
    def _copy_reference_inputs(self, target: Path) -> None:
        manifest = self.load_manifest()
        target_manifest = target / self.manifest_relative_path
        target_manifest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.manifest_path, target_manifest)
        for entry in manifest.shards:
            source = self.root / entry.file
            destination = target / entry.file
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        target_config = target / self.config_relative_path
        target_config.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.config_path, target_config)

    def _first_diff(self, current_doc: Any, reference_doc: Any, artifact: str) -> dict[str, Any]:
        current_sentences = current_doc.get("sentences", []) if isinstance(current_doc, dict) else []
        reference_sentences = (
            reference_doc.get("sentences", []) if isinstance(reference_doc, dict) else []
        )
        limit = max(len(current_sentences), len(reference_sentences))
        for sentence_position in range(limit):
            if sentence_position >= len(current_sentences):
                entry = reference_sentences[sentence_position]
                return {
                    "artifact": artifact,
                    "shard_index": entry.get("shard_index"),
                    "sentence_id": entry.get("sentence_id"),
                    "token_index": 0,
                    "field": "missing_sentence_in_incremental",
                }
            if sentence_position >= len(reference_sentences):
                entry = current_sentences[sentence_position]
                return {
                    "artifact": artifact,
                    "shard_index": entry.get("shard_index"),
                    "sentence_id": entry.get("sentence_id"),
                    "token_index": 0,
                    "field": "extra_sentence_in_incremental",
                }
            current_entry = current_sentences[sentence_position]
            reference_entry = reference_sentences[sentence_position]
            if canonical_bytes(current_entry) == canonical_bytes(reference_entry):
                continue
            current_heads = current_entry.get("heads", [])
            reference_heads = reference_entry.get("heads", [])
            current_labels = current_entry.get("labels", [])
            reference_labels = reference_entry.get("labels", [])
            current_distances = current_entry.get("distances", [])
            reference_distances = reference_entry.get("distances", [])
            token_limit = max(
                len(current_heads),
                len(reference_heads),
                len(current_labels),
                len(reference_labels),
                len(current_distances),
                len(reference_distances),
            )
            first_token = 0
            field = "sentence_metadata"
            for token_index in range(token_limit):
                if (
                    token_index >= len(current_heads)
                    or token_index >= len(reference_heads)
                    or current_heads[token_index] != reference_heads[token_index]
                ):
                    first_token = token_index
                    field = "head_index"
                    break
                if (
                    token_index >= len(current_labels)
                    or token_index >= len(reference_labels)
                    or current_labels[token_index] != reference_labels[token_index]
                ):
                    first_token = token_index
                    field = "dependency_label"
                    break
                if (
                    token_index >= len(current_distances)
                    or token_index >= len(reference_distances)
                    or current_distances[token_index]
                    != reference_distances[token_index]
                ):
                    first_token = token_index
                    field = "dependency_distance"
                    break
            return {
                "artifact": artifact,
                "shard_index": current_entry.get(
                    "shard_index", reference_entry.get("shard_index")
                ),
                "sentence_id": current_entry.get(
                    "sentence_id", reference_entry.get("sentence_id")
                ),
                "token_index": first_token,
                "field": field,
            }
        if isinstance(current_doc, dict) and "diagnostics" in current_doc:
            current_diagnostics = current_doc.get("diagnostics", [])
            reference_diagnostics = (
                reference_doc.get("diagnostics", []) if isinstance(reference_doc, dict) else []
            )
            for index in range(max(len(current_diagnostics), len(reference_diagnostics))):
                if index >= len(current_diagnostics):
                    shard = reference_diagnostics[index].get("shard_index")
                    sentence = reference_diagnostics[index].get("sentence_id")
                    token = 0
                elif index >= len(reference_diagnostics):
                    shard = current_diagnostics[index].get("shard_index")
                    sentence = current_diagnostics[index].get("sentence_id")
                    token = 0
                elif canonical_bytes(current_diagnostics[index]) == canonical_bytes(
                    reference_diagnostics[index]
                ):
                    continue
                else:
                    shard = current_diagnostics[index].get("shard_index")
                    sentence = current_diagnostics[index].get("sentence_id")
                    violations = current_diagnostics[index].get("violations", [])
                    token = (
                        int(violations[0].get("token_index", 0))
                        if violations
                        else 0
                    )
                return {
                    "artifact": artifact,
                    "shard_index": shard,
                    "sentence_id": sentence,
                    "token_index": token,
                    "field": "diagnostic",
                }
        return {
            "artifact": artifact,
            "shard_index": 0,
            "sentence_id": None,
            "token_index": 0,
            "field": "document",
        }

    def compare_views(self, reference_views_dir: Path) -> dict[str, Any]:
        artifacts = ("trees", "diagnostics", "summary")
        compared: dict[str, str] = {}
        first_diff: dict[str, Any] | None = None
        for artifact in artifacts:
            current_path = self.views_dir / f"{artifact}.json"
            reference_path = reference_views_dir / f"{artifact}.json"
            current_bytes = current_path.read_bytes()
            reference_bytes = reference_path.read_bytes()
            compared[artifact] = {
                "current": sha256_bytes(current_bytes),
                "reference": sha256_bytes(reference_bytes),
            }
            if current_bytes == reference_bytes:
                continue
            if first_diff is None:
                first_diff = self._first_diff(
                    read_json(current_path),
                    read_json(reference_path),
                    artifact,
                )
        if first_diff is not None:
            raise EngineError(
                E_INCREMENTAL_MISMATCH,
                "incremental output differs from a from-scratch full build",
                {
                    "first_diff": first_diff,
                    "shard_index": first_diff.get("shard_index"),
                    "token_index": first_diff.get("token_index"),
                    "current_views": str(self.views_dir),
                    "reference_views": str(reference_views_dir),
                },
            )
        return {
            "ok": True,
            "artifacts": list(artifacts),
            "digests": compared,
            "current_views": str(self.views_dir),
            "reference_views": str(reference_views_dir),
        }

    def verify_against_full(self) -> dict[str, Any]:
        build_result = self.build()
        if build_result["status"] != "complete":
            raise EngineError(
                "E_INVALID_STAGE",
                "cannot verify an incomplete incremental build",
                {
                    "status": build_result["status"],
                    "valid_stages": list(STAGES),
                },
            )
        self.materialize_views()
        runtime_root = self.root / "runtime"
        runtime_root.mkdir(parents=True, exist_ok=True)
        temp_root = Path(
            tempfile.mkdtemp(prefix=".verify-full-", dir=str(runtime_root))
        )
        try:
            self._copy_reference_inputs(temp_root)
            full_engine = Engine(
                temp_root,
                state_dir=".verify/state",
                views_dir=".verify/views",
                config_path=self.config_relative_path,
                manifest_path=self.manifest_relative_path,
            )
            full_result = full_engine.build()
            if full_result["status"] != "complete":
                raise EngineError(
                    "E_INVALID_STAGE",
                    "reference full build did not complete",
                    {"valid_stages": list(STAGES)},
                )
            comparison = self.compare_views(full_engine.views_dir)
            current_summary = self.get_summary()
            current_views = str(self.views_dir.relative_to(self.root))
            return {
                **comparison,
                "current_views": current_views,
                "reference_views": "<from-scratch-full-build>",
                "full_match": True,
                "summary_digest": current_summary["summary_digest"],
                "tree_digest": current_summary["tree_digest"],
                "diagnostic_digest": current_summary["diagnostic_digest"],
            }
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


__all__ = ["Engine"]
