"""Four-stage pipeline orchestration with stop/resume and incremental rebuilds."""

from __future__ import annotations

import os
from dataclasses import dataclass

from PIL import Image

from .checkpoint import CheckpointStore
from .config import STAGES, WorkbenchConfig
from .detection import detect_shard
from .diagnosis import aggregate
from .errors import IncrementalMismatch
from .hashing import digest_json
from .recognition import (
    build_template_library,
    recognize_shard,
    strategy_library_characters,
    truth_frequency_order,
)
from .strategy import decide_shard


@dataclass
class RunStats:
    processed_shards: int = 0
    processed_stages: int = 0
    reused_artifacts: int = 0
    rebuilt_artifacts: int = 0
    invalidations: list = None
    stopped: bool = False

    def to_dict(self) -> dict:
        return {
            "processed_shards": self.processed_shards,
            "processed_stages": self.processed_stages,
            "reused_artifacts": self.reused_artifacts,
            "rebuilt_artifacts": self.rebuilt_artifacts,
            "invalidations": self.invalidations or [],
            "stopped": self.stopped,
        }


class Pipeline:
    def __init__(self, workspace: str, sample_dir: str, config: WorkbenchConfig | None = None):
        self.workspace = os.path.abspath(workspace)
        self.sample_dir = os.path.abspath(sample_dir)
        self.store = CheckpointStore(self.workspace)
        self.config = config or self._load_or_default_config()
        self.stop_flag = os.path.join(self.workspace, "STOP")

    def _load_or_default_config(self) -> WorkbenchConfig:
        if os.path.exists(self.store.config_path):
            return self.store.load_config()
        return WorkbenchConfig()

    # ------------------------------------------------------------- import
    def import_sample(self, manifest: dict) -> None:
        self.store.atomic_write_json(self.store.manifest_path, manifest)
        self.store.atomic_write_json(self.store.config_path, self.config.to_dict())
        self.store.compute_input_fingerprints(self.sample_dir)

    def request_stop(self) -> None:
        with open(self.stop_flag, "w", encoding="utf-8") as handle:
            handle.write("stop\n")

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_flag)

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_flag):
            os.remove(self.stop_flag)

    # ------------------------------------------------------------- images
    def _image(self, shard: dict) -> Image.Image:
        return Image.open(os.path.join(self.sample_dir, shard["path"]))

    def _library(self, manifest: dict, strategy_id: str):
        ordered = truth_frequency_order(manifest["shards"])
        return build_template_library(strategy_library_characters(ordered, strategy_id), self.config)

    # ------------------------------------------------------------- stages
    def _run_detect(self, shard: dict, strategy_id: str) -> dict:
        return detect_shard(self._image(shard), self.config, shard["image_id"], strategy_id)

    def _run_decision(self, shard: dict, detection: dict, template_count: int, cell_count: int) -> dict:
        return decide_shard(
            shard["group"],
            shard["char_count"],
            detection["component_count"],
            detection["candidate_count"],
            template_count,
            cell_count,
            self.config,
        )

    def _run_recognize(self, shard: dict, detection: dict, decision: dict, manifest: dict) -> tuple[dict, dict]:
        library = self._library(manifest, decision["recognize_strategy"])
        result = recognize_shard(self._image(shard), detection, library, self.config)
        return result, {
            "characters": library["characters"],
            "size": library["size"],
            "grid_digests": library["grid_digests"],
            "recognize_strategy": decision["recognize_strategy"],
        }

    # ------------------------------------------------------------- run
    def run(self, stats: RunStats | None = None) -> RunStats:
        stats = stats or RunStats()
        stats.invalidations = stats.invalidations or []
        manifest = self.store.load_manifest()
        current_inputs = self.store.compute_input_fingerprints(self.sample_dir)
        config_version = self.config.version()
        shard_by_id = {shard["image_id"]: shard for shard in manifest["shards"]}

        for shard in manifest["shards"]:
            # The stop flag is only consulted between a shard's indivisible
            # stages, so a requested stop always finishes the current unit.
            self._run_shard(shard, current_inputs, config_version, manifest, stats)
            if self.stop_requested():
                stats.stopped = True
                return stats

        if not self.stop_requested():
            self._run_diagnosis(manifest, stats)
        return stats

    def _run_shard(self, shard: dict, current_inputs: dict, config_version: str, manifest: dict, stats: RunStats) -> None:
        image_id = shard["image_id"]
        input_fingerprint = current_inputs[image_id]
        with self.store.shard_lock(image_id):
            journal = self.store.read_journal(image_id)
            is_new_shard = not journal
            first_invalid = self._first_invalid_stage(journal, input_fingerprint, config_version)
            if first_invalid is not None:
                removed = self.store._invalidate_locked(image_id, first_invalid)
                reasons = ["new_shard"] if is_new_shard else self._invalidation_reasons(
                    journal, input_fingerprint, config_version
                )
                if not is_new_shard:
                    stats.invalidations.append(
                        {
                            "image_id": image_id,
                            "from_stage": first_invalid,
                            "reasons": reasons,
                            "removed": removed,
                        }
                    )
                journal = self.store.read_journal(image_id)

            done = {record["stage"] for record in journal}
            sequence = len(journal)

            if "detect" not in done:
                baseline = self._run_detect(shard, "detect_fast")
                sequence += 1
                fingerprint = self.store.stage_fingerprint("detect", input_fingerprint, config_version, None, baseline)
                self.store._publish_locked(
                    image_id,
                    "detect",
                    sequence,
                    input_fingerprint,
                    config_version,
                    None,
                    fingerprint,
                    baseline,
                )
                stats.rebuilt_artifacts += 1
                stats.processed_stages += 1
                journal = self.store.read_journal(image_id)
            else:
                stats.reused_artifacts += 1

            if "decision" not in done:
                baseline = self.store.load_artifact(image_id, "detect")
                full_library = self._library(manifest, "recog_deep")
                cell_count = self._estimate_cell_count(shard)
                decision = self._run_decision(shard, baseline, full_library["size"], cell_count)
                if "error" in decision:
                    raise IncrementalMismatch(
                        "no strategy fits the configured budgets",
                        {"image_id": image_id, "decision": decision},
                    )
                refined = self._run_detect(shard, decision["detect_strategy"])
                decision_payload = {
                    "image_id": image_id,
                    "decision": decision,
                    "refined_detection": refined,
                }
                sequence += 1
                upstream = self._journal_fingerprint(journal, "detect")
                fingerprint = self.store.stage_fingerprint(
                    "decision", input_fingerprint, config_version, upstream, decision_payload
                )
                self.store._publish_locked(
                    image_id,
                    "decision",
                    sequence,
                    input_fingerprint,
                    config_version,
                    upstream,
                    fingerprint,
                    decision_payload,
                )
                stats.rebuilt_artifacts += 1
                stats.processed_stages += 1
            else:
                stats.reused_artifacts += 1

            if "recognize" not in done:
                decision_payload = self.store.load_artifact(image_id, "decision")
                detection = decision_payload["refined_detection"]
                recognition, library_meta = self._run_recognize(
                    shard, detection, decision_payload["decision"], manifest
                )
                recognize_payload = {
                    "image_id": image_id,
                    "recognition": recognition,
                    "library": library_meta,
                }
                sequence += 1
                upstream = self._journal_fingerprint(
                    self.store.read_journal(image_id), "decision"
                )
                fingerprint = self.store.stage_fingerprint(
                    "recognize", input_fingerprint, config_version, upstream, recognize_payload
                )
                self.store._publish_locked(
                    image_id,
                    "recognize",
                    sequence,
                    input_fingerprint,
                    config_version,
                    upstream,
                    fingerprint,
                    recognize_payload,
                )
                stats.rebuilt_artifacts += 1
                stats.processed_stages += 1
            else:
                stats.reused_artifacts += 1
            stats.processed_shards += 1

    def _estimate_cell_count(self, shard: dict) -> int:
        return shard["char_count"]

    @staticmethod
    def _journal_fingerprint(journal: list[dict], stage: str) -> str | None:
        for record in journal:
            if record["stage"] == stage:
                return record["stage_fingerprint"]
        return None

    def _first_invalid_stage(self, journal: list[dict], input_fingerprint: str, config_version: str) -> str | None:
        if not journal:
            return "detect"
        by_stage = {record["stage"]: record for record in journal}
        previous_fingerprint = None
        for stage in STAGES[:-1]:
            record = by_stage.get(stage)
            if record is None:
                return stage
            if record["input_fingerprint"] != input_fingerprint:
                return stage
            if record["config_version"] != config_version:
                return stage
            if record.get("upstream_fingerprint") != previous_fingerprint:
                return stage
            previous_fingerprint = record["stage_fingerprint"]
        return None

    @staticmethod
    def _invalidation_reasons(journal: list[dict], input_fingerprint: str, config_version: str) -> list[str]:
        if not journal:
            return ["new_shard"]
        by_stage = {record["stage"]: record for record in journal}
        reasons: list[str] = []
        first = journal[0]
        if first["input_fingerprint"] != input_fingerprint:
            reasons.append("input_pixel_or_truth_changed")
        if first["config_version"] != config_version:
            reasons.append("config_version_changed")
        if not reasons:
            previous_fingerprint = None
            for stage in STAGES[:-1]:
                record = by_stage.get(stage)
                if record is None or record.get("upstream_fingerprint") != previous_fingerprint:
                    reasons.append(f"upstream_fingerprint_changed_at_{stage}")
                    break
                previous_fingerprint = record["stage_fingerprint"]
        return reasons or ["downstream_rebuild"]

    # ------------------------------------------------------------- diagnosis
    def _run_diagnosis(self, manifest: dict, stats: RunStats) -> None:
        detections, decisions, recognitions = {}, {}, {}
        for shard in manifest["shards"]:
            image_id = shard["image_id"]
            decision_payload = self.store.load_artifact(image_id, "decision")
            recognize_payload = self.store.load_artifact(image_id, "recognize")
            detections[image_id] = decision_payload["refined_detection"]
            decisions[image_id] = decision_payload["decision"]
            recognitions[image_id] = recognize_payload["recognition"]
        report = aggregate(manifest, detections, decisions, recognitions, self.config)
        report_path = os.path.join(self.store.diagnostics_dir, "report.json")
        self.store.atomic_write_json(report_path, report)
        stats.processed_stages += 1

    def load_report(self) -> dict:
        return self.store.read_json(os.path.join(self.store.diagnostics_dir, "report.json"))

    # ------------------------------------------------------------- verification
    def verify_incremental_equals_full(self, reference_report: dict) -> None:
        current = self.load_report()
        current_digest = current.pop("summary_digest", None)
        reference_digest = reference_report.pop("summary_digest", None)
        if digest_json(current) != digest_json(reference_report):
            raise IncrementalMismatch(
                "incremental report differs from a clean full rebuild",
                {
                    "incremental_digest": current_digest,
                    "full_digest": reference_digest,
                    "diff": self._diff_summary(current, reference_report),
                },
            )

    @staticmethod
    def _diff_summary(current: dict, reference: dict) -> dict:
        keys = sorted(set(current) | set(reference))
        return {
            key: {"incremental": current.get(key), "full": reference.get(key)}
            for key in keys
            if current.get(key) != reference.get(key)
        }
