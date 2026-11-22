"""Orchestration: fingerprints, incremental invalidation, full-run parity and recovery."""
from __future__ import annotations

import os
import shutil

from .checkpoints import CheckpointStore
from .codec import canonical_bytes, digest, read_json, write_json
from .config import PipelineConfig, STAGE_ORDER
from .detect import region_binding_index, run_detection
from .errors import IncrementalMismatchError
from .evidence import build_evidence
from .normalize import run_normalize
from .parse import run_parse
from .resolve import run_resolve


class Pipeline:
    """Stateful, resumable, byte-deterministic four-stage contract-chain pipeline."""

    def __init__(self, workspace: str, config: PipelineConfig | None = None):
        self.workspace = os.path.abspath(workspace)
        os.makedirs(self.workspace, exist_ok=True)
        self.store = CheckpointStore(self.workspace)
        self.config_path = os.path.join(self.workspace, "config.json")
        self.sample_path = os.path.join(self.workspace, "sample.json")
        if config is not None:
            self.config = config
            write_json(self.config_path, config.to_dict())
        elif os.path.exists(self.config_path):
            self.config = PipelineConfig.from_dict(read_json(self.config_path))
        else:
            self.config = PipelineConfig()
            write_json(self.config_path, self.config.to_dict())

    # ---------- input management ----------
    def load_sample(self, sample: dict | None = None, path: str | None = None) -> dict:
        if sample is None:
            sample = read_json(path or self.sample_path)
        write_json(self.sample_path, sample)
        return sample

    def update_config(self, config: PipelineConfig) -> None:
        self.config = config
        write_json(self.config_path, config.to_dict())

    # ---------- fingerprinting ----------
    def _fingerprints(self, sample: dict) -> dict:
        sample_digest = digest(sample)
        page_digest = digest(sample["pages"])
        previous = "ROOT"
        fingerprints = {}
        builders = {
            "detect": [page_digest, self.config.stage_digest("detect")],
            "parse": [sample_digest, self.config.stage_digest("parse")],
            "normalize": [sample_digest, self.config.stage_digest("normalize")],
            "resolve": [
                sample_digest,
                self.config.stage_digest("resolve"),
                self.config.stage_digest("normalize"),
            ],
        }
        for stage in STAGE_ORDER:
            material = {"inputs": builders[stage], "upstream": previous}
            fingerprints[stage] = digest(material)
            previous = fingerprints[stage]
        return fingerprints

    def _execute_stage(self, stage, sample, context):
        stop_check = self.store.check_stop
        if stage == "detect":
            return run_detection(sample, self.config.detect, sample["seed"])
        if stage == "parse":
            return run_parse(sample, context["bindings"], self.config.parse, stop_check)
        if stage == "normalize":
            return run_normalize(context["parse"], self.config.normalize, stop_check)
        if stage == "resolve":
            return run_resolve(context["parse"], context["normalize"], self.config.resolve)
        raise KeyError(stage)

    def _input_digest(self, stage, sample, context):
        if stage == "detect":
            return digest(sample["pages"])
        if stage == "parse":
            return digest({"sample": sample, "detection": context["detect"]})
        if stage == "normalize":
            return digest({"parse": context["parse"]})
        return digest({"parse": context["parse"], "normalize": context["normalize"]})

    # ---------- run modes ----------
    def run(self, sample: dict | None = None, stop_after: str | None = None) -> dict:
        """Run all missing stages; reused stages keep their original sequence numbers."""
        sample = sample or read_json(self.sample_path)
        write_json(self.sample_path, sample)
        fingerprints = self._fingerprints(sample)
        context = {}
        stopped = None
        for stage in STAGE_ORDER:
            if self.store.has_stage(stage):
                record = read_json(self.store.stage_record_path(stage))
                if record["fingerprint"] != fingerprints[stage]:
                    self.store.truncate_from(stage)
                else:
                    context[stage] = self.store.get_stage(stage)
                    if stage == "detect":
                        context["bindings"] = region_binding_index(sample, context["detect"])
                    continue
            product = self._execute_stage(stage, sample, context)
            record = self.store.publish(
                stage,
                product,
                self._input_digest(stage, sample, context),
                self.config.stage_digest(stage),
                fingerprints[stage],
            )
            context[stage] = product
            if stage == "detect":
                context["bindings"] = region_binding_index(sample, product)
            if stop_after == stage or self.store.stop_requested():
                stopped = stage
                break
        evidence = None
        if self.store.has_stage("resolve") and stopped is None:
            evidence = build_evidence(context["parse"], context["normalize"], context["resolve"])
            write_json(os.path.join(self.workspace, "evidence_manifest.json"), evidence)
        return {
            "completed_stages": [stage for stage in STAGE_ORDER if self.store.has_stage(stage)],
            "stopped_at": stopped,
            "stop_requested": self.store.stop_requested(),
            "evidence": evidence,
            "fingerprints": fingerprints,
        }

    def resume(self) -> dict:
        self.store.clear_stop()
        return self.run()

    def run_full(self, sample: dict | None = None) -> dict:
        """Force a clean from-scratch run inside this workspace."""
        for stage in reversed(STAGE_ORDER):
            if self.store.has_stage(stage):
                self.store.truncate_from(stage)
        self.store.clear_stop()
        return self.run(sample)

    def run_incremental(self, new_sample: dict | None = None,
                        new_config: PipelineConfig | None = None) -> dict:
        """Reuse unaffected upstream stages, rebuild downstream, then verify byte parity."""
        baseline_sample = read_json(self.sample_path)
        baseline_fingerprints = self._fingerprints(baseline_sample)
        if new_config is not None:
            self.update_config(new_config)
        target_sample = new_sample or baseline_sample
        target_fingerprints = self._fingerprints(target_sample)

        earliest = None
        for stage in STAGE_ORDER:
            if baseline_fingerprints[stage] != target_fingerprints[stage]:
                earliest = stage
                break
        if earliest is not None:
            self.store.truncate_from(earliest)
        write_json(self.sample_path, target_sample)
        incremental = self.run(target_sample)

        parity = self.verify_parity(target_sample)
        return {
            "earliest_changed_stage": earliest,
            "incremental_run": incremental,
            "parity": parity,
        }

    def verify_parity(self, sample: dict) -> dict:
        """Compare incremental products against a from-scratch run byte-for-byte."""
        current = {
            stage: canonical_bytes(self.store.get_stage(stage))
            for stage in STAGE_ORDER
            if self.store.has_stage(stage)
        }
        current_records = self.store.records()
        scratch = os.path.join(self.workspace, "_full_scratch")
        try:
            fresh = Pipeline(scratch, self.config)
            fresh.run_full(sample)
            differences = []
            for stage in STAGE_ORDER:
                if not fresh.store.has_stage(stage):
                    continue
                fresh_bytes = canonical_bytes(fresh.store.get_stage(stage))
                if stage not in current or current[stage] != fresh_bytes:
                    first_ref = self._first_ref_diff(stage, current.get(stage), fresh_bytes)
                    differences.append({"stage": stage, "first_ref_id": first_ref})
            if differences:
                raise IncrementalMismatchError(
                    "incremental rebuild differs from full rebuild",
                    {"first_difference": differences[0], "all_differences": differences},
                )
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        # Sequence numbers must still advance continuously in the incremental workspace.
        seqs = [record["seq"] for record in current_records]
        if seqs != list(range(1, len(seqs) + 1)):
            raise IncrementalMismatchError(
                "checkpoint sequence is not continuous", {"sequences": seqs}
            )
        return {"byte_identical": True, "stages": list(current), "sequences": seqs}

    def _first_ref_diff(self, stage, current_bytes, fresh_bytes):
        if current_bytes is None:
            return self._first_ref_from_bytes(fresh_bytes)
        current_obj = read_json(self.store.stage_product_path(stage)) if self.store.has_stage(stage) else {}
        fresh_obj = _loads(fresh_bytes)
        key = {
            "parse": "entries",
            "normalize": "results",
            "resolve": "groups",
        }.get(stage)
        if not key:
            return None
        current_ids = [item.get("ref_id") or item.get("group_id") for item in current_obj.get(key, [])]
        fresh_ids = [item.get("ref_id") or item.get("group_id") for item in fresh_obj.get(key, [])]
        for index, ref_id in enumerate(fresh_ids):
            if index >= len(current_ids) or current_ids[index] != ref_id:
                return ref_id
        return fresh_ids[0] if fresh_ids else None

    @staticmethod
    def _first_ref_from_bytes(raw):
        obj = _loads(raw)
        for key in ("entries", "results", "groups"):
            if obj.get(key):
                first = obj[key][0]
                return first.get("ref_id") or first.get("group_id")
        return None

    def status(self) -> dict:
        sample = read_json(self.sample_path) if os.path.exists(self.sample_path) else None
        fingerprints = self._fingerprints(sample) if sample else {}
        return {
            "workspace": self.workspace,
            "stages": {
                stage: {
                    "present": self.store.has_stage(stage),
                    "record": read_json(self.store.stage_record_path(stage))
                    if self.store.has_stage(stage)
                    else None,
                    "fingerprint": fingerprints.get(stage),
                }
                for stage in STAGE_ORDER
            },
            "checkpoint_records": self.store.records(),
            "stop_requested": self.store.stop_requested(),
            "config_digest": self.config.full_digest(),
        }


def _loads(raw: bytes):
    import json

    return json.loads(raw.decode("utf-8"))
