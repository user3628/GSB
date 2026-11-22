"""Four-stage pipeline orchestration with budget scheduling and recovery."""

import json
import os

from .align import align_shard
from .audio import read_wav, validate_audio
from .checkpoint import CheckpointStore
from .config import STAGES, estimate_frame_count
from .decoder import verify_consistency
from .errors import BudgetExhaustedError, PipelineError
from .frontend import (
    build_frontend_artifact,
    featureize,
    read_feature_blob,
    write_feature_blob,
)
from .hashing import canonical_json, digest_json, sha256_file
from .manifest import ordered_shards

STOP_FILENAME = "STOP_REQUESTED"


class Pipeline:
    def __init__(self, config, manifest, data_dir, workspace_dir, stop_observer=None):
        self.config = config
        self.manifest = manifest
        self.data_dir = data_dir
        self.workspace = workspace_dir
        self.store = CheckpointStore(workspace_dir)
        self.stop_observer = stop_observer
        self.feature_dir = os.path.join(workspace_dir, "features")
        os.makedirs(self.feature_dir, exist_ok=True)
        self.diagnostics = []

    def stop_requested(self):
        if os.path.exists(os.path.join(self.workspace, STOP_FILENAME)):
            return True
        if self.stop_observer is not None and self.stop_observer():
            return True
        return False

    def plan_admission(self):
        """Admit shards in declared order under all three budget constraints."""
        budget = self.config.budget
        global_used = 0
        plan = []
        for entry in ordered_shards(self.manifest):
            estimated = estimate_frame_count(
                entry["declared_samples"], self.config.frame
            )
            reasons = []
            if global_used + estimated > budget.global_frame_budget:
                reasons.append("global_frame_budget")
            if estimated > budget.worker_frame_quota:
                reasons.append("worker_frame_quota")
            if estimated > budget.shard_frame_latency:
                reasons.append("shard_frame_latency")
            if reasons:
                plan.append(
                    {
                        "shard_id": entry["shard_id"],
                        "admitted": False,
                        "estimated_frames": estimated,
                        "reasons": reasons,
                    }
                )
                continue
            plan.append(
                {
                    "shard_id": entry["shard_id"],
                    "admitted": True,
                    "estimated_frames": estimated,
                    "reasons": [],
                }
            )
            global_used += estimated
        return plan, global_used

    def _feature_blob_path(self, shard_id):
        return os.path.join(self.feature_dir, f"{shard_id}.f32")

    def _load_or_run_frontend(self, entry, wav_path, input_fingerprint):
        shard_id = entry["shard_id"]
        try:
            record = self.store.load_record(shard_id, "frontend")
        except Exception:
            record = None
        if record is not None and self._fingerprints_match(record, input_fingerprint):
            blob_path = self._feature_blob_path(shard_id)
            features, offsets, padded = featureize(
                read_wav(wav_path)[1], self.config.frame
            )
            return features, record, "reused"
        sample_rate, samples = read_wav(wav_path)
        quality = validate_audio(sample_rate, samples, entry)
        features, offsets, padded = featureize(samples, self.config.frame)
        artifact = build_frontend_artifact(samples, self.config.frame)
        artifact["quality"] = quality
        write_feature_blob(self._feature_blob_path(shard_id), features)
        if record is not None:
            record = self.store.force_rebuild(
                shard_id, "frontend", artifact, input_fingerprint,
                self.config.fingerprint(),
            )
            reason = "rebuilt:input_or_config_changed"
        else:
            record = self.store.publish(
                shard_id, "frontend", artifact, input_fingerprint,
                self.config.fingerprint(),
            )
            reason = "new"
        return features, record, reason

    def _fingerprints_match(self, record, input_fingerprint):
        return (
            record["input_fingerprint"] == input_fingerprint
            and record["config_fingerprint"] == self.config.fingerprint()
        )

    def _cached_stage(self, shard_id, stage, input_fingerprint):
        try:
            record = self.store.load_record(shard_id, stage)
        except Exception:
            return None
        if record is not None and self._fingerprints_match(record, input_fingerprint):
            return record
        return None

    def process_shard(self, entry, wav_path):
        entry = dict(entry)
        entry.setdefault("sample_rate", self.manifest["sample_rate"])
        shard_id = entry["shard_id"]
        input_fingerprint = sha256_file(wav_path)
        stage_trace = []
        features, frontend_record, frontend_mode = self._load_or_run_frontend(
            entry, wav_path, input_fingerprint
        )
        stage_trace.append({"stage": "frontend", "mode": frontend_mode})

        decode_record = self._cached_stage(shard_id, "decode", input_fingerprint)
        if decode_record is None:
            decode_artifact = verify_consistency(
                features, self.config.frame, self.config.decoder
            )
            decode_record = self._publish_stage(
                shard_id, "decode", decode_artifact, input_fingerprint
            )
            stage_trace.append({"stage": "decode", "mode": "new"})
        else:
            stage_trace.append({"stage": "decode", "mode": "reused"})

        align_record = self._cached_stage(shard_id, "align", input_fingerprint)
        if align_record is None:
            align_artifact = align_shard(
                decode_record["artifact"], entry, self.config.align
            )
            align_record = self._publish_stage(
                shard_id, "align", align_artifact, input_fingerprint
            )
            stage_trace.append({"stage": "align", "mode": "new"})
            if align_artifact["status"] == "MISMATCH":
                self.diagnostics.append(
                    {
                        "code": "E_ENDPOINT_MISMATCH",
                        "context": {
                            "shard_id": shard_id,
                            "out_of_bounds": align_artifact["out_of_bounds"],
                        },
                    }
                )
        else:
            stage_trace.append({"stage": "align", "mode": "reused"})
        return {
            "shard_id": shard_id,
            "input_fingerprint": input_fingerprint,
            "stages": stage_trace,
            "frontend": frontend_record["artifact"],
            "decode": decode_record["artifact"],
            "align": align_record["artifact"],
        }

    def process_shard_stages(self, entry, wav_path, enabled_stages):
        """Run only the requested stages; existing upstream stages are reused."""
        entry = dict(entry)
        entry.setdefault("sample_rate", self.manifest["sample_rate"])
        shard_id = entry["shard_id"]
        input_fingerprint = sha256_file(wav_path)
        trace = []
        result = {"shard_id": shard_id, "input_fingerprint": input_fingerprint}
        if "frontend" in enabled_stages:
            features, frontend_record, mode = self._load_or_run_frontend(
                entry, wav_path, input_fingerprint
            )
            trace.append({"stage": "frontend", "mode": mode})
            result["frontend"] = frontend_record["artifact"]
        else:
            record = self._cached_stage(shard_id, "frontend", input_fingerprint)
            if record is None:
                from .errors import InvalidStageError

                raise InvalidStageError(
                    "frontend artifact is required before later stages",
                    {"shard_id": shard_id},
                )
            sample_rate, samples = read_wav(wav_path)
            features, _, _ = featureize(samples, self.config.frame)
            trace.append({"stage": "frontend", "mode": "reused"})
            result["frontend"] = record["artifact"]
        if "decode" in enabled_stages:
            decode_record = self._cached_stage(shard_id, "decode", input_fingerprint)
            if decode_record is None:
                decode_artifact = verify_consistency(
                    features, self.config.frame, self.config.decoder
                )
                decode_record = self._publish_stage(
                    shard_id, "decode", decode_artifact, input_fingerprint
                )
                trace.append({"stage": "decode", "mode": "new"})
            else:
                trace.append({"stage": "decode", "mode": "reused"})
            result["decode"] = decode_record["artifact"]
        if "align" in enabled_stages:
            decode_record = self._cached_stage(shard_id, "decode", input_fingerprint)
            align_record = self._cached_stage(shard_id, "align", input_fingerprint)
            if align_record is None:
                align_artifact = align_shard(
                    decode_record["artifact"], entry, self.config.align
                )
                align_record = self._publish_stage(
                    shard_id, "align", align_artifact, input_fingerprint
                )
                trace.append({"stage": "align", "mode": "new"})
                if align_artifact["status"] == "MISMATCH":
                    self.diagnostics.append(
                        {
                            "code": "E_ENDPOINT_MISMATCH",
                            "context": {
                                "shard_id": shard_id,
                                "out_of_bounds": align_artifact["out_of_bounds"],
                            },
                        }
                    )
            else:
                trace.append({"stage": "align", "mode": "reused"})
            result["align"] = align_record["artifact"]
        result["stages"] = trace
        return result

    def _publish_stage(self, shard_id, stage, artifact, input_fingerprint):
        try:
            return self.store.publish(
                shard_id, stage, artifact, input_fingerprint,
                self.config.fingerprint(),
            )
        except Exception:
            return self.store.force_rebuild(
                shard_id, stage, artifact, input_fingerprint,
                self.config.fingerprint(),
            )

    def run(self):
        plan, planned_global = self.plan_admission()
        results = []
        unprocessed = []
        stop_after = None
        actual_global = 0
        for item in plan:
            entry = next(
                shard for shard in ordered_shards(self.manifest)
                if shard["shard_id"] == item["shard_id"]
            )
            if not item["admitted"]:
                unprocessed.append(
                    {
                        "shard_id": item["shard_id"],
                        "estimated_frames": item["estimated_frames"],
                        "reasons": item["reasons"],
                    }
                )
                continue
            if self.stop_requested():
                stop_after = "stop_requested_between_shards"
                unprocessed.append(
                    {
                        "shard_id": item["shard_id"],
                        "estimated_frames": item["estimated_frames"],
                        "reasons": ["graceful_stop"],
                    }
                )
                continue
            wav_path = os.path.join(self.data_dir, entry["wav_file"])
            try:
                result = self.process_shard(entry, wav_path)
            except PipelineError as exc:
                unprocessed.append(
                    {
                        "shard_id": item["shard_id"],
                        "estimated_frames": item["estimated_frames"],
                        "reasons": [exc.code],
                        "error": exc.to_dict(),
                    }
                )
                self.diagnostics.append(exc.to_dict())
                continue
            results.append(result)
            actual_global += result["frontend"]["frame_count"]
        summary = self._build_summary(plan, results, unprocessed, actual_global, stop_after)
        self.store.write_summary(summary)
        return summary

    def _build_summary(self, plan, results, unprocessed, actual_global, stop_after):
        alignment = [result["align"] for result in results]
        verified = [item for item in alignment if item["status"] != "UNVERIFIED"]
        all_hits = sum(item["hits"] for item in verified)
        all_refs = sum(item["reference_count"] for item in verified)
        deviations = [
            value
            for item in verified
            if item["mean_abs_deviation_ms"] is not None
            for value in (item["mean_abs_deviation_ms"],)
        ]
        endpoint_failures = [item for item in alignment if item["status"] == "MISMATCH"]
        return {
            "config_fingerprint": self.config.fingerprint(),
            "manifest_fingerprint": digest_json(self.manifest),
            "budget": {
                "global_frame_budget": self.config.budget.global_frame_budget,
                "worker_frame_quota": self.config.budget.worker_frame_quota,
                "shard_frame_latency": self.config.budget.shard_frame_latency,
                "planned_frames": sum(item["estimated_frames"] for item in plan),
                "consumed_frames": actual_global,
            },
            "processed_shards": [result["shard_id"] for result in results],
            "unprocessed": unprocessed,
            "alignment": {
                "verified_shards": len(verified),
                "unverified_shards": [
                    item["shard_id"] for item in alignment if item["status"] == "UNVERIFIED"
                ],
                "total_reference_words": all_refs,
                "total_hits": all_hits,
                "mean_deviation_ms": round(
                    sum(deviations) / len(deviations), 3
                ) if deviations else None,
                "max_deviation_ms": max(
                    (item["max_abs_deviation_ms"] for item in verified
                     if item["max_abs_deviation_ms"] is not None),
                    default=None,
                ),
                "endpoint_failures": endpoint_failures,
                "mixed_script_words": sorted(
                    {
                        word
                        for item in alignment
                        for word in item.get("mixed_script_words", [])
                    }
                ),
            },
            "diagnostics": self.diagnostics,
            "stop_after": stop_after,
            "result_fingerprint": digest_json(
                {
                    "processed": [
                        {
                            "shard_id": result["shard_id"],
                            "decode_digest": digest_json(result["decode"]),
                            "align_digest": digest_json(result["align"]),
                        }
                        for result in results
                    ],
                    "unprocessed": unprocessed,
                }
            ),
        }


def request_stop(workspace_dir):
    path = os.path.join(workspace_dir, STOP_FILENAME)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("stop\n")
    return path


def clear_stop(workspace_dir):
    path = os.path.join(workspace_dir, STOP_FILENAME)
    if os.path.exists(path):
        os.remove(path)
