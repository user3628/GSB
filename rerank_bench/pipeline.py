"""Pipeline orchestration: workspace, fingerprints, invalidation, execution."""

from __future__ import annotations

import os
import shutil
import time

from . import errors
from .checkpoints import CheckpointStore, atomic_write_json, read_json
from .config import Config, load_config
from .errors import PipelineError
from .hashing import content_digest, file_digest
from .scheduler import plan_schedule
from .shardio import count_candidates, load_shard
from .stages import SHARD_STAGES, assemble_stage, rerank_stage, score_stage

GLOBAL_SHARD = "_global"


class Workspace:
    """On-disk workspace: imported data, checkpoints, artifacts, exports."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.data_dir = os.path.join(self.root, "data")
        self.export_dir = os.path.join(self.root, "export")
        self.state_path = os.path.join(self.root, "state.json")
        self.stop_path = os.path.join(self.root, "stop.request")
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.export_dir, exist_ok=True)
        self.store = CheckpointStore(self.root)

    def import_data(self, source_dir):
        """Import shards + config + manifest from a generated data dir."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        shutil.copytree(source_dir, self.data_dir)
        self.clear_runtime()
        return self.manifest()

    def clear_runtime(self):
        """Reset checkpoints/artifacts/state (used before fresh runs)."""
        for path in (self.store.ckpt_dir, self.store.art_dir, self.export_dir):
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
        for path in (self.state_path, self.stop_path):
            if os.path.exists(path):
                os.remove(path)

    @property
    def config_path(self):
        return os.path.join(self.data_dir, "schedule.yaml")

    @property
    def manifest_path(self):
        return os.path.join(self.data_dir, "manifest.json")

    def config(self) -> Config:
        return load_config(self.config_path)

    def manifest(self) -> dict:
        manifest = read_json(self.manifest_path)
        if manifest is None:
            raise PipelineError(
                "E_CONFIG",
                f"no manifest at {self.manifest_path}; import data first")
        return manifest

    def shard_path(self, shard_file) -> str:
        return os.path.join(self.data_dir, shard_file)

    def load_state(self) -> dict:
        return read_json(self.state_path, default={})

    def save_state(self, state: dict) -> None:
        atomic_write_json(self.state_path, state)

    def request_stop(self) -> None:
        atomic_write_json(self.stop_path, {"requested": True})

    def clear_stop(self) -> None:
        if os.path.exists(self.stop_path):
            os.remove(self.stop_path)

    def stop_requested(self) -> bool:
        return os.path.exists(self.stop_path)


def shard_fingerprints(shard_digest: str, config: Config) -> dict:
    """Chained per-stage version fingerprints for one shard."""
    fps = {}
    upstream = ""
    for stage in SHARD_STAGES:
        fps[stage] = content_digest({
            "stage": stage,
            "shard": shard_digest,
            "config": config.stage_config_digest(stage),
            "upstream": upstream,
        })
        upstream = fps[stage]
    return fps


def aggregate_fingerprint(shard_fps: dict, plan: dict, config: Config) -> str:
    admitted = [a["shard_id"] for a in plan["admitted"]]
    return content_digest({
        "stage": "aggregate",
        "shards": {sid: shard_fps[sid]["rerank"] for sid in sorted(admitted)},
        "plan": {
            "admitted": [a["shard_id"] for a in plan["admitted"]],
            "rejected": [r["shard_id"] for r in plan["rejected"]],
        },
        "config": config.stage_config_digest("aggregate"),
    })


def _shard_infos(ws: Workspace) -> list:
    infos = []
    for shard in ws.manifest()["shards"]:
        infos.append({
            "shard_id": shard["shard_id"],
            "shard_index": shard["shard_index"],
            "candidates": count_candidates(ws.shard_path(shard["file"])),
        })
    return infos


def _classify_invalidation(rec, shard_digest, config, stage) -> str:
    inputs = rec.get("inputs", {})
    if inputs.get("shard_digest") != shard_digest:
        return "shard_content_changed"
    if inputs.get("config_stage_digest") != config.stage_config_digest(stage):
        return f"config_changed:{stage}"
    return "upstream_invalidated"


def detect_and_invalidate(ws: Workspace, config: Config) -> list:
    """Invalidate stages whose fingerprints drifted; return event log."""
    events = []
    manifest = ws.manifest()
    for shard in manifest["shards"]:
        shard_id = shard["shard_id"]
        shard_digest = file_digest(ws.shard_path(shard["file"]))
        fps = shard_fingerprints(shard_digest, config)
        for idx, stage in enumerate(SHARD_STAGES):
            rec = ws.store.get_record(shard_id, stage)
            if rec is None:
                break
            if rec["input_fingerprint"] == fps[stage]:
                continue
            reason = _classify_invalidation(rec, shard_digest, config, stage)
            for downstream in SHARD_STAGES[idx:]:
                ws.store.invalidate(shard_id, downstream)
            events.append({
                "shard_id": shard_id,
                "earliest_stage": stage,
                "invalidated_stages": SHARD_STAGES[idx:],
                "reason": reason,
                "old_fingerprint": rec["input_fingerprint"],
                "new_fingerprint": fps[stage],
            })
            break
    plan = plan_schedule(_shard_infos(ws), config)
    shard_fps = {
        s["shard_id"]: shard_fingerprints(
            file_digest(ws.shard_path(s["file"])), config)
        for s in manifest["shards"]
    }
    agg_fp = aggregate_fingerprint(shard_fps, plan, config)
    rec = ws.store.get_record(GLOBAL_SHARD, "aggregate")
    if rec is not None and rec["input_fingerprint"] != agg_fp:
        ws.store.invalidate(GLOBAL_SHARD, "aggregate")
        events.append({
            "shard_id": GLOBAL_SHARD,
            "earliest_stage": "aggregate",
            "invalidated_stages": ["aggregate"],
            "reason": "upstream_or_budget_changed",
            "old_fingerprint": rec["input_fingerprint"],
            "new_fingerprint": agg_fp,
        })
    return events


def run_shard_stages(workspace_root: str, shard_id: str, shard_file: str,
                     config_raw: dict, latency_budget_ms: int,
                     clock=None, stages=None) -> dict:
    """Run (or reuse) assemble/score/rerank for one shard.

    Used by both the serial runner and worker processes. Never raises:
    errors are returned as structured objects so reduction can continue.
    """
    clock = clock or time.monotonic
    started = clock()
    try:
        config = Config(config_raw)
        ws = Workspace(workspace_root)
        shard_path = ws.shard_path(shard_file)
        shard_digest = file_digest(shard_path)
        fps = shard_fingerprints(shard_digest, config)
        groups, candidates = load_shard(shard_path)
        evaluations = sum(len(v) for v in candidates.values())
        stage_status = {}
        stages = stages or SHARD_STAGES
        upstream_fp = ""
        artifact = None
        for stage in stages:
            fp = fps[stage]
            if ws.store.is_valid(shard_id, stage, fp):
                artifact = ws.store.get_artifact(shard_id, stage)
                stage_status[stage] = "reused"
            else:
                if stage != "assemble" and artifact is None:
                    raise PipelineError(
                        errors.E_INVALID_STAGE,
                        f"upstream artifact missing before stage {stage} "
                        f"for shard {shard_id}",
                        shard_id=shard_id, stage=stage)
                if stage == "assemble":
                    artifact = assemble_stage(groups, candidates, config)
                elif stage == "score":
                    artifact = score_stage(artifact, config)
                else:
                    artifact = rerank_stage(artifact, config)
                ws.store.publish(shard_id, stage, fp, config.config_version,
                                 artifact)
                rec = ws.store.get_record(shard_id, stage)
                rec["inputs"] = {
                    "shard_digest": shard_digest,
                    "config_stage_digest": config.stage_config_digest(stage),
                    "upstream_fingerprint": upstream_fp,
                }
                atomic_write_json(ws.store.record_path(shard_id, stage), rec)
                stage_status[stage] = "done"
            upstream_fp = fp
            elapsed_ms = int((clock() - started) * 1000)
            if elapsed_ms > latency_budget_ms:
                raise PipelineError(
                    errors.E_LATENCY_BUDGET,
                    f"shard {shard_id} exceeded latency budget "
                    f"{latency_budget_ms}ms after stage {stage}",
                    shard_id=shard_id, stage=stage,
                    elapsed_ms=elapsed_ms, budget_ms=latency_budget_ms,
                )
        return {
            "shard_id": shard_id,
            "status": "done",
            "stages": stage_status,
            "evaluations": evaluations,
        }
    except PipelineError as exc:
        return {"shard_id": shard_id, "status": "error", "error": exc.to_dict()}
    except Exception as exc:  # crash isolation: structured, reduction-safe
        return {
            "shard_id": shard_id,
            "status": "error",
            "error": errors.error_dict(
                errors.E_WORKER_FAILURE, f"{type(exc).__name__}: {exc}",
                shard_id=shard_id),
        }
