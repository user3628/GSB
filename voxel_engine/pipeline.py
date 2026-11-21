"""Pipeline orchestration: version fingerprints, invalidation, staged build.

Every stage instance gets a version fingerprint chained from its content
inputs:

    fp(projection:view) = H(stage, cfg1(view), sha256(image bytes))
    fp(voxelize:view)   = H(stage, cfg2, fp(projection:view))
    fp(fusion)          = H(stage, cfg3, [fp(voxelize:view) for view in order])
    fp(sparsify)        = H(stage, cfg4, fp(fusion))

where cfg* are the exact config subsets each stage depends on. Changing view
pixels, camera parameters, voxel resolution, truncation distance, fusion
radius or the sparsity threshold therefore changes the fingerprint of the
earliest dependent stage and - through the chain - of every downstream
stage, while untouched upstream stages keep their fingerprints and their
published artifacts are reused (never recomputed).
"""

from __future__ import annotations

import shutil
from pathlib import Path

from . import artifacts
from .checkpoints import CheckpointStore, fold_chain
from .errors import E_INVALID_STAGE, E_STOPPED, EngineError
from .serialization import atomic_write_json, hash_json, read_file_bytes, sha256_hex
from .stages import fusion as fusion_stage
from .stages import projection as projection_stage
from .stages import sparsify as sparsify_stage
from .stages import voxelize as voxelize_stage

STAGE_PROJECTION = "projection"
STAGE_VOXELIZE = "voxelize"
STAGE_FUSION = "fusion"
STAGE_SPARSIFY = "sparsify"
STAGES = (STAGE_PROJECTION, STAGE_VOXELIZE, STAGE_FUSION, STAGE_SPARSIFY)

STOP_FILENAME = "STOP_REQUESTED"
FUSION_INSTANCE = STAGE_FUSION
SPARSIFY_INSTANCE = STAGE_SPARSIFY


def projection_instance(view_id: str) -> str:
    return f"{STAGE_PROJECTION}:{view_id}"


def voxelize_instance(view_id: str) -> str:
    return f"{STAGE_VOXELIZE}:{view_id}"


def artifact_rel_path(instance: str, view_id=None, fingerprint: str = "") -> str:
    # Content-addressed paths: republishing an instance never overwrites the
    # artifact referenced by an older checkpoint record, keeping the
    # append-only checkpoint chain verifiable.
    tag = fingerprint[:12]
    if instance.startswith(STAGE_PROJECTION + ":"):
        return f"artifacts/projection/{view_id}.{tag}.ve1"
    if instance.startswith(STAGE_VOXELIZE + ":"):
        return f"artifacts/voxelize/{view_id}.{tag}.ve2"
    if instance == FUSION_INSTANCE:
        return f"artifacts/fusion/global.{tag}.ve3"
    return f"artifacts/sparsify/global.{tag}.ve4"


def _projection_config(manifest, view, image_sha: str) -> dict:
    cfg = manifest.config
    return {
        "stage": STAGE_PROJECTION,
        "view_id": view.view_id,
        "ray_step": cfg["ray_step"],
        "max_ray_depth": cfg["max_ray_depth"],
        "truncation_distance": cfg["truncation_distance"],
        "width": view.width,
        "height": view.height,
        "intrinsic": {
            "fx": view.intrinsic.fx,
            "fy": view.intrinsic.fy,
            "cx": view.intrinsic.cx,
            "cy": view.intrinsic.cy,
        },
        "extrinsic": {
            "rotation": list(view.extrinsic.rotation),
            "translation": list(view.extrinsic.translation),
        },
        "image_sha256": image_sha,
        "landmarks": [
            {
                "landmark_id": lm.landmark_id,
                "pixel": [lm.pixel[0], lm.pixel[1]],
                "depth": lm.depth,
                "node": lm.node,
            }
            for lm in view.landmarks
        ],
    }


def _voxelize_config(manifest) -> dict:
    cfg = manifest.config
    return {
        "stage": STAGE_VOXELIZE,
        "voxel_resolution": cfg["voxel_resolution"],
        "truncation_distance": cfg["truncation_distance"],
        "scene_origin": list(cfg["scene_origin"]),
        "scene_size": list(cfg["scene_size"]),
        "voxel_feature_dim": cfg["voxel_feature_dim"],
    }


def _fusion_config(manifest) -> dict:
    return {"stage": STAGE_FUSION, "fusion_radius": manifest.config["fusion_radius"]}


def _sparsify_config(manifest) -> dict:
    return {
        "stage": STAGE_SPARSIFY,
        "sparsity_threshold": manifest.config["sparsity_threshold"],
    }


def _fingerprint(stage: str, config_version: str, input_fingerprint: str, upstream_fps: dict) -> str:
    return hash_json(
        {
            "stage": stage,
            "config_version": config_version,
            "input_fingerprint": input_fingerprint,
            "upstream_fingerprints": upstream_fps,
        }
    )


def build_plan(manifest, image_hashes: dict) -> list:
    """Compute the ordered desired plan of stage instances + fingerprints."""
    plan = []
    fps = {}
    voxelize_cfg = _voxelize_config(manifest)
    voxelize_cfg_version = hash_json(voxelize_cfg)
    for view in manifest.views:
        cfg = _projection_config(manifest, view, image_hashes[view.view_id])
        cfg_version = hash_json(cfg)
        input_fp = image_hashes[view.view_id]
        fp = _fingerprint(STAGE_PROJECTION, cfg_version, input_fp, {})
        instance = projection_instance(view.view_id)
        fps[instance] = fp
        plan.append(
            {
                "instance": instance,
                "stage": STAGE_PROJECTION,
                "view_id": view.view_id,
                "config": cfg,
                "config_version": cfg_version,
                "input_fingerprint": input_fp,
                "upstreams": {},
                "fingerprint": fp,
            }
        )
    for view in manifest.views:
        upstream = projection_instance(view.view_id)
        upstream_fps = {upstream: fps[upstream]}
        input_fp = fps[upstream]
        fp = _fingerprint(STAGE_VOXELIZE, voxelize_cfg_version, input_fp, upstream_fps)
        instance = voxelize_instance(view.view_id)
        fps[instance] = fp
        plan.append(
            {
                "instance": instance,
                "stage": STAGE_VOXELIZE,
                "view_id": view.view_id,
                "config": voxelize_cfg,
                "config_version": voxelize_cfg_version,
                "input_fingerprint": input_fp,
                "upstreams": upstream_fps,
                "fingerprint": fp,
            }
        )
    fusion_cfg = _fusion_config(manifest)
    fusion_cfg_version = hash_json(fusion_cfg)
    fusion_upstreams = {
        voxelize_instance(view.view_id): fps[voxelize_instance(view.view_id)]
        for view in manifest.views
    }
    fusion_input = hash_json(
        [fusion_upstreams[voxelize_instance(view.view_id)] for view in manifest.views]
    )
    fusion_fp = _fingerprint(STAGE_FUSION, fusion_cfg_version, fusion_input, fusion_upstreams)
    fps[FUSION_INSTANCE] = fusion_fp
    plan.append(
        {
            "instance": FUSION_INSTANCE,
            "stage": STAGE_FUSION,
            "view_id": None,
            "config": fusion_cfg,
            "config_version": fusion_cfg_version,
            "input_fingerprint": fusion_input,
            "upstreams": fusion_upstreams,
            "fingerprint": fusion_fp,
        }
    )
    sparsify_cfg = _sparsify_config(manifest)
    sparsify_cfg_version = hash_json(sparsify_cfg)
    sparsify_upstreams = {FUSION_INSTANCE: fusion_fp}
    sparsify_fp = _fingerprint(
        STAGE_SPARSIFY, sparsify_cfg_version, fusion_fp, sparsify_upstreams
    )
    plan.append(
        {
            "instance": SPARSIFY_INSTANCE,
            "stage": STAGE_SPARSIFY,
            "view_id": None,
            "config": sparsify_cfg,
            "config_version": sparsify_cfg_version,
            "input_fingerprint": fusion_fp,
            "upstreams": sparsify_upstreams,
            "fingerprint": sparsify_fp,
        }
    )
    return plan


def _config_diff(old: dict, new: dict) -> list:
    reasons = []
    for key in sorted(set(old) | set(new)):
        if old.get(key) != new.get(key):
            reasons.append(
                {"kind": "config_changed", "key": key, "old": old.get(key), "new": new.get(key)}
            )
    return reasons


class Pipeline:
    """Executes the staged build against one workspace."""

    def __init__(self, workspace, manifest, images: dict):
        self.workspace = Path(workspace)
        self.manifest = manifest
        self.images = images
        self.store = CheckpointStore(workspace)
        self.stop_path = self.workspace / STOP_FILENAME
        self.invalidation_path = self.workspace / "state" / "invalidation.json"

    def _image_hashes(self) -> dict:
        return {
            view_id: sha256_hex(data.tobytes()) for view_id, data in self.images.items()
        }

    def _stop_requested(self) -> bool:
        return self.stop_path.exists()

    def request_stop(self) -> None:
        self.stop_path.touch()

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def build(self, mode: str = "incremental", stop_after=None, lock_timeout_ms: int = 5000) -> dict:
        if mode not in ("incremental", "full", "resume"):
            raise ValueError(f"unknown build mode: {mode}")
        if mode == "resume":
            self.clear_stop()
        elif self._stop_requested():
            raise EngineError(
                E_STOPPED,
                "a stop was requested; run recover + build --mode resume to continue",
                {"stop_file": STOP_FILENAME},
            )
        if mode == "full":
            self._reset_workspace()
        plan = build_plan(self.manifest, self._image_hashes())
        chain, _ignored, _truncation = self.store.scan()
        effective, _orphaned = fold_chain(chain)
        report = {
            "mode": mode,
            "status": "completed",
            "stopped_at": None,
            "instances": [],
            "rebuilt": [],
            "reused": [],
        }
        cache = {}
        seq = len(chain)
        for entry in plan:
            instance = entry["instance"]
            published = effective.get(instance)
            if published is not None and published["fingerprint"] == entry["fingerprint"]:
                report["instances"].append(
                    {
                        "instance": instance,
                        "stage": entry["stage"],
                        "action": "reused",
                        "fingerprint": entry["fingerprint"],
                        "seq": published["seq"],
                    }
                )
                report["reused"].append(instance)
                continue
            if self._stop_requested():
                report["status"] = "stopped"
                report["stopped_at"] = instance
                break
            reasons = self._invalidation_reasons(entry, published, effective)
            artifact_bytes, payload = self._execute(entry, cache)
            record = self.store.publish(
                stage=entry["stage"],
                instance=instance,
                view_id=entry["view_id"],
                fingerprint=entry["fingerprint"],
                input_fingerprint=entry["input_fingerprint"],
                config_version=entry["config_version"],
                config=entry["config"],
                upstream_fingerprints=entry["upstreams"],
                artifact_bytes=artifact_bytes,
                artifact_rel_path=artifact_rel_path(
                    instance, entry["view_id"], entry["fingerprint"]
                ),
                expected_prev_seq=seq,
                lock_timeout_ms=lock_timeout_ms,
            )
            seq = record["seq"]
            effective[instance] = record
            cache[instance] = payload
            report["instances"].append(
                {
                    "instance": instance,
                    "stage": entry["stage"],
                    "action": "rebuilt",
                    "fingerprint": entry["fingerprint"],
                    "seq": seq,
                    "reasons": reasons,
                }
            )
            report["rebuilt"].append(instance)
            if stop_after is not None and instance == stop_after:
                self.request_stop()
        atomic_write_json(self.invalidation_path, report)
        return report

    def _invalidation_reasons(self, entry: dict, published, effective: dict) -> list:
        if published is None:
            return [{"kind": "missing", "detail": "no published artifact"}]
        reasons = []
        if published["input_fingerprint"] != entry["input_fingerprint"]:
            reasons.append(
                {
                    "kind": "input_changed",
                    "old": published["input_fingerprint"],
                    "new": entry["input_fingerprint"],
                }
            )
        reasons.extend(_config_diff(published.get("config", {}), entry["config"]))
        for upstream, fp in entry["upstreams"].items():
            old_fp = published.get("upstream_fingerprints", {}).get(upstream)
            if old_fp != fp:
                reasons.append(
                    {
                        "kind": "upstream_changed",
                        "upstream": upstream,
                        "old": old_fp,
                        "new": fp,
                    }
                )
        return reasons

    def _load_upstream_payload(self, instance: str, cache: dict, effective_record):
        if instance in cache:
            return cache[instance]
        artifact_abs = self.workspace / effective_record["artifact_path"]
        data = read_file_bytes(artifact_abs)
        if instance.startswith(STAGE_PROJECTION + ":"):
            payload = artifacts.decode_projection(data)
        elif instance.startswith(STAGE_VOXELIZE + ":"):
            payload = artifacts.decode_voxel_block(data)
        elif instance == FUSION_INSTANCE:
            payload = artifacts.decode_fused_field(data)
        else:
            payload = artifacts.decode_sparse_field(data)
        cache[instance] = payload
        return payload

    def _execute(self, entry: dict, cache: dict):
        """Run one stage instance; return (artifact_bytes, payload)."""
        stage = entry["stage"]
        if stage == STAGE_PROJECTION:
            view = next(v for v in self.manifest.views if v.view_id == entry["view_id"])
            result = projection_stage.run_projection(
                view, self.images[view.view_id], self.manifest.config
            )
            return artifacts.encode_projection(result), result
        if stage == STAGE_VOXELIZE:
            upstream = projection_instance(entry["view_id"])
            records = self._require_payload(entry, upstream, cache)
            raw = voxelize_stage.run_voxelization(records, self.manifest.config)
            stats = {
                "occupied_voxels": raw["occupied_voxels"],
                "occluded_skips": raw["occluded_skips"],
                "total_hits": raw["total_hits"],
            }
            normalized = fusion_stage.normalize_block(raw)
            return artifacts.encode_voxel_block(normalized, stats), normalized
        if stage == STAGE_FUSION:
            blocks = []
            for view in self.manifest.views:
                instance = voxelize_instance(view.view_id)
                blocks.append(self._require_payload(entry, instance, cache))
            fused = fusion_stage.run_fusion(
                blocks, float(self.manifest.config["fusion_radius"])
            )
            return artifacts.encode_fused_field(fused), fused
        fused = self._require_payload(entry, FUSION_INSTANCE, cache)
        sparse = sparsify_stage.run_sparsification(
            fused, float(self.manifest.config["sparsity_threshold"])
        )
        return artifacts.encode_sparse_field(sparse), sparse

    def _require_payload(self, entry: dict, upstream: str, cache: dict):
        if upstream in cache:
            return cache[upstream]
        chain, _ignored, _truncation = self.store.scan()
        effective, _orphaned = fold_chain(chain)
        record = effective.get(upstream)
        if record is None:
            raise EngineError(
                E_INVALID_STAGE,
                f"upstream artifact '{upstream}' missing for '{entry['instance']}'",
                {"instance": entry["instance"], "missing_upstream": upstream},
            )
        return self._load_upstream_payload(upstream, cache, record)

    def _reset_workspace(self) -> None:
        for name in ("checkpoints", "artifacts", "state", "locks"):
            path = self.workspace / name
            if path.exists():
                shutil.rmtree(path)
        self.clear_stop()
