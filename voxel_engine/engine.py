"""High-level engine facade shared by the CLI and the HTTP interface."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image

from . import artifacts
from .checkpoints import CheckpointStore, fold_chain
from .errors import (
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_NOT_FOUND,
    EngineError,
)
from .manifest import load_manifest
from .pipeline import (
    FUSION_INSTANCE,
    SPARSIFY_INSTANCE,
    Pipeline,
    build_plan,
    projection_instance,
    voxelize_instance,
)
from .serialization import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json,
    read_file_bytes,
    sha256_hex,
)

INPUTS_DIR = "inputs"
MANIFEST_NAME = "manifest.json"


def _load_image(path) -> np.ndarray:
    with Image.open(path) as img:
        return np.asarray(img.convert("RGB"), dtype=np.uint8).copy()


class Engine:
    def __init__(self, workspace):
        self.workspace = Path(workspace)
        self.inputs_dir = self.workspace / INPUTS_DIR
        self.manifest_path = self.inputs_dir / MANIFEST_NAME
        self.store = CheckpointStore(self.workspace)

    def import_dataset(self, manifest_path) -> dict:
        """Import a scene manifest + its shard PNGs into the workspace."""
        manifest_path = Path(manifest_path)
        manifest = load_manifest(manifest_path)
        self.inputs_dir.mkdir(parents=True, exist_ok=True)
        for view in manifest.views:
            src = manifest_path.parent / view.image
            if not src.is_file():
                raise EngineError(
                    E_NOT_FOUND,
                    f"shard image missing: {view.image}",
                    {"view_id": view.view_id, "image": view.image},
                )
            dst = self.inputs_dir / view.image
            dst.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_bytes(dst, read_file_bytes(src))
        atomic_write_bytes(self.manifest_path, read_file_bytes(manifest_path))
        return {
            "imported_views": [view.view_id for view in manifest.views],
            "manifest": str(self.manifest_path),
        }

    def _require_manifest(self):
        if not self.manifest_path.is_file():
            raise EngineError(
                E_NOT_FOUND,
                "no manifest imported into this workspace",
                {"workspace": str(self.workspace)},
            )
        return load_manifest(self.manifest_path)

    def _load_images(self, manifest) -> dict:
        images = {}
        for view in manifest.views:
            path = self.inputs_dir / view.image
            if not path.is_file():
                raise EngineError(
                    E_NOT_FOUND,
                    f"shard image missing in workspace: {view.image}",
                    {"view_id": view.view_id},
                )
            images[view.view_id] = _load_image(path)
        return images

    def _pipeline(self) -> Pipeline:
        manifest = self._require_manifest()
        images = self._load_images(manifest)
        return Pipeline(self.workspace, manifest, images)

    def build(self, mode: str = "incremental", stop_after=None, lock_timeout_ms: int = 5000) -> dict:
        return self._pipeline().build(
            mode=mode, stop_after=stop_after, lock_timeout_ms=lock_timeout_ms
        )

    def stop(self) -> dict:
        manifest = self._require_manifest()
        pipeline = Pipeline(self.workspace, manifest, {})
        pipeline.request_stop()
        return {"stop_requested": True}

    def recover(self) -> dict:
        return self.store.recover()

    def replace_view(self, view_id: str, image_path) -> dict:
        """Wholesale replacement of one view shard (incremental entry)."""
        manifest = self._require_manifest()
        view = next((v for v in manifest.views if v.view_id == view_id), None)
        if view is None:
            raise EngineError(
                E_NOT_FOUND, f"unknown view_id '{view_id}'", {"view_id": view_id}
            )
        image_path = Path(image_path)
        data = read_file_bytes(image_path)
        decoded = _load_image(image_path)
        if decoded.shape[0] != view.height or decoded.shape[1] != view.width:
            raise EngineError(
                E_INVALID_STAGE,
                "replacement image dimensions do not match the manifest",
                {
                    "view_id": view_id,
                    "expected": [view.height, view.width],
                    "got": [int(decoded.shape[0]), int(decoded.shape[1])],
                },
            )
        atomic_write_bytes(self.inputs_dir / view.image, data)
        return {"view_id": view_id, "image_sha256": sha256_hex(data)}

    def update_config(self, updates: dict) -> dict:
        """Change global config values (incremental entry)."""
        if not self.manifest_path.is_file():
            raise EngineError(E_NOT_FOUND, "no manifest imported", {})
        raw = json.loads(read_file_bytes(self.manifest_path).decode("utf-8"))
        changed = {}
        for key, value in updates.items():
            if key not in raw["config"]:
                raise EngineError(
                    E_INVALID_STAGE, f"unknown config key '{key}'", {"key": key}
                )
            old = raw["config"][key]
            raw["config"][key] = value
            changed[key] = {"old": old, "new": value}
        atomic_write_bytes(self.manifest_path, canonical_json(raw))
        return {"updated": changed}

    def status(self) -> dict:
        manifest = self._require_manifest()
        images = self._load_images(manifest)
        image_hashes = {
            view_id: sha256_hex(data.tobytes()) for view_id, data in images.items()
        }
        plan = build_plan(manifest, image_hashes)
        chain, ignored, truncation = self.store.scan()
        effective, orphaned = fold_chain(chain)
        stages = []
        for entry in plan:
            published = effective.get(entry["instance"])
            if published is None:
                state = "pending"
                seq = None
            elif published["fingerprint"] == entry["fingerprint"]:
                state = "published"
                seq = published["seq"]
            else:
                state = "stale"
                seq = published["seq"]
            stages.append(
                {
                    "instance": entry["instance"],
                    "stage": entry["stage"],
                    "status": state,
                    "desired_fingerprint": entry["fingerprint"],
                    "published_fingerprint": (
                        published["fingerprint"] if published else None
                    ),
                    "seq": seq,
                }
            )
        invalidation = None
        inv_path = self.workspace / "state" / "invalidation.json"
        if inv_path.is_file():
            invalidation = json.loads(read_file_bytes(inv_path).decode("utf-8"))
        sparsity = None
        sparse_rec = effective.get(SPARSIFY_INSTANCE)
        if sparse_rec is not None:
            try:
                payload = artifacts.decode_sparse_field(
                    read_file_bytes(self.workspace / sparse_rec["artifact_path"])
                )
                sparsity = payload["stats"]
            except (OSError, artifacts.ArtifactCorrupt):
                sparsity = None
        return {
            "workspace": str(self.workspace),
            "last_seq": len(chain),
            "stages": stages,
            "ignored_records": ignored,
            "orphaned_records": [
                {"seq": rec["seq"], "instance": rec["instance"]} for rec in orphaned
            ],
            "truncation": truncation,
            "invalidation": invalidation,
            "sparsity_stats": sparsity,
        }

    def _collect_digests(self) -> dict:
        manifest = self._require_manifest()
        images = self._load_images(manifest)
        image_hashes = {
            view_id: sha256_hex(data.tobytes()) for view_id, data in images.items()
        }
        plan = build_plan(manifest, image_hashes)
        chain, _ignored, _truncation = self.store.scan()
        effective, _orphaned = fold_chain(chain)
        digests = {}
        for entry in plan:
            record = effective.get(entry["instance"])
            if record is None or record["fingerprint"] != entry["fingerprint"]:
                raise EngineError(
                    E_INVALID_STAGE,
                    f"stage '{entry['instance']}' is not freshly published; build first",
                    {"instance": entry["instance"]},
                )
            data = read_file_bytes(self.workspace / record["artifact_path"])
            digests[entry["instance"]] = {
                "sha256": sha256_hex(data),
                "bytes": data,
                "artifact_path": record["artifact_path"],
            }
        contribution = b"".join(
            digests[projection_instance(view.view_id)]["bytes"] for view in manifest.views
        )
        field_bytes = digests[SPARSIFY_INSTANCE]["bytes"]
        sparse_payload = artifacts.decode_sparse_field(field_bytes)
        sparsity_bytes = canonical_json(sparse_payload["stats"])
        digests["_summary"] = {
            "contribution_digest": sha256_hex(contribution),
            "fused_field_digest": digests[FUSION_INSTANCE]["sha256"],
            "global_field_digest": sha256_hex(field_bytes),
            "sparsity_stats_sha256": sha256_hex(sparsity_bytes),
        }
        return digests

    @staticmethod
    def _first_field_diff(bytes_a: bytes, bytes_b: bytes, kind: str):
        if kind == "projection":
            pa = artifacts.decode_projection(bytes_a)
            pb = artifacts.decode_projection(bytes_b)
            if pa["count"] != pb["count"]:
                return {"record_index": int(min(pa["count"], pb["count"]))}
            for idx in range(pa["count"]):
                if (
                    pa["t"][idx] != pb["t"][idx]
                    or (pa["xyz"][idx] != pb["xyz"][idx]).any()
                    or (pa["rgb"][idx] != pb["rgb"][idx]).any()
                ):
                    return {"record_index": int(idx)}
            return {}
        if kind == "voxel_block":
            ba = artifacts.decode_voxel_block(bytes_a)["voxels"]
            bb = artifacts.decode_voxel_block(bytes_b)["voxels"]
        elif kind == "fusion":
            ba = artifacts.decode_fused_field(bytes_a)["field"]
            bb = artifacts.decode_fused_field(bytes_b)["field"]
        else:
            ba = artifacts.decode_sparse_field(bytes_a)["field"]
            bb = artifacts.decode_sparse_field(bytes_b)["field"]
        for coord in sorted(set(ba) | set(bb)):
            if ba.get(coord) != bb.get(coord):
                return {"voxel": [int(c) for c in coord]}
        return {}

    def verify_consistency(self) -> dict:
        """Byte-compare the current (possibly incremental) state with a fresh
        from-scratch full build of the same inputs."""
        current = self._collect_digests()
        with tempfile.TemporaryDirectory(prefix="voxel_verify_") as tmp:
            fresh = Engine(Path(tmp) / "workspace")
            fresh.import_dataset(self.manifest_path)
            fresh.build(mode="full")
            reference = fresh._collect_digests()
        manifest = self._require_manifest()
        order = [projection_instance(v.view_id) for v in manifest.views]
        order += [voxelize_instance(v.view_id) for v in manifest.views]
        order += [FUSION_INSTANCE, SPARSIFY_INSTANCE]
        kind_of = {
            "projection": "projection",
            "voxelize": "voxel_block",
            "fusion": "fusion",
            "sparsify": "sparsify",
        }
        for instance in order:
            if current[instance]["sha256"] == reference[instance]["sha256"]:
                continue
            stage = instance.split(":")[0]
            diff = self._first_field_diff(
                current[instance]["bytes"],
                reference[instance]["bytes"],
                kind_of[stage],
            )
            details = {"stage": stage, "instance": instance}
            details.update(diff)
            raise EngineError(
                E_INCREMENTAL_MISMATCH,
                f"incremental state diverges from full rebuild at '{instance}'",
                details,
            )
        report = {
            "match": True,
            "summary": current["_summary"],
            "stages_compared": order,
        }
        atomic_write_json(self.workspace / "exports" / "validation_report.json", report)
        return report

    def export_summary(self) -> dict:
        manifest = self._require_manifest()
        chain, _ignored, _truncation = self.store.scan()
        effective, _orphaned = fold_chain(chain)
        sparse_rec = effective.get(SPARSIFY_INSTANCE)
        if sparse_rec is None:
            raise EngineError(
                E_INVALID_STAGE, "sparsify stage not published; build first", {}
            )
        payload = artifacts.decode_sparse_field(
            read_file_bytes(self.workspace / sparse_rec["artifact_path"])
        )
        field = payload["field"]
        voxels = [
            {
                "voxel": [int(c) for c in coord],
                "color": [float(x) for x in color],
                "weight": float(weight),
            }
            for coord, (color, weight) in sorted(field.items())
        ]
        summary = {
            "dataset": manifest.dataset_name,
            "seed": manifest.seed,
            "views": [view.view_id for view in manifest.views],
            "config": manifest.config,
            "sparsity_stats": payload["stats"],
            "voxel_count": len(voxels),
            "voxels": voxels,
            "digests": self._collect_digests()["_summary"],
        }
        out = self.workspace / "exports" / "summary.json"
        atomic_write_json(out, summary)
        return {"summary_path": str(out), "voxel_count": len(voxels)}
