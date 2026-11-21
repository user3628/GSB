from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import digest_file, digest_object, read_json, write_json_file
from .errors import WorkbenchError, head_mismatch


@dataclass(frozen=True)
class HeadData:
    head_id: int
    layer_name: str
    head_index: int
    shard_file: str
    local_index: int
    activation: np.ndarray
    query_proj: np.ndarray
    key_proj: np.ndarray

    @property
    def sample_count(self) -> int:
        return int(self.activation.shape[0])

    @property
    def seq_len(self) -> int:
        return int(self.activation.shape[1])

    @property
    def head_dim(self) -> int:
        return int(self.activation.shape[2])


@dataclass(frozen=True)
class Dataset:
    manifest_path: Path
    manifest: dict[str, Any]
    data_digest: str
    heads: tuple[HeadData, ...]

    def by_id(self) -> dict[int, HeadData]:
        return {head.head_id: head for head in self.heads}


def current_manifest_path(root: Path) -> Path:
    root = root.resolve()
    pointer = root / "data" / "current.json"
    if not pointer.exists():
        return root / "data" / "manifest.json"
    payload = read_json(pointer)
    relative = payload.get("manifest_path")
    if not isinstance(relative, str):
        raise WorkbenchError("E_DATA", "data/current.json has no manifest_path")
    path = (root / relative).resolve()
    _ensure_within(root, path)
    return path


def load_dataset(root: Path, manifest_path: Path | None = None) -> Dataset:
    root = root.resolve()
    path = (manifest_path or current_manifest_path(root)).resolve()
    _ensure_within(root, path)
    if not path.is_file():
        raise WorkbenchError("E_DATA", f"manifest not found: {path}")
    manifest = read_json(path)
    if manifest.get("schema") != "headworkbench.manifest/v1":
        raise WorkbenchError("E_DATA", "unsupported manifest schema")
    manifest_without_digest = dict(manifest)
    recorded_digest = manifest_without_digest.pop("manifest_digest", None)
    if recorded_digest != digest_object(manifest_without_digest):
        raise WorkbenchError("E_DATA_INTEGRITY", "manifest digest does not match its contents")

    shards = manifest.get("shards")
    head_entries = manifest.get("heads")
    if not isinstance(shards, list) or not isinstance(head_entries, list):
        raise WorkbenchError("E_DATA", "manifest must contain shard and head lists")

    shard_payloads: dict[str, dict[str, Any]] = {}
    shard_arrays: dict[str, dict[str, np.ndarray]] = {}
    for shard_entry in shards:
        relative = str(shard_entry.get("shard_file", ""))
        shard_path = (path.parent / relative).resolve()
        _ensure_within(root, shard_path)
        if not shard_path.is_file():
            raise WorkbenchError("E_DATA", f"shard not found: {relative}")
        actual_digest = digest_file(shard_path)
        if actual_digest != shard_entry.get("sha256"):
            raise WorkbenchError(
                "E_DATA_INTEGRITY",
                f"shard digest mismatch: {relative}",
                details={"expected": shard_entry.get("sha256"), "actual": actual_digest},
            )
        try:
            with np.load(shard_path, allow_pickle=False) as npz:
                arrays = {name: np.asarray(npz[name]) for name in npz.files}
        except Exception as exc:
            raise WorkbenchError("E_DATA", f"cannot read shard {relative}: {exc}") from exc
        required = {"activations", "query_proj", "key_proj", "head_ids", "layer_names", "head_indices"}
        missing = required.difference(arrays)
        if missing:
            raise WorkbenchError("E_DATA", f"shard {relative} missing arrays", details={"missing": sorted(missing)})
        shard_payloads[relative] = shard_entry
        shard_arrays[relative] = arrays

    heads: list[HeadData] = []
    seen_ids: set[int] = set()
    for entry in head_entries:
        try:
            head_id = int(entry["head_id"])
            relative = str(entry["shard_file"])
            local_index = int(entry["local_index"])
            layer_name = str(entry["layer_name"])
            head_index = int(entry["head_index"])
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkbenchError("E_HEAD_MISMATCH", f"invalid manifest head entry: {exc}") from exc
        if head_id in seen_ids:
            raise WorkbenchError("E_HEAD_MISMATCH", f"duplicate head id {head_id}", details={"head_id": head_id})
        seen_ids.add(head_id)
        arrays = shard_arrays.get(relative)
        if arrays is None:
            raise head_mismatch(
                f"head {head_id} references unknown shard {relative}",
                head_id=head_id,
                shard=relative,
            )
        activation_all = arrays["activations"]
        head_ids_array = arrays["head_ids"]
        if activation_all.ndim != 4:
            raise head_mismatch(
                f"activations must be [samples, heads, seq, dim], got {activation_all.shape}",
                head_id=head_id,
                shard=relative,
            )
        if local_index < 0 or local_index >= activation_all.shape[1]:
            raise head_mismatch(
                f"head id {head_id} local index {local_index} outside shard head list",
                head_id=head_id,
                shard=relative,
                activation_head_dim=int(activation_all.shape[3]),
            )
        if head_id not in head_ids_array.tolist():
            raise head_mismatch(
                f"head id {head_id} is outside the shard head manifest",
                head_id=head_id,
                shard=relative,
                activation_head_dim=int(activation_all.shape[3]),
            )
        listed_index = int(np.flatnonzero(head_ids_array == head_id)[0])
        if listed_index != local_index:
            raise head_mismatch(
                f"head id {head_id} local index {local_index} disagrees with shard list index {listed_index}",
                head_id=head_id,
                shard=relative,
                activation_head_dim=int(activation_all.shape[3]),
            )
        activation = activation_all[:, local_index, :, :]
        if activation.ndim != 3:
            raise head_mismatch(f"head {head_id} activation is not three-dimensional", head_id=head_id, shard=relative)
        activation_dim = int(activation.shape[2])
        query_proj = arrays["query_proj"]
        key_proj = arrays["key_proj"]
        expected_projection = (activation_all.shape[1], activation_dim, activation_dim)
        if tuple(query_proj.shape) != expected_projection:
            raise head_mismatch(
                f"head {head_id} query projection shape {query_proj.shape} does not match activation head dimension {activation_dim}",
                head_id=head_id,
                activation_head_dim=activation_dim,
                projection_shape=query_proj.shape,
                shard=relative,
            )
        if tuple(key_proj.shape) != expected_projection:
            raise head_mismatch(
                f"head {head_id} key projection shape {key_proj.shape} does not match activation head dimension {activation_dim}",
                head_id=head_id,
                activation_head_dim=activation_dim,
                projection_shape=key_proj.shape,
                shard=relative,
            )
        q = np.asarray(query_proj[local_index], dtype=np.float64)
        k = np.asarray(key_proj[local_index], dtype=np.float64)
        if q.shape[0] != activation_dim or q.shape[1] != k.shape[0] or k.shape[1] != activation_dim:
            raise head_mismatch(
                f"head {head_id} projection dimensions are inconsistent",
                head_id=head_id,
                activation_head_dim=activation_dim,
                projection_shape=q.shape,
                shard=relative,
            )
        if not np.isfinite(activation).all() or not np.isfinite(q).all() or not np.isfinite(k).all():
            raise WorkbenchError("E_DATA", f"head {head_id} contains non-finite values")
        heads.append(
            HeadData(
                head_id=head_id,
                layer_name=layer_name,
                head_index=head_index,
                shard_file=relative,
                local_index=local_index,
                activation=np.ascontiguousarray(activation, dtype=np.float64),
                query_proj=np.ascontiguousarray(q, dtype=np.float64),
                key_proj=np.ascontiguousarray(k, dtype=np.float64),
            )
        )

    heads.sort(key=lambda item: item.head_id)
    if [head.head_id for head in heads] != sorted(seen_ids):
        raise WorkbenchError("E_HEAD_MISMATCH", "head manifest is incomplete")
    data_digest = digest_object(
        {
            "manifest_digest": manifest["manifest_digest"],
            "shards": sorted((item["shard_file"], item["sha256"]) for item in shards),
        }
    )
    return Dataset(manifest_path=path, manifest=manifest, data_digest=data_digest, heads=tuple(heads))


def install_dataset(
    root: Path,
    *,
    manifest_path: Path,
    shard_paths: list[Path] | None = None,
    config_path: Path | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    manifest_path = manifest_path.resolve()
    _ensure_within(root, manifest_path)
    source_dataset = load_dataset(root, manifest_path)
    source_manifest = source_dataset.manifest

    selected_shards = sorted({head.shard_file for head in source_dataset.heads})
    source_shard_files: list[Path] = []
    for relative in selected_shards:
        candidate = (manifest_path.parent / relative).resolve()
        _ensure_within(root, candidate)
        if not candidate.is_file():
            raise WorkbenchError("E_DATA", f"shard not found: {relative}")
        source_shard_files.extend([candidate])
    if shard_paths:
        for supplied in shard_paths:
            resolved = supplied.resolve()
            _ensure_within(root, resolved)
            if not resolved.is_file():
                raise WorkbenchError("E_DATA", f"supplied shard not found: {resolved}")
            if resolved not in source_shard_files:
                source_shard_files.append(resolved)

    batch_material = {
        "manifest_digest": source_manifest["manifest_digest"],
        "shards": [(path.name, digest_file(path)) for path in sorted(source_shard_files)],
    }
    batch_digest = digest_object(batch_material)
    target_dir = root / "data" / "imports" / f"batch-{batch_digest[:16]}"
    shard_target = target_dir / "shards"
    shard_target.mkdir(parents=True, exist_ok=True)

    by_name: dict[str, str] = {}
    for index, source in enumerate(sorted(source_shard_files)):
        target_name = source.name
        if target_name in by_name.values():
            target_name = f"{index:02d}-{source.name}"
        target = shard_target / target_name
        if not target.exists() or digest_file(target) != digest_file(source):
            shutil.copy2(source, target)
        by_name[str(source)] = target_name

    source_to_relative = {
        str((manifest_path.parent / entry["shard_file"]).resolve()): f"shards/{by_name[str((manifest_path.parent / entry['shard_file']).resolve())]}"
        for entry in source_manifest["shards"]
        if str((manifest_path.parent / entry["shard_file"]).resolve()) in by_name
    }
    copied_manifest = dict(source_manifest)
    copied_manifest["shards"] = [
        {**entry, "shard_file": source_to_relative[str((manifest_path.parent / entry["shard_file"]).resolve())]}
        for entry in source_manifest["shards"]
        if str((manifest_path.parent / entry["shard_file"]).resolve()) in source_to_relative
    ]
    copied_manifest["heads"] = [
        {**entry, "shard_file": source_to_relative[str((manifest_path.parent / entry["shard_file"]).resolve())]}
        for entry in source_manifest["heads"]
        if str((manifest_path.parent / entry["shard_file"]).resolve()) in source_to_relative
    ]
    copied_manifest.pop("manifest_digest", None)
    copied_manifest["manifest_digest"] = digest_object(copied_manifest)
    target_manifest = target_dir / "manifest.json"
    write_json_file(target_manifest, copied_manifest)

    # Validate the copied layout before publishing the pointer.
    installed = load_dataset(root, target_manifest)
    pointer = root / "data" / "current.json"
    write_json_file(
        pointer,
        {
            "schema": "headworkbench.data-pointer/v1",
            "manifest_path": str(target_manifest.relative_to(root)),
            "manifest_digest": copied_manifest["manifest_digest"],
        },
    )
    config_summary = None
    if config_path is not None:
        from .config import install_config

        config_summary = install_config(root, config_path)
    return {
        "batch_digest": batch_digest,
        "manifest_path": str(target_manifest.relative_to(root)),
        "manifest_digest": copied_manifest["manifest_digest"],
        "data_digest": installed.data_digest,
        "head_count": len(installed.heads),
        "head_ids": [head.head_id for head in installed.heads],
        "shard_count": len(copied_manifest["shards"]),
        "config": config_summary,
    }


def _ensure_within(root: Path, path: Path) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkbenchError(
            "E_PATH",
            "all inputs must reside inside the workspace",
            details={"path": str(path), "workspace": str(root)},
        ) from exc

