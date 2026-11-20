"""Workspace, manifest, and deterministic chain planning."""

from __future__ import annotations

import copy
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import chained_fingerprint, sha256_file
from .errors import InvalidRequestError, NotFoundError
from .ops import OperatorSpec, infer_output_shape, normalize_operator
from .storage import FileLock, atomic_write_bytes, atomic_write_json, read_json


@dataclass
class ChainPlan:
    shard_id: str
    tensor_path: Path
    tensor_fingerprint: str
    tensor_shape: tuple[int, ...]
    specs: list[OperatorSpec]
    output_shapes: list[tuple[int, ...]]
    chain_prefixes: list[str]
    config_version: str
    raw_operators: list[dict[str, Any]]

    @property
    def final_shape(self) -> tuple[int, ...]:
        return self.output_shapes[-1]


class Workspace:
    """A file-backed deterministic workbench workspace."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.manifest_path = self.root / "manifest.json"
        self.expectations_path = self.root / "expectations.json"

    def exists(self) -> bool:
        return self.manifest_path.is_file()

    def load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.is_file():
            raise NotFoundError("workspace manifest does not exist", path=str(self.manifest_path))
        document = read_json(self.manifest_path)
        if not isinstance(document, dict):
            raise InvalidRequestError("manifest root must be an object", path=str(self.manifest_path))
        if not isinstance(document.get("config_version"), str) or not document["config_version"]:
            raise InvalidRequestError("manifest requires a non-empty config_version")
        if not isinstance(document.get("shards"), list) or not document["shards"]:
            raise InvalidRequestError("manifest requires a non-empty shards array")
        identifiers = set()
        for index, shard in enumerate(document["shards"]):
            if not isinstance(shard, dict):
                raise InvalidRequestError("shard entry must be an object", shard_position=index)
            shard_id = shard.get("id")
            if not isinstance(shard_id, str) or not shard_id:
                raise InvalidRequestError("shard requires a non-empty id", shard_position=index)
            if shard_id in identifiers:
                raise InvalidRequestError("duplicate shard id", shard_id=shard_id)
            identifiers.add(shard_id)
            if not isinstance(shard.get("tensor"), str) or not shard["tensor"]:
                raise InvalidRequestError("shard requires a tensor path", shard_id=shard_id)
            if not isinstance(shard.get("operators"), list) or not shard["operators"]:
                raise InvalidRequestError("shard requires a non-empty operators array", shard_id=shard_id)
        return document

    def save_manifest(self, document: dict[str, Any]) -> None:
        atomic_write_json(self.manifest_path, document, pretty=True)

    def shard_entry(self, shard_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        manifest = self.load_manifest()
        for shard in manifest["shards"]:
            if shard["id"] == shard_id:
                return manifest, shard
        raise NotFoundError("shard not found", shard_id=shard_id, workspace=str(self.root))

    def list_shards(self) -> list[dict[str, Any]]:
        manifest = self.load_manifest()
        result = []
        for shard in manifest["shards"]:
            path = self.tensor_path(shard)
            result.append(
                {
                    "id": shard["id"],
                    "tensor": shard["tensor"],
                    "tensor_exists": path.is_file(),
                    "operator_count": len(shard["operators"]),
                }
            )
        return result

    def tensor_path(self, shard: dict[str, Any]) -> Path:
        candidate = (self.root / shard["tensor"]).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise InvalidRequestError(
                "tensor path must stay inside the workspace",
                shard_id=shard.get("id"),
                tensor_path=shard.get("tensor"),
            ) from exc
        return candidate

    def load_tensor(self, shard: dict[str, Any]) -> np.ndarray:
        path = self.tensor_path(shard)
        if not path.is_file():
            raise InvalidRequestError(
                "shard tensor file is missing",
                shard_id=shard["id"],
                tensor_path=str(path),
            )
        try:
            array = np.load(path, allow_pickle=False)
        except Exception as exc:  # NumPy raises several format-specific errors.
            raise InvalidRequestError(
                "unable to load shard tensor",
                shard_id=shard["id"],
                tensor_path=str(path),
            ) from exc
        if not isinstance(array, np.ndarray) or array.ndim != 4:
            raise InvalidRequestError(
                "shard tensor must be a rank-4 NHWC .npy array",
                shard_id=shard["id"],
                shape=list(getattr(array, "shape", [])),
            )
        if min(array.shape) < 1:
            raise InvalidRequestError(
                "shard tensor dimensions must be positive",
                shard_id=shard["id"],
                shape=list(array.shape),
            )
        return np.ascontiguousarray(array, dtype=np.float64)

    def load_expectations(self) -> dict[str, Any]:
        if not self.expectations_path.is_file():
            return {"format_version": 1, "shards": {}}
        document = read_json(self.expectations_path)
        if not isinstance(document, dict) or not isinstance(document.get("shards", {}), dict):
            raise InvalidRequestError("expectations must be an object with a shards mapping")
        return document

    def stage_expectations(self, shard_id: str) -> list[dict[str, Any]]:
        document = self.load_expectations()
        shard = document.get("shards", {}).get(shard_id, {})
        stages = shard.get("stages", []) if isinstance(shard, dict) else []
        return stages if isinstance(stages, list) else []

    def plan_shard(self, shard_id: str) -> ChainPlan:
        manifest, shard = self.shard_entry(shard_id)
        tensor_path = self.tensor_path(shard)
        tensor = self.load_tensor(shard)
        tensor_fingerprint = sha256_file(tensor_path)
        config_version = manifest["config_version"]
        raw_operators = shard["operators"]
        specs: list[OperatorSpec] = []
        shapes: list[tuple[int, ...]] = []
        prefixes: list[str] = []
        current_shape = tuple(int(item) for item in tensor.shape)

        # Normalize and statically validate the entire chain before any stage is
        # executed.  A later invalid operator therefore cannot leave a partial
        # chain behind on this invocation.
        for index, raw in enumerate(raw_operators):
            spec = normalize_operator(raw, shard_id, index)
            output_shape = infer_output_shape(spec, current_shape)
            specs.append(spec)
            shapes.append(output_shape)
            current_shape = output_shape

        prefix_seed = chained_fingerprint("chain", config_version, shard_id, tensor_fingerprint)
        for spec in specs:
            prefix_seed = chained_fingerprint("stage", prefix_seed, spec.operator_index, spec.fingerprint)
            prefixes.append(prefix_seed)

        return ChainPlan(
            shard_id=shard_id,
            tensor_path=tensor_path,
            tensor_fingerprint=tensor_fingerprint,
            tensor_shape=tuple(int(item) for item in tensor.shape),
            specs=specs,
            output_shapes=shapes,
            chain_prefixes=prefixes,
            config_version=config_version,
            raw_operators=raw_operators,
        )

    def import_shard(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Copy a local .npy shard into the workspace and append its chain."""

        if not isinstance(payload, dict):
            raise InvalidRequestError("import payload must be an object")
        shard_id = payload.get("id")
        source_path = payload.get("source_path")
        operators = payload.get("operators")
        expectations = payload.get("expectations")
        if not isinstance(shard_id, str) or not shard_id or any(ch in shard_id for ch in "/\\"):
            raise InvalidRequestError("import id must be a simple non-empty file name")
        if not isinstance(source_path, str) or not source_path:
            raise InvalidRequestError("import requires source_path")
        if not isinstance(operators, list) or not operators:
            raise InvalidRequestError("import requires a non-empty operators array")
        source = Path(source_path).expanduser().resolve()
        if not source.is_file():
            raise InvalidRequestError("source .npy file does not exist", source_path=str(source))
        if source.suffix.lower() != ".npy":
            raise InvalidRequestError("import source must have a .npy extension")
        array = np.load(source, allow_pickle=False)
        if not isinstance(array, np.ndarray) or array.ndim != 4:
            raise InvalidRequestError("imported tensor must be a rank-4 NHWC array")

        destination_relative = Path("tensors") / f"{shard_id}.npy"
        destination = self.root / destination_relative
        with FileLock(self.root / ".workspace.lock"):
            manifest = self.load_manifest()
            original_manifest = copy.deepcopy(manifest)
            original_expectations = copy.deepcopy(self.load_expectations())
            if any(shard.get("id") == shard_id for shard in manifest["shards"]):
                raise InvalidRequestError("shard id already exists", shard_id=shard_id)
            try:
                atomic_write_bytes(destination, source.read_bytes())
                manifest["shards"].append(
                    {
                        "id": shard_id,
                        "tensor": destination_relative.as_posix(),
                        "operators": operators,
                    }
                )
                self.save_manifest(manifest)

                if expectations is not None:
                    if not isinstance(expectations, dict):
                        raise InvalidRequestError("expectations must be an object")
                    document = self.load_expectations()
                    document.setdefault("shards", {})[shard_id] = {"stages": expectations.get("stages", [])}
                    atomic_write_json(self.expectations_path, document, pretty=True)

                # Validate the complete imported chain now, but do not execute it.
                self.plan_shard(shard_id)
            except Exception:
                self.save_manifest(original_manifest)
                atomic_write_json(self.expectations_path, original_expectations, pretty=True)
                try:
                    destination.unlink()
                except FileNotFoundError:
                    pass
                raise

        return {
            "shard_id": shard_id,
            "tensor": destination_relative.as_posix(),
            "tensor_shape": list(array.shape),
            "operator_count": len(operators),
        }

    def copy_to(self, destination: Path) -> None:
        destination = destination.resolve()
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(self.root, destination)

