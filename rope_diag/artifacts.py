"""Immutable, content-addressed stage artifact storage."""

from __future__ import annotations

import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .digest import atomic_write_json, atomic_write_npy, digest_array, digest_arrays, read_json, read_npy
from .errors import RopeError
from .locking import exclusive_file_lock


def safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return cleaned or "item"


@dataclass(frozen=True)
class ArtifactRef:
    stage: str
    shard_id: str
    artifact_digest: str
    path: str
    arrays: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "shard_id": self.shard_id,
            "artifact_digest": self.artifact_digest,
            "path": self.path,
            "arrays": dict(sorted(self.arrays.items())),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactRef":
        return cls(
            stage=str(value["stage"]),
            shard_id=str(value["shard_id"]),
            artifact_digest=str(value["artifact_digest"]),
            path=str(value["path"]),
            arrays={str(key): str(digest) for key, digest in dict(value["arrays"]).items()},
        )


class ArtifactStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / "artifacts"
        self.lock_root = self.workspace / "locks" / "artifacts"

    def put(
        self,
        stage: str,
        shard_id: str,
        arrays: Mapping[str, np.ndarray],
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactRef:
        array_map = {name: np.ascontiguousarray(value) for name, value in arrays.items()}
        artifact_digest = digest_arrays(array_map)
        array_digests = {name: digest_array(value) for name, value in array_map.items()}
        final_dir = (
            self.root
            / safe_component(stage)
            / safe_component(shard_id)
            / artifact_digest
        )
        lock_path = self.lock_root / f"{artifact_digest}.lock"
        with exclusive_file_lock(lock_path):
            if final_dir.exists():
                self._verify_existing(final_dir, artifact_digest)
                return self._ref(stage, shard_id, artifact_digest, array_digests)

            final_dir.parent.mkdir(parents=True, exist_ok=True)
            temp_dir = final_dir.parent / f".tmp-{artifact_digest}-{uuid.uuid4().hex}"
            temp_dir.mkdir(parents=False, exist_ok=False)
            try:
                for name in sorted(array_map):
                    atomic_write_npy(temp_dir / f"{safe_component(name)}.npy", array_map[name])
                manifest = {
                    "schema_version": 1,
                    "stage": stage,
                    "shard_id": shard_id,
                    "artifact_digest": artifact_digest,
                    "arrays": {
                        name: {
                            "file": f"{safe_component(name)}.npy",
                            "digest": array_digests[name],
                            "shape": list(array_map[name].shape),
                            "dtype": array_map[name].dtype.str,
                        }
                        for name in sorted(array_map)
                    },
                    "metadata": dict(metadata or {}),
                }
                atomic_write_json(temp_dir / "manifest.json", manifest)
                try:
                    os.rename(temp_dir, final_dir)
                except OSError:
                    if not final_dir.exists():
                        raise
                    shutil.rmtree(temp_dir, ignore_errors=True)
                self._verify_existing(final_dir, artifact_digest)
            except BaseException:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise
        return self._ref(stage, shard_id, artifact_digest, array_digests)

    def _ref(
        self,
        stage: str,
        shard_id: str,
        artifact_digest: str,
        array_digests: Mapping[str, str],
    ) -> ArtifactRef:
        relative = (
            Path("artifacts")
            / safe_component(stage)
            / safe_component(shard_id)
            / artifact_digest
        )
        return ArtifactRef(
            stage=stage,
            shard_id=shard_id,
            artifact_digest=artifact_digest,
            path=str(relative),
            arrays=dict(sorted(array_digests.items())),
        )

    def _verify_existing(self, directory: Path, expected_digest: str) -> None:
        manifest = read_json(directory / "manifest.json")
        if not manifest or manifest.get("artifact_digest") != expected_digest:
            raise RopeError(
                "E_IO", "内容寻址产物清单损坏", {"path": str(directory)}
            )

    def load(self, ref: ArtifactRef | Mapping[str, Any]) -> dict[str, np.ndarray]:
        resolved = ref if isinstance(ref, ArtifactRef) else ArtifactRef.from_dict(ref)
        directory = self.workspace / resolved.path
        manifest = read_json(directory / "manifest.json")
        if not manifest:
            raise RopeError(
                "E_INVALID_STAGE", "阶段产物缺失", {"artifact": resolved.to_dict()}
            )
        arrays: dict[str, np.ndarray] = {}
        for name, info in sorted(dict(manifest.get("arrays", {})).items()):
            array = read_npy(directory / str(info["file"]))
            if digest_array(array) != str(info["digest"]):
                raise RopeError(
                    "E_IO",
                    "阶段产物数组摘要不匹配",
                    {"path": str(directory), "array": name},
                )
            arrays[name] = array
        actual_digest = digest_arrays(arrays)
        if actual_digest != resolved.artifact_digest:
            raise RopeError(
                "E_IO",
                "阶段产物聚合摘要不匹配",
                {
                    "path": str(directory),
                    "expected": resolved.artifact_digest,
                    "actual": actual_digest,
                },
            )
        return arrays

    def metadata(self, ref: ArtifactRef | Mapping[str, Any]) -> dict[str, Any]:
        resolved = ref if isinstance(ref, ArtifactRef) else ArtifactRef.from_dict(ref)
        manifest = read_json(self.workspace / resolved.path / "manifest.json", default={})
        return dict(manifest.get("metadata", {}))
