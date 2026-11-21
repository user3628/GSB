from __future__ import annotations

import json
import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import BusinessError
from .util import array_digest, canonical_json_bytes, sha256_hex


DEFAULT_SEED = 20261122
DEFAULT_LOSS_SCALE = 64.0
SHARD_SPECS = (
    ("s0", (16,), 16),
    ("s1", (8, 3), 24),
    ("s2", (12,), 12),
    ("s3", (5, 4), 20),
)
PARTIAL_SHARD_SPEC = ("s_partial", (4,), 7)


@dataclass(frozen=True)
class LoadedShard:
    shard_id: str
    source_file: str
    master: np.ndarray
    fp16_copy: np.ndarray
    momentum: np.ndarray
    fp16_digest: str
    master_digest: str
    momentum_digest: str
    full: bool
    written: int
    capacity: int


def _rng_for(seed: int, shard_id: str) -> np.random.Generator:
    # PCG64 accepts a SeedSequence generated from stable integer material.
    material = int.from_bytes(sha256_hex(f"{seed}:{shard_id}".encode())[:16].encode(), "big")
    return np.random.Generator(np.random.PCG64(material))


def make_sample_arrays(seed: int, shard_id: str, shape: tuple[int, ...]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = _rng_for(seed, shard_id)
    master = rng.uniform(-0.75, 0.75, size=shape).astype(np.float32)
    # Make a few values exactly representable to exercise zero-error points.
    flat = master.reshape(-1)
    if flat.size:
        flat[0] = np.float32(0.125)
        if flat.size > 1:
            flat[1] = np.float32(-0.25)
    # The on-disk FP16 copy is the scaled training representation. The loss
    # scale is deliberately part of the deterministic sample contract.
    scaled = master.astype(np.float64) * DEFAULT_LOSS_SCALE
    fp16_copy = scaled.astype(np.float16)
    momentum = rng.normal(0.0, 0.05, size=shape).astype(np.float32)
    return master, fp16_copy, momentum


def _digests(master: np.ndarray, fp16_copy: np.ndarray, momentum: np.ndarray) -> dict[str, str]:
    return {
        "fp16": array_digest(fp16_copy, declared_dtype="float16"),
        "fp32_master": array_digest(master, declared_dtype="float32"),
        "momentum": array_digest(momentum, declared_dtype="float32"),
    }


def _save_npz_deterministic(path: Path, **arrays: np.ndarray) -> None:
    """Write an NPZ with fixed archive timestamps and stable member ordering."""

    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(arrays):
            buffer = io.BytesIO()
            np.lib.format.write_array(
                buffer,
                np.asanyarray(arrays[name]),
                allow_pickle=False,
            )
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o600 << 16
            archive.writestr(info, buffer.getvalue())


def generate_sample_workspace(
    output_dir: str | Path,
    *,
    seed: int = DEFAULT_SEED,
    loss_scale: float = DEFAULT_LOSS_SCALE,
    overwrite: bool = True,
) -> dict[str, Any]:
    root = Path(output_dir)
    shard_dir = root / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    if manifest_path.exists() and not overwrite:
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    shards: dict[str, Any] = {}
    for shard_id, shape, capacity in SHARD_SPECS:
        master, fp16_copy, momentum = make_sample_arrays(seed, shard_id, shape)
        path = shard_dir / f"{shard_id}.npz"
        _save_npz_deterministic(
            path,
            master=master,
            fp16_copy=fp16_copy,
            momentum=momentum,
            written=np.asarray(capacity, dtype=np.int64),
            capacity=np.asarray(capacity, dtype=np.int64),
            full=np.asarray(True),
        )
        shards[shard_id] = {
            "path": path.relative_to(root).as_posix(),
            "shape": list(shape),
            "numel": int(master.size),
            "capacity": capacity,
            "written": capacity,
            "full": True,
            "dtype": "float32",
            "digests": _digests(master, fp16_copy, momentum),
        }

    partial_id, partial_shape, partial_capacity = PARTIAL_SHARD_SPEC
    rng = _rng_for(seed, partial_id)
    master = rng.uniform(-0.5, 0.5, size=partial_shape).astype(np.float32)
    flat = master.reshape(-1)
    written = max(0, flat.size - 2)
    # Allocate the full capacity but persist the number of live elements; the
    # unused tail is deterministic and must never enter a formal checkpoint.
    if written < flat.size:
        flat[written:] = np.nan
    fp16_copy = (master.astype(np.float64) * loss_scale).astype(np.float16)
    momentum = rng.normal(0.0, 0.05, size=partial_shape).astype(np.float32)
    partial_path = shard_dir / f"{partial_id}.npz"
    _save_npz_deterministic(
        partial_path,
        master=master,
        fp16_copy=fp16_copy,
        momentum=momentum,
        written=np.asarray(written, dtype=np.int64),
        capacity=np.asarray(partial_capacity, dtype=np.int64),
        full=np.asarray(False),
    )
    # Include a deliberately unsafe digest so a broken client that ignores the
    # full flag can be detected as partial before checksum validation.
    partial_digests = {
        "fp16": array_digest(fp16_copy[:written], declared_dtype="float16"),
        "fp32_master": array_digest(master.flatten()[:written], declared_dtype="float32"),
        "momentum": array_digest(momentum.flatten()[:written], declared_dtype="float32"),
    }
    shards[partial_id] = {
        "path": partial_path.relative_to(root).as_posix(),
        "shape": list(partial_shape),
        "numel": int(master.size),
        "capacity": partial_capacity,
        "written": written,
        "full": False,
        "dtype": "float32",
        "digests": partial_digests,
    }

    manifest: dict[str, Any] = {
        "format_version": 1,
        "seed": seed,
        "loss_scale": loss_scale,
        "generator": "pcg64_uniform_normal_v1",
        "shards": shards,
        "formal_shard_ids": [shard_id for shard_id, _, _ in SHARD_SPECS],
        "partial_shard_ids": [partial_id],
    }
    manifest["manifest_digest"] = sha256_hex(canonical_json_bytes(manifest))
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


class SampleCatalog:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.manifest_path = self.root / "manifest.json"
        if not self.manifest_path.exists():
            raise BusinessError(
                "E_SAMPLE_INTEGRITY",
                "sample manifest does not exist",
                details={"path": str(self.manifest_path)},
            )
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.shards: dict[str, Any] = self.manifest.get("shards", {})
        self.formal_shard_ids = tuple(self.manifest.get("formal_shard_ids", []))

    def shard_metadata(self, shard_id: str) -> dict[str, Any]:
        metadata = self.shards.get(shard_id)
        if metadata is None:
            raise BusinessError(
                "E_UNKNOWN_SHARD",
                f"unknown shard: {shard_id}",
                details={"shard_id": shard_id},
            )
        return metadata

    def load_shard(self, shard_id: str) -> LoadedShard:
        metadata = self.shard_metadata(shard_id)
        if not bool(metadata.get("full", False)):
            raise BusinessError(
                "E_SHARD_NOT_FULL",
                "partial shards cannot enter a formal checkpoint manifest",
                details={
                    "shard_id": shard_id,
                    "written": metadata.get("written"),
                    "capacity": metadata.get("capacity"),
                },
            )
        path = (self.root / str(metadata["path"])).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise BusinessError("E_SAMPLE_INTEGRITY", "sample path escapes sample root") from exc
        if not path.exists():
            raise BusinessError(
                "E_SAMPLE_INTEGRITY",
                "sample shard file is missing",
                details={"shard_id": shard_id, "path": str(path)},
            )
        with np.load(path, allow_pickle=False) as archive:
            try:
                master = np.asarray(archive["master"], dtype=np.float32)
                fp16_copy = np.asarray(archive["fp16_copy"], dtype=np.float16)
                momentum = np.asarray(archive["momentum"], dtype=np.float32)
                written = int(np.asarray(archive["written"]).item())
                capacity = int(np.asarray(archive["capacity"]).item())
                full = bool(np.asarray(archive["full"]).item())
            except (KeyError, ValueError, TypeError) as exc:
                raise BusinessError(
                    "E_SAMPLE_INTEGRITY",
                    "sample shard is malformed",
                    details={"shard_id": shard_id},
                ) from exc
        if not full or written < capacity:
            raise BusinessError(
                "E_SHARD_NOT_FULL",
                "partial shards cannot enter a formal checkpoint manifest",
                details={"shard_id": shard_id, "written": written, "capacity": capacity},
            )
        expected_shape = tuple(metadata.get("shape", master.shape))
        if master.shape != expected_shape or fp16_copy.shape != expected_shape or momentum.shape != expected_shape:
            raise BusinessError(
                "E_SAMPLE_INTEGRITY",
                "sample shard shape does not match manifest",
                details={"shard_id": shard_id},
            )
        if not (
            np.all(np.isfinite(master))
            and np.all(np.isfinite(fp16_copy.astype(np.float32)))
            and np.all(np.isfinite(momentum))
        ):
            raise BusinessError(
                "E_SAMPLE_INTEGRITY",
                "sample shard contains non-finite values",
                details={"shard_id": shard_id},
            )
        actual = _digests(master, fp16_copy, momentum)
        expected = metadata.get("digests", {})
        if actual != {
            "fp16": expected.get("fp16"),
            "fp32_master": expected.get("fp32_master"),
            "momentum": expected.get("momentum"),
        }:
            raise BusinessError(
                "E_CHECKSUM_MISMATCH",
                "sample shard checksum does not match the deterministic manifest",
                details={"shard_id": shard_id, "actual": actual, "expected": expected},
            )
        return LoadedShard(
            shard_id=shard_id,
            source_file=str(metadata["path"]),
            master=master,
            fp16_copy=fp16_copy,
            momentum=momentum,
            fp16_digest=actual["fp16"],
            master_digest=actual["fp32_master"],
            momentum_digest=actual["momentum"],
            full=True,
            written=written,
            capacity=capacity,
        )
