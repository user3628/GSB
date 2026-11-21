"""Deterministic local tensor shard and manifest loading."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from .digest import digest_arrays
from .errors import RopeError


@dataclass
class ShardData:
    shard_id: str
    path: Path
    query: np.ndarray
    key: np.ndarray
    content_fingerprint: str
    metadata: dict[str, Any]

    @property
    def shape(self) -> tuple[int, int, int, int]:
        return tuple(int(value) for value in self.query.shape)  # type: ignore[return-value]

    @property
    def batch(self) -> int:
        return int(self.query.shape[0])

    @property
    def heads(self) -> int:
        return int(self.query.shape[1])

    @property
    def sequence_length(self) -> int:
        return int(self.query.shape[2])

    @property
    def head_dim(self) -> int:
        return int(self.query.shape[3])

    def summary(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "path": str(self.path),
            "shape": list(self.shape),
            "batch": self.batch,
            "heads": self.heads,
            "sequence_length": self.sequence_length,
            "head_dim": self.head_dim,
            "content_fingerprint": self.content_fingerprint,
            "metadata": self.metadata,
        }


def load_shard(path: str | Path, shard_id: str | None = None) -> ShardData:
    target = Path(path)
    if not target.exists():
        raise RopeError("E_NOT_FOUND", "分片文件不存在", {"path": str(target)})
    try:
        with np.load(target, allow_pickle=False) as archive:
            q_name = "q" if "q" in archive.files else "query"
            k_name = "k" if "k" in archive.files else "key"
            missing = [name for name in (q_name, k_name) if name not in archive.files]
            if missing:
                raise RopeError(
                    "E_INVALID_INPUT",
                    "分片缺少查询或键数组（q/query、k/key）",
                    {"path": str(target), "missing": missing, "keys": archive.files},
                )
            query = np.array(archive[q_name], copy=True)
            key = np.array(archive[k_name], copy=True)
    except RopeError:
        raise
    except Exception as exc:
        raise RopeError(
            "E_INVALID_INPUT", f"分片读取失败: {exc}", {"path": str(target)}
        ) from exc

    if query.ndim != 4 or key.ndim != 4:
        raise RopeError(
            "E_INVALID_INPUT",
            "查询与键张量必须为四维 [批量, 头数, 序列长度, 头维度]",
            {"path": str(target), "q_shape": list(query.shape), "k_shape": list(key.shape)},
        )
    if query.shape != key.shape:
        raise RopeError(
            "E_INVALID_INPUT",
            "查询与键张量形状必须一致",
            {"path": str(target), "q_shape": list(query.shape), "k_shape": list(key.shape)},
        )
    if not np.issubdtype(query.dtype, np.floating) or not np.issubdtype(
        key.dtype, np.floating
    ):
        raise RopeError(
            "E_INVALID_INPUT",
            "查询与键张量必须是浮点类型",
            {"path": str(target), "q_dtype": str(query.dtype), "k_dtype": str(key.dtype)},
        )
    if any(dimension <= 0 for dimension in query.shape):
        raise RopeError(
            "E_INVALID_INPUT", "张量所有维度必须大于 0", {"shape": list(query.shape)}
        )
    fingerprint = digest_arrays({"q": query, "k": key})
    resolved_id = shard_id or target.stem
    return ShardData(
        shard_id=resolved_id,
        path=target.resolve(),
        query=query,
        key=key,
        content_fingerprint=fingerprint,
        metadata={},
    )


def _read_manifest(target: Path) -> dict[str, Any]:
    try:
        if target.suffix.lower() in {".yaml", ".yml"}:
            with target.open("r", encoding="utf-8") as handle:
                value = yaml.safe_load(handle)
        else:
            with target.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
    except Exception as exc:
        raise RopeError(
            "E_INVALID_INPUT", f"形状清单读取失败: {exc}", {"path": str(target)}
        ) from exc
    if not isinstance(value, dict) or not isinstance(value.get("shards"), list):
        raise RopeError(
            "E_INVALID_INPUT", "形状清单必须包含 shards 数组", {"path": str(target)}
        )
    return value


def load_manifest(path: str | Path) -> tuple[list[ShardData], dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        raise RopeError("E_NOT_FOUND", "形状清单不存在", {"path": str(target)})
    manifest = _read_manifest(target)
    shards: list[ShardData] = []
    seen: set[str] = set()
    for item in manifest["shards"]:
        if not isinstance(item, dict) or "id" not in item or "file" not in item:
            raise RopeError(
                "E_INVALID_INPUT", "清单项必须包含 id 与 file", {"item": item}
            )
        shard_id = str(item["id"])
        if shard_id in seen:
            raise RopeError("E_INVALID_INPUT", "清单分片标识重复", {"shard_id": shard_id})
        seen.add(shard_id)
        shard_path = Path(str(item["file"]))
        if not shard_path.is_absolute():
            shard_path = target.parent / shard_path
        shard = load_shard(shard_path, shard_id)
        expected_shape = item.get("shape")
        if expected_shape is not None and list(shard.shape) != list(expected_shape):
            raise RopeError(
                "E_INVALID_INPUT",
                "分片实际形状与清单不一致",
                {
                    "shard_id": shard_id,
                    "expected_shape": expected_shape,
                    "actual_shape": list(shard.shape),
                },
            )
        shard.metadata = {
            key: value
            for key, value in item.items()
            if key not in {"id", "file", "shape"}
        }
        shards.append(shard)
    if not shards:
        raise RopeError("E_INVALID_INPUT", "形状清单至少需要一个分片", {"path": str(target)})
    return shards, manifest
