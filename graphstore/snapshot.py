"""图快照：版本识别、摘要校验与磁盘加载。

快照 JSON 结构::

    {
      "version": 3,
      "generated_at": "2026-11-23T10:00:00+08:00",
      "nodes": [{"id": ..., "label": ..., "properties": {...}}, ...],
      "edges": [{"source": ..., "target": ..., "type": ..., "weight": ...}, ...],
      "checksum": {"algorithm": "sha256", "value": "..."}
    }

摘要只覆盖确定性的图内容（排序后的节点与边），与生成时间等元数据无关，
保证同一份图在任何环境下摘要一致。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from .errors import SnapshotInvalidError
from .models import Edge, Node

CHECKSUM_ALGORITHM = "sha256"


def compute_checksum(nodes: Iterable[Node], edges: Iterable[Edge]) -> str:
    payload = {
        "nodes": sorted((node.to_dict() for node in nodes), key=lambda item: item["id"]),
        "edges": sorted(
            (edge.to_dict() for edge in edges),
            key=lambda item: (item["source"], item["type"], item["target"]),
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class GraphSnapshot:
    version: int
    generated_at: str
    nodes: dict[str, Node]
    edges: dict[tuple[str, str, str], Edge]
    checksum: str

    @classmethod
    def build(cls, version, generated_at, nodes, edges, checksum=None):
        node_map = {node.id: node for node in nodes}
        edge_map = {(edge.source, edge.type, edge.target): edge for edge in edges}
        if len(node_map) != len(nodes):
            raise SnapshotInvalidError("存在重复节点 id", {"duplicate": "node"})
        if len(edge_map) != len(edges):
            raise SnapshotInvalidError("存在重复边 (source,type,target)", {"duplicate": "edge"})
        actual = compute_checksum(node_map.values(), edge_map.values())
        if checksum is not None and checksum != actual:
            raise SnapshotInvalidError(
                "快照校验摘要不匹配",
                {"expected": checksum, "actual": actual},
            )
        return cls(
            version=version,
            generated_at=generated_at,
            nodes=node_map,
            edges=edge_map,
            checksum=checksum or actual,
        )

    def edge_list(self):
        return list(self.edges.values())

    def outgoing(self, source):
        return [edge for edge in self.edges.values() if edge.source == source]


def parse_snapshot(raw, *, expected_min_version: Optional[int] = None) -> GraphSnapshot:
    """解析并完整校验一份快照字典。

    任何结构、版本、摘要问题都抛 :class:`SnapshotInvalidError`，调用方负责
    保留旧快照，因此本函数不触碰任何已加载状态。
    """
    if not isinstance(raw, dict):
        raise SnapshotInvalidError("快照根节点必须是 JSON 对象", {"reason": "root"})

    version = raw.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise SnapshotInvalidError("版本号必须是正整数", {"version": version})
    if expected_min_version is not None and version <= expected_min_version:
        raise SnapshotInvalidError(
            "版本号必须严格单调递增",
            {"version": version, "required_gt": expected_min_version},
        )

    generated_at = raw.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at:
        raise SnapshotInvalidError("generated_at 必须是非空字符串", {"version": version})

    raw_nodes = raw.get("nodes")
    raw_edges = raw.get("edges")
    if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
        raise SnapshotInvalidError("nodes 与 edges 必须是数组", {"version": version})

    try:
        nodes = [Node.from_dict(item) for item in raw_nodes]
        edges = [Edge.from_dict(item) for item in raw_edges]
    except ValueError as exc:
        raise SnapshotInvalidError(str(exc), {"version": version}) from exc

    node_ids = {node.id for node in nodes}
    dangling = sorted(
        {
            endpoint
            for edge in edges
            for endpoint in (edge.source, edge.target)
            if endpoint not in node_ids
        }
    )
    if dangling:
        raise SnapshotInvalidError(
            "边引用了不存在的节点",
            {"version": version, "missing_nodes": dangling},
        )

    checksum_field = raw.get("checksum") or {}
    if not isinstance(checksum_field, dict):
        raise SnapshotInvalidError("checksum 必须是对象", {"version": version})
    algorithm = checksum_field.get("algorithm")
    value = checksum_field.get("value")
    if algorithm != CHECKSUM_ALGORITHM or not isinstance(value, str):
        raise SnapshotInvalidError(
            f"仅支持 {CHECKSUM_ALGORITHM} 摘要",
            {"version": version, "checksum": checksum_field},
        )

    try:
        return GraphSnapshot.build(version, generated_at, nodes, edges, checksum=value)
    except SnapshotInvalidError as exc:
        exc.context.setdefault("version", version)
        raise


def load_snapshot_file(path, *, expected_min_version: Optional[int] = None) -> GraphSnapshot:
    file_path = Path(path)
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SnapshotInvalidError(
            "快照文件不存在", {"path": str(file_path)}
        ) from exc
    except json.JSONDecodeError as exc:
        raise SnapshotInvalidError(
            "快照文件不是合法 JSON",
            {"path": str(file_path), "detail": exc.msg, "at": exc.pos},
        ) from exc
    return parse_snapshot(raw, expected_min_version=expected_min_version)
