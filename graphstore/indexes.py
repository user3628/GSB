"""节点标签倒排索引与边类型邻接索引。

* ``label_index``：``标签 -> 节点标识集合``。
* ``adj_index``：``(边类型, 源节点) -> {目标节点: 权重}``，对外暴露为按目标
  标识稳定排序的 ``(目标, 权重)`` 列表。

索引绑定快照版本，随快照一起不可变替换；增量更新时只复制并修改受影响的键，
并返回受影响的依赖键集合，供缓存级联失效使用。空条目在修改后立即清理。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Set, Tuple

from .snapshot import GraphSnapshot

LABEL_DEP_PREFIX = "label:"
ADJ_DEP_PREFIX = "adj:"


def label_dependency(label: str) -> str:
    return f"{LABEL_DEP_PREFIX}{label}"


def adj_dependency(edge_type: str, source: str) -> str:
    return f"{ADJ_DEP_PREFIX}{edge_type}:{source}"


@dataclass(frozen=True, slots=True)
class IndexBundle:
    version: int
    label_index: Dict[str, Set[str]]
    adj_index: Dict[Tuple[str, str], Dict[str, float]]
    reverse_refs: Dict[str, Set[str]] = field(default_factory=dict)

    @classmethod
    def from_snapshot(cls, snapshot: GraphSnapshot) -> "IndexBundle":
        label_index: Dict[str, Set[str]] = {}
        adj_index: Dict[Tuple[str, str], Dict[str, float]] = {}
        for node in snapshot.nodes.values():
            label_index.setdefault(node.label, set()).add(node.id)
        for edge in snapshot.edges.values():
            adj_index.setdefault((edge.type, edge.source), {})[edge.target] = edge.weight
        reverse_refs = cls._build_reverse_refs(label_index, adj_index)
        return cls(snapshot.version, label_index, adj_index, reverse_refs)

    @staticmethod
    def _build_reverse_refs(label_index, adj_index) -> Dict[str, Set[str]]:
        refs: Dict[str, Set[str]] = {}
        for label, node_ids in label_index.items():
            refs.setdefault(label_dependency(label), set()).update(
                f"node:{node_id}" for node_id in node_ids
            )
        for (edge_type, source), targets in adj_index.items():
            refs.setdefault(adj_dependency(edge_type, source), set()).update(
                f"edge:{source}->{target}:{edge_type}" for target in targets
            )
        return refs

    def lookup_label(self, label: str) -> List[str]:
        return sorted(self.label_index.get(label, ()))

    def lookup_adjacency(self, edge_type: str, source: str) -> List[Tuple[str, float]]:
        targets = self.adj_index.get((edge_type, source), {})
        return sorted(targets.items(), key=lambda item: item[0])

    def outgoing_types(self, source: str) -> List[str]:
        return sorted(edge_type for (edge_type, src) in self.adj_index if src == source)

    def apply_delta(
        self,
        version: int,
        *,
        nodes_added: Dict[str, object],
        nodes_removed: Dict[str, str],
        edges_added: List[object],
        edges_removed: List[object],
    ) -> Tuple["IndexBundle", FrozenSet[str]]:
        """在新索引上增量应用变更，返回 (新索引, 受影响依赖键)。

        ``nodes_removed`` 映射 ``节点 id -> 旧标签``；入边/出边由调用方提前
        展开为 ``edges_removed``。本方法只触碰受影响的标签与邻接键。
        """
        label_index = {label: set(ids) for label, ids in self.label_index.items()}
        adj_index = {key: dict(targets) for key, targets in self.adj_index.items()}
        affected: Set[str] = set()

        for node_id, old_label in nodes_removed.items():
            entry = label_index.get(old_label)
            if entry is not None:
                entry.discard(node_id)
                affected.add(label_dependency(old_label))
                if not entry:
                    del label_index[old_label]

        for node_id, node in nodes_added.items():
            label_index.setdefault(node.label, set()).add(node_id)
            affected.add(label_dependency(node.label))

        for edge in edges_removed:
            key = (edge.type, edge.source)
            targets = adj_index.get(key)
            if targets is not None:
                targets.pop(edge.target, None)
                affected.add(adj_dependency(edge.type, edge.source))
                if not targets:
                    del adj_index[key]

        for edge in edges_added:
            key = (edge.type, edge.source)
            adj_index.setdefault(key, {})[edge.target] = edge.weight
            affected.add(adj_dependency(edge.type, edge.source))

        bundle = IndexBundle(
            version=version,
            label_index=label_index,
            adj_index=adj_index,
            reverse_refs=self._build_reverse_refs(label_index, adj_index),
        )
        return bundle, frozenset(affected)

    def labels(self) -> List[str]:
        return sorted(self.label_index)

    def adjacency_keys(self) -> List[Tuple[str, str]]:
        return sorted(self.adj_index)
