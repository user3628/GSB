"""图数据模型：节点与有向边。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True, slots=True)
class Node:
    id: str
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise ValueError("节点必须是对象")
        node_id = raw.get("id")
        label = raw.get("label")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("节点 id 必须是非空字符串")
        if not isinstance(label, str) or not label:
            raise ValueError(f"节点 {node_id} 的 label 必须是非空字符串")
        properties = raw.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError(f"节点 {node_id} 的 properties 必须是对象")
        return cls(id=node_id, label=label, properties=dict(properties))

    def to_dict(self):
        return {"id": self.id, "label": self.label, "properties": dict(self.properties)}


@dataclass(frozen=True, slots=True)
class Edge:
    source: str
    target: str
    type: str
    weight: float = 1.0

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise ValueError("边必须是对象")
        source = raw.get("source")
        target = raw.get("target")
        edge_type = raw.get("type")
        for name, value in (("source", source), ("target", target), ("type", edge_type)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"边的 {name} 必须是非空字符串: {raw}")
        weight = raw.get("weight", 1.0)
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"边 {source}->{target} 的 weight 必须是数值")
        return cls(source=source, target=target, type=edge_type, weight=float(weight))

    def to_dict(self):
        return {
            "source": self.source,
            "target": self.target,
            "type": self.type,
            "weight": self.weight,
        }
