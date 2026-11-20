"""配置快照与组件声明的数据模型及校验摘要算法。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

SNAPSHOT_REQUIRED_FIELDS = ("version", "generated_at", "config_items", "checksum")


def compute_checksum(config_items: dict[str, Any]) -> str:
    """对配置项集合做规范化序列化后取 SHA-256，作为校验摘要。

    规范化保证键顺序、空白差异不影响摘要，样本与实现共用同一算法。
    """
    canonical = json.dumps(config_items, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Snapshot:
    version: int
    generated_at: str
    config_items: dict[str, Any]
    checksum: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Snapshot":
        missing = [name for name in SNAPSHOT_REQUIRED_FIELDS if name not in raw]
        if missing:
            raise ValueError(f"快照缺少必填字段: {', '.join(missing)}")
        version = raw["version"]
        if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
            raise ValueError("快照版本号必须为正整数")
        generated_at = raw["generated_at"]
        if not isinstance(generated_at, str) or not generated_at.strip():
            raise ValueError("generated_at 必须为非空字符串")
        config_items = raw["config_items"]
        if not isinstance(config_items, dict):
            raise ValueError("config_items 必须为对象")
        checksum = raw["checksum"]
        if not isinstance(checksum, str) or not checksum.strip():
            raise ValueError("checksum 必须为非空字符串")
        return cls(
            version=version,
            generated_at=generated_at,
            config_items=config_items,
            checksum=checksum,
        )

    def checksum_valid(self) -> bool:
        return compute_checksum(self.config_items) == self.checksum

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "config_items": self.config_items,
            "checksum": self.checksum,
        }


@dataclass(frozen=True)
class ComponentDeclaration:
    component_id: str
    depends_on_keys: tuple[str, ...]
    expected_version: int
    endpoint: str
    depends_on_components: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ComponentDeclaration":
        for name in ("component_id", "depends_on_keys", "expected_version", "endpoint"):
            if name not in raw:
                raise ValueError(f"组件声明缺少必填字段: {name}")
        component_id = str(raw["component_id"])
        depends_on_keys = tuple(str(key) for key in raw["depends_on_keys"])
        endpoint = str(raw["endpoint"])
        if not component_id or not endpoint:
            raise ValueError("component_id 与 endpoint 不能为空")
        expected_version = raw["expected_version"]
        if not isinstance(expected_version, int) or isinstance(expected_version, bool) or expected_version < 0:
            raise ValueError("expected_version 必须为非负整数")
        deps = tuple(str(dep) for dep in raw.get("depends_on_components", ()))
        return cls(
            component_id=component_id,
            depends_on_keys=depends_on_keys,
            expected_version=expected_version,
            endpoint=endpoint,
            depends_on_components=deps,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "depends_on_keys": list(self.depends_on_keys),
            "expected_version": self.expected_version,
            "endpoint": self.endpoint,
            "depends_on_components": list(self.depends_on_components),
        }


def load_declarations(raw_list: list[dict[str, Any]]) -> dict[str, ComponentDeclaration]:
    declarations: dict[str, ComponentDeclaration] = {}
    for raw in raw_list:
        declaration = ComponentDeclaration.from_dict(raw)
        if declaration.component_id in declarations:
            raise ValueError(f"组件标识重复: {declaration.component_id}")
        declarations[declaration.component_id] = declaration
    for declaration in declarations.values():
        unknown = [dep for dep in declaration.depends_on_components if dep not in declarations]
        if unknown:
            raise ValueError(
                f"组件 {declaration.component_id} 依赖了未声明的组件: {', '.join(unknown)}"
            )
    return declarations
