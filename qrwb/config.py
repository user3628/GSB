"""YAML 改写配置的加载、校验与版本指纹。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from .util import canonical_dumps, fingerprint

ROUTES = ("synonym", "glyph", "bridge")
STAGES = ("dispatch", "candidate", "orchestration", "validation")


@dataclass(frozen=True)
class Config:
    raw: Dict[str, Any]

    # ---- 便捷访问器 -----------------------------------------------------
    @property
    def route_cfg(self) -> Dict[str, Any]:
        return dict(self.raw["routes"])

    def route_enabled(self, route: str) -> bool:
        return bool(self.raw["routes"][route].get("enabled", True))

    def route_max(self, route: str) -> int:
        return int(self.raw["routes"][route]["max_candidates"])

    def route_weight(self, route: str) -> float:
        return float(self.raw["routes"][route]["weight"])

    @property
    def budget(self) -> Dict[str, Any]:
        return dict(self.raw["budget"])

    @property
    def adjudication(self) -> Dict[str, Any]:
        return dict(self.raw["adjudication"])

    @property
    def simulation(self) -> Dict[str, Any]:
        return dict(self.raw.get("simulation", {}))

    @property
    def unknown_marker_template(self) -> str:
        return str(self.raw["bridge"].get("unknown_marker", "[UNK:{token}]"))

    @property
    def config_version(self) -> str:
        """业务配置版本指纹：仅覆盖参与阶段产物的配置段。

        simulation 等运行期非版本字段不计入，避免无关调参触发全量失效。
        """
        versioned = {key: self.raw[key] for key in (
            "version", "synonym", "glyph", "bridge", "routes",
            "budget", "adjudication")}
        return fingerprint(versioned)

    def to_dict(self) -> Dict[str, Any]:
        # 深拷贝，避免外部修改
        return yaml.safe_load(canonical_dumps(self.raw))


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("配置文件顶层必须是映射结构")
    cfg = Config(raw=data)
    validate_config(cfg)
    return cfg


def validate_config(cfg: Config) -> None:
    raw = cfg.raw
    for key in ("synonym", "glyph", "bridge", "routes", "budget",
                "adjudication"):
        if key not in raw:
            raise ValueError(f"配置缺少必填段: {key}")
    for route in ROUTES:
        section = raw["routes"].get(route)
        if section is None:
            raise ValueError(f"routes.{route} 缺失")
        if int(section["max_candidates"]) < 0:
            raise ValueError(f"routes.{route}.max_candidates 不能为负")
        float(section["weight"])
    budget = raw["budget"]
    for key in ("global_evaluations", "per_channel_quota",
                "max_concurrent_channels", "per_request_latency_ms"):
        if key not in budget:
            raise ValueError(f"budget.{key} 缺失")
        if float(budget[key]) < 0:
            raise ValueError(f"budget.{key} 不能为负")
    if int(budget["max_concurrent_channels"]) < 1:
        raise ValueError("budget.max_concurrent_channels 必须 >= 1")
    adj = raw["adjudication"]
    if int(adj["max_adoptions_per_query"]) < 1:
        raise ValueError("adjudication.max_adoptions_per_query 必须 >= 1")
    for key in ("log_confidence_weight", "route_weight", "overlap_weight"):
        float(adj[key])


def write_default_config(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    data = DEFAULT_CONFIG
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(data, handle, sort_keys=True, allow_unicode=True,
                       default_flow_style=False)


DEFAULT_CONFIG: Dict[str, Any] = {
    "version": {
        "config_name": "qrwb-demo",
        "dict_versions": {
            "synonym": "syn-v1",
            "glyph": "glyph-v1",
            "bridge": "bridge-v1",
        },
    },
    "synonym": {
        "table_size": 36,
        "seed": 20240101,
        "group_size": 3,
    },
    "glyph": {
        "bucket_count": 16,
        "seed": 20240102,
    },
    "bridge": {
        "seed": 20240103,
        "unknown_marker": "[UNK:{token}]",
    },
    "routes": {
        "synonym": {"enabled": True, "max_candidates": 3, "weight": 0.90},
        "glyph":   {"enabled": True, "max_candidates": 2, "weight": 0.60},
        "bridge":  {"enabled": True, "max_candidates": 2, "weight": 0.75},
    },
    "budget": {
        "global_evaluations": 120,
        "per_channel_quota": 60,
        "max_concurrent_channels": 2,
        "per_request_latency_ms": 50.0,
    },
    "adjudication": {
        "max_adoptions_per_query": 2,
        "log_confidence_weight": 1.0,
        "route_weight": 0.5,
        "overlap_weight": 0.3,
    },
    "simulation": {
        "work_ms": 1,
        "jitter_ms": 2,
        "seed": 77,
    },
}
