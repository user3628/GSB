"""YAML configuration loading and stage-specific fingerprinting."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from .errors import error
from .utils import canonical_json_bytes, digest_named_files, sha256_bytes


SCHEMA_VERSION = 1
STAGES = {
    1: "tokenize",
    2: "statistics",
    3: "postings",
    4: "query_summary",
}
STAGE_NAMES = {name: number for number, name in STAGES.items()}


class IndexConfig:
    """Validated configuration plus deterministic input discovery."""

    def __init__(self, path: Path, raw: dict[str, Any]):
        self.path = path.resolve()
        self.base_dir = self.path.parent
        self.raw = raw
        self.schema_version = raw.get("schema_version", SCHEMA_VERSION)
        self.index_version = str(raw.get("index_version", "local-v1"))
        self.config_fingerprint = sha256_bytes(canonical_json_bytes(raw))
        self.shard_paths = self._discover_shards()
        self.dataset_digest = digest_named_files(self.shard_paths)
        self.query_path = self._resolve_path(raw.get("queries", "queries.jsonl"))
        self.query_digest = (
            digest_named_files([self.query_path])
            if self.query_path.exists()
            else sha256_bytes(b"")
        )
        self.stopwords = self._normalized_stopwords()

    @classmethod
    def load(cls, path: str | Path) -> "IndexConfig":
        config_path = Path(path)
        try:
            with config_path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle)
        except FileNotFoundError:
            raise error("E_CONFIG", "配置文件不存在", path=str(config_path))
        except (OSError, yaml.YAMLError) as exc:
            raise error("E_CONFIG", "配置文件无法读取", path=str(config_path), reason=str(exc))
        if not isinstance(raw, dict):
            raise error("E_CONFIG", "YAML 顶层必须是映射", path=str(config_path))
        config = cls(config_path, raw)
        config._validate_common()
        return config

    def _resolve_path(self, value: Any) -> Path:
        path = Path(str(value))
        if not path.is_absolute():
            path = self.base_dir / path
        return path.resolve()

    def _discover_shards(self) -> list[Path]:
        pattern = str(self.raw.get("documents", {}).get("glob", "shards/*.jsonl"))
        base = self.base_dir
        if Path(pattern).is_absolute():
            paths = sorted(Path("/").glob(pattern.lstrip("/").replace("\\", "/")))
        else:
            paths = sorted(base.glob(pattern))
        files = [path.resolve() for path in paths if path.is_file()]
        if not files:
            raise error(
                "E_DATA",
                "未发现 JSONL 文档分片",
                glob=pattern,
                base=str(base),
            )
        return files

    def _normalized_stopwords(self) -> list[str]:
        values = self.raw.get("stopwords", [])
        if values is None:
            values = []
        if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
            raise error("E_CONFIG", "stopwords 必须是字符串列表")
        from .tokenizer import normalize_token

        normalized = sorted({normalize_token(item) for item in values if normalize_token(item)})
        return normalized

    def _validate_common(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise error(
                "E_CONFIG",
                "不支持的配置 schema_version",
                expected=SCHEMA_VERSION,
                actual=self.schema_version,
            )
        if not isinstance(self.raw.get("documents", {}), dict):
            raise error("E_CONFIG", "documents 必须是映射")
        if not isinstance(self.raw.get("tokenizer", {}), dict):
            raise error("E_CONFIG", "tokenizer 必须是映射")
        if not isinstance(self.raw.get("bm25", {}), dict):
            raise error("E_CONFIG", "bm25 必须是映射")
        if not isinstance(self.raw.get("postings", {}), dict):
            raise error("E_CONFIG", "postings 必须是映射")
        if not isinstance(self.raw.get("query", {}), dict):
            raise error("E_CONFIG", "query 必须是映射")
        self._number("bm25.k1", default=1.2)
        self._number("bm25.b", default=0.75)
        self._integer("quantization.scale", default=10000)
        self._integer("postings.block_size", default=8)
        self._integer("postings.skip_step", default=4)
        self._integer("query.cache_capacity", default=32)
        self._integer("query.max_limit", default=1000)
        self._number("query.latency_budget_ms", default=1000.0)

    def _number(self, dotted: str, default: float) -> float:
        value = self._lookup(dotted, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise error("E_CONFIG", f"{dotted} 必须是数值", value=value)
        return float(value)

    def _integer(self, dotted: str, default: int) -> int:
        value = self._lookup(dotted, default)
        if isinstance(value, bool) or not isinstance(value, int):
            raise error("E_CONFIG", f"{dotted} 必须是整数", value=value)
        return value

    def _lookup(self, dotted: str, default: Any) -> Any:
        current: Any = self.raw
        for part in dotted.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def value(self, dotted: str, default: Any = None) -> Any:
        return self._lookup(dotted, default)

    def stage_config(self, stage: int) -> dict[str, Any]:
        """Return only the configuration that can affect a stage."""

        if stage == 1:
            return {
                "schema_version": self.schema_version,
                "index_version": self.index_version,
                "tokenizer": copy.deepcopy(self.raw.get("tokenizer", {})),
                "stopwords": list(self.stopwords),
            }
        if stage == 2:
            return {
                "schema_version": self.schema_version,
                "bm25": copy.deepcopy(self.raw.get("bm25", {})),
                "quantization": copy.deepcopy(self.raw.get("quantization", {})),
            }
        if stage == 3:
            return {
                "schema_version": self.schema_version,
                "postings": copy.deepcopy(self.raw.get("postings", {})),
            }
        if stage == 4:
            return {
                "schema_version": self.schema_version,
                "query": copy.deepcopy(self.raw.get("query", {})),
            }
        raise error("E_INVALID_STAGE", "阶段编号非法", stage=stage)

    def stage_input_digest(self, stage: int) -> str:
        if stage == 1:
            return self.dataset_digest
        if stage == 2:
            return self.stage_fingerprint(1)
        if stage == 3:
            return self.stage_fingerprint(2)
        if stage == 4:
            return sha256_bytes(
                canonical_json_bytes(
                    {
                        "upstream": self.stage_fingerprint(3),
                        "queries": self.query_digest,
                    }
                )
            )
        raise error("E_INVALID_STAGE", "阶段编号非法", stage=stage)

    def stage_fingerprint(self, stage: int) -> str:
        """Compute a chain fingerprint before any stage output is built."""

        if stage == 1:
            payload = {
                "schema": "sparse-index-stage-v1",
                "stage": 1,
                "input_digest": self.dataset_digest,
                "config": self.stage_config(1),
            }
            return sha256_bytes(canonical_json_bytes(payload))
        if stage in (2, 3):
            payload = {
                "schema": "sparse-index-stage-v1",
                "stage": stage,
                "upstream_fingerprint": self.stage_fingerprint(stage - 1),
                "config": self.stage_config(stage),
            }
            return sha256_bytes(canonical_json_bytes(payload))
        if stage == 4:
            payload = {
                "schema": "sparse-index-stage-v1",
                "stage": 4,
                "upstream_fingerprint": self.stage_fingerprint(3),
                "query_digest": self.query_digest,
                "config": self.stage_config(4),
            }
            return sha256_bytes(canonical_json_bytes(payload))
        raise error("E_INVALID_STAGE", "阶段编号非法", stage=stage)

    def stage_fingerprints(self) -> dict[str, str]:
        return {STAGES[stage]: self.stage_fingerprint(stage) for stage in range(1, 5)}

    def normalized_snapshot(self) -> dict[str, Any]:
        """Configuration snapshot used to explain invalidations to humans."""

        return {
            "schema_version": self.schema_version,
            "index_version": self.index_version,
            "config": copy.deepcopy(self.raw),
            "stopwords_normalized": list(self.stopwords),
        }

    def config_value(self, dotted: str, default: Any = None) -> Any:
        return self._lookup(dotted, default)

    def require_stage3_parameters(self) -> None:
        skip_step = self._lookup("postings.skip_step", 4)
        block_size = self._lookup("postings.block_size", 8)
        if isinstance(skip_step, bool) or not isinstance(skip_step, int) or skip_step < 1:
            raise error(
                "E_INVALID_SKIP_STEP",
                "跳表步长必须为正整数",
                skip_step=skip_step,
            )
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < 1:
            raise error("E_CONFIG", "分块大小必须为正整数", block_size=block_size)
        if self._lookup("postings.delta_encoding", True) is not True:
            raise error("E_CONFIG", "当前编码器要求启用差值编码")
        if self._lookup("postings.varint_encoding", "unsigned_leb128") != "unsigned_leb128":
            raise error("E_CONFIG", "不支持的变长字节编码", varint=self._lookup("postings.varint_encoding"))

    def require_stage2_parameters(self) -> None:
        scale = self._lookup("quantization.scale", 10000)
        if isinstance(scale, bool) or not isinstance(scale, int) or scale <= 0:
            raise error("E_CONFIG", "量化缩放因子必须为正整数", scale=scale)


def load_config(path: str | Path) -> IndexConfig:
    return IndexConfig.load(path)
