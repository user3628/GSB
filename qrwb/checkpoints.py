"""阶段检查点持久化、真实文件锁、序号连续性与版本失效传播。"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from .config import STAGES
from .errors import (E_FINGERPRINT_MISMATCH, E_INVALID_STAGE,
                     E_STALE_CHECKPOINT, make_error)
from .util import FileLock, append_text, canonical_dumps, fingerprint

STAGE_SEQ = {stage: idx + 1 for idx, stage in enumerate(STAGES)}
STAGE_FP_FIELD = {
    "dispatch": "plan_fingerprint",
    "candidate": "candidates_fingerprint",
    "orchestration": "orchestration_fingerprint",
    "validation": "validation_fingerprint",
}
# 每个阶段产物所依赖的输入指纹（用于最早受影响阶段定位）
STAGE_INPUT_FIELDS = {
    "dispatch": ("input_fingerprint", "normalization_fingerprint"),
    "candidate": ("input_fingerprint", "normalization_fingerprint",
                  "synonym_fingerprint", "glyph_fingerprint",
                  "bridge_fingerprint", "routes_fingerprint"),
    "orchestration": ("input_fingerprint", "budget_fingerprint"),
    "validation": ("input_fingerprint", "adjudication_fingerprint"),
}


class CheckpointStore:
    """每个查询一条追加式 JSONL 检查点日志 + 按查询索引。"""

    def __init__(self, workspace: str) -> None:
        self.workspace = workspace
        self.cp_dir = os.path.join(workspace, "checkpoints")
        self.lock_dir = os.path.join(workspace, "locks")
        self.state_dir = os.path.join(workspace, "state")
        for directory in (self.cp_dir, self.lock_dir, self.state_dir):
            os.makedirs(directory, exist_ok=True)
        self.global_lock = os.path.join(self.lock_dir, "checkpoints.lock")
        self._records: Dict[str, List[Dict[str, Any]]] = {}
        self._by_id: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._load()

    # ---- 加载与索引 -----------------------------------------------------
    def _log_path(self, query_id: str) -> str:
        return os.path.join(self.cp_dir, f"{query_id}.jsonl")

    def _load(self) -> None:
        if not os.path.isdir(self.cp_dir):
            return
        for name in sorted(os.listdir(self.cp_dir)):
            if not name.endswith(".jsonl"):
                continue
            path = os.path.join(self.cp_dir, name)
            query_id = name[:-len(".jsonl")]
            records: List[Dict[str, Any]] = []
            with open(path, "r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)  # 原子写保证不会出现半写行
                    records.append(record)
                    self._by_id[(query_id, record["stage"])] = record
            self._records[query_id] = records

    # ---- 查询接口 -------------------------------------------------------
    def get(self, query_id: str, stage: str) -> Optional[Dict[str, Any]]:
        return self._by_id.get((query_id, stage))

    def latest_seq(self, query_id: str) -> int:
        records = self._records.get(query_id)
        if not records:
            return 0
        return max(int(r["seq"]) for r in records)

    def completed_stages(self, query_id: str) -> List[str]:
        return [r["stage"] for r in self._records.get(query_id, [])]

    def all_query_ids(self) -> List[str]:
        return sorted(self._records.keys())

    def stage_versions(self, query_id: str) -> Dict[str, Any]:
        """供状态接口观测的版本指纹与失效信息。"""
        out: Dict[str, Any] = {}
        for stage in STAGES:
            record = self.get(query_id, stage)
            if record is None:
                out[stage] = None
                continue
            out[stage] = {
                "seq": record["seq"],
                "config_version": record["config_version"],
                "input_fingerprint": record["input_fingerprint"],
                "product_fingerprint": record["product_fingerprint"],
                "product_digest": record["product_digest"],
                "stage_fingerprints": record["stage_fingerprints"],
            }
        return out

    # ---- 发布（带真实文件锁 + 序号校验） --------------------------------
    def publish(self, query_id: str, stage: str, *,
                input_fingerprint: str, config_version: str,
                product: Dict[str, Any],
                stage_fingerprints: Dict[str, str],
                expect_fingerprints: Optional[Dict[str, str]] = None
                ) -> Dict[str, Any]:
        """原子发布一条阶段检查点。

        校验规则：
        * 阶段必须合法且前置阶段存在，否则 E_INVALID_STAGE；
        * 同阶段重复/落后序号发布返回 E_STALE_CHECKPOINT；
        * 期望指纹与既有记录不符返回 E_FINGERPRINT_MISMATCH；
        全部通过后在全局文件锁内追加原子写。
        """
        if stage not in STAGE_SEQ:
            raise make_error(E_INVALID_STAGE, f"未知阶段: {stage}",
                             query_id=query_id, stage=stage)
        with FileLock(self.global_lock):
            current_seq = self.latest_seq(query_id)
            existing = self.get(query_id, stage)
            # 同阶段重复发布（无论序号大小）一律视为落后/重复
            if existing is not None:
                raise make_error(
                    E_STALE_CHECKPOINT,
                    f"阶段 {stage} 已存在（当前序号 {current_seq}），重复/落后发布被拒",
                    query_id=query_id, stage=stage,
                    current_seq=current_seq,
                    attempted_stage_order=STAGE_SEQ[stage],
                    existing_product=existing.get("product_fingerprint"))
            # 阶段顺序只能向前：新阶段的固定序必须大于已发布最大阶段序
            max_stage_order = max(
                (STAGE_SEQ[r["stage"]]
                 for r in self._records.get(query_id, [])), default=0)
            if STAGE_SEQ[stage] <= max_stage_order:
                raise make_error(
                    E_INVALID_STAGE,
                    f"阶段跳转非法：已发布至阶段序 {max_stage_order}，"
                    f"试图回退发布阶段 {stage}(序 {STAGE_SEQ[stage]})",
                    query_id=query_id, stage=stage,
                    current_seq=current_seq,
                    attempted_stage_order=STAGE_SEQ[stage])
            # 序号按查询内单调连续推进，不依赖固定阶段编号
            target_seq = current_seq + 1
            if expect_fingerprints:
                for field_name, expected in expect_fingerprints.items():
                    record_field = stage_fingerprints.get(field_name)
                    if record_field is not None and record_field != expected:
                        raise make_error(
                            E_FINGERPRINT_MISMATCH,
                            f"指纹 {field_name} 不匹配，禁止以旧产物冒充新结果",
                            query_id=query_id, stage=stage,
                            field=field_name,
                            expected=expected, actual=record_field)

            record = {
                "query_id": query_id,
                "stage": stage,
                "seq": target_seq,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "stage_fingerprints": dict(sorted(stage_fingerprints.items())),
                "product_fingerprint": fingerprint(product),
                "product_digest": _product_digest(product),
                "product": product,
            }
            path = self._log_path(query_id)
            append_text(path, canonical_dumps(record))
            self._records.setdefault(query_id, []).append(record)
            self._by_id[(query_id, stage)] = record
            return record

    # ---- 版本失效传播 ---------------------------------------------------
    def expected_stage_fingerprints(self, query_id: str,
                                    version_bundle: Dict[str, str]
                                    ) -> Dict[str, Dict[str, str]]:
        """计算当前版本下每个阶段应有的输入指纹集合。"""
        out: Dict[str, Dict[str, str]] = {}
        for stage, fields in STAGE_INPUT_FIELDS.items():
            out[stage] = {field: version_bundle[field] for field in fields}
        return out

    def first_invalid_stage(self, query_id: str,
                            version_bundle: Dict[str, str]
                            ) -> Optional[Tuple[str, Dict[str, str]]]:
        """返回最早受影响阶段及其不匹配的指纹原因；全部有效返回 None。"""
        expected = self.expected_stage_fingerprints(query_id, version_bundle)
        for stage in STAGES:
            record = self.get(query_id, stage)
            if record is None:
                return stage, expected[stage]
            actual = record["stage_fingerprints"]
            mismatched = {field: {"expected": expected[stage][field],
                                  "actual": actual.get(field)}
                          for field in expected[stage]
                          if actual.get(field) != expected[stage][field]}
            if mismatched:
                return stage, mismatched
        return None

    def invalidate_from(self, query_id: str, stage: str,
                        reason: Dict[str, Any]) -> List[str]:
        """使某阶段及其全部下游检查点失效（删除日志并重写）。"""
        cutoff = STAGE_SEQ[stage]
        kept = [r for r in self._records.get(query_id, []) if r["seq"] < cutoff]
        removed = [r["stage"] for r in self._records.get(query_id, [])
                   if r["seq"] >= cutoff]
        with FileLock(self.global_lock):
            # 物理日志截断重写（kept 为空则删除）；内存索引同步清理，
            # 使后续发布按“当前最大序号”连续推进，不会误判非法阶段跳转。
            path = self._log_path(query_id)
            if kept:
                with open(path, "w", encoding="utf-8", newline="\n") as handle:
                    for record in kept:
                        handle.write(canonical_dumps(record) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            elif os.path.exists(path):
                os.remove(path)
            self._records[query_id] = kept
            for gone_stage in removed:
                self._by_id.pop((query_id, gone_stage), None)
            event = {
                "query_id": query_id,
                "invalidated_from": stage,
                "removed_stages": removed,
                "reason": reason,
            }
            append_text(os.path.join(self.state_dir, "invalidations.jsonl"),
                        canonical_dumps(event))
        return removed

    def reset_all(self) -> None:
        with FileLock(self.global_lock):
            for query_id in list(self._records.keys()):
                path = self._log_path(query_id)
                if os.path.exists(path):
                    os.remove(path)
            self._records.clear()
            self._by_id.clear()


def _product_digest(product: Dict[str, Any]) -> Dict[str, Any]:
    """产物摘要：保留关键标量，避免接口读取整份候选。"""
    digest: Dict[str, Any] = {"keys": sorted(product.keys())}
    for key in ("status", "query_id", "candidates_fingerprint",
                "plan_fingerprint", "orchestration_fingerprint",
                "validation_fingerprint"):
        if key in product:
            digest[key] = product[key]
    if "candidates" in product:
        digest["candidate_count"] = len(product["candidates"])
    if "adoptions" in product:
        digest["adoption_count"] = len(product["adoptions"])
    return digest
