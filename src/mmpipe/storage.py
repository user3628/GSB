"""工作区存储：检查点、原子发布、序号保护、失效传播。

目录布局：
    workspace/
      config.json                 当前管线配置
      inputs/texts.jsonl          已导入文本样本
      inputs/images.json          已导入图像描述样本
      checkpoints/cp_NNNNNN.json  不可变检查点记录（信封含 checksum）
      products/<stage>_NNNNNN.json 阶段产物（不可变）
      current/<stage>.json        当前有效产物（原子替换）
      state.json                  当前阶段状态（可由检查点重建的缓存）
      logs/events.jsonl           运行事件日志（追加写）
      logs/invalidations.jsonl    失效事件日志（追加写）
      locks/workspace.lock        跨进程文件锁
      stop.flag                   停止请求标志（存在即请求停止）
"""

import json
import os
import time
from typing import Any

from . import STAGES
from .codec import canonical_dumps, digest_bytes
from .errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    PipeError,
)
from .platform_io import (
    append_jsonl,
    atomic_write,
    cleanup_tmp_files,
    file_lock,
    read_jsonl,
)

CP_DIR = "checkpoints"
PRODUCT_DIR = "products"
CURRENT_DIR = "current"
LOCK_PATH = "locks/workspace.lock"
STATE_PATH = "state.json"
CONFIG_PATH = "config.json"
INPUTS_DIR = "inputs"
LOGS_DIR = "logs"
STOP_FLAG = "stop.flag"

CP_FIELDS = ("seq", "stage", "input_fingerprint", "config_version",
             "product_digest", "product_path", "created_at", "checksum")


class Workspace:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.lock_path = os.path.join(self.root, LOCK_PATH)
        os.makedirs(os.path.join(self.root, CP_DIR), exist_ok=True)
        os.makedirs(os.path.join(self.root, PRODUCT_DIR), exist_ok=True)
        os.makedirs(os.path.join(self.root, CURRENT_DIR), exist_ok=True)
        os.makedirs(os.path.join(self.root, INPUTS_DIR), exist_ok=True)
        os.makedirs(os.path.join(self.root, LOGS_DIR), exist_ok=True)
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        self.cleaned_tmp: list[str] = []
        self.quarantined: list[str] = []
        self._heal_and_rebuild()

    def cleanup_stale_tmp(self) -> list[str]:
        """显式清理遗留临时文件（仅在确认无并发写入时调用）。"""
        self.cleaned_tmp = (
            cleanup_tmp_files(os.path.join(self.root, CP_DIR))
            + cleanup_tmp_files(os.path.join(self.root, CURRENT_DIR))
            + cleanup_tmp_files(os.path.join(self.root, PRODUCT_DIR))
        )
        return self.cleaned_tmp

    # ---------- 路径与配置 ----------
    def path(self, *parts: str) -> str:
        return os.path.join(self.root, *parts)

    def save_config(self, config: dict) -> None:
        atomic_write(self.path(CONFIG_PATH), canonical_dumps(config))

    def load_config(self) -> dict:
        with open(self.path(CONFIG_PATH), "rb") as handle:
            return json.loads(handle.read())

    def config_exists(self) -> bool:
        return os.path.exists(self.path(CONFIG_PATH))

    # ---------- 事件与停止 ----------
    def log_event(self, event: str, **fields: Any) -> None:
        record = {"ts": time.time_ns(), "event": event, **fields}
        append_jsonl(self.path(LOGS_DIR, "events.jsonl"), record)

    def events(self) -> list[dict]:
        return read_jsonl(self.path(LOGS_DIR, "events.jsonl"))

    def request_stop(self) -> None:
        atomic_write(self.path(STOP_FLAG),
                     canonical_dumps({"requested_at": time.time_ns()}))

    def stop_requested(self) -> bool:
        return os.path.exists(self.path(STOP_FLAG))

    def clear_stop(self) -> None:
        try:
            os.unlink(self.path(STOP_FLAG))
        except FileNotFoundError:
            pass

    # ---------- 检查点读取与状态重建 ----------
    def _load_checkpoint_file(self, filename: str) -> dict | None:
        full = self.path(CP_DIR, filename)
        try:
            with open(full, "rb") as handle:
                raw = handle.read()
            envelope = json.loads(raw)
            if not isinstance(envelope, dict) or "checksum" not in envelope:
                return None
            checksum = envelope.pop("checksum")
            if digest_bytes(canonical_dumps(envelope)) != checksum:
                return None
            if any(field not in envelope for field in CP_FIELDS if field != "checksum"):
                return None
            if not os.path.exists(self.path(envelope["product_path"])):
                return None
            return envelope
        except (json.JSONDecodeError, OSError, ValueError, TypeError):
            return None

    def load_valid_checkpoints(self) -> list[dict]:
        """返回通过校验且序号从 1 连续的检查点序列；损坏/跳跃记录被忽略。"""
        entries: list[dict] = []
        cp_dir = self.path(CP_DIR)
        for name in os.listdir(cp_dir):
            if not (name.startswith("cp_") and name.endswith(".json")):
                continue
            cp = self._load_checkpoint_file(name)
            if cp is not None:
                entries.append(cp)
        entries.sort(key=lambda c: c["seq"])
        valid: list[dict] = []
        expected = 1
        for cp in entries:
            if cp["seq"] != expected:
                break
            if valid and cp["seq"] <= valid[-1]["seq"]:
                break
            valid.append(cp)
            expected += 1
        return valid

    def _rebuild_state_if_needed(self) -> None:
        state_path = self.path(STATE_PATH)
        need_rebuild = True
        if os.path.exists(state_path):
            try:
                with open(state_path, "rb") as handle:
                    state = json.loads(handle.read())
                if isinstance(state, dict) and state.get("format") == "mmpipe-state/v1":
                    need_rebuild = False
            except (json.JSONDecodeError, OSError):
                need_rebuild = True
        if need_rebuild:
            self._write_state(self._build_state(self.load_valid_checkpoints(), []))

    def _heal_and_rebuild(self) -> None:
        """打开工作区时自愈：隔离损坏/断链检查点，并以最长连续有效链重建状态。

        - 校验失败或序号不在连续链上的检查点移动到 checkpoints/quarantine/；
        - state.json 仅依据有效检查点与仍适用的失效记录重建；
        - 恢复后 head_seq 即最近一致检查点，下游阶段重新计算。
        """
        cp_dir = self.path(CP_DIR)
        valid = self.load_valid_checkpoints()
        valid_names = {f"cp_{cp['seq']:06d}.json" for cp in valid}
        quarantine_dir = os.path.join(cp_dir, "quarantine")
        for name in sorted(os.listdir(cp_dir)):
            if not (name.startswith("cp_") and name.endswith(".json")):
                continue
            if name in valid_names:
                continue
            os.makedirs(quarantine_dir, exist_ok=True)
            src = os.path.join(cp_dir, name)
            dst = os.path.join(quarantine_dir, name)
            try:
                os.replace(src, dst)
                self.quarantined.append(dst)
            except OSError:
                pass
        invalidations = read_jsonl(self.path(LOGS_DIR, "invalidations.jsonl"))
        valid_seqs = {cp["seq"] for cp in valid}
        live_invalidations = [
            inv for inv in invalidations
            if inv.get("superseded_seq") in valid_seqs]
        self._write_state(self._build_state(valid, live_invalidations))

    def _build_state(self, checkpoints: list[dict],
                     invalidations: list[dict]) -> dict:
        current: dict[str, dict] = {}
        for cp in checkpoints:
            current[cp["stage"]] = {
                "seq": cp["seq"],
                "status": "published",
                "stage": cp["stage"],
                "input_fingerprint": cp["input_fingerprint"],
                "config_version": cp["config_version"],
                "product_digest": cp["product_digest"],
                "product_path": cp["product_path"],
                "created_at": cp["created_at"],
            }
        # 失效标记以“被取代的检查点序号”为准；同阶段重建后发布的新检查点
        # （序号大于 superseded_seq）不再视为失效。
        for inv in invalidations:
            stage = inv["stage"]
            cur = current.get(stage)
            if cur is not None and cur["seq"] == inv["superseded_seq"]:
                current[stage] = {**cur, "status": "invalidated",
                                   "invalidation": inv}
        return {"format": "mmpipe-state/v1", "current": current,
                "invalidations": invalidations}

    def _write_state(self, state: dict) -> None:
        atomic_write(self.path(STATE_PATH), canonical_dumps(state))

    def state(self) -> dict:
        with open(self.path(STATE_PATH), "rb") as handle:
            return json.loads(handle.read())

    def _reload_state_locked(self) -> dict:
        with open(self.path(STATE_PATH), "rb") as handle:
            return json.loads(handle.read())

    def stage_state(self, stage: str) -> dict | None:
        return self.state()["current"].get(stage)

    def invalidations(self) -> list[dict]:
        return read_jsonl(self.path(LOGS_DIR, "invalidations.jsonl"))

    # ---------- 失效传播 ----------
    def invalidate_from(self, stage: str, reason: str, component: str,
                        old_fingerprint: str | None, new_fingerprint: str) -> int:
        """使 stage 及其全部下游阶段失效；返回失效阶段数。"""
        if stage not in STAGES:
            raise PipeError(E_INVALID_STAGE, f"unknown stage {stage!r}", stage=stage)
        count = 0
        with file_lock(self.lock_path):
            state = self._reload_state_locked()
            checkpoints = self.load_valid_checkpoints()
            invalidations = list(read_jsonl(
                self.path(LOGS_DIR, "invalidations.jsonl")))
            start = STAGES.index(stage)
            for name in STAGES[start:]:
                cur = state["current"].get(name)
                if cur is None or cur.get("status") == "invalidated":
                    continue
                record = {
                    "ts": time.time_ns(),
                    "stage": name,
                    "reason": reason,
                    "component": component,
                    "old_fingerprint": old_fingerprint,
                    "new_fingerprint": new_fingerprint,
                    "superseded_seq": cur["seq"],
                }
                append_jsonl(self.path(LOGS_DIR, "invalidations.jsonl"), record)
                invalidations.append(record)
                state["current"][name] = {**cur, "status": "invalidated",
                                          "invalidation": record}
                count += 1
            self._write_state(self._build_state(checkpoints, invalidations))
            self.log_event("invalidation", from_stage=stage, reason=reason,
                           component=component, count=count)
        return count

    # ---------- 阶段发布（并发保护） ----------
    def publish(self, stage: str, product: dict, input_fingerprint: str,
                config_version: str, expected_base_seq: int | None = None) -> dict:
        if stage not in STAGES:
            raise PipeError(E_INVALID_STAGE, f"unknown stage {stage!r}", stage=stage)
        with file_lock(self.lock_path):
            checkpoints = self.load_valid_checkpoints()
            last_seq = checkpoints[-1]["seq"] if checkpoints else 0
            state = self._reload_state_locked()

            if expected_base_seq is not None and expected_base_seq != last_seq:
                raise PipeError(
                    E_STALE_CHECKPOINT,
                    f"base seq {expected_base_seq} != workspace head {last_seq}",
                    stage=stage)

            cur = state["current"].get(stage)
            if cur is not None and cur.get("status") == "published":
                if (cur["input_fingerprint"] == input_fingerprint
                        and cur["config_version"] == config_version):
                    raise PipeError(E_STALE_CHECKPOINT,
                                    f"stage {stage!r} already published at seq {cur['seq']}",
                                    stage=stage)
                raise PipeError(E_STALE_CHECKPOINT,
                                f"stage {stage!r} already published at seq {cur['seq']}; "
                                "invalidate before republish",
                                stage=stage)

            stage_index = STAGES.index(stage)
            if stage_index > 0:
                prev = STAGES[stage_index - 1]
                prev_state = state["current"].get(prev)
                if prev_state is None or prev_state.get("status") != "published":
                    raise PipeError(E_INVALID_STAGE,
                                    f"predecessor stage {prev!r} is not published",
                                    stage=stage)

            seq = last_seq + 1
            product_bytes = canonical_dumps(product)
            product_name = f"{stage}_{seq:06d}.json"
            product_rel = os.path.join(PRODUCT_DIR, product_name)
            atomic_write(self.path(product_rel), product_bytes)
            cp_body = {
                "seq": seq,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "product_digest": digest_bytes(product_bytes),
                "product_path": product_rel,
                "created_at": time.time_ns(),
            }
            envelope = {**cp_body,
                        "checksum": digest_bytes(canonical_dumps(cp_body))}
            atomic_write(self.path(CP_DIR, f"cp_{seq:06d}.json"),
                         canonical_dumps(envelope))
            atomic_write(self.path(CURRENT_DIR, f"{stage}.json"), product_bytes)

            checkpoints = self.load_valid_checkpoints()
            invalidations = read_jsonl(self.path(LOGS_DIR, "invalidations.jsonl"))
            self._write_state(self._build_state(checkpoints, invalidations))
            self.log_event("publish", stage=stage, seq=seq,
                           input_fingerprint=input_fingerprint,
                           config_version=config_version)
            return cp_body

    def load_current_product(self, stage: str) -> dict | None:
        cur = self.stage_state(stage)
        if cur is None or cur.get("status") != "published":
            return None
        with open(self.path(cur["product_path"]), "rb") as handle:
            return json.loads(handle.read())

    def head_seq(self) -> int:
        cps = self.load_valid_checkpoints()
        return cps[-1]["seq"] if cps else 0

    def corrupt_checkpoint_count(self) -> tuple[int, int]:
        """返回 (磁盘检查点文件数, 有效检查点数)，差值即被忽略的损坏记录。"""
        total = len([n for n in os.listdir(self.path(CP_DIR))
                     if n.startswith("cp_") and n.endswith(".json")])
        return total, len(self.load_valid_checkpoints())
