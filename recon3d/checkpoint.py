"""持久化阶段检查点与并发发布保护。

工作区布局（``run_dir`` 下）::

    artifacts/<stage>/<tile_id>.json   阶段产物（原子写）
    checkpoints/journal.jsonl          仅追加的检查点记录
    checkpoints/lock                   跨进程文件锁
    cache/                             查询缓存
    exports/                           点云与验证摘要导出

每条检查点记录含：阶段名、分片标识、输入内容指纹、配置版本、产物摘要、
单调序号、记录自身内容摘要。记录先写临时文件再原子 rename，写入中断只
会残留 .tmp 文件，journal 永不出现半写行。恢复时忽略损坏/不完整/序号
重复的记录，只接受从 1 开始连续递增的最长一致前缀。
"""

from __future__ import annotations

import os

from . import PIPELINE_STAGES
from .canonical import atomic_write, dumps, file_lock, fingerprint, fingerprint_bytes, read_json, write_json
from .errors import InvalidStageError, StaleCheckpointError

JOURNAL_NAME = "journal.jsonl"
LOCK_NAME = "lock"


class CheckpointStore:
    def __init__(self, run_dir: str):
        self.run_dir = run_dir
        self.checkpoint_dir = os.path.join(run_dir, "checkpoints")
        self.artifact_dir = os.path.join(run_dir, "artifacts")
        self.cache_dir = os.path.join(run_dir, "cache")
        self.export_dir = os.path.join(run_dir, "exports")
        for directory in (self.checkpoint_dir, self.artifact_dir, self.cache_dir, self.export_dir):
            os.makedirs(directory, exist_ok=True)
        self.journal_path = os.path.join(self.checkpoint_dir, JOURNAL_NAME)
        self.lock_path = os.path.join(self.checkpoint_dir, LOCK_NAME)

    # ------------------------------------------------------------------ #
    def artifact_path(self, stage: str, tile_id: str) -> str:
        return os.path.join(self.artifact_dir, stage, f"{tile_id}.json")

    def write_artifact(self, stage: str, tile_id: str, payload: dict) -> str:
        path = self.artifact_path(stage, tile_id)
        return write_json(path, payload)

    def read_artifact(self, stage: str, tile_id: str) -> dict:
        return read_json(self.artifact_path(stage, tile_id))

    def has_artifact(self, stage: str, tile_id: str) -> bool:
        return os.path.exists(self.artifact_path(stage, tile_id))

    # ------------------------------------------------------------------ #
    def load_journal(self) -> list[dict]:
        """读取并校验 journal，返回从序号 1 连续递增的最长一致记录前缀。"""
        if not os.path.exists(self.journal_path):
            return []
        valid = []
        seen = set()
        with open(self.journal_path, "rb") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = read_json_from_bytes(line)
                    seq = record["seq"]
                    checksum = record.pop("record_digest")
                    if fingerprint(record) != checksum or seq in seen:
                        break
                    if seq != len(valid) + 1:
                        break
                    record["record_digest"] = checksum
                    seen.add(seq)
                    valid.append(record)
                except Exception:
                    # 损坏/不完整/无法解析的记录：停止采纳后续内容
                    break
        return valid

    def latest_seq(self) -> int:
        journal = self.load_journal()
        return journal[-1]["seq"] if journal else 0

    def latest_consistent(self) -> dict | None:
        journal = self.load_journal()
        return journal[-1] if journal else None

    # ------------------------------------------------------------------ #
    def publish(
        self,
        stage: str,
        tile_id: str,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        expect_tile: str | None = None,
    ) -> dict:
        """在文件锁内比较分片内序号，仅允许连续推进，否则 E_STALE_CHECKPOINT。"""
        if stage not in PIPELINE_STAGES:
            raise InvalidStageError(
                "unknown pipeline stage", stage=stage, tile_id=tile_id
            )
        with file_lock(self.lock_path):
            journal = self.load_journal()
            next_seq = len(journal) + 1
            prior = [r for r in journal if r["stage"] == stage and r["tile_id"] == tile_id]
            if prior:
                latest = prior[-1]
                # 输入指纹未变却重新发布 = 落后/重复发布，拒绝；
                # 输入指纹变化属于合法的失效重建，允许以新序号推进
                if latest["input_fingerprint"] == input_fingerprint:
                    raise StaleCheckpointError(
                        "stale or duplicate publish with unchanged input fingerprint",
                        stage=stage,
                        tile_id=tile_id,
                        last_seq=latest["seq"],
                    )
                # 显式声明期望分片时，跨分片错位发布被拒绝
                if expect_tile is not None and expect_tile != tile_id:
                    raise StaleCheckpointError(
                        "cross-tile out-of-order publish rejected",
                        stage=stage,
                        tile_id=tile_id,
                        expected_tile=expect_tile,
                    )
            record = {
                "seq": next_seq,
                "stage": stage,
                "tile_id": tile_id,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
            }
            record["record_digest"] = fingerprint(record)
            self._append_record(record)
            return record

    def _append_record(self, record: dict) -> None:
        # 追加行也走临时文件 + 原子拼接，保证任意中断不留半写 journal
        line = dumps(record) + b"\n"
        tmp = self.journal_path + f".tmp.{os.getpid()}"
        with open(tmp, "wb") as handle:
            if os.path.exists(self.journal_path):
                with open(self.journal_path, "rb") as existing:
                    handle.write(existing.read())
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.journal_path)

    def stage_status(self) -> list[dict]:
        status = []
        for record in self.load_journal():
            status.append(
                {
                    "seq": record["seq"],
                    "stage": record["stage"],
                    "tile_id": record["tile_id"],
                    "artifact_digest": record["artifact_digest"],
                    "input_fingerprint": record["input_fingerprint"],
                    "config_version": record["config_version"],
                }
            )
        return status


def read_json_from_bytes(data: bytes):
    from .canonical import loads

    return loads(data)
