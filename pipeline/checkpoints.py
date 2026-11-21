"""阶段检查点持久化。

记录文件 ``record-000001.json`` 顺序编号，单条记录包含阶段名、输入内容指纹、
配置版本、产物摘要、单调序号与自校验摘要。写入全部走原子替换；恢复时只接受
从 1 开始、序号连续、阶段顺序正确且摘要自洽的最长前缀，其余记录隔离到
``quarantine/`` 并在状态中可观测。
"""

import os
import shutil
import time

from pipeline import STAGES
from pipeline.atomicio import atomic_write_json, canonical_json_bytes, read_json
from pipeline.errors import E_CORRUPT_CHECKPOINT, E_STALE_CHECKPOINT, PipelineError
from pipeline.hashing import digest_json, sha256_bytes

RECORD_PREFIX = "record-"
RECORD_SUFFIX = ".json"
RECORD_FIELDS = (
    "sequence",
    "stage",
    "input_fingerprint",
    "config_fingerprint",
    "artifact_summary",
    "record_digest",
)


def _record_filename(sequence):
    return f"{RECORD_PREFIX}{sequence:06d}{RECORD_SUFFIX}"


def _make_record(sequence, stage, input_fingerprint, config_fingerprint, artifact_summary):
    record = {
        "sequence": sequence,
        "stage": stage,
        "input_fingerprint": input_fingerprint,
        "config_fingerprint": config_fingerprint,
        "artifact_summary": artifact_summary,
    }
    record["record_digest"] = digest_json(record)
    return record


def _record_is_self_consistent(record):
    if not isinstance(record, dict) or any(field not in record for field in RECORD_FIELDS):
        return False
    digest = record.pop("record_digest")
    try:
        expected = digest_json(record)
    finally:
        record["record_digest"] = digest
    return digest == expected


class CheckpointStore:
    def __init__(self, workspace):
        self.ws = workspace
        self.dir = workspace.checkpoint_dir

    def _quarantine(self, path, reason):
        os.makedirs(self.ws.quarantine_dir, exist_ok=True)
        stamp = f"{time.strftime('%Y%m%dT%H%M%S')}-{os.path.basename(path)}"
        destination = os.path.join(self.ws.quarantine_dir, stamp)
        if os.path.exists(path):
            shutil.move(path, destination)
        return {"file": os.path.basename(path), "reason": reason, "quarantined_as": stamp}

    def load_valid_records(self):
        """返回 (有效前缀记录列表, 隔离事件列表)。"""
        os.makedirs(self.dir, exist_ok=True)
        names = sorted(
            name
            for name in os.listdir(self.dir)
            if name.startswith(RECORD_PREFIX) and name.endswith(RECORD_SUFFIX)
        )
        records = []
        quarantined = []
        for name in names:
            path = os.path.join(self.dir, name)
            expected_sequence = len(records) + 1
            expected_stage = STAGES[(expected_sequence - 1) % len(STAGES)]
            try:
                record, _ = read_json(path)
                if not _record_is_self_consistent(record):
                    raise PipelineError(E_CORRUPT_CHECKPOINT, "记录摘要不自洽")
                if record["sequence"] != expected_sequence:
                    raise PipelineError(
                        E_CORRUPT_CHECKPOINT,
                        "序号重复、跳跃或不连续",
                        {"got": record["sequence"], "expected": expected_sequence},
                    )
                if record["stage"] != expected_stage:
                    raise PipelineError(
                        E_CORRUPT_CHECKPOINT,
                        "阶段顺序与序号错位",
                        {"got": record["stage"], "expected": expected_stage},
                    )
            except (OSError, ValueError, KeyError, PipelineError) as exc:
                reason = exc.code if isinstance(exc, PipelineError) else "UNREADABLE_RECORD"
                # 当前文件及其后所有记录都不可信，整体隔离后停止扫描。
                quarantined.append(self._quarantine(path, reason))
                for later_name in names[names.index(name) + 1 :]:
                    quarantined.append(
                        self._quarantine(os.path.join(self.dir, later_name), "AFTER_CORRUPT_PREFIX")
                    )
                break
            records.append(record)
        return records, quarantined

    def publish(self, stage, input_fingerprint, config_fingerprint, artifact_summary):
        """在调用方持有工作区锁的前提下发布一条新检查点。

        仅允许序号 = 当前有效前缀长度 + 1 且阶段匹配，否则 E_STALE_CHECKPOINT。
        """
        records, _ = self.load_valid_records()
        expected_sequence = len(records) + 1
        expected_stage = STAGES[len(records) % len(STAGES)]
        if stage != expected_stage:
            raise PipelineError(
                E_STALE_CHECKPOINT,
                "跨阶段错位或落后/重复发布被拒绝",
                {"got_stage": stage, "expected_stage": expected_stage, "sequence": expected_sequence},
            )
        record = _make_record(
            expected_sequence, stage, input_fingerprint, config_fingerprint, artifact_summary
        )
        target = os.path.join(self.dir, _record_filename(expected_sequence))
        if os.path.exists(target):  # 并发残留保护，正常不会发生
            raise PipelineError(
                E_STALE_CHECKPOINT,
                "目标序号记录已存在，拒绝重复发布",
                {"sequence": expected_sequence},
            )
        atomic_write_json(target, record)
        return record

    def truncate_from_stage(self, stage_index, reason):
        """失效指定阶段及其全部下游检查点，记录隔离事件并返回被移除序号。"""
        records, _ = self.load_valid_records()
        removed = []
        for record in records[stage_index:]:
            path = os.path.join(self.dir, _record_filename(record["sequence"]))
            event = self._quarantine(path, "INVALIDATED:" + reason)
            removed.append({"sequence": record["sequence"], "stage": record["stage"], **event})
        return removed

    def append_invalidation_event(self, event):
        """以原子追加方式写入失效事件日志。"""
        line = canonical_json_bytes(event)
        os.makedirs(self.ws.state_dir, exist_ok=True)
        if os.path.exists(self.ws.invalidation_log):
            with open(self.ws.invalidation_log, "rb") as handle:
                existing = handle.read()
        else:
            existing = b""
        atomic_path = self.ws.invalidation_log + ".tmp"
        with open(atomic_path, "wb") as handle:
            handle.write(existing + line)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(atomic_path, self.ws.invalidation_log)

    def read_invalidation_events(self):
        import json

        if not os.path.exists(self.ws.invalidation_log):
            return []
        events = []
        with open(self.ws.invalidation_log, "rb") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    events.append(json.loads(line.decode("utf-8")))
        return events

    def artifact_digest(self, artifact_path):
        with open(artifact_path, "rb") as handle:
            return sha256_bytes(handle.read())
