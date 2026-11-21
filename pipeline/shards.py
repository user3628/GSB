"""训练/推理分片读写与目录内容指纹。"""

import json
import os

from pipeline.atomicio import atomic_write
from pipeline.hashing import sha256_bytes


def write_jsonl(path, records):
    lines = [
        json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        for record in records
    ]
    atomic_write(path, ("\n".join(lines) + "\n").encode("utf-8"))


def read_jsonl(path):
    records = []
    with open(path, "rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line.decode("utf-8")))
            except json.JSONDecodeError as exc:
                raise ValueError(f"分片 {path} 第 {line_number} 行不是合法 JSON: {exc}") from exc
    return records


def write_json_array(path, records):
    text = json.dumps(records, sort_keys=True, ensure_ascii=False, indent=2)
    atomic_write(path, (text + "\n").encode("utf-8"))


def read_json_array(path):
    with open(path, "rb") as handle:
        data = json.loads(handle.read().decode("utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"分片 {path} 顶层必须是 JSON 数组")
    return data


def list_shards(directory, suffix):
    if not os.path.isdir(directory):
        return []
    return sorted(
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if name.endswith(suffix)
    )


def directory_fingerprint(directory, suffix):
    """目录指纹包含相对路径名与文件内容，重命名/改动均会改变指纹。"""
    files = list_shards(directory, suffix)
    parts = []
    for path in files:
        rel = os.path.relpath(path, directory).replace(os.sep, "/")
        with open(path, "rb") as handle:
            parts.append(rel + ":" + sha256_bytes(handle.read()))
    joined = "\n".join(parts)
    return sha256_bytes(joined.encode("utf-8")), [os.path.basename(p) for p in files]
