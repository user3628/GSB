"""生成确定性分块列存样本：三种编码、空值位图、跨块主键、损坏块、不同解压代价。

布局（artifacts/sample/）：
  snapshot_v1/       正常快照（版本 1，12 块 x 100 行）
  snapshot_v2/       正常快照（版本 2，用于版本单调/整体替换验证）
  snapshot_badsum/   清单摘要被篡改的快照（E_SNAPSHOT_INVALID）
  snapshot_corrupt/  含损坏块的快照（E_BLOCK_CORRUPT）
"""
from __future__ import annotations

import json
import os
import random
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pkq.columnar import write_snapshot
from pkq.encoding import write_block

ROWS_PER_BLOCK = 100
N_BLOCKS = 12


def build_rows(rng: random.Random, block_id: int):
    """返回 (pks, cols)。每 10 个主键重复 3 次，制造同主键多行命中。"""
    pks: list[int] = []
    base = block_id * 1000
    i = 0
    while len(pks) < ROWS_PER_BLOCK:
        pk = base + i * 7  # 块内稀疏升序主键
        pks.append(pk)
        if i % 10 == 5 and len(pks) + 2 < ROWS_PER_BLOCK:
            pks.extend([pk, pk])  # 同主键重复行
        i += 1
    pks = pks[:ROWS_PER_BLOCK]
    n = len(pks)
    val_a = [(i // 8) * 100 if (i % 13) else None for i in range(n)]      # rle 友好 + 空值
    val_b = [rng.choice([7, 42, 99, 1234]) if i % 11 else None for i in range(n)]  # dict
    name = [f"row-{block_id}-{i}" if i % 17 else None for i in range(n)]  # dict str + 空值
    cols = {
        "pk": (list(pks), "int64", "fixed"),
        "val_a": (val_a, "int64", "rle"),
        "val_b": (val_b, "int64", "dict"),
        "name": (name, "str", "dict"),
    }
    return pks, cols


def decode_cost(block_id: int) -> int:
    """不同解压开销分布：前 4 块廉价，中间线性增长，末两块昂贵。"""
    if block_id < 4:
        return 50 + block_id * 10
    if block_id < 10:
        return 100 + (block_id - 4) * 120
    return 900 + (block_id - 10) * 600


def make_snapshot(root: str, version: int, corrupt_block: int | None = None) -> dict:
    rng = random.Random(20261122 + version)
    blocks_dir = os.path.join(root, "blocks")
    os.makedirs(blocks_dir, exist_ok=True)
    entries = []
    for bid in range(N_BLOCKS):
        pks, cols = build_rows(rng, bid)
        path = os.path.join(blocks_dir, f"block_{bid:04d}.bin")
        entry = write_block(path, bid, pks, cols, decode_cost(bid))
        entry["file"] = f"blocks/block_{bid:04d}.bin"
        entries.append(entry)
    columns = [
        {"name": "pk", "dtype": "int64", "nullable": False},
        {"name": "val_a", "dtype": "int64", "nullable": True},
        {"name": "val_b", "dtype": "int64", "nullable": True},
        {"name": "name", "dtype": "str", "nullable": True},
    ]
    checksum = write_snapshot(root, version, f"2026-11-22T00:00:{version:02d}Z",
                              columns, entries)
    if corrupt_block is not None:
        # 篡改指定块 payload 的一个字节（保持文件长度），使其摘要校验失败
        victim = os.path.join(blocks_dir, f"block_{corrupt_block:04d}.bin")
        with open(victim, "r+b") as f:
            f.seek(-1, os.SEEK_END)
            last = f.read(1)
            f.seek(-1, os.SEEK_END)
            f.write(bytes([last[0] ^ 0xFF]))
    return {"version": version, "checksum": checksum, "blocks": len(entries)}


def main(out_root: str = "artifacts/sample"):
    if os.path.exists(out_root):
        shutil.rmtree(out_root)
    os.makedirs(out_root, exist_ok=True)
    info = {}
    info["snapshot_v1"] = make_snapshot(os.path.join(out_root, "snapshot_v1"), 1)
    info["snapshot_v2"] = make_snapshot(os.path.join(out_root, "snapshot_v2"), 2)
    info["snapshot_corrupt"] = make_snapshot(os.path.join(out_root, "snapshot_corrupt"), 3,
                                             corrupt_block=5)
    # 摘要被篡改的快照：先生成正常再改清单
    bad = os.path.join(out_root, "snapshot_badsum")
    info["snapshot_badsum"] = make_snapshot(bad, 4)
    mpath = os.path.join(bad, "manifest.json")
    manifest = json.load(open(mpath, encoding="utf-8"))
    manifest["generated_at"] = "2026-11-22T99:99:99Z"  # 篡改内容但不更新摘要
    json.dump(manifest, open(mpath, "w", encoding="utf-8"), indent=2)
    with open(os.path.join(out_root, "sample.json"), "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "artifacts/sample")
