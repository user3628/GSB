"""分块列存快照：清单写入/加载、版本单调与整体摘要校验、原子替换。"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass

from .encoding import read_block, DecodedBlock
from .errors import SnapshotInvalid


def _canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class BlockIndexEntry:
    block_id: int
    file: str
    pk_min: int
    pk_max: int
    row_count: int
    decode_cost: int


@dataclass
class Snapshot:
    version: int
    generated_at: str
    columns: list[dict]          # [{name, dtype, nullable}]
    blocks: list[BlockIndexEntry]  # 按主键升序
    root: str

    def decode_block(self, entry: BlockIndexEntry) -> DecodedBlock:
        return read_block(os.path.join(self.root, entry.file), expected_block_id=entry.block_id)

    def locate_blocks(self, keys: sorted) -> list[BlockIndexEntry]:
        """按升序主键集合定位命中的分块（二分），返回去重后按主键升序的块索引。"""
        import bisect
        hit: dict[int, BlockIndexEntry] = {}
        mins = [b.pk_min for b in self.blocks]
        maxs = [b.pk_max for b in self.blocks]
        for k in keys:
            i = bisect.bisect_left(maxs, k)
            if i < len(self.blocks) and mins[i] <= k <= maxs[i]:
                hit[self.blocks[i].block_id] = self.blocks[i]
        return [hit[b.block_id] for b in self.blocks if b.block_id in hit]


def write_snapshot(root: str, version: int, generated_at: str, columns: list[dict],
                   blocks: list[BlockIndexEntry | dict]) -> str:
    """写入 manifest.json（含整体校验摘要），返回摘要。"""
    manifest = {
        "version": version,
        "generated_at": generated_at,
        "columns": columns,
        "blocks": [dict(b.__dict__) if isinstance(b, BlockIndexEntry) else dict(b) for b in blocks],
    }
    manifest["checksum"] = _sha256(_canonical(manifest))
    os.makedirs(root, exist_ok=True)
    tmp = os.path.join(root, "manifest.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    os.replace(tmp, os.path.join(root, "manifest.json"))
    return manifest["checksum"]


def load_manifest(root: str) -> dict:
    path = os.path.join(root, "manifest.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception as exc:
        raise SnapshotInvalid(f"清单不可读: {exc}")
    checksum = manifest.pop("checksum", None)
    if not checksum or _sha256(_canonical(manifest)) != checksum:
        raise SnapshotInvalid("清单整体校验摘要失败")
    blocks = manifest.get("blocks", [])
    for i in range(len(blocks) - 1):
        if blocks[i]["pk_max"] >= blocks[i + 1]["pk_min"]:
            raise SnapshotInvalid("分块索引未按主键升序或区间重叠")
    for b in blocks:
        if not os.path.exists(os.path.join(root, b["file"])):
            raise SnapshotInvalid(f"块文件缺失: {b['file']}")
    manifest["checksum"] = checksum
    return manifest


class SnapshotStore:
    """当前生效快照。加载失败保留旧快照继续服务；成功后整体替换，不混合新旧分块索引。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._current: Snapshot | None = None

    def current(self) -> Snapshot | None:
        with self._lock:
            return self._current

    def load(self, root: str) -> Snapshot:
        manifest = load_manifest(root)  # 校验失败抛 SnapshotInvalid，旧快照不动
        new_version = int(manifest["version"])
        with self._lock:
            if self._current is not None and new_version <= self._current.version:
                raise SnapshotInvalid(
                    f"版本非单调递增: 当前 {self._current.version}, 新 {new_version}",
                    current_version=self._current.version, new_version=new_version)
            snap = Snapshot(
                version=new_version,
                generated_at=manifest["generated_at"],
                columns=manifest["columns"],
                blocks=[BlockIndexEntry(**b) for b in manifest["blocks"]],
                root=root,
            )
            self._current = snap  # 整体替换
            return snap
