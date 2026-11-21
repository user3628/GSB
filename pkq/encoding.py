"""三种编码块的确定性解码：fixed / rle / dict。

块文件布局：
  4B  magic "PKQB"
  4B  header_len (big-endian)
  header_len B  header JSON（含 header 自身摘要字段 header_checksum 之外的 canonical JSON 的 sha256）
  payload ...   各列 payload 顺序拼接

header JSON:
  block_id, pk_min, pk_max, row_count, decode_cost,
  columns: [{name, dtype, encoding, null_bitmap_hex, payload_offset, payload_len, payload_sha256}],
  header_checksum
"""
from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass

import numpy as np

from .errors import BlockCorrupt

MAGIC = b"PKQB"
ENCODINGS = ("fixed", "rle", "dict")


def _canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- 编码

def encode_column(values: list, dtype: str, encoding: str) -> tuple[bytes, bytes]:
    """返回 (null_bitmap, payload)。values 中 None 表示空值。"""
    n = len(values)
    bitmap = bytearray((n + 7) // 8)
    for i, v in enumerate(values):
        if v is None:
            bitmap[i // 8] |= 1 << (i % 8)
    nn = [v for v in values if v is not None]

    if encoding == "fixed":
        if dtype != "int64":
            raise ValueError("fixed 编码仅支持 int64")
        payload = np.array([v if v is not None else 0 for v in values], dtype="<i8").tobytes()
    elif encoding == "rle":
        if dtype != "int64":
            raise ValueError("rle 编码仅支持 int64")
        out = bytearray()
        i = 0
        seq = [v if v is not None else 0 for v in values]
        while i < n:
            j = i
            while j + 1 < n and seq[j + 1] == seq[i]:
                j += 1
            out += struct.pack("<qI", int(seq[i]), j - i + 1)
            i = j + 1
        payload = bytes(out)
    elif encoding == "dict":
        uniq = sorted({v for v in nn})
        if dtype == "int64":
            body = b"".join(struct.pack("<q", int(v)) for v in uniq)
        elif dtype == "str":
            body = b""
            for v in uniq:
                b = str(v).encode("utf-8")
                body += struct.pack("<I", len(b)) + b
        else:
            raise ValueError(f"未知 dtype {dtype}")
        index = {v: k for k, v in enumerate(uniq)}
        idx = np.array([index[v] if v is not None else 0 for v in values], dtype="<u4").tobytes()
        payload = struct.pack("<I", len(uniq)) + body + idx
    else:
        raise ValueError(f"未知编码 {encoding}")
    return bytes(bitmap), payload


# ---------------------------------------------------------------- 解码

def decode_column(dtype: str, encoding: str, row_count: int, null_bitmap: bytes, payload: bytes) -> list:
    """确定性解码：同一输入多次解码逐位一致。"""
    if encoding == "fixed":
        arr = np.frombuffer(payload, dtype="<i8", count=row_count)
        vals = [int(x) for x in arr]
    elif encoding == "rle":
        vals = []
        off = 0
        while off < len(payload):
            v, run = struct.unpack_from("<qI", payload, off)
            off += 12
            vals.extend([int(v)] * run)
        if len(vals) != row_count:
            raise ValueError("rle 展开行数与块头不一致")
    elif encoding == "dict":
        (k,) = struct.unpack_from("<I", payload, 0)
        off = 4
        table: list = []
        if dtype == "int64":
            for _ in range(k):
                (v,) = struct.unpack_from("<q", payload, off)
                off += 8
                table.append(int(v))
        elif dtype == "str":
            for _ in range(k):
                (ln,) = struct.unpack_from("<I", payload, off)
                off += 4
                table.append(payload[off:off + ln].decode("utf-8"))
                off += ln
        else:
            raise ValueError(f"未知 dtype {dtype}")
        idx = np.frombuffer(payload, dtype="<u4", count=row_count, offset=off)
        vals = [table[int(i)] for i in idx]
    else:
        raise ValueError(f"未知编码 {encoding}")

    out = []
    for i, v in enumerate(vals):
        if null_bitmap[i // 8] >> (i % 8) & 1:
            out.append(None)
        else:
            out.append(v)
    return out


# ---------------------------------------------------------------- 块读写

@dataclass
class DecodedBlock:
    block_id: int
    pk_min: int
    pk_max: int
    row_count: int
    decode_cost: int
    pks: list[int]            # 行内主键升序
    columns: dict[str, list]  # 列名 -> 与 pks 等长的值序列（含 None）


def write_block(path, block_id: int, pks: list[int], columns: dict[str, tuple[list, str, str]],
                decode_cost: int) -> dict:
    """columns: {name: (values, dtype, encoding)}。返回可放入 manifest 的块索引项。"""
    row_count = len(pks)
    if any(pks[i] > pks[i + 1] for i in range(row_count - 1)):
        raise ValueError("块内主键必须升序")
    col_entries = []
    payload = b""
    for name, (values, dtype, encoding) in columns.items():
        if len(values) != row_count:
            raise ValueError(f"列 {name} 行数与主键不一致")
        bitmap, col_payload = encode_column(values, dtype, encoding)
        col_entries.append({
            "name": name, "dtype": dtype, "encoding": encoding,
            "null_bitmap_hex": bitmap.hex(),
            "payload_offset": 0,  # 稍后填充
            "payload_len": len(col_payload),
            "payload_sha256": _sha256(col_payload),
        })
        payload += col_payload
    off = 0
    for e in col_entries:
        e["payload_offset"] = off
        off += e["payload_len"]

    header = {
        "block_id": block_id,
        "pk_min": pks[0] if pks else -1,
        "pk_max": pks[-1] if pks else -1,
        "row_count": row_count,
        "decode_cost": decode_cost,
        "columns": col_entries,
    }
    header["header_checksum"] = _sha256(_canonical(header))
    hbytes = _canonical(header)
    with open(path, "wb") as f:
        f.write(MAGIC)
        f.write(struct.pack(">I", len(hbytes)))
        f.write(hbytes)
        f.write(payload)
    return {"block_id": block_id, "file": path.name if hasattr(path, "name") else str(path).rsplit("/", 1)[-1],
            "pk_min": header["pk_min"], "pk_max": header["pk_max"],
            "row_count": row_count, "decode_cost": decode_cost}


def read_block(path, expected_block_id: int | None = None) -> DecodedBlock:
    """读取并校验块头（主键区间、行数、编码类型、摘要），失败抛 BlockCorrupt。"""
    try:
        raw = open(path, "rb").read()
    except OSError as exc:
        raise BlockCorrupt(f"块文件不可读: {exc}", block_id=expected_block_id if expected_block_id is not None else -1)
    bid = expected_block_id if expected_block_id is not None else -1
    try:
        if raw[:4] != MAGIC:
            raise ValueError("magic 不匹配")
        (hlen,) = struct.unpack_from(">I", raw, 4)
        header = json.loads(raw[8:8 + hlen].decode("utf-8"))
        payload = raw[8 + hlen:]
        bid = int(header["block_id"])
        checksum = header.pop("header_checksum")
        if _sha256(_canonical(header)) != checksum:
            raise ValueError("块头摘要校验失败")
        if expected_block_id is not None and bid != expected_block_id:
            raise ValueError(f"块号不一致: 期望 {expected_block_id} 实际 {bid}")
        row_count = int(header["row_count"])
        if row_count < 0 or header["pk_min"] > header["pk_max"]:
            raise ValueError("块头主键区间非法")
        columns: dict[str, list] = {}
        pks: list[int] | None = None
        for col in header["columns"]:
            if col["encoding"] not in ENCODINGS:
                raise ValueError(f"未知编码类型 {col['encoding']}")
            start = int(col["payload_offset"])
            end = start + int(col["payload_len"])
            cp = payload[start:end]
            if len(cp) != int(col["payload_len"]) or _sha256(cp) != col["payload_sha256"]:
                raise ValueError(f"列 {col['name']} 摘要校验失败")
            vals = decode_column(col["dtype"], col["encoding"], row_count,
                                 bytes.fromhex(col["null_bitmap_hex"]), cp)
            if col["name"] == "pk":
                pks = vals
            columns[col["name"]] = vals
        if pks is None:
            raise ValueError("缺少 pk 列")
        if any(pks[i] is None or pks[i] > pks[i + 1] for i in range(row_count - 1)):
            raise ValueError("块内主键未升序")
        return DecodedBlock(bid, int(header["pk_min"]), int(header["pk_max"]), row_count,
                            int(header["decode_cost"]), pks, columns)
    except BlockCorrupt:
        raise
    except Exception as exc:
        raise BlockCorrupt(f"块 {bid} 校验失败: {exc}", block_id=bid)


def estimate_decoded_bytes(row_count: int, n_columns: int) -> int:
    """单任务解压后内存估计：主键 8B + 每列 16B + 每列位图 1B，按行计。"""
    return row_count * (8 + n_columns * 17)
