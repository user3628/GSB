"""确定性哈希与内容指纹。

使用 FNV-1a 64 位作为特征哈希（纯整数运算，跨平台/跨 Python 版本一致），
SHA-256 作为内容摘要；JSON 序列化统一走 :mod:`pipeline.atomicio` 的规范化
编码器，保证同一逻辑内容得到逐字节一致的字节流。
"""

import hashlib

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK64 = 0xFFFFFFFFFFFFFFFF


def fnv1a64(data):
    """对字节串计算 FNV-1a 64 位哈希，返回非负整数。"""
    if isinstance(data, str):
        data = data.encode("utf-8")
    h = FNV_OFFSET
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & MASK64
    return h


def feature_index(token, dimension):
    """把词项稳定映射到 [0, dimension) 的特征下标。"""
    return fnv1a64(token) % dimension


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_text(text):
    return sha256_bytes(text.encode("utf-8"))


def digest_json(obj):
    """对可 JSON 序列化对象计算规范化字节流的 SHA-256 摘要。"""
    from pipeline.atomicio import canonical_json_bytes

    return sha256_bytes(canonical_json_bytes(obj))


def short_digest(digest, length=16):
    """把长摘要截断为适合做版本号的短摘要。"""
    return digest[:length]
