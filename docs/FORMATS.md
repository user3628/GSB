# 统计快照格式规范

## 1. 语义摘要与版本

两种线格式都携带 `snapshot_version`。导入后计算语义摘要：

```text
sha256(canonical_json({
  "version": snapshot_version,
  "relations": [{
    "name": ...,
    "row_count": ...,
    "ndv_ratio_fixed": ...,
    "correlation_fixed": ...,
    "stale_keys": [...]
  }]
}))
```

语义摘要不包含线格式、字节序或 CRC。因此语义相同的二进制与 JSONL 样本拥有相同
digest，可用于互操作验证与版本绑定。

## 2. 紧凑二进制 `JCSB`

### 2.1 头部（固定 40 字节）

| 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 4 | 魔数 `JCSB` |
| 4 | 1 | 格式版本（1 或 2） |
| 5 | 1 | 大小端标记：`0x4C` 小端，`0x42` 大端 |
| 6 | 2 | 头部长度，当前为 40 |
| 8 | 4 | 标志位：bit0=`body_crc32`，bit1=`optional_bitmap` |
| 12 | 8 | 快照版本 `u64` |
| 20 | 4 | 记录数 `u32` |
| 24 | 8 | body 长度 `u64` |
| 32 | 4 | body 的 CRC32 |
| 36 | 4 | 前 36 字节的 CRC32 |

头部 CRC 先于 body 解析验证。未知标志位表示未知必填扩展，返回
`E_FORMAT_UNSUPPORTED`。

### 2.2 记录

body 是连续记录，每条记录先有一个按头部端序编码的 `u32` 记录长度前缀：

```text
u32 record_length
u16 name_length
u8[name_length] relation_name (UTF-8)
u8 presence_bitmap
u64 row_count
float32|float64 ndv_ratio        # v1: float32, v2: float64
float64 correlation_coefficient  # bit1 存在时
u16 stale_count                  # bit2 存在时
  [u16 key_length, u8[key_length] key] ...
```

`presence_bitmap`：

- bit0：NDV 比例存在，必须为 1。
- bit1：相关系数存在。v1 禁止设置；v2 可省略，省略时默认 `1.0` 并记录降级。
- bit2：过期键标记列表存在。

解码器要求 `record_length` 与字段实际消费长度严格一致、body 长度与头部一致、
所有 CRC 一致。否则返回 `E_FORMAT_CORRUPT`，且不写任何快照/报告状态。

### 2.3 版本差异

- v1：NDV 比例使用 `float32`，不携带相关系数，按 `1.0` 补齐并记录降级。
- v2：NDV 比例使用 `float64`，可选相关系数与过期键列表。
- v3 及以后：本构建不支持，返回 `E_FORMAT_UNSUPPORTED`。

## 3. JSON Lines

首行是 manifest，后续每行一个关系的统计记录。实例：

```json
{"format_version":2,"record_count":6,"records_crc32":"...","required_features":["crc32","stale_keys"],"schema":"jcs.stats.jsonl","snapshot_version":1001,"type":"manifest"}
{"correlation_coefficient":0.9,"ndv_permille":750,"record_crc32":"...","relation":"customers","row_count":60000,"stale_keys":["legacy_segment"],"type":"relation"}
```

关系记录字段：

- `relation`：关系名。
- `row_count`：非负整数，最大 `2^63-1`。
- `ndv_permille`：v2 的千分比整数，范围 `0..1000`。
- `ndv_percent`：v1 旧字段，百分整数，范围 `0..100`，触发精度降级。
- `correlation_coefficient`：可选，`[0,1]`；缺省时补 `1.0` 并记录降级。
- `stale_keys`：布尔、字符串或字符串数组，表示过期统计键标记。
- `record_crc32`：对“移除 `record_crc32` 后的 canonical JSON”计算 CRC32。

manifest 的 `record_count` 必须等于关系记录行数。`records_crc32` 是对所有关系记录
的 canonical JSON 行依次加换行后计算 CRC32。任一不一致返回 `E_FORMAT_CORRUPT`。

`required_features` 中出现本构建不认识的必填特性，或 `format_version` 高于兼容矩阵，
返回 `E_FORMAT_UNSUPPORTED`，并把 `REJECT` 导入报告写入 SQLite。

## 4. 兼容矩阵

| 格式 | v1 | v2 | v3+ |
|---|---|---|---|
| binary | `DEGRADE_ACCEPT` | `ACCEPT` | `REJECT` |
| jsonl | `DEGRADE_ACCEPT` | `ACCEPT` | `REJECT` |

`DEGRADE_ACCEPT` 的详情至少包含：

- 旧 NDV 精度较低（float32 或百分整数）。
- 缺失相关系数，使用声明默认值 `1.000000000`。

导入报告无论接受、降级还是拒绝都会持久化；`E_FORMAT_CORRUPT` 与 `E_STAT_INVALID`
不会写入快照或报告状态。

## 5. 数值归一化

- NDV 比例统一为 `1e9` 定点整数。
- JSON 千分比 `833` 变为 `833000000`；百分整数 `83` 变为 `830000000`，均为精确映射。
- 二进制浮点比例使用十进制字符串表示后半偶舍入到 9 位小数。
- 每条关系输出 `source_precision_places`、`rounding_place` 与
  `rounding_error_units`（1e-12 精度）。
- 快照级 `normalization.cumulative_rounding_error_units` 汇总所有记录的舍入误差。

