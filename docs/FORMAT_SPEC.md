# 持久化格式规范

## 1. 统一内部表示

| 字段 | 内部单位 | 类型/约束 |
|---|---|---|
| `cir_bps`, `pir_bps` | bit/s | 非负整数，上限 `2^64-1` |
| `cbs_bytes`, `pbs_bytes` | byte | 正整数，上限 `2^64-1`，且 `CBS <= PBS` |
| `priority` | 无 | 0..7 |
| `timestamp_us`, `generated_at_us` | microsecond | 非负整数 |
| `snapshot_no` | 无 | 正整数，持久化上限 `2^63-1` |

策略关系约束：`PIR >= CIR`；父策略存在时才允许声明父队列；子整形器的 CIR/PIR 分别不得超过父整形器；父依赖不得成环。同一导入批次中同队列标识重复时，最后出现的记录获胜。

## 2. 分片 JSON 行格式

每个非空行是一个完整 JSON 对象；v3 示例：

```json
{"format_version":3,"snapshot_no":1,"generated_at_ms":1700000000123.456,"queue_id":"voice","cir_kbps":2000,"pir_kbps":4000,"cbs_kb":4,"pbs_kb":8,"priority":1,"match_rule":"udp and dst port 5060","parent_queue_id":"wan","timestamp_ms":1700000000123.456}
```

字段与换算：

```text
cir_bps   = cir_kbps × 1000
pir_bps   = pir_kbps × 1000
cbs_bytes = cbs_kb × 1024
pbs_bytes = pbs_kb × 1024
timestamp_us = timestamp_ms × 1000
```

换算使用 `Decimal`，结果必须是整数，否则 `E_UNIT_PRECISION`；越界为 `E_UNIT_OVERFLOW`。每行重复携带的 `snapshot_no`、`generated_at_*` 必须一致。可选别名 `cir/pir/cbs/pbs` 仍按 kbps/KB 解释。

版本行为：

- v1：不得携带 PIR/PBS；导入时补齐 `PIR=CIR`、`PBS=CBS`。
- v2：PIR/PBS 可缺省；缺省时按同一规则补齐。
- v3：当前版本，正常导出包含全部字段。
- v4+：整批 `E_FORMAT_UNSUPPORTED`，不产生部分导入。

未知必填扩展可声明为 `"required_fields":["future_field"]`，出现未知名称时整批拒绝。

## 3. 紧凑二进制格式

所有多字节整数按头部的大小端标记解释。v1/v2/v3 头长度均为 38 字节。

### 3.1 头部

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `QOSB` |
| 4 | 1 | version | 1/2/3 |
| 5 | 1 | endian | `1=little`, `2=big` |
| 6 | 2 | flags | 未知必填特性使用 `0x8000` |
| 8 | 2 | header_length | 当前为 38 |
| 10 | 4 | record_count | 记录数 |
| 14 | 1 | timestamp_unit | `0=µs` |
| 15 | 1 | rate_unit | `0=bps` |
| 16 | 1 | burst_unit | `0=byte` |
| 17 | 1 | reserved | 0 |
| 18 | 8 | snapshot_no | 无符号 64 位 |
| 26 | 8 | generated_at_us | 无符号 64 位 |
| 34 | 4 | header_crc32 | CRC32(byte 0..33) |

### 3.2 记录

```text
u32 record_body_length
body[record_body_length]
u32 crc32(body)
```

`body` 布局：

| 字段 | 编码 |
|---|---|
| presence bitmap | u16 |
| queue_id length | u16 |
| queue_id | UTF-8 bytes |
| 存在的条件字段 | 按下表顺序紧排 |

存在位：

| 位 | 字段 | 编码 |
|---:|---|---|
| `0x0001` | CIR | u64 bps |
| `0x0002` | PIR | u64 bps |
| `0x0004` | CBS | u64 bytes |
| `0x0008` | PBS | u64 bytes |
| `0x0010` | priority | u16 |
| `0x0020` | match_rule | u16 length + UTF-8 |
| `0x0040` | parent_queue_id | u16 length + UTF-8 |
| `0x0080` | timestamp_us | u64 |

未知位（如 `0x0100`）表示未知必填字段，返回 `E_FORMAT_UNSUPPORTED`。每条记录 CRC32 只覆盖该条 `body`；任一 CRC 失败返回 `E_CRC_MISMATCH`，整个输入不提交。

v1 记录不带 PIR/PBS 位；缺失的 PIR/PBS 分别按 `CIR`、`CBS` 补齐并记录降级明细。

## 4. 版本兼容矩阵

| 格式 | v1 | v2 | v3 | v4+ |
|---|---|---|---|---|
| binary | 降级接受 | 接受 | 接受 | 拒绝 |
| jsonl | 降级接受 | 接受 | 接受 | 拒绝 |

拒绝业务码固定为 `E_FORMAT_UNSUPPORTED`，并声明 `partial_import=false`。

## 5. 时间戳截断

内部表示始终为整数微秒。调用方可在导入时声明：

- `preserve_us`：保留到微秒；不足 1 µs 的部分朝零截断。
- `floor_ms` / `truncate_ms`：向负无穷方向对齐到毫秒边界。
- `ceil_ms`：向正无穷方向对齐。
- `nearest_ms`：半毫秒向上对齐。
- `toward_zero_ms`：朝零对齐。

导入报告的 `timestamp_normalization.records` 对每条记录给出：

```text
normalized_timestamp_us
rounding_bit                   # 1 表示发生了不可逆精度变化
rounding_applied
rounding_remainder_us = normalized - exact_source_us
rounding_direction
```

顶层 `cumulative_rounding_error_us` 是各记录余数的 Decimal 总和。
