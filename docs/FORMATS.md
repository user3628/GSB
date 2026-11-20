# 持久化格式规范

## 1. 紧凑二进制格式 `.kvsb`

### 1.1 文件头（12 字节）

| 偏移 | 长度 | 字段 | 说明 |
|---|---:|---|---|
| 0 | 4 | magic | 固定 ASCII `KVSB` |
| 4 | 1 | format_version | 当前为 `0x01` |
| 5 | 1 | endianness | `0x01` 小端，`0x02` 大端 |
| 6 | 2 | reserved | 必须为 `0x0000` |
| 8 | 4 | record_count | 无符号 32 位，按 endianness 编码 |

### 1.2 记录帧

每条记录：

| 字段 | 长度 | 说明 |
|---|---:|---|
| payload_length | 4 | `payload` 字节数，按文件 endianness 编码 |
| payload | N | 见下 |
| crc32 | 4 | `zlib.crc32(payload) & 0xFFFFFFFF`，按文件 endianness 编码 |

`payload`：

| 字段 | 长度 | 说明 |
|---|---:|---|
| key_length | 4 | UTF-8 key 字节数 |
| key | K | 非空 UTF-8 |
| value_length | 4 | UTF-8 JSON value 字节数 |
| value_json | V | 规范化 JSON 文本，可为任意 JSON 值 |
| version | 8 | 有符号 64 位，非负 |
| written_at_ms | 8 | Unix epoch 毫秒，有符号 64 位 |
| expires_at_ms | 8 | 绝对过期 Unix epoch 毫秒；`-1` 表示永不过期 |

### 1.3 校验与导入原子性

解码器先校验魔数、版本、endianness、字段布局、记录数与文件末尾一致性。任何记录的 payload CRC32 不匹配都返回 `E_FORMAT_INVALID`。所有记录在进入导入冲突解决前已完成校验，因此失败时不会导入已经通过校验的前缀记录。

导入时 `written_at_ms * 1000` 与 `expires_at_ms * 1000` 归一化为内部微秒；`-1` 归一化为 `null`。

## 2. JSON Lines 格式 `.jsonl`

UTF-8 文本，每行一个 JSON 对象，空行忽略。当前导出字段：

| 字段 | 必需 | 语义 |
|---|---|---|
| `key` | 是 | 非空字符串 |
| `value` | 否 | 任意 JSON 值；缺省补 `null` 并报告 |
| `version` | 否 | 非负整数；缺省补 `0` 并报告 |
| `written_at_us` | 二选一 | Unix epoch 微秒整数 |
| `written_at_ms` | 二选一 | Unix epoch 毫秒整数，导入乘以 1000 |
| `expires_at_us` | 可选 | 绝对过期微秒；显式 `null` 表示永不过期 |
| `expires_at_ms` | 可选 | 绝对过期毫秒，导入乘以 1000 |
| `ttl_us` | 可选 | 相对该记录 `written_at_us` 的微秒 TTL |
| `ttl_ms` | 可选 | 相对该记录 `written_at_us` 的毫秒 TTL |
| `deleted` | 可选 | 外部导入只允许 `false`/缺省；墓碑不从外部格式导入 |

规则：

- `written_at_us` 与 `written_at_ms` 互斥。
- 绝对过期字段与 TTL 字段互斥；多个绝对字段或 TTL 字段同时出现会返回 `E_FORMAT_INVALID`。
- 缺少写入时间时，使用导入开始时刻并在该记录的 `filled_fields` 中记录 `written_at_us`。
- 缺少 `expires_at*` 和 TTL 时，补 `expires_at_us = null`，并记录补全字段。
- TTL 基准是记录自带的写入时间：`expires_at_us = written_at_us + ttl`。
- `expires_at_us` 不得早于 `written_at_us`；等于写入时间表示立即到期。
- 未知字段不丢弃：保存在内部 `extras`，导入报告逐记录与汇总列出，JSON 导出时写回顶层。

## 3. 协商报告

导入响应包含 `negotiation`：

- `input_format`: `compact_binary` 或 `json_lines`
- `detected_by`: 魔数或首个非空字符 `{`
- `record_count`
- `source_timestamp_unit` / `target_timestamp_unit`
- `expiry_semantics`: `absolute`、`relative_ttl`、`mixed` 或 `none`
- `unknown_fields`
- `filled_fields`
- `records_with_filled_fields`
- `records_with_unknown_fields`
- 二进制额外给出 `format_version`、`endianness`、`checksums_verified`

每个导入结果记录还包含 `filled_fields`、`transformations`、`unknown_fields`、`outcome`、`reason` 和发生冲突时的 `existing` 元数据。

## 4. 内部段文件

内部段文件仅供本组件持久化，不是外部互操作格式。文件头为：

```text
"KVSG0001" | header_length(u32 BE) | header_json | crc32(header_json)(u32 BE)
```

帧为：

```text
payload_length(u32 BE) | payload_json | crc32(payload_json)(u32 BE)
```

头部 JSON 含 `format_version`、`segment_id`、`kind`、`snapshot_seq`、`generated_at_us`、`record_endianness`。帧 payload 含内部记录字段及 `deleted`、`extras`。读取索引项时总是按偏移重读并校验 CRC，不信任缓存值。
