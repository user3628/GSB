# 格式规范与版本兼容矩阵

## 1. 内部规范表示

导入后所有记录进入同一内部表示：

- 消息 ID、队列名、正文、内容类型、投递状态、记录版本、最后写入时间与扩展字段。
- `produced_at_us` 与 `expires_at_us` 均保存为整数微秒。
- 本组件的所有时间源都先截断到毫秒再乘 1000，因此内部时间落在毫秒网格上。
- JSONL 的 `ttl_ms` 是相对于该行 `produced_at_ms` 的 TTL；导入时计算 `expires_at_us = (produced_at_ms + ttl_ms) * 1000`。
- 二进制直接携带绝对 `produced_at_us` 与可选 `expires_at_us`。

`sub_ms_policy` 是调用方显式声明的亚毫秒处理规则：

- `truncate`（默认）：向下截断到毫秒，并在导入报告 `sub_ms_normalizations` 中列出原始值、归一化值与丢弃的微秒数。
- `reject`：发现亚毫秒值即返回 `E_TIME_PRECISION_UNSUPPORTED`，不改变任何持久化状态。

## 2. 紧凑二进制格式

### 2.1 文件头

文件头固定 16 字节，小端序：

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | magic | 固定 ASCII `MSMQ` |
| 4 | 1 | version | 格式版本，当前为 `2`；支持读取 `1`、`2` |
| 5 | 1 | flags | bit0 表示“含未知必填扩展”；其余位保留必须为 0 |
| 6 | 2 | reserved | 保留，当前写 0 |
| 8 | 4 | metadata_len | UTF-8 JSON 元数据字节数，uint32 |
| 12 | 4 | record_count | 记录数，uint32，导入前先检查批次上限 |

元数据 JSON：

```json
{
  "queues": [
    {
      "name": "demo-orders",
      "semantics": "at_least_once",
      "visibility_timeout_ms": 500,
      "version": 1,
      "extensions": {"owner": "local-sample"}
    }
  ],
  "extensions": {}
}
```

队列对象未知的可选键合并进 `extensions`，导入后写回。

### 2.2 记录

每条记录以 uint32 `record_len` 开头。`record_len` 覆盖其后的记录信封与载荷，不包含自身 4 字节。

```text
[u32 record_len]
[u16 field_bitmap]
[u16 schema_version]
[u32 crc32]
[payload]
```

- CRC32 使用标准 IEEE CRC32，覆盖完整 `payload`，不覆盖长度前缀、位图、schema 版本或 CRC 字段本身。
- `schema_version` 当前与文件版本一致；解码时以文件版本和位图决定字段规则。
- `record_len` 的安全上限为 1 MiB，超过即 `E_FORMAT_INVALID`。

### 2.3 可选字段存在位图

| bit | 字段 | 编码 | 缺失默认值 |
|---:|---|---|---|
| 0 | `message_id` | text | 必填 |
| 1 | `queue` | text | 必填 |
| 2 | `body` | text | 必填 |
| 3 | `produced_at_us` | u64 | 必填 |
| 4 | `expires_at_us` | u64 | `produced_at_us + 60_000_000` |
| 5 | `content_type` | text | `text/plain` |
| 6 | `state` | u8：0 pending、1 leased、2 acked、3 expired | `pending` |
| 7 | `record_version` | u32 | `1` |
| 8 | `last_write_at_us` | u64 | `produced_at_us` |
| 9 | `delivery_count` | u32 | `0` |
| 10 | `extensions` | JSON text | `{}` |
| 11 | `lease` | JSON text：`owner`、`token`、`expires_at_us` | 空租约 |

`text` 编码为 `[u32 byte_length][UTF-8 bytes]`。字段按 bit 升序写入载荷。bit 0-3 必须存在。v1 只允许 bit 0-3；v2 允许 bit 0-11。未知位图位返回 `E_FORMAT_INVALID`，未知可选业务字段放入 `extensions` 对象。

### 2.4 v1 降级

紧凑 v1 文件合法但只能声明基本字段。导入结论是 `degrade_accept`：

- 缺失到期时间按记录产生时间 + 60 秒填充。
- 缺失内容类型、状态、记录版本、最后写入时间、投递次数按矩阵默认值填充。
- 每个填充项进入 `fields_filled`；其中版本缺失造成的关键降级进入 `degradations`。
- 若有亚毫秒时间，按 `sub_ms_policy` 处理并写入 `sub_ms_normalizations`。

## 3. 分片 JSON 行格式

第一行是 manifest，之后每行是一条记录，使用 UTF-8 与短分隔符 JSON。

### 3.1 Manifest

```json
{
  "_type": "mq_manifest",
  "format": "jsonl",
  "version": 1,
  "queues": [
    {
      "name": "demo-orders",
      "semantics": "at_least_once",
      "visibility_timeout_ms": 500,
      "version": 1,
      "extensions": {}
    }
  ],
  "extensions": {}
}
```

导入器也容忍没有 manifest 的记录流：此时按 version 1 处理，并从记录中的队列名派生默认队列元数据（至少一次、30 秒可见性超时），同时在报告中标记填充项。

### 3.2 记录行

必填字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 消息标识 |
| `queue` | string | 队列归属 |
| `body` | string | UTF-8 消息体 |
| `produced_at_ms` | integer | 生产时间，毫秒 |
| `ttl_ms` | integer | 相对生产时间的 TTL，毫秒 |

`id` 是规范字段；导入器也接受 `message_id` 作为同义字段，但同一行同时给出且值不同会被拒绝。

可选字段：`content_type`、`state`、`record_version`、`last_write_at_ms`、`delivery_count`、`extensions`、`lease`。未知顶层键与 `extensions` 内键合并保留，下一次同格式导出时写回顶层。

JSONL 批次的记录上限不含 manifest 行。

## 4. 容量上限

| 格式 | 单条正文 | 单批记录 | 其他 |
|---|---:|---:|---|
| 紧凑二进制 v2 | 8192 字节 | 128 | 单记录载荷 1 MiB 安全上限 |
| JSONL v1 | 4096 字节 | 64 | 单行 65536 字节安全上限 |

超过单条正文限制返回 `E_RECORD_TOO_LARGE`；超过批次限制返回 `E_BATCH_SPLIT_REQUIRED`。编码与解码都不会静默截断正文或批次。导出目标格式容量不足时同样返回业务错误。

## 5. 显式兼容矩阵

| 格式 | 版本/条件 | 结论 | 行为 |
|---|---|---|---|
| compact | v1 | `degrade_accept` | 按声明默认值填充并记录降级 |
| compact | v2 | `accept` | 正常导入 |
| compact | >2 | `reject` / `E_FORMAT_INCOMPATIBLE` | 不改变状态 |
| compact | 任意版本 + flags bit0 | `reject` / `E_FORMAT_INCOMPATIBLE` | 未知必填扩展 |
| jsonl | v1 | `accept` | 正常导入 |
| jsonl | >1 | `reject` / `E_FORMAT_INCOMPATIBLE` | 不改变状态 |
| jsonl | `requires_unknown_extension=true` | `reject` / `E_FORMAT_INCOMPATIBLE` | 未知必填扩展 |

兼容检查发生在解析记录前。拒绝路径只留下审计导入报告，不写入队列或消息。

## 6. 往返等价

固定样本验证的等价字段是：

- `message_id`
- `queue`
- `body`
- `content_type`
- `produced_at_us`
- `expires_at_us`（即绝对过期时刻）
- `state`
- `record_version`
- `last_write_at_us`
- `delivery_count`
- `extensions`

两种格式从同一队列导出，再分别导入全新数据库，其规范快照与源快照完全一致，差异只允许出现在内部队列版本号、来源格式/版本等非语义审计字段。

