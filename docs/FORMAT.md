# 格式与兼容规范

## 1. 紧凑二进制格式

### 1.1 全局头（64 字节固定部分）

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `MSPB` |
| 4 | 1 | formatVersion | 格式版本 |
| 5 | 1 | endian | `0` 小端，`1` 大端；作用于其后所有整数 |
| 6 | 2 | flags | 必须为 0 |
| 8 | 8 | snapshotVersion | 单调递增快照版本 |
| 16 | 8 | generatedAtMs | 快照生成时间，Unix 毫秒 |
| 24 | 4 | recordCount | 记录数 |
| 28 | 4 | snapshotIDLen | 紧随头部的快照 ID 字节数 |
| 32 | 32 | digest | SHA-256 原始摘要 |
| 64 | N | snapshotID | UTF-8 字节 |

整体摘要计算范围：

```text
SHA256(header[0:32] || snapshotID || record[0] || record[1] || ...)
```

摘要槽 `header[32:64]` 不参与计算。因此版本号、生成时间、记录数、快照 ID 与全部记录都被绑定。头部或摘要被篡改时返回 `E_FORMAT_CORRUPT`，偏移为 `32`。

### 1.2 记录帧

| 相对偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `MSPR` |
| 4 | 1 | recordVersion | 当前为 `1` |
| 5 | 2 | optionalBitmap | 可选字段存在位图 |
| 7 | 1 | flags | 当前必须为 0 |
| 8 | 4 | payloadLen | 载荷字节数，不含本字段与 CRC |
| 12 | payloadLen | payload | 字段序列 |
| 12+payloadLen | 4 | crc32 | IEEE CRC32，覆盖记录头与载荷 |

错误定位：

- 记录魔数错误：记录起始偏移。
- 长度前缀越界/截断：长度字段偏移 `recordStart+8`。
- CRC 不一致：CRC 字段偏移。
- 记录数量之后仍有数据：剩余数据起始偏移。
- 解码器先完成全部帧校验，再返回任何策略。

### 1.3 记录载荷

所有字符串使用 `uint32` 字节长度前缀。字段顺序固定：

1. `policyId`
2. `virtualServiceId`
3. `policyRevision` u64
4. `lastWriteMs` i64
5. `match.method`
6. `match.pathPrefix`
7. header 数量 u16，随后按 key 字典序的 key/value 字符串对
8. 按 bitmap 存在与否依次出现：
   - `rateLimitBps` u64
   - `timeoutMs` u64
   - `circuitWindowMs` u64
   - `rateWindowMs` u64
   - `capturedAtMs` i64
   - `windowAlignment` u8：`0=none`、`1=up`、`2=down`

Bitmap：

| 位 | 字段 |
|---:|---|
| 0 | rate limit |
| 1 | timeout |
| 2 | circuit window |
| 3 | rate window |
| 4 | captured timestamp |
| 5 | window alignment |
| 6-15 | 保留；出现非零位视为未知必填字段并拒绝 |

二进制速率单位固定为 bps，时间单位固定为毫秒。

## 2. 分片 JSON Lines 格式

首行必须是 header，中间每行一个 policy，末行必须是 trailer。

### 2.1 header

```json
{"type":"header","format":"mesh-policy-jsonl","formatVersion":2,"snapshotVersion":2,"snapshotId":"...","generatedAtMs":1760000100000,"policyCount":4}
```

### 2.2 policy

必填字段：

- `type` = `policy`
- `policyId`
- `virtualServiceId`
- `match.method`
- `match.pathPrefix`

常用字段：

- `policyRevision`
- `lastWriteMs`
- `rateLimitKbps`：JSON 源单位 kbps
- `timeoutUs`、`circuitWindowUs`、`rateWindowUs`：JSON 源单位微秒
- `capturedAtUs`：JSON 源单位微秒
- `windowAlignment`：`none|up|down`
- `optional`：本行实际出现的可选字段名数组，例如 `["rateLimitKbps","capturedAtUs"]`
- `requiredFeatures`：高版本必填特性列表

未知顶层字段会被收集；若其出现在 `requiredFeatures` 中，裁决拒绝并在 `unknownFields` 中返回字段名。

### 2.3 trailer 与摘要

```json
{"type":"trailer","policyCount":4,"digest":"sha256:..."}
```

摘要范围：

```text
SHA256(header 原始文本 || policy line 1 原始文本 || ... || policy line N 原始文本)
```

去除行尾 `LF`，若存在 `CRLF` 则同时去除 `CR`；不包含尾部换行。header 元数据也参与摘要，因此篡改版本号同样会触发 `E_FORMAT_CORRUPT`。

### 2.4 行级错误

- 空白行、非法 JSON、header/trailer 类型错误：返回所在字节偏移与行号。
- 记录数不一致：返回 header 行偏移。
- 摘要不一致：返回 trailer 行偏移。

## 3. 显式兼容矩阵

矩阵文件：`internal/compat/matrix.json`，通过 `go:embed` 编入二进制。

当前规则：

| 格式 | 版本 | 结论 |
|---|---:|---|
| binary | 1 | `downgrade_accept` |
| binary | 2 | `accept` |
| binary | 3 | `reject` |
| jsonl | 1 | `downgrade_accept` |
| jsonl | 2 | `accept` |
| jsonl | 3 | `reject` |

未声明版本一律 `reject`，错误码 `E_FORMAT_UNSUPPORTED`。矩阵同时声明默认值：

```json
{
  "rateLimitBps": 1000000,
  "timeoutMs": 1500,
  "circuitWindowMs": 30000,
  "rateWindowMs": 60000,
  "windowAlignment": "up",
  "policyRevision": 1
}
```

裁决输出不含时间戳或随机值，同一输入重复协商得到逐字一致的 JSON。

## 4. 归一化规则

- `kbps -> bps`：乘以 1000，使用 `uint64` 上限检查。
- 微秒时长 -> 毫秒：按调用方选择 `floor|ceil|nearest`，并检查 `int64` 上限。
- 微秒时间戳：
  - `remainderUs = timestampUs % 1000`
  - 非零余数时 `roundingBit = 1`
  - `errorUs = convertedMs*1000 - sourceUs`
  - 每条策略输出当前累计误差；快照输出最终累计误差
- 窗口边界：
  - `none`：使用归一化时间戳
  - `down`：向下对齐到窗口整数倍
  - `up`：向上对齐到窗口整数倍
  - 同时检查开始/结束时间溢出
