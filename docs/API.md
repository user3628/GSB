# 本地 HTTP API

基址：`http://127.0.0.1:18080`。所有成功响应包含 `ok: true`；业务失败均为：

```json
{"ok":false,"error":{"code":"...","message":"...","details":{}}}
```

## 健康与格式

### `GET /api/health`

返回数据库路径、恢复是否执行、当前快照序号/校验和/策略数。

### `GET /api/formats/compatibility`

返回显式版本兼容矩阵和当前版本 3。

### `POST /api/formats/negotiate`

```json
{"format":"auto","content_base64":"eyJmb3JtYXRfdmVyc2lvbiI6M30K"}
```

也可用 `content` 传 UTF-8 文本。返回检测格式、版本、大小端（二进制）、记录数和 `accept|degraded_accept|reject` 结论。

## 策略导入/导出

### `POST /api/policies/import`

```json
{
  "format": "auto",
  "content_base64": "eyJmb3JtYXRfdmVyc2lvbiI6M30K",
  "timestamp_truncation": "preserve_us",
  "snapshot_no_override": 2,
  "apply_snapshot": true
}
```

`content` 与 `content_base64` 二选一。`apply_snapshot=false` 时只解码校验。成功响应含规范化快照、导入报告、重复/降级/时间舍入明细及提交结果。

### `POST /api/policies/validate`

请求体同上；强制不提交，只返回报告。

### `GET /api/policies/export?format=binary&version=3&endianness=big`

直接返回文件字节，媒体类型为 `application/octet-stream` 或 `application/x-ndjson`。响应头包含 `X-QoS-Snapshot-Seq`、`X-QoS-Snapshot-No`、`X-QoS-Snapshot-Checksum`。

### `POST /api/policies/export`

```json
{"format":"jsonl","version":3,"response_encoding":"text"}
```

返回 base64；文本格式还可返回 `content`。二进制不能以文本返回。

## 快照

- `GET /api/snapshots/current`：当前完整快照。
- `GET /api/snapshots?limit=50`：已提交快照列表。
- `GET /api/snapshots/{seq}`：按 SQLite 快照序列号读取不可变历史版本。
- `POST /api/snapshots/load`：提交规范化内部策略。

快照加载失败（版本、依赖环、越界、非单调等）统一返回 `E_SNAPSHOT_INVALID`；`details.cause_code` 保留底层业务码，旧快照保持可用。

## 仿真与诊断

### `POST /api/simulations`

内联样本：

```json
{
  "relative_tolerance": "0.10",
  "samples": [
    {"timestamp_ms": 0, "queue_id": "voice", "packet_bytes": 1000, "color": "green"},
    {"timestamp_ms": 1, "queue_id": "video", "length_bytes": 1500, "color": "yellow"}
  ]
}
```

或本地样本：

```json
{"sample_name":"traffic.jsonl","relative_tolerance":"0.10"}
```

仿真批次始终先写意图并提交结果；返回 `batch_id`、完整逐包结果、逐队列统计和诊断。

### `POST /api/simulations/from-sample/{sample_name}`

仅允许 `samples/` 目录下直接的 `.json`/`.jsonl` 文件，禁止路径穿越。

### `GET /api/simulations/{batch_id}`

读取已持久化批次；强杀恢复后的中止批次 `result_json`/`diagnostics_json` 为 `null`。

### `GET /api/diagnostics/latest`

返回最近已提交诊断批次。无已提交批次时返回 404 业务错误。

## 恢复

### `GET /api/recovery/report`

返回启动时扫描结果、被中止意图、补记的中止仿真批次、最后完整快照及明确的事实边界说明。

## 常见业务码

| 业务码 | 含义 |
|---|---|
| `E_FORMAT_INVALID` | 魔数、JSON、长度、UTF-8 等结构错误 |
| `E_FORMAT_UNSUPPORTED` | 高版本或未知必填字段 |
| `E_CRC_MISMATCH` | 头部或记录 CRC32 不匹配 |
| `E_UNIT_PRECISION` | 单位换算无法得到整数 |
| `E_UNIT_OVERFLOW` | 超过无符号 64 位或持久化范围 |
| `E_DEPENDENCY_CYCLE` | 父依赖成环 |
| `E_RATE_EXCEEDED` | 子整形器速率超过父整形器 |
| `E_PARENT_NOT_FOUND` | 引用了不存在的父整形器 |
| `E_POLICY_INVALID` | 策略字段关系非法 |
| `E_SNAPSHOT_INVALID` | 快照版本/序号/内容/校验失败 |
| `E_SNAPSHOT_MISSING` | 尚无完整快照 |
| `E_SAMPLE_INVALID` | 流量样本非法 |
| `E_REQUEST_INVALID` | HTTP 模型校验失败 |
