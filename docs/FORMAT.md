# 快照格式与兼容规则

## 1. 语义模型

内部统一使用以下表示：

- 权重：`weight_permille`，范围 `1..1000` 的整数。
- 时间：Unix 毫秒整数 `*_at_ms`。
- 探测方式：`http`、`tcp`、`lease`。
- 探测参数：JSON 对象，至少包含 `timeout_ms`；HTTP 还包含 `expected_status` 与 `expected_body_contains`。
- 探测窗口：`probe_window_ms`，v2 字段，实际单次探测超时取 `min(timeout_ms, probe_window_ms)`。

格式层只负责表示，归一化层负责把版本差异变成上述语义。转换流程因此固定为：

```text
decode -> negotiate -> normalize -> conflict/import 或 encode(target)
```

## 2. 紧凑二进制

### 2.1 文件头（20 字节）

| 偏移 | 长度 | 字段 | 说明 |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `SRMW` |
| 4 | 1 | version | `1` 或 `2`；更高版本拒绝 |
| 5 | 1 | endianness | `1` 小端，`2` 大端 |
| 6 | 1 | kind | `1` registration，`2` probe_result |
| 7 | 1 | flags | bit 0 = 含未知必填字段 |
| 8 | 4 | record_count | 按端序标记编码 |
| 12 | 8 | created_at_ms | 按端序标记编码，当前保留为 0 |

文件头中的固定字段按网络序读取；`record_count`、`created_at_ms`、记录长度、CRC、记录内数值均按端序标记解释。

### 2.2 记录帧

```text
uint32 payload_length
byte[payload_length] payload
uint32 crc32(payload)
```

每条记录独立校验。长度不足、CRC 不符、payload 尾随字节、记录数与实际不符都返回 `E_FORMAT_CORRUPT`，并在 `details.record_index`（0 起）与 `details.record_number`（1 起）指出记录序号。

### 2.3 payload

payload 首先是一个按端序编码的 `uint16` 可选字段位图，随后是字段：

- 字符串：`uint16 length + UTF-8 bytes`。
- 探测参数：`uint32 length + UTF-8 JSON object`。
- 注册记录：service_name、instance_id、endpoint、weight、lease_ttl_ms、probe_method、registered_at_ms。
  - v1 weight 为 IEEE-754 float32 比例值。
  - v2 weight 为 uint16 千分比。
- 探测结果：公共字段、checked_at_ms、success、status、outcome、latency_ms。

注册记录位图：

| 位 | 字段 |
|---:|---|
| 0 | probe_params |
| 1 | lease_version |
| 2 | probe_window_ms |
| 3 | last_heartbeat_at_ms |

探测结果位图：

| 位 | 字段 |
|---:|---|
| 0 | probe_params |
| 1 | lease_version |
| 2 | probe_window_ms |
| 3 | error_code |
| 4 | detail |

## 3. 分片 JSONL

每个非空行是一个对象，每行都包含服务与探测字段；`sequence` 用于跨分片恢复全局顺序。

```json
{"format_version":2,"kind":"registration","sequence":0,"service_name":"orders","instance_id":"orders-1","endpoint":"http://127.0.0.1:28765/health/ok","weight":875,"lease_ttl_ms":15000,"probe_method":"http","probe_params":{"timeout_ms":800,"expected_status":200,"expected_body_contains":"OK"},"registered_at_us":1700000100000000,"lease_version":7,"probe_window_ms":600}
```

- JSONL 时间字段使用微秒（`*_us`），也接受 `*_ms`，但同一记录不能混用。
- v1 JSONL 的 `weight` 是浮点比例；v2 是整数千分比。
- 一行 JSON 损坏、必填空字段、格式版本混用、类型错误均不会被跳过；错误带全局记录序号。
- 多分片是独立的 UTF-8 JSONL 字节串列表；解码后按 `sequence` 排序。

## 4. 兼容矩阵

| 输入 | v1 | v2 | v3+ |
|---|---|---|---|
| binary | 降级接受 | 接受 | 拒绝 `E_FORMAT_UNSUPPORTED` |
| JSONL | 降级接受 | 接受 | 拒绝 `E_FORMAT_UNSUPPORTED` |

显式 matrix 由 `GET /api/v1/compatibility` 返回。v3+ 或设置未知必填字段位时，不进入归一化、不写批次、不修改实例。

## 5. 归一化策略

### 权重

- v2 千分比必须是整数且位于 `1..1000`；浮点数即使是 `750.0` 也拒绝。
- v1 浮点比例必须由调用方声明：

```json
{"source_scale":"ratio","rounding_rule":"half_up","declared":true}
```

支持 `half_up`、`floor`、`ceil`、`truncate`。报告逐条输出源值、目标千分比和舍入余量。负值、NaN/Infinity、比例大于 1、四舍五入后为 0 或超过 1000 均返回 `E_WEIGHT_INVALID`。未声明策略时，浮点比例或浮点/定点混用返回 `E_WEIGHT_POLICY_REQUIRED`。

### 时间

- 二进制字段已是毫秒，不再截断。
- JSONL 微秒转毫秒前必须声明：

```json
{"truncation_rule":"floor","declared":true}
```

支持 `floor`、`ceil`、`half_up`；报告输出每条记录的微秒余数。缺少声明返回 `E_TIME_POLICY_REQUIRED`。
