# HTTP API

服务默认监听 `http://127.0.0.1:8787`。成功响应：

```json
{"ok": true, "data": {}}
```

失败响应：

```json
{"ok": false, "error": {"code": "E_REQUEST_INVALID", "message": "...", "details": {}}}
```

接口均设置 `Cache-Control: no-store`，不访问外部资源。

## 提交裁决

`POST /api/v1/adjudications`

请求体为单个批次 JSON。成功解码与整批校验后总是返回 HTTP 200，批次中的逐条失败通过 `data.outcome`、`data.code` 和 `data.items[].code` 表达。结构/整数溢出错误返回 400；同 ID 不同内容返回 409。

关键响应字段：

```json
{
  "batch_id": "...",
  "mode": "full",
  "window_version": 7,
  "tolerance_config_version": "cfg-...",
  "outcome": "accepted | rejected | partial",
  "code": "OK | E_SIGNATURE_MISMATCH | E_TIMESTAMP_TOO_OLD | E_TIMESTAMP_AHEAD | E_NONCE_REPLAYED | E_PRECHECK_INCOMPLETE",
  "sample_rate": 1,
  "precheck_method": "all_fields | deterministic_prefix_v1",
  "confidence": "exact | bounded_partial | exact_failure_in_prefix",
  "tolerance_boundary_rule": "inclusive: ...",
  "idempotent_replay": false,
  "items": [{
    "request_id": "...",
    "result": "accepted | rejected | not_prechecked",
    "code": "OK",
    "offset_ms": 100,
    "tolerance_lower_bound_ms": -500,
    "tolerance_upper_bound_ms": 500,
    "triggered_boundary": "none | lower | upper",
    "boundary_inclusive": true,
    "first_occupied_at_ms": 1800000000000,
    "remaining_window_ms": 4900,
    "window_version": 7,
    "tolerance_config_version": "cfg-...",
    "signature_stage": {"status": "passed", "window_version": 7, "tolerance_config_version": "cfg-..."},
    "timestamp_stage": {"status": "passed", "window_version": 7, "tolerance_config_version": "cfg-..."},
    "nonce_stage": {"status": "failed", "code": "E_NONCE_REPLAYED", "window_version": 7, "tolerance_config_version": "cfg-..."}
  }]
}
```

## 读取已提交裁决

`GET /api/v1/adjudications/{batch_id}`

- 200：返回完整裁决。
- 404 `E_BATCH_NOT_FOUND`：没有已提交裁决。

读取只在完整提交之后可见。

## 一致性快照

`GET /api/v1/diagnostics/snapshot`

查询参数：

| 参数 | 说明 |
| --- | --- |
| `version` | 可选；窗口版本。空或 `0` 表示最新已提交版本 |
| `budget_ms` | 可选；缺省用配置默认值。显式 `0` 表示零预算部分结果 |
| `include` | 逗号分隔：`occupancy,nonces,trails`；缺省全部 |
| `client_id` | 可选，过滤占用与去重表 |
| `batch_id` | 可选，过滤轨迹 |
| `trail_limit` | 可选，默认 500，最大 2000 |

响应：

```json
{
  "snapshot_window_version": 10,
  "tolerance_config_version": "cfg-...",
  "as_of_ms": 1800000011000,
  "complete": false,
  "partial": true,
  "budget_ms": 0,
  "elapsed_ms": 0,
  "completed_sections": [],
  "incomplete_sections": ["occupancy", "nonces", "trails"],
  "reason": "budget_exhausted_before_read_transaction",
  "occupancy": null,
  "nonces": null,
  "trails": null
}
```

部分结果不会从其他版本补数据。`GET /api/v1/diagnostics/occupancy`、`/nonces`、`/trails` 使用同一语义，但只请求单个区段。

## fast/full 差异

`GET /api/v1/diagnostics/mode-differences?batch_id=...&budget_ms=40`

对完整裁决，按同一已提交版本与存储的 precheck limit 逐项投影快速模式。响应含 `full_result`、`full_code`、`fast_result`、`fast_code`、`differs`、`explanation`；未预检后缀的 fast 值为 `not_prechecked / E_PRECHECK_INCOMPLETE`。

对快速裁决，`full_result_available=false`，`full_code=NOT_PRE_EVALUATED`，不会把未检查项补成通过。零预算返回带版本标签的 `partial=true` 报告。

## 客户端版本不一致

`POST /api/v1/diagnostics/inconsistencies`

```json
{
  "batch_id": "06-sample-reclaim-base-006",
  "budget_ms": 40,
  "views": [
    {"client_id": "reader-old", "picked_window_version": 6},
    {"client_id": "reader-new", "picked_window_version": 8, "picked_config_version": "cfg-optional"}
  ]
}
```

响应为每个客户端视图给出实际/视图结论与差异字段：

```json
{
  "batch_id": "06-sample-reclaim-base-006",
  "window_version": 6,
  "tolerance_config_version": "cfg-...",
  "differs": true,
  "complete": true,
  "partial": false,
  "views": [{
    "client_id": "reader-new",
    "picked_window_version": 8,
    "picked_config_version": "cfg-...",
    "batch_window_version": 6,
    "decision_config_version": "cfg-...",
    "batch_visible_at_version": true,
    "decision_batch_code": "OK",
    "viewed_batch_code": "E_NONCE_REPLAYED",
    "differs": true,
    "differences": [{
      "field": "items[0].code",
      "decision_value": "OK",
      "client_viewed_value": "E_NONCE_REPLAYED",
      "explanation": "..."
    }],
    "explanation": "..."
  }]
}
```

`GET /api/v1/diagnostics/inconsistencies?batch_id=...&client_id=...&picked_window_version=...&budget_ms=...` 是单视图便捷形式。

## 恢复

- `GET /api/v1/diagnostics/recovery`：最近一次恢复报告。
- `GET /api/v1/diagnostics/recovery/history?limit=20`：历史恢复报告。

报告字段：`pending_intents`、`confirmed_committed`、`aborted`、`details[]`、`facts_only=true`。`details[]` 含意图、批次、计划版本、动作、决策是否存在与孤儿提交事实是否存在。

## 配置、健康与样本

- `GET /healthz`：健康状态与配置版本。
- `GET /api/v1/config`：不含密钥的配置视图。
- `GET /api/v1/samples`：内嵌样本文件名。
- `GET /api/v1/samples/{name}`：内嵌样本内容。

## 错误码与 HTTP 状态

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| `E_REQUEST_INVALID` | 400 | JSON/整数溢出或整批结构校验失败；未裁决 |
| `E_SIGNATURE_MISMATCH` | 200（逐条） | 签名摘要不一致 |
| `E_TIMESTAMP_TOO_OLD` | 200（逐条） | 早于闭区间下界 |
| `E_TIMESTAMP_AHEAD` | 200（逐条） | 晚于闭区间上界 |
| `E_NONCE_REPLAYED` | 200（逐条） | nonce 在有效窗口内重复 |
| `E_CONFLICTING_REQUEST` | 409 | 幂等键内容冲突，持久化结论不变 |
| `E_BATCH_NOT_FOUND` | 404 | 没有已提交裁决 |
| `E_VERSION_INVALID` | 400 | 请求了未提交/未来窗口版本 |
| `E_PRECHECK_INCOMPLETE` | 200（批次/逐条） | fast 未预检项，明确非通过 |
| `E_RECOVERY_UNAVAILABLE` | 503 | 恢复报告不可读 |
| `E_INTERNAL` | 500 | 内部错误 |
