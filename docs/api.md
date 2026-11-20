# HTTP API

服务默认监听 `127.0.0.1:8765`，拒绝非回环地址。请求与响应均为 UTF-8 JSON，成功的顶层字段为 `ok: true`，业务失败为 `ok: false`。

## 事件摄入

`POST /v1/events/batches`

```json
{
  "batch_id": "local-b001",
  "sequence": 1,
  "events": [
    {
      "timestamp_ms": 1700000000100,
      "route": "/checkout",
      "success": true,
      "latency_us": 28000
    }
  ]
}
```

规则：

- `batch_id + sequence` 是幂等键。
- 同键同内容重复提交返回首次提交结果并附 `duplicate: true`。
- 同键不同内容返回 `409 E_CONFLICTING_REPORT`，保留首次提交。
- 响应中的路由结果链接同一复合版本下的窗口、熔断、配额版本。

## 批次查询

`GET /v1/events/batches/{batch_id}/{sequence}`

返回 `COMMITTED` 或 `ABORTED` 状态、内容哈希、首次提交结果与中止原因。不存在时返回 `E_BATCH_NOT_FOUND`。

## 限流裁决

`POST /v1/limit/decision`

```json
{
  "route": "/large/api/checkout",
  "current_concurrency": 32,
  "budget_watermark": "0.70",
  "target_latency_us": 50000,
  "as_of_ms": 1700000060000,
  "window_version": 1,
  "composite_version": 18
}
```

`window_version`、`window_start_ms`、`composite_version` 可选；一旦提供且不是当前版本，返回 `E_STALE_WINDOW`。

响应关键字段：

- `decision.allowed/code/reason/current_limit/headroom`；
- `composite_version`；
- `window.version/version_id`；
- `breaker.version/version_id/state`；
- `quota.version/version_id/derivation`；
- `consistency.same_window_version_in_limiter_and_breaker`。

`OPEN` 状态直接拒绝并标记 `DENIED_BY_BREAKER`；`HALF_OPEN` 使用降权配额和至少一个探针名额。当前窗口 P95 样本不足返回 `E_INSUFFICIENT_SAMPLES`。

## 熔断状态

`GET /v1/circuit/state?route=/path&as_of_ms=...`

返回持久化 `state`、`breaker_version`、`opened_at_ms`、`next_probe_at_ms`、`cooldown_elapsed` 与 `effective_state_on_next_evaluation`。该 GET 不修改状态。

## 手动迁移

`POST /v1/circuit/transition`

```json
{
  "route": "/checkout",
  "to_state": "OPEN",
  "occurred_at_ms": 1700000030000,
  "reason": "SRE_MANUAL_TRIP"
}
```

合法路径：

- `CLOSED -> OPEN`
- `OPEN -> HALF_OPEN`，且 `occurred_at_ms >= next_probe_at_ms`
- `HALF_OPEN -> CLOSED`
- `HALF_OPEN -> OPEN`

其他路径返回 `E_INVALID_TRANSITION`，事务回滚且不修改已持久化状态。同状态请求幂等返回 `changed: false`。

## 窗口查询

`GET /v1/windows?route=/path&limit=20`

返回按窗口结束时间倒序的摘要，同时给出 P50/P95/P99 最近秩与线性值、成功率、最大方法偏差和误差契约。

`GET /v1/windows/stats?route=/path&window_start_ms=...&version=...&strict=P99`

`strict` 可为 `true`/`all`/`P50`/`P95`/`P99`。任一目标分位低于样本下限时返回 `E_INSUFFICIENT_SAMPLES`。

## 配额推导

`GET /v1/quota/derivation?route=/path&composite_version=...`

返回固定点缩放步骤、基础配额、并发因子、目标延迟因子、熔断权重、阻尼前后值、取舍原因、误差上界和多窗口传播明细。请求非当前复合版本返回 `E_STALE_WINDOW`。

## 恢复报告

`GET /v1/recovery/report` 或 `GET /v1/recovery/report?run_id=12`

返回数据库中最近一次或指定启动恢复事实。强制终止的旧进程不会生成退出报告；该接口不承诺此类报告。

## 错误码

| 业务码 | 典型 HTTP | 含义 |
|---|---:|---|
| `E_CONFLICTING_REPORT` | 409 | 同一批次键内容冲突 |
| `E_INVALID_TRANSITION` | 409 | 非法或冷却未满足的状态迁移 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 分位/裁决样本不足 |
| `E_STALE_WINDOW` | 409/422 | 窗口或复合版本不是当前版本，或窗口已陈旧 |
| `E_BATCH_NOT_FOUND` | 404 | 批次不存在 |
| `E_ROUTE_NOT_FOUND` | 404 | 路由没有已提交窗口 |
| `E_VALIDATION` | 400/413 | 请求字段或大小不合法 |
| `E_METHOD_NOT_ALLOWED` | 405 | 方法不支持 |
| `E_NOT_FOUND` | 404 | 路径或恢复运行不存在 |

