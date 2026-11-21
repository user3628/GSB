# HTTP API 契约

基础地址：`http://127.0.0.1:8765`。请求体为 JSON；成功响应包含 `"ok": true`；失败响应使用统一错误封装。

## 主题

### `POST /api/v1/topics`

```json
{
  "topic": "orders",
  "partitions": 3,
  "group_id": "analytics",
  "min_retained_seq": 1
}
```

同名同配置重复声明为幂等成功，`created` 为 `false`；配置不同返回 `E_CONFLICTING_TOPIC`。

### `GET /api/v1/topics/{topic}`

返回主题配置及每个分区的 `next_seq`、保留下界和当前事件数。

## 发布

### `POST /api/v1/topics/{topic}/events`

别名：`POST /api/v1/topics/{topic}/publish`

```json
{
  "batch_id": "batch-001",
  "events": [
    {
      "event_id": "evt-001",
      "partition_key": "customer-001",
      "event_time": "2026-01-15T08:00:00Z",
      "key": "alpha",
      "measure": 12.5,
      "payload": {"source": "local"}
    },
    {
      "event_id": "evt-002",
      "partition_key": "customer-002",
      "event_time": "2026-01-15T08:00:01Z",
      "key": "beta",
      "measure": null
    }
  ]
}
```

字段规则：

- 顶层 `batch_id` 可被事件自己的 `batch_id` 覆盖；最终每个事件都必须有批次标识。
- `key` 与 `event_key` 均可；还支持从 `body.key` 读取。
- `measure` 同时接受 `metric`、`value` 或 `body.measure`。缺失和 `null` 等价于计数加一、求和加零。
- `payload` 同时接受 `data` 或 `body.payload`，必须是对象。
- `event_time` 接受 ISO-8601 或 epoch 秒；总线规范化为 UTC 微秒格式。
- `body_digest` 可选；提供时必须是规范化事件体的 SHA-256。

成功响应：

```json
{
  "ok": true,
  "batch_id": "batch-001",
  "accepted": 2,
  "duplicates": 0,
  "events": [
    {
      "event_id": "evt-001",
      "partition": 1,
      "production_seq": 1,
      "state": "ACCEPTED",
      "body_digest": "...",
      "batch_id": "batch-001",
      "duplicate": false
    }
  ],
  "intent_id": "..."
}
```

## 查询事件

### `GET /api/v1/events/{event_id}`

返回事件主体、`state_trace`、`deliveries` 和 `confirmations`。

### `GET /api/v1/events?topic=orders&partition=0&state=ACKED&limit=200`

按主题、分区和状态过滤，最大 `limit=5000`。

## 拉取和确认

### `POST /api/v1/consumers/poll`

```json
{
  "topic": "orders",
  "group_id": "analytics",
  "partition": 0,
  "consumer_id": "worker-1",
  "lease_seconds": 30
}
```

`lease_seconds` 范围 `0.05..3600`。成功投递返回事件、位点、重放窗口和租约令牌。分区已有有效租约时返回 `409 E_LEASE_HELD`。

无事件时返回：

```json
{
  "ok": true,
  "event": null,
  "reason": "NO_EVENT",
  "offset": 5,
  "replaying": false,
  "lease": null
}
```

重放屏障未解除时 `reason` 为 `REPLAY_BARRIER`。

### `POST /api/v1/consumers/ack`

```json
{
  "topic": "orders",
  "group_id": "analytics",
  "partition": 0,
  "consumer_id": "worker-1",
  "event_id": "evt-001",
  "lease_token": "..."
}
```

重复确认同一 ACKED 事件返回 `"idempotent": true`，不重复更新位点或聚合。没有活动投递返回 `E_NOT_DISPATCHED`；租约被其他消费者持有时返回 `E_LEASE_HELD`。

## 位点

### `GET /api/v1/consumers/offset?topic=orders&group_id=analytics&partition=0`

```json
{
  "ok": true,
  "topic": "orders",
  "group_id": "analytics",
  "partition": 0,
  "offset": 4,
  "replaying": true,
  "replay_target": 1,
  "replay_end": 9,
  "version": 12,
  "updated_at": "..."
}
```

`offset` 是下一个待消费序号。`replay_target <= offset < replay_end` 时处于重放窗口。

### `POST /api/v1/consumers/offset/rewind`

```json
{
  "topic": "orders",
  "group_id": "analytics",
  "partition": 0,
  "target_seq": 1,
  "reason": "operator_replay"
}
```

目标必须是已提交序号。早于保留下界返回 `E_OFFSET_EXPIRED`；不存在或向前跳跃返回 `E_OFFSET_NOT_FOUND`。成功响应包含旧位点、新位点、重放边界、回退确认数和回退事件数。

## 聚合和快照

### `GET /api/v1/aggregates?topic=orders&group_id=analytics`

```json
{
  "ok": true,
  "topic": "orders",
  "group_id": "analytics",
  "groups": [
    {"key": "alpha", "count": 7, "sum": 2000000000007.2, "compensation": -0.0001953125}
  ]
}
```

`sum` 是 `measure_sum + compensation`，`groups` 按 `key` 字典序稳定排序。

### `GET /api/v1/snapshot?topic=orders&group_id=analytics`

在一个只读事务快照内同时返回 `events`、`offsets`、`aggregates`，用于验证多表一致可见性。

## 死信

### `POST /api/v1/events/{event_id}/dead-letter`

```json
{
  "topic": "orders",
  "group_id": "analytics",
  "partition": 0,
  "event_id": "evt-001",
  "reason": "poison_event"
}
```

仅允许 `ACCEPTED`、`DISPATCHED`、`REDELIVERED` 到 `DEAD_LETTERED`。ACKED 事件执行此操作返回 `E_INVALID_TRANSITION`。若该事件正是位点头部，死信事务推进位点。

## 验证和恢复

### `GET /api/v1/verify?topic=orders`

返回结构化检查列表。顶层 `ok` 只有在所有检查通过时才为 `true`。检查使用一个 SQLite 只读快照。

### `GET /api/v1/recovery/reports/latest`

返回最近一次服务启动时写入的恢复报告，包括 pending 意图、释放租约、位点边界检查、回退事件/确认和聚合重建数量。

## 统一错误

```json
{
  "ok": false,
  "error": {
    "code": "E_OFFSET_EXPIRED",
    "message": "回拨目标早于分区最早保留序号。",
    "context": {"target_seq": 0, "min_retained_seq": 1}
  }
}
```

即使路由不存在或请求结构校验失败，也使用同样的 JSON 错误封装。

## 测试钩子

`test_crash_after_intent: true` 只用于发布、拉取、确认或回拨请求。服务必须以 `--enable-test-hooks` 启动，否则返回 `E_TEST_HOOK_DISABLED`。钩子在意图已提交、业务事务尚未开始时执行进程级强制终止，用于验证恢复，不用于生产环境。
