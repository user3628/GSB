# HTTP API

成功响应：

```json
{"ok": true, "data": {}, "request_id": "req-..."}
```

失败响应：

```json
{"ok": false, "error": {"code": "E_...", "message": "...", "details": {}}}
```

所有变更接口接受可选 `request_id`。同一 `request_id` 与同一请求指纹重复调用会复用持久化结果；不同内容返回 `E_CONFLICTING_REQUEST`。

## 诊断与样本

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 交互诊断页 |
| GET | `/api/health` | 健康状态与最近恢复摘要 |
| GET | `/api/dashboard` | 单快照诊断数据 |
| POST | `/api/demo/seed` | 加载 `-seed` 指定的固定样本 |
| GET | `/api/state-transitions` | 状态迁移审计 |
| GET | `/api/recovery/latest` | 最近恢复报告 |
| GET | `/api/recovery/reports` | 恢复报告列表 |

## 主题与消息

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/topics` | 列出/创建主题 |
| GET/POST | `/api/messages` | 列出/发布消息 |
| POST | `/api/messages/{id}/dead-letter` | `VISIBLE -> DEAD_LETTERED`，非法迁移报错 |

发布示例：

```json
{
  "request_id": "pub-1",
  "message_id": "ord-1",
  "topic": "orders",
  "partition": 0,
  "partition_key": "customer-7",
  "body": "order body",
  "transactional": true,
  "txn_id": "tx-1"
}
```

显式路由要求 `partition`；哈希路由要求非空 `partition_key`。

## 分区消费

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/partitions` | 分区诊断列表 |
| GET | `/api/partitions/{topic}/{p}` | 单分区视图 |
| POST | `/api/partitions/{topic}/{p}/pull` | 获取/续租并拉取 |
| POST | `/api/partitions/{topic}/{p}/commit` | 提交消费位点并释放租约 |
| POST | `/api/partitions/{topic}/{p}/replay` | 回退并进入重放 |
| POST | `/api/partitions/{topic}/{p}/retention` | 设置最早保留序号 |
| POST | `/api/partitions/{topic}/{p}/priority` | 设置调度优先级 |
| POST | `/api/partitions/{topic}/{p}/force-expire` | 调试强制过期租约 |

拉取：

```json
{"request_id":"pull-1","owner":"consumer-a","limit":1,"lease_version":3,"lease_injection":"timeout_preempt"}
```

首次拉取可省略 `lease_version`；响应返回新版本。`timeout_preempt` 仅用于本地演示，会先过期其他所有者的租约。

提交：

```json
{"request_id":"commit-1","owner":"consumer-a","lease_version":4,"through_seq":7}
```

回退：

```json
{"request_id":"replay-1","owner":"consumer-a","lease_version":5,"from_seq":2,"lease_injection":"new_message_on_replay"}
```

## 事务裁决

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/txns` | 事务列表，`?pending=1` 过滤 |
| GET | `/api/txns/{id}` | 单事务 |
| POST | `/api/txns/{id}/resolve` | 查询本地协调器并裁决 |
| GET | `/test/txn/{id}` | 协调器测试端点，支持 `mode` 注入 |

裁决请求：

```json
{"request_id":"resolve-1","injection_mode":"commit","force_recheck":false,"query_timeout_ms":150}
```

`injection_mode`：`commit`、`rollback`、`refuse`、`hang`、`conflict`。超时返回 `E_TXN_TIMEOUT`；矛盾复核返回 `E_TXN_CONFLICT`。

## 调度器

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/scheduler` | 配置、等待队列、在途数、最近轮次 |
| POST | `/api/scheduler/config` | 替换配置 |
| POST | `/api/scheduler/tasks` | 入队 `txn` 或 `lease_expire` |
| POST | `/api/scheduler/run` | 运行一轮，可传 `{"budget_ms":1}` |

配置示例：

```json
{"global_concurrency":2,"per_partition_lease_limit":1,"queue_capacity":2,"round_budget_ms":5,"query_timeout_ms":100,"min_dispatch_ms":2}
```

队列满返回 `E_QUEUE_FULL`。预算不足的轮次返回 `degraded_budget`，步骤包含 `SKIPPED` 和剩余预算。

## 注入与恢复

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/injections` | 列出/登记持久注入 |
| GET | `/api/observations` | 注入观测 |
| POST | `/api/debug/crash-after-intent` | 需要 `-allow-crash`；意图落盘后退出进程 |

崩溃演示请求：

```json
{"operation":"pull","topic":"orders","partition":1,"owner":"crasher","request_id":"crash-1"}
```

支持 `pull`、`commit`、`replay`、`txn_resolve`。重启同一数据库后读取 `/api/recovery/latest`。
