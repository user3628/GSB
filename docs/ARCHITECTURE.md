# NonceGuard 架构与需求追踪

## 组件

- `cmd/gate`：HTTP API、交互式控制台、启动恢复扫描；同时挂载进程内 `/peer/*` 回环对端。
- `cmd/peer`：可独立监听的本地回环代理对端，支持任意延迟参数。
- `internal/model`：请求/配置模型、严格 JSON 解析、字段与错误码、窗口边界说明。
- `internal/canonical`：长度前缀规范化字符串、固定种子 HMAC-SHA256 签名和配置摘要。
- `internal/sample`：固定种子样本、异常样本、回收样本和 v2 配置。
- `internal/store`：纯 Go `modernc.org/sqlite`、WAL、schema、恢复扫描。
- `internal/engine`：单写者裁决、快速采样、版本读取、幂等请求、并发控制和回收。
- `internal/httpapi`：JSON API、统一错误上下文、静态 UI。
- `web/index.html`：零外链专业控制台。
- `scripts/demo.sh`：构建、起停回环服务、五类异常、`kill -9`、重启恢复和续跑断言。

## 关键不变量

### 配置版本

- 配置版本严格连续：新版本必须为当前版本 `+1`。
- `<= current` 返回 `E_STALE_SNAPSHOT`；`> current+1` 返回 `E_CONFIG_VERSION_GAP`。
- 新快照先校验整体摘要和字段，再在同一事务写 `config_snapshots` 与 `active_config`。
- 校验或事务失败不会修改活动版本。
- 一次批次只加载一个 `model.Snapshot`，签名、偏移和 Nonce 窗口均从该对象读取。

### 时间与窗口

- 偏移：`offset = request.timestamp_ms - reference_ms`。
- 偏移容忍区间为 `[min_offset_ms, max_offset_ms]`，两端包含。
- Nonce 窗口为 `[first_seen_ms, first_seen_ms + nonce_window_ms)`，左闭右开。
- 当 `reference_ms >= first_seen_ms + nonce_window_ms` 时，旧占用到期，可删除并重新占用。
- 所有持久化时间均为整数毫秒；非负亚毫秒差值向零/floor 截断后再比较。
- `clock_state.high_water_ms` 保存单调高水位。即使外部传入较小 `reference_ms` 或系统时钟回拨，有效参考时刻也不会倒退。

### 单写者与可见性

- `Engine.mu` 将配置提交和完整批次裁决串行化。
- 只读查询不获取写锁；SQLite WAL 允许读连接并发读取已提交数据。
- 同一 `request_id` 的相同内容在注册表中只对应一条权威结论；并发批次在互斥锁后等待并复用。
- 相同 `request_id` 但 `content_hash` 不同返回 `E_CONFLICTING_REQUEST`，并记录 `conflict_attempts`。
- 一个批次的签名检查、Nonce 占用、结论、批次项和意图完成状态在同一个 SQLite 事务提交；查询不会看到“签名已写、Nonce/结论未写”的中间状态。

### 意图与强制终止

每次批次提交：

1. 批次任务和所有 `intents.status=open` 在意图事务中落盘并提交。
2. 裁决事务写入 `signature_checks`、`nonce_occupancy`、`decisions`、`request_registry`，并把对应意图改为 `committed/reused`。
3. 裁决事务原子提交。

`kill -9` 若发生在步骤 2 期间，SQLite 自动回滚未提交事务；已提交的 open intent 仍在。重启或 `POST /v1/recovery/run` 扫描 open intent 和处理中任务，输出落盘计数，清理与 open intent 关联的防御性孤儿行，并把批次标记为 `interrupted_recovered`。随后可调用 `/v1/batches/{id}/resume` 重放固定批次；已存在的相同请求会复用权威结论。

## 快速模式

- 方法：`deterministic-prefix-cap-v1`，按声明的 `precheck_limit` 固定选择前 N 条。
- 输出 `sample_rate = N/total`、签名字段覆盖率、Nonce 覆盖率、选择下标和置信说明。
- 未选记录返回 `E_FAST_NOT_PRECHECKED` / `not_prechecked`，不是通过。
- 快速模式是影子只读检查，不写 `nonce_occupancy` 或 `decisions`，也不改变窗口。
- `mode=compare` 先跑快速影子检查，再跑完整裁决，并逐条输出 `match`、`difference` 或 `not_prechecked`。

## 异常到业务码

| 外部参数/行为 | 业务码 |
| --- | --- |
| 非法 Nonce/权重/JSON | `E_REQUEST_INVALID` |
| HMAC 不匹配 | `E_SIGNATURE_MISMATCH` |
| 早于/晚于偏移边界 | `E_TIMESTAMP_EARLY` / `E_TIMESTAMP_LATE` |
| 窗口内重复 `client_id+nonce` | `E_NONCE_REPLAYED` |
| 配置版本陈旧/跳号/摘要坏 | `E_STALE_SNAPSHOT` / `E_CONFIG_VERSION_GAP` / `E_SNAPSHOT_DIGEST_INVALID` |
| 同请求 ID 内容不一致 | `E_CONFLICTING_REQUEST` |
| peer 超过客户端超时 | `E_PEER_TIMEOUT` |
| peer 端口未监听 | `E_PEER_REFUSED` |
| peer ACK 晚于确认期限但早于 HTTP 超时 | `E_PEER_LATE_ACK` |
| 意图写入后外部中断 | `E_BATCH_INTERRUPTED` |

所有失败响应均为顶层包含 `ok:false`、`code`、`message`、`context` 的 JSON。
