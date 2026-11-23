# 架构说明：行级写权租约与版本化读快照仲裁引擎

## 1. 目标与边界

引擎把并发控制拆成两条相互独立、但在提交点严格协同的机制：

- **写侧**：按行授予带 TTL 的排他写权（lease）。同一行同一时刻至多一个持有者；
  租约到期未续自动失效，其他客户端可重新申请，`lease_version` 单调递增用于识别抢占。
- **读侧**：全局 `commit_ver` 单调递增，读者在请求中携带读快照版本，读取
  `commit_ver <= snapshot` 的最新未删除、未回收行版本，读路径不持有任何行锁。

引擎**不提供跨语句 MVCC 事务**。一次“编辑”的原子性由单个 SQLite 写事务保证：
意图记录、新行版本、`commit_ver` 推进、租约释放要么全部可见，要么全部不可见。

## 2. 组件与目录

- `internal/clock`：时钟抽象。`System` 用真实 `time.Timer`；`Manual` 用 channel
  停车、`Advance` 唤醒，供确定性测试驱动租约到期与等待超时，全程无空转。
- `internal/store`：SQLite schema、事务辅助、行版本/租约/意图/幂等请求/读快照
  注册表的所有 SQL。WAL + `synchronous=FULL` + 5 秒 `busy_timeout`。
- `internal/arbiter`：仲裁策略核心。
  - `gate.go` 行级互斥与等待队列（channel 唤醒，非轮询）。
  - `leases.go` 申请/续约/释放/到期，`rows.go` 版本化读与同事务提交校验，
  - `recycle.go` 保留集与回收，`recovery.go` 强制终止后基于事实的恢复，
  - `precheck.go`/`reconcile.go` 快速预检与完整路径逐项核对，
  - `idempotency.go` 基于 `request_id` 的只执行一次语义。
- `internal/httpapi`：仅绑定回环地址的 HTTP 接口与统一错误信封。
- `cmd/arbiter`：进程入口，启动时先恢复、再播种、再对外服务。
- `sample`：确定性种子、场景 JSON、可直接运行的本地脚本。

## 3. 持久化模型（SQLite）

| 表 | 作用 |
| --- | --- |
| `meta` | 保存全局单调 `commit_ver` |
| `row_versions` | 只追加的行版本；回收置 `retired_at` 而非删除，关联 `intent_id` |
| `write_leases` | 每行一条租约，`lease_version` 随每次授予递增 |
| `write_intents` | 写前意图 WAL：`prepared` → `completed`/`rolled_back` |
| `inflight_requests` | 幂等请求台账：`running`/`completed`/`aborted` |
| `active_snapshots` | 活跃读快照，回收时保护其仍需读取的旧版本 |
| `recovery_events` | 恢复审计事件，仅记录已持久化事实 |

所有变更事务都遵循同一顺序：`INSERT write_intents(prepared)` → 应用变更 →
`UPDATE write_intents SET completed`，三者在同一事务提交。

## 4. 关键一致性论证

### 4.1 同一事务内的双重校验（需求 3、6）

提交时在一个写事务内依次读取租约与最新行版本：

1. 租约不存在/版本不符/持有者不符/状态非 granted/已过期 → `E_LEASE_LOST`，
   并在 `details.diff` 中给出实际持有者、实际租约号、实际行版本与快照号。
2. `base_row_version` 不是最新 → `E_EDIT_STALE`，附当前最新行版本号。
3. 两项都通过才分配新 `commit_ver`、插入新行版本并把租约置为 `released`。

单连接 + 事务内读已持写锁，其他进程无法在两个校验之间插入提交，因此不存在
“行版本校验通过但租约已失效”的部分可见提交。失败路径不会推进 `commit_ver`。

### 4.2 可中断的真实等待（需求 4）

申请冲突且 `wait_ms > 0` 时，调用方在 `rowGate` 注册一个 `leaseWaiter` 并在
channel 上 `select` 停车，同时等待：上下文取消、等待 channel 被关闭、睡眠到期。
释放或到期在行锁内 `wakeAll` 关闭 channel；被唤醒者重新在锁内复查持久化租约，
只有一个调用方能观察到“无有效租约”。超时者从队列移除并返回
`E_LEASE_TIMEOUT`。等待由 `clock.Sleeper` 实现，生产环境是 `time.Timer`，
不存在轮询空转，也没有“失控协程”：取消时通过内层 context 让辅助协程退出。

### 4.3 回收与读快照稳定（需求 1、5）

回收在每行单写者锁内执行。保护下限为
`max(声明 min_retain_snapshots, 最低活跃读快照)`，并保证至少保留
`min_retain_versions` 个版本；最新行版本永不回收。被回收的行保留记录但置
`retired_at`，读取时命中即返回 `E_VERSION_RETIRED`，绝不会回退到更新的版本。
同一快照重复读取：回收前返回固定内容，回收后稳定返回 `E_VERSION_RETIRED`。

### 4.4 幂等（需求 6）

`request_id` 在数据库唯一。相同 id + 相同 payload 哈希：首个调用执行并保存
响应 JSON；并发重复调用在内存 gate 的 channel 上停车等待，结束后复用持久化响应。
相同 id 但 payload/kind/key 不一致返回 `E_CONFLICTING_REQUEST`。校验失败的请求
被标记为 `aborted`，客户端修正后可用同一 id 重试。

### 4.5 强制终止恢复（需求 7）

重启时 `Recover` 仅依据持久化行：

- **prepared acquire/renew/release/expire**：若持久化租约仍停在意图版本，
  回滚到 `expired`/`released`，使行重新可申请；租约版本已前进则只记录核实跳过。
- **prepared commit**：通过 `row_versions.intent_id` 找孤儿行版本并删除，
  按现存最大 `commit_ver` 回退全局版本号；悬挂的 granted 租约置 `expired`。
- **prepared recycle**：正常单事务不可能出现“已回收 + prepared”。故障注入若
  制造该状态，恢复保留安全的 retired 状态（读者得到 `E_VERSION_RETIRED` 而非
  混合内容），并记录偏差而不是虚构回滚。
- `running` 请求统一置 `aborted`（可重试）；活跃读快照标记 `orphaned`，继续
  保守保护旧版本，直到显式 `prune_orphans`。

### 4.6 快速预检与完整提交可核对（需求 8）

面板打开用一次一致的持久化读取同时获得行内容与租约状态并预热缓存。预检在声明
预算内给出 `editable`、`mode`（`fast_cache`/`fast_refresh`/`fast_lease_refresh`）
与事实依据；预算为零或超时返回 `completed=false` + `E_PRECHECK_INCOMPLETE`，
绝不拼接跨快照结论。完整提交失败时 `/edits/reconcile` 与提交错误均返回同一
`Diff` 结构：实际/预期行版本、实际/提交租约号、实际持有者、租约状态、快照号。

## 5. 错误码对照

| 代码 | 触发条件 |
| --- | --- |
| `E_SNAPSHOT_AHEAD` | 快照号 > 当前 `commit_ver`，不返回任何行 |
| `E_VERSION_RETIRED` | 该快照所需行版本已被回收 |
| `E_NOT_FOUND` / `E_ROW_DELETED` | 快照内无行 / 行在该快照已删除 |
| `E_WRITE_LEASE_HELD` | 申请时存在未过期租约，附持有者与到期时刻 |
| `E_LEASE_STALE` | 续约/释放携带的租约号落后 |
| `E_LEASE_LOST` | 提交时租约失效、被抢占或持有者不符，附原因与 diff |
| `E_EDIT_STALE` | 提交基于的行版本已非最新，附当前最新行版本号 |
| `E_LEASE_TIMEOUT` | 有界等待超时，等待者已出队 |
| `E_CONFLICTING_REQUEST` | 同一 `request_id` 携带不一致 payload |
| `E_PRECHECK_INCOMPLETE` | 预算内未完成预检，禁止返回混合结论 |
