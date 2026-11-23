# 实际验证结果

环境：Linux amd64，Go 1.27（`go.mod` 声明 `go 1.22`，仅用标准库 `net/http`、
`database/sql` 与 `modernc.org/sqlite v1.34.4`）。所有服务绑定 127.0.0.1，
数据库为本地 SQLite 文件，无外部网络服务、无容器。

## 1. 自动化测试

命令：

```bash
go vet ./...
go test -race ./...
```

结果：全部通过（`-race` 竞态检测无告警），共 15 个测试用例：

| 用例 | 覆盖需求 |
| --- | --- |
| `TestReadSnapshotMatrix` | 需求 1：快照越界、旧快照稳定、提交后新版本可见 |
| `TestLeaseHeldStaleAndExpiry` | 需求 2、4：占用冲突、过期失效、抢占后租约号递增、`E_LEASE_STALE` |
| `TestCommitChecksShareTransactionAndFailClosed` | 需求 3：双校验同事务，失败时无新版本、`commit_ver` 不前进 |
| `TestDeleteVisibilityAndMonotonicity` | 需求 1、5：删除标记版本与旧快照可见性 |
| `TestInterruptibleWaitWokenByRelease` | 需求 4：channel 真实唤醒、非空转、单一持有者 |
| `TestBoundedWaitTimesOutAndLeavesQueue` | 需求 4：超时返回 `E_LEASE_TIMEOUT` 且等待者出队 |
| `TestWaitCancellationIsReal` | 需求 4：上下文取消真实中断等待 |
| `TestRecycleRespectsActiveSnapshotsAndRetention` | 需求 5：活跃快照保护、最新版本不回收、`E_VERSION_RETIRED` 稳定 |
| `TestIdempotentCommitExecutesOnceAndDetectsConflict` | 需求 6：只执行一次、复用响应、`E_CONFLICTING_REQUEST` |
| `TestConcurrentDuplicatesReuseOneResult` | 需求 6：并发重复请求只产生一个行版本 |
| `TestCrossProcessIdempotency` | 需求 6：两个引擎打开同一 SQLite 文件，持久化选主防止跨进程双执行 |
| `TestRecoveryRollsBackPreparedGrantAndCommit` | 需求 7：授予回滚、孤儿版本丢弃、版本号回退、恢复幂等 |
| `TestPrecheckBudgetAndReconcileDiff` | 需求 8：预检模式、零预算未完成标识、逐项 diff |
| `TestLoopbackGuardAndErrorEnvelope` | 执行约束：拒绝非回环绑定、错误信封稳定 |
| `TestHTTPEndToEndCommitAndRetiredRead` | 需求 1、5、6：HTTP 全链路提交、幂等、回收后读语义 |

## 2. 确定性样本演示

命令：`./sample/scripts/run-all.sh`（自动构建二进制，五个脚本分别使用独立
端口 18090-18094 与独立 SQLite 文件）。结果：`ALL_DEMOS_OK`。

- `basic-flow.sh`：面板打开 → 申请 → 冲突 `E_WRITE_LEASE_HELD` → 预检 → 提交
  → 同请求标识复用 → 旧快照内容稳定 → 越界快照 `E_SNAPSHOT_AHEAD`。
- `concurrent-edits.sh`：立即冲突、后台有界等待、释放后真实唤醒授予、
  旧租约号 `E_LEASE_STALE`、等待超时 `E_LEASE_TIMEOUT`、TTL 到期后重新授予。
- `snapshot-matrix.sh`：越界/旧快照/最新快照矩阵；活跃快照保护版本不被回收；
  释放读者后回收，旧快照两次读取均为 `E_VERSION_RETIRED`，绝不返回新内容。
- `precheck-reconcile.sh`：预检可编辑 → 租约被抢占 → reconcile 输出实际持有者/
  租约号/行版本 diff → 完整提交 `E_LEASE_LOST`、`E_EDIT_STALE` → 零预算
  `E_PRECHECK_INCOMPLETE`。
- `crash-recovery.sh`：故障注入写入未完成授予与未完成提交（含孤儿行版本），
  `kill -9` 强制终止后用同一数据库重启；恢复报告含 `rollback_acquire` 与
  `discard_commit`，原占用行可重新申请、孤儿内容不可见且 `commit_ver` 回退到
  最后持久化值；恢复事件可经 `GET /recovery/report` 查询。

## 3. 强制终止复现方式（Windows 等价操作）

本仓库在 Linux 下以 `kill -9 <pid>` 复现；Windows 上等价命令为：

```powershell
taskkill /F /PID <pid>
```

随后重新执行启动命令即可触发基于持久化意图的恢复，可用 `-recovery-report`
导出 JSON 报告。故障注入路由仅在 `-enable-fault-injection` 时挂载，默认关闭。

## 4. 关键观测结论

- 提交失败（租约失效或行版本过期）后，数据库中不存在半成品行版本，
  `meta.commit_ver` 保持原值。
- 回收不删除历史行，仅置 `retired_at`；因此旧快照的错误码稳定可重复。
- 等待路径在手动时钟测试中验证为 channel 停车/唤醒，无忙等；取消会清理队列。
- 恢复动作全部可在 `recovery_events` 与恢复报告中逐条核对，报告字段只来自
  SQLite 已持久化行。
