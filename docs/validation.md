# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）。

## 环境

```text
$ go version
go version go1.27.4 linux/amd64

$ sqlite3 --version
3.46.1 2024-08-13 09:16:08 ...

$ gcc --version | head -1
gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0
```

`go.mod` 声明 `go 1.22`，第三方依赖仅 `github.com/mattn/go-sqlite3 v1.14.53`，并已执行 `go mod vendor`。

## 非缓存全量测试

命令：

```bash
go test -mod=vendor -count=1 ./...
```

实际结果：

```text
?       auditdrift/cmd/auditdrift       [no test files]
?       auditdrift/internal/app [no test files]
ok      auditdrift/internal/drift       0.003s
?       auditdrift/internal/model       [no test files]
ok      auditdrift/internal/normalize   0.002s
ok      auditdrift/internal/probe       0.167s
?       auditdrift/internal/sample      [no test files]
ok      auditdrift/internal/schedule    0.103s
ok      auditdrift/internal/store       0.251s
```

完整日志：`artifacts/full-test.log`。

覆盖的关键断言：

- JSONL 与旧版 LPB 解析出的 25 条 `Observation` 用 `reflect.DeepEqual` 逐字段一致。
- 微秒截断/四舍五入的舍入位、本条误差和累计误差正确。
- `[2,4)` 边界值 4 判为偏移、3 判为合规。
- 数值/集合类别不符返回 `E_VALUE_TYPE_MISMATCH`。
- 服务枚举偏移、注册表缺失/多余/混合差异、来源冲突分类正确。
- 有界队列满返回 `E_AUDIT_QUEUE_FULL`。
- 配额 20 时按严重度/主机顺序批准前两个 weight=9 修复，剩余 2，后续修复任务确定性降级。
- 80ms 轮预算下 5 个阻塞任务全部 `DEFERRED/round_budget_exhausted`，峰值并发为 2。
- 真实 HTTP 慢端点在约 60ms 触发 `E_PROBE_TIMEOUT`，连接追踪器观察到 `closed>=1 && active==0`。
- 调度器 + 慢探测在 `MaxConcurrency=1` 下两个任务约 0.11s 完成，证明超时后并发槽释放。
- 关闭端口返回 `E_PROBE_REFUSED`。
- 非法基线批次返回 `E_BASELINE_INVALID`，且 `baselines/sessions/intents` 均为 0 行。
- 错误基础版本返回 `E_STALE_BASE`，版本保持 1。
- weight 4→10 后版本升到 2，直接依赖、第二跳传播和主机汇总视图版本均为 2。
- 期望区间收紧和依赖边新增也会触发结论/传播重建。
- 未完成传播意图下 observed version=2、view version=1 返回 `E_STALE_READ`，详情包含两个版本。
- 16 个并发只读事务/查询可同时读取已提交版本。
- 崩溃意图恢复后无 PENDING、`integrity_check=ok`、汇总与结论计数一致。

## 竞态检测

命令：

```bash
go test -mod=vendor -race -count=1 ./internal/schedule ./internal/probe ./internal/store
```

实际结果：

```text
ok      auditdrift/internal/schedule    1.111s
ok      auditdrift/internal/probe       1.175s
ok      auditdrift/internal/store       1.303s
```

完整日志：`artifacts/race-test.log`。

另执行 `go vet -mod=vendor ./...`，无输出并以 0 退出。

## 一键验收与 SIGKILL 恢复

命令：

```bash
bash scripts/acceptance.sh
```

完整输出：`artifacts/acceptance-run.log`。脚本每次删除并重建 `.acceptance-tmp/`，因此可重复运行。

关键实际结果：

```text
==> [3/7] fixed seed and dual-format samples
baselines: 8, observations: 25

==> [4/7] budget and weighted quota degradation
budget/quota OK: {'DONE': 11, 'DEFERRED': 13}, remaining=2

==> [5/7] monotonic increment, E_STALE_BASE and cascade
increment cascade OK: version=2
stale base OK: E_STALE_BASE returned and committed version unchanged

==> [7/7] two SIGKILL recovery cycles and sqlite3 fact check
repeat recovery OK: normalized reports match
{
  "pending": 1,
  "aborted": 1,
  "reset": 0,
  "version": 1,
  "status": "RECOVERED",
  "tasks": {
    "DEFERRED": 13,
    "DONE": 11
  },
  "final_pending": 0,
  "integrity": "ok",
  "consistent": true
}
```

`reset=0` 是因为审计轮结束后没有遗留 `RUNNING` 任务；恢复流程仍包含 `RUNNING -> PENDING` 的 SQL 和专门的状态重置路径。

脚本通过 `kill -9` 强杀 `crash-demo` 进程两次。每次强杀前，进程已 durable 写入一条 PENDING `snapshot_increment` 意图，并开启修改 `host_summaries.drift=999` 的未提交事务。重启恢复后，两次归一化报告一致，未提交的 999 漂移计数不可见。

## sqlite3 核对

验收脚本实际查询：

```text
host    total  done  conformant  drift  deferred  errors  max_drift_level
------  -----  ----  ----------  -----  --------  ------  ---------------
host-a  8      8     8           0      0         0       0
host-b  8      2     1           1      6         0       4
host-c  8      1     0           1      7         0       4

summary_mismatch_count=0
```

并且：

```sql
SELECT COUNT(*) FROM intents WHERE status='PENDING';
-- 0
```

## 验证边界

- 所有 HTTP 探测均指向 `127.0.0.1`/`httptest` 本地端点或立即关闭的本地端口。
- 未访问外部服务、未使用 Docker、未使用真实生产目标。
- 强制终止由验收脚本在本机对 CLI 子进程执行 `kill -9`，不是模拟函数调用。
