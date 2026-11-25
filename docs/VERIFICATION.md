# 验证记录

本文件记录在本地 Linux（WSL Ubuntu）环境下的实际验证结果。所有命令均可离线复跑；
Go 模块通过 `GOPROXY=https://goproxy.cn,direct` 拉取后进入本地模块缓存，运行期不依赖网络。

## 1. 环境

| 组件 | 实际版本 |
| --- | --- |
| Go | go1.27.4 linux/amd64（模块声明 `go 1.22`，兼容 1.22+） |
| sqlite3 CLI | 3.46.1 |
| SQLite 驱动 | modernc.org/sqlite v1.34.4（纯 Go，无 CGO） |

## 2. 自动化测试

命令：

```
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

结果：全部通过（31 个测试函数），`-race` 无数据竞争，`go vet` 无告警。覆盖点：

- 量纲解析：`40Gi/512Mi/250m/0.25` 正确换算；负数、未知后缀、溢出被拒。
- 状态机：七种状态的合法/非法迁移判定；终态识别。
- 归一化：重复 ID、未知组、非法量纲、非单调版本、组容量超成员均返回 `E_SPEC_INVALID`。
- 批组织：相邻同模型/同节点/同优先级合批，优先级硬分隔，显存/样本/超限延迟触发拆分并保序。
- 准入：节点占用与组预留同事务更新；超额返回带 shortfall 的 `E_QUOTA_EXCEEDED` 且请求保持 PENDING。
- 抢占：迁移 PREEMPTED 与配额真实释放在同一事务，释放后可立即重新准入。
- 轮次：并发上限 + 有界队列，队列满返回 `E_EXEC_QUEUE_FULL` 且溢出批次降级、请求不丢；预算降级确定性。
- 幂等/冲突：并发同 ID 准入只执行一次；in-flight 内容不一致返回 `E_CONFLICTING_REQUEST`。
- 并发读：8 读者 × 20 轮读取，节点用量与组预留恒等，无中间可见状态。
- 崩溃恢复：子进程 `os.Exit(137)` 强制终止后重开 DB，断言回到最后完整版本且不变量成立；孤儿意图被标记 ABORTED；恢复幂等。

## 3. 确定性样本

```
gpusched generate -out samples -seed 20261126
```

两次生成 sha256 完全一致：

```
ded7dec84850e29673913d66126f7c03ace2491d11f6d25487baa3ba1a4488e6  samples/sample.json
```

样本含 2 个配额组、4 个节点、15 个请求、2 个链式摘要快照与 1 个增量片段。

## 4. HTTP 端到端（固定样本）

以 `-auto-sample` 启动后对 15 个请求准入，观测：

- 14 个请求成功 ADMITTED，1 个因组算力/节点容量不足保持 PENDING（`E_QUOTA_EXCEEDED` 带差额）。
- sqlite3 跨表核对，两组均满足
  `quota_groups.memory_reserved == Σ nodes.memory_used` 与
  `quota_groups.compute_reserved == Σ nodes.compute_used_m`（mem_match=1, comp_match=1）。
- 调度轮次：达到全局并发上限后批次进入有界队列，队列满时溢出批次落 DOWNGRADED，成员请求保持
  ADMITTED；接口返回 503 `E_EXEC_QUEUE_FULL` 但已裁决批次全部提交。
- 抢占一个 BATCHED 请求：观测到 `freed_memory_bytes=12884901888`、`freed_compute_milli=180`，
  组预留同步下降；将其 `PREEMPTED->PENDING` 后重新准入成功，组占用恢复，不变量仍成立。

## 5. 强制终止与恢复演练

命令：

```
scripts/crash_recovery.sh
```

脚本在独立临时目录启动服务、装载样本、准入请求、武装 `round:after_commit` 崩溃点并触发轮次，
进程以退出码 137 被强制终止；随后重启（启动自动执行恢复）。实际观测：

```
process terminated as expected
rounds_committed_or_recovered        = 1
requests_not_left_running_after_restart = 0
mem_match=1 comp_match=1             （两组均一致）
batches: RUNNING=4
intents: COMMITTED=6
CRASH_RECOVERY_OK
```

恢复报告 `running_rounds`、`pending_intents` 均为空，说明不存在悬挂轮次/意图，恢复只陈述落盘事实。

预提交（`round:before_commit`）路径由 Go 子进程测试 `TestCrashRecoverySubprocess` 覆盖：在
`tx.Commit` 前强制终止时，SQLite WAL 原子丢弃整个轮次事务，重启后无 BATCHED/RUNNING 残留，
受影响请求保持 ADMITTED 且配额不变量成立。该点在真实进程中存在 OS 调度窗口（钩子与提交的
先后由运行时决定），因此端到端脚本默认采用确定性的提交后演练点。

## 6. 手工核对示例

```
sqlite3 data/gpusched.db \
  "SELECT g.id, g.memory_reserved,
          (SELECT COALESCE(SUM(memory_used),0) FROM nodes n WHERE n.group_id=g.id) AS node_used
   FROM quota_groups g;"
sqlite3 data/gpusched.db "SELECT status, COUNT(*) FROM intents GROUP BY status;"
sqlite3 data/gpusched.db "SELECT status, COUNT(*) FROM batches GROUP BY status;"
```
