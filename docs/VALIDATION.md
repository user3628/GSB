# 实际验证记录

执行日期：2026-11-25（Asia/Shanghai）

## 环境

```text
go version go1.27.4 linux/amd64
模块依赖：modernc.org/sqlite v1.34.5
Windows 目标：GOOS=windows GOARCH=amd64 交叉编译通过
```

任务目标环境为 Windows + Go 1.22 以上；本次执行容器为 Linux，代码只使用 Go 标准库
和纯 Go SQLite 驱动，并额外完成 Windows 交叉编译检查。验证器自身用
`exec.Cmd` + `Process.Kill()` 实现跨平台强杀。

## 命令与结果

### 全量测试

```bash
go test ./... -count=1
```

结果：全部通过。

```text
?   ampcheckpoint/cmd/server      [no test files]
?   ampcheckpoint/cmd/verify      [no test files]
ok  ampcheckpoint/internal/domain
ok  ampcheckpoint/internal/engine
ok  ampcheckpoint/internal/httpapi
ok  ampcheckpoint/internal/mixed
?   ampcheckpoint/internal/sample [no test files]
```

### 竞态检测

```bash
go test -race ./... -count=1
```

结果：全部通过；并发只读测试在检查点持续提交期间反复读取固定版本，未观察到部分
提交或数据竞争。

### 静态检查与 Windows 编译

```bash
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
```

结果：均通过，无输出。

### 真实子进程端到端验证

```bash
go run ./cmd/verify
```

验证器执行的步骤：

1. 构建并启动 `cmd/server` 子进程，等待 `/healthz`。
2. 从 `/v1/sample` 取固定种子样本，提交策略 v1。
3. 提交 round 1，核对全量驻留、精度偏差和预算。
4. 提交增量 v2，核对增量触发的真实驱逐。
5. 提交紧张写预算策略 v20 和 round 3，核对跳过清单、剩余预算和队列。
6. 提交队列上限策略 v21，核对 `E_WRITE_QUEUE_FULL` 且不推进检查点版本。
7. 核对 `E_STALE_SNAPSHOT`、`E_STALE_BASE`、`E_PARTITION_INVALID`。
8. 连续增长损失缩放，核对成功推进及 `E_LOSS_SCALE_OVERFLOW`。
9. 核对 `E_VRAM_POOL_FULL` 且不提交检查点。
10. 提交 `fault_after_intent=evict`，收到 HTTP 202 后 `Process.Kill()` 强杀。
11. 同一端口重启，核对扫描恢复、最后已提交版本、池事实和精度事实不变。

实际输出保存在 `artifacts/verification-output.json`。关键数值：

| 项目 | 结果 |
| --- | --- |
| 固定种子 | `20261125` |
| 样本分区批次 SHA-256 | `272e326ead24322ba8c1e6813ae339440727b9235dd409fa8c802eb6339ee1c9` |
| 初始检查点 | v1，8 个驻留副本，VRAM `19968/20480` 字节 |
| 初始精度 | 累计绝对偏差 `23169` 微单位，最大绝对偏差 `484`，最大相对偏差 `432 ppm` |
| 增量后检查点 | v2，驱逐 3 个分片，VRAM `19712/20480` 字节 |
| 被驱逐分片 | `shard-00`、`shard-01`、`shard-02`，全部 `PERSISTENT/PERSISTED` |
| 紧张预算检查点 | v3，写预算 `9000`，写入 `8320`，剩余 `680`，跳过/排队 `17` |
| 损失缩放 | 9 次成功增长后第 10 次 `E_LOSS_SCALE_OVERFLOW`，未污染当前策略 |
| 强杀意图 | `EVICTING`，基础检查点 v3 |
| 恢复证据 | 20 个暂存分片回到 `PENDING`，3 个驱逐中止 |
| 恢复前后池事实 | VRAM `19584`、PERSISTENT `59136`，均不变 |
| 最新完整检查点 | 仍为 v3 |
| 业务错误码 | 六类指定错误码均由真实 HTTP 响应核对 |

## 测试覆盖

- 精度标记、非正字节数、int64 总量溢出和缺失主参数均返回
  `E_PARTITION_INVALID`。
- FP16 binary16 往返、定点误差、摘要域分离和 FP16 溢出。
- 策略版本单调、陈旧快照/基础版本、校验和不匹配。
- 损失缩放增长、回退、阈值与乘法溢出，失败不改变已提交策略。
- 全量驻留、LRU 驱逐、持久池回落、VRAM 上限。
- 写盘预算、时间预算、严格优先级阻塞和队列溢出。
- 精度逐元素比较、累计舍入误差、派生结构和恢复基线一致性。
- 并发只读与串行检查点提交。
- 未提交 `EVICTING` 意图重启恢复，且已提交池/精度事实不变。
- 真实 HTTP 错误 JSON、404 和非法请求体。
