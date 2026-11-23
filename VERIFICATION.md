# 实际验证记录

验证日期：2026-11-24（Asia/Shanghai）。

当前执行环境是 Windows 工作区中的 WSL Ubuntu 运行器，不是原生 Windows 桌面；因此实际命令在 Linux 上执行，并额外完成了 Windows amd64 交叉编译。PowerShell 演示脚本已交付，但本次未在原生 PowerShell 中执行。

## 环境

```text
Go:      go1.27.4 linux/amd64（项目 go.mod 为 go 1.22.0）
SQLite:  3.46.1
OS:      WSL/Linux amd64
CGO:     Windows 交叉编译使用 CGO_ENABLED=0
Docker:  未使用
```

## 自动化测试

命令：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

结果：

```text
ok  upstreampool/internal/client
ok  upstreampool/internal/config
ok  upstreampool/internal/service
ok  upstreampool/internal/sim
ok  upstreampool/internal/statemachine
go vet ./...        PASS
go test -race ./... PASS
```

覆盖点：

- 七阶段合法/非法迁移，非法返回 `E_INVALID_TRANSITION`。
- 复用路径 `IDLE -> HANDSHAKING -> SEND_BODY`，`max_reuses=2` 时第三次使用后关闭。
- 请求持续时间和空闲回收时间溢出检查。
- 池满 REJECT、排队 120ms 超时及 `waited_ms`。
- 整批/分段最终快照和 5 个中途快照逐项一致。
- 重复批次不推进、冲突批次返回 `E_CONFLICTING_BATCH`。
- 真实拨号拒绝返回 `E_DIAL_FAILED`。
- 48 MiB 慢接收端真实写入超时返回 `E_SEND_TIMEOUT`，套接字随后不可写。
- 未完成 `APPLY_PLAN` 意图恢复、批次续算。
- 检查点篡改返回 `E_CHECKPOINT_CORRUPT`。
- 8 并发读者与写者并发时的快照内部一致，在 race detector 下通过。

## Windows 交叉编译

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/poolctl-windows-amd64.exe ./cmd/poolctl
file /tmp/poolctl-windows-amd64.exe
```

结果：

```text
PE32+ executable for MS Windows 10.00 (console), x86-64, 16 sections
```

## 完整演示

命令：

```bash
bash scripts/demo.sh
```

最近一次输出（完整文件：`artifacts/demo-run.txt`）：

```text
[1/6] build
[2/6] start deterministic loopback upstreams and workbench
[3/6] submit whole track and segmented track
PASS snapshot equality: whole == segmented, final + 5 midpoints
[4/6] duplicate/conflict and pool exhaustion
PASS idempotency, E_CONFLICTING_BATCH, E_POOL_ACQUIRE_TIMEOUT(waited_ms=120), E_DIAL_FAILED
[5/6] force-kill after durable intent
... Killed  DIAG_CRASH_AFTER_PLAN_INTENT_MS=30000 ... serve ...
[6/6] restart, recover and compare resumed result
PASS recovery: aborted intent=1 resumed=1; snapshot == uninterrupted baseline
database: /tmp/upool-demo.52hasK/workbench.db
endpoints            5
tracks               4
batches              5
requests             16
request_io           10
checkpoints          59
transitions          51
request_results      16
final_connections    6
connection_uses      10
intents              10
recovery_reports     1
demo artifacts: /tmp/upool-demo.52hasK
```

该脚本实际完成一次整批推进、一次分两段推进并比对最终与 5 个中途快照、一次池耗尽排队超时、一次 REJECT、一次真实拨号失败，以及对已落盘意图的 `kill -9` 和重启续算。

## SQLite CLI 抽查

主数据库：

```sql
select track_id,batch_id,status from batches order by track_id,batch_id;
select track_id,count(*) as snapshots,sum(is_checkpoint) as explicit_checkpoints,max(seq) from checkpoints group by track_id;
```

实际结果：

```text
demo-dial-failure  dial-failure-batch  APPLIED
demo-exhaustion    exhaustion-batch    APPLIED
demo-full          full-batch          APPLIED
demo-segmented     segment-1           APPLIED
demo-segmented     segment-2           APPLIED

demo-dial-failure  3   1   3
demo-exhaustion    12  2   12
demo-full          22  4   22
demo-segmented     22  4   22
```

崩溃恢复数据库：

```text
INGEST_BATCH  APPLIED
APPLY_PLAN    ABORTED   {"pending_batches":["crash-segment-1"],"request_count":3}
APPLY_PLAN    APPLIED   {"pending_batches":["crash-segment-1"],"request_count":3}
INGEST_BATCH  APPLIED
APPLY_PLAN    APPLIED   {"pending_batches":["crash-segment-2"],"request_count":6}

crash-segment-1  APPLIED
crash-segment-2  APPLIED
```

这证明外部强制终止发生在意图落盘之后、计划提交之前；恢复把该意图标记 ABORTED，然后重新执行并完成同一批次。

## 关键一致性结果

- `demo-full` 与 `demo-segmented` 最终快照：逐项相等。
- `at_ms = 0, 300, 401, 500, 602` 中途快照：逐项相等。
- 复用连接 `edge-a-c000001` 的阶段序列包含：

```text
CONNECTING -> HANDSHAKING -> SEND_BODY -> RECV_RESPONSE -> IDLE
IDLE -> HANDSHAKING -> SEND_BODY -> RECV_RESPONSE -> IDLE
IDLE -> HANDSHAKING -> SEND_BODY -> RECV_RESPONSE -> DRAINING -> CLOSED
```

- 强制终止后恢复轨迹的最终快照与未中断基线逐项相等。

