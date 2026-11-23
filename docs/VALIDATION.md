# 实际验证记录

本记录基于工作区当前源码与内置确定性样本执行。报告时间字段固定为 `FIXED_SAMPLE_2026-01-01T00:00:00Z`，不依赖执行时的墙钟。

## 环境与构建

```text
Go: go version go1.27.4 linux/amd64
Module language target: go 1.22.0
SQLite: modernc.org/sqlite v1.34.5 (pure Go)
CGO: disabled for final build
OS: Linux amd64
```

执行：

```bash
CGO_ENABLED=0 go build -trimpath -o fuzzlab ./cmd/fuzzlab
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go build -trimpath -o fuzzlab-windows-amd64.exe ./cmd/fuzzlab
file fuzzlab-windows-amd64.exe
```

结果：

```text
fuzzlab-windows-amd64.exe: PE32+ executable for MS Windows 10.00 (console), x86-64, 16 sections
sha256: 2c58e4b7830c353d08007169eb544158be909127bca3770f96f2d163d3992914
```

该 Windows 产物由 `CGO_ENABLED=0` 构建，使用纯 Go SQLite 驱动。

## 自动测试

```bash
go test ./...
go vet ./...
go test -race ./internal/protocol ./internal/graph ./internal/store \
  ./internal/replay ./internal/report ./internal/scheduler \
  ./internal/target ./internal/verify
```

结果：全部通过，`go vet` 无输出；竞态检测未报告数据竞争。

已覆盖：

- 帧编码/解码、CRC16 篡改、越界帧排空。
- 五状态迁移、非法迁移停留、轨迹记录。
- 删除/修改种子导致的直接与传递失效闭包。
- 缺失引用 `E_DEP_INVALIDATED` 与算子环 `E_DEP_CYCLE`。
- 拒绝变更不推进用例集版本。
- 旧用例集版本保留且可重新加载。
- 并发上限、等待队列与峰值观测。
- `E_CASE_BUDGET` 的部分结果保留。
- 幂等回放键与重复快照抑制。
- 快照跳号、连续链回退和失败提交不推进最大版本。
- 整体替换识别。
- 报告覆盖丢失与重复运行字节一致。
- 五类隔离注入场景。

## 增量与失效演示

从空库执行：

```bash
./fuzzlab init
./fuzzlab replay --batch-size 2 --concurrency 2
./fuzzlab apply --file data/change-01-incremental.json
./fuzzlab replay --batch-size 2 --concurrency 2
./fuzzlab apply --file data/change-02-break-dependency.json
./fuzzlab replay --batch-size 2 --concurrency 2
```

关键结果：

| 阶段 | 用例集版本 | 用例数 | 算子数 | 结论 |
| --- | ---: | ---: | ---: | --- |
| 基线 | 1 | 3 | 5 | 全部 `E_OK`，提交快照 1-2 |
| 增量 | 2 | 4 | 7 | 新增 `seed-bulk`、两个算子、`case-bulk` 与四条断言；提交快照 3-4 |
| 删除 ping 种子 | 3 | 4 | 7 | `case-ping`、`op-ping-append` 及相关断言失效；提交快照 5-6 |

版本 3 回放结果：

```text
content_digest=301f025e7da6378fae12d7bc52697772501cd6f8e0ef3d9b93d592889c28d073
scheduler_limit=2 peak_in_flight=2 peak_queued=2 completed=2 failed=0
invalid_cases=case-ping
batch=batch-001 code=E_OK cases=2 unprocessed=0 coverage=4 snapshot=5 new=true
batch=batch-002 code=E_DEP_INVALIDATED cases=2 unprocessed=0 coverage=5 snapshot=6 new=true
case=case-ping code=E_DEP_INVALIDATED final_state=HANDSHAKE root=case:case-ping -> seed:seed-ping
snapshot_chain_state=CONTIGUOUS
```

覆盖变化：

```text
coverage_lost:
  READY -> READY
  READY -> CLOSED
```

失效根因示例：

```text
case:case-ping -> seed:seed-ping
operator:op-ping-append -> seed:seed-ping
assertion:assert-ping-ready -> seed:seed-ping
```

## 确定性报告

执行两次：

```bash
./fuzzlab report --batch-size 2 --concurrency 2
sha256sum report/diagnostic-report.json
./fuzzlab report --batch-size 2 --concurrency 2
sha256sum report/diagnostic-report.json
```

两次结果一致：

```text
report_bytes=15510
sha256=1917028f94a3f945f481c48421054a8525baa57b21343f3e7e5a26f221935662
```

报告未包含临时端口、进程 ID、绝对数据库路径或瞬时调度峰值。快照链为 1-6，父链连续。

## 异常注入验证

命令：

```bash
./fuzzlab verify --out data/verification.json
```

六个场景全部 `passed=true`：

| 场景 | 业务码 | 用例集版本 | 最大快照版本 | 结论 |
| --- | --- | --- | --- | --- |
| `missing-seed` | `E_DEP_INVALIDATED` | 1 -> 1 | 0 -> 0 | 缺失引用被拒绝，版本不推进 |
| `operator-cycle` | `E_DEP_CYCLE` | 1 -> 1 | 0 -> 0 | 依赖成环被拒绝，版本不推进 |
| `bad-checksum` | `E_BAD_CHECKSUM` | 1 -> 1 | 0 -> 0 | 篡改校验得到预期错误，dry-run 不提交快照 |
| `snapshot-gap` | `E_SNAPSHOT_VERSION` | 1 -> 1 | 3 -> 3 | 断链回退到连续版本 1，失败提交不推进最大版本 |
| `concurrent-race` | `E_OK` | 1 -> 1 | 0 -> 3 | 峰值并发 2、排队峰值 3，三个快照各提交一次且链连续 |
| `forced-restart` | `E_OK` | 1 -> 1 | 0 -> 3 | 子进程首批提交后退出码 99，重启复用已提交批次并补齐其余两个 |

验证 JSON SHA-256：

```text
b9e3099752c98bb600e265a6c779693dbcbfd0629a57ac1b8841db0fec316615
```

## 强制终止细节

`forced-restart` 场景实际启动当前 CLI 子进程：

```text
replay --batch-size 1 --concurrency 1 --crash-after-batches 1
```

子进程在第一个快照提交后执行 `os.Exit(99)`。父验证进程重新打开同一 SQLite 数据库，观察到：

```text
snapshot_count=1
max_snapshot_version=1
snapshot_chain_state=CONTIGUOUS
caseset_version=1
```

随后正常重启同一回放命令。最终：

```text
snapshot_count=3
max_snapshot_version=3
snapshot_chain_state=CONTIGUOUS
```

说明已提交批次不回退，其余未提交批次整体重新回放。Windows 上可用任务管理器结束进程，或：

```powershell
Stop-Process -Id <PID> -Force
```

然后重新执行相同 `replay` 命令观察恢复结果。

## 产物

- `data/change-01-incremental.json`：增量新增样本。
- `data/change-02-break-dependency.json`：删除种子触发失效样本。
- `data/change-03-replace-all-cases.json`：用例标识整体替换样本。
- `data/inject-missing-seed.json`、`data/inject-operator-cycle.json`：外部注入变更集。
- `data/verification.json`：六场景验证结果。
- `report/diagnostic-report.json`：确定性诊断报告。
