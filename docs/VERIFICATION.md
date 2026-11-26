# 实际验证记录

验证时间：2026-11-26（Asia/Shanghai）。

## 环境

- `go version go1.27.4 linux/amd64`
- `gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0`
- SQLite CLI/library：`3.46.1`
- Go 驱动：`github.com/mattn/go-sqlite3 v1.14.24`
- 依赖拉取：`GOPROXY=https://goproxy.cn,direct`

## 自动化测试

已执行并通过：

```bash
./scripts/verify.sh
# 或单独执行：
gofmt -w cmd internal
go vet ./...
go test ./...
go test -race -count=1 ./...
```

依赖已放入 `vendor/`，默认离线使用 `-mod=vendor`；脚本日志另存为 `artifacts/verification/verify-script.log`。

完整普通测试日志保存在 `artifacts/verification/go-test.txt`，竞态日志保存在 `artifacts/verification/go-test-race.txt`。

覆盖的关键验收点：

- `internal/codec/codec_test.go`：fixed/RLE/dictionary 全量与区间解码一致；固定截断、RLE 零长度 run、dictionary 越界索引均返回 `E_BLOCK_CORRUPT` 且块号/偏移正确。
- `internal/plan/plan_test.go`：整体解码、分段大小、峰值内存、`E_BLOCK_UNBOUNDED` 最小行宽和预算详情。
- `internal/agg/agg_test.go`：部分和合并、确定性、溢出窗口/分组、舍入轨迹、朴素 float64 误差界。
- `internal/sched/scheduler_test.go`：分区/块稳定排序、预算剩余、延后列表、`E_DECODE_QUEUE_FULL`、并发池上限。
- `internal/engine/engine_test.go`：无效/旧版本分片不影响 v1；预算延后；重复批次复用；Continue 只复用未变化段；跨批次全量结果与逐行解码一致；并发 Query；固定样本数值界。
- `internal/store/recovery_test.go`：真实启动测试子进程，写入 pending 意图后发送 `SIGKILL`，重开 SQLite 验证 rolled-back/needs-recompute/reusable；另测 pending 意图匹配已提交事实时归入 committed。
- `internal/api/server_test.go`：httptest 回环服务上的 health、events、demo shard、202 延后、continue、query 和 recovery。

测试结果摘要：所有包均 `ok`，包括 `-race`；无失败用例。

## 固定样本精度结果

`TestFixedSampleNumericDifferenceBound` 对固定种子生成的全部分组给出：

```text
groups=7
max_pre_round_observed=1.14e-12
max_pre_round_bound=4.8e-11
max_output_diff=0.000000
max_output_bound=0.005000
cumulative_rounding_error=-0.0027
```

含义：int64 精确和与逐行 float64 朴素和的偏差低于量化累加界；按 2 位小数 half-up 后，本样本输出与朴素舍入输出完全一致；所有分组舍入残差累计为 `-0.0027`（输入精度 4 位小数）。

## 真实 SIGKILL 恢复

执行：

```bash
go build -o bin/logagg ./cmd/logagg
./bin/logagg prepare-crash --db artifacts/recovery/logagg.db
```

实际 shell 报告退出码 `137`，即 `SIGKILL`。崩溃后、恢复前的数据库副本保留在：

- `artifacts/recovery/logagg-crashed-preserved.db`

其中 SQLite 事实为：

```text
shard-switch:shard-local-001:v1 | shard_switch | committed
segment-start:crash-pending-block-1 | segment_start | pending
segment_work: crash-session / block 1 / pending_decode
```

运行：

```bash
./bin/logagg recover --db artifacts/recovery/logagg.db
```

实际报告（完整 JSON：`artifacts/verification/recovery-report.json`）：

```text
committed=0
rolled_back=1   # segment-start:crash-pending-block-1
needs_recompute=1 # shard-local-001 v1 block 1 rows [0,180)
reusable_partial_sums=1 # block 0 rows [0,240)
```

随后：

```bash
./bin/logagg resume-crash --db artifacts/recovery/logagg.db
```

实际结果（完整 JSON：`artifacts/verification/resume-output.json`）：

```text
status=completed
reused_blocks=[0]
processed_blocks=[1]
groups=7
```

这证明恢复只依据 SQLite 事实：block 0 复用已提交部分和，block 1 重算并提交。

## HTTP 回环验证

实际启动：

```bash
./bin/logagg serve --addr 127.0.0.1:18080 --db artifacts/http/logagg.db
```

小预算 `byte_budget=4000` 提交 block 0（3840 字节）和 block 1（2880 字节）时：

```text
HTTP 202
status=deferred
processed=[block 0]
deferred=[block 1]
remaining_budget=160
```

用大预算调用 `/continue` 后：

```text
status=completed
processed=[block 1]
reused=[block 0]
query_groups=7
session_version=2
```

响应和恢复输出保存在：

- `artifacts/verification/http-health.json`
- `artifacts/verification/http-advance-small.out`
- `artifacts/verification/http-continue.json`
- `artifacts/verification/http-query.json`
- `artifacts/verification/http-recovery.json`
- `artifacts/verification/http-events.json`
- `artifacts/verification/http-server.log`

## 未做事项

- 未使用 Docker 或其他容器。
- 未访问外部数据集或未授权目标；样本和事件均由固定种子本地生成。
- 未执行 git commit。
