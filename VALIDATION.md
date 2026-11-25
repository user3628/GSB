# 实际验证记录

验证时间：2026-11-25 10:43 (+08:00)。

环境：

- `go version go1.27.4 linux/amd64`；`go.mod` 声明 `go 1.22`，依赖锁定 `modernc.org/sqlite v1.34.4`。
- `sqlite3` 3.46.1。
- 未使用 Docker、GPU、外部服务或网络素材。

## 1. 格式化、静态检查与跨平台构建

实际执行：

```bash
gofmt -w cmd internal flashattn.go
test -z "$(gofmt -l cmd internal flashattn.go)"
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
```

结果：全部退出码 0；Windows amd64 交叉编译通过，文件锁实现包含 Unix/Windows 两套代码。

## 2. 单元与集成测试

实际执行：

```bash
go test -count=1 ./...
go test -race -count=1 ./internal/store ./internal/service
```

结果摘要（完整输出保存在 `reports/go-test-count1.txt` 与 `reports/go-test-race.txt`）：

```text
ok  flashattention/internal/attn
ok  flashattention/internal/audit
ok  flashattention/internal/scheduler
ok  flashattention/internal/service
ok  flashattention/internal/store
ok  flashattention/internal/versioning
RACE: ok internal/store, internal/service
```

覆盖：

- short、非整除 D/S、因果 + logsum 策略对 float64 参考的数值一致性；
- 同输入两次运行的输出与逐行块摘要哈希一致；
- 审计通过及篡改输出返回带分片/行块/max_abs_err 的 `E_ERROR_BOUND`；
- 前缀纠错、K/V 追加与从零全量计算逐位一致；
- 因果模式下早于受影响键的行块直接复用检查点；
- 优雅停止 2 个检查点后续跑，输出哈希与未中断基线一致；
- 损坏 blob/摘要不符和重复序号记录被恢复流程忽略；
- 重复、回退、缺口和未登记指纹变更返回 `E_STALE_CHECKPOINT`；
- 4 个独立 Store 句柄/worker 并发发布时序号连续推进到 20；
- 内存规划收缩与 `E_MEMORY_BUDGET` 明细；
- 版本指纹和因果块级失效区间。

## 3. 确定性演示

实际执行：

```bash
# 如需干净复跑，先备份或清空旧的 data/ 与 reports/
go run ./cmd/flashattn demo --data data --reports reports
```

退出码：0。总报告：`reports/demo.json`。

五个内置样本输出哈希：

| 样本 | 输出哈希前缀 | 最大绝对偏差 | 峰值相对偏差 | 最小有效位数 | 行块误差界 |
|---|---:|---:|---:|---:|---:|
| `short` | `07b25be253bb` | `2.32e-10` | `6.41e-10` | 4.84 | `4.80e-06` |
| `long` | `5e4d4a605494` | `5.96e-08` | `8.55e-07` | 4.16 | `4.86e-06` |
| `odd_dim` | `244a7b0eb841` | `5.95e-08` | `2.43e-07` | 4.70 | `4.86e-06` |
| `causal` | `14a2eff13eac` | `5.96e-08` | `1.96e-07` | 4.80 | `4.86e-06` |
| `extreme` | `9fdacc9400b8` | `1.53e-05` | `1.94e-04` | 3.58 | `2.01e-05` |

误差界公式和组成保存在每个 `reports/<shard>-run.json` 的 `audit.blocks[*].derivation` 与 `bound_components` 中；每个行块另含最多 32 个显式 `sample_positions`，同时执行全量坐标核查。

## 4. 前缀增量与追加

`odd_dim` 修正区间 `[10,14)`（只命中列块 1）：

- `byte_identical=true`；
- 5 个行块各重算 1 个受影响列块，共 5 次块计算；
- 复用未受影响列块 25 次；
- fallback 0；
- 发布 5 个新版本检查点。

`append_demo` 从 key_len=12 追加到 16（命中列块 3）：

- `byte_identical=true`；
- 16 个行块各重算 1 个追加列块，共 16 次；
- 复用既有列块 48 次；
- fallback 0；
- 发布 16 个检查点。

逐位比较包含输出 float32 bits、逐行最大值 M、指数和 Sum 与 LSE。

## 5. 中断恢复

演示复制 `long` 样本为 `recovery_long`，在 4 个持久化检查点后返回 `service.ErrGracefulStop`；随后执行 resume：

```text
recovery_hash_matches=true
```

恢复输出哈希等于未中断 `long` 运行：`5e4d4a6054942a92b868e55917341759`。

## 6. 数值诊断

`extreme` 样本产生 `E_NUMERIC` 诊断码。实测包含：

- 行 0、列 0 的 extreme value：scaled logit ≈ 282.84；
- 行 0、列 1..7 的 7 个 `exp_underflow_clamp` 精确位置；
- 行 1 的 `width_would_clamp`：指数和 160.0，超过声明的 8 位有符号上限 127；
- 非有限输入/累加路径返回错误码 `E_NUMERIC`；`--strict-numeric` 会把稳定钳制诊断也提升为命令错误。

## 7. SQLite 与并发发布

最终数据库实测：

```text
checkpoints: 1395
block_contributions: 1902
invalidations: 2
```

可用以下命令复核：

```bash
sqlite3 data/flashattn.db '.tables'
go run ./cmd/flashattn status --shard odd_dim
go run ./cmd/flashattn versions --shard odd_dim
```

并发测试使用 4 个独立 `store.Store`/OS 文件锁句柄向同一库竞争发布；只有连续 `seq` 成功，落后者收到 `E_STALE_CHECKPOINT` 后重读重试，最终 head seq=20。

## 8. 内存预算

2048 字节预算下，`short` 样本的规划决策为：

```text
split batch dimension: execute 2 batches independently
shrink row_block from 4 to 1
```

拆分后执行与审计通过。1 字节预算下规划器返回 `E_MEMORY_BUDGET`，报告中保留逐项字节明细。

## 9. 严格模式指纹与并发迁移复验

复验时间：2026-11-25 10:56 (+08:00)。

本轮交接检查发现 `attn.Config.StrictNumeric` 会进入配置 JSON 指纹：普通运行与 `--strict-numeric` 运行因此得到不同版本指纹，进而触发全量失效和重发检查点。已将该字段改为 `json:"-"`（`internal/attn/types.go:31`）；严格模式只改变诊断是否提升为命令错误，不改变张量数学、分块参数或版本状态。

新增/加强的回归：

- `internal/service/numeric_persistence_test.go:12`：先非严格首发，再严格恢复；断言错误码为 `E_NUMERIC`、`checkpoints_written == 0`，并恢复带位置的事件。
- `internal/store/migration_test.go:11`：旧 `checkpoints` 表缺少 `events_json` 时，`Store.Open` 自动补列。
- `cmd/flashattn/main_test.go:10`：CLI 同时支持子命令前后的 `--data/--reports`。

首次全量复跑时，4 个 Store 句柄并发打开数据库曾出现一次 `SQLITE_BUSY`；原因是迁移 DDL 未经过 OS 文件锁。已让 `Store.migrate` 使用同一把锁（`internal/store/store.go:120`），并保留 `events_json` 缺列迁移。随后实测：

```bash
gofmt -l cmd internal flashattn.go          # 无输出
go vet ./...                                # PASS
go test -count=1 ./...                      # PASS
go test -race -count=1 ./internal/store ./internal/service
go test -count=20 ./internal/store          # PASS
go test -race -count=5 ./internal/store     # PASS
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

完整输出分别保存在：

- `reports/go-test-count1.txt`
- `reports/go-test-race.txt`
- `reports/go-test-store-count20.txt`
- `reports/go-test-store-race-count5.txt`
- `reports/go-vet.txt`
- `reports/windows-cross-build.txt`

最终代码重新生成干净 `data/` 后，`demo` 退出码 0，五个样本哈希与上表一致；`deterministic_rerun_ok=true`、两个增量协议均 `byte_identical=true`、`recovery_hash_matches=true`。

使用真实二进制验证稳定退出码（不再经过 `go run` 包装）：

```bash
go build -o /tmp/flashattn-final ./cmd/flashattn
/tmp/flashattn-final --data data --reports reports \
  run --shard extreme --strict-numeric
# exit_code=4, error.code=E_NUMERIC
```

结构化结果保存在 `reports/extreme-strict-summary.json`：

```json
{
  "exit_code": 4,
  "error_code": "E_NUMERIC",
  "checkpoints_written": 0,
  "numeric_event_count": 9,
  "persisted_event_count": 8
}
```

其中 8 个块事件（1 个 `extreme_value` 与行 0、列 1..7 的 7 个 `exp_underflow_clamp`）从 SQLite 恢复；另 1 个 `width_would_clamp` 由恢复后的行状态确定性重算，因此合并诊断总数仍为 9，位置不丢失。

最终干净库计数保存在 `reports/sqlite-counts.txt`：

```text
checkpoints_total=1395
checkpoints_active=217
block_contributions=1902
invalidations=2
versions=8
```
