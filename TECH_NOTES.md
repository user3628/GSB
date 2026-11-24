# 离线 A/B 实验分阶段评测组件 — 技术说明

本交付是一个无外部服务依赖的 Go 组件：内置固定种子样本生成器、确定性分阶段指标、
CTR 区间估计与裁决、纯 Go SQLite 持久化、崩溃恢复、版本替换识别、并发检查点保护，
以及可集成的库 API 和 `abeval` 命令行。

- Go module：`abeval`
- 语言级别：`go 1.22`
- SQLite 驱动：`modernc.org/sqlite v1.29.10`（纯 Go，无 CGO）
- 实际验证环境：`go version go1.27.4 linux/amd64`

## 一键验证

```bash
go mod download
./scripts/verify.sh
```

脚本会执行：

- `gofmt -l .`
- `go vet ./...`
- `go build ./...`
- `go test ./... -count=1`
- `go run ./cmd/abeval demo --out-dir artifacts/demo`
- `go run ./examples/local`

最近一次完整结果：全部测试通过，demo `"ok": true`，恢复报告三份文件 SHA-256 相同。
详细测试日志见 `artifacts/verification/go-test-v.log`。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `model.go` | 配置、事件、指标、裁决、报告和状态 DTO |
| `errors.go` | 稳定业务错误码与带事件/环节上下文的 `Error` |
| `canonical.go` | 规范 JSON、缩进 JSON、SHA-256 摘要 |
| `validate.go` | 配置、流量、阶段连续性、事件单调性/分组/点击/延迟/覆盖校验 |
| `stats.go` | 显式排序、nearest-rank 分位、分组指标、CTR 标准误和置信区间 |
| `evaluate.go` | 样本门槛、放量/观察/回滚规则、依据项与被否定理由、纯函数报告 |
| `sample.go` | 固定种子的六类内置样本 |
| `store.go` | SQLite schema、DSN、事务导入、内容版本识别、事件和状态读取 |
| `checkpoint.go` | 条件检查点提交、阶段推进、报告重建与导出 |
| `runner.go` | 阶段边界优雅停止与恢复执行器 |
| `cmd/abeval/main.go` | CLI：`generate/import/advance/run/status/version/report/demo` |
| `cmd/abeval/demo.go` | 四类场景、版本替换、并发写入的本地演示 |
| `examples/local/main.go` | 直接引用库 API 的完整本地示例 |

## 常用 CLI

```bash
go build -o /tmp/abeval ./cmd/abeval

/tmp/abeval generate --scenario rollout --out /tmp/dataset.json
/tmp/abeval import --db /tmp/ab.db --input /tmp/dataset.json
/tmp/abeval advance --db /tmp/ab.db --batch batch-rollout
/tmp/abeval run --db /tmp/ab.db --batch batch-rollout
/tmp/abeval status --db /tmp/ab.db --batch batch-rollout
/tmp/abeval version --db /tmp/ab.db --batch batch-rollout
/tmp/abeval report --db /tmp/ab.db --batch batch-rollout --out /tmp/report.json

# 确定性一键示例
go run ./cmd/abeval demo --out-dir artifacts/demo

# 库 API 示例
go run ./examples/local
```

## 确定性指标与裁决

阶段按配置中的半开时间区间 `[start_ms, end_ms)` 划分。事件时间必须非递减；事件落在
所有阶段区间之外时返回顶层 `E_INVALID_EVENT`，并在 `details.sub_code` 中给出
`E_STAGE_COVERAGE`。

每个阶段同时输出：

- 本阶段两组曝光、点击、CTR、p50/p95/p99；
- 从第 1 阶段累计到当前阶段的两组曝光、点击、CTR、p50/p95/p99；
- CTR 绝对差值、相对提升、标准误、95% 正态近似区间；
- 配置的延迟分位（默认 p95）差值、绝对阈值和回退阈值；
- 每条放量/回滚依据的通过状态、实际值、阈值和否定理由。

分位算法为显式 `sort.Slice` 后使用 nearest-rank：

```text
rank = ceil(p * n)
value = sorted_latencies[rank-1]
```

所有输出切片按配置中的 `groups` 与 `stages` 顺序构造；报告 DTO 字段顺序固定，
不依赖 Go map 遍历。相同 Dataset 会产生相同 JSON 字节和 SHA-256。

裁决顺序：

1. 任一组未达到该阶段 `min_exposures_per_group`：`observe/OBSERVE_INSUFFICIENT_SAMPLE`，绝不默认放量。
2. 样本足够后，若 CTR 跌幅和置信区间上界同时满足回滚条件，或延迟越过绝对/回归阈值：`rollback`。
3. 若 CTR 差值达到最小提升、区间下界大于 0，且延迟全部满足：`rollout`。
4. 否则：`observe/OBSERVE_THRESHOLD_NOT_MET`。

## SQLite 状态与崩溃恢复

核心表：

- `batches`：批次当前版本、三类摘要、当前连续阶段、状态、最终结论和报告；
- `revisions`：每个批次的版本与内容摘要历史；
- `events`：导入时持久化的有序事件；
- `stage_checkpoints`：阶段统计 JSON、裁决 JSON、摘要哈希和 writer 标识。

有效阶段记录由部分唯一索引保护：

```sql
CREATE UNIQUE INDEX uq_stage_checkpoint_active
ON stage_checkpoints(batch_id, stage_index)
WHERE status = 'valid';
```

`CommitStage` 使用 `BEGIN IMMEDIATE` 事务，并执行条件更新：

```sql
UPDATE batches
SET current_stage = ?, status = 'running'
WHERE batch_id = ?
  AND current_stage = ?
  AND version = ?
  AND content_hash = ?;
```

只有 `stage_index = current_stage + 1` 且条件更新影响一行时才能插入有效统计。重复、
落后或跳跃写入均返回 `E_STALE_CHECKPOINT`。最终阶段的 checkpoint、最终裁决和报告在
同一事务提交。

优雅停止：`Runner.RequestStop()` 或 CLI 收到 SIGINT/SIGTERM 后，不中断正在执行的阶段；
该阶段事务提交后，将批次标记为 `graceful_stopped` 并退出。重启创建新的 `Runner`，
从 SQLite 中的 `current_stage` 继续。

强制终止：CLI 提供 `--crash-after-stage N` 故障注入，在阶段事务提交后立即向自身发送
SIGKILL。被 kill 时不执行 defer 或退出报告；重启只读取已提交数据。测试
`TestCLIForceSIGKILLRecoveryByteIdentical` 验证新进程恢复报告与连续执行逐字节相同。

## 内容版本、整体替换与回退

每个 Dataset 计算：

- `config_hash = sha256(canonical_json(config))`
- `sample_hash = sha256(canonical_json(events))`
- `content_hash = sha256(config_hash + ":" + sample_hash)`

导入规则：

- 相同 `content_hash`：幂等返回；
- 新 `content_hash` 且 `version > active_version`：整体替换，旧 checkpoint/revision 标记为
  `superseded`，删除并重建活动事件，`current_stage` 重置为 0；
- `version <= active_version`：返回 `E_VERSION_ROLLBACK`，CLI 退出码 `10`；
- 摘要命中历史 revision：即使传入更大的新版本号，也判定为内容回退并返回
  `E_VERSION_ROLLBACK`。

## 错误码

| 错误码 | 场景 | CLI 退出码 |
| --- | --- | ---: |
| `E_INVALID_CONFIG` | 配置、阶段、流量或参数不合法 | 2 |
| `E_INVALID_EVENT` | 事件 ID 缺失/重复、时间戳下降、分组未配置、点击非 0/1、延迟为负、阶段覆盖不到 | 2 |
| `E_STAGE_COVERAGE` | 阶段覆盖错误的子码/纯函数直接分配阶段时使用 | 2 |
| `E_STALE_CHECKPOINT` | 重复、落后、跳跃或 revision 不匹配的检查点写入 | 11 |
| `E_VERSION_ROLLBACK` | 版本号回退或内容摘要回退到历史版本 | 10 |
| `E_NOT_FOUND` | 批次不存在 | 12 |
| `E_BUSY` | SQLite 锁等待后仍忙，`retryable=true` | 13 |

业务错误 JSON 包含 `code/message/stage/event_id/field/details`，便于定位事件、环节和阈值上下文。

## 内置样本与实际验收结果

生成器种子：`20261125`。普通/回滚/恢复场景每阶段 800 事件，不足场景每阶段 100 事件，
共 3 个阶段。

`go run ./cmd/abeval demo --out-dir artifacts/demo` 的最终摘要：

| 场景 | 结论 | 依据码 | 报告 SHA-256 |
| --- | --- | --- | --- |
| `rollout` | `rollout` | `ROLLOUT_CRITERIA_MET` | `3210d5b1ec9a523cf47f2bba45f067a5a81fce497d0373ed32c782d3ddc68e8f` |
| `insufficient` | `observe` | `OBSERVE_INSUFFICIENT_SAMPLE` | `dddc8f022825a82f34832547fc2827c048b78d7f3ace07aa8b2165dad9f89120` |
| `rollback` | `rollback` | `ROLLBACK_MULTIPLE_SIGNALS` | `d054b8ba5a8a236e07149f121721bf56d613a524a94bff58e52bf0321af88c1b` |

恢复验收：

```text
7ca039fe533d048ae5d231e209abbfa648bc4657c100862e8c51de7a080f6bb9  recovery-continuous.report.json
7ca039fe533d048ae5d231e209abbfa648bc4657c100862e8c51de7a080f6bb9  recovery-restarted.report.json
7ca039fe533d048ae5d231e209abbfa648bc4657c100862e8c51de7a080f6bb9  recovery-force-restart.report.json
```

并发验收：demo 的 8 个同进程竞争写入为 `committed=1, stale=7`；
`TestSQLiteCommitStageMultipleProcesses` 启动 8 个真实测试二进制进程写同一阶段，
结果为 1 个成功、7 个退出码 20（`E_STALE_CHECKPOINT`）。

## 集成建议

- 服务或批处理系统可直接调用 `abeval.GenerateDataset`、`abeval.EvaluateDataset`、
  `abeval.OpenStore`、`Store.ImportDataset`、`Runner.Run` 和 `Store.ExportReport`。
- 多实例部署时共享同一个 SQLite 文件并使用 WAL；写事务已经使用 `BEGIN IMMEDIATE`、
  busy timeout 和条件更新。更高并发场景可将 `Store` 的 SQL 事务模型平移到 Postgres/MySQL。
- 报告中不包含墙钟时间、PID 或随机 writer 名称，因此连续执行、优雅停止后续跑、
  SIGKILL 后恢复得到的正式报告字节一致；writer 诊断仅保存在检查点元数据中。
