# cockpit-audio：多音区交互引擎

一个可嵌入 Go 领域组件与可重复运行的车载四音区演示。程序用固定种子在系统临时目录生成本地 WAV 分片和 JSON 清单，覆盖音区 VAD、跨音区归属、毫秒时间轴编辑、低延迟查询、增量重算、SQLite 快照恢复和并发发布保护。

- Go 1.22+；仅使用标准库与 `modernc.org/sqlite v1.29.0`。
- 依赖已放入 `vendor/`，可 `GOPROXY=off` 离线测试；纯 Go SQLite，Windows 无需 C 编译器。
- 原始任务说明保留在 `docs/TASK_SPEC.md`，架构细节见 `docs/ARCHITECTURE.md`，实测记录见 `docs/VALIDATION.md`。

## 快速开始

Windows PowerShell：

```powershell
go version
go test ./...

$w = Join-Path $env:TEMP "cockpit-audio-demo"
go run ./cmd/cockpit-audio demo --workdir $w --fresh
go run ./cmd/cockpit-audio verify --workdir (Join-Path $env:TEMP "cockpit-audio-verify")
go run ./cmd/cockpit-audio query --workdir $w --type active_zones --at 250 --budget-ms 5
```

Linux/macOS：

```bash
go test ./...
w=$(mktemp -d /tmp/cockpit-demo-XXXXXX)
go run ./cmd/cockpit-audio demo --workdir "$w" --fresh
go run ./cmd/cockpit-audio verify --workdir "$w-verify"
go run ./cmd/cockpit-audio query --workdir "$w" \
  --type attribution_basis --command shard-001-cmd-temperature
```

演示会在工作目录生成：

- `samples/*.wav`、`samples/session_manifest.json`：固定种子样本与会话清单。
- `state.db`：SQLite WAL 数据库和阶段快照。
- `report.json`：确定性的 VAD、归属、时间轴、摘要和编辑审计。
- `query-results.json`：标准查询结果。
- `sample-edits.json`：使用 `--sample-edits` 时的编辑审计。

## 常用命令

```bash
# 确定性四音区样本 + 四类编辑演示
go run ./cmd/cockpit-audio demo --workdir "$w" --fresh --sample-edits

# 模拟提交若干单元后硬退出（不执行优雅关闭），随后恢复
go run ./cmd/cockpit-audio demo --workdir "$w" --fresh --crash-after 3
go run ./cmd/cockpit-audio resume --workdir "$w"

# 结构化注入错误
go run ./cmd/cockpit-audio inject --kind sample_count --workdir "$w-inject"
go run ./cmd/cockpit-audio inject --kind rate_conflict --workdir "$w-inject"
go run ./cmd/cockpit-audio inject --kind speed_non_positive --workdir "$w-inject"
```

支持的注入：`sample_count`、`rate_conflict`、`offset_overlap`、`max_speakers_zero`、`mask_iterations_negative`、`speed_non_positive`。

## 已实现的稳定错误码

- `E_SAMPLE_COUNT_MISMATCH`：清单样本总数与 WAV 不符。
- `E_RATE_CONFLICT`：清单采样率与 WAV 头冲突。
- `E_INVALID_PARAM`：偏移重叠、人数上限为零、掩码轮数为负、声速非正等。
- `E_LATENCY_BUDGET`：查询超过预算，带 `query_id`、`elapsed_ms`、`budget_ms`。
- `E_INCREMENTAL_MISMATCH`：增量结果与全量重算不一致，带 `first_difference`。
- `E_STALE_CHECKPOINT`：落后/重复/跨分片发布、序号回退或快照篡改。

## 主要可集成 API

```go
ds, _ := cockpitaudio.GenerateDataset(cockpitaudio.GenerateOptions{Root: "./samples"})
cfg := cockpitaudio.DefaultConfig()
full, _ := cockpitaudio.BuildSession(ds, cfg, nil)

svc, _ := cockpitaudio.NewQueryService(cfg, full)
resp, err := svc.Query(ctx, cockpitaudio.QueryRequest{
    Type: cockpitaudio.QueryActiveZones, AtMS: 250,
})

store, _ := cockpitaudio.OpenStore("./state.db")
report, err := cockpitaudio.NewEngine(ds, cfg, store, "worker-1").Run(ctx)
```

验收测试覆盖四音区检测、明确/歧义归属、查询缓存、延迟错误、并发编辑与结算一致性、六类增量变更、注入错误码、存储 CAS/篡改/回退，以及取消和硬退出后的逐字节恢复。
