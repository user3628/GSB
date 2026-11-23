# dnsdetect

一个纯 Go、可嵌入的本地 DNS 缓存投毒检测领域组件。组件从三种互不兼容的探针产物中恢复统一事件，按固定 100 ns 时间刻度裁决，使用 SQLite 事务保存不可变侦测快照，并输出字节稳定的 JSON 报告。

- 无 Docker、无外部数据库服务、无网络探测。
- SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，可 `CGO_ENABLED=0` 构建。
- 模块声明 `go 1.22`，Windows 10/11、Windows Server 2019+ 和 Linux 均可运行。
- 样本由内置确定性生成器写入工作目录，不依赖账号、线上站点或外部数据。

## 快速开始

Windows PowerShell（Go 1.22+）：

```powershell
$env:GOFLAGS = "-mod=mod"
$env:GOCACHE = "$PWD\.gocache"
$env:GOPROXY = "https://goproxy.cn,direct"  # 仅首次拉取依赖；缓存后可用 off
go mod tidy
go mod download
go test ./...
go run ./cmd/dnsdetect demo --workdir .\work
go run ./cmd/dnsdetect verify --workdir .\work-verify
Get-Content .\work\report.json
```

Linux/macOS：

```bash
GOFLAGS=-mod=mod GOCACHE="$PWD/.gocache" go mod tidy
GOFLAGS=-mod=mod GOCACHE="$PWD/.gocache" go mod download
go test ./...
go run ./cmd/dnsdetect demo --workdir ./work
```

完成后工作目录包含：

- `work/samples/`：v1/v2 二进制、JSON 行、长度前缀文本块及 `samples/malformed/` 六类畸形样本。
- `work/state.db`：SQLite 快照库。
- `work/report.json`：稳定排序、仅含定点时间刻度的 JSON 报告。

常用命令：

```bash
go run ./cmd/dnsdetect samples --workdir ./work
go run ./cmd/dnsdetect verify --workdir ./work-verify
go run ./cmd/dnsdetect replay --db ./work/state.db --version 1 --report ./work/replay.json
go run ./cmd/dnsdetect demo --workdir ./work --keep-state
```

`--keep-state` 会复用已有快照库；若输入与配置生成的内容摘要完全一致，提交是幂等的，不会仅因重复运行推进版本号。默认 `demo` 会重新建立 `state.db`。

离线构建验证：

```bash
CGO_ENABLED=0 GOPROXY=off go test ./...
```

## 可集成 API

根包 `dnsdetect` 提供宿主系统直接调用的接口：

```go
dataset, err := dnsdetect.GenerateDataset("./samples") // 本地确定性样本
config := dnsdetect.DefaultConfig()
config.WindowAnchorTicks = dataset.ObservationStartTicks

store, err := dnsdetect.OpenSnapshotStore("./state.db")
defer store.Close()
engine := dnsdetect.NewEngine(config, store)
result, err := engine.Process(ctx, dataset.Sources)
err = dnsdetect.ExportReport("./report.json", result.Report)
```

主要入口：

- `LoadSource`：单文件、单解析会话，返回 `SourceSession` 与隔离的 `ErrorLedger`。
- `NewScheduler`/`Scheduler.Load`：并发解析、排队背压和实时 `SchedulerStats`。
- `Adjudicate`：纯函数式窗口配对与投毒裁决，便于宿主复用或单测。
- `NewEngine`/`Engine.Process`：加载、归并、裁决、快照提交的一体化流程。
- `OpenSnapshotStore`/`Load`/`Replay`/`VerifyHead`：快照持久化、链校验、回放和整体替换锚定。
- `WriteReport`/`ExportReport`/`Report.CanonicalBytes`：确定性 JSON 输出。

二进制来源需在 `SourceSpec.ObservationStart` 中传入观察起点的 Unix 100 ns 刻度。JSON 自带 RFC3339 时间；文本块自带 FILETIME。

## 时间与窗口

所有内部时间都是相对 Unix 纪元的 `int64` 100 ns 刻度；整数路径没有浮点。

- 相对纳秒：`base_ticks + ns/100`，截断为 `abs(ns%100)` ns。
- RFC3339：`unix_sec*10_000_000 + nsec/100`，截断为 `nsec%100` ns。
- FILETIME：`int64(filetime) - 116_444_736_000_000_000`，精确无截断。

每次成功换算都会写入会话级误差账本；账本按来源与窗口汇总 `max_truncation_ns`、`total_truncation_ns` 和保守的 `union_bound_ticks`。算术溢出返回 `E_TIME_OVERFLOW`，该事件不会写入账本。

窗口由 `WindowAnchorTicks + index*WindowSpanTicks` 唯一确定，报告同时输出窗口起止刻度，可离线复算。关联判定使用事件时间误差区间：完全在 `AssociationWindowTicks` 内才直接裁决；区间跨越边界时进入 `uncertain_associations`，不产生投毒结论。

## 检测规则

事件先按“固定窗口 + 32 位事务标识 + 标签长度规范化问题名”配对；同一事务的多来源记录按优先级选择主记录，其余登记为佐证。默认优先级为二进制 30、JSON 20、文本 10。

四类稳定裁决码：

- `POISON_UNSOLICITED_RESPONSE`：响应没有同窗口查询，或明确落在所有查询关联窗之外。
- `POISON_IRRELEVANT_ADDITIONAL`：应答附加区的 A/AAAA owner 不属于问题名子树。
- `POISON_TTL_MISMATCH`：同一 owner/type 在应答与已关联缓存中的 TTL 不同。
- `POISON_MULTI_ADDRESS`：同一规范化 owner 出现多个不同地址。

`ASSOC_UNCERTAIN` 只作为不确定关联输出，不混入投毒 findings。每条 finding 都带来源路径、字节偏移、窗口、事务标识、定点时间刻度和相关事件引用。

## 快照、恢复与替换识别

每个快照在单个 SQLite 事务中追加，保存版本号、创建刻度、格式版本、来源优先级、误差账本摘要、窗口进度、内容摘要和前序内容摘要。

- 内容相同：幂等返回已有版本，不推进版本号。
- 版本跳号：`Load` 返回 `E_SNAPSHOT_VERSION`，并保留最近连续版本作为回退。
- 行内容或摘要链不一致：返回 `E_SNAPSHOT_REPLACED`。
- 进程在事务提交前被强杀：SQLite 丢弃整个事务，重启后重新裁决。
- 进程在提交后被杀：已提交版本完整保留，重复运行按内容摘要幂等。

仅比较版本号无法识别“整库替换”。宿主应持久化可信的 `SnapshotHead{Version, ContentDigest}`，启动时调用 `VerifyHead`；这样即使替换后的库仍从版本 1 开始，也会因内容摘要不同返回 `E_SNAPSHOT_REPLACED`。

## 资源与确定性

- `MaxConcurrency` 控制同时解析的源数量，多余源进入队列等待而非拒绝。
- `MaxEventsPerSession` 超出返回 `E_EVENT_BUDGET`。
- `MaxSpanTicksPerSession` 超出返回 `E_WINDOW_BUDGET`。
- `SchedulerStats` 提供 `queued`、`in_flight`、`in_flight_bytes`、`completed`、`failed`、累计输入字节和并发上限。
- 合并与裁决在并发解析完成后串行执行，findings 按来源路径、事务标识、业务码稳定排序。
- 报告没有墙钟字符串或本地时间，时间字段仅为整数刻度，并固定声明 `"sampling":"fixed-100ns"`。

## 线格式与错误码

完整格式、字段偏移和六类畸形样本说明见 [FORMATS.md](FORMATS.md)。

## 验证记录

本机实际执行（Go 1.27.4，linux/amd64；模块目标 Go 1.22）：

```text
$ go test ./...
ok      dnsdetect

$ go test -race ./...
ok      dnsdetect

$ CGO_ENABLED=0 GOPROXY=off go test ./...
ok      dnsdetect

$ go vet ./...
(无输出)

$ go test -cover ./...
coverage: 76.5% of statements (root package); 69.7% total

$ CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...
(通过；测试二进制与 CLI 均可交叉编译)
```

端到端样本运行结果：

```text
sources=4 pairs=6 findings=4 uncertain=0
POISON_UNSOLICITED_RESPONSE=1
POISON_IRRELEVANT_ADDITIONAL=1
POISON_TTL_MISMATCH=1
POISON_MULTI_ADDRESS=1
```

一键自检命令实际输出：

```text
$ go run ./cmd/dnsdetect verify --workdir /tmp/dnsdetect-verify-final
verified malformed=6 findings=4 pairs=6 snapshot_version=1 report_bytes=13639
```

对同一工作目录连续执行三次 `demo`（包含一次 `--keep-state`），`report.json` 的 SHA-256 均为：

```text
90f2f91b9da680334e8548c8b7cf3b0b245f929f447bcd0b3a4580db93e1989f
```

自动化测试覆盖：三来源解析与规范化、跨源主记录/佐证、四类迹象、100 ns 边界不确定、事件/跨度/时间溢出、六类畸形码、快照跳号与回退、内容替换、提交前/提交后强杀恢复、并发排队以及报告字节级一致。

## 验收对照

| 验收点 | 实现/测试 |
|---|---|
| 跨源配对与主记录/佐证 | `TestGeneratedDatasetFindingsAndPairing` |
| 四类投毒迹象 | `TestGeneratedDatasetFindingsAndPairing`、样例 `verify` |
| 定点时间边界不确定 | `TestAssociationBoundaryUncertainty` |
| 溢出不污染账本 | `TestOverflowDoesNotPolluteLedger` |
| 事件/跨度预算 | `TestSessionBudgets` |
| 六类畸形业务码 | `TestMalformedSamplesBusinessCodes`、`TestSnapshotGapSample` |
| 畸形输入不推进快照 | `TestMalformedInputsDoNotAdvanceSnapshot` |
| 快照链/回放/整体替换 | `TestSnapshotChainReplayAndTamper`、`TestWholeDatabaseReplacementNeedsContentAnchor` |
| 提交前/后强杀恢复 | `TestCrashRecovery`（子进程强制退出） |
| 报告字节级一致 | `TestReportIsByteStableAndSorted`、三次 `demo` 哈希 |

## 已知边界

- 同名多地址是启发式投毒迹象；正常轮询 DNS 也可能出现多 A/AAAA。
- 默认严格禁止同窗口内一个事务标识映射到多个问题名；若现网存在合法复用，可设置 `AllowTXIDQNameConflict`。
- 整体替换检测需要宿主保管外部内容摘要锚；只有 SQLite 文件本身时无法从密码学上区分攻击者重建的完整替代链。
