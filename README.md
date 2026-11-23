# Join Shuffle 聚合引擎

这是一个可本地集成的 Go 领域组件与完整示例：读取左右两张带版本号、生成时间、整数键列、`float64` 度量列、空值位图和 SHA-256 摘要的列式快照；按请求执行确定性哈希 Shuffle、分区内稳定排序、内连接/左外连接和分组求和/均值/计数。所有状态持久化到本地 SQLite，不依赖容器、线上站点、账号或外部数据集。

## 关键能力

- 快照整体加载、摘要校验和严格单调版本检查；失败返回 `E_SNAPSHOT_INVALID`，旧快照保持不变。
- 三种可显式切换的求和实现：`naive`、`kahan`、`double_double_fixed`；参数、补偿状态摘要、误差模型和误差上界均随结果返回。
- 显式算法不会被静默切换；`auto` 会输出推导后的算法、路径、依据和适用结论。
- 同一 Join 键使用确定性哈希进入同一分区；分区内先按空值、度量绝对值、键和源索引稳定排序，再连接。
- 高精度路径按“量级桶 → 部分和绝对值 → 分区号”稳定归并；快速路径按“分区号”稳定归并。归并依据随结果输出。
- 每次请求实际执行分区数 `1..P` 的完整对照，输出每个键的最大相对偏差；超过声明容差返回 `E_PRECISION_TOLERANCE_EXCEEDED`，上下文包含键、双方数值、分区数和容差。
- 偏斜检测输出阈值、最大分区占比、偏斜分区和受影响键。
- 分区统计、部分和、分组结果、精度声明与请求完成状态在同一个 SQLite 事务中提交；查询不会看到半成品。
- 意图记录先于计算持久化；进程被强杀后重启扫描未完成意图，标记为中止并保留最后一个完整提交版本。
- 相同 `request_id` + 相同内容并发等待并复用结果；内容不同返回 `E_CONFLICTING_REQUEST`。
- 完整实验器会自行构建并启动服务器、做单/多分区与三算法对比、验证错误码、强杀进程、重启恢复并续算核对。

## 环境与版本

- Windows 10/11 或 Linux；Go 1.21+。
- 本次实际验证环境：Linux amd64、Go `1.27.4`、SQLite CLI `3.46.1`；同时通过 `GOOS=windows GOARCH=amd64 go build ./...` 交叉编译。
- Go SQLite 驱动固定为纯 Go 的 `modernc.org/sqlite v1.34.5`，见 `go.mod`/`go.sum`。
- SQLite CLI 仅用于人工核对，运行时不需要 CGO 或外部 SQLite 库。

```bash
go version
sqlite3 --version
go mod download
```

## 快速开始

### Go 组件集成

公开门面位于 `pkg/joinshuffle`，外部模块无需导入 `internal` 包：

```go
import (
    "context"
    "net/http"

    "example.com/joinshuffle/pkg/joinshuffle"
)

engine, err := joinshuffle.Open("data/engine.db")
if err != nil { panic(err) }
defer engine.Close()

_, err = engine.LoadSnapshots(context.Background(), joinshuffle.LoadSnapshotsRequest{
    Left:  joinshuffle.SnapshotLoadInput{Path: "data/left.snapshot"},
    Right: joinshuffle.SnapshotLoadInput{Path: "data/right.snapshot"},
})
// engine.Submit、GetJob、GetPartitions、GetResults、GetPartialSums、
// GetErrorDeclaration、GetRoute、Recover、RecoveryReport 均可直接调用。
_ = http.ListenAndServe("127.0.0.1:18080", engine.Handler())
```

门面同时导出确定性样本生成/读写、请求响应类型、算法与路径常量以及全部业务错误码。

### 1. 生成确定性样本

```bash
go run ./cmd/samplegen -out data
# 或
./scripts/generate_samples.sh
```

Windows PowerShell：

```powershell
.\scripts\generate_samples.ps1
```

默认参数为种子 `20260101`、生成时间 `2026-01-01T00:00:00Z`、左表 2048 行、右表 1536 行。多次运行得到逐位一致的 `left.snapshot`、`right.snapshot` 和摘要文件：

```text
left.snapshot           ad62331f2267d10f9c0256b557f4ef3c8ca9b3c0c52f8a9030ead2ec4d543c2e
right.snapshot          a23309837363bc5e9f2f0d8233de31ab52052deccdd40ddc2b99dac78b5511ff
```

摘要文件与整文件 SHA-256 一致，可直接核对：

```bash
(cd data && sha256sum -c left.snapshot.sha256 right.snapshot.sha256)
```

### 2. 启动本地服务

默认监听 `127.0.0.1:18080`；端口可由 `PORT` 指定，数据库位置可由 `SHUFFLE_DB` 指定。

```bash
PORT=18080 ./scripts/run_server.sh
# 等价于
go run ./cmd/server -addr 127.0.0.1:18080 -db data/engine.db
```

PowerShell：

```powershell
$env:PORT = "18080"
.\scripts\run_server.ps1
```

启动时先扫描未完成意图，再尝试从 SQLite 元数据恢复上次快照。健康检查：

```bash
curl http://127.0.0.1:18080/healthz
```

### 3. 加载快照

```bash
curl -X POST http://127.0.0.1:18080/v1/snapshots/load \
  -H 'Content-Type: application/json' \
  -d '{
    "left":  {"path":"data/left.snapshot"},
    "right": {"path":"data/right.snapshot"}
  }'
```

`checksum_path` 可省略，默认使用 `<snapshot>.sha256`；也可传 `sha256` 期望值。左右快照作为一对原子替换；任意一侧版本未严格递增、文件损坏、内嵌摘要或外部摘要不符，都不会改变当前快照。

### 4. 提交聚合请求

```bash
curl -X POST http://127.0.0.1:18080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "demo-dd-p4",
    "join_type": "left",
    "partition_count": 4,
    "skew_threshold": 0.35,
    "algorithm": "double_double_fixed",
    "fixed_scale_bits": 80,
    "relative_tolerance": 1e-12,
    "compute_budget": 0
  }'
```

主要字段：

| 字段 | 说明 |
| --- | --- |
| `request_id` | `[A-Za-z0-9._:-]{1,128}`，幂等键 |
| `join_type` | `inner` 或 `left`，缺省 `inner` |
| `partition_count` | `1..256`，必填 |
| `skew_threshold` | `(0,1]`，缺省 `0.35` |
| `algorithm` | `naive`、`kahan`、`double_double_fixed`、`auto`，必填 |
| `fixed_scale_bits` | DoubleDouble 定点小数位数，`32..256`，缺省 `80` |
| `relative_tolerance` | 分区数变化时的相对容差，缺省 `1e-12` |
| `compute_budget` | 非负估算工作量预算；`0` 表示不限制 |
| `merge_rule` | `partition_ascending` 或 `magnitude_bucket_partition_asc`；缺省按路径选择 |

### 5. 查询结果

```bash
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4/partitions
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4/results
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4/partial-sums
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4/error-declaration
curl http://127.0.0.1:18080/v1/jobs/demo-dd-p4/route
curl http://127.0.0.1:18080/v1/recovery/report
```

查询只读取已提交版本。请求处于运行态时返回 `E_EXECUTION_IN_PROGRESS`；崩溃恢复后的请求返回 `E_EXECUTION_ABORTED`，用相同 `request_id` 和完全相同内容重新提交即从新尝试续算。

### 6. 一键可重复实验

```bash
./scripts/run_experiment.sh
```

PowerShell：

```powershell
.\scripts\run_experiment.ps1
```

脚本默认使用 `artifacts/experiment` 并在运行前清理固定请求 ID，实验报告写入 `artifacts/experiment/report.json`。可用 `-workdir` 和 `-clean` 显式控制。实验内容包括：

1. 两次生成样本并逐位比较。
2. 三种算法分别执行单分区与四分区，比较同键结果和最大偏差。
3. 与 DoubleDouble 定点参考结果比较实际误差。
4. `auto` 路径选择声明。
5. 重复请求复用、内容冲突、损坏快照不覆盖旧版本。
6. 写入意图后强杀服务进程，重启扫描恢复，查询中止状态，再以相同请求续算并逐位核对。

## 数据与聚合语义

### 列式快照格式

文件为小端二进制：`8B magic | u64 version | i64 generated_at_unix_nano | u64 rows | u32 side_len | u32 flags | side | i64 keys[rows] | f64 measures[rows] | null_bitmap[ceil(rows/8)] | SHA-256(payload)`。末尾内嵌摘要覆盖正文；外部 `.sha256` 覆盖完整文件。非空度量拒绝 `NaN` 和 `Inf`。

### Join 与空值

- 键的哈希为确定性 SplitMix64，`partition = hash(key) % P`，因此同一键的左右记录必定同分区。
- 每侧分区内稳定排序：非空值优先，`abs(measure)` 升序，键升序，源索引升序。
- 一次匹配行的数值为存在的分量之和：左有效则计入左值，右有效则计入右值；都有效时为 `left + right`。左外连接未匹配的右分量为空，不参与求和/均值。
- `group_count` 包含所有连接输出行（含度量空值行）；`value_count` 只包含至少一个有效分量的行；`average = sum / value_count`，没有有效值时 `sum` 与 `average` 为 `null`。

### 三种求和

记 `n = value_count`、`u = 2^-53`、`S = Σ|value|`：

| 算法 | 状态 | 绝对误差上界声明 |
| --- | --- | --- |
| `naive` | 顺序部分和 | `gamma_(n-1) * S`，`gamma_n = n*u/(1-n*u)` |
| `kahan` | 和 + 补偿量 | `(2*u + n*u^2) * S` |
| `double_double_fixed` | DoubleDouble 高低分量 + `2^-scale_bits` 定点整数 | `n*2^-(scale_bits+1) + u*\|sum\|` |

每个分组返回 `absolute_error_bound`、`relative_error_bound`、`average_absolute_error_bound`、`average_relative_error_bound` 和 `compensation_state_digest`。均值上界由和上界除以 `value_count`，再加一次最终除法舍入 `u*|average|` 得到。全局声明还包含总绝对上界、最大相对上界、最大均值相对上界、算法参数、路径、误差模型和适用结论。上界是保守声明，实际分区偏差另行实测并返回。

DoubleDouble 路径同时维护误差自由变换得到的 `hi/lo` 补偿状态和定点大整数；部分和合并使用定点整数精确相加，因此对该路径，同一输入的归并顺序不影响最终整数值。`fixed_scale_bits` 始终显式出现在请求或响应中。

### 分区数与归并

同一 Join 键按契约固定落入同一分区，所以一个键通常只有一个非空分区部分和；引擎仍执行以下完整流程：

1. 计算请求的分区计划与 `1..P` 的完整替代执行。
2. 对每个键收集全部分区部分和，按声明的稳定规则排序后合并。
3. 以单分区结果作为参考，计算每个键在各分区数下的最大相对偏差。
4. 任一偏差超过 `relative_tolerance` 时返回 `E_PRECISION_TOLERANCE_EXCEEDED`，并在 `context` 中给出键、双方数值、分区数、偏差与容差。

高精度路径默认规则为 `magnitude_bucket_partition_asc`；快速路径默认规则为 `partition_ascending`。响应中的 `merge_order_basis` 给出可直接审计的文字依据。

## 精度路径选择

`algorithm=auto` 时，引擎根据相对容差、分区数、偏斜、量级比 `max_abs/min_positive_abs`、估算工作量与预算决定：

- 严格容差或极端量级比：`double_double_fixed`，高精度路径。
- 中等严格容差、分区较多或存在偏斜：`kahan`，高精度路径。
- 宽松容差且预算受限：`naive`，快速路径。

响应 `route` 包含 `requested_algorithm`、`resolved_algorithm`、`path`、`basis[]`、`budget_satisfied`、误差模型和适用结论。显式算法只使用指定算法，不会静默切换。

## 持久化、事务与恢复

SQLite 表包括：

- `snapshots`：已加载快照版本和路径/摘要。
- `requests`：请求指纹、状态、提交版本、存储响应/错误。
- `intents`：`pending/completed/failed/aborted` 意图日志。
- `job_plans`：算法、路径、归并规则、容差与声明。
- `partitions`：分区记录数、字节估计、占比、偏斜标记和受影响键。
- `partial_sums`：每分区每键部分和、补偿量、定点整数、计数和摘要。
- `group_results`：最终和、均值、计数、和/均值误差上界和最大偏差。
- `recovery_events`：仅基于持久化事实的恢复动作。

执行顺序：事务一写入/更新请求并写入 `pending` 意图；计算在内存完成；事务二原子写入分区计划、所有分区统计、所有部分和、全部分组结果、`commit_version`、请求完成状态和意图完成状态。任何中途失败都不会留下可见的部分结果。启动恢复将仍为 `running` 的请求和 `pending` 意图标记为 `aborted`，最后一个 `completed` 版本不变；相同内容重试获得新 `attempt`。

人工核对：

```bash
sqlite3 data/engine.db \
  'select request_id,status,commit_version,attempts from requests;'
sqlite3 data/engine.db \
  'select request_id,partition,record_count,joined_records from partitions order by request_id,partition;'
sqlite3 data/engine.db \
  'select request_id,action,detail from recovery_events;'
```

## 错误响应

统一格式：

```json
{"ok":false,"error":{"code":"E_CONFLICTING_REQUEST","message":"...","context":{}}}
```

| 业务码 | 含义 |
| --- | --- |
| `E_SNAPSHOT_INVALID` | 摘要、版本、列布局或文件无效；旧快照保持可用 |
| `E_PRECISION_TOLERANCE_EXCEEDED` | 某键跨分区偏差超过容差，带键和双方数值 |
| `E_CONFLICTING_REQUEST` | 相同 `request_id` 使用了不同内容 |
| `E_EXECUTION_ABORTED` | 未完成意图已被恢复扫描标记中止 |
| `E_EXECUTION_IN_PROGRESS` | 结果尚未完整提交或同请求仍在运行 |
| `E_INVALID_REQUEST` | 参数校验失败 |
| `E_NOT_FOUND` / `E_NOT_READY` | 请求不存在 / 快照尚未加载 |

## 测试与实测结果

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
./scripts/run_experiment.sh
```

本次实际验证结果（2026-11-24）：

- `go test ./... -count=1` 与 `go test -race ./... -count=1`：全部通过；覆盖快照确定性/损坏拒绝、三种求和、Join/左外语义、多分区一致性、精度错误上下文与确定性键选择、快照旧版本保留、持久化版本下界、幂等并发、冲突、恢复续算和恢复前残留产物回滚。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go build ./...`：通过。
- `./scripts/run_experiment.sh`：退出码 `0`，报告 `artifacts/experiment/report.json`。
- 固定样本左/右 SHA-256：`ad62331f2267d10f9c0256b557f4ef3c8ca9b3c0c52f8a9030ead2ec4d543c2e` / `a23309837363bc5e9f2f0d8233de31ab52052deccdd40ddc2b99dac78b5511ff`。
- 三种算法单分区与四分区同键最大相对偏差均为 `0`。
- 相对 DoubleDouble 参考结果的最大实际差异：朴素 `2.447569544025058e-4`，Kahan `1.387112469481221e-6`，DoubleDouble `0`。
- 自动路径在严格容差和极端量级比下选择 `double_double_fixed` / `high_precision`，依据同时包含偏斜占比 `0.494699 > 0.35` 与量级比 `1e27`。
- 强杀后 SQLite 中 `exp-crash-recover` 保留 `aborted` 意图和 `aborted_uncommitted_attempt` 事件；重启查询返回 `E_EXECUTION_ABORTED`；相同请求续算为 `attempt=2`，与已提交 DoubleDouble 参考逐位一致。
- 同请求重复提交复用同一 `commit_version`；不同算法内容返回 `E_CONFLICTING_REQUEST`；损坏快照返回 `E_SNAPSHOT_INVALID` 且活动版本未改变。
- SQLite 直查显示 8 个 `completed` 请求（共 8 个提交版本）、1 个 `aborted` 意图、1 条恢复事件，以及 23 行分区统计、160 行部分和和 160 行分组结果；`sha256sum -c` 对两个快照均返回 `OK`。

## 目录

```text
cmd/server           HTTP 服务入口
cmd/samplegen        确定性样本生成器
cmd/experiment       端到端可重复实验器
internal/snapshot    列式二进制格式、摘要和原子写文件
internal/sample      固定种子样本
internal/sum         三种求和、误差模型、状态摘要和稳定归并
internal/engine      哈希分区、稳定排序、Join、偏斜与分区对照
internal/store       SQLite schema、事务、恢复和查询
internal/service     快照原子替换、幂等协调、路径选择和编排
internal/httpapi     本地 JSON API
pkg/joinshuffle      面向外部 Go 模块的公开集成门面
scripts              sh/PowerShell 脚本
docs/design.md       二进制布局、状态机和事务设计细节
```

详细设计见 [docs/design.md](docs/design.md)。
