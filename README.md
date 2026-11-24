# RecordLab：不可信二进制本地分析工作台

RecordLab 是一个纯本地、确定性的 Go 工作台，用于验证长度前缀记录容器的安全解码、嵌套资源约束、单任务/全局内存预算裁决、SQLite 版本快照恢复，以及并发下的可重复报告。

## 交付内容

- `internal/workbench/decoder.go`：长度前缀解析、边界校验、嵌套深度/累计预算、恢复轨迹、确定性分配计量。
- `internal/workbench/generator.go`：内置确定性样本与任务清单生成器，不依赖时间或系统随机源。
- `internal/workbench/engine.go`：并行调度、声明预算准入、单任务实测预算裁决、断点续跑。
- `internal/workbench/store.go`：SQLite 单事务版本快照、完整状态哈希、父链哈希、跳号/篡改检测。
- `internal/workbench/report.go`：按任务标识稳定排序的确定性 JSON 报告。
- `internal/workbench/injection.go`：五类异常注入验证入口。
- `main.go`：`run/versions/edit/replay/rejected/export/inject/sample` 子命令界面。
- `*_test.go`：边界、预算、快照恢复、并发可重复性与注入测试。

## 环境与构建

要求 Go 1.22 或更高版本。SQLite 使用锁定的 `github.com/mattn/go-sqlite3 v1.14.24`，该驱动需要 CGO。

Windows 本地准备：

1. 安装 Go 1.22+，确认 `go version` 可用。
2. 安装可用的 C 编译器（如 MSYS2/MinGW-w64 或 TDM-GCC），并确认 `gcc` 在 `PATH` 中。
3. 执行 `go env -w CGO_ENABLED=1`。
4. 在项目目录执行：

```powershell
go mod download
go build -o recordlab.exe .
go test ./... -count=1
```

运行期不需要网络、账号、容器或外部样本。SQLite 文件和报告写入任意本地可写目录。JSON 报告、快照状态和注入结论不含绝对路径、实际时间或随机标识。

## 快速运行

```powershell
$work = Join-Path $env:TEMP "recordlab-demo"
New-Item -ItemType Directory -Force $work | Out-Null
.\recordlab.exe run --db "$work\state.db" --out "$work\report.json" --concurrency 4
.\recordlab.exe versions --db "$work\state.db"
.\recordlab.exe rejected --db "$work\state.db"
.\recordlab.exe replay --db "$work\state.db" --task t05-depth --step 1
.\recordlab.exe inject --dir "$work\inject" --kind all --out "$work\injections.json"
```

## CLI

| 命令 | 作用 |
| --- | --- |
| `run` | 生成/继续任务，执行解码与预算裁决，写 JSON 报告；`--concurrency` 控制工作协程数，`--stop-after N` 用于确定性中断验证。 |
| `versions` | 展示版本链、状态哈希和链哈希；检测跳号并显示 `E_SNAPSHOT_VERSION`。 |
| `edit` | 修改样本参数或预算策略，提交新的 `policy-edit` 快照并清空旧任务结果。 |
| `replay` | 从已提交快照回放某任务；`--step N` 查看单条状态迁移事件。 |
| `rejected` | 展示被拒任务、业务码、预算和原因。 |
| `export` | 从快照重新导出最终 JSON 报告。 |
| `inject` | 运行五类异常注入并断言业务码、控制组结论和快照完整性。 |
| `sample` | 导出确定性二进制样本，便于独立检查。 |

配置参数：`--seed --records --payload --max-depth --nested-budget --global-budget --task-budget --scenario --inject --config FILE`。`--config` 接受只包含需要修改字段的 JSON，例如：

```json
{"max_depth": 3, "nested_budget": 512, "global_memory_budget": 131072}
```

## 二进制格式

容器头 15 字节，全部定长整数使用大端：

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 4 | `RXC1` |
| 4 | 1 | 格式版本 `1` |
| 5 | 2 | 记录数 `uint16` |
| 7 | 8 | 记录流声明长度 `uint64` |

每条记录：`record_id uint16 | declared_body_len uint64 | body`。body 首部为 `field_count uint16`，随后是字段：`kind uint8 | declared_len uint64 | payload`。

- `kind=1`：变长字段，payload 长度必须等于声明长度。
- `kind=2`：嵌套块，payload 首部为 `child_count uint16`，其余为子记录序列。
- 所有 `offset + length` 都在父级边界内重新计算；不会按恶意声明长度预分配内存。

错误优先级和语义：

| 业务码 | 触发条件 |
| --- | --- |
| `E_TRUNCATED` | 声明长度超出父级剩余字节，但长度加法没有溢出。 |
| `E_LENGTH_WRAP` | `uint64(offset)+length` 发生无符号回绕。 |
| `E_LENGTH_OVERFLOW` | 长度和不回绕，但超过有符号地址范围 `MaxInt64`，无法安全物化。 |
| `E_DEPTH_EXCEEDED` | 子记录深度超过 `--max-depth`。 |
| `E_NESTED_BUDGET` | 单父记录内子声明累计超过父级剩余量或 `--nested-budget`。 |
| `E_TASK_MEMORY_BUDGET` | 声明预算超过单任务上限，或实测峰值超过任务声明预算/单任务上限。 |
| `E_GLOBAL_BUDGET` | 按任务标识顺序累计声明预算时，无法放入剩余全局预算。 |
| `E_SNAPSHOT_VERSION` | 版本跳号、状态哈希不符、父链断裂或状态迁移不连续。 |

严重边界错误保留此前完整解码记录作为安全前缀，并单独记录失败记录。深度/嵌套预算错误被视为可恢复子树错误：终止子树、跳到父记录边界、继续父级兄弟记录；轨迹包含 `subtree_abort` 与 `parent_recover` 事件及其字节偏移。

## 内存度量与预算

解码器通过显式分配钩子统计解析器实际拥有的字节数：变长字段副本、记录摘要槽、轨迹/问题数组增长和摘要字符串。临时 payload 副本在哈希/拷贝完成后归还；峰值按存活字节的历史最大值记录，分配次数为显式分配操作数。

该计量不读取 `runtime.MemStats`，也不做事后按样本长度估算，因此不受 GC、协程调度或并发度影响。输入样本生成、SQLite 和最终 JSON 序列化不计入单任务解码预算。

准入按任务标识稳定排序：先检查任务声明预算是否超过单任务上限，再从剩余全局预算中永久扣除已准入任务的声明预算。超过并发度的任务进入工作队列排队，不产生拒绝码。实测峰值超预算在解码结束后标记为 `E_TASK_MEMORY_BUDGET`，任务仍保留在报告中。

## 快照与恢复

`snapshots` 表保存 `version`、`kind`、`parent_digest`、完整 `state_json`、`state_sha256`、`chain_digest`。每个任务结果在独立 SQLite 事务中提交；状态包含配置、任务定义、已完成结果、轨迹、度量与版本号。

加载时同时校验：

- 版本必须从 1 连续递增。
- 完整状态 JSON 的 SHA-256 必须匹配。
- 父链哈希必须匹配，链哈希覆盖父摘要与当前状态摘要。
- 状态内嵌版本必须匹配行版本。
- 策略快照不得携带结果；任务快照只能追加一条新结果，已提交结果不得被改写。

因此仅替换版本号、替换完整状态、删除中间版本或改写旧任务结果都会被拒绝。运行时会回退到最近连续合法版本并从该状态继续；已提交任务不重做，未提交任务重做。

为了让并发度不改变报告字节：工作协程可以并行解码，但提交前沿始终按任务标识排序逐个事务提交。并发度不进入快照状态，也不出现在报告中。

## 报告确定性

报告包含任务标识、声明预算、准入码、解码码、裁决码、最终状态、样本摘要、记录摘要、峰值内存、分配次数、轨迹、拒绝原因、全局资源使用和完整快照链。任务按 `task_id` 稳定排序，记录按解析序号排序；没有墙钟字段，仅使用固定采样标记 `FIXED_SAMPLE_V1`。

## 异常注入

`inject --kind all` 在独立临时目录创建隔离数据库，覆盖：

1. `length_wrap`：`E_LENGTH_WRAP`。
2. `deep_nesting`：`E_DEPTH_EXCEEDED`。
3. `task_budget`：`E_TASK_MEMORY_BUDGET`。
4. `global_budget`：`E_GLOBAL_BUDGET`。
5. `snapshot_gap`：实际删除中间快照，断言 `E_SNAPSHOT_VERSION`、回退到版本 1、恢复后报告与全新运行逐字节一致。

每个数据注入同时与未注入控制组比较：非目标任务的任务结论、样本摘要、记录摘要、轨迹和度量必须一致。注入只通过参数和隔离数据库完成，不修改源码，也不污染工作区预算/并发配置。

## 实际验证结果

本机（Go 1.27.4，linux/amd64，CGO 开启）执行：

```text
$ go test ./... -count=1
ok  	recordlab	0.244s
ok  	recordlab/internal/workbench	0.951s

$ go test -race ./... -count=1
ok  	recordlab	1.582s
ok  	recordlab/internal/workbench	2.343s

$ go vet ./...
（无输出）

$ go build ./...
（无输出）

$ go build -gcflags='recordlab/...=-lang=go1.22' ./...
（无输出；按 Go 1.22 语言版本编译通过）
```

基线报告摘要（`--concurrency 4`，9 个任务）：

```json
{
  "task_count": 9,
  "completed_count": 7,
  "rejected_count": 2,
  "finding_count": 5,
  "global_memory_budget": 262144,
  "global_reserved_bytes": 229384,
  "global_peak_sum_bytes": 53456,
  "task_memory_budget": 131072,
  "snapshot_count": 10
}
```

任务结论：`t01 E_OK`、`t02 E_TRUNCATED`、`t03 E_LENGTH_WRAP`、`t04 E_LENGTH_OVERFLOW`、`t05 E_DEPTH_EXCEEDED`、`t06 E_NESTED_BUDGET`、`t07 E_TASK_MEMORY_BUDGET`、`t08 E_GLOBAL_BUDGET`、`t09 E_OK`。`t07` 实测峰值 5224 字节，声明预算 8 字节；`t08` 声明预算 120000 字节，因剩余全局预算不足被拒。

并发可重复性：分别以 `--concurrency 1`、`2`、`8` 在三个全新数据库运行，报告 SHA-256 均为：

```text
641ff117728e96478cdafcce508b0098ba2da1a515c3847ce04ebc275481647d
```

中断恢复：`--stop-after 3` 首次得到 `snapshots=4 new_tasks=3 interrupted=true`；再次运行得到 `snapshots=10 new_tasks=6`；第三次运行得到 `new_tasks=0`。恢复后的最终报告与一次跑完的报告 SHA-256 完全相同。

注入结果：

```json
{
  "all_passed": true,
  "cases": [
    {"kind": "deep_nesting", "actual_target_code": "E_DEPTH_EXCEEDED", "controls_unaffected": true, "snapshots_valid": true, "passed": true},
    {"kind": "global_budget", "actual_target_code": "E_GLOBAL_BUDGET", "controls_unaffected": true, "snapshots_valid": true, "passed": true},
    {"kind": "length_wrap", "actual_target_code": "E_LENGTH_WRAP", "controls_unaffected": true, "snapshots_valid": true, "passed": true},
    {"kind": "snapshot_gap", "actual_target_code": "E_SNAPSHOT_VERSION", "controls_unaffected": true, "snapshots_valid": true, "snapshot_fallback_version": 1, "passed": true},
    {"kind": "task_budget", "actual_target_code": "E_TASK_MEMORY_BUDGET", "controls_unaffected": true, "snapshots_valid": true, "passed": true}
  ]
}
```

`artifacts/demo-run/report.json`、`artifacts/demo-run/injections.json` 以及 `artifacts/concurrency-{1,2,8}/report.json` 是本次运行保留的验收产物。

## Windows 一致性说明

报告不包含平台路径或时间；所有整数编码、排序和 JSON 序列化均为显式确定逻辑。内存计量使用固定结构与显式字节计数，在 Windows 本地相同架构、相同构建参数下可重复。请使用同一 `go.mod` 锁定版本并以 `go build -o recordlab.exe .` 构建，避免不同 SQLite/CGO 工具链影响二进制本身；SQLite 文件不要求字节一致，报告与业务结论要求字节一致。
