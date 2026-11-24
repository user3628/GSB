# 本地列式向量化执行组件

这是一套可直接集成的 Go 实现，面向程序内置的确定性列式快照，提供 Scan → Filter → Project → Aggregate 算子链、逐批小向量与合并大向量双路径、定点数值契约、SQLite 意图持久化和强制终止恢复。

实现不依赖 Docker、外部数据文件、网络服务或账号。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，不要求 CGO，可在 Windows 本地文件系统与回环 HTTP 上运行。

## 目录

- `cmd/server`：本地 HTTP 服务入口。
- `cmd/seed`：一次性生成并持久化内置快照与样例算子链。
- `internal/snapshot`：确定性样本生成与快照完整性校验。
- `internal/model`：快照、位图、算子链、执行轨迹和统一业务错误。
- `internal/fixed`：scale=1e6、`int64` 定点运算及溢出/除零/舍入契约。
- `internal/engine`：双路径批执行、Filter 位图与运算、聚合和异常注入。
- `internal/store`：SQLite schema、意图、provisional 批次、最终发布和恢复扫描。
- `internal/service`：版本、快照原子替换、单写者串行控制、比较服务。
- `internal/httpapi`：JSON HTTP API 与统一失败响应。
- `internal/verify`：双路径逐行/聚合定点相对容差比较。
- `scripts/demo.ps1`：Windows PowerShell 完整演示（真实终止进程后恢复）。
- `scripts/demo.sh`：Linux/macOS 等价演示，便于本地复核。

## 快速开始（Windows）

要求 Go 1.22 或以上：

```powershell
go version
go mod download
go test ./...
go run ./cmd/seed -db data/columnar.db
go run ./cmd/server -listen 127.0.0.1:8080 -db data/columnar.db
```

`cmd/seed` 会用固定常量生成快照和样例链并写入 SQLite；重复执行会保留已有版本。服务也支持在空库上自动生成快照。固定样本为：

- 版本：`1`
- 行数：`257`
- 种子：`20240517`
- 列：`id`、`quantity`、`price`（scale=1e6）、`discount`（scale=1e6）、`category`（字典）
- 选择位图：`all`、`none`、`sparse`、`dense`

完整演示：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

演示会清理并重新创建 `.demo-run\columnar.db`，启动服务，完成双路径一致性对比、五类异常注入，随后在第二批后用退出码 `91` 强制终止服务进程，再重启读取恢复报告。

## 快照契约

快照包含单调递增版本号、确定性生成时间、行数、列定义、字典表、各列空值位图和整体 SHA-256。

加载时在发布前检查：

- `row_count > 0`、版本为正、生成时间可解析、列名唯一。
- 所有空值位图长度严格等于 `ceil(row_count/8)`，尾部越界位必须为 0。
- 整数列与定点列长度等于行数。
- 定点列 `scale_units` 必须为 `1000000`。
- 非空字典索引必须落在字典表范围内；空值的字典索引只允许 `-1` 或合法索引。
- 四种选择位图都存在，行数等于快照行数且字长规范。
- 重新计算的 SHA-256 必须与声明摘要一致。

任何失败返回 `E_SNAPSHOT_INVALID`。持久化失败时内存中的旧快照不会被替换，旧版本仍可查询和执行。

## 选择位图语义

位图按 64 位字存储，`Rows` 与字长分离，从而能检出长度不匹配。Filter 在每一批中：

1. 根据批次输入构造新的输入位图；
2. 新建谓词位图；
3. 用 `predicateBits AND batchInputBits` 得到新位图；
4. 从不修改输入位图。

零选择率是合法结果：批次数为 0，聚合计数值为 0，不返回错误。位图长度、空字数组与行数不一致时返回 `E_BITMAP_MISMATCH`。

## 双执行路径

路径由请求批大小、大向量阈值和预选行数共同决定：

- `merged_large_vector`：`batch_size >= threshold` 且 `selected_rows >= threshold`，把所有预选行合并为一个批次。
- `small_vector`：其他情况，按 `batch_size` 切批。

`PathDecision` 输出 `path`、`selected_rows`、`batch_size`、`large_threshold`、`batch_count` 和可读的 `reason`。

两条路径共享同一聚合累加器语义。最终均值只对总 sum/count 计算一次，不合并已舍入的批次均值，因此结果逐位一致；比较服务仍按固定点单位提供相对容差参数。

## 定点契约

- 所有定点值均为 `int64`，scale 固定为 `1_000_000`。
- 加、减、乘、除、均值中间积使用大整数计算并检查最终 `int64` 边界。
- 溢出返回 `E_NUMERIC_OVERFLOW`，上下文包含 `column`、`batch`（适用时）和 `operation`。
- 除零返回 `E_DIVIDE_BY_ZERO`。
- 均值使用 `half_away_from_zero`，文本固定 6 位小数。
- `rounding_error_units = sum - rounded_units * count`，同批重复计算逐位一致。

## 版本与并发

- 快照版本必须单调递增；落后版本返回 `E_STALE_VERSION`。
- 算子链按 `(chain_id, version)` 持久化；提交版本不高于当前版本返回 `E_STALE_VERSION`。
- 同一 `(chain_id, chain_version)` 的执行在进程内使用单写者互斥锁串行推进。
- 批次结果先以 `provisional` 落盘；最终事务同时完成：
  - `provisional` → `committed`；
  - 写入最终聚合；
  - 执行状态改为 `committed`。
- 查询只读取 `committed` 执行；运行、失败、恢复中的执行不会返回批次或聚合，避免看到部分结果。

## SQLite 意图、失败与恢复

数据库默认路径由 `-db` 指定。主要表：

- `snapshots`：完整快照和摘要。
- `chains`：版本化算子链。
- `executions`：路径决策、位图、状态和错误。
- `batches`：`pending` / `provisional` / `committed` 批次事实。
- `aggregate_results`：只有完整提交版本才存在。
- `intents`：路径选择、批次准备、批次结果、最终发布和失败清理意图。
- `recovery_reports`：持久化恢复报告。

顺序保证：

1. 执行创建事务先写路径意图，再创建执行和 pending 批次。
2. 每批计算前写 `batch_prepare` 意图。
3. 批次结果事务先写 `batch_result` 意图，再写 provisional 轨迹与聚合。
4. 最终事务先写 `finalize` 意图，再发布批次、最终聚合和执行状态。

启动时扫描 `running` 执行：

- `provisional` 批次回滚为 `pending`；
- provisional 批次聚合与最终聚合全部丢弃；
- 执行改为 `recovered`，业务码 `E_EXEC_ABORTED`；
- 恢复报告包含已落盘意图、路径、回滚批次、丢弃聚合和逐项差异。

## HTTP API

默认回环地址 `127.0.0.1:8080`。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 健康检查 |
| GET | `/api/v1/meta` | 业务码、位图形态、路径与状态枚举 |
| POST | `/api/v1/snapshots/generate` | 用内置生成器创建并加载快照 |
| POST | `/api/v1/snapshots/load` | 校验后加载完整快照 JSON |
| GET | `/api/v1/snapshots/current` | 当前快照 |
| GET | `/api/v1/snapshots/{version}` | 按版本读取快照 |
| POST | `/api/v1/chains` | 提交带版本的算子链 |
| GET | `/api/v1/chains/{id}/versions/{version}` | 读取算子链版本 |
| POST | `/api/v1/chains/{id}/execute` | 触发执行，可注入异常 |
| GET | `/api/v1/chains/{id}/versions/{version}/executions` | 列出执行 |
| GET | `/api/v1/chains/{id}/versions/{version}/batches` | 读取已提交批次轨迹，可带 `execution_id` |
| GET | `/api/v1/chains/{id}/versions/{version}/aggregates` | 读取已提交聚合，可带 `execution_id` |
| GET | `/api/v1/executions/{id}` | 读取执行；未提交时不返回部分结果 |
| GET | `/api/v1/executions/{id}/compare?right=...&tolerance_ppm=1` | 双执行逐行/定点聚合比较 |
| GET | `/api/v1/recovery/reports` | 恢复报告 |
| GET | `/api/v1/faults` | 五类注入及业务码 |
| POST | `/api/v1/faults/probe` | 查询单个注入业务码 |

统一失败响应：

```json
{
  "error": {
    "code": "E_NUMERIC_OVERFLOW",
    "message": "injected fixed point overflow",
    "context": {
      "column": "price",
      "batch": 2,
      "execution_id": "fault-fixed_overflow"
    }
  }
}
```

### 生成快照

```json
{"version":1,"rows":257,"seed":20240517}
```

### 执行请求

```json
{
  "execution_id": "small-all",
  "chain_version": 1,
  "bitmap_form": "all",
  "batch_size": 8,
  "large_vector_threshold": 128,
  "injections": []
}
```

异常注入的 `kind` 为：

| kind | 业务码 |
|---|---|
| `bitmap_mismatch` | `E_BITMAP_MISMATCH` |
| `fixed_overflow` | `E_NUMERIC_OVERFLOW` |
| `dict_index` | `E_DICT_INDEX` |
| `divide_by_zero` | `E_DIVIDE_BY_ZERO` |
| `exec_aborted` | `E_EXEC_ABORTED` |

`crash_after_batch` 仅用于恢复演示；服务必须带 `-allow-crash`（或环境变量 `COLUMNAR_ALLOW_CRASH=1`）才接受，正常生产启动默认关闭。

## 验证结果

完整原始记录见 `artifacts/validation.txt`。当前工作区实际执行：

```text
$ go test ./...
ok  columnar-executor/internal/fixed
ok  columnar-executor/internal/httpapi
ok  columnar-executor/internal/model
ok  columnar-executor/internal/service
ok  columnar-executor/internal/snapshot
```

`PORT=18181 ./scripts/demo.sh` 的实际输出：

```text
consistency: small_path=small_vector batches=33; large_path=merged_large_vector batches=1; rows_equal=true; aggregates_equal=true
fault: kind=bitmap_mismatch code=E_BITMAP_MISMATCH http=422 status=failed published=false batches=0 aggregates=0
fault: kind=fixed_overflow code=E_NUMERIC_OVERFLOW http=422 status=failed published=false batches=0 aggregates=0
fault: kind=dict_index code=E_DICT_INDEX http=422 status=failed published=false batches=0 aggregates=0
fault: kind=divide_by_zero code=E_DIVIDE_BY_ZERO http=422 status=failed published=false batches=0 aggregates=0
fault: kind=exec_aborted code=E_EXEC_ABORTED http=409 status=aborted published=false batches=0 aggregates=0
forced termination: process_exit_code=91
recovery: execution=crash-demo status=recovered published=false rolled_back_batches=[1, 2] discarded_aggregates=[1, 2] intents=5
recovery artifact: report=1
```

并发与竞态检查也已执行：

```text
$ go test -race ./...
ok  columnar-executor/internal/fixed
ok  columnar-executor/internal/httpapi
ok  columnar-executor/internal/model
ok  columnar-executor/internal/service
ok  columnar-executor/internal/snapshot
```

另外已执行 Windows 交叉编译：

```text
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/server
```

当前执行环境为 WSL/Linux，因此实际进程终止与恢复演示在 Linux 上完成；同一代码路径已通过 Windows 编译，PowerShell 脚本提供 Windows 本地等价流程。

## 设计边界

- 快照是进程内不可变值，并持久化到 SQLite；执行始终绑定 `base_snapshot_version`。
- 每个算子链版本在单进程内单写者；跨进程同时写同一 SQLite 文件不在本组件保障范围内。
- 字典聚合提供 `count_distinct`；数值聚合提供 `count`、`count_non_null`、`sum`、`avg`、`min`、`max`。
- 结果按完整执行版本发布，不以“已算完的批次数”对外暴露进行中的聚合结果。
