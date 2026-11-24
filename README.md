# 确定性训练数据预处理管线

本仓库交付一个可在 Windows 空白工作区直接构建运行的 Go 1.22+ 组件。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，不需要 CGO 或 Docker。服务默认监听 `http://127.0.0.1:18080`。

## 快速开始（Windows PowerShell）

```powershell
go version
go mod download

$env:DATA_DIR = "D:\ppt-data"
$env:ADDR = "127.0.0.1:18080"
go run ./cmd/server
```

也可以直接在仓库根目录运行 `go run . server` 启动服务。

另开一个 PowerShell：

```powershell
# 写入内置固定策略 v1
curl.exe -X POST http://127.0.0.1:18080/v1/policies/default

# 生成固定样本并跑完整五阶段
curl.exe -X POST "http://127.0.0.1:18080/v1/demo/run?batch_id=demo-0001"

# 逐阶段重算验证
go run ./cmd/verify --data-dir $env:DATA_DIR --batch demo-0001
```

等价的根入口形式是：

```powershell
go run . verify -data-dir $env:DATA_DIR -batch demo-0001
```

也可以构建本地 EXE：

```powershell
go build -o .\bin\preprocess-server.exe .\cmd\server
go build -o .\bin\preprocess-verify.exe .\cmd\verify
.\bin\preprocess-server.exe --data-dir D:\ppt-data
.\bin\preprocess-verify.exe --data-dir D:\ppt-data --batch demo-0001
```

如果默认 Go 代理下载超时，可设置 `$env:GOPROXY="https://goproxy.cn,direct"`。

## 输入与清洗规则

原始记录每行必须是 5 个 `|` 分隔字段：

```text
timestamp_ms|sample_id|text_length|label|shard_key
```

示例：

```text
1767225660000|demo-sample-000000|72|alpha|north
```

- 字段非法、时间戳/长度无法解析：`E_RECORD_INVALID`，整批不写入 `batches` 或阶段结果；仅保留执行意图/拒绝事实。
- 同批次样本 ID 重复：保留首次出现的记录，后续计入 `duplicate_count` 和 `duplicate_ids`。
- 空文本（`text_length=0`）：默认 `keep_null_mask`，张量对应特征写零并置空值掩码；策略也支持 `drop`。
- 超长文本（长度大于 `cleaning.max_text_length`）：默认 `truncate`，有效长度截断到阈值；策略也支持 `drop`。
- 非法标签：默认 `reject_batch` 并返回 `E_LABEL_INVALID`；策略可改为 `drop`。
- 空分片键：返回 `E_SHARD_KEY_MISSING`。

## HTTP 接口

所有失败响应均包含顶层和 `error` 对象两份业务码：

```json
{"ok":false,"code":"E_LABEL_INVALID","message":"...","details":{},"error":{"code":"E_LABEL_INVALID","message":"..."}}
```

| 方法与路径 | 用途 |
| --- | --- |
| `GET /healthz` | 健康检查 |
| `GET /v1/generator/policy` | 获取带正确摘要的固定策略 v1 |
| `GET /v1/generator/raw?count=96&namespace=demo` | 获取固定种子原始样本 |
| `POST /v1/policies/default` | 首次启动时写入默认策略；已存在则返回当前策略 |
| `POST /v1/policies` | 提交完整策略快照；旧版本返回 `E_STALE_SNAPSHOT` |
| `POST /v1/policies/patch` | 提交只含受影响字段和 `base_version` 的增量；基础版本不符返回 `E_STALE_BASE` |
| `GET /v1/policies/{version}` | 查询指定策略版本 |
| `POST /v1/batches` | 提交批次；支持 JSON 或 query 参数 |
| `POST /v1/injections/{kind}` | 异常注入专用入口 |
| `GET /v1/batches/{batchID}` | 查询批次持久化状态和最终响应 |
| `GET /v1/batches/{batchID}/stages?policy_version=0` | 查询阶段摘要 |
| `GET /v1/batches/{batchID}/shards?policy_version=0&include_invalid=true` | 查询分片索引和统计 |
| `GET /v1/recovery` | 查询最近一次恢复报告 |
| `POST /v1/recovery` | 在单写者锁保护下手动执行同一恢复扫描 |

批次示例：

```powershell
$body = @{ batch_id = "b-raw"; raw_input = "1767225660000|a|10|alpha|north`n1767225720000|b|20|beta|south`n" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:18080/v1/batches -ContentType "application/json" -Body $body
```

## 异常注入

可使用 `POST /v1/injections/{kind}`，也可在 `POST /v1/batches?inject=<kind>` 中使用：

| `kind` | 业务码 | 持久化行为 |
| --- | --- | --- |
| `invalid_label` | `E_LABEL_INVALID` | 不产生批次阶段/分片 |
| `missing_shard_key` | `E_SHARD_KEY_MISSING` | 不产生批次阶段/分片 |
| `sample_ratio_invalid` | `E_SAMPLE_RATIO_INVALID` | 采样比例越界，不进入执行 |
| `id_conflict` | `E_ID_CONFLICT` | 与已提交样本身份冲突，新批次不提交 |
| `stage_interrupted` | `E_STAGE_INTERRUPTED` | 保留已完成阶段摘要，无可见分片，意图保持未完成 |

附加故障钩子：

- `inject=record_invalid`：返回 `E_RECORD_INVALID`。
- `inject=digest_mismatch`：用于 `/v1/policies/patch?inject=digest_mismatch`，返回 `E_SHARD_DIGEST_MISMATCH`，策略版本不前进，旧分片保留。
- `inject=force_kill&interrupt_stage=tensorize`：模拟进程以 137 终止；该钩子不返回 JSON，重启时由恢复扫描处理。

## 确定性与张量

- 策略摘要是将 `digest` 置空后的规范 JSON 的 SHA-256。
- 每个阶段的随机源为 `SplitMix64(SHA256("preprocess-pipeline-v1", stage, input_digest, policy.seed))`；不使用全局 `math/rand`、`crypto/rand` 或当前时间生成阶段结果。
- 阶段链为 `parse_clean -> filter -> shard -> sample -> tensorize`；每阶段均保存 `input_digest`、`output_digest` 和统计 JSON。
- 分片使用 FNV-1a(`salt + shard_key`) 对声明分片数取模，保证不重不漏。
- 偏斜阈值只产生 `W_RECORD_SKEW` / `W_BYTE_SKEW` 告警，不改变分片归属。
- `.tensor` 文件包含 `PPT1` 头、JSON header 和 Little-Endian `float32` body；header 含 `dtype`、`shape`、`sample_count`、`feature_dim`、`null_mask`、`null_count` 与 body 摘要。

## 并发、事务与恢复

- 所有策略/批次写操作竞争同一把进程内写锁，等价于按当前版本串行的单写者；HTTP 读请求通过 SQLite WAL 并发读取。
- 相同 `batch_id` 的并发请求由 leader 执行，其余等待并复用同一 `intent_id` 和结果。
- 分片先写入 `%DATA_DIR%\tmp\<intent_id>\*.tensor`，校验摘要后原子 rename 到 `objects\...\<intent_id>\shard-xxx.tensor`。
- 张量阶段摘要、ready 分片索引、样本身份、批次完成状态和意图完成状态在同一个 SQLite 事务提交；读者不会看到“分片索引已提交但 tensorize 摘要未提交”的状态。
- 策略增量提交会重放所有已提交批次；新策略、新阶段摘要、新分片索引和旧分片 `invalidated` 状态在同一事务边界提交。
- 启动时自动扫描 `running/prepared` 意图：删除该意图临时目录和未索引的新产物，将意图和运行中批次在同一事务中标记为 `aborted`，报告最后完整阶段与 `shards_to_rebuild`。

## 验证工具

```powershell
go run ./cmd/verify --data-dir $env:DATA_DIR --batch demo-0001
```

工具读取 SQLite 中的原始输入、策略和阶段摘要，重新执行五个阶段，并额外读取磁盘 `.tensor` 复核 SHA-256、header 计数和重算摘要。一致时退出码为 0；不一致时输出每个失配阶段的：

```json
{
  "stage": "sample",
  "expected_output_digest": "...",
  "actual_output_digest": "..."
}
```

编译后的验证工具在失配时以退出码 2 结束；使用 `go run` 时 Go 包装层可能打印 `exit status 2`，shell 看到的 `go run` 退出码可能为 1，但 stdout 中的 JSON 仍包含完整失配报告。

## 代码地图

- `internal/domain`：策略、固定生成器、五阶段纯函数、确定性 RNG、张量编码。
- `internal/store`：SQLite schema、策略/批次/阶段/分片/意图/恢复记录持久化。
- `internal/app`：单写者、同批次合并、文件发布、策略级联和恢复扫描。
- `internal/api`：HTTP 路由与统一错误 JSON。
- `internal/verify`：持久化摘要回放比对。
- `cmd/server`、`cmd/verify`：HTTP 服务和 CLI 验证工具。

## 实际验证结果（2026-11-25）

在交付环境中实际执行：

- `go version`：`go version go1.27.4 linux/amd64`；`go.mod` 声明 `go 1.22`，可面向 Windows Go 1.22+。
- `go test ./... -count=1`：全部包通过；`internal/app` 与 `internal/domain` 均 `ok`。
- `go test -race ./internal/app ./internal/domain -count=1`：通过。
- `CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./cmd/server ./cmd/verify`：成功，产物为 PE32+ x86-64。
- 全新临时 SQLite/数据目录 HTTP 验收：5 个阶段、4 个分片；验证器 `ok=true, checked_stages=5, checked_shards=4`。
- 五类注入分别返回 `E_LABEL_INVALID`、`E_SHARD_KEY_MISSING`、`E_SAMPLE_RATIO_INVALID`、`E_ID_CONFLICT`、`E_STAGE_INTERRUPTED`。
- `stage_interrupted@tensorize` 后持久化阶段为 `parse_clean/filter/shard/sample`，ready 分片数为 0；恢复报告给出 `last_complete_stage=sample`、`shards_to_rebuild=[0,1,2,3]`、临时目录已删除。
- `inject=force_kill` 时服务进程以 137 退出；重启日志为 `startup_recovered_intents=1`，恢复报告给出 `trigger=startup`、未完成意图数 1、空最后阶段边界（在解析前强杀时）和 `shards_to_rebuild=[0,1,2,3]`。
- 旧版本重复提交返回 `E_STALE_SNAPSHOT`，错误 `base_version` 返回 `E_STALE_BASE`；合法 patch 到 v2 后验证器对 v2 回放 `ok=true,mismatches=0`，v1 分片状态为 `invalidated`，v2 有 4 个 ready 分片。
- `inject=digest_mismatch` 返回 `E_SHARD_DIGEST_MISMATCH`；策略版本仍为 2，旧分片摘要未变且验证通过。
- 在数据目录副本中把 `sample` 阶段的 `output_digest` 篡改为 64 个 `0` 后，验证器输出失配阶段 `sample`、期望摘要 `000000000000...` 与实际摘要，并由编译后的验证器返回退出码 2。

交付沙箱的 `127.0.0.1:18080` 被无关进程 `motionlab` 占用，因此上述实时 HTTP 验收使用同一固定配置的 `127.0.0.1:18082` 和 `ADDR` 覆盖；代码与 Windows 默认值仍为 `127.0.0.1:18080`。

---

## 原始任务说明

训练数据预处理方案已完成评审，现在需要在 Windows 空白工作区将其实现为可运行组件。输入由程序内置固定种子生成器产出：原始记录（每行含毫秒时间戳、样本标识、文本片段长度、标签、分片键）与带版本号的预处理策略快照（清洗规则、过滤阈值、分片规则、归一化参数、随机种子与采样比例）。管线需依次完成解析清洗、过滤、分片、确定性采样与张量化输出，把每个分片产物写入本地存储，并把每个阶段的输入摘要、输出摘要与统计写入 SQLite，供验证工具按版本回放核对。核心难点是阶段可复现与异常可验证：任何阶段被强制终止后，重启都必须依据持久化事实恢复或中止，且不得产生半成品分片；策略快照以增量片段提交时，受影响分片必须级联失效并在同一事务边界内重建。

任务环境与准备：
在 Windows 空白工作区安装 Go 1.22 或以上版本，并确认 go version 可用。
使用 go mod init 初始化模块，引入现代 SQLite 驱动的纯 Go 绑定（如 modernc.org/sqlite 或 mattn/go-sqlite3），不要求 CGO。
程序内置固定种子生成器产出原始记录样本与策略快照，运行前无需外部数据集或网络。
启动前设置本地数据目录用于分片产物写入，SQLite 数据库文件放在同一目录下。
通过 go run 或 go build 生成本地可执行程序，HTTP 服务监听回环地址的固定端口。

业务要求：
1. 实现样本解析与清洗：逐行解析时间戳、样本标识、文本片段长度、标签与分片键，非法行返回业务码 E_RECORD_INVALID 并整批不入库；同一批次内样本标识重复须按确定性规则保留首条并把重复条数计入统计，清洗规则须显式声明对空文本、超长文本与非法标签的处理方式。
2. 实现带版本号的策略快照与基础版本校验：快照含单调递增版本号、生成时间、清洗与过滤规则、分片规则、归一化参数、随机种子与采样比例，以及整体校验摘要；提交时版本号不大于当前版本返回 E_STALE_SNAPSHOT，增量片段只含受影响字段并携带基础版本，基础版本非当前版本返回 E_STALE_BASE 且不改变任何状态。
3. 实现固定种子驱动的阶段确定性：从解析、过滤、分片、采样到张量化的每一步随机性只能来自策略快照声明的种子与本阶段输入摘要的组合，禁止使用全局随机源或当前时间；同一策略版本与同一样本重复执行必须逐位一致，中间摘要须写入 SQLite 并可逐阶段复核。
4. 实现分片规则与张量化输出：按分片键把记录划分到声明数量的分片并保证分片间不重不漏，样本数与字节量的分布偏斜超过声明阈值时输出偏斜告警但不得改变分片归属；张量化输出须带数据类型、空值掩码与样本数，写入前校验摘要一致，不一致返回 E_SHARD_DIGEST_MISMATCH 并保留旧分片产物。
5. 实现异常注入接口：请求参数可注入非法标签、分片键缺失、采样比例越界、重复标识冲突与阶段中途注入中断标记五类情形，每类返回互不相同的业务码（至少含 E_LABEL_INVALID、E_SHARD_KEY_MISSING、E_SAMPLE_RATIO_INVALID、E_ID_CONFLICT、E_STAGE_INTERRUPTED），注入后复查的阶段统计与分片状态必须与持久化事实一致。
6. 实现单写者执行与多读者查询的并发一致：管线执行按策略版本单写者串行，多个验证查询可并发读取已提交结果；同一批次标识重复提交只执行一次、其余等待并复用结果；查询只能看到某一完整提交版本，不得出现某分片已写入而其阶段摘要尚未提交的中间状态。
7. 使用 SQLite 持久化阶段摘要、分片产物索引与意图日志，执行前先写意图记录再提交事务；进程被强制终止后重启须扫描未完成意图，把批次回滚到最后一个完整阶段边界或标记为已中止，恢复报告只包含已落盘事实并可据此判定哪些分片需重算。
8. 提供本地 HTTP 接口与结果验证工具：接口覆盖策略提交、样本批次提交、按批次查询阶段摘要与分片统计、恢复报告查询，失败响应统一为含业务码的 JSON；验证工具依据持久化阶段摘要逐阶段重算并比对，输出不一致阶段、期望摘要与实际摘要。

执行约束：不使用 Docker 或其他容器环境；使用本地确定性样本验证，不访问未授权目标。按上述任务意图交付相应代码、测试或技术说明，记录实际验证结果。

验收：
本地样本跑通各阶段并逐阶段复算摘要；异常注入与强制终止后恢复报告须与持久化事实一致。
