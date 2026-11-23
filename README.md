# Logic Plan Snapshot Editor

一个本地 SQLite 支撑的逻辑计划快照编辑器。它从两种可互换格式导入算子树，按显式兼容矩阵裁决版本，支持带基础版本的增量结构编辑，并把快照、算子、四类派生结构与意图日志持久化到 SQLite。

## 实现范围

- 算子类型：`Scan`、`Filter`、`Project`、`Join`、`Aggregate`、`Sort`。
- 节点字段：全局唯一 `id`、类型、子节点顺序、列引用、谓词、可选统计绑定。
- 树校验：单根、无环、每个节点至多一个父节点、算子元数、列引用必须由上游产出；失败返回 `E_PLAN_INVALID`。
- 两种格式：紧凑二进制与 JSON Lines，均携带格式版本、快照序号与生成时间。
- 版本裁决：v1 降级接受并补齐缺失字段；v2 接受；v3/未知版本或未知必填特性拒绝。
- 快照切换：新版本完整写入 SQLite 后，才用一个原子指针替换只读内存状态。
- 增量编辑：增加/删除节点、改谓词、改列引用、改统计、调整子节点（含 Join 顺序）。
- 派生结构：树摘要、列引用索引、谓词依赖索引、代价估计；按受影响节点及祖先链做增量重建。
- 一致性：单写者串行提交，多读者无锁读取完整不可变版本；提交 ID 持久化幂等。
- 故障恢复：每次提交先持久化 pending 意图，重启扫描未完成意图、清理不完整版本并生成落盘事实报告。

## 环境与依赖

- Go `1.22+`。当前验证环境为 `go1.27.4 linux/amd64`。
- SQLite 驱动：`github.com/mattn/go-sqlite3 v1.14.28`，通过 cgo 使用本地 C SQLite 实现；项目的 `go.mod` 已固定版本。Windows 需准备可用的 cgo C 工具链。
- 驱动源码及许可证已放入 `vendor/`，因此 `GOPROXY=off go test ./...` 可完全离线构建。
- 仅使用 Go 标准库的 `encoding/binary`、`hash/crc32`、`encoding/json`、`database/sql` 及上述 SQLite 驱动。
- 样本全部位于 `samples/`，不需要网络或外部账号。
- 默认监听 `127.0.0.1:8080`，SQLite 默认位于 `data/editor.db`。
- 两份验证脚本需要 `bash`、`curl`、`python3`；编辑器与 Go 测试不依赖这些脚本。

```bash
go version
go test ./...
go vet ./...
go build -o bin/editor ./cmd/editor
./bin/editor -addr 127.0.0.1:8080 -db data/editor.db
```

重新生成固定样本：

```bash
go run ./cmd/samplegen samples
```

## 领域模型与列语义

`columns` 的解释取决于算子类型：

| 算子 | 子节点 | `columns` 含义 | 产出列 |
|---|---|---|---|
| `Scan` | 0 | 扫描产出列 | 自身 `columns` |
| `Filter` | 1 | 上游列引用 | 子节点全部产出列 |
| `Project` | 1 | 上游列引用，同时作为投影产出 | 自身 `columns` |
| `Join` | ≥2 | 上游列引用 | 子节点产出列按子节点顺序合并去重 |
| `Aggregate` | 1 | 上游列引用，同时作为聚合产出 | 自身 `columns` |
| `Sort` | 1 | 排序键引用 | 子节点全部产出列 |

每个非 `Scan` 节点的 `columns` 与 `predicate.columns` 都必须命中其所有上游（后代）产出列。节点 ID 在全树唯一。谓词表达式在本编辑器内按规范化字符串保存，列依赖由 `predicate.columns` 显式交换，不实现通用 SQL 语法解析器。

## 二进制格式 `LPSB`

文件采用所选端序编码所有多字节整数。头部固定 26 字节：

| 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 4 | 魔数 `LPSB` |
| 4 | 1 | 格式版本字节 |
| 5 | 1 | 端序：`0x01` 小端，`0x02` 大端 |
| 6 | 8 | 快照序号 `snapshot_seq` |
| 14 | 8 | 生成时间 Unix 秒 |
| 22 | 4 | 算子记录数 |

每条记录：

```text
u32 record_length            # payload 长度 + 4
payload[record_length - 4]
u32 crc32_ieee(payload)
```

`payload` 布局：

```text
u8   record_kind             # 1 = operator
u8   optional_field_bitmap
       bit0 = columns present
       bit1 = predicate present
       bit2 = stats present
       bits3..7 must be zero in v1/v2
u32  id_len; bytes id
u32  type_len; bytes type
u16  child_count; repeated {u32 len; bytes child_id}
if bit0: u32 count; repeated {u32 qualifier_len; bytes; u32 name_len; bytes}
if bit1: u32 expr_len; bytes expr; then column list as above
if bit2: u32 operator_id_len; bytes; i64 row_count;
         u32 distinct_count; repeated {u32 key_len; bytes key; i64 value}
```

v1 编码省略列引用与统计绑定；导入时分别补为空列表和 `row_count=0, distinct={}`，并记录降级明细。CRC 不匹配、截断、非法长度、未知端序或未知记录位返回 `E_FORMAT_CORRUPT`，上下文包含 1-based `record_seq` 与计算出的 `crc32`。

## JSON Lines 格式

第一条非空行是快照元数据封装，之后每行是算子或统计绑定：

```json
{"record":"snapshot","format":"jsonl","format_version":2,"snapshot_seq":200,"generated_at":"2026-11-20T08:00:00Z"}
{"record":"operator","id":"scan_orders","type":"Scan","parent":"","columns":[...],"predicate":null}
{"record":"stats","operator_id":"scan_orders","row_count":10000,"distinct":{"orders.id":10000}}
```

- `parent` 是父算子 ID；根为空。
- `children` 可显式给出；若省略，则按带相同 `parent` 的行顺序推导。显式 `children` 与 `parent` 必须互相一致。
- 统计绑定可内嵌在算子行，也可作为独立 `stats` 行。
- v1 允许省略 `columns` 与 `stats`，按上述默认值降级。
- JSON/行级解析错误返回 `E_FORMAT_CORRUPT`，上下文带物理行号和该行 CRC32。

## 兼容矩阵

矩阵在 `internal/codec/compat.go` 中显式声明，也可通过 `GET /v1/compatibility` 查询。

| 格式 | 版本 | 结论 | 行为 |
|---|---:|---|---|
| binary / jsonl | 1 | `degrade` | 补齐 `columns=[]`、默认统计绑定并记录明细 |
| binary / jsonl | 2 | `accept` | 当前格式 |
| binary / jsonl | 3 | `reject` | 含未知必填字段，返回 `E_FORMAT_UNSUPPORTED` |
| 其他 | 其他 | `reject` | 返回 `E_FORMAT_UNSUPPORTED` |

元数据声明任意未知 `required_features` 时，即使版本在矩阵内也返回 `E_FORMAT_UNSUPPORTED`。所有拒绝发生在快照切换之前，旧快照保持完整可用。

## 版本与增量片段

导入快照的 `snapshot_seq` 直接成为提交版本。成功增量提交使版本加一。导入序号必须严格大于当前版本，否则：

```json
{
  "code": "E_SNAPSHOT_STALE",
  "message": "snapshot sequence is not greater than the currently committed version",
  "context": {"imported_snapshot_seq": 100, "current_version": 204}
}
```

增量片段示例：

```json
{
  "commit_id": "edit-predicate-001",
  "base_version": 200,
  "operations": [
    {
      "op": "set_predicate",
      "node_id": "filter_high_value",
      "predicate": {"expr": "orders.amount > 500", "columns": [{"qualifier":"orders","name":"amount"}]}
    }
  ]
}
```

支持的操作：

- `add_node`：提供完整 `node`；可用 `parent_id` 与可选 `position` 直接挂接。
- `delete_node`：删除节点，并把其子节点列表原位提升给父节点；最终树元数仍由整树校验裁决。
- `set_predicate`、`set_columns`、`set_children`、`set_stats`。
- `set_children` 同时用于调整 Join 子节点顺序。

编辑器从所有变更节点出发计算新树祖先链与旧树祖先链，只重建这些节点的树摘要、索引贡献和成本；未受影响节点的摘要/成本对象直接复用。四类派生结构先在内存新状态中完整构造，再随 SQLite 版本事务一并提交，最后原子发布。

重建范围按操作依赖选择，并在提交响应的 `rebuilt_structures` 中返回：

| 操作 | 树摘要 | 列索引 | 谓词索引 | 成本估计 |
|---|---:|---:|---:|---:|
| `set_predicate` | 是 | 否 | 是 | 否 |
| `set_columns` | 是 | 是 | 否 | 否 |
| `set_stats` | 是 | 否 | 否 | 是 |
| `add_node` / `delete_node` / `set_children` | 是 | 是 | 是 | 是 |

多个操作合并时取并集。诸如“改谓词 + 改统计”的片段会同时重建谓词索引和成本估计。

- 基础版本不是当前版本：`E_STALE_BASE`，不写意图、不改状态。
- 同一基础版本的两个并发提交：全局单写者锁令第一个成功，第二个看到新版本后返回 `E_STALE_BASE`。
- 同一 `commit_id` 与相同内容：等待首个提交完成后复用持久化结果，响应 `replayed=true`。
- 同一 `commit_id` 与不同内容：`E_CONFLICTING_REQUEST`。

## 提交与恢复协议

每个写请求执行：

1. 完整解码、格式/版本裁决、树校验与基础版本检查；失败时尚未写意图。
2. 独立提交一条 `intents(status=pending)` 持久化意图。
3. 在内存构造新不可变 `State`（树、四类派生索引、降级明细）。
4. 单个 SQLite 事务写入版本行、全部算子、四类派生结构、降级明细、幂等结果、`current_version`，并把意图标为 `completed`。
5. 事务成功后执行故障检查点，随后 `atomic.Pointer.Store(newState)`；旧状态在所有已持有读者离开后自然释放。
6. 查询只读取当前指针，因此只能看到某个完整提交版本，不会看到“摘要已换、索引仍指向旧列”的中间态。

启动恢复只依据 SQLite 落盘事实：

- pending 意图的目标版本若已完整且是当前指针目标，则补记为 completed；
- 否则标为 aborted，保留审计事实；
- 清除不属于任何已登记版本的操作/派生/降级行；
- 清除不完整的版本行，并把当前指针移到最新的完整版本；
- 报告写入 `recovery_reports`，同时通过 `GET /v1/recovery` 返回。

SQLite 使用 WAL 与 `synchronous=FULL`。持久化表包括 `snapshot_versions`、`operators`、`derived_indexes`、`degradations`、`intents`、`commits`、`meta`、`recovery_events` 与 `recovery_reports`。

强制终止检查点：

| 阶段 | 含义 |
|---|---|
| `import_after_intent` | 导入意图已落盘，尚未重建/提交 |
| `import_during_rebuild` | 已进入重建前检查点 |
| `import_after_commit` | SQLite 已提交，内存指针尚未切换 |
| `patch_after_intent` | 增量意图已落盘，尚未重建/提交 |
| `patch_during_rebuild` | 已进入重建前检查点 |
| `patch_after_commit` | SQLite 已提交，内存指针尚未切换 |

通过环境变量或 HTTP 配置 `crash`（立即 `os.Exit(137)`）或 `pause`（挂起供外部 SIGKILL）。生产路径不使用 panic 或延迟清理来“修复”未提交事务。

## HTTP API

所有失败响应均为：

```json
{"code":"E_...","message":"...","context":{...}}
```

| 方法/路径 | 作用 |
|---|---|
| `GET /healthz` | 进程与当前版本 |
| `GET /v1/compatibility` | 显式兼容矩阵 |
| `POST /v1/snapshots/import` | 原始二进制/JSONL body，或 JSON `data_base64` 封装 |
| `GET /v1/snapshot` | 当前快照摘要、树摘要、降级计数 |
| `GET /v1/tree` | 当前算子、上游列引用与产出列 |
| `GET /v1/indexes/columns` | 列引用/产出索引 |
| `GET /v1/indexes/predicates` | 谓词依赖索引 |
| `GET /v1/estimates` | 逐节点与根成本估计 |
| `GET /v1/degradations` | 当前版本降级明细 |
| `GET /v1/state` | 树、派生结构、降级明细的版本化快照 |
| `GET /v1/recovery` | 最近一次启动恢复报告 |
| `POST /v1/patches` | 提交增量片段 |
| `POST/GET/DELETE /v1/admin/crash-point` | 设置/查询/移除故障检查点 |

导入示例：

```bash
curl -sS -X POST \
  -H 'Content-Type: text/plain' \
  -H 'X-Request-ID: import-200' \
  --data-binary @samples/plan_current.jsonl \
  http://127.0.0.1:8080/v1/snapshots/import
```

导入的幂等标识可来自 `X-Request-ID`、`X-Commit-ID`、查询参数 `request_id`/`commit_id`，或 JSON 封装中的同名 `request_id`/`commit_id` 字段。

JSON 封装支持二进制：

```json
{"request_id":"import-binary","encoding":"base64","data_base64":"..."}
```

## 业务码

| 码 | HTTP | 场景 |
|---|---:|---|
| `E_PLAN_INVALID` | 422 | 重复 ID、环、多根、元数错误、列引用无上游产出 |
| `E_FORMAT_CORRUPT` | 400 | 魔数/端序/长度/CRC/JSONL 行解析失败 |
| `E_FORMAT_UNSUPPORTED` | 422 | 高版本或未知必填特性 |
| `E_SNAPSHOT_STALE` | 409 | 导入快照序号不大于当前版本 |
| `E_STALE_BASE` | 409 | 增量基础版本非当前版本 |
| `E_CONFLICTING_REQUEST` | 409 | 同一提交 ID 内容不一致 |
| `E_SNAPSHOT_NOT_FOUND` | 404 | 尚未加载快照 |
| `E_BAD_REQUEST` | 400 | 请求体、方法、管理参数错误 |

## 固定样本

| 文件 | 说明 |
|---|---|
| `plan_v1.bin` / `plan_v1.jsonl` | v1，缺列引用与统计；各产生 8 条降级明细 |
| `plan_v1_degradations.expected.json` | v1 二进制降级明细预期 |
| `plan_current.jsonl` | v2 当前计划，序号 200 |
| `plan_current.bin` | 同一计划二进制版，序号 201 |
| `plan_next.jsonl` / `plan_next.bin` | 后续替换快照，序号 300/301 |
| `plan_v3_required.jsonl` / `.bin` | 高版本未知必填字段，必须拒绝 |
| `patch_001_set_predicate.json` | 改谓词与统计，基础 200 → 201 |
| `patch_002_reorder_join.json` | 调整 Join 顺序，201 → 202 |
| `patch_003_add_filter.json` | 增 Filter，202 → 203 |
| `patch_004_delete_filter.json` | 删 Filter 并提升子节点，203 → 204 |
| `patch_stale_base.json` | 基础 200，已过期时返回 `E_STALE_BASE` |
| `patch_conflicting_commit.json` | 复用 `edit-predicate-001` 但内容不同 |

## 实际验证结果

以下命令均已在当前工作区实际执行：

```text
$ go test ./...
ok  example.com/logicplan/editor/internal/api
ok  example.com/logicplan/editor/internal/codec
ok  example.com/logicplan/editor/internal/engine
ok  example.com/logicplan/editor/internal/model
ok  example.com/logicplan/editor/internal/planstate

$ go test -race ./internal/engine ./internal/api ./internal/planstate
ok  example.com/logicplan/editor/internal/engine
ok  example.com/logicplan/editor/internal/api
ok  example.com/logicplan/editor/internal/planstate

$ go vet ./...
# no findings

$ GOPROXY=off go test ./...
# 使用 vendor/ 中的驱动源码，全部通过，不访问网络

$ go run ./cmd/samplegen samples && sha256sum samples/*   # 重生成前后哈希一致
sample determinism: PASS

$ ./scripts/smoke.sh
SMOKE PASS: formats/patches/indexes/estimates/recovery at version 204

$ ./scripts/crash_recovery.sh
PASS base import version=200
PASS process terminated at patch_after_intent (exit 137)
PASS restart restored complete version 200 and aborted the patch intent
PASS patch can be retried after recovery at version=201
PASS process terminated at import_after_intent (exit 137)
PASS restart restored complete version 201 and aborted the import intent
PASS import can be retried after recovery at version=300
ALL CRASH-RECOVERY CHECKS PASSED
```

测试覆盖：两种格式与大/小端、CRC 错误上下文、v1 降级、v3 拒绝、快照替换与序号回退、无效/损坏批次不触碰旧快照、四个增量编辑、派生索引级联一致性、同基础版本并发、幂等重放/冲突、重启持久化、意图中强杀恢复、提交后未发布内存状态的恢复，以及真实独立进程 `exit(137)` 演练。

## 目录

```text
cmd/editor/           HTTP 进程入口
cmd/samplegen/        固定样本生成器
internal/api/         HTTP 路由、JSON 错误
internal/codec/       二进制/JSONL、兼容矩阵
internal/model/       领域模型与整树校验
internal/planstate/   不可变状态、增量编辑、四类派生结构
internal/store/       SQLite schema、事务、意图与恢复
internal/engine/      导入/增量提交协议、幂等与原子发布
internal/fault/       强杀/暂停检查点
samples/              固定快照、增量与预期明细
scripts/              烟测与真实进程恢复演练
```
