# 多轮对话意图槽位迁移与跨版本语义角色对齐工作台

这是一个纯本地、确定性的 Go 工作台。它从固定种子生成的 JSONL 会话出发，把新版和旧版两代语义角色槽位统一到新版角色体系，逐轮生成意图/槽位状态轨迹，并对歧义映射、越界值、角色漂移和未回应澄清给出可追溯报告。

实现不依赖真实模型、GPU、外部服务、线上站点或预置数据集。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`。

## 环境与 Windows 构建

要求 Go 1.22 或以上。Windows PowerShell 示例：

```powershell
go version
go build -o dialogbench.exe .\cmd\dialogbench
go test .\...

.\dialogbench.exe generate
.\dialogbench.exe run
.\dialogbench.exe demo
```

依赖已锁定在 `go.mod` / `go.sum`：`modernc.org/sqlite v1.29.10`、`golang.org/x/sys v0.19.0`。Windows 文件锁使用 `LockFileEx`；Linux/macOS 使用 `flock`。本次交付环境为 Linux，已额外执行：

```text
GOOS=windows GOARCH=amd64 go build -o dialogbench-windows.exe ./cmd/dialogbench
GOOS=windows GOARCH=amd64 go test ./... -run '^$'
```

两项 Windows 交叉构建/测试编译均通过。

## 快速演示

```powershell
.\dialogbench.exe generate
.\dialogbench.exe demo
```

`demo` 会重置演示检查点，先用 `v1` 角色映射完整运行，再把活动映射切换到 `v2` 增量重跑，并把确定性产物写到 `artifacts/demo/`：

- `v1-state.json` / `v1-alignments.json`：升级前状态和校验。
- `final-state.json` / `final-alignments.json`：升级后统一状态与三类清单。
- `fingerprints.json`：每轮四阶段输入/输出指纹及会话汇总指纹。
- `invalidations.json`：最早失效边界、原因和重建前后指纹。
- `summary.json`：运行、漂移、歧义、越界与失效计数。

固定样本种子为 `20261125`，包含 4 个会话、16 轮：

- `S01`：中英混排、新/旧槽位混合、中文日期归一化、优先级值越界。
- `S02`：旧码 `501` 同时对应两个同优先级角色，保留两项候选；旧码 `502` 通过取值范围消歧；旧码 `503` 通过优先级消歧。
- `S03`：旧码 `301` 在 `v1` 中是 `delivery.date`，在 `v2` 中变为 `subscription.start_date`，用于角色漂移。
- `S04`：意图回流成环，且两个待澄清槽位在会话结束前未回应。

## 工程结构

```text
cmd/dialogbench/       CLI 入口
internal/domain/       统一模型、规范化 JSON、稳定业务错误码
internal/generator/    固定种子会话、角色表、v1/v2 映射和规则生成器
internal/upgrade/      旧码解析、范围兼容性、优先级裁决和值归一化
internal/codec/        新版与旧版结构双向编解码
internal/engine/       四阶段执行、状态演进、对齐、指纹、失效与恢复
internal/store/        SQLite 检查点、事务发布、文件锁、完整性校验
internal/httpapi/      本地 HTTP API
data/                  确定性样本（可由 generate 重建）
artifacts/demo/        已生成的实际演示与校验摘要
```

## 统一状态模型

内部 `TurnState` 保存：

- `intent`：当前意图。
- `confirmed_slots`：累计已确认槽位，含新版角色名、规范值、取值范围、来源、来源轮次和字符偏移。
- `pending_slots`：待澄清槽位，含候选角色、请求轮次和解决轮次；已解决项不会删除，历史仍可追踪。
- `changes`：本轮意图转移、槽位确认、冲突、待澄清新增/解决等变更集。
- `triggered_rules`、`map_version`、`intent_history`、`diagnostics`。

相同角色被不同值重复确认时，不覆盖旧值；记录 `SLOT_CONFLICT` 和冲突前后的值。意图回到曾经访问过的状态时记录 `INTENT_TRANSITION_LOOP`，并保留访问轮次。会话最后一轮对仍活动的待澄清项记录 `PENDING_UNANSWERED`。

## 两代编解码

`internal/codec` 提供会话级双向编解码：

- 新版结构：`role + value_range + value`，保留来源轮次和范围边界。
- 旧版结构：`integer role_code + free text value + declared map version + legacy value_range`。
- 旧版编码不会“先到先得”选择歧义码。它只选择解码后能唯一回到同一角色的旧码；没有这样的码时直接报错。
- 同一新版角色对应多个旧码时，优先复用槽位原本的旧码；否则选择确定性的最小可用旧码。

测试 `TestNewCodecPreservesIntentSlotsRangesAndTurnOwnership` 和 `TestLegacyCodecUsesUnambiguousCodes` 会比较意图、角色、规范值、范围和来源轮次。

## 旧码升级与歧义裁决

`internal/upgrade` 的裁决顺序：

1. 校验旧槽位声明的映射版本。活动映射必须同版本，或显式列入 `compatible_declarations`；否则返回 `E_MAP_VERSION_CONFLICT`。
2. 确认旧码存在于声明映射和活动映射中；不存在返回 `E_UNKNOWN_ROLE_CODE`。
3. 按声明范围类型归一化自由文本：整数、枚举、日期区间；失败返回 `E_VALUE_NORMALIZE`。
4. 保留旧码的全部活动候选，逐项检查旧范围包含关系和规范化值的新范围兼容性。
5. 在兼容候选中按优先级降序裁决；最高优先级仍有多项时保留全部候选并标记歧义，不做静默选择。
6. 没有兼容候选时返回 `E_ROLE_RANGE_INCOMPATIBLE`，附会话、轮次、字段和候选角色。

`v2` 映射通过 `compatible_declarations: ["v1", "v2"]` 明确支持旧声明的升级；未列入的版本会触发版本冲突，而不是猜测。

## 四阶段执行与增量失效

每轮依次执行 `parse -> upgrade -> evolve -> align`，每个阶段完成后写一条检查点。轮次是优雅停止的不可拆分单元；停止检查发生在轮次起始和 `align` 完成后。

阶段输入指纹分别为：

- `parse`：原始轮次内容。
- `upgrade`：解析产物摘要、活动映射内容、角色表、归一化配置。
- `evolve`：升级产物摘要、规则、角色表、上一轮状态摘要、是否末轮。
- `align`：状态摘要、对齐配置与阈值、角色表、映射版本。

执行器在读取检查点时先校验摘要；输入指纹一致则复用。首次不一致时调用 `InvalidateFrom`，删除该边界及其全部下游当前检查点，但保留历史表，然后原子发布新结果。失效日志保存会话、轮次、阶段、原因、旧/新输入摘要和旧/新输出摘要。

验证覆盖了话语变化、槽位标注变化、映射表变化、规则变化、归一化精度变化和对齐阈值变化。

## 检查点、并发与错误分类

SQLite 位于 `<workspace>/.dialogbench/state.db`，使用 WAL、`synchronous=FULL` 和事务。跨进程发布另加真实操作系统文件锁 `<workspace>/.dialogbench/publish.lock`。

发布事务会：

- 比较调用方读取到的全局 `next_seq`，条件更新单调序号。
- 要求同一会话从第 1 轮 `parse` 开始，阶段和轮次连续。
- 拒绝重复、落后、跳阶段、跳轮次和跨会话错位发布。
- 校验当前记录摘要、历史最大序号与元数据计数，区分回退和摘要篡改。
- 仅在正常推进或显式失效后的重建中接受新记录；历史记录不删除。

稳定业务错误码：

| 错误码 | 含义 |
| --- | --- |
| `E_STALE_CHECKPOINT` | 重复、落后、非连续或错位发布 |
| `E_CHECKPOINT_ROLLBACK` | 当前序号低于已提交历史 |
| `E_CHECKPOINT_TAMPERED` | 记录载荷、记录摘要或计数被替换 |
| `E_STAGE_TRANSITION` | 非法阶段跳转 |
| `E_MISSING_PREREQUISITE` | 缺输入文件、前置阶段产物或不能完整导出 |
| `E_MAP_VERSION_CONFLICT` | 映射版本不兼容/缺失 |
| `E_UNKNOWN_ROLE_CODE` | 旧角色码不存在 |
| `E_UNKNOWN_ROLE` | 新版角色不存在 |
| `E_VALUE_NORMALIZE` | 自由文本无法归一化 |
| `E_ROLE_RANGE_INCOMPATIBLE` | 无范围兼容目标 |
| `E_INVALID_REQUEST` | 请求结构或会话轮次非法 |

所有带上下文的错误均包含 `session_id`、`turn_index`、`stage`、`field`（适用时）与 `detail`。

## 对齐校验

`align` 每轮输出三个确定性排序的清单：

- `out_of_range`：值不满足新版角色范围，附角色、字段、值、范围、原因和原话语字符偏移。
- `role_drifts`：同一旧码因映射升级从旧角色变为新角色，附前后角色、旧码、活动映射、声明映射、历史状态摘要和字符偏移。
- `ambiguous`：旧码无法唯一裁决时保留全部候选、优先级、兼容性和字符偏移。

漂移首次确认轮次报告一次，不按后续累计状态重复堆积。对齐阈值 `max_findings_per_category` 可控制每类最大条数。

## CLI

```text
dialogbench generate
dialogbench run [--clear-stop] [--reset]
dialogbench resume
dialogbench demo
dialogbench status
dialogbench export [--out file]
dialogbench align [--session S01]
dialogbench fingerprints [--session S01]
dialogbench invalidations [--session S01]
dialogbench stop [--reason text]
dialogbench import --file sessions.jsonl
dialogbench map-set --file role_map_v3.json [--activate]
dialogbench map-activate --version v2
dialogbench verify
dialogbench serve [--addr 127.0.0.1:8080]
```

## HTTP

默认只监听 `127.0.0.1:8080`：

```text
GET  /healthz
POST /v1/generate
POST /v1/import                  (body: JSONL)
POST /v1/run                     (body: {"clear_stop":false,"reset":false})
POST /v1/resume
POST /v1/stop                    (body: {"reason":"..."})
GET  /v1/sessions
GET  /v1/sessions/{id}/turns
GET  /v1/sessions/{id}/fingerprints
GET  /v1/alignments?session=S01
GET  /v1/invalidations?session=S01
GET  /v1/export
GET  /v1/active-map
POST /v1/active-map              (body: {"version":"v2"})
PUT  /v1/role-maps/{version}     (body: RoleMap；?activate=true 可选)
POST /v1/verify
```

`/v1/run` 为同步执行；另一个并发请求可以调用 `/v1/stop`。停止请求在 SQLite 中持久化，当前轮完成后退出。`/v1/resume` 清除停止标记并从已提交检查点继续。

## 本次实测结果

执行环境：Linux amd64，Go `1.27.4`；模块声明目标为 Go `1.22.0`。以下命令均通过：

```text
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
GOOS=windows GOARCH=amd64 go build -o dialogbench-windows.exe ./cmd/dialogbench
GOOS=windows GOARCH=amd64 go test ./... -run '^$' -count=1
```

测试覆盖：固定种子确定性、旧码歧义/优先级/范围裁决、版本冲突、归一化错误上下文、两代编解码往返、越界与字符偏移、映射升级漂移、意图环、未回应待澄清、槽位冲突、增量失效边界、并发发布、重复/错位发布、篡改/回退分类、停止恢复字节一致、HTTP 生成/运行/导出/错误码。

`demo` 实际摘要：

```text
sessions=4 turns=16
out_of_range=1
role_drifts=2
ambiguous=1
diagnostics=11
v1_export_digest=a7fd2e9e1cd7005fc87d7bcd7c801e03879f8441fc17657ba14c455e16b6a5b0
v2_export_digest=c77d0c59fce13b355cb70ec4829a51b6fc8d9bd18780fc445e0db813178ae08f
```

连续执行两次 `dialogbench demo` 后，`artifacts/demo/` 下全部 7 个文件的 SHA-256 对比无差异。恢复测试使用 16 轮完整运行与“第 5 轮后停止再恢复”的导出逐字节比较，结果一致。

## 确定性说明

- 不写入任何墙上时钟、随机 UUID 或进程相关信息到业务状态。
- 会话、轮次、规则、候选和清单均显式排序。
- JSON 使用关闭 HTML 转义的规范编码；指纹为 SHA-256。
- SQLite 序号会因失效事件递增，但导出的状态、对齐和指纹只由输入与配置决定。
- 强制终止后不生成退出报告；恢复只读取 SQLite 已提交事务与历史表。未提交事务由 SQLite 原子回滚。
