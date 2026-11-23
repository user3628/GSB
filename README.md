# configimpact

面向本地多组件平台的配置变更影响面引擎。它以版本化 JSON 文档描述组件、分组默认值、依赖关系、监听键和键值，回答两个问题：

1. 一个或多个配置键变化后，哪些组件必须重新物化，传播来源和跳数是什么？
2. 一个携带观察版本的读请求能否拿到该版本完整有效的配置，还是必须返回 `E_STALE_READ`？

实现使用 Go 1.22+ 和纯 Go SQLite 驱动 `modernc.org/sqlite v1.34.5`，不依赖 Docker、外部服务或 CGO。

## 快速开始

```bash
go mod download
go test ./...
go test -race ./...
go run ./cmd/configimpact demo -fresh
```

Windows PowerShell：

```powershell
go version                    # 需要 Go 1.22+
go env GOOS GOARCH CGO_ENABLED
go mod download
go test ./...
$env:GOOS = "windows"
$env:CGO_ENABLED = "0"
go build ./...
```

在 Linux 上交叉构建 Windows 版本：

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

如果默认 Go 代理不可达，可显式使用可达镜像：

```bash
GOPROXY=https://goproxy.cn,direct go mod download
```

## 文档模型

`testdata/sample.json` 是固定回归样本，包含分组默认值、三层依赖、同键冲突、不同监听键集合和未受影响组件。

```json
{
  "version": 1,
  "groups": [
    {
      "id": "base",
      "defaults": [
        {"key": "log.level", "type": "string", "value": "info", "order": 10}
      ]
    }
  ],
  "components": [
    {
      "id": "db",
      "group": "base",
      "depends_on": [],
      "watches": ["db.dsn", "log.level"],
      "config": [
        {"key": "db.dsn", "type": "string", "value": "sqlite://primary", "order": 10}
      ]
    }
  ]
}
```

支持类型：`string`、`int`、`float`、`bool`、`json`，并接受 `integer`、`number`、`boolean` 别名。`null` 不是已定义值；未提供的键通过省略表达。

加载校验包括：

- 文档版本、组件标识和分组标识合法且唯一。
- 依赖目标存在、无自依赖、无重复依赖、无依赖环。
- 键路径符合点分段格式，例如 `db.pool_size`。
- 同一分组或组件内不重复声明同一键。
- 值与声明类型一致。
- JSON 解码拒绝未知字段和尾部垃圾内容。

失败统一返回 `E_DOC_INVALID`。校验在写事务之前完成，因此旧文档、旧物化和旧引用均保持可读。

## 有效配置裁决

每个组件的候选来源有三类：

- 本地分组默认值。
- 本地组件显式声明。
- 每个直接依赖的完整有效配置，递归包含其分组和上游依赖。

同一键如有多个候选，按以下顺序裁决：

1. `order` 更大者优先。这是主规则，允许分组默认值通过更高生效序号覆盖较低序号的显式值。
2. `order` 相同：本地显式声明优先于上游显式声明，近层依赖优先于远层依赖，显式声明优先于继承默认值。
3. 仍相同：按 owner、来源种类和来源路径稳定排序。

每个 `EffectiveValue` 输出：

- 胜出值、类型和 order。
- `winner`：胜出来源。
- `sources`：按优先级排序的全部候选来源链。
- `resolution`：胜出原因。

`via` 示例：API 的值经由 `api -> auth` 来自 auth 的显式声明，则来源路径为 `["api", "auth"]`。两条路径都未提供该键时，值不会出现在 `values` 中，也不会以零值填充；被监听但未解析的键列在 `undefined_keys`。

## 补丁与影响面

补丁只包含变更键和基础版本：

```json
{
  "id": "patch-001",
  "base_version": 1,
  "changes": [
    {
      "key": "db.pool_size",
      "scope": "component",
      "component": "db",
      "type": "int",
      "value": 16,
      "order": 20
    }
  ]
}
```

`scope` 可为 `component` 或 `group`。省略时仅在该键只有一个已有声明的情况下自动推断；存在歧义则返回 `E_PATCH_INVALID`。

影响面的直接种子包括：

- `watches` 包含任一变更键的组件。
- 键值被修改或新增的组件。
- 分组默认值变化的组内全部组件。

随后沿依赖反向边 BFS，所有下游组件都进入必须重新物化集合。输出包含：

- `affected_components`：组件、触发键、原因种子、最短依赖跳数、自身下游集合。
- `downstream`：每个种子组件可达的全部下游组件。
- 每个组件的物化路径、阈值、比例、耗时估计与实际耗时。

`base_version` 不等于当前版本时返回 `E_STALE_BASE`，不会写意图或改变任何状态。

## 物化路径

对受影响组件，`touched_keys / watch_keys` 与显式阈值比较：

- 比例严格小于阈值：`incremental`，只重算触发键，其余键复用旧物化结果和来源链。
- 比例大于等于阈值：`full`，整组件重新解析。
- 本次补丁未涉及的组件：`reuse`，复制上一版本快照但保留真实 `materialized_version`。
- 首次物化：强制 `full`。

同一文档版本下，两条路径必须产生逐键相同的 `values`、`sources` 和 `undefined_keys`；测试对此做深度比较。决策输出同时包含 `threshold`、`ratio`、`estimated_duration_ns` 和 `actual_duration_ns`。

## 读可见性与陈旧读

读请求必须绑定观察版本：

```go
result, err := engine.Read(ctx, configimpact.ReadRequest{
    ComponentID:     "worker",
    ObservedVersion: 2,
})
```

- 历史完整快照永久保留，旧观察版本仍可读取。
- 返回结果标注 `observed_version`、`snapshot_version` 和真实的 `materialized_version`。
- 观察版本高于最后完整版本、对应快照或组件物化缺失、或物化版本对观察版本不可见时，返回 `E_STALE_READ`。
- 错误详情包含 `observed_version`、`committed_version`、`lagging_version` 和可用的 `latest_materialized_version`。
- 提交进行期间，按旧版本观察的读仍从旧完整快照完成；按尚未提交的新版本观察的读返回 `E_STALE_READ`。

## 原子性与恢复

SQLite 保存文档版本、物化配置、影响记录、引用关系和意图日志。提交协议如下：

1. 校验补丁和基础版本，不写状态。
2. 单独事务写入 `patch_log(in_progress)` 与 `intents(pending)` 并提交。
3. 计算影响面、物化和引用。
4. 在单一事务内写入文档、全部物化、全部影响、全部引用，推进 `current_version`，完成补丁日志和意图。
5. 任一步失败时，在释放最终事务锁后把意图和补丁标记为 `aborted`。

进程在步骤 3 或提交前被强杀时，步骤 2 的意图已持久化，步骤 4 的 SQLite 事务不会部分提交。重启扫描未完成意图，将其标记为 `aborted`；同时验证每个文档行是否具备完整的组件物化和影响记录，把当前版本恢复为已持久化的最大完整文档版本。恢复报告只报告数据库中存在的事实：扫描数量、被中止意图、完整版本列表、被丢弃的不完整版本、最后完整版本和孤立快照行数。

已中止的补丁可再次提交相同 ID：只有成功提交才算“生效一次”。已完成补丁重复提交返回持久化的原结果；同 ID 不同内容返回 `E_CONFLICTING_PATCH`。

## Go API

主要入口：

```go
engine, err := configimpact.Open(ctx, "platform.db", configimpact.Options{
    IncrementalThreshold: 0.5,
})
defer engine.Close()

load, err := engine.LoadJSON(ctx, documentBytes)
patchResult, err := engine.SubmitPatchJSON(ctx, patchBytes)
read, err := engine.Read(ctx, configimpact.ReadRequest{ComponentID: "api", ObservedVersion: 2})
impact, err := engine.ImpactAt(ctx, 2, "api")
refs, err := engine.ReferencesAt(ctx, 2, "api")
doc, err := engine.DocumentAt(ctx, 2)
report := engine.RecoveryReport()
```

业务错误使用 `*configimpact.BusinessError`，可用 `configimpact.CodeOf(err)` 或 `configimpact.IsCode(err, code)` 判断。

## CLI

```bash
go run ./cmd/configimpact load    -db artifacts/demo.db -doc testdata/sample.json
go run ./cmd/configimpact patch   -db artifacts/demo.db -patch testdata/patch.json
go run ./cmd/configimpact read    -db artifacts/demo.db -component worker -observe 2
go run ./cmd/configimpact impact  -db artifacts/demo.db -version 2 -component worker
go run ./cmd/configimpact refs    -db artifacts/demo.db -version 2 -component worker
go run ./cmd/configimpact recover -db artifacts/demo.db
go run ./cmd/configimpact demo -fresh
```

所有成功结果输出 JSON 到 stdout；业务错误输出 JSON 到 stderr 并以非零状态退出。

## 验证

已执行并通过：

- `go test ./...`
- `go test -race ./...`
- 真实子进程强杀恢复测试：`TestRecoveryAfterForcedTermination`
- Windows amd64 交叉构建：`GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/configimpact`
- 固定样本端到端 demo：版本 1 补丁到版本 2，全部 6 个组件受影响，历史读保留旧值，未来版本读返回 `E_STALE_READ`。

详细设计见 `docs/DESIGN.md`，实测记录见 `docs/VERIFICATION.md`。

## 约束

- 一个数据库同时只应由一个 Engine 写者进程提交；同进程写者已串行化。多进程并发写不在当前范围。
- 补丁是修改/新增语义，不包含删除键。
- 所有历史版本保留，暂未实现快照压缩。
- Windows 构建使用纯 Go SQLite，不需要安装 C 编译器或系统 SQLite。
