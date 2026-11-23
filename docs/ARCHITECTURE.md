# 架构说明

## 目标与约束

系统面向多组件平台的故障恢复协调，核心不变量是：

1. 任一已提交版本都包含完整一致的组件、依赖图、派生影响面、恢复批次和健康一致度。
2. 增量只允许基于当前版本提交；并发写者中只有一个成功，其他得到 `E_STALE_BASE`。
3. 派生结构变化必须在同一 SQLite 事务边界内原子发布，读者不可见中间状态。
4. 意图先于业务写入落盘；强制终止后只保留最后完整版本。
5. 恢复演练使用真实回环 HTTP，超时必须取消上下文、关闭连接并释放预留。
6. 同一快照版本的定点计算逐位确定。

实现只依赖本地回环网络与 SQLite，不使用容器或外部服务。

## 分层

| 层 | 路径 | 职责 |
|---|---|---|
| 领域模型 | `internal/model` | 组件、资源、探测、快照、增量、演练、业务错误 |
| 规范与校验 | `internal/spec` | 严格 JSON 解码、规范化、重复 ID、摘要、增量应用 |
| 图推导 | `internal/graph` | Tarjan SCC、环污染、拓扑批次、影响面、部分推导 |
| 定点计算 | `internal/fixed` | 微单位一致度、half-even、补偿求和、容差比较 |
| 持久化 | `internal/store` | SQLite schema、单事务版本提交、意图、引用、恢复报告 |
| 协调引擎 | `internal/engine` | 单写者、幂等、级联重建、真实演练、预留 |
| HTTP/UI | `internal/httpapi`、`internal/webui` | JSON 契约、固定样本、嵌入式控制台 |
| 演示/端点 | `cmd/demo`、`cmd/testendpoint` | 可重复验收与真实探测目标 |

## 规范化与增量

`spec.DecodeSnapshot` 使用 `DisallowUnknownFields` 和尾随值检查。`NormalizeSnapshot`：

- 校验版本、时间、权重、探测 URL/超时、观测和一致度配置；
- 按输入顺序保留重复组件首条，记录后续重复项；
- 将边转换为组件的 `depends_on`，校验两端引用存在；
- 对资源和聚合资源做 `int64` 非负/溢出检查；
- 排序组件、边和依赖，生成 SHA-256 校验摘要。

`spec.ApplyIncrement` 在基础快照的克隆上按顺序执行：

- `patch_resource`：资源或容量局部替换；
- `patch_probe`：健康探测或恢复请求局部替换；
- `patch_observation`：观测状态局部替换；
- `add_edge` / `remove_edge`：依赖边增删。

每个操作产生 `Ref{kind,key,field}`，作为级联失效的变更集合。成功后重新规范化候选快照，因而增量结果与整快照提交共享同一套不变量。

## 图与恢复排序

`graph.Analyze` 的步骤：

1. 从规范化边建立上游到下游的邻接表。
2. 用 Tarjan 算法识别 SCC；尺寸大于 1 或含自环的 SCC 视为依赖环。
3. 环内组件标记 `blocked=cycle`；从环出发可到达的非环组件标记 `upstream_blocked`，并记录经过路径。
4. 对未被环污染的节点执行拓扑分层，得到 `topology_batches`；分层保证依赖者批次严格大于被依赖者。
5. 对每个可达下游计算最短传播跳数 `hop`，只保留受污染图中可排程的拓扑层。
6. 将候选图摘要写入 `graph_digest`。

依赖环提交被拒绝，但分析结果仍包含未污染组件批次、受阻组件、环 ID、诊断和逐组件影响面，通过 `partial_result` 返回。已提交版本不受影响。

## 引用闭包与级联失效

`buildDerivedRefs` 为以下派生视图建立显式引用：

- `resource_summary/{component}`：组件资源、容量和权重；
- `batch/{component}`：组件和所有依赖边；
- `diagnostics/graph`：组件依赖与边；
- `impact/{origin}/{affected}`：起点、受影响组件和影响传播所依赖的边；
- `consistency/{component}` 与 `consistency/aggregate`：观测、资源、容量、探测、恢复请求、权重和依赖。

增量提交时：

1. 使用旧引用的完整闭包和本次 `changed_refs` 计算 `invalidated`。
2. 对新候选快照执行完整派生计算，得到 `rebuilt` 集合。
3. 在同一个 SQLite 事务中写入新版本的全部派生行。
4. 只有事务提交成功后才替换内存中的不可变 `StateView`。

因此不存在“影响面已更新但批次仍是旧值”的可见窗口。策略在回执中显式标记为 `full_derived_rebuild_in_single_transaction`。

## 版本与幂等

`Engine.writeMu` 使提交、重建、演练推进串行。`Store.CommitVersion` 在事务内再次读取 `meta.current_version`，并与 `ExpectedCurrent` 比较，防止绕过单写者或重启竞态。

`submissions` 表以幂等键为主键，保存请求类型、请求内容摘要、状态和已落盘响应：

- 首次请求插入 `pending`；
- 成功后事务更新为 `committed`，保存 HTTP 状态与完整响应；
- 拒绝后保存为 `rejected`，其中包括业务码和上下文；
- 相同键 + 相同内容且已成功：直接反序列化旧响应并追加 `"replayed": true`；
- 相同键 + 不同内容：`E_CONFLICTING_REQUEST`。

所有写请求在 `intents` 中先插入 `pending` 记录，再开始业务校验与原子事务。失败的候选会同时结束意图和提交回执。

## SQLite 持久化

主要表：

- `meta`：当前已提交版本；
- `snapshots`：规范化快照、重复组件、校验/派生摘要；
- `components`、`edges`：版本化规范数据；
- `derived_components`、`derived_impacts`、`derived_batches`、`derived_consistency`：派生视图；
- `derived_refs`：派生结构与源字段/边的引用关系；
- `derived_rebuilds`：变更触发、失效集合、重建集合和策略；
- `submissions`：幂等键与持久回执；
- `intents`：写意图状态机；
- `drills`、`reservation_audit`：演练与预留审计；
- `recovery_reports`：启动恢复事实。

数据库使用 WAL、`synchronous=FULL`、外键和 busy timeout。`CommitVersion` 是一个事务：写入快照、组件、边、派生、引用、重建记录、幂等回执、意图完成状态和当前版本指针，最后统一提交。

## 强杀恢复

启动时 `RecoverInterrupted` 扫描：

- `intents.state='pending'`；
- `submissions.state='pending'`；
- `drills.state='running'`。

在一个事务中把它们标记为中止，并写入恢复报告。报告事实只包含 SQLite 中已提交版本、被中止意图及恢复规则。由于快照与 `current_version` 同事务，事务外的强杀只会得到“全部提交”或“全部未提交”两种状态。

恢复后重新打开数据库时，`engine.New` 从当前快照重建图、一致度和引用，并校验持久化的 `derived_digest`。不一致则拒绝启动，避免在损坏派生状态上继续服务。

## 恢复演练

`ExecuteDrill` 按已提交影响面的拓扑批次推进；每批使用 `max_concurrency` 信号量。每个组件：

1. 获取资源预留；
2. 对配置的健康 URL 发起真实 HTTP 探测；
3. 对恢复 URL 发起真实恢复请求；
4. 根据结果分类 `OK`、连接拒绝、业务拒绝、超时或网络错误；
5. `defer` 释放预留并写入审计。

`probe.Client` 为每次操作创建独立 `http.Transport`，禁用 keep-alive 和代理，用 `context.WithTimeout` 限定连接、响应头和响应体读取；超时后取消上下文并关闭空闲连接。演练结果、逐组件审计、幂等回执和意图完成状态在 `CompleteDrill` 中同事务发布，因此查询不会观察到“恢复成功但预留尚未释放”的状态。

## 定点一致度

所有值使用 `int64` 微单位。组件本地分数先保持精确分子，再按 half-even 舍入到微单位；聚合使用补偿求和（Neumaier 形式）减少累加误差。输出包括：

- 精确分子、分母、补偿量与补偿后总和；
- 每组件本地舍入余数与模式；
- 累计舍入误差和 ppb；
- 阈值、容差、决策、稳定 tie-break 列表；
- 对完整结果序列化后的 SHA-256 摘要。

阈值比较和容差计算全部是整数运算。

## 控制台

嵌入的静态控制台提供四个视图：

- 总览：版本、依赖图、影响面、拓扑批次、一致度定点轨迹；
- 变更实验室：资源/探测/边/环/陈旧基线模板、幂等键、级联回执；
- 恢复演练：真实端点状态、批次瀑布、业务码、预留证据；
- 审计证据：版本账本、意图日志、启动恢复报告、引用关系。

控制台只调用本机同源 API，不访问外部网络。
