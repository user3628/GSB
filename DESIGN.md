# 配置订阅树收敛与陈旧读裁决管线 — 技术说明

## 1. 目标与范围

本管线面向本地多组件平台的读路径，围绕“键路径树 + 组件订阅 + 上游依赖”三类事实，
在单进程内提供：

- 带单调版本号的完整快照与局部增量提交；
- 最长前缀匹配、嵌套前缀隔离与多来源覆盖裁决；
- 有效配置视图、受影响组件集合与传播跳数重算；
- 携带观察版本的陈旧读裁决（拒绝降级返回旧视图/部分视图）；
- 级联失效与单事务原子重建；
- 单写者/多读者并发、提交幂等与冲突检测；
- 意图日志（write-ahead intent）驱动的强制终止恢复。

运行期只依赖 Go 标准库与纯 Go 的 `modernc.org/sqlite`，无容器、无外部服务；
依赖已通过 `vendor/` 固化，断网环境可用 `-mod=vendor` 构建。

## 2. 目录结构

```
cmd/configtree/      服务入口（生成样本、启动 HTTP、可选崩溃钩子）
cmd/crashdemo/       真实 os.Exit(137) 硬杀与重启恢复演示
internal/spec/       模型、确定性校验摘要、整批校验（E_SPEC_INVALID）
internal/pathtree/   点分段键路径 Trie：最长前缀匹配与跳数
internal/seed/       内置固定种子，写出确定性 JSON 样本到 samples/
internal/engine/     SQLite 持久化、提交、级联重建、陈旧读、恢复
internal/httpapi/    本地 HTTP 接口与统一 JSON 错误响应
scripts/validate.sh  基于固定样本的端到端验证脚本
samples/             由种子生成器写出的样本（验证脚本读取同一批）
```

## 3. 数据模型与校验

- 键路径按 `.` 分段，空段非法；`spec.HopDistance` 计算前缀到键的段差。
- 配置项：`key_path / type / value / order`，同快照内键唯一，类型限定
  `string|int|float|bool|json`，值必须是合法 JSON。
- 组件：`id` 唯一；`sub_prefixes` 必须是某个已声明键路径的路径前缀；
  `wildcard=true` 为显式通配订阅（不得再声明具体前缀）；依赖边必须引用
  已存在组件，且依赖图无环、无自环、无重复边。
- 快照携带 `version / generated_at / checksum`；`checksum` 是对排序后
  配置项与组件集合的确定性 SHA-256（见 `spec.Checksum`）。
- 任一校验失败返回 `E_SPEC_INVALID`，整批不生效：校验在任何写库动作之前
  完成，增量也先在内存副本上套用并跑完整组件校验。

## 4. 前缀匹配与覆盖裁决

- 每个组件的订阅前缀构造成一棵 `pathtree.Trie`；匹配沿键路径下行，收集全部
  命中的终端前缀后取最深者（最长前缀优先），并输出匹配前缀与跳数。
- 嵌套前缀隔离：父前缀命中的键不会被计入子前缀。例如 `auth` 订阅
  `service.http.tls`，其上游 `gateway` 订阅 `service.http`；
  `service.http.tls.enabled` 在 auth 视图中归属于自身的子前缀（跳数 1），
  而 `service.http.host` 仅通过上游依赖边进入 auth 视图。
- 视图候选来源：组件自身订阅（`self`，显式来源优先级 100）与每个上游组件
  订阅（`upstream`，优先级 50）。同一键多来源时裁决顺序：
  1. 生效顺序号 `order` 大者胜；
  2. 顺序号相同则显式来源优先级高者胜；
  3. 仍相同按来源类型与组件标识确定性决胜。
  胜出来源写入视图条目，每个落选候选生成一条冲突明细（键、双方来源、
  裁决依据 `order|source_priority|...`）。
- 通配组件匹配全部键，前缀记为 `*`，跳数为键的段数。

## 5. 版本、快照与增量

- 快照版本必须严格大于当前版本，否则 `E_STALE_SNAPSHOT`，且不触碰任何已提交状态。
- 增量携带 `base_version / target_version`；base 不等于当前版本即
  `E_STALE_BASE`。因此同一 base 的两个并发增量，在单写者串行化下只有一个
  能成功，另一个观察到版本已推进而收到 `E_STALE_BASE`。
- 增量支持 `upsert|delete` 配置项与 `replace` 组件订阅/依赖；非法操作或
  套用后破坏整批约束，一律 `E_SPEC_INVALID` 且零副作用。

## 6. 两阶段事务、意图日志与级联原子重建

所有写操作在单写者锁内串行，并拆成两个 SQLite 事务：

1. **事实事务**：先向 `intents` 写一条 `status=prepared` 的意图（含待收敛
   键路径与原始请求），再写入/替换配置项、组件、订阅、依赖事实，提交。
2. **派生事务**：级联失效受影响组件在 `views / view_refs` 中的全部派生行，
   在同一事务内重建全部组件有效视图、引用关系（`key / prefix / dep_edge`）、
   受影响集合，写入 `commits` 与幂等记录，并把意图置为 `completed` 后提交。

读者只能看到两个已提交事务之一的状态，因此不可能观察到“键已写入而视图摘要
未更新”，也不可能出现“上游视图已重建、下游受影响集合仍指向旧依赖边”。
`view_refs` 显式记录每个组件派生结构对键路径、订阅前缀与依赖边的引用关系，
依赖改向（`delta_v3_redirect.json`）后旧边 `worker->auth` 的引用被删除并
替换为新边 `worker->gateway`。

受影响集合从直接订阅被改键（或通配命中）的组件出发，沿反向依赖边做全下游
闭包，记录触发键、传播路径与传播跳数，并标记是否存在未收敛组件。

## 7. 陈旧读裁决

`GET /v1/components/{id}/view?observed_version=N`：

- 不存在未完成意图且 `N >= 视图版本`：返回完整视图，标注 `used_version`。
- 存在 `prepared` 意图（重算未完成/崩溃后尚未恢复）且 `N > 视图版本`：
  返回 `E_STALE_READ`，上下文含视图版本、观察版本、待收敛键路径清单与
  未完成意图 ID；**不返回更旧视图，也不返回部分视图**。
- 恢复完成后未完成意图被中止，视图严格停留在最后一个完整提交版本；恢复
  报告中的每个组件结论都由已落盘的 `views/intents/commits` 行复核得出。

## 8. 强制终止恢复

启动时 `recoverPendingIntents` 扫描 `status=prepared` 的意图：

- 按版本顺序重放 `commits` 中 `status=committed` 的载荷，重建事实表到最后
  完整版本（派生表本就只描述该版本，保持不动）；
- 将未完成意图置 `aborted`，对应增量在 `commits` 标记 `aborted`；
- 生成并持久化恢复报告（最后良好版本、被中止意图、每个组件的视图版本、
  状态与待收敛键）。

`cmd/crashdemo` 用真实子进程在阶段一提交后、阶段二开始前调用 `os.Exit(137)`，
父进程先以“不触发恢复”的只读句柄观察到 `E_STALE_READ`，再正常打开引擎完成
回滚恢复。

## 9. 并发与幂等

- 进程内 `writeMu` 保证快照、增量与级联重建单写者串行；读路径不持写锁，
  配合 SQLite WAL 支持多读者并发读取完整提交版本。
- 幂等键 `commit_id`：进行中的同键请求在 `inflight` 上单飞等待并复用结果；
  已完成的同键请求比对原始请求体，一致则复用（响应含 `reused:true`），
  不一致返回 `E_CONFLICTING_REQUEST`。

## 10. HTTP 接口与错误码

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/commits/snapshots` | 提交完整快照 |
| POST | `/v1/commits/deltas` | 提交增量片段 |
| GET  | `/v1/components/{id}/view?observed_version=N` | 陈旧读裁决 + 有效视图 |
| GET  | `/v1/affected/{version}` | 某版本受影响组件集合 |
| GET  | `/v1/commits` | 提交/中止记录 |
| GET  | `/v1/recovery` | 最近一次启动恢复报告 |
| GET  | `/healthz` | 健康检查 |

提交信封统一为 `{"commit_id":"...","payload":{...}}`。错误响应统一为
`{"code":"E_...","message":"...","context":{...}}`，覆盖
`E_SPEC_INVALID / E_STALE_SNAPSHOT / E_STALE_BASE / E_STALE_READ /
E_CONFLICTING_REQUEST`（另有 `E_NOT_FOUND`）。

## 11. 本地验证

```
go test ./...                 # 单元与引擎集成测试
./scripts/validate.sh         # 固定样本 HTTP 端到端 + 硬杀恢复
go run ./cmd/crashdemo        # 真实 os.Exit(137) 恢复演示（等价命令）
go run ./cmd/configtree -gen-seed -samples samples
go run ./cmd/configtree -db data/pipeline.db -addr 127.0.0.1:8080
```

固定样本（`internal/seed` 确定性生成）覆盖：嵌套前缀与自身/上游同源覆盖、
通配订阅、缓存增量、同 base 并发增量、依赖改向、错误 base、非法整批与
陈旧快照版本。

## 12. 关键设计取舍

- 纯 Go SQLite 驱动避免 CGO 与容器依赖；WAL + 单写者锁兼顾多读者与串行写。
- 事实与派生分离、意图先行，使崩溃恢复只需“回滚事实 + 中止意图”，无需
  补偿半成的派生结构。
- 校验全部前置且在内存副本上预演，保证任何拒绝路径都不留副作用。
