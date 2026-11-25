# 告警降噪与根因关联引擎 — 技术说明

纯本地、无外部监控平台依赖的 Go 引擎。样本由内置固定种子（`sample.FixedSeed`）
确定性生成；持久化使用纯 Go 驱动 `modernc.org/sqlite`（无 cgo），WAL 模式支持
单写者 + 多读者并发。

## 1. 模块结构

| 包 | 职责 |
| --- | --- |
| `internal/errcode` | 稳定业务错误码（`E_SAMPLE_INVALID` 等） |
| `internal/model` | 领域类型、定点运算、状态机、指纹规范化、输入校验、拓扑 |
| `internal/sample` | 固定种子 LCG 生成器 + 策略快照校验摘要 |
| `internal/storage` | SQLite schema、版本表、意图（intent）记录、请求台账、CRUD |
| `internal/engine` | 候选生成、去重合并、抑制裁决、根因评分、增量提交、原子重建、采样/完整路径、巡检调度、崩溃恢复 |
| `internal/httpapi` | 仅绑定 `127.0.0.1` 高位端口的 HTTP 接口 |
| `cmd/alarmengine` | `demo` / `serve` / `send` 驱动 |

## 2. 关键语义（显式契约）

### 2.1 时间戳与抑制窗口边界
- 时间戳为整数毫秒；不做亚毫秒截断，比较直接使用整数。
- 去重组在 `W` 开窗、窗口 `D`，覆盖**闭区间** `[W, W+D]`：
  `t <= W+D` 合并入当前组；首个 `t > W+D` 的候选**新开条目**，绝不并入旧条目。
- 抑制释放时刻 = `W+D+1`（不再被闭区间覆盖的第一毫秒）。
- 有效去重窗口 = `min(策略抑制窗口, 服务静默窗口)`，保证两个声明边界都不被静默跨越。

### 2.2 指纹
- 由策略声明的 `service_id` / `metric` / `labels` 子集构成；标签按键字典序规范化。
- 规范化文本经 SHA-256 得到指纹，输入字段顺序不影响结果。

### 2.3 告警状态机
`CANDIDATE -> FIRING|SUPPRESSED|RESOLVED`，`FIRING -> SUPPRESSED|RESOLVED`，
`SUPPRESSED -> FIRING|RESOLVED`，`RESOLVED` 为终态。非法迁移返回
`E_INVALID_TRANSITION` 且不修改已持久化状态。

### 2.4 抑制索引
- 规则按 `(service, zone, hop)` 合取匹配，空 service/zone 为通配；hop 为告警服务到
  满足约束服务的最小依赖跳数（确定性 BFS，邻居按字典序展开）。
- 一条告警命中多规则时，**按声明次序**（slice 顺序）取首个为主抑制原因，同时保留全部命中明细。

### 2.5 定点根因评分（无裸浮点）
- 全部使用整数定点，评分单位为 milli（刻度 1000），舍入误差记录在 nano（刻度 1e9）。
- `hopDecay = 1000000 * (1 - 0.15*hops)`，0.15 以有理数 150/1000 精确表示。
- `timeProx = round_half_away(1000000 * max(0, window-delta)/window)`，窗口 30000ms。
- `final = round_half_away(0.6*hopDecay + 0.4*timeProx)`，权重按 per-mille 整数表达。
- 比较使用相对容差整数判定：`|a-b| <= tol_per_mille * max(|a|,|b|) / 1000`；
  容差内按候选服务标识字典序、再按跳数稳定打破平局。
- 每条评分输出 hops、整数 time delta、触发告警 id、每步舍入方式与累计舍入误差。

## 3. 版本、增量提交与原子重建

- 观测/策略增量携带 `base_version`；与当前已提交版本不符返回 `E_STALE_BASE`，不改任何状态。
- 快照带单调递增版本号与整体校验摘要（规范化 SHA-256）；版本回退或摘要不符返回
  `E_SNAPSHOT_INVALID`，旧快照保持完全可用，禁止新旧内容混合。
- 派生重建（候选告警、抑制索引、根因评分、derived_refs）在**同一事务**内先整体删除再重建，
  读者只能看到旧完整版本或新完整版本，不存在“抑制索引已更新而评分仍旧”的中间可见状态。
- 请求台账实现幂等：同 `request_id` 同载荷只执行一次并复用结果；载荷不一致返回
  `E_CONFLICTING_REQUEST`。

## 4. 采样路径 vs 完整路径

- 采样方法为确定性步长采样（`deterministic-stride`），输出采样率、方法、置信说明与选中下标。
- 采样路径复用与完整路径完全相同的阈值/指纹/抑制管线，未经过抑制裁决的告警不计入结论。
- `ComparePaths` 输出逐条可核对的告警 id 差异（only-in-full / shared）及共享根因键，
  采样根因候选与完整路径定位到相同 hops 与 time delta 依据。

## 5. 并行巡检与预算

- 受 `concurrency_limit`（固定 worker 池）与 `round_budget_ms` 双重约束；超量任务进入有界队列，
  队列满返回 `E_PATROL_QUEUE_FULL`，不静默跳过。
- 预算耗尽的降级次序是确定性的：灵敏度档位（HIGH<MEDIUM<LOW）→ 可用区字典序轮转 → 服务 id，
  并输出剩余预算。

## 6. 单写者、强制终止与恢复

- 所有写操作先在事务内插入 `STARTED` 意图并落盘，再写领域表，完成后标记 `COMMITTED`。
- 引擎用 `writeMu` 串行化所有写；只读查询走独立只读连接池，可并发读取某个完整提交版本。
- 崩溃点通过外部参数 `--crash-point` 注入（不改源码），模拟 SIGKILL（进程退出码 97）。
- 重启时 `Recover()` 仅依据 SQLite 已落盘事实：
  - 未完成 INGEST：删除悬挂版本的观测与派生行，版本指针回滚到 `last_complete_version`；
  - 未完成快照替换：删除悬挂快照行与派生行，保持上一完整版本；
  - 协调 STARTED 请求台账，使原 request id 可干净重提。

## 7. Windows 操作（任务目标平台）

```powershell
# 1. 安装 Go 1.21+（官方 msi），验证
go version
# 2. 拉取纯 Go 驱动（无需 gcc/cgo）
go mod tidy
# 3. 运行内置固定种子验收场景
go run .\cmd\alarmengine demo --db .\data\engine.db
# 4. 启动回环 HTTP 服务（高位端口；占用时显式 E_BIND_FAILED）
go run .\cmd\alarmengine serve --port 18123 --db .\data\engine.db
# 5. 本地回环发送端模拟
go run .\cmd\alarmengine send --port 18123 --request-id req-1 --base-version 1 --count 10
# 6. 外部参数注入崩溃（禁止改源码）
go run .\cmd\alarmengine demo --db .\data\c.db --crash-point CRASH_AFTER_INTENT_INGEST
# 7. sqlite3 CLI 核对持久化事实
sqlite3 .\data\c.db "SELECT intent_id,kind,status FROM intents;"
```

## 8. 业务错误码

`E_SAMPLE_INVALID`、`E_STALE_BASE`、`E_SNAPSHOT_INVALID`、`E_INVALID_TRANSITION`、
`E_PATROL_QUEUE_FULL`、`E_CONFLICTING_REQUEST`、`E_BIND_FAILED`、`E_INTERNAL`。

## 9. 验证记录

- `go test ./...` 全绿；`go test -race ./...` 无数据竞争；`go vet` 干净；`gofmt` 无差异。
- 引擎包语句覆盖率约 72%。
- CLI 端到端：去重合并、抑制多规则裁决、65 条根因评分、采样/完整逐条差异、预算降级、
  三类崩溃点（INGEST / SNAPSHOT / MID_REBUILD）注入后重启均成功恢复。
