# 主机安全基线漂移审计引擎技术说明

## 1. 范围与结构

本仓库是一个纯本地 Go 服务/CLI，不依赖外部账号或服务。唯一第三方 Go 依赖是 CGO 版 SQLite 驱动 `github.com/mattn/go-sqlite3`，依赖已放入 `vendor/`，可离线测试和构建。

- `internal/model`：基线、观测、结论、主机汇总、稳定错误码。
- `internal/sample`：固定种子样本生成器，产出 8 条基线、3 台主机、25 条同源观测。
- `internal/normalize`：JSON Lines 与 `AUDITDRIFT-LPB/1` 长度前缀旧版格式解析、毫秒归一化和舍入误差。
- `internal/drift`：分类比较、定点等级合成、依赖传播。
- `internal/schedule`：有界队列、并发上限、轮预算、主机轮转和加权修复配额。
- `internal/probe`：真实 loopback HTTP 探测、独立超时、连接追踪、可延迟测试端点。
- `internal/store`：SQLite schema、意图记录、单调快照、增量提交、原子重建、陈旧读和崩溃恢复。
- `internal/app`：把样本、探测、调度和持久化串成一次审计轮次。
- `cmd/auditdrift`：`seed`、`init`、`audit`、`report`、`increment-demo`、`stale-base-demo`、`crash-demo`、`recover`、`endpoint` 命令。
- `scripts/acceptance.sh`：一键验收和两次 SIGKILL 恢复比对。

## 2. 稳定错误码

| 错误码 | 触发点 |
| --- | --- |
| `E_BASELINE_INVALID` | 基线 ID 重复、权重非正、依赖不存在、区间/枚举/集合期望非法 |
| `E_VALUE_TYPE_MISMATCH` | 数值/字符串/集合类型与基线类别不符，或探测值无法解析 |
| `E_AUDIT_QUEUE_FULL` | 并发槽位占满且有界等待队列已满 |
| `E_PROBE_TIMEOUT` | 单次探测超过独立超时；底层连接被关闭，并发槽释放 |
| `E_PROBE_REFUSED` | loopback 目标端口拒绝连接 |
| `E_STALE_READ` | 存在未完成传播意图，结论视图版本落后于观察版本 |
| `E_STALE_BASE` | 增量携带的基础版本不是当前版本；不改变已提交状态 |

## 3. 样本与双格式归一化

固定种子为 `20261125`。生成器从同一组 `model.Observation` 同时编码：

- `observations.jsonl`：毫秒时间字段 `sampled_at_ms`，实际值为 JSON number/string/array/object。
- `observations.legacy.lpb`：微秒字段 `ts_us`，外层记录和内层字段均为 `<byte-length>:<payload>` 长度前缀帧；可选 `note`、`collector` 可缺失。

旧版微秒时间通过调用方声明的规则转换：

- `truncate_millisecond`：`ms = us / 1000`，正时间戳丢弃 `us % 1000`。
- `nearest_millisecond`：余数至少 500 时进一。
- `ceiling_millisecond`：存在亚毫秒余数即进一。

每条 `RoundingInfo` 输出源精度、目标精度、丢弃位数（微秒到毫秒为 3）、余数、是否进位、本条有符号误差和累计误差。归一化后的 `Observation` 不携带源格式字段；格式差异只保留在独立舍入报告中，因此同源 JSONL 与旧版记录可 `reflect.DeepEqual`。

## 4. 漂移判定

### 4.1 分类比较

- `account_policy`、`audit_policy`：必须是整数，按 `[min,max]`、`(min,max)` 等显式端点关系判断；`Interval.String()` 和结论详情都输出端点开闭。
- `service_startup`：必须是字符串，先核对声明枚举，再与 `expected_text` 比较。
- `registry_acl`：必须是集合，输出 `missing_elements` 与 `extra_elements`；仅缺失为 `missing`，仅多余为 `extra`，两者同时存在归为 `value_offset`。
- 无任何观测/探测值：`missing`。
- 多个不同归一化值：`source_conflict`。

HTTP 探测返回可解析但不符合期望的值时，不返回探测错误，而是进入同一判定核心并产生普通漂移结论。

### 4.2 定点等级

偏移量用千分比整数 `offset_permille` 表示，集合差异和枚举失配也折算为整数分值。等级分数为：

```text
score_scaled = severity_weight * 1000 + offset_permille
level = clamp(1 + floor((score_scaled - 1) / 2500), 1, 4)
```

`LevelRounding` 保存被除数、除数、商、余数和 `floor` 说明，全程不使用浮点数合成等级。

## 5. 调度、预算与配额

`schedule.OrderTasks` 先按严重度权重降序分层；同一权重内按主机字典序建立队列并逐轮弹出，实现主机轮转，避免单台主机连续占满同权重任务。

`Scheduler` 的准入模型：

1. `MaxConcurrency` 个活动槽位。
2. 槽位满后任务进入容量为 `QueueSize` 的显式等待队列。
3. 活动槽和等待队列都满时返回 `E_AUDIT_QUEUE_FULL`。
4. `RoundBudget` 派生 `context.Context`；预算到期后运行中任务收到取消，未启动任务标记 `DEFERRED/round_budget_exhausted`，并通过 `pump()` 释放活动槽。
5. 所有任务结束后，按 `Task.Order` 做确定性配额裁决。只有结论为漂移且被配额批准的修复动作消耗 `severity_weight`。
6. 严格优先级策略下，一旦下一修复动作的加权成本超过剩余配额，该动作及后续需要修复的任务均标记 `DEFERRED/repair_quota_exhausted`；降级记录包含顺序、成本、剩余配额和判定依据。合规任务仍保持 `DONE`，因为它们不需要修复动作。

## 6. HTTP 探测

`probe.Client` 为每次调用执行 `context.WithTimeout(parent, Timeout)`，因此每个探测有独立超时。Transport 设置 `DisableKeepAlives=true` 和 `req.Close=true`；超时后取消请求上下文并关闭空闲连接。测试用 `ConnTracker` 包装拨号连接，轮询断言 `closed >= 1 && active == 0`。

错误映射：

- 探测自身超时：`E_PROBE_TIMEOUT`。
- `ECONNREFUSED` 或错误文本为 connection refused：`E_PROBE_REFUSED`。
- 父轮预算先到期：返回原始 `context.DeadlineExceeded`，调度器归类为预算降级，而不是探测超时。

`probe.NewTestEndpoint` 基于 `httptest.Server` 监听 `127.0.0.1`，可按 baseline ID 注入延迟；`probe.NewListeningEndpoint` 支持固定回环地址。`ClosedLoopbackURL` 提供真实拒绝连接端口。

## 7. 依赖传播与陈旧读

若 B 依赖 A，则建立反向边 `A -> B`。传播按主机隔离，从每个自身等级大于 0 的基线出发进行有界 BFS：

```text
boost = max(0, source_own_level - hop + 1)
target_level = min(4, target_own_level + max_boost)
```

每条影响保留源基线、目标基线、跳数、源等级、增益和完整路径；跳数超过 `propagation_max_hops`（样本会话为 3）后停止，环路径不会重复进入。

读接口携带 `observed_version`：

- 若结论 `view_version <= observed_version` 且没有目标版本未完成的传播意图，返回结论并标注 `at_or_before_observed`。
- 若存在 `PENDING` 的 `propagation/snapshot_increment/session_initialize` 意图且目标版本至少为观察版本，而当前视图落后，则返回 `E_STALE_READ`，详情含 `view_version`、`observed_version` 和待处理意图。

正常增量提交在同一 SQLite 事务内完成结论、下游等级和主机汇总重建，因此已提交版本不会暴露半重建状态；陈旧读分支用于传播窗口/崩溃意图仍存在时的裁决。

## 8. 快照、增量和原子重建

每个会话维护单调版本：

- `snapshots(version, base_version, intent_id, ...)`。
- `baselines(session_id, snapshot_version, id, ...)` 保存每个完整版本，增量请求体只携带变化项。
- `CommitIncrement` 先持久化 `snapshot_increment` 意图，再开启 `immediate` 写事务。
- 若 `base_version != current_version`，把意图标记为 `REJECTED/E_STALE_BASE` 并提交该状态；业务表不变。
- 合并增量子项后重新执行 `model.ValidateBaselines`；非法时标记意图拒绝，业务表不变。
- 新版本插入后，在同一事务中同步任务权重/新增基线、删除被删基线任务、重建全部结论与传播、重算主机汇总，并把 `current_version`、`last_complete_version`、`view_version` 一起推进。

因此期望取值、依赖边或严重度权重变化都会导致该基线、下游和主机汇总在同一事务边界内重建。

## 9. SQLite 单写者、多读者和恢复

`Store.Open` 使用 WAL、`synchronous=FULL`、`busy_timeout=5000` 和外键开启：

- writer 连接池 `SetMaxOpenConns(1)`，DSN 使用 `_txlock=immediate`；另有 `writeMu` 串行化会话写流程。
- reader 连接池最多 8 个连接，使用 deferred 读事务；`WithReadOnlyTx` 提供完整只读事务。WAL 下读者读取已提交版本，不读取未提交的半事务状态。

业务写入遵循意图先行：

1. 向 `intents` 自动提交一条 `PENDING` 记录并 fsync。
2. 开启 SQLite 事务写业务事实。
3. 在同一事务末尾把意图更新为 `COMMITTED`。
4. 提交事务。

任务结论提交会在同一事务内 upsert `conclusions`、更新 `audit_tasks`、扣减配额，并重算该会话所有主机汇总，所以不可能提交“某基线已是 `DEFERRED` 而对应主机汇总仍是旧计数”的状态。

恢复流程 `Store.Recover`：

1. 先写 `recovery_run` 意图。
2. 扫描所有业务 `PENDING` 意图。
3. 对存在会话的意图，在事务中标记 `ABORTED`、把 `RUNNING` 任务重置为 `PENDING`、将会话 current/view 版本回滚到 `last_complete_version`，并重算主机汇总。
4. 对没有会话的初始化意图，仅标记 `ABORTED`。
5. 输出只来自 SQLite 查询的 JSON 报告，包含意图数、版本、任务状态计数、主机汇总、`PRAGMA integrity_check` 和一致性违规。

真实 `kill -9` 后，SQLite 自动回滚未提交事务；已 durable 的 `PENDING` 意图成为恢复线索。`crash-demo` 会先落意图，再开启一个修改 `host_summaries.drift=999` 的未提交事务并等待，供外部强杀。

## 10. 常用命令

```bash
go test -mod=vendor -count=1 ./...
go test -mod=vendor -race ./internal/schedule ./internal/probe ./internal/store
bash scripts/acceptance.sh

# 手动分步演示
go run ./cmd/auditdrift seed --dir .demo/seed
go run ./cmd/auditdrift init --db .demo/audit.db --quota 25
go run ./cmd/auditdrift audit --db .demo/audit.db --concurrency 3 --budget 5s
go run ./cmd/auditdrift increment-demo --db .demo/audit.db
go run ./cmd/auditdrift crash-demo --db .demo/audit.db
# 另一个终端：kill -9 <pid>
go run ./cmd/auditdrift recover --db .demo/audit.db
sqlite3 .demo/audit.db "select status,count(*) from intents group by status;"
```

`scripts/acceptance.sh` 每次删除并重建 `.acceptance-tmp/`，连续执行两次 SIGKILL/恢复周期，并用 Python 归一化比较两份恢复报告。
