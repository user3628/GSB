# 网络策略增量编译组件说明

Go module 为 `netpolcompiler`，SQLite 驱动固定为 `github.com/mattn/go-sqlite3 v1.14.24`。HTTP 默认只监听 `127.0.0.1:18080`，数据目录默认 `/tmp/netpol-compiler-data`，启动时会自动创建。

## 快速运行

```bash
go run ./cmd/netpol-compiler --seed-sample
# 或指定可写目录
go run ./cmd/netpol-compiler --addr 127.0.0.1:18080 --data-dir ./.localdata --seed-sample
```

一键本地验收（编译、HTTP、并发、陈旧读、SIGKILL 恢复、Go 测试）：

```bash
./scripts/verify.sh
```

完整实际输出写入 `artifacts/verification-results.txt`。

## 包结构

- `internal/model`：命名空间、规则、选择器、端口区间、快照、增量模型；首非法字段校验与规范化摘要。
- `internal/sample`：固定种子 `327` 的确定性样本、增量样例和重排工具。
- `internal/compiler`：选择器匹配/包含、端口合并、冲突对、逐端口裁决、稳定摘要。
- `internal/fixedpoint`：Q6 定点一致度、舍入误差、相对容差比较与命名空间字典序平局。
- `internal/store`：SQLite schema、版本化规则/视图/引用、幂等请求、意图记录和恢复查询。
- `internal/service`：单写者、幂等单飞、待重建状态、原子事务、缓存指针切换和恢复。
- `internal/api`：回环 HTTP JSON API 和统一错误信封。

## 规则与选择器

规则主键为 `(namespace,id)`，字段包括：

```json
{
  "namespace": "demo",
  "id": "demo-allow-edge",
  "action": "allow",
  "source": {
    "matchLabels": {"role": "edge"},
    "exists": ["env"],
    "notIn": [{"key": "breakglass", "values": ["true"]}]
  },
  "destination": {"matchLabels": {"app": "web", "tier": "frontend"}},
  "ports": [{"start": 8080, "end": 8090}],
  "protocol": "TCP",
  "priority": 100,
  "declarationOrder": 1
}
```

- `action` 支持 `allow|deny`，大小写不敏感并规范化为小写。
- `protocol` 支持 `TCP|UDP`，大小写不敏感并规范化为大写。
- 端口为闭区间，合法范围 `1..65535`，要求 `start <= end`；省略 `ports` 表示该协议全端口。
- 优先级范围 `0..1000`，数值越大越优先。
- 同优先级先按显式 `declarationOrder`；未提供该字段时按规则 ID 字典序作为稳定“规范化声明次序”，因此规则数组重排不会改变编译或裁决摘要。

校验错误统一为：

```json
{"ok":false,"error":{"code":"E_RULE_INVALID","field":"rules[0].action","context":{"reason":"..."}}}
```

## 版本提交与并发

- `POST /v1/snapshots`：整份快照。`version <= currentVersion` 返回 `E_STALE_SNAPSHOT`，不写状态。
- `POST /v1/increments`：增量。`baseVersion != currentVersion` 返回 `E_STALE_BASE`。成功后新版本为 `baseVersion+1`。
- 增量 `changes[].op` 支持 `upsert`、`delete` 和 `replace_namespace`。
- 所有快照、增量和级联重建由同一把写锁串行化。SQLite 事务提交后，服务通过 `atomic.Pointer` 一次性替换完整命名空间视图映射；读方只能拿到旧完整版或新完整版。
- 同一 `requestId` 的并发请求只有一个执行，其余等待并复用同一响应；同 ID 但规范化内容哈希不同返回 `E_CONFLICTING_REQUEST`。成功响应与请求记录在同一写事务中落库。

## 编译与裁决契约

- 选择器索引为每条规则生成 `ruleId:source` 与 `ruleId:destination` 条目。
- 包含关系输出 `strict_subset` 或 `equal`，并保留具体/宽泛选择器对应的原规则 ID。
- 端口按协议排序并合并；合并规则是 `next.start <= current.end + 1`：`next.start <= current.end` 为相交，`next.start == current.end + 1` 为相邻。视图输出每个协议及总计的 `rawCount`、`mergedCount` 和合并事件。
- 裁决按 `priority desc`，同优先级按稳定声明次序。结果输出全部命中规则、决定规则和每规则依据。
- 端口逐个展开判定。部分重叠时，不同端口可以有不同动作；总动作在不一致时为 `mixed_by_port`。
- `compiledDigest` 和 `decisionDigest` 分别固定编译结果与裁决结果；相同版本、选择器和端口在规则重排后摘要不变。

## 定点一致度

使用 `Q6=1,000,000`。默认权重为：

- 已编译规则数项：`0.20 * min(ruleCount,10)/10`
- 无冲突项：`0.50 * (1 - conflictPairs/totalPairs)`
- 端口合并项：`0.30 * (rawIntervals-mergedIntervals)/rawIntervals`

每个中间除法均采用 `round_half_up` 到 Q6，并在 `roundingSteps` 中输出公式、舍入值、本地误差和传播到总分的累计误差。最终 `cumulativeRoundingError` 是整数 Q6 结果相对精确有理数总分的误差分数。

阈值比较不使用 float：

- `scoreQ >= thresholdQ`：`exceeds_threshold`
- 差额不超过按 Q6 四舍五入的相对容差：`within_relative_tolerance`
- 否则：`below_threshold`

容差边界恰好相差 1 Q6 时分别标记 `inside_tolerance_at_boundary` 与 `outside_tolerance_one_q_unit`。命名空间排序在相对容差内视为平局，再按命名空间 ID 字典序稳定排序。

## 观察版本与陈旧读

`GET /v1/views/{namespace}?observationVersion=N` 和 `POST /v1/adjudications?observationVersion=N`：

- 若存在 `version <= N` 的完整视图，返回该视图并标注 `usedVersion`、`currentVersion` 和 `freshness`。
- 若该命名空间有目标版本为 `T` 的未完成级联，且 `N >= T`，返回 `E_STALE_READ`，上下文包含 `viewVersion`、`observedVersion`、`targetVersion`、`lagVersions` 和原因。
- 当 `N < currentVersion` 时，从 SQLite 读取 `<= N` 的最后一个不可变版本化视图。

可用 `--rebuild-delay 2s` 制造可观察的重建窗口；自动化测试则使用进程内 barrier 稳定复现 `E_STALE_READ`。

## SQLite、意图日志与崩溃恢复

数据目录包含：

- `policies.db`：SQLite 数据库（WAL 模式）。
- `policies.db-wal`、`policies.db-shm`：SQLite WAL 文件。
- `intent.log`：意图记录的追加式 JSONL 镜像。

主要表：

- `snapshots`：快照/增量版本和规范化 JSON。
- `rules`：每个版本的完整规则集。
- `compile_views`：选择器索引、端口视图、裁决/冲突、一致度和完整编译 JSON。
- `view_references`：派生组件到规则字段、命名空间和快照版本的引用。
- `consistency_reports`：跨命名空间一致度报告。
- `intents`：`prepared|committed|aborted|failed` 意图。
- `requests`：请求哈希、终态响应和错误码。

提交流程是：先持久化 `intents(status=prepared)` 并同步 `intent.log`，再编译；规则、视图、引用、一致度报告、版本元数据、意图完成状态和幂等响应在同一个 SQLite 事务中提交。

重启时 `Service.New` 自动执行 `Recover`：

- 扫描所有 `prepared|compiling` 意图，报告落盘的 `snapshotPresent` 和 `readyViewRows`。
- 将未完成意图及关联 processing 请求标记为 `aborted`。
- 删除任何非 `ready` 视图和孤立引用行（原子事务保证通常为 0 行）。
- 载入最后一个 ready 版本。

恢复报告只来自 SQLite 落盘事实，可通过 `GET /v1/recovery` 查询。

## HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 健康检查 |
| `GET /v1/sample/snapshot?seed=327&version=1` | 固定种子样本（统一信封的 `data` 是快照） |
| `GET /v1/sample/increment?baseVersion=1&requestId=...` | 确定性增量样例 |
| `POST /v1/snapshots` | 提交整份快照，可在体或 `X-Request-ID` 中带请求 ID |
| `POST /v1/increments` | 提交增量 |
| `GET /v1/views/{namespace}?observationVersion=N` | 查询编译视图 |
| `POST /v1/adjudications?observationVersion=N` | 提交标签/协议/端口裁决请求 |
| `GET /v1/consistency?observationVersion=N&threshold=0.75&relativeTolerancePpm=50000` | 一致度、误差说明、阈值结论 |
| `GET /v1/recovery` | 最近一次启动恢复报告 |
| `POST /v1/admin/recover` | 立即重新扫描并执行恢复 |
| `GET /v1/requests/{requestId}` | 查询幂等请求记录 |

裁决请求示例：

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/adjudications?observationVersion=1 \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace":"demo",
    "sourceLabels":{"role":"edge","env":"prod"},
    "destinationLabels":{"app":"web","tier":"frontend"},
    "protocol":"TCP",
    "ports":[8083,8087,8092]
  }'
```
