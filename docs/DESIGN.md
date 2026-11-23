# 设计说明

## 1. 模块与职责

| 包 | 职责 |
| --- | --- |
| `internal/freshness` | 输入模型、整批校验、规范化 HMAC、时间闭区间、响应/诊断 DTO |
| `internal/config` | 本地配置、回环地址校验、配置版本快照 |
| `internal/store` | SQLite 事务、单写者、幂等、nonce 表、版本账本、恢复、诊断查询 |
| `internal/service` | 校验策略编排、错误映射、查询预算边界 |
| `internal/httpapi` | 本地 HTTP API、内嵌样本、静态诊断工作台 |
| `cmd/workbench` | 服务启动、恢复扫描、优雅退出 |
| `cmd/samplegen` | 固定密钥下的确定性样本生成 |

运行时只使用 Go 标准库和 `modernc.org/sqlite`。SQLite 驱动为纯 Go 实现，因此 Windows 构建无需 CGO；仍提供 gcc 检测以兼容有本地 C 工具链的环境。

## 2. 输入模型与整批原子校验

批次字段：

```json
{
  "schema_version": 1,
  "batch_id": "01-sample-accept-001",
  "mode": "full",
  "gateway_reference_time_ms": 1800000000000,
  "requests": [{
    "request_id": "req-alpha-accept",
    "timestamp_ms": 1800000000100,
    "route_key": "orders.create",
    "client_id": "client-alpha",
    "nonce": "nonce-alpha-00000001",
    "signature": "64-char lowercase hex",
    "weight": 2,
    "tolerance_ms": 500,
    "window_ms": 5000
  }]
}
```

整批校验在任何意图创建前完成。以下任一失败均返回 `E_REQUEST_INVALID`，整批不裁决、不占用 nonce、不消耗窗口版本：

- nonce：16–64 字节，字符集 `[A-Za-z0-9_-]`。
- `timestamp_ms >= 0`；`weight > 0`；批次权重累计不得溢出 int64。
- `0 <= tolerance_ms <= server.max_tolerance_ms`；`0 < window_ms <= server.max_window_ms`。
- `batch_id`、`request_id`、`client_id`、`route_key` 的字符集与长度约束。
- 批次内 `request_id` 唯一；空 `request_id` 按 `req-0001` 形式确定性补齐。
- JSON 整数超出 int64 时由解码器拒绝，HTTP 层统一转为 `E_REQUEST_INVALID`。
- `gateway_reference_time_ms` 覆盖仅在 `allow_reference_override=true` 时可用，否则也是整批校验错误。

## 3. 签名规则

固定本地密钥只从本地配置读取。规范字符串使用域分隔与长度前缀，避免字段边界移动：

```text
gwfresh.request.v1|
len(timestamp_ms):timestamp_ms|
len(route_key):route_key|
len(client_id):client_id|
len(nonce):nonce|
len(weight):weight|
len(tolerance_ms):tolerance_ms|
len(window_ms):window_ms|
```

摘要为 `hex(HMAC-SHA256(key, canonical))`。签名绑定了 nonce、权重、tolerance 与 window，修改任一字段都会返回 `E_SIGNATURE_MISMATCH`。校验使用 `hmac.Equal` 常数时间比较。

## 4. 时间新鲜度

```text
offset_ms = timestamp_ms - gateway_reference_time_ms
lower = -tolerance_ms
upper = +tolerance_ms
accept iff lower <= offset_ms <= upper
```

边界关系为显式双闭区间。`offset == lower` 与 `offset == upper` 均通过；`offset < lower` 返回 `E_TIMESTAMP_TOO_OLD` 且 `triggered_boundary=lower`；`offset > upper` 返回 `E_TIMESTAMP_AHEAD` 且 `triggered_boundary=upper`。每个逐条结果都输出 `offset_ms`、上下界和该规则文本。

## 5. nonce 去重与回收

当前占用表主键为 `(client_id, nonce)`。首次通过签名与时间检查的请求执行：

```text
first_occupied_at_ms = gateway_reference_time_ms
expires_at_ms = first_occupied_at_ms + window_ms
```

重复请求返回 `E_NONCE_REPLAYED`，并附带首次占用时刻、首次批次/请求、到期时刻和剩余窗口。不同客户端的相同 nonce 互不影响。

`nonce_events` 永久保留 `occupy` / `reclaim` 历史，用于按版本重建诊断视图。写事务开始时，对 `expires_at_ms <= reference_time_ms` 的当前 nonce 执行回收；回收前查询 `intent_nonce_refs`，如果存在其他 pending 意图引用该 `(client_id, nonce)`，本版本暂不回收。回收事件与当前占用表的删除和当前批次裁决处于同一事务、同一 `window_version`。因此“窗口到期后允许复用”与“去重结论”不会读取不同版本。

## 6. 完整模式与快速模式

完整模式检查每条请求的签名、时间、nonce，`sample_rate = 1`，置信说明为 `exact`。

快速模式使用配置中的 `fast_precheck_limit`，按输入顺序取确定性前缀。前缀内的请求走完整检查；前缀外请求：

```json
{
  "result": "not_prechecked",
  "code": "E_PRECHECK_INCOMPLETE",
  "prechecked": false,
  "signature_stage": {"status": "not_prechecked"},
  "timestamp_stage": {"status": "not_prechecked"},
  "nonce_stage": {"status": "not_prechecked"}
}
```

前缀无失败但存在未预检项时，批次结果为 `partial`、错误码 `E_PRECHECK_INCOMPLETE`、置信 `bounded_partial`。前缀内出现失败则为 `rejected` 并返回该失败码。未预检项不会进入 nonce 表，也不会在任何字段中被描述为 accepted。

对于完整模式裁决，`mode-differences` API 使用该裁决存储的同一版本与同一 precheck limit 逐条投影 fast 结果；每个跳过后缀项都会显示 `fast_result=not_prechecked` 和差异说明。对于原本就是 fast 的裁决，完整结果明确为 `full_result_available=false`，不会补造完整结论。

## 7. 版本一致性

每次成功解析并完成的批次提交获得新的全局 `window_version`。所有请求、签名阶段、时间阶段、nonce 阶段、nonce 事件和审计轨迹行写入相同版本。`tolerance_config_version` 是配置快照哈希，包含签名密钥摘要、服务端边界、precheck 上限等策略字段，但不暴露密钥原文。

写路径顺序：

1. 获取进程内批次单写者锁。
2. 读取现有 `batch_id`：同内容复用；异内容 `E_CONFLICTING_REQUEST`。
3. 在独立事务中提交 `pending` 意图，并写入该意图引用的 nonce 集合。
4. 开启裁决写事务：回收、逐条判定、占用 nonce、写轨迹、写批次/条目、推进 server state、将意图改为 `committed`。
5. 原子提交。失败则尝试把意图标为 `aborted`。

只读查询在 SQLite 读事务内选择一个已提交版本。响应中的 `snapshot_window_version`、`tolerance_config_version`、每条占用记录和每条轨迹都带版本标签。历史占用由 `nonce_events <= version` 重建，不读取当前表后混入旧批次。

## 8. 预算与部分结果

`snapshot` 支持 `budget_ms` 与 `version`：

- 预算足够：`complete=true`，一次返回 `occupancy`、`nonces`、`trails`。
- 预算为 0 或在区段间耗尽：`partial=true`，只返回已完成区段，所有已完成区段仍来自同一读事务与同一版本。
- 区段查询中途超时：该区段不返回，列入 `incomplete_sections`，不会回退到另一个版本补齐。
- 未来/未提交版本返回 `E_VERSION_INVALID`。

零预算显式返回 `partial=true` 与版本标签，而不是静默回落到默认预算。

## 9. 客户端版本不一致

`POST /api/v1/diagnostics/inconsistencies` 接受多个视图：

```json
{
  "batch_id": "06-sample-reclaim-base-006",
  "budget_ms": 40,
  "views": [
    {"client_id": "reader-old", "picked_window_version": 6},
    {"client_id": "reader-new", "picked_window_version": 8}
  ]
}
```

签名与时间字段是不可变事实；nonce 可见性按 `picked_window_version` 从 `nonce_events` 重建，并排除当前批次自身的占用，避免把已可见的 accepted 结论误判为自重放。报告按客户端输出：

- `client_id`
- `picked_window_version` / `picked_config_version`
- `batch_window_version` / `decision_config_version`
- 是否可见、实际与视图批次码
- `differs` 与逐字段 `field / decision_value / client_viewed_value / explanation`

当客户端拾取版本早于批次提交时，明确给出 `E_VERSION_NOT_VISIBLE`；当 later 版本看到后续 nonce 占用时，则可把同一历史批次显示为 `E_NONCE_REPLAYED`。这是可解释的版本差异，不是静默漂移。

## 10. 幂等与并发

- 进程内 `sync.Mutex` 保证裁决单写者；SQLite `WAL` 与读事务支持并发查询。
- `batch_id` 是提交幂等键；`content_hash` 覆盖模式与全部签名请求字段，不包含服务端派生的网关参考时刻。这样同一批次在稍后重试仍复用首次裁决，而不会因时钟推进被误判为内容冲突。
- 同内容重复提交等待首个写者完成，然后返回同一已提交决策并设置 `idempotent_replay=true`。
- 同 `batch_id` 不同内容返回 `E_CONFLICTING_REQUEST`，原裁决与版本不变。
- 进程重启后，如果批次已提交但客户端未收到响应，重试仍会复用已持久化结果。

## 11. 意图持久化与强制终止恢复

SQLite 表 `intents` 是提交围栏。意图先以 `pending` 状态独立提交；随后所有裁决事实在一个事务内提交，并把意图改为 `committed`。因此崩溃点只有两类：

- 裁决事务前/中：磁盘无批次行；写事务中的 nonce、轨迹、条目均随事务回滚。
- 裁决事务提交后：批次、nonce、轨迹和意图 `committed` 同时可见。

启动恢复扫描所有 `pending` 意图：

- 若存在同批次完整裁决：标记 `confirmed_committed`，保留事实。
- 否则：按该意图的 batch id + planned version 防御性删除可能存在的孤儿 nonce/事件/轨迹，标记意图 `aborted`，并写入 `recovery_reports`。

恢复报告只包含查询到的持久化事实：决策是否存在、孤儿行数量、动作与计划版本。正常写入不会产生半提交状态；删除逻辑处理外部损坏或旧版本遗留。计划版本中止后允许版本号出现空洞，诊断只暴露已提交版本。

演示与测试通过 `WORKBENCH_CRASH_POINT=after_intent|after_writes` 在子进程中 `os.Exit`，前一个在意图提交后退出，后一个在写事务尚未提交时退出。测试会检查退出码、恢复动作、决策不可见、nonce 不残留。

## 12. 工作台

`internal/httpapi/web` 使用原生 HTML/CSS/JS，随二进制嵌入，不依赖 CDN。页面提供：样本载入与提交、逐条裁决表、版本标签、占用/去重/轨迹快照、fast/full 差异、客户端版本差异与恢复历史。

## 13. 已知边界

- SQLite 单机单写者适合本任务和本地诊断，不是跨节点一致性方案。
- 按历史版本重建 nonce 状态会扫描 `nonce_events`；样本和本地规模足够，超大规模应增加物化快照或分区索引。
- 快速模式的“完整结果”仅对原本完整裁决可精确投影；对 fast 裁决不会伪造完整检查。
