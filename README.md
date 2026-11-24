# 伸缩策略迁移与副本预算重估组件

面向本地单机推理集群的策略升级核对组件。它把旧版扁平策略确定性迁移为分层策略，支持紧凑二进制与 JSONL 两种等价格式，使用 SQLite 保存版本快照、迁移批次、派生结构、引用关系与意图日志，并提供 HTTP API 和可重复演示脚本。

当前环境验证：`go version go1.27.4 linux/amd64`，满足 Go 1.22+；SQLite 使用纯 Go 驱动 `modernc.org/sqlite`。

## 快速开始

```bash
go mod tidy
go test ./...
go vet ./...
./scripts/demo.sh
```

演示脚本会在本地回环地址启动服务、初始化配额、迁移旧策略、分别导入 JSONL/二进制、提交增量、验证并发 `E_STALE_BASE`，最后以 `POLICY_FAILPOINT=mid_rebuild` 强制终止进程并重启查询恢复报告。默认尝试 `18082` 且被占用时自动选择空闲回环端口；也可用 `PORT=...` 强制指定。

单独启动服务：

```bash
go run ./cmd/policyd -addr 127.0.0.1:18082 -db data/policies.db
```

演示固定流程的版本号：

| 步骤 | 快照版本 |
|---|---:|
| 写入配额组 | 1 |
| 旧版策略迁移 | 2 |
| JSONL 全量导入 | 3 |
| 二进制全量导入 | 4 |
| 权重增量提交 | 5 |
| 并发同基线增量 | 6（一胜一败） |
| 故障点终止并恢复 | 保持 6 |

## 目录

- `cmd/policyd`：HTTP 服务，支持 `POLICY_FAILPOINT=after_intent|mid_rebuild`。
- `cmd/samplegen`：从 JSONL 生成确定性二进制样本。
- `internal/policy`：模型、校验、双格式编解码、兼容矩阵、迁移裁决、副本推导与定点加权指数。
- `internal/store`：SQLite 快照事务、意图日志、级联重建、恢复报告。
- `internal/httpapi`：`net/http` API 与统一业务码错误体。
- `samples/`：固定配额、旧版、JSONL、二进制、歧义迁移、高版本及增量样本。
- `docs/design.md`：格式、公式、事务与恢复状态机、表结构。

## 核心语义

### 新版副本推导

固定规则（整数运算，无浮点）：

```text
concurrency_need = max(ceil(target_concurrency / 64),
                       ceil(target_concurrency * 100 / (64 * latency_budget_ms)))
bounded          = clamp(concurrency_need, min_replicas, max_replicas)
resource_cap     = min(quota.vram_bytes / vram_bytes_per_replica,
                       quota.compute_milli / compute_milli_per_replica)
desired          = min(bounded, resource_cap)
```

原生新版导入要求配额组存在且容量至少能支撑 `min_replicas`，否则返回 `E_SPEC_INVALID`。放置索引按服务标识 FNV-1a 哈希与副本序号确定性生成，不使用随机数。

### 旧版迁移

旧版没有弹性规则和跨服务权重，因此按显式降级规则补齐：

- `weight_permille = 1000`；
- `target_concurrency = fixed_replicas * 64`，`latency_budget_ms = 100`；
- `elastic_enabled=false` 时 `min=max=fixed_replicas`；
- `elastic_enabled=true` 时 `min=max(1, fixed-floor(fixed*10%))`、`max=fixed+ceil(fixed*20%)`；
- `deprecated_fields.quota_group_ref` 映射为配额引用，缺省为 `q-default`；
- `deprecated_fields.placement_zone` 映射为放置区；`owner`/`legacy_note` 为已知可丢弃字段；
- 未声明废弃字段、缺失配额组或迁移预算不自洽时，整批返回 `E_MIGRATION_AMBIGUOUS`，响应仍带 `fixed_replicas/expected_replicas/difference/allowed_difference` 数值明细。

默认自洽容差为相对 50‰（5%，向上取整）加绝对 0 副本，可由导入参数覆盖。逐服务副本核对之外，迁移计划还用默认权重计算旧固定副本均值与新期望副本均值，输出 `weighted_index_equivalence` 做全局定点容差核对。所有容差判断使用整数比较。

### 跨服务加权指数

指数是“期望副本数的千分比权重加权平均”，以 `1_000_000` 为定点尺度：

```text
index_exact = sum(desired_i * weight_i) / sum(weight_i)
index_micros = round_half_up(index_exact * 1_000_000)
```

每个服务输出精确分子、分母、该步取整方式（`exact`/`floor`/`half_up`）和舍入误差分子；采用精确整数残差补偿，输出 `compensated_adjustment_micros` 与累计舍入误差分子/分母，不使用浮点。阈值比较使用绝对/相对容差：

```text
tolerance_micros = ceil(threshold_micros * relative_tolerance_permille / 1000)
exceeded         = index_micros - threshold_micros > tolerance_micros
```

差值在容差内视为未越界；服务始终按服务标识字典序处理并输出打破平局顺序。

## 兼容矩阵

| 输入 | 版本 | 结论 | 行为 |
|---|---:|---|---|
| `legacy_json` | 0 | `degrade_accept` | 推导上下界、补权重、映射白名单废弃字段 |
| `jsonl` | 1 | `accept` | 原生分层 v1 |
| `binary` | 1 | `accept` | 原生分层 v1，小端或大端 |
| `jsonl` | 2 | `reject` | `E_FORMAT_UNSUPPORTED` |
| `binary` | 2 | `reject` | `E_FORMAT_UNSUPPORTED` |
| `legacy_json` | 1 | `reject` | 无声明语义 |

格式协商、迁移裁决、导入审计和生成快照位于同一 `base_version` 事务上下文中，响应与审计都记录目标快照版本。

## 二进制格式 v1

头部 10 字节：

| 偏移 | 长度 | 内容 |
|---:|---:|---|
| 0 | 4 | 魔数 `RSPB` |
| 4 | 1 | 格式版本 `0x01` |
| 5 | 1 | 端序 `L` 或 `B` |
| 6 | 4 | 记录数 |

每条记录：

| 字段 | 说明 |
|---|---|
| type `uint8` | `0x01` 为策略；其他值返回 `E_RECORD_CORRUPT` 与记录序号 |
| presence `uint16` | 可选字段位图，低 4 位分别为 VRAM、算力、弹性标志、放置区 |
| length `uint32` | payload 长度，越界返回 `E_RECORD_CORRUPT` |
| payload | 服务标识、副本下/上界、目标并发、延迟预算、配额引用、权重及位图对应可选字段 |
| crc32 `uint32` | IEEE CRC32，覆盖 type、presence、length 和 payload |

JSONL 每行为一个完整策略，含 `format/format_version/record_type`，未知记录类型返回 `E_RECORD_CORRUPT`，未知字段不静默忽略。

## HTTP API

| 方法与路径 | 说明 |
|---|---|
| `GET /healthz` | 健康检查 |
| `GET /v1/compatibility` | 显式兼容矩阵与默认参数 |
| `POST /v1/import?base_version=N` | 原始 JSONL/二进制导入，支持 `format=auto|jsonl|binary`、`mode=replace|merge` |
| `POST /v1/migrate?base_version=N` | 原始旧版 JSON 迁移 |
| `POST /v1/quota-groups?base_version=N` | 原子替换配额组 |
| `PUT /v1/quota-groups/{id}?base_version=N` | 更新一个配额组并级联重建引用服务 |
| `POST /v1/incremental` | 携带 `base_version` 的增量片段 |
| `GET /v1/state` | 当前策略、派生副本、放置、引用、重建事件与加权指数 |
| `GET /v1/recovery-report` | 最近一次启动恢复报告 |

错误统一为：

```json
{
  "error": {
    "code": "E_RECORD_CORRUPT",
    "message": "record CRC32 mismatch",
    "record_index": 3,
    "details": {}
  }
}
```

业务码：`E_SPEC_INVALID`、`E_RECORD_CORRUPT`、`E_FORMAT_UNSUPPORTED`、`E_MIGRATION_AMBIGUOUS`、`E_STALE_BASE`、`E_NOT_FOUND`、`E_INTERNAL`。

## 原子性与恢复

每个写操作先在独立自动提交事务中持久化 `pending` 意图（迁移还包含 `pending` 批次），随后在一个 SQLite 事务内完成：

1. 校验 `base_version == current_version`；
2. 读取同一基快照的策略与配额；
3. 解析/迁移/补丁并校验整批；
4. 写入新快照、策略、配额、派生服务、放置、引用和加权指数；
5. 更新 `state.current_version` 并把意图标为 `committed`。

任一步失败会回滚，并在回滚后把意图和迁移批次标为 `aborted`。进程在事务中被强制终止时只留下 `pending` 意图；重启扫描后标记为 `recovered_aborted`、迁移批次标记 `aborted`，并把当前指针校正到最后一个 `complete` 快照。恢复报告只汇总 SQLite 已持久化事实。

可用故障点：

```bash
POLICY_FAILPOINT=after_intent go run ./cmd/policyd ...   # 意图已落库，重建尚未开始
POLICY_FAILPOINT=mid_rebuild go run ./cmd/policyd ...    # 快照行已写、状态未切换，进程退出 86
```

## 实际验证结果

`go test ./...`：

```text
ok  example.com/replicabudget/internal/httpapi
ok  example.com/replicabudget/internal/policy
ok  example.com/replicabudget/internal/store
```

`go vet ./...` 与 `go test -race ./...` 均通过。

`./scripts/demo.sh` 实际输出关键结果：

```text
dual-format index: 4833333 (jsonl == binary)
migration: accepted; degraded weight=1000; ambiguous numeric mismatch=4 replicas
cascade: svc-b weight changed; index 5222222 > threshold
concurrency: one success, one E_STALE_BASE (codes 200/409)
crash: curl_exit=52, process_exit=86, committed version stayed 6
recovery: action=aborted_unfinished_intents, intent=incremental, last_complete=6
final version: 6; policies: 4/6/3 replicas; references: 12
```

关键验收点均有自动化测试覆盖：双格式语义一致、大端/小端往返、CRC/长度/记录类型损坏定位、迁移降级与歧义数值、并发同基线一胜一败、级联重建、回滚、恢复报告、统一 HTTP 错误体。
