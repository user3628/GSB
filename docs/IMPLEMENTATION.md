# 弹性伸缩平台实现说明

本目录是评审后需求的可运行本地实现。服务仅依赖 Go 标准库 `database/sql` 与纯 Go SQLite 驱动 `modernc.org/sqlite`，不需要 CGO、Docker、外部监控或外网。

## 运行

```bash
go version          # 需要 Go 1.22+
./scripts/smoke.sh  # 生成固定种子样本，检查工作目录、SQLite、回环端口
./scripts/demo.sh   # 完整演示：加载、单服务增量、全局增量、陈旧读、kill -9 恢复
```

手工启动：

```bash
go run ./cmd/platform generate --out artifacts/seed.json
go run ./cmd/platform serve --addr 127.0.0.1:18099 --db data/platform.db
# 另一个终端
go run ./cmd/platform demo --base-url http://127.0.0.1:18099 --seed artifacts/seed.json
```

如果端口写 `127.0.0.1:0`，可再加 `--port-file /tmp/platform-port`，程序会把系统分配的实际端口写入该文件。

Windows 可使用官方 Go 安装包后直接运行等价的 `go run ./cmd/platform ...` 命令；`scripts/demo.sh` 适用于 Linux/macOS/WSL。

## 代码结构

- `cmd/platform`：CLI，包含 `generate`、`smoke`、`serve`、`demo`、`arm-crash`。
- `internal/model`：服务、订阅、快照、增量、决策、影响集合、恢复报告和业务错误码。
- `internal/seed`：固定种子 `20261125` 的确定性服务与样本生成器。
- `internal/engine`：摘要、拓扑、影响闭包、最近秩 P90、定点风险分、物化指标和伸缩决策。
- `internal/store`：纯 Go SQLite 打开参数与 schema。
- `internal/app`：版本单写者、意图/幂等、事务、级联重建、恢复和读取快照。
- `internal/httpapi`：JSON HTTP 接口与错误状态映射。

## 固定样本

生成 5 个服务、每个服务 2 个实例、每实例 6 条请求数样本和 12 条延迟样本，共 180 条：

- `edge-gateway` 依赖 `auth-svc`、`catalog-svc`
- `auth-svc` 依赖 `session-svc`
- `catalog-svc` 依赖 `inventory-svc`
- `session-svc`、`inventory-svc` 是叶子上游

演示增量：

- 单服务增量只改 `inventory-svc` 的 1 条样本，传播范围为 `inventory-svc -> catalog-svc -> edge-gateway`。
- 全局增量改写全部 180 条样本，实际触及比例超过阈值，自动退回全量重算。
- 崩溃增量使用独立提交号 `inc-crash-global`，只写 pending 意图后等待脚本 `kill -9`。

## 定点数与决策公式

所有分值和比例使用 `int64`，定点刻度：

```text
Scale = 1_000_000
```

自身指标先按采集权重做确定性加权均值：

```text
own_micro = Σ(value_i * collection_weight_i) * Scale / Σ(collection_weight_i)
```

有效指标把自身聚合值和直接依赖的同名指标作为来源；自身来源权重为采集权重之和，上游来源权重为订阅权重，再做整数加权平均。`source_breakdown` 输出每个来源的归一化权重。

P90 使用最近秩法：

```text
rank = ceil(90 * n / 100)
p90  = sorted_latency[rank-1]
```

每个实例输出样本数 `n`、秩、P90、请求总数、采集权重和推导中间值。样本数少于服务策略中的 `min_latency_samples` 时返回 `E_INSUFFICIENT_SAMPLES`。

伸缩推导：

```text
estimated_concurrency_micro = requests * p90_ms * Scale / window_ms
saturation_ratio_micro      = estimated_concurrency_micro / target_concurrency
latency_ratio_micro         = p90_ms * Scale / latency_budget_ms
risk_micro                  = (600000*saturation + 400000*latency) / Scale

concurrency_replicas = ceil(Σ(requests_i * p90_i) / (window_ms * target_concurrency))
latency_replicas     = ceil(instance_count * weighted_latency_ratio_micro / Scale)
raw_replicas         = max(concurrency_replicas, latency_replicas, 1)
desired              = clamp(raw_replicas, min_replicas, max_replicas)
```

响应中的 `formula`、`basis`、两个信号副本数、`raw_replicas` 和 `clamped` 构成完整推导依据。

## 影响集合

`GET /v1/impact/{service}` 同时返回：

- `impact_set`：以该服务为触发源，沿反向依赖 BFS 得到的受影响下游，包含 `hops` 和完整 `path`。
- `upstream_dependencies`：沿正向依赖 BFS 得到的上游依赖及跳数。

例如 `inventory-svc` 的影响集合为 `catalog-svc`（1 跳）和 `edge-gateway`（2 跳）。

## 增量与全量路径

重建运行记录包含：

- `selected_path`：`incremental` 或 `full`
- `threshold_micro`：订阅/样本项阈值，种子策略为 `250000`（25%）
- `propagation_threshold_micro`：传播服务占比阈值，种子策略为 `750000`（75%）
- `touched_*`、`total_*`、`touched_ratio_micro`
- `propagation_cardinality`、`service_count`
- `recompute_scope`、`reason`
- 全局指标与决策的 SHA-256 摘要

组合触及比例为：

```text
touched_ratio = (touched_subscription_items + touched_sample_records)
                * Scale / (total_subscription_items + total_sample_records)
```

出现以下任一情况会全量重建：

- 请求显式 `mode=full`
- 依赖关系变化，影响集合基数可能改变
- 组合触及比例超过声明阈值
- 反向传播服务占比超过传播阈值

同一版本先跑增量再强制全量时，测试和演示都会比较全局 `metrics_checksum`、`decisions_checksum`，应用层测试还逐字节比较规范化 JSON。

## 并发、幂等与恢复

- 所有快照、增量、目录和重建写操作由进程内互斥锁按版本串行化。
- SQLite 开启 WAL、外键、busy timeout 和 `synchronous=FULL`。
- 读取使用只读事务，版本元数据与派生视图在同一个 SQLite 快照中读取，不会跨连接拼接新旧版本。
- 每个新提交先自动提交写入 `intents(status='pending')`，然后在一个事务内更新规范状态、级联重建派生表/引用表，并把意图更新为 `committed`。
- 相同 `commit_id` 和相同请求摘要的并发/重试请求等待首个请求完成并重放 `response_json`；相同 `commit_id` 但请求摘要不同返回 `E_CONFLICTING_REQUEST`。
- 启动时扫描 pending 意图并标记为 `aborted`；若存在未完成意图或物化版本落后，会基于最后完整的 SQLite 规范状态在同一事务内全量重建指标、影响集合、决策和引用。
- `GET /v1/recovery` 返回最近一次只基于 SQLite 落盘记录生成的报告。

## HTTP 接口

写请求可在 JSON 中传 `commit_id`，也可用 `Idempotency-Key` 请求头。

| 方法与路径 | 用途 |
| --- | --- |
| `GET /healthz` | 健康、当前版本、物化版本、DB 可写检查 |
| `GET /v1/services` | 服务目录与策略 |
| `PUT/POST /v1/catalog` | 加载/替换服务、依赖、订阅和伸缩策略 |
| `POST /v1/snapshots` | 加载新版本完整快照并原子替换当前样本/物化结果 |
| `POST /v1/snapshots/replace`、`PUT /v1/snapshots/current` | 全量快照替换入口，校验通过后原子切换 |
| `GET /v1/snapshots/current` | 读取当前完整快照 |
| `POST /v1/increments` | 提交局部样本、订阅或依赖增量 |
| `GET /v1/metrics/{service}?observed_version=N` | 按观察版本读取有效定点指标 |
| `GET /v1/decisions/{service}?observed_version=N` | 读取伸缩决策与推导依据 |
| `GET /v1/impact/{service}?observed_version=N` | 查询影响集合/上游依赖和跳数 |
| `POST /v1/rebuilds` | 显式触发 `full` 或指定范围的 `incremental` 重建 |
| `GET /v1/rebuilds/latest`、`GET /v1/rebuilds/{id}` | 查询重建路径、阈值、比例和范围 |
| `GET /v1/recovery` | 查询最近崩溃恢复报告 |

失败响应格式：

```json
{
  "error": {
    "code": "E_STALE_READ",
    "message": "materialized version is behind the reader observed version",
    "context": {
      "observed_version": 9999,
      "current_version": 3,
      "current_materialized_version": 3
    }
  }
}
```

已覆盖业务码：`E_SNAPSHOT_INVALID`、`E_STALE_BASE`、`E_INSUFFICIENT_SAMPLES`、`E_STALE_READ`、`E_CONFLICTING_REQUEST`，以及 `E_INVALID_REQUEST`、`E_NOT_FOUND`、`E_MODEL_INVALID`、`E_INTERNAL`。

## 实际验证

最终验证输出保存在：

- `artifacts/go-test.log`
- `artifacts/race-test.log`
- `artifacts/final-checks.log`
- `artifacts/demo-verification.log`
- `artifacts/seed.json`

在本次环境中：

- `go version` 为 `go1.27.4 linux/amd64`；`go.mod` 声明 `go 1.22`，SQLite 驱动固定为兼容 Go 1.22 的 `modernc.org/sqlite v1.29.10`。
- `CGO_ENABLED=0 go test ./... -count=1` 全部通过。
- `CGO_ENABLED=1 go test -race ./internal/app ./internal/httpapi -count=1` 通过。
- Windows/amd64 与 macOS/arm64 的 `CGO_ENABLED=0` 交叉编译通过。
- `./scripts/demo.sh` 完成固定种子加载、并发同基版本增量、单服务增量、全局增量、路径摘要一致性、陈旧读、样本不足、冲突请求和 `kill -9` 后恢复。
