# Profiling Sampling & Hotspot Service

一个完全本地运行、可交互的剖析采样与热点定位服务。它使用固定种子生成器产出调用栈样本，按策略版本维护分层蓄水池，在服务样本数越过精确上限后显式切换到近似路径，并在 SQLite 中通过单写者事务、WAL 多读者和意图日志提供可恢复的一致提交。

## 环境与依赖

- Go: `go1.27.4 linux/amd64`（项目声明 `go 1.22`，使用 Go 1.22 的 `ServeMux` 路径参数能力）
- SQLite 开发库: `pkg-config --modversion sqlite3` 实测 `3.46.1`
- Go SQLite 驱动: `modernc.org/sqlite v1.34.5`（纯 Go，无需 CGO）
- 运行方式: 本机进程、SQLite 文件、固定种子样本；不使用 Docker、GPU、线上账号或外部数据集

## 快速开始

```bash
go build -o bin/profile-server ./cmd/server
./bin/profile-server -db data/profile.db -addr 127.0.0.1:8080
```

另开终端：

```bash
# 获取内置确定性策略
curl -sS 'http://127.0.0.1:8080/v1/demo/policy?version=1' > /tmp/policy.json
curl -sS -X POST -H 'Content-Type: application/json' --data-binary @/tmp/policy.json \
  http://127.0.0.1:8080/v1/policies

# 生成并摄入一个带固定种子的调用栈样本分片
curl -sS 'http://127.0.0.1:8080/v1/demo/chunk?seed=42&batch_id=demo-1&count=32&duplicate_every=9' > /tmp/chunk.json
curl -sS -X POST -H 'Content-Type: application/json' --data-binary @/tmp/chunk.json \
  http://127.0.0.1:8080/v1/samples

# 同一只读事务内查询蓄水池与热点结论
curl -sS 'http://127.0.0.1:8080/v1/versions/1/snapshot?limit=10'
```

## 一键演示

```bash
./scripts/demo.sh
```

脚本会依次完成：

1. 提交策略 v1，摄入小批次，确认 `checkout` 走 `exact`。
2. 摄入大批次，确认 `checkout` 切换为 `approximate`，逐帧误差均不超过偏差上界。
3. 提交策略增量 v2，确认蓄水池、热点和引用在同一事务边界内原子重建。
4. 在 `PROFILE_CRASH_STAGE=during_reservoir_replace` 下于蓄水池替换事务中途强制退出。
5. 重启后验证恢复报告、提交序号未前进、未提交批次不可见；重试后才推进提交。

实测输出见 [docs/VERIFICATION.md](docs/VERIFICATION.md)。

## HTTP API

所有错误响应统一为：

```json
{
  "code": "E_SAMPLE_INVALID",
  "message": "sample weight must be a positive integer",
  "context": {"field": "samples[2].weight", "value": 0},
  "request_id": "req-..."
}
```

| Method | Path | 作用 |
|---|---|---|
| `GET` | `/healthz` | 健康与当前提交坐标 |
| `GET` | `/v1/current` | 当前策略与提交坐标 |
| `POST` | `/v1/policies` | 整体替换策略快照 |
| `POST` | `/v1/policies/increments` | 基于当前版本提交增量，自动生成 `base+1` |
| `GET` | `/v1/policies/{version}` | 查询策略快照 |
| `GET` | `/v1/policies/{version}/rebuild` | 查询级联重建报告 |
| `POST` | `/v1/samples` | 摄入样本分片（批次级幂等） |
| `GET` | `/v1/batches/{batch_id}` | 查询已提交批次 |
| `GET` | `/v1/versions/{version}/reservoirs` | 查询该策略版本的蓄水池 |
| `GET` | `/v1/versions/{version}/hotspots` | 查询热点、偏差上界、置信区间与逐帧精确参照 |
| `GET` | `/v1/versions/{version}/differences` | 查询精确/近似逐帧差异 |
| `GET` | `/v1/versions/{version}/modes` | 查询精确/近似路径选择与依据 |
| `GET` | `/v1/versions/{version}/snapshot` | 单只读事务同时返回蓄水池与热点 |
| `GET` | `/v1/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/v1/demo/policy?version=1` | 内置确定性策略 |
| `GET` | `/v1/demo/chunk?...` | 内置固定种子样本分片 |

查询参数：

- `service`、`channel`：过滤服务或渠道。
- `limit`：热点查询每服务返回的帧数；`0` 表示全部。
- `commit_seq`：要求返回的快照仍处于指定完整提交；不匹配时返回 `E_NOT_FOUND`，防止跨提交拼接。

业务错误码：

- `E_SAMPLE_INVALID`：样本/策略解析或约束校验失败，整批不入库。
- `E_STALE_SNAPSHOT`：完整快照版本不大于当前版本。
- `E_STALE_BASE`：增量基础版本或样本携带的策略版本不是当前版本。
- `E_SAMPLE_CONFLICT`：相同样本标识对应不同内容。
- `E_CONFLICTING_REQUEST`：同一批次标识携带不同载荷，或增量版本声明冲突。

## 输入模型

样本分片：

```json
{
  "batch_id": "batch-1",
  "policy_version": 1,
  "samples": [
    {
      "id": "sample-1",
      "timestamp_ms": 1700000000000,
      "service": "checkout",
      "frames": [
        {"id": "http.server", "name": "http.server", "file": "server.go", "line": 41},
        "checkout.handler",
        "pricing.calculate"
      ],
      "weight": 7,
      "channel": "cpu"
    }
  ]
}
```

`frames` 支持字符串简写或结构化帧对象。样本内容标识为 `timestamp_ms + service + frames + weight`；`channel` 是传输/来源标记，不参与冲突哈希，因此同一样本跨渠道重复到达会去重并累计重复次数。

策略快照：

```json
{
  "version": 1,
  "generated_at": "2026-11-26T00:00:00Z",
  "channels": {
    "cpu": {"target_permille": 250, "bias_correction": 4.0},
    "wall": {"target_permille": 1000, "bias_correction": 1.0}
  },
  "services": {
    "checkout": {"reservoir_capacity": 8, "exact_limit": 4, "bias_correction": 1.1},
    "search": {"reservoir_capacity": 12, "exact_limit": 20, "bias_correction": 1.0}
  },
  "stratification": {"depth_prefixes": [1, 3], "separator": ">"},
  "default_reservoir_capacity": 12,
  "default_exact_limit": 16,
  "round_time_budget_ms": 200,
  "ranking_relative_tolerance": 0.005,
  "replacement_rule": "weighted-bottom-k-sha256-v1"
}
```

增量示例：

```json
{
  "base_version": 1,
  "services": {
    "checkout": {"reservoir_capacity": 16, "exact_limit": 8}
  },
  "channels": {
    "cpu": {"target_permille": 500}
  }
}
```

## 关键实现

- 分层蓄水池键：`service + 每个配置深度前缀的帧序列`，再与渠道组合；每个服务容量受策略约束。
- 确定性替换：`priority = -ln(U) / weight`，`U` 由 `SHA-256(rule|sample_id|reservoir_id)` 生成；保留最小优先级集合，平局按样本 ID。
- 精确路径：服务唯一去重样本数 `<= exact_limit` 时扫描全量样本，输出精确未加权计数和加权热度。
- 近似路径：超过上限时只从各蓄水池保留集合估计，响应显式标记 `mode/used_reservoir/approximation_basis`，并附带全量精确参照和逐帧差异。
- 偏差上界：`1 - selected_weighted_total / population_weighted_total`，对每个帧的份额误差成立；置信区间由该确定性保留质量界对称展开，覆盖率为保守下界。
- 热点排序：份额先转 1e12 定点整数；相对容差使用 ppm 整数比较；容差内形成 tie group，组内按 `frame_id` 升序，结论返回完整 top tie 集合。
- 提交边界：策略提交、样本摄入、蓄水池替换、热点重建、批次记录和 `state_meta` 更新处于同一 SQLite 写事务。
- 恢复：写事务前先单独提交意图；同一事务内将意图标记为 committed。启动时扫描 pending 意图，按目标批次/策略是否存在判定是否已提交，并清理大于最后完整 `commit_seq` 的孤儿行。

更完整的算法证明、状态机和一致性说明见 [docs/DESIGN.md](docs/DESIGN.md)，请求/响应示例见 [docs/API.md](docs/API.md)。

## 测试

```bash
go test ./...
go test -race ./...
```

测试覆盖：输入归一化与非法整批回滚、跨渠道去重、冲突原子性、蓄水池容量/替换轨迹、精确到近似切换、偏差上界与逐帧差异、容差排序、同基础版本并发增量、级联原子重建、批次幂等、并发一致快照，以及真实子进程 `SIGKILL/exit(137)` 后的恢复与重试。
