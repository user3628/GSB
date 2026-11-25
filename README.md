# qosshaper — LAN QoS 策略下发与整形诊断工作台

这是一个可直接在本机运行的独立 Go 服务。它维护带父子依赖的 QoS 整形队列，接受整份策略快照和增量片段，在同一 SQLite 事务内级联重建有效速率、分层令牌桶视图与整形一致度评分，并对确定性离线流量执行整数令牌桶仿真。

- 仅使用 Go 标准库与纯 Go 的 `modernc.org/sqlite`，不需要 Docker、外部服务或 CGO。
- 默认监听 `127.0.0.1`；端口被占用时自动退回到系统分配的可用本地端口。
- SQLite 位于工作区 `data/qosshaper.db`，首次启动自动建表。
- 固定种子样本写入 `data/policy_sample.json`、`data/traffic_sample.jsonl`、`data/sample_meta.json` 与 `data/seed.txt`。

## 环境与启动

要求 Go 1.22 或更高版本。Windows 10/11 与 Windows Server 均可运行。

```powershell
go version
go mod tidy
go test ./...
go run ./cmd/qosshaper -addr 127.0.0.1:18080 -workdir .
```

启动时 stdout 会打印实际监听地址：

```text
LISTEN=http://127.0.0.1:18080
DB=C:\work\data\qosshaper.db
SAMPLE_SEED=20261125
SAMPLE_POLICY=C:\work\data\policy_sample.json
SAMPLE_TRAFFIC=C:\work\data\traffic_sample.jsonl
```

`-addr 127.0.0.1:0` 可让操作系统选择空闲端口。`-seed` 修改固定样本种子；默认种子是 `20261125`。

## 一键演示

```powershell
go run ./cmd/demo
# 或者
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

演示程序编译并启动真实 HTTP 子进程，在隔离工作区 `data/demo-runtime` 中依次执行：

1. 加载固定样本的完整快照（版本 1）。
2. 触发 432 包的确定性仿真并打印业务码、版本与整形结论。
3. 只提交 `video` 队列的 CIR 下调（版本 2），验证 `video-rtp` 级联继承，而 `voice` 的 `calculation_version` 仍为 1、`valid_through_version` 为 2。
4. 用旧观察版本读取仿真诊断，得到 HTTP 409 与 `E_STALE_READ`。
5. 用环境变量故障注入让服务在 SQLite 事务中暂停，演示程序从外部执行 `Process.Kill()`；重启后查询恢复报告，确认未完成提交标记为 `ABORTED`，当前版本仍为 2，崩溃提交中的新 CIR 不可见。

演示输出的实际结果摘要见 [docs/DESIGN.md](docs/DESIGN.md#实际验证记录)。

## 核心接口

所有响应均为 JSON。失败响应形如：

```json
{
  "code": "E_STALE_READ",
  "message": "the observed version is no longer a complete readable version",
  "context": {
    "observed_version": 1,
    "view_version": 2,
    "current_version": 2,
    "reason": "observed_version_replaced"
  }
}
```

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/healthz` | 当前完整版本 |
| `POST` | `/v1/policy/snapshot` | 提交整份策略快照 |
| `POST` | `/v1/policy/increment` | 提交增量片段 |
| `GET` | `/v1/policy/queues?observe_version=N` | 原子读取全部队列派生视图 |
| `GET` | `/v1/policy/queues/{queue_id}?observe_version=N` | 队列声明、有效速率、令牌桶视图、一致度 |
| `POST` | `/v1/simulations` | 在指定完整版本上触发仿真 |
| `GET` | `/v1/simulations/{simulation_id}?observe_version=N` | 仿真汇总 |
| `GET` | `/v1/simulations/{simulation_id}/queues/{queue_id}?observe_version=N` | 单队列诊断、每包令牌与舍入明细 |
| `GET` | `/v1/diagnostics/queues/{queue_id}?observe_version=N` | 读取该版本最近一次队列诊断 |
| `GET` | `/v1/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/v1/sample` | 固定样本元数据 |
| `GET` | `/v1/requests/{request_id}` | 幂等请求的最终状态 |
| `GET` | `/v1/commits` | 已提交版本审计记录 |

### 快照

```json
{
  "request_id": "snapshot-001",
  "version": 1,
  "queues": [
    {
      "queue_id": "wan",
      "cir_bps": 100000123,
      "pir_bps": 100000123,
      "cbs_bytes": 64000,
      "pbs_bytes": 128000,
      "priority": 1,
      "match": {"src_port_min": 0, "src_port_max": 65535, "dscp_values": []},
      "parent_id": null
    },
    {
      "queue_id": "video",
      "cir_bps": 20000377,
      "pir_bps": 30000377,
      "cbs_bytes": 32000,
      "pbs_bytes": 64000,
      "priority": 5,
      "match": {"src_port_min": 2000, "src_port_max": 4000, "dscp_values": [34, 46]},
      "parent_id": "wan"
    }
  ]
}
```

规则：

- `queue_id` 唯一；CIR/PIR/CBS/PBS 均为正；`CBS <= PBS`、`CIR <= PIR`。
- `parent_id` 必须引用同一快照中的队列；依赖图不得有环。
- 快照版本必须严格大于当前版本，否则 `E_STALE_SNAPSHOT`。
- 任意校验失败返回 `E_POLICY_INVALID`；环返回 `E_DEPENDENCY_CYCLE`，并附 `cycle` 与去重后的 `queue_ids`。
- 校验在事务外完成，失败不会改变已加载队列、版本或派生结构。

### 增量

```json
{
  "request_id": "increment-parent-cir",
  "base_version": 1,
  "queue_patches": [
    {"queue_id": "video", "cir_bps": 1234567}
  ],
  "parent_changes": [
    {"queue_id": "video-rtp", "parent_id": "video"}
  ],
  "delete_queue_ids": []
}
```

- `base_version` 必须等于当前版本，否则 `E_STALE_BASE`，且不提交任何状态。
- 同一基础版本的并发增量由进程内单写者串行化；只有一个成功，另一个得到 `E_STALE_BASE`。
- 新队列的 patch 必须给出 `cir_bps`、`pir_bps`、`cbs_bytes`、`pbs_bytes`、`priority`、`match` 全部字段。
- `parent_changes` 中 `parent_id: null` 表示改为根队列。
- 删除父队列会级联删除其子树；若删除后策略为空，返回 `E_POLICY_INVALID`。
- `request_id` 是全局幂等键。同一 ID 重试返回第一次的响应，不重复执行。

### 仿真

```json
{
  "request_id": "simulation-at-v2",
  "policy_version": 2,
  "queue_ids": ["video", "video-rtp"]
}
```

`queue_ids` 为空时仿真策略内全部队列。仿真使用启动时生成的固定流量文件；相同策略版本和相同样本重复运行，逐包结果逐位一致。单队列诊断响应包含：

- 最终承诺/峰值令牌余量、发送字节、丢弃字节、整形结论；
- 每包的公开令牌、峰值令牌、颜色、发送/丢弃字节；
- 每次补充的 `raw_millibits`、`whole_bits`、`carry_after_millibits`、容量溢出与文字说明；
- 队列级累计舍入余数。

`observe_version` 必须仍等于当前完整版本。旧版本观察不会被悄悄读取，而是返回 `E_STALE_READ`，上下文包含 `observed_version`、`view_version`、`current_version`。未受影响队列可以保留旧的 `calculation_version`，只要其 `valid_through_version >= observed_version`，返回时同时标注两者。

## 业务码

| 业务码 | 典型 HTTP | 含义 |
|---|---:|---|
| `E_POLICY_INVALID` | 422 | 模型、字段、父引用或空策略非法 |
| `E_DEPENDENCY_CYCLE` | 422 | 父子依赖有环，附环路径 |
| `E_STALE_SNAPSHOT` | 409 | 快照版本不大于当前版本 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前版本 |
| `E_STALE_READ` | 409 | 观察版本已被替换或派生视图滞后 |
| `E_NO_POLICY` | 404 | 尚未提交策略 |
| `E_NOT_FOUND` | 404 | 队列、仿真或请求不存在 |
| `E_SIMULATION_FAILED` | 422 | 样本/仿真数值契约失败 |
| `E_REQUEST_CONFLICT` | 409 | 请求 ID 被其他操作占用或仍挂起 |
| `E_ABORTED` | 409 | 未完成写入已被崩溃恢复中止 |
| `E_INTERNAL` | 500 | 本地存储或内部错误 |

## 一致性、恢复与故障注入

- SQLite 使用 WAL、`synchronous=FULL`、busy timeout 和多读连接；所有提交、级联重建、仿真持久化由单个写者互斥。
- 每次提交先把请求写入 `intents` 表，再开启主事务。策略行、全部受影响派生视图、提交记录、`current_version` 与意图完成状态在同一个事务内提交。
- 读者只使用只读事务。在一个请求中，版本、视图、有效速率和一致度取自同一数据库快照，不会看到父队列新、子队列旧的混合状态。
- 启动时扫描 `intents(status='pending')`，清理未来版本的派生/仿真残留，把不完整请求改为 `aborted`，并把回滚版本写入 `recovery_runs`。
- 故障注入仅用于本地演示/测试：
  - `QOS_FAILPOINT_STAGE=derived|simulation`
  - `QOS_FAILPOINT_FILE=<ready 文件路径>`
  - `QOS_FAILPOINT_SLEEP_MS=30000`

失败注入点位于未提交事务内部；外部终止进程后，SQLite 会回滚整个事务。重启服务时恢复报告会给出请求、阶段、回滚版本、派生与令牌状态回滚标志。

## 验证

```powershell
go test ./... -count=1
go test -race ./internal/qos ./internal/httpapi -count=1
go vet ./...
go run ./cmd/demo
```

测试覆盖非法快照/增量不改变状态、环检测、同基础版本并发、幂等请求、仅子树级联重建、未受影响队列不重算、暂停事务时的原子读、旧版本/滞后视图裁决、整数补充余数、三色标记、重复仿真逐位一致，以及启动恢复扫描。数值契约与实测输出见 [docs/DESIGN.md](docs/DESIGN.md)。
