# 多集群配置分发协调器

这是一个可直接嵌入其他 Python 服务的配置分发领域组件，同时提供本地 FastAPI 适配器和完整端到端演示。实现只依赖 Python 3.11+、FastAPI、uvicorn、httpx、pydantic 与 SQLite，不需要账号、容器或在线服务。

核心保证：

- 全量快照与增量片段共享严格单调递增的版本序列。
- 增量使用 `base_version` 做乐观并发控制；同一基础版本并发提交只有一个能成功。
- 影响分析区分直接配置消费者与下游组件，输出确定性的拓扑批次；无关组件不会进入影响集合。
- 每个组件的期望配置摘要、跨集群一致度评分、分发批次索引都在同一 SQLite 事务内失效并重建。
- 所有业务写操作先提交 `PENDING` 意图记录，再执行原子业务事务；正常完成后在同一事务里标记 `COMPLETE`。
- 进程在重建或推送中途被 `os._exit` 强制终止后，可通过 `recover()` 基于持久化事实恢复到最后一个完整版本或中止分发。
- HTTP 超时使用真实的 connect/read/write/pool 截止时间，超时后显式关闭客户端 socket。

## 目录

```text
config_coordinator/
  api.py                 FastAPI 适配器与统一错误体
  coordinator.py         版本提交、影响分析、分发、回滚、恢复
  derived.py             派生摘要、一致度、批次索引与失效闭包
  endpoint_server.py     本地多集群 HTTP 测试端点
  models.py              Pydantic 领域模型
  push.py                httpx 推送与失败分类
  state_machine.py       组件状态机
  store.py               SQLite schema、事务、意图、分发运行
  topology.py            依赖环检测与拓扑批次
  crash_worker.py        可重复的强制终止探针
samples/                 固定快照、增量与七组件声明
scripts/demo.py          端到端演示入口
scripts/run_api.py       本地启动协调器 API
scripts/run_endpoint_server.py
tests/                   16 项自动化验收测试
VALIDATION.md            实际执行结果
```

## Windows 安装与运行

在项目根目录打开 PowerShell：

```powershell
py -3.11 --version
py -3.11 -m pip install -r requirements.txt
py -3.11 scripts\demo.py
py -3.11 -m pytest -q
```

也可以使用 `python` 代替 `py -3.11`。演示会自动选择一个本机空闲端口、在子进程中启动本地测试端点，不访问外部网络。

单独启动协调器 API：

```powershell
$env:CONFIG_COORDINATOR_DB="artifacts\coordinator.db"
py -3.11 scripts\run_api.py --port 18080
```

单独启动本地端点服务：

```powershell
py -3.11 scripts\run_endpoint_server.py --port 18765
```

## 固定样本

- `samples/snapshot-v1.json`：基线快照，版本 1，六个配置键。
- `samples/snapshot-v2.json`：另一份全量快照，版本 2，用于共享版本序列与全量校验。
- `samples/increment-feature-checkout.json`：只修改 `feature.checkout` 一个键，基础版本 1。
- `samples/increment-push-failure.json`：故障/超时处置场景，基础版本 2。
- `samples/increment-crash.json`：恢复后重试场景，基础版本 3。
- `samples/components.json`：三集群、七组件声明，包含 `{endpoint_base}` 占位符。

七组件为：

- `east/west/south-gateway`：直接依赖 `app.timeout`、`feature.checkout`、`feature.search`。
- `east/west/south-orders`：直接依赖 `db.pool`、`feature.checkout`，并分别依赖同集群 gateway，因此是 gateway 的下游。
- `east-audit`：只依赖 `logging.level`，用于验证无关组件不会被错误纳入。

快照摘要使用规范 JSON 计算，算法见 `config_coordinator.codec.compute_snapshot_checksum`，形如 `sha256:<hex>`。摘要覆盖 `version`、原样提供的 ISO-8601 `generated_at` 与 `configs`。

## 领域语义

### 全量快照

```http
POST /snapshots
Content-Type: application/json

{
  "snapshot": {
    "version": 1,
    "generated_at": "2026-11-20T08:00:00Z",
    "configs": {"a": 1},
    "checksum": "sha256:..."
  },
  "components": [
    {
      "component_id": "svc",
      "config_keys": ["a"],
      "expected_config_version": 1,
      "endpoint_url": "http://127.0.0.1:18765/clusters/east/push",
      "cluster_id": "east",
      "logical_component": "svc",
      "depends_on": []
    }
  ]
}
```

- `version <= current_version`：`E_STALE_SNAPSHOT`。
- 摘要不匹配：`E_SNAPSHOT_INVALID`。
- 校验和失败与过期失败都不写入快照、版本指针、组件状态或派生结果。

### 增量片段

```http
POST /increments

{
  "base_version": 1,
  "generated_at": "2026-11-22T09:00:00Z",
  "config_set": {"feature.checkout": false},
  "config_delete": [],
  "components_upsert": [],
  "components_remove": [],
  "reason": "single-key change"
}
```

- 成功版本固定为 `base_version + 1`，和全量快照使用同一版本空间。
- `base_version != current_version`：`E_STALE_BASE`，状态不变。
- 两个并发增量都基于同一版本时，SQLite `BEGIN IMMEDIATE` 串行化比较与提交；后到者收到 `E_STALE_BASE`。
- 片段可同时更新配置键与组件声明；全部改动、派生重建与版本指针在同一事务提交。

### 影响分析与批次

`GET /impact/{version}`、`GET /plans/{version}` 和分发入口使用同一分析器：

1. 配置键实际值变化或组件声明变化形成初始变更引用。
2. 引用该配置键的组件、自身声明变化的组件为直接影响。
3. 沿 `depends_on` 反向遍历，加入全部下游组件。
4. 只对影响集合做确定性拓扑排序；集合外依赖视为已满足。
5. 完整依赖图出现环时返回 `E_DEPENDENCY_CYCLE`，`details.cycle` 给出闭环路径，`details.cycle_ids` 给出环上标识。

组件声明先可被提交保存，但影响查询、计划查询和分发都会拒绝环；分发在创建 run 或修改状态前即失败。

### 组件状态机

```text
PENDING      -> DISPATCHED, FAILED
DISPATCHED   -> APPLIED, FAILED, ROLLED_BACK
APPLIED      -> ROLLED_BACK, PENDING
FAILED       -> ROLLED_BACK, PENDING
ROLLED_BACK  -> PENDING
```

`DISPATCHED -> ROLLED_BACK` 用于恢复时把未确认推送标记为中止。`* -> PENDING` 只用于新快照使组件重新进入待分发状态。非法迁移返回 `E_INVALID_TRANSITION`，组件状态与事件均保持不变。每次迁移写入 `state_events`，包含 `from_status`、`to_status`、`reason` 和 `snapshot_version`。

### 派生结果与引用

每个组件在每个已提交版本上维护三种产物：

- `expected_digest:<component>`：组件依赖配置与期望配置版本的规范摘要。摘要不含组件标识，因此同逻辑组件可跨集群比较。引用 `config:<key>` 与 `component:<id>`。
- `consistency_score:<component>`：同 `logical_component` 跨集群组成员中，`last_applied_digest == expected_digest` 的比例。引用组内组件、期望摘要产物与状态。
- `batch_index:<component>`：分发拓扑批次。引用组件声明、自身期望摘要和依赖组件。

变更引用通过 `artifact:` 引用继续传播到一致度和批次产物，形成失效闭包。新版本的所有产物在同一个 `BEGIN IMMEDIATE` 事务中写入；读取事务先固定 `current_version`，再读取该版本快照、状态与全部产物，因此不会看到“新摘要 + 旧一致度”的中间状态。

### 分发、推送与回滚

```http
POST /dispatches

{
  "snapshot_version": 1,
  "policy": "ROLLBACK",
  "timeout_seconds": 0.2
}
```

- 每个快照版本最多一个 `dispatch_runs` 记录；并发触发者等待或复用已完成结果，重复触发返回 `reused: true`，不会再次向端点推送。
- 组件严格按批次顺序串行推进；批次内按组件标识稳定排序。
- 推送体包含 `snapshot_version`、配置子集、期望摘要与确定性 `request_id`。
- 端点 2xx 记为 `APPLIED`；业务拒绝记为 `E_PUSH_REJECTED`；连接错误记为 `E_PUSH_CONNECTION`；真实 HTTP 超时记为 `E_PUSH_TIMEOUT`；409/冲突幂等键记为 `E_CONFLICTING_PUSH`。
- `CONTINUE`：失败组件 `FAILED`，其余继续。
- `ROLLBACK`：继续收集失败事实，结束后逆序回滚本轮所有 `APPLIED` 组件，结论为 `ROLLED_BACK` 或 `ROLLBACK_INCOMPLETE`。

失败组件同时带一个统一错误对象：

```json
{
  "code": "E_PUSH_TIMEOUT",
  "message": "push exceeded 0.2s and the client socket was closed",
  "details": {"component_id": "west-gateway", "http_status": null}
}
```

### 意图与恢复

业务写路径统一为：

1. 独立事务提交 `PENDING` 意图记录。
2. `BEGIN IMMEDIATE` 执行业务写事务。
3. 在同一业务事务中发布所有业务行并标记意图 `COMPLETE`。
4. 普通异常回滚业务事务并把意图标记为 `ABORTED`。
5. 强制终止不会执行第 2/3 步的 COMMIT，SQLite 回滚未提交页面，但第 1 步的 `PENDING` 意图已持久化。

`Coordinator.recover()` 与 FastAPI 启动生命周期会扫描：

- 未完成 `SUBMIT_*`：标记中止，最后完整版本保持不变。
- 未完成 `PUSH` 且状态停在 `DISPATCHED`：迁移到 `ROLLED_BACK`，原因 `RECOVERED_UNCONFIRMED_PUSH`。
- 未完成 `ROLLBACK` 且状态为 `APPLIED`：迁移到 `ROLLED_BACK`。
- 仍为 `RUNNING` 的分发：标记为 `ABORTED`，结论 `ABORTED_PROCESS_TERMINATION`。

恢复报告只读取已提交的意图、run、状态和事件行，不猜测未落盘结果。

## API 摘要

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/health` | 健康与当前版本 |
| `POST` | `/snapshots` | 提交全量快照与组件声明 |
| `POST` | `/increments` | 提交增量片段 |
| `GET` | `/state?version=` | 某一完整版本的快照、状态、事件、派生结果 |
| `GET` | `/snapshots/{version}/derived` | 派生结果与重建范围 |
| `GET` | `/impact/{version}` | 影响分析与失效闭包 |
| `GET` | `/plans/{version}` | 拓扑批次计划 |
| `POST` | `/dispatches` | 单写者分发 |
| `GET` | `/dispatches/{version}` | 分发运行与结果 |
| `GET` | `/components/{id}` | 组件状态与轨迹 |
| `POST` | `/components/{id}/transitions` | 人工状态迁移 |
| `POST` | `/recovery` | 扫描未完成意图/分发 |
| `GET` | `/errors` | 错误码目录 |

业务错误统一为：

```json
{
  "code": "E_STALE_BASE",
  "message": "increment base version is no longer current",
  "details": {"base_version": 1, "current_version": 2}
}
```

最低错误码集合：`E_STALE_SNAPSHOT`、`E_STALE_BASE`、`E_DEPENDENCY_CYCLE`、`E_INVALID_TRANSITION`、`E_PUSH_TIMEOUT`、`E_CONFLICTING_PUSH`。另含 `E_SNAPSHOT_INVALID`、`E_PUSH_REJECTED`、`E_PUSH_CONNECTION`、`E_DISPATCH_NOT_FOUND` 等。

## 本地端点控制

测试服务支持一个进程模拟多个集群路径：

```text
POST /clusters/{cluster}/push
POST /clusters/{cluster}/rollback
GET  /clusters/{cluster}/state/{component}
POST /control/modes
POST /control/reset
GET  /control/stats
```

模式支持 `success`、`reject`、`timeout`、`conflict`。`timeout` 会真实延迟响应；客户端读超时关闭 socket 后，端点记录 `client_disconnects`。端点还维护请求幂等账本：相同 `request_id` 与相同负载返回重放结果，不同负载返回 `E_CONFLICTING_PUSH`。

## 演示输出

`python scripts/demo.py` 会打印：

- 全量与增量两次受影响组件清单、批次与失效产物类型统计。
- 每次分发逐组件状态、耗时、错误码、回滚结果和完整状态轨迹。
- 拒绝与超时策略处置后的最终结论。
- 强制终止退出码、恢复前后版本、被中止意图、恢复状态迁移和恢复后重试版本。

实际执行结果见 [VALIDATION.md](VALIDATION.md)。

