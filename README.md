# 本地 Kubernetes 调度预算与亲和性裁决仿真引擎

这是一个面向离线评估的确定性调度模拟器。它读取工作区内的 JSON 节点/Pod 规格，
在全局并发、单轮逻辑时间与单节点单轮放置上限的约束下推进调度轮次，并通过本地
FastAPI 服务和 SQLite 持久化事实输出放置、否决原因、节点余量及强杀恢复报告。

运行时不访问 Kubernetes 集群，也不访问外部网络；样本、数据库与 HTTP 服务均在本地。

## 1. 环境准备（Windows）

目标环境为 Python 3.11 及以上。PowerShell 示例：

```powershell
cd C:\path\to\workspace
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python --version
```

如果 PowerShell 禁止激活脚本，可直接使用：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

确认本地端口可用：

```powershell
python -c "import socket; s=socket.socket(); s.bind(('127.0.0.1', 8000)); print('port 8000 free'); s.close()"
```

无需 Docker、容器运行时或集群访问。SQLite 使用 Python 标准库 `sqlite3`。

## 2. 工作区内容

```text
schedsim/
  quantities.py    CPU 毫核 / 内存字节精确解析
  spec.py          全批规格校验、字段路径错误、拓扑键校验
  affinity.py      标签、污点、Pod 亲和/反亲和、定点打分
  engine.py        预算轮次、逐个扣减、持久化裁决、恢复
  storage.py       SQLite schema、意图记录、事实查询
  api.py           FastAPI 路由与统一业务错误体
  cli.py           serve / run-sample / recover 命令
samples/
  nodes.json       5 个确定性节点
  pods.json        9 个确定性待调度 Pod
  budget.json      并发、时间、单节点轮次预算与打分容差
  spec_boundary_valid.json    合法规格边界
  spec_boundary_invalid.json  非法单位、负数、未知标签键等
tests/             27 项单元、API、强杀恢复测试
data/              默认 SQLite 数据库目录
```

默认预算样本见 `samples/budget.json`：

```json
{
  "concurrency_limit": 2,
  "time_budget_ms": 12,
  "per_node_round_limit": 1,
  "queue_capacity": 20,
  "node_check_cost_ms": 1,
  "score_tolerance": 5
}
```

## 3. 本地启动与演示

启动回环 HTTP 服务：

```powershell
python -m schedsim.cli serve --db data\scheduler.db --samples samples --host 127.0.0.1 --port 8000
```

启动后 OpenAPI 文档位于 `http://127.0.0.1:8000/docs`。

另开终端，提交本地样本并逐轮调度：

```powershell
$body = @{ sample = "default"; batch_id = "demo-1" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/batches/sample -ContentType "application/json" -Body $body

Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/batches/demo-1/rounds -ContentType "application/json" -Body "{}"
```

重复执行 rounds 接口直至批次状态为 `COMPLETED`。也可以一次跑完并输出完整 JSON：

```powershell
python -m schedsim.cli run-sample --db data\demo.db --samples samples --batch-id demo-cli
```

其他查询：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/batches/demo-1/results
Invoke-RestMethod http://127.0.0.1:8000/api/v1/batches/demo-1/placements
Invoke-RestMethod http://127.0.0.1:8000/api/v1/batches/demo-1/rejections
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/nodes/node-a?batch_id=demo-1"
Invoke-RestMethod http://127.0.0.1:8000/api/v1/batches/demo-1/recovery
```

直接提交本地边界样本：

```powershell
$json = Get-Content .\samples\spec_boundary_valid.json -Raw
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/batches -ContentType "application/json" -Body $json
```

## 4. HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/batches` | 提交完整节点/Pod/预算批次；校验成功后一次性持久化 |
| `POST` | `/api/v1/batches/sample` | 提交工作区样本，body 为 `{"sample":"default","batch_id":"..."}` |
| `POST` | `/api/v1/samples/{sample}/submit` | 按样本名提交 |
| `POST` | `/api/v1/batches/{batch_id}/rounds` | 触发下一调度轮次，可传 `{"round_no":N}` |
| `POST` | `/api/v1/batches/{batch_id}/abort` | 中止未完成批次 |
| `GET` | `/api/v1/batches/{batch_id}` | 批次状态、Pod 状态与轮次摘要 |
| `GET` | `/api/v1/batches/{batch_id}/results` | 放置结论、否决明细、轮次与节点余量 |
| `GET` | `/api/v1/batches/{batch_id}/placements` | 放置结论与余量 |
| `GET` | `/api/v1/batches/{batch_id}/rejections` | 按 Pod 分组的节点否决明细 |
| `GET` | `/api/v1/batches/{batch_id}/pods/{pod_name}/rejections` | 单个 Pod 否决明细 |
| `GET` | `/api/v1/nodes/{node_name}?batch_id=...` | 节点余量、逐次扣减记录、约束明细 |
| `GET` | `/api/v1/batches/{batch_id}/recovery` | 批次恢复报告 |
| `GET` | `/api/v1/recovery` | 全部恢复报告与启动扫描结果 |

统一错误体：

```json
{
  "code": "E_SPEC_INVALID",
  "message": "batch specification is invalid; no batch was persisted or scheduled",
  "context": {
    "failures": [
      {"path": "pods[0].requests.cpu", "reason": "invalid CPU quantity or unit; expected cores or millicores", "value": "1.5m"}
    ]
  }
}
```

| 业务码 | HTTP 状态 | 触发条件 |
|---|---:|---|
| `E_SPEC_INVALID` | 422 | 数量/单位/字段/资源限额/未知节点标签键等规格错误；整批不落库 |
| `E_TOPOLOGY_KEY_MISSING` | 422 | Pod 亲和/反亲和的 topologyKey 未被任何节点标签声明 |
| `E_SCHED_QUEUE_FULL` | 429 | Pod 数超过并发槽与有界等待队列容量；整批不落库 |
| `E_INVALID_TRANSITION` | 409 | 重复批次 ID、终态批次继续调度、轮次非单调等 |
| `E_BATCH_NOT_FOUND` | 404 | 批次或该批次中的节点/Pod 不存在 |

## 5. 调度语义与数值契约

### 规格解析

- CPU 内部统一为毫核整数：`500m -> 500`、`2 -> 2000`、`1.5 -> 1500`。
- 内存内部统一为字节整数：`512Mi -> 536870912`、`1Gi -> 1073741824`。
- 负数、非法单位、超出有符号 64 位、无法精确表示的分数单位均拒绝。
- 所有节点先于 Pod 归一化；Pod 引用的节点选择器/节点亲和性标签键必须在至少一个节点上声明。
- 校验错误聚合为具体路径，例如 `nodes[0].allocatable.cpu`、`pods[1].affinity.nodeAffinity...`。
- 规格错误发生在任何批次写入之前，满足整批 all-or-nothing。

### 硬性裁决

每个候选节点都会独立检查并留痕，不会静默跳过：

- 节点健康状态；
- `nodeSelector` 精确匹配；
- required 节点亲和性：`In`、`NotIn`、`Exists`、`DoesNotExist`、`Gt`、`Lt`，术语内部 AND、术语之间 OR；
- `NoSchedule` / `NoExecute` 污点与容忍匹配；`PreferNoSchedule` 只做软性降权；
- CPU/内存余量；
- required Pod 亲和与反亲和；
- topologyKey 在候选节点缺失时不猜测默认域，记录 `topology_key_missing` 否决；
- 单节点本轮放置上限 `node_placement_cap`。

否决记录包含 `pod_name`、`node_name`、`constraint_type`、约束原文或规范化表达式、当时节点余量快照及结构化明细。

### 资源余量

节点从 `allocatable` 开始，按放置序号逐个扣减。每次放置都持久化
`remaining_before` 与 `remaining_after`；中间态可直接查询。任何一步出现负数都会
中止并报状态一致性错误，而不是截断到 0。资源不足时记录请求值、余量、差额及快照。

### 定点打分与选点

所有分值均为整数，不使用二进制浮点：

```text
total = 1000 * Σ(命中的 preferred 节点亲和 weight)
      + 1000 * Σ(命中的 preferred Pod 亲和 weight)
      - 1000 * Σ(命中的 preferred Pod 反亲和 weight)
      + resource_balance
      - 500  * 未容忍的 PreferNoSchedule 污点数

resource_balance = 1000 - abs(cpu_free_bp - memory_free_bp)
cpu_free_bp     = floor(1000 * remaining_cpu / allocatable_cpu)
memory_free_bp  = floor(1000 * remaining_memory / allocatable_memory)
```

候选节点先按名称稳定排序，再取最高分。与最高分差值不超过
`score_tolerance` 的候选进入容差集合，最终按节点名字典序打破平局。相同输入与相同
SQLite 事实会得到逐位一致的分值、分项和选点。

### 预算与队列

- `concurrency_limit`：每轮最多尝试的 Pod 调度任务数。
- `time_budget_ms`：确定性逻辑毫秒预算；每检查一个节点消耗 `node_check_cost_ms`。不读取墙钟，保证重复演示一致。
- `per_node_round_limit`：每个节点每轮最多成功放置数；触顶时 Pod 保持 pending，下一轮重试。
- `queue_capacity`：批次提交时，超过并发槽的 Pod 进入有界等待队列；`pod_count > concurrency_limit + queue_capacity` 返回 `E_SCHED_QUEUE_FULL`。
- 任务顺序为 `(-priority, arrival_seq, pod_name)`。
- 时间预算不允许完成下一 Pod 时，不部分提交该 Pod；输出 `ROUND_TIME_BUDGET_EXHAUSTED` 跳过清单。
- 并发槽满而未处理的 Pod 输出 `CONCURRENCY_LIMIT` 并保留在批次内。
- 所有轮次均不会突破并发上限，宁可牺牲单轮覆盖。

### Pod 状态

- `PLACED`：已持久化放置事实。
- `PENDING`：尚未处理、预算延后、节点本轮上限重试，或 required Pod 亲和尚待依赖 Pod 落位。
- `UNSCHEDULABLE`：本轮所有节点均被不可恢复约束否决。
- `ABORTED`：管理员中止。

## 6. SQLite、意图与强杀恢复

数据库表覆盖 `batches`、`pod_states`、`intents`、`rounds`、`decisions`、
`placements` 和 `recoveries`。SQLite 使用 WAL 与完整同步，所有业务写入先创建
`intents` 记录；单 Pod 的否决、放置与状态更新在同一事务中提交。

强杀恢复流程：

1. 重启时扫描 `intents.status = 'PENDING'`。
2. 从已提交的 `placements`、`rounds`、`decisions`、`pod_states` 重算节点余量。
3. 未完成轮次标记 `INTERRUPTED`，批次恢复为 `INCOMPLETE`，保留后续继续调度的能力。
4. 写恢复报告；报告明确标注 `factual_only: true`。

恢复报告不伪造进程退出报告：

```json
{
  "exit_report_available": false,
  "exit_report": null,
  "process_exit_observed": false
}
```

### 强杀演示

```powershell
python -m schedsim.cli run-sample --db data\crash.db --samples samples --batch-id crash-demo --crash-after-placements 2
# 进程以 92 强制退出

# 重启后 HTTP 服务启动时会自动扫描；也可先显式执行：
python -m schedsim.cli recover --db data\crash.db
```

`--crash-after-intent` 会在首个调度轮次意图记录落盘后强杀；`--crash-after-placements N`
会在第 N 个放置事务提交后强杀。

## 7. 实际验证结果

验证时间：2026-11-23。当前执行环境为工作区内的 Python 3.14.4（WSL Ubuntu）；
代码仅使用 Python 3.11+ 标准能力，路径与命令均兼容 Windows。

```text
$ python3 -m pytest -q
27 passed
```

默认样本 `run-sample` 实际结果：

```text
STATUS COMPLETED
web-api          -> node-a  (round 1, score 20500)
web-worker       -> node-c  (round 1, score 1000)
gpu-trainer      -> node-c  (round 2, score 1000)
cache            -> node-d  (round 2, score 11000)
affinity-client  -> node-d  (round 3, score 963)
metrics-agent    -> node-b  (round 4, score 5988)
log-shipper      -> node-b  (round 5, score 985)

no-fit                           UNSCHEDULABLE (资源不足)
taint-blocked                    UNSCHEDULABLE (污点且无容忍)
node-e                           healthy=false，始终未参与放置
否决明细                         59 条
round 1-4 每轮逻辑耗时 10ms，round 5 为 5ms，均未超过 12ms 预算
```

实际回环 HTTP 验证（服务绑定 `127.0.0.1:18766`）：

```text
GET  /health                         -> status ok, mode local-offline
POST /api/v1/batches/sample          -> PENDING, 9 Pods, waiting_queue 7
POST /rounds x5                      -> COMPLETED, 7 placements, 59 rejections
每轮 queue_after.waiting_count       -> 5, 3, 1, 0, 0
GET  /nodes/node-a?batch_id=...      -> remaining 1500m / 1610612736 bytes
GET  /batches/http-final/results     -> placement 内含 affinity_satisfaction 证据
GET  /batches/missing/results        -> HTTP 404 E_BATCH_NOT_FOUND
```

实际强杀验证：

```text
CRASH_EXIT=92
pending_intents_before=1
placements_before=[web-api->node-a, web-worker->node-c]
batch_status_before=RUNNING
recovery reports=1, exit_report_available=false
batch_status_after=INCOMPLETE
round statuses=[INTERRUPTED]
final_status=COMPLETED, final_placement_count=7
```

边界规格样本 `spec_boundary_invalid.json` 会返回 `E_SPEC_INVALID`，失败路径至少包含：

```text
nodes[0].allocatable.cpu
nodes[0].allocatable.memory
nodes[0].healthy
pods[0].requests.cpu
pods[0].requests.memory
pods[0].nodeSelector.undeclared.example/key
pods[0].affinity.nodeAffinity...matchExpressions[0].key
```

## 8. 说明与边界

- 本引擎模拟本地调度语义，不执行 kubelet、容器创建、抢占或真实绑定。
- 逻辑时间预算用于可重复演示，不是墙钟 deadline；请求不会因机器速度不同产生不同结果。
- Pod 亲和命名空间默认取当前 Pod namespace；显式 `namespaces` 时按列表匹配。
- topologyKey 必须在至少一个节点标签中声明；单个候选节点缺少该标签时记录否决而不用默认值猜测。
- 恢复报告只输出 SQLite 已落盘事实和显式恢复动作；强杀不会生成或虚构“正常退出报告”。
