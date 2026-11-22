# LeaseCoord：本地多进程租约选举与故障转移协调器

LeaseCoord 是一个纯本地、可嵌入的协调组件。候选节点通过回环 HTTP 接口注册、发送心跳、竞选、续约与退位；协调器使用 SQLite 单写者事务维护唯一主租约，并在主节点心跳失联或租约到期后执行故障转移。

项目不依赖 Docker、真实网络或外部基础设施。固定样本、确定性时钟注入、本地 HTTP 端口和 SQLite 文件足以完成竞态、乱序、规模变化与强制终止恢复验证。

## 环境与启动

要求 Python 3.11+、SQLite 3（Python 标准库 `sqlite3`）和 NumPy。

Windows PowerShell 示例：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\pip install -e .
.\.venv\Scripts\python -m leasecoord.server --db .data\coordinator.sqlite --port 8765
```

Linux/macOS 对应命令：

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .
python -m leasecoord.server --db .data/coordinator.sqlite --port 8765
```

服务默认只监听 `127.0.0.1:8765`。启动时先执行恢复扫描，然后打印实际监听地址与实例 ID。主要参数：

| 参数/环境变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `--exact-limit` / `LEASE_EXACT_LIMIT` | 64 | 不超过此候选数时走精确全量路径 |
| `--heartbeat-timeout-ms` / `LEASE_HEARTBEAT_TIMEOUT_MS` | 5000 | 最近心跳超过该值时节点失联 |
| `--sample-fraction` / `LEASE_SAMPLE_FRACTION` | 0.10 | 非决定性优先级桶的采样比例 |
| `LEASE_PRIORITY_MIN/MAX` | 0/100 | 允许注册的优先级范围 |
| `LEASE_MIN/MAX_TTL_MS` | 100/86400000 | 允许注册的租约 TTL 范围 |
| `--allow-time-override` | 关闭 | 允许测试用的 `X-Coordinator-Now-Ms`/`now_ms`，生产环境不要开启 |

本地确定性冒烟：

```bash
python -m leasecoord.server --db /tmp/coordinator.sqlite --port 0 \
  --exact-limit 2 --heartbeat-timeout-ms 1000 --allow-time-override
```

## 项目结构

```text
leasecoord/
  coordinator.py   租约/节点状态机、单写者操作、故障转移、恢复
  election.py      精确全量裁决与优先级分桶采样裁决
  store.py         SQLite schema、WAL 读事务、BEGIN IMMEDIATE 写事务、不变量校验
  http_api.py      本地 JSON HTTP 适配器
  server.py        CLI 启动入口
  samples.py       固定样本加载与确定性大样本展开
samples/
  candidates_small.json   10 节点固定样本与心跳序列
  candidates_large.json   4096 节点固定确定性样本规格
tests/
  test_coordinator.py     并发、租约、故障转移、崩溃恢复等
  test_http.py            HTTP 契约与并发请求
  crash_child.py          强制终止子进程
scripts/acceptance.py     固定样本端到端验收
artifacts/acceptance_report.json  最近一次实际验收结果
```

## 数据模型与不变量

节点字段：`id`、`priority`、`lease_ttl_ms`、`last_heartbeat_ms`、`status`、`leader_lease_version`、`registered_version`、`updated_at_ms`。节点状态为 `ACTIVE`、`SUSPECT`、`LEADER`、`FOLLOWER`。

租约字段：`holder_id`、`version`、`expires_at_ms`、`term`、`state`、`updated_at_ms`。租约状态为 `FREE`、`GRANTED`、`EXPIRED`。

关键不变量在每个租约写事务的 `COMMIT` 前通过 `assert_lease_invariants()` 校验：

- `GRANTED` 租约必须有持有人，且该节点必须是唯一 `LEADER`。
- `LEADER.leader_lease_version` 必须等于当前租约 `version`。
- 非 `GRANTED` 状态不得存在任何 `LEADER` 节点。
- 竞选、授予、续约、退位、故障转移和失效迁移中的租约版本与节点状态在同一 SQLite 事务内提交，因此读写都不会看到“租约已授予但节点仍为 SUSPECT”的中间状态。

心跳时间使用单调 `max(committed, requested, now-clamped)` 更新。旧时间戳或乱序请求不会回退最近心跳；带旧 `expected_lease_version` 的续约/退位会返回 `E_STALE_LEASE`。

## HTTP 接口

成功响应均为 JSON 对象并含 `"ok": true`；业务失败响应为：

```json
{
  "ok": false,
  "error": {
    "code": "E_LEASE_HELD",
    "message": "a valid lease is already held",
    "context": {
      "holder_id": "alpha",
      "expires_at_ms": 1760000015000,
      "lease_version": 2
    }
  }
}
```

| 方法与路径 | 用途 |
| --- | --- |
| `POST /v1/nodes` | 注册/更新节点；重复注册保留租约版本 |
| `POST /v1/nodes/{id}/heartbeat` | 更新最近心跳 |
| `POST /v1/nodes/{id}/transition` | 受状态机约束的管理迁移；`LEADER` 不可直接指定 |
| `POST /v1/elections/campaign` | 竞选；租约有效时失败 |
| `POST /v1/lease/renew` | 当前持有者在到期前续约 |
| `POST /v1/lease/resign` | 当前持有者退位 |
| `POST /v1/lease/failover` | 在主节点心跳失联/租约到期时执行故障转移 |
| `GET /v1/leader` | 查询有效主节点与租约摘要 |
| `GET /v1/lease` | 查询租约；同时给出存储状态与有效状态 |
| `GET /v1/nodes`、`GET /v1/nodes/{id}` | 查询节点有效状态 |
| `GET /v1/recovery` | 最近一次持久化恢复报告 |
| `GET /v1/events` | 最近租约审计事件 |
| `GET /healthz`、`GET /v1/config` | 健康与配置 |

示例：

```bash
curl -X POST http://127.0.0.1:8765/v1/nodes \
  -H 'Content-Type: application/json' \
  -d '{"id":"alpha","priority":90,"lease_ttl_ms":15000,"last_heartbeat_ms":1760000000000}'

curl -X POST http://127.0.0.1:8765/v1/elections/campaign \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"alpha","expected_lease_version":0,"request_id":"campaign-1"}'

curl -X POST http://127.0.0.1:8765/v1/lease/renew \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"alpha","expected_lease_version":1}'
```

`request_id` 是可选幂等键：完全相同的请求会返回已提交结果并标记 `"replayed": true`；同一键用于不同负载/操作则返回 `E_CONFLICTING_REQUEST`。

## 选举算法

候选资格由已注册、优先级范围、心跳新鲜度和非 `SUSPECT` 状态共同决定。排序规则固定为：

1. 优先级降序；
2. 同优先级时节点 ID 字典序升序。

### 精确路径

候选总数 `<= exact_limit` 时，对全部候选做资格检查和完整排序，响应标记 `algorithm: "exact_full_scan"`、`estimated: false`，并输出比较次数、候选集合摘要和决胜依据。

### 分桶采样路径

候选总数 `> exact_limit` 时，按优先级降序分桶：

- 从最高优先级开始逐桶检查，第一个存在有效候选的桶为“决定性桶”。
- 决定性桶做完整比较，因此可以确定全局最高优先级与最小节点 ID。
- 更低优先级桶使用基于 SHA-256 的确定性抽样，输出种群数、抽样数、桶内有效比例估计、Wilson 95% 区间、整体采样率和置信说明。
- 响应标记 `algorithm: "priority_stratified_sampling"`、`estimated: true`、`estimated_winner: true`，并给出 `difference_vs_exact.same_winner = true` 的依据。

这个设计把“比较工作量”限制在决定性桶加低优先级样本；同时利用优先级支配关系确定性证明获胜者与精确路径一致，避免用纯概率采样产生不可核对的主节点差异。采样桶的种群/资格统计仍是估计值，报告会明确其估计范围。

## 并发、持久化与恢复

SQLite 使用 WAL 模式。读查询使用独立只读连接和快照事务，可以并发执行；所有租约写请求通过 `BEGIN IMMEDIATE` 串行化，因此跨线程与跨进程只有一个写事务能够推进租约版本。

每次授予、续约、退位或故障转移按以下顺序执行：

1. 在独立事务中提交 `PENDING` 意图，记录操作、节点、期望版本、请求指纹、PID、实例 ID 与主机名。
2. 开启 `BEGIN IMMEDIATE` 写事务，重新读取已提交状态，校验期望版本与业务条件。
3. 同一事务内更新租约、节点状态、`leader_lease_version`、审计事件，并把意图改为 `COMMITTED`。
4. 提交事务；若业务校验失败，事务回滚且意图改为 `ABORTED`。

进程在步骤 1 后、步骤 3 提交前被强制终止时：SQLite 自动回滚未提交的租约事务，`PENDING` 意图保留。重启恢复扫描会依据持久化的 PID/主机/实例信息识别孤儿意图并标记 `ABORTED`；租约保持最后一个完整提交版本。若原进程仍存活，则不会误中止其进行中的意图。

恢复报告仅使用 SQLite 已提交事实，内容包括：

- 发现与中止的孤儿意图；
- 最后完整租约的持有者、版本、到期时刻与任期；
- 重新评估后的失效节点清单、失联阈值和状态更新；
- 过期/失联主租约是否需要故障转移；
- 明确限制：强制终止不可能产生完整优雅退出报告，报告不虚构内存事实。

若主节点仍持有已存储的 `GRANTED` 租约但心跳已失联，恢复报告只报告该事实并保留版本；随后显式或请求触发的故障转移在同一事务内完成“旧主 `SUSPECT` + 新主 `LEADER` + 租约版本递增”。

## 业务错误码

| 业务码 | 典型场景 |
| --- | --- |
| `E_NODE_INVALID` | 未注册、优先级越界、TTL 非法、心跳超时、节点为 `SUSPECT` |
| `E_LEASE_HELD` | 有效租约存在；响应携带当前持有者、版本与到期时刻 |
| `E_NO_ELIGIBLE_CANDIDATE` | 无心跳有效候选；租约版本保持不变 |
| `E_STALE_LEASE` | 期望版本落后、过期后续约/退位 |
| `E_CONFLICTING_REQUEST` | 幂等键冲突、相同期望版本的并发冲突请求 |
| `E_INVALID_TRANSITION` | 非法节点/租约迁移、健康主节点上的无强制故障转移 |
| `E_BAD_REQUEST` / `E_NOT_FOUND` / `E_INTERNAL` | HTTP 输入、路由与内部错误 |

## 验证

```bash
python -m pytest -q
python scripts/acceptance.py
```

测试覆盖：注册更新与版本连续性、非法竞选不改租约、同优先级 ID 决胜、续约/退位、无候选故障转移保留版本、精确/采样同解、线程并发竞选、独立进程并发竞选、随机交错生命周期、乱序心跳、非法状态迁移、HTTP 错误契约、强制终止恢复和幂等重放。

本次实际结果（环境：Python 3.14.4、SQLite 3.46.1、NumPy 2.2.1；详细数据见 `artifacts/acceptance_report.json`）：

- `python -m pytest -q`：`14 passed`。
- 4096 节点固定样本：精确路径与采样路径均选 `node-00000`，租约版本均为 `1`；采样率 `0.121094`，实际比较/检查 `496/4096` 个候选，决定性桶 41 个，共 101 个优先级桶，置信等级 `0.95`。
- 小样本 10 节点：精确与采样均选 `alpha`；续约后租约版本 `1 -> 2`。
- 24 个并发竞选请求：成功 `1` 个，其余 `23` 个均为 `E_LEASE_HELD`，最终租约版本 `1`。
- 6 个独立 Python 进程同时竞选同一 SQLite：成功 `1` 个，其余 `5` 个为 `E_LEASE_HELD`，租约版本只推进到 `1`。
- 故障转移：初始版本 `1`，续约后 `2`；无有效候选时保持版本 `2`；恢复 `bravo` 心跳后故障转移版本 `3`。
- 强制终止：子进程在 `RENEW` 意图提交后以码 `91` 退出；预恢复租约版本 `1`，恢复后仍为 `1`，孤儿意图标记 `ABORTED`，10 个失效节点全部进入报告；后续故障转移版本 `2`，新主 `bravo`。
- CLI 回环烟测：`127.0.0.1` 动态端口启动成功，注册、竞选和主节点查询均返回 `ok: true`。

## 设计边界

- 这是本地单协调器模型，SQLite 文件是唯一事实源；多个客户端进程可以并发访问同一个 HTTP 服务，也可按需要由多个进程打开同一 SQLite 执行同一意图协议。
- 采样结论显式标注为估计，并附桶统计与置信区间；获胜者身份由决定性桶完整比较认证，但低优先级桶的资格总数不是全量精确值。
- 恢复不会自动猜测未提交意图中的结果，也不会承诺强制终止后存在完整退出报告。若需要启动即故障转移，应在恢复完成后调用 `/v1/lease/failover`。
- 测试时间可通过 `--allow-time-override` 固定；生产运行应使用系统时钟并关闭该选项。
