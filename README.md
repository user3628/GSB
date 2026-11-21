# 本地确定性事件总线

面向本地单机场景的事件总线流式集成中间件。FastAPI 提供本地回环 HTTP 接口，SQLite WAL 持久化主题、事件、投递租约、确认、消费位点、状态轨迹和聚合状态；不连接外部消息系统，也不需要任何账号。

核心路径：

```text
生产者 --HTTP batch--> ACCEPTED 事件 / 分区内无缺口序号
消费者 --HTTP poll---> 分区租约 + 最小未确认事件
消费者 --HTTP ack----> ACKED + 确认记录 + 位点推进 + 聚合更新
调用方 --HTTP rewind-> 位点回拨 + 租约释放 + 确认回退 + 聚合重算 + 顺序重放
进程被 kill ----------> 重启扫描 PENDING 意图与持久化事实，恢复到完整提交边界
```

已用固定种子样本完成正常消费、租约超时重投、确认中途强制终止恢复、位点回拨重放和未中断参考运行比对。实际结果见 `artifacts/e2e_result.json` 与 `artifacts/verification_results.json`。

## 快速开始

要求 Python 3.11+。本次实际验证环境为 Python 3.14.4、SQLite 3.46.1。依赖：FastAPI、Uvicorn、pytest、requests、httpx。

Windows PowerShell：

```powershell
cd <工作目录>
py -3.11 -m venv --system-site-packages .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -c "import fastapi, uvicorn, pytest, requests, sqlite3; print(sqlite3.sqlite_version)"
```

Linux/macOS：

```bash
python3 -m venv --system-site-packages .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -c "import fastapi, uvicorn, pytest, requests, sqlite3; print(sqlite3.sqlite_version)"
```

`--system-site-packages` 允许在依赖已预装的离线环境中直接使用本地包。若使用标准虚拟环境，可在本地 wheel 目录可用时执行：

```bash
python -m pip install --no-index --find-links <本地wheel目录> -r requirements.txt
```

默认数据库文件是 `var/eventbus.db`，也可通过 `EVENTBUS_DB` 或 `--db` 指定本地路径。

### 生成固定样本

```bash
python scripts/generate_sample.py --output data/sample_events.json
```

固定种子为 `20261122`。样本包含 24 个事件、3 个分区、4 个事件键，以及 `1e12`、`-1e12`、`1e-6`、普通浮点、`null` 和缺失度量值。当前种子下 3 个分区各 8 个事件。

### 启动服务

```bash
python -m scripts.run_server --db var/eventbus.db --host 127.0.0.1 --port 8765
```

等价 Uvicorn 入口：

```bash
EVENTBUS_DB=var/eventbus.db python -m uvicorn main:app --host 127.0.0.1 --port 8765 --workers 1
```

Windows PowerShell：

```powershell
$env:EVENTBUS_DB = "var/eventbus.db"
python -m uvicorn main:app --host 127.0.0.1 --port 8765 --workers 1
```

必须使用一个 Uvicorn worker。SQLite 支持事务级单写者，但当前启动恢复策略面向单进程本地部署。

本地端点：

- 健康检查：`GET /health`
- OpenAPI：`GET /docs`
- 错误码目录：`GET /api/v1/error-codes`
- 状态机定义：`GET /api/v1/meta/state-machine`

## API 摘要

完整契约见 `docs/API.md`。

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/v1/topics` | 声明主题、分区数与消费者组 |
| POST | `/api/v1/topics/{topic}/events` | 原子发布事件批次 |
| GET | `/api/v1/events/{event_id}` | 事件、状态轨迹、投递和确认 |
| POST | `/api/v1/consumers/poll` | 按组和分区申请租约并拉取最小未确认事件 |
| POST | `/api/v1/consumers/ack` | 确认当前租约事件 |
| GET | `/api/v1/consumers/offset` | 查询位点及重放窗口 |
| POST | `/api/v1/consumers/offset/rewind` | 回拨到已提交生产序号并触发重放 |
| GET | `/api/v1/aggregates` | 按事件键读取稳定有序的聚合结果 |
| GET | `/api/v1/snapshot` | 在同一 SQLite 读快照中读取事件、位点和聚合 |
| GET | `/api/v1/verify` | 读取持久化状态并执行一致性验证 |
| GET | `/api/v1/recovery/reports/latest` | 最近一次启动恢复报告 |

成功响应带 `"ok": true`。业务失败统一返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_LEASE_HELD",
    "message": "该分区已有未过期投递租约。",
    "context": {"partition": 0, "holder_consumer_id": "slow-consumer"}
  }
}
```

## 业务语义

### 主题、分区和事件

- 分区路由：`sha256("{topic}\x1f{partition_key}")` 的整数形式对分区数取模。
- 分区生产序号从声明的最早保留序号开始，由总线在发布事务内单调分配；批次失败时整批回滚，因此不会留下序号缺口。
- 事件输出包含 `event_id`、`partition_key`、`production_seq`、`event_time`、`body_digest`、`batch_id`、事件键、度量和状态。
- `body_digest = sha256(canonical_json({key, measure, payload}))`。若请求显式提供 `body_digest`，必须与计算值一致，否则返回 `E_EVENT_INVALID`。
- 幂等指纹包含主题、分区键、规范化事件时刻、事件键、规范化度量和 payload；不包含批次标识和总线分配的生产序号。因此同一事件在新批次重试仍可幂等成功。
- 同一 `event_id` 内容一致时返回成功并标记 `duplicate`；内容不一致返回 `E_CONFLICTING_EVENT`，首次内容及首次序号保持不变。
- 任一事件解析失败返回 `E_EVENT_INVALID`，整批不写入事件或生产序号。

### 事件状态机

```text
INITIAL       -> ACCEPTED
ACCEPTED      -> DISPATCHED | DEAD_LETTERED
DISPATCHED    -> ACKED | REDELIVERED | DEAD_LETTERED
REDELIVERED   -> DISPATCHED | DEAD_LETTERED
ACKED         -> REDELIVERED        # 仅显式位点回拨/恢复回退
DEAD_LETTERED -> 终止
```

每次合法迁移都写入 `event_state_trace`。重复确认已 ACKED 事件是幂等成功，不新增迁移和聚合贡献；未投递确认返回 `E_NOT_DISPATCHED`；非法迁移返回 `E_INVALID_TRANSITION`，事务回滚，事件状态和轨迹不变。

### 投递、租约和位点

- 位点是“下一个待消费生产序号”。初始值等于分区最早保留序号；确认序号 `s` 后推进到 `s+1`。
- 同主题、消费者组、分区最多一个 `ACTIVE` 租约。并发拉取只有一个成功，其余返回 `E_LEASE_HELD`。
- 拉取只返回位点处的最小事件。旧租约超时后先落库为 `EXPIRED`，事件迁移到 `REDELIVERED`，随后按原序号重投，不会跳到后继事件。
- 慢消费者未确认时位点不推进，因此其他消费者不能越过该事件。
- 确认必须匹配消费者、事件和租约令牌，并在同一事务内完成：`DISPATCHED -> ACKED`、投递结束、确认记录、位点推进、聚合更新、意图提交。

### 位点回拨和重放

- `target_seq` 必须是该分区已提交的生产序号，并成为新的“下一个待消费序号”。
- `target_seq < min_retained_seq` 返回 `E_OFFSET_EXPIRED`；目标不存在、或试图向前跳跃，返回 `E_OFFSET_NOT_FOUND`。
- 回拨事务会同时释放活动租约、把目标及之后的有效确认标记为 `ROLLED_BACK`、把 ACKED 事件迁回 REDELIVERED、回拨位点并重算聚合。不会出现“位点已回拨但租约仍指向旧位点”的已提交状态。
- 原位点被记录为重放边界 `replay_end`。重放期间 `production_seq >= replay_end` 的新事件暂缓投递；重放到边界后自动恢复正常投递。

### 确定性聚合

- 按事件键分组，每个已确认事件计数加一。
- `measure` 缺失或为 `null` 时仍计数，求和贡献为 `0`；非数字、非有限值返回 `E_EVENT_INVALID`。
- 使用 Neumaier 补偿求和，持久化主和与补偿项。
- 每个键的贡献按 `(partition, production_seq)` 规范顺序重算，因此确认批次边界、跨分区确认顺序或进程重启不会改变逐项聚合结果。
- 输出按键字典序稳定排序。

### 意图、事务与恢复

发布、投递、确认、回拨和死信操作都先单独提交一条 `PENDING` 意图，再执行状态事务。状态事务内同时把所有业务变更与意图 `COMMITTED` 标记提交，因此不存在“业务已提交但意图仍 PENDING”的正常窗口。

进程被强制终止后，重启恢复只依据持久化事实：

1. 将所有仍为 `PENDING` 的意图标记为 `ABORTED`。
2. 释放上次进程遗留的活动租约，把相应事件真实迁回 `REDELIVERED`。
3. 从事件状态和有效确认计算每个分区的最后完整提交边界；如有偏移，回拨位点并建立重放窗口。
4. 从有效确认重建聚合状态。
5. 写入 `recovery_reports`。报告不声称来自退出钩子，只声明来自 SQLite 中已持久化的意图和状态。

### 读一致性和并发

- SQLite 使用 WAL、`synchronous=FULL` 和 30 秒 busy timeout。
- 写操作使用 `BEGIN IMMEDIATE`，并且当前进程使用全局写者锁；这是比分区间单写者更强的串行保证。
- 多读者使用独立连接和显式 `BEGIN` 读事务。事件 ACK、确认记录、位点推进、聚合更新在同一提交版本中可见。
- `/api/v1/snapshot` 在单个读快照中读取三类状态，便于外部核对。

## 错误码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_EVENT_INVALID` | 400 | 事件/批次无法解析；整批不入库 |
| `E_CONFLICTING_EVENT` | 409 | 事件标识内容冲突；保留首次内容 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移；不修改事件状态 |
| `E_LEASE_HELD` | 409 | 分区已有有效租约 |
| `E_OFFSET_EXPIRED` | 410 | 回拨目标早于最早保留序号 |
| `E_OFFSET_NOT_FOUND` | 404 | 目标不是已提交序号或不是回拨 |
| `E_NOT_DISPATCHED` | 409 | 未经有效投递却尝试确认 |
| `E_TOPIC_NOT_FOUND` | 404 | 主题不存在 |
| `E_CONFLICTING_TOPIC` | 409 | 同名主题配置冲突 |
| `E_GROUP_MISMATCH` | 409 | 消费者组不匹配 |
| `E_PARTITION_NOT_FOUND` | 404 | 分区不存在 |
| `E_EVENT_NOT_FOUND` | 404 | 事件不存在 |
| `E_REQUEST_INVALID` | 400 | 请求结构或参数错误 |
| `E_TEST_HOOK_DISABLED` | 403 | 未启用强制终止演示钩子 |
| `E_ROUTE_NOT_FOUND` | 404 | 路由不存在 |
| `E_INTERNAL` | 500 | 未预期内部错误 |

## 验证

### 自动测试

```bash
pytest -q
```

覆盖：分区路由与无缺口序号、批次原子性、幂等与冲突、状态轨迹、并发拉取、租约超时、重复确认并发、回拨边界、重放屏障、补偿求和、分批聚合等价、崩溃恢复、聚合修复、HTTP 错误封装和读快照一致性。本次实际结果：`19 passed`。

### 强制终止端到端演示

```bash
python scripts/e2e_demo.py --db var/e2e.db --reference-db var/e2e_reference.db --artifact artifacts/e2e_result.json
```

脚本启动真实 Uvicorn 子进程，通过 HTTP 完成：

1. 发布固定样本并正常顺序消费。
2. 制造 `E_LEASE_HELD`，等待租约超时，验证同一序号重投。
3. 在确认意图已提交后执行真实 `os._exit(86)`，客户端连接中断。
4. 用同一 SQLite 文件重启服务，读取恢复报告，验证确认未生效、租约释放、聚合不变，然后从同一序号继续。
5. 完成全部消费后回拨分区 0 到序号 1，验证聚合回退和 1..8 顺序重放。
6. 在重放期间发布分区 0 的新事件，验证其序号 9 被屏障暂缓，重放到边界后才投递。
7. 用另一个未中断的参考数据库完成相同事件集合，逐项比较最终聚合。

本次实际结果摘要：

- 错误码覆盖：`E_EVENT_INVALID`、`E_CONFLICTING_EVENT`、`E_LEASE_HELD`、`E_NOT_DISPATCHED`、`E_INVALID_TRANSITION`、`E_OFFSET_EXPIRED`、`E_OFFSET_NOT_FOUND`。
- 租约超时：期望序号 3，实际重投序号 3，attempt=2。
- 崩溃恢复：确认前位点未推进，PENDING ACK 意图被 abort，期望及实际重投序号均为 4。
- 回拨重放：分区 0 重放序号严格为 `1,2,3,4,5,6,7,8`；重放期间新事件序号 9 未越过屏障。
- 恢复前后聚合差异：所有键 `count_delta=0`、`sum_delta=0.0`。
- 最终聚合与从未中断参考运行逐项完全相等：`true`。
- 独立验证器：7 项检查全部通过。

### 结果验证工具

```bash
python scripts/verify_state.py --db var/e2e.db --topic orders --output artifacts/e2e_verification.json
```

检查项包括：分区生产序号连续性、状态迁移链合法性、确认与聚合一致性、位点历史/完整边界、租约唯一性、无 PENDING 意图、状态值合法。工具只读持久化状态，异常时以退出码 1 结束。

## 目录

```text
eventbus/                  # 服务、存储、状态机、聚合、验证器、HTTP API
scripts/generate_sample.py # 固定种子样本生成器
scripts/run_server.py      # 本地 Uvicorn 启动入口
scripts/e2e_demo.py        # HTTP 端到端与强制终止恢复演示
scripts/verify_state.py    # 持久化状态验证 CLI
data/sample_events.json    # 已生成的确定性样本及期望摘要
tests/                     # pytest 单元、并发、HTTP、恢复测试
docs/API.md                # API 契约
docs/DESIGN.md             # 一致性设计与恢复算法
artifacts/                 # 实际测试结论与端到端证据
var/                       # 运行期 SQLite 文件（默认忽略）
```

## 已知边界

- 面向本地单机、单 Uvicorn worker；不实现跨主机复制或分布式共识。
- SQLite 事务提供崩溃原子性；模拟钩子只用于证明“意图已持久化、业务事务未提交”时能够恢复，不用于生产。
- 当前没有物理删除数据的保留策略；`min_retained_seq` 是声明的保留下界，回拨校验使用该下界。
- 强制终止时不会产生优雅退出报告；只有重启后基于持久化事实生成的恢复报告。
