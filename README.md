# SQLite 流式分组聚合执行引擎

面向本地 SQLite 的流式窗口聚合服务。应用通过 FastAPI 暴露本地 HTTP 接口，并提供可交互的数据编辑与窗口回看页面。事件按批次原子写入，支持滚动（tumbling）、滑动（sliding）、会话（session）三类窗口，以及精确定点聚合、类型演化、窗口状态机、迟到队列、校验快照和强制终止后的断点续算。

## 快速启动

环境使用 Python 3.11，不依赖 Docker、外部数据库或网络服务。

```bash
# Python 3.11 + 独立虚拟环境 + pip 依赖
uv python install 3.11
uv venv --python 3.11 --seed .venv
source .venv/bin/activate
pip install -r requirements.txt

# 启动交互应用
STREAM_AGG_DB=data/engine.db .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

浏览器打开 `http://127.0.0.1:8000/`。页面可录入/编辑批次事件 JSON，选择窗口规格与度量类型，查询窗口聚合、迟到队列、类型演化、快照和恢复报告。

完整演示与测试（会启动监督进程、执行演示、在事务中强制杀死服务、自动重启、对比续算结果，然后运行 pytest）：

```bash
./scripts/run_demo.sh
```

实际输出记录在 `artifacts/verification.txt`，演示摘要、快照和恢复报告也保存在 `artifacts/`。

只用 HTTP 服务时可运行：

```bash
.venv/bin/python -m pytest -q
```

## 交付结构

```text
app/
  main.py          FastAPI 路由、统一错误处理、静态页面
  engine.py        批次事务、窗口执行、状态机、意图日志、快照、恢复、查询
  windows.py       三类窗口归属与边界判定
  aggregate.py     定点聚合与分位扩展
  numeric.py       度量类型校验、精确缩放、格式化
  models.py        SQLAlchemy 表模型
  db.py            SQLite WAL/事务配置、固定读快照
  schemas.py       Pydantic 请求模型
static/index.html  本地交互式数据编辑与操作界面
samples/           固定整数、定点小数、迟到、乱序、崩溃样本与空快照
scripts/demo.py    可重复 HTTP 演示与基线比对
scripts/crash_worker.py  子进程强制终止测试工具
scripts/run_demo.sh      服务监督、演示、pytest 一键执行
tests/             窗口、类型、状态机、快照、崩溃恢复、HTTP、并发测试
docs/              事务、快照与恢复设计说明
artifacts/         实际验证输出、演示快照和恢复报告
```

## 核心语义

### 度量类型与精确定点聚合

- 每批次声明 `measure_type`：`{"kind":"integer"}` 或 `{"kind":"decimal","scale":2}`。
- 整数列的非空值必须是 JSON integer；定点小数列接受可精确表示在声明 scale 内的整数、JSON number 或十进制字符串。
- 任一非空度量与声明不符时，整批不入库并返回 `E_MEASURE_TYPE_MISMATCH`，上下文包含批次、事件、声明类型和收到的值。
- 聚合不经过浮点运算。每个窗口状态保存 `sum_scaled`、`min_scaled`、`max_scaled` 与当前最宽 scale；scale 提升时所有既有值乘以 10 的幂，切换回整数时仅影响后续输入声明，已有小数状态保留最宽 scale。
- 输出同时给出可读值与缩放整数，例如 `sum="13.25"`、`sum_scaled="1325"`、`scale=2`、`aggregate_type="decimal"`、`rounding="none"`。
- 每个分组保存类型演化序列，记录 `initial/promotion/narrowing` 原因、依据和每步舍入方式。降级不会对历史聚合值舍入。
- 空值计入 `count`，不计入 `non_null_count`、`sum`、`min`、`max`。全空分组确定返回 `sum/min/max = null` 与 `all_null=true`。
- 分位扩展使用窗口内精确样本的 `nearest_rank` 算法；它只增加 `quantiles` 字段，不改变计数、求和、最小、最大值。

### 窗口与边界

请求通过 `window_spec.boundary` 显式选择：

- `left_closed_right_open`：`start <= ts < end`
- `left_closed_right_closed`：`start <= ts <= end`

每个窗口响应都回显 `{"start":"closed","end":"open|closed"}`。

- **滚动**：`start = floor(timestamp_ms / width_ms) * width_ms`，按纪元边界对齐。右闭边界在精确边界时间会产生两个相邻归属窗口，这是声明的闭区间语义。
- **滑动**：窗口起点按 `step_ms` 在纪元边界对齐，`step_ms <= width_ms`，同一事件可进入多个重叠窗口。例如 `width=1000, step=500` 的事件通常进入 2 个窗口。
- 查询范围本身为闭区间；窗口与范围的相交判断遵循窗口自身边界，右开窗口结束时间恰好等于 `from_ms` 时不算相交。
- **会话**：同一分组键按空闲间隔切分。新事件与当前会话超过 `gap_ms` 时，旧会话自动 `OPEN -> CLOSED` 并建立新会话；会话结束时间随最后一个事件推进。会话切分状态单独持久化。
- 批次声明 `timestamp_order="sort"` 时，按 `(timestamp_ms, event_id)` 排序后处理；声明 `"reject"` 且时间戳非单调时整批拒绝并返回 `E_OUT_OF_ORDER`。

### 状态机与迟到事件

合法迁移只有：

```text
OPEN -> CLOSED -> EMITTED
```

- 新事件只合并到 `OPEN` 窗口。
- 命中 `CLOSED` 或 `EMITTED` 窗口时：
  - `late_policy="queue"`：事件写入迟到队列，窗口聚合完全不变，批次仍可提交。
  - `late_policy="reject"`：整批回滚并返回 `E_LATE_EVENT`。
- 非法迁移返回 `E_INVALID_TRANSITION`，窗口状态和全局 `state_version` 均不改变。
- `window_close` 只关闭 `end_ms <= through_ms` 的 OPEN 窗口；`window_emit` 先合法关闭满足条件的 OPEN 窗口，再发射满足条件的 CLOSED 窗口。

### 快照与版本

- 快照包含窗口聚合、迟到队列、会话切分状态、类型演化、分组当前类型、批次/事件日志和窗口迁移审计。
- 快照使用规范化 JSON 的 SHA-256 校验摘要；版本号单调生成。
- 加载要求 `incoming_version > loaded_snapshot_version`，并在单个 SQLite 事务内删除旧状态、整体写入快照状态，绝不混合新旧窗口。
- 创建快照只生成版本，不改变已加载版本；加载快照才推进 `loaded_snapshot_version`。
- 版本不递增、摘要失败、结构不合法或替换入库失败均返回 `E_SNAPSHOT_INVALID`，加载前状态保持不变。

### 意图日志与强制终止恢复

每次批次合入、窗口关闭/发射/显式迁移、快照创建/加载前，先以独立事务写入 `PENDING` 意图。业务状态、批次终态、全局版本和意图 `COMPLETED` 在同一个 SQLite 事务中提交。

- 进程在状态事务中途被杀死时，SQLite 原子回滚未提交改动，磁盘上只留下 `PENDING` 意图。
- 下次启动扫描 `PENDING` 意图：已提交批次会被对账；未提交批次写入 `ABORTED` 记录，后续重复批次不能产生状态。
- 恢复报告只列已落盘事实：发现的意图、执行的动作、被中止批次、当前状态版本、最后完整快照版本。
- 事务结束后续算后续批次，与“从未中断且跳过了被中止批次”的基线逐窗口一致。

### 单写者与多读者

- 进程内所有状态变更持有全局写锁；SQLite 使用 WAL、`synchronous=FULL`、30 秒 busy timeout。
- 每批次中的窗口、类型、迟到、批次记录和 `state_version` 在同一事务提交。
- 查询显式开启 SQLite 读事务，`state_version`、窗口和演化记录来自同一个 WAL 快照，不会读到部分合入状态。
- 相同 `batch_id`、相同内容的并发提交串行等待后复用已提交结果；内容不同返回 `E_CONFLICTING_BATCH`。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/batches` | 原子摄入批次 |
| `GET` | `/api/batches` | 批次状态 |
| `GET` | `/api/events` | 按时间和分组查询已落盘事件 |
| `GET` | `/api/windows` | 按时间范围、分组、窗口类型、状态回看聚合 |
| `GET` | `/api/late-events` | 迟到队列 |
| `GET` | `/api/type-evolution` | 每个分组的类型演化序列 |
| `POST` | `/api/windows/close` | 关闭到指定时间的窗口 |
| `POST` | `/api/windows/emit` | 关闭并发射窗口 |
| `POST` | `/api/windows/transition` | 显式状态迁移与非法迁移验证 |
| `POST` | `/api/snapshots` | 创建带校验摘要的快照 |
| `GET` | `/api/snapshots` | 快照列表 |
| `GET` | `/api/snapshots/{version}` | 导出完整快照 JSON |
| `POST` | `/api/snapshots/load` | 从 JSON 整体替换状态 |
| `POST` | `/api/snapshots/{version}/load` | 加载本库已存快照 |
| `GET` | `/api/recovery/report` | 最近一次恢复报告 |
| `POST` | `/api/recovery/scan` | 手动扫描未完成意图 |
| `GET` | `/api/status` | 状态版本、窗口计数、快照版本等 |
| `GET` | `/api/health` | 健康检查 |

失败响应统一为：

```json
{
  "code": "E_LATE_EVENT",
  "message": "event targets a non-OPEN window and late_policy=reject",
  "context": {
    "batch_id": "...",
    "event_id": "...",
    "target_window_id": "...",
    "target_state": "CLOSED"
  }
}
```

错误码包括 `E_MEASURE_TYPE_MISMATCH`、`E_INVALID_TRANSITION`、`E_LATE_EVENT`、`E_SNAPSHOT_INVALID`、`E_CONFLICTING_BATCH`，以及 `E_OUT_OF_ORDER`、`E_DUPLICATE_EVENT`、`E_INVALID_WINDOW_SPEC`、`E_BATCH_ABORTED`、`E_INVALID_REQUEST`、`E_NOT_FOUND`、`E_INTERNAL`。

## 固定样本

- `samples/events_integer.json`：整数段。
- `samples/events_decimal.json`：scale=2 定点小数段。
- `samples/events_integer_again.json`：切回整数。
- `samples/events_late.json` / `events_late_reject.json`：迟到队列和拒绝策略。
- `samples/events_nonmonotonic.json`：乱序排序场景。
- `samples/events_crash.json` / `events_continuation.json`：崩溃批次与恢复后续算。
- `samples/snapshot_empty_v1.json`：带有效摘要的空快照。
- `samples/window_specs.json`：三类窗口规格。

## 实际验证结果

2026-11-23 在 Python 3.11.18、SQLite 3.53.1 本地文件上执行 `./scripts/run_demo.sh`：

- 演示通过：整数 -> 定点小数 -> 整数演化序列为 `integer(0) -> decimal(2) -> integer(0)`。
- 滑动窗口验证到多个重叠窗口，混合类型窗口输出例如 `sum=1.25`、`scale=2`、`rounding=none`。
- 关闭 9 个窗口后提交迟到事件，产生 2 条迟到队列记录，已关闭窗口聚合不变；`reject` 策略返回 `E_LATE_EVENT` 且状态版本不变。
- 快照 v1 整体加载后，快照后新增窗口消失，`snapshot_diff=[]`。
- 服务在批次事务中被强制终止，监督进程自动重启；恢复报告将 `sample-crash-007` 标记为 `ABORTED`，`durable_facts_only=true`。
- 重启后提交续算批次，与未中断基线窗口逐项比较 `recovery_diff=[]`。
- 演示探测并输出 `E_MEASURE_TYPE_MISMATCH`、`E_CONFLICTING_BATCH`、`E_LATE_EVENT`、`E_INVALID_TRANSITION`、`E_SNAPSHOT_INVALID`。
- `pytest`：17 项测试全部通过。

验证原文：`artifacts/verification.txt`；恢复报告：`artifacts/demo-recovery-report.json`；演示摘要：`artifacts/demo-summary.json`；演示快照：`artifacts/demo-snapshot-v1.json`；服务重启日志：`artifacts/demo-server.log`。

## 实现说明与边界

- 分位扩展为精确 `nearest_rank`，会保留窗口样本值；主要四项指标仍是增量维护，不依赖该样本列表。
- 同一批次内乱序事件严格按声明策略处理。跨批次回看只会合并到仍为 OPEN 的窗口，否则进入迟到队列或拒绝。
- 会话窗口按分组维护当前切分状态；时间顺序推进时自动关闭过期会话。闭区间边界会允许端点事件归属相邻窗口，接口中始终回显该语义。
- SQLite 是唯一持久化介质；`data/*.db` 属于运行时文件，不纳入版本控制。
