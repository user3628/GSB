# SQLite 慢查询诊断与改写工作台

面向本地 SQLite 数据的慢查询诊断、规则改写与一致性验证工作台。项目完全离线运行，提供 Flask HTTP API、SQLite 持久化、可审计状态轨迹、强制终止后的启动恢复报告，以及固定样本的端到端演示。

## 核心能力

- 加载并归一化慢查询样本；整批校验失败时一个样本也不入库。
- 使用带摘要校验的统计快照判定慢因：缺少索引、选择率估算偏差、锁等待、返回行数过多，并补充顶层 OR 结构识别。
- 生成四类可执行改写：`ADD_INDEX`、`REORDER_PREDICATES`、`SPLIT_OR`、`ADD_LIMIT`。
- 在临时 SQLite 副本上执行原查询、DDL 与改写，比较计划代价和完整排序行集合摘要。
- 状态机持久化在 SQLite 中，建议与 `SUGGESTED` 同事务提交，数据库触发器阻止非法状态。
- 写操作先提交意图记录；重启扫描未完成意图。中断的分析恢复到 `CREATED`，中断的验证标记 `FAILED`，绝不直接标记 `VERIFIED`。
- 统计快照按版本整体替换；旧会话永久绑定分析时版本，退休版本不能解释新会话。

## 快速开始（Windows）

1. 安装 Python 3.11，并在安装器中启用 `py` launcher。确认：

```bat
py -3.11 --version
```

2. 在项目根目录执行初始化（创建 `.venv`、安装 Flask/pytest、生成固定样本并运行测试）：

```bat
setup_windows.bat
```

3. 运行完整本地演示：

```bat
run_demo.bat
```

演示会启动真实 localhost HTTP 服务，依次提交样本与快照、诊断、验证、替换快照，并用两个子进程在 `ANALYZING` 中途执行 `os._exit(86)`，随后重启并查询恢复报告。正常结束输出 `DEMO PASS`。

4. 启动常驻 API：

```bat
run_server.bat
```

默认监听 `http://127.0.0.1:8765`，数据库为 `artifacts\workbench.sqlite3`，样本库为 `data\slowquery_sample.sqlite3`。

### Linux / macOS / 当前验证环境

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python scripts/build_fixtures.py
python -m pytest -q
python scripts/demo.py
python run.py serve --database artifacts/workbench.sqlite3 --sample-db data/slowquery_sample.sqlite3
```

## 配置

启动路径可通过命令行、JSON 配置文件或环境变量指定：

```bash
python run.py serve \
  --database artifacts/workbench.sqlite3 \
  --sample-db data/slowquery_sample.sqlite3 \
  --host 127.0.0.1 \
  --port 8765
```

```bash
python run.py serve --config config.example.json
```

支持的环境变量：`SLOWQUERY_DB`、`SLOWQUERY_SAMPLE_DB`、`SLOWQUERY_HOST`、`SLOWQUERY_PORT`。完整配置项见 `config.example.json`。

阈值均可配置：慢查询耗时、扫描行数、锁等待、返回行数、选择率偏差比例、验证 LIMIT 上限和结果行上限。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/health` | 健康状态、路径与活动快照版本 |
| `POST` | `/api/samples` | 提交样本数组，或 `{"samples": [...]}` |
| `POST` | `/api/snapshots` | 提交带摘要的统计快照并整体替换活动版本 |
| `GET` | `/api/snapshots/active` | 查询活动快照摘要 |
| `POST` | `/api/sessions` | 创建或幂等获取会话，`{"session_id":"...","sample_id":"..."}` |
| `POST` | `/api/sessions/{id}/diagnose` | 触发诊断；可选 `{"snapshot_version": 1}` |
| `GET` | `/api/sessions/{id}` | 查询状态、分析、建议、验证记录与状态轨迹 |
| `GET` | `/api/sessions/{id}/suggestions` | 查询已提交建议 |
| `GET` | `/api/sessions/{id}/trace` | 查询状态事件轨迹 |
| `POST` | `/api/sessions/{id}/verify` | 验证建议；可选 `{"suggestion_id":"..."}`，缺省验证第一条 |
| `GET` | `/api/recovery/report` | 最近一次持久化启动恢复报告 |

统一错误体：

```json
{
  "error": {
    "code": "E_VERIFY_MISMATCH",
    "message": "Rewrite result set differs from the original query result set.",
    "details": {}
  }
}
```

| 业务码 | HTTP | 含义 |
| --- | --- | --- |
| `E_SAMPLE_INVALID` | 400 | 样本解析、类型、范围、时间或 SQL 校验失败；整批不入库 |
| `E_SNAPSHOT_INVALID` | 400 | 摘要、summary、结构或版本单调性失败；未加载快照时禁止诊断 |
| `E_SNAPSHOT_RETIRED` | 409 | 请求的旧快照已被替换，不得用于新诊断 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移或当前状态不允许该操作 |
| `E_SESSION_NOT_FOUND` | 404 | 会话不存在 |
| `E_VERIFY_MISMATCH` | 422 | 改写结果行集合或行数与原始查询不一致；会话转 `FAILED` |
| `E_VERIFY_ERROR` | 422 | SQLite 执行或资源处理失败；会话转 `FAILED` |
| `E_SESSION_CONFLICT` | 409 | 同一 `session_id` 绑定了不同样本 |
| `E_REQUEST_INVALID` | 400 | JSON 请求格式错误 |

## 状态机

```text
CREATED -> ANALYZING -> SUGGESTED -> ANALYZING -> VERIFIED
                     \-> FAILED <-----/
ANALYZING -> CREATED   （仅启动恢复：丢弃未提交分析）
```

- `VERIFIED` 与 `FAILED` 是终态，不能再进入 `ANALYZING`。
- `SUGGESTED` 与建议集合在同一 SQLite 事务中提交。
- 数据库触发器同时拒绝非法状态、慢查询 `SUGGESTED` 无建议、以及无验证记录的 `VERIFIED`。
- `GET /api/sessions/{id}` 的 `trace` 来自与状态更新同事务写入的 `session_events`。

## 慢因与改写规则

| 检测条件 | 慢因 | 生成建议 |
| --- | --- | --- |
| 等值谓词集合不能由单一索引覆盖 | `MISSING_INDEX` | `ADD_INDEX`，列按 NDV 从高到低排列 |
| 统计估算显著低于上报扫描行数且存在低 NDV 列 | `SELECTIVITY_ESTIMATE_BIAS` | `REORDER_PREDICATES` |
| 顶层 OR | `OR_PREDICATE` | `SPLIT_OR`，使用 `UNION` 保持行集合语义 |
| 锁等待达到阈值 | `LOCK_WAIT` | 复用索引/OR/LIMIT 建议；必要时生成锁窗口 LIMIT |
| 估算返回行数达到阈值且无 LIMIT | `EXCESSIVE_RETURNED_ROWS` | `ADD_LIMIT` |

慢查询判定阈值为 OR 关系：耗时、扫描行数或锁等待任一达到阈值即判定为慢查询。只有加载了统计快照才能生成建议；每条建议包含规则标识、预期收益、快照版本和可执行 SQL。

固定样本与预期结果：

| 样本 | 预期慢因 | 验证结论 |
| --- | --- | --- |
| `sq-missing-index` | 缺 `(customer_id,status)` 复合索引 | `VERIFIED`，代价 250 -> 5 |
| `sq-selectivity` | 选择率估算偏差 | `VERIFIED`，结果 0 行一致 |
| `sq-lock-wait` | 锁等待 + 返回行过多 | `VERIFIED`，247 行一致 |
| `sq-or-split` | 顶层 OR + 返回行过多 | `VERIFIED`，434 行一致 |
| `sq-many-rows` | 返回行过多 | `VERIFIED`，251 行一致 |
| `sq-full-scan` | 返回行过多 | `FAILED / E_VERIFY_MISMATCH`，1000 -> 500 行 |
| `sq-fast` | 非慢查询 | 无建议 |

## 验证与资源释放

验证器把样本库通过 SQLite backup API 复制到临时文件，在同一临时副本上：

1. 对原查询执行 `EXPLAIN QUERY PLAN` 并计算扫描行代价估计。
2. 执行原查询，生成包含列名和完整行多重集的排序摘要。
3. 在临时副本执行建议 DDL，再执行改写 SQL。
4. 比较行数、摘要和代价；不一致返回 `E_VERIFY_MISMATCH`，执行异常返回 `E_VERIFY_ERROR`。
5. 在 `finally` 中关闭源连接、复制目标连接、验证连接并删除临时数据库。验证记录的 `detail.resources` 给出四个资源释放标记。

验证不使用 sleep、虚假超时或未关闭连接的“等待”来伪造结果。

## 持久化、恢复与并发

- SQLite 使用 WAL、`synchronous=FULL`、显式 `BEGIN IMMEDIATE`；每个 API 操作使用独立连接。
- 每个写操作先提交一条 `operation_intents` 意图，再在业务事务中修改数据并把意图标为完成。
- 同一会话的诊断/验证由会话锁串行化，进程内所有写操作再由单写者锁串行；只读请求不获取写锁，可从 WAL 快照并发读取。
- 重复诊断同一会话只执行一次；后到请求等待并读取已提交结果。
- 重启扫描 `PENDING` 意图：中断分析恢复 `CREATED`；中断验证标记 `FAILED`，无验证记录则绝不为 `VERIFIED`。
- 恢复报告只读取已提交的 intents、sessions、events、suggestions 和 verification_records，并报告三项不变量。
- 快照表保留历史版本供旧会话验证；活动指针原子切换。新诊断只允许活动版本，旧版本请求返回 `E_SNAPSHOT_RETIRED`。

详细设计见 `docs/ARCHITECTURE.md`。

## 固定样本与可重复性

```bash
python scripts/build_fixtures.py
```

该命令重建 `data/slowquery_sample.sqlite3`、`samples.json`、`snapshot_v1.json`、`snapshot_v2.json`。连续重建的样本库 SHA-256 固定为：

```text
41ccb3502b2e30658fbd4f9a97498662977458ce547c525f6c158de704c743d8
```

## 验收结果

```text
38 passed in 4.56s
```

完整演示输出保存在 `artifacts/demo_output.txt`，关键结论与运行环境记录在 `docs/VERIFICATION.md`。演示覆盖成功验证、结果不一致失败、资源释放、快照退休、重复复用，以及两次真实进程强杀后的恢复报告。

## 范围说明

- 工作台仅接受单条只读 `SELECT`/`WITH ... SELECT` 样本，验证始终针对临时数据库副本。
- SQL 规则解析面向常见 SQLite `SELECT ... FROM ... WHERE ...` 形态；复杂 CTE/JOIN 查询仍会原样执行验证，但慢因识别可能较少。
- 建议代价来自 `EXPLAIN QUERY PLAN` 与快照 NDV 的确定性估算，不依赖机器负载或实际耗时。
- 推荐每个工作台数据库只运行一个服务进程；SQLite 仍会以事务和 WAL 保护已提交一致性。
