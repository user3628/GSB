# 本地分片上传与元数据一致性组件

面向本地对象存储场景的分片上传组件，供上层领域数据编辑/操作应用集成。纯本地
SQLite 持久化，不依赖任何真实对象存储服务或外部网络；样本固定生成、可重复复现。

## 1. 环境与安装

要求 Python 3.11+（开发验证环境为 Python 3.14），SQLite 由标准库 `sqlite3` 提供。

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
python scripts/make_samples.py     # 生成固定样本与 SHA-256 清单
python -m scripts.init_db --reset  # 生成空库（WAL 模式，6 张业务表）
```

关键环境变量：`SHARD_STORE_DB`（数据库路径，默认 `data/shard_store.db`）、
`SHARD_STORE_SAMPLES`（样本目录，默认 `samples/`）。

## 2. 目录结构

```
shard_store/
├── app/
│   ├── config.py                 # 配置与分片序号/大小常量
│   ├── errors.py                 # 业务码注册表 + 统一 JSON 错误信封
│   ├── utils.py                  # SHA-256、时间戳、ETag 语义
│   ├── db/
│   │   ├── schema.py             # 6 张表的 DDL（WAL、外键、检查约束）
│   │   └── connection.py         # 连接工厂与显式事务（BEGIN IMMEDIATE）
│   ├── repositories.py           # 会话/分片/对象/意图 四个仓储（全部 SQL）
│   ├── services/
│   │   ├── locking.py            # 会话级、对象键级进程内写锁注册表
│   │   ├── session_service.py    # 会话生命周期与冲突策略
│   │   ├── part_service.py       # 分片校验、幂等、乱序容忍
│   │   ├── merge_service.py      # 两阶段意图提交 + fast/full 双路径
│   │   ├── object_service.py     # 当前版本指针与历史版本查询
│   │   └── recovery_service.py   # 未完成意图扫描与恢复报告
│   ├── api/
│   │   ├── schemas.py            # Pydantic 请求模型
│   │   └── routes.py             # HTTP 路由
│   └── main.py                   # FastAPI 入口，lifespan 启动即恢复
├── scripts/
│   ├── init_db.py                # 数据库初始化/重置
│   ├── make_samples.py           # 确定性样本 + manifest.json
│   └── demo_e2e.py               # 真实 uvicorn 子进程端到端演示
├── tests/                        # 28 个 pytest 用例（按 7 条业务要求组织）
├── samples/                      # 固定本地分片样本与 SHA-256 清单
├── requirements.txt
├── run_demo.sh / run_demo.bat
└── README.md
```

## 3. 数据模型

| 表 | 职责 |
| --- | --- |
| `upload_sessions` | 会话：对象键、状态（open/merging/completed/cancelled/aborted）、分片大小上下界、期望总大小、策略、计数器 |
| `parts` | 分片清单：`(session_id, part_number)` 主键、字节大小、SHA-256、上传时刻 |
| `part_bodies` | 分片原始字节（与清单分离，完整校验路径重算摘要时读取） |
| `object_versions` | 不可变对象版本：`(object_key, version)` 主键、总大小、分片数、对象摘要、ETag |
| `objects_current` | 每键当前版本指针（旧版本保留但不再被读取为当前版本） |
| `merge_intents` / `recovery_reports` | 合并意图日志（pending/committed/aborted）与恢复报告 |

对象摘要公式（两条路径共用，保证结果一致）：
`object_sha256 = sha256( concat( 各分片 sha256 十六进制串，按 part_number 升序 ) )`
ETag 语义：`"<对象摘要前16位>-<分片数>"`。

## 4. 业务码与统一错误响应

所有错误（含参数校验）均返回统一信封：

```json
{"ok": false, "error": {"code": "E_PART_MISSING", "message": "...", "details": {...}}}
```

| 业务码 | 触发条件 | HTTP |
| --- | --- | --- |
| `E_SESSION_CONFLICT` | 同键存在进行中会话且策略为 reject；或会话处于 merging | 409 |
| `E_SESSION_NOT_FOUND` / `E_OBJECT_NOT_FOUND` | 会话/对象不存在 | 404 |
| `E_SESSION_COMPLETED` / `E_SESSION_CANCELLED` / `E_SESSION_ABORTED` | 对终态会话做非法操作 | 409 |
| `E_PART_INVALID` | 序号越界、空分片、超出大小上下界、非法 base64/参数 | 400 |
| `E_PART_MISMATCH` | 同序号重传内容摘要不一致，保留首次提交 | 409 |
| `E_DIGEST_MISMATCH` | 客户端摘要不符，或完整路径重算摘要与记录不符 | 409 |
| `E_PART_MISSING` | 合并时分片序号不从 1 连续 | 409 |
| `E_SIZE_MISMATCH` | 分片大小之和与期望总大小/会话计数器不一致 | 409 |
| `E_SIMULATED_CRASH` | 崩溃注入点（意图已落库、元数据未写） | 503 |
| `E_REQUEST_INVALID` | Pydantic 请求体校验失败 | 422 |

## 5. 关键设计如何对应业务要求

1. **会话与分片模型**：创建时 `reject` 直接返回 `E_SESSION_CONFLICT` 并附已有
   会话 ID；`allow` 允许并存，规则是“并行上传、合并按对象键串行、各自领取递增
   版本号”。非法序号/大小在加锁与任何写入之前抛 `E_PART_INVALID`，会话零改动。
   末分片可低于下界，前提是恰好补满 `expected_total_size`。
2. **幂等与乱序**：`(session_id, part_number)` 主键保证同序号只存一条；摘要一致
   返回 `status=idempotent`，不一致返回 `E_PART_MISMATCH` 且首次提交保留。清单
   一律按 `part_number` 排序返回，合并定序只看序号。
3. **合并前校验**：在只读连接上先校验连续性、摘要记录、总大小，任何失败都在写
   元数据之前返回，因此不可能出现“对象可见但清单不完整”。
4. **原子提交与版本**：合并分两个事务——事务一写 pending 意图并把会话置
   `merging`；事务二在同一 `BEGIN IMMEDIATE` 事务内写 `object_versions`、更新
   `objects_current` 指针、意图置 committed、会话置 completed。重复合并返回
   `E_SESSION_COMPLETED` 并在 details 中带回已有结果。版本号由
   `MAX(version)+1` 在写事务内分配，复合唯一键保证不重复。
5. **双路径等价**：
   - fast：直接使用 `parts` 表持久化的每片 SHA-256 与上传时刻作为证据，输出
     `source_table`、证据清单与公式；
   - full：逐片从 `part_bodies` 重算 SHA-256，输出每片耗时与总耗时；
   两者摘要公式相同，28 个测试与演示均断言摘要/元数据一致，差异仅在覆盖度与耗时。
6. **并发与可见性**：进程内按会话 ID、对象键维护可重入锁；SQLite WAL 允许读不
   阻塞写、写不阻塞读，所有写事务 `BEGIN IMMEDIATE` 串行化。分片行与计数器
   刷新在同一事务提交，读端只能看到完整提交版本。同键并发合并由对象锁 +
   `next_version` 写事务共同保证版本 1..N 不重复。
7. **崩溃恢复**：强制终止后磁盘上只剩 pending 意图 + merging 会话、无对象元数据。
   新进程 lifespan 启动即扫描（也可 `POST /api/v1/recovery/run` 手动触发），
   对仍处于 merging 的会话重新置为 open 并把旧意图标 aborted（可重试），输出
   仅基于持久化事实的报告；对状态已偏离的意图直接标 aborted。恢复后重传分片
   仍在，重新合并得到与中断前相同的对象摘要。组件不承诺 SIGKILL 后生成完整
   退出报告。

## 6. HTTP API 一览

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/v1/sessions` | 发起会话（可带大小约束、期望总大小、策略、冲突策略） |
| `GET  /api/v1/sessions` / `GET /api/v1/sessions/{id}` | 会话列表/详情 |
| `POST /api/v1/sessions/{id}/parts/{n}` | 提交分片（base64，可选客户端 sha256） |
| `GET  /api/v1/sessions/{id}/parts` | 分片清单（按序号排序） |
| `POST /api/v1/sessions/{id}/merge` | 合并，body 可选 `strategy` 与 `simulate_crash_after_intent` |
| `POST /api/v1/sessions/{id}/cancel` | 取消会话（幂等） |
| `GET  /api/v1/objects/{key}` | 当前对象元数据 |
| `GET  /api/v1/objects/versions/{key}` | 版本历史；加 `?version=N` 查单版本 |
| `POST /api/v1/recovery/run` / `GET /api/v1/recovery/reports` | 恢复扫描/报告 |
| `GET  /api/v1/health` | 健康检查；交互式文档 `/docs` |

启动服务：`uvicorn app.main:app --host 127.0.0.1 --port 8000`。

## 7. 验证方式与结果

```bash
python -m pytest          # 28 个用例：乱序、幂等、缺口、大小、双路径、并发、恢复
python scripts/demo_e2e.py  # 真实 uvicorn 子进程的五个规定端到端场景
# 或一键执行：./run_demo.sh（Windows 用 run_demo.bat）
```

实际验证结果见仓库同级 `VERIFICATION.md`（最近一次运行的完整输出摘录）。
演示固定断言：alpha 对象摘要 `31db61a0e66c8fec7fad81f79ce06204e40db81cf961bc7a4e2024c17e5614a3`
在乱序 fast 合并、full 合并、崩溃恢复后重合并三种路径下完全一致。

## 8. 范围与约束

- 不使用 Docker 或任何容器；全部数据来自 `samples/` 固定样本，无外部网络调用。
- 进程内写锁覆盖单进程多线程场景；多进程写入由 SQLite 单写者锁、
  `BEGIN IMMEDIATE` 与 busy_timeout 兜底，版本号分配在数据库层仍具备唯一性。
- 崩溃注入通过合并请求参数实现，效果与 SIGKILL 后磁盘状态等价（意图已提交、
  元数据未提交），便于确定性、可重复地演示恢复语义。
