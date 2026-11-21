# 本地训练实验追踪组件 — 技术说明

面向本地训练实验的可复现追踪组件。无容器、无外部数据库服务、无外部追踪
平台；仅依赖 Python 3.11+ 标准库（`sqlite3` / `http.server` / `struct` /
`zlib` / `threading`）与 NumPy（仅用于分位数计算）。

## 1. 模块结构

| 模块 | 职责 |
| --- | --- |
| `exp_tracker/errors.py` | 统一业务错误码与 JSON 错误体 |
| `exp_tracker/models.py` | `Run` / `MetricPoint` / `Artifact` 领域模型 |
| `exp_tracker/versions.py` | 段式工具链版本解析与三态比较 |
| `exp_tracker/timeutil.py` | 毫秒/微秒归一化、舍入记账、保留期换算 |
| `exp_tracker/binary.py` | 紧凑二进制 ETRK 编解码 |
| `exp_tracker/jsonl_fmt.py` | 分片 JSON 行 ETJL 编解码 |
| `exp_tracker/negotiation.py` | 显式兼容矩阵与协商裁决 |
| `exp_tracker/store.py` | SQLite 存储、派生缓存、快照、意图与恢复 |
| `exp_tracker/samples.py` | 固定种子确定性样本与 CRC 篡改工具 |
| `exp_tracker/server.py` | 仅回环的 HTTP 接口（单写者多读者） |
| `scripts/demo.py` | 可复现端到端演示 |
| `tests/` | 51 个单元/集成/接口测试 |

## 2. 两种持久化格式

### 2.1 紧凑二进制 ETRK

文件头与记录结构：

```
MAGIC b"ETRK"(4B) | format_version(1B) | endian(1B, 0x01 LE / 0x02 BE)
record := length_prefix(uint32) | payload(length_prefix B) | crc32(uint32)
terminator := length_prefix == 0(uint32)
```

- 所有多字节整数遵循头部端序标记；
- 每条记录 payload 以 16 位可选字段存在位图开头，缺省可选字段零字节；
- `length_prefix` 与实际剩余字节不符或 `crc32(payload)` 不符，抛出
  `E_RECORD_CORRUPT`，上下文带 `record_index`；
- v1 指标时间为毫秒整数（精度更低），v2 为微秒整数并增加降级日志字段。

### 2.2 分片 JSON 行 ETJL

- 每行一个运行对象，行内自带 `format_version`，可独立检视；
- 必含运行标识、实验名、指标序列、工具链版本；
- `params` 在 v1 允许整段缺省；指标时间为微秒整数；
- 每行含相对保留期 `retention_seconds` 与记录自带 `base_time_us`，导入时
  据此重算绝对过期时刻 `expires_at_us`；
- 非法 JSON / 缺必填字段抛出 `E_RECORD_CORRUPT` 并带行号。

### 2.3 互操作

两种格式导出再导入后，参数、指标序列（步号/数值/时刻）、工具链版本、制品
摘要逐字段还原；测试 `tests/test_interop.py` 同时校验二进制→存储→再导出
JSON、JSON→存储→再导出二进制两条路径的字段保真。

## 3. 版本协商与兼容矩阵

矩阵在 `negotiation.COMPATIBILITY_MATRIX` 中显式声明，每种
`(kind, version)` 唯一映射到三种结论：

| kind | version | 结论 | 说明 |
| --- | --- | --- | --- |
| binary | 2 | accept | 当前格式，微秒精度 |
| binary | 1 | downgrade_accept | 毫秒精度，记录降级明细 |
| jsonl | 2 | accept | 当前格式 |
| jsonl | 1 | downgrade_accept | 可能缺参数，回填默认值 |
| jsonl | 0 | reject | 已退役 → `E_VERSION_RETIRED` |
| 任意 | 矩阵外/未来版本 | reject | `E_FORMAT_UNSUPPORTED`，整批不入库 |

- 降级时回填 `negotiation.DEFAULT_PARAMS`，并在每条运行上附加
  `downgrades` 明细（回填了哪些键、精度从 ms 投影等）；
- `POST /precheck` 只读文件头/首行并应用矩阵，不打开任何写事务，保证不
  改变已提交状态。

## 4. 时间与版本语义归一化

- 内部统一为微秒整数；二进制毫秒经 `ms_to_us` 放大，JSON 微秒直接使用；
- 截断规则 `truncate` / `round` / `preserve` 由调用方声明；
  `normalize_batch` 输出每条记录的舍入位 `rounded_digits`、丢弃量
  `discarded_us` 与批次累计误差 `cumulative_error_us`；
- 相对保留期经 `retention_to_expiry(base_time_us, retention_seconds)`
  换算为绝对过期时刻；
- 工具链版本按数字核心段（`2.10 > 2.9`）+ 预发布段（`2.1.0-rc3 < 2.1.0`，
  预发布标识自然序 `rc10 > rc3`）比较，输出 lower/equal/higher，比较结果
  反对称，不依赖输入顺序。

## 5. 冲突解决与批次原子生效

`store.resolve_conflict` 的确定性裁决顺序：

1. 工具链版本更高 → `overwrite`，更低 → `skip`；
2. 版本相同且指标摘要签名一致 → `skip`；
3. 版本相同、签名不同：以最后写入时间打破平局，新者覆盖、旧者跳过；
4. 全部相同仍冲突 → `reject`，单运行返回 `E_CONFLICTING_RUN`。

整批导入先对稳定读快照裁决全部记录；只要出现任一 `reject`，不打开数据写
事务、整批不入库，并以独立事务写入 `committed=false` 的报告，因此报告不会
与已提交状态矛盾。提交路径在单事务内完成行替换、摘要与索引刷新，失败整体
回滚。

## 6. 派生缓存增量更新与级联重建

- `metric_summaries` 按 `(experiment, toolchain_version)` 维护计数与分位数；
- `param_index` 按参数键维护引用它的运行集合；
- 新增/覆盖运行只把受影响的 `(实验, 版本)` 摘要与受影响参数键加入集合，
  针对这些条目重建，不做全量重建；
- `cascade_rebuild(experiment)` 在同一事务内删除并重建该实验全部摘要与
  参数索引，提交前读者仍看到旧版本，杜绝“摘要已更新、索引仍指旧运行”的
  中间可见状态（SQLite 单事务原子性保证）。

## 7. 快照整体替换

快照为携带 `snapshot_format_version`、`snapshot_seq`、`generated_at_us` 的
JSON 文件。`load_snapshot`：

1. 解析失败/字段非法 → `E_SNAPSHOT_INVALID`；
2. 快照格式版本不在支持集合 → `E_SNAPSHOT_INVALID`；
3. 快照序号低于当前序号 → `E_STALE_BASE`（单调不回退）；
4. 在单个 `BEGIN IMMEDIATE` 事务内清空并重建运行表、指标点、制品、参数索引
   与摘要，再推进版本号。

事务提交前旧快照始终可读，任何异常回滚，不出现新旧混合。

## 8. 并发模型与强制终止恢复

- 写者连接由进程内 `RLock` 串行化（单写者）；读者使用线程本地、
  `query_only` 的独立连接，配合 WAL 实现多读者并发读已提交版本；
- 每次写先持久化 `intents` 意图（含请求标识、基线版本、载荷），再在事务内
  修改数据；同一 `request_id` 重复提交只执行一次，其余请求等待并复用已持久
  化结果（事件仅在意图仍 pending 时等待，避免完成后事件丢失死锁）；
- 进程被强制终止时，SQLite 保证未提交事务不留任何部分行；重启扫描
  `state=pending` 的意图，依据“无对应已提交事务”这一持久化事实标记
  `aborted`，存储保持在最后一个完整版本，恢复报告只陈述已持久化事实。

## 9. 统一错误码与 HTTP 接口

错误响应统一为：

```json
{"error": {"code": "E_...", "message": "...", "context": {...}}}
```

覆盖 `E_RECORD_CORRUPT`、`E_FORMAT_UNSUPPORTED`、`E_CONFLICTING_RUN`、
`E_SNAPSHOT_INVALID`、`E_STALE_BASE`、`E_VERSION_RETIRED`，另含
`E_NOT_FOUND` / `E_INVALID_REQUEST`。服务仅绑定 `127.0.0.1`，非回环地址
直接拒绝构造。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` `/runs` `/runs/{id}` | 健康检查、运行查询 |
| GET | `/summaries` `/params?key=` | 摘要与参数依赖索引 |
| GET | `/reports` `/reports/{id}` `/recovery` | 冲突报告与恢复结论 |
| POST | `/runs` | 提交运行（支持 `expected_revision` 幂等） |
| POST | `/precheck` | 无副作用协商预检 |
| POST | `/imports` | 协商 + 原子批量导入 |
| POST | `/normalize` | ms/us 归一化与舍入记账 |
| POST | `/cascade` | 摘要/索引级联重建 |
| POST | `/snapshots/export` `/snapshots/load` | 快照导出与整体替换 |

## 10. 本地验证结果

- `python -m unittest discover -s tests`：全部通过（格式、协商、归一化、
  版本语义、冲突、原子批处理、级联、快照、并发幂等、恢复、HTTP）；
- `python scripts/demo.py`：确定性样本完成双格式导出、跨格式导入、jsonl v1
  参数回填降级、二进制 v1 毫秒降级、CRC 篡改返回 `E_RECORD_CORRUPT` 且版本
  号不变、重复导入确定性 skip、级联重建范围输出、坏快照保持旧状态、强制
  终止后 pending 意图被标记 aborted 且最后完整版本可用。

### Windows 复现步骤

```bat
scripts\setup_env.bat
scripts\run_tests.bat
.venv\Scripts\python.exe scripts\demo.py
```

所有样本由固定种子 `samples.SEED` 生成，演示仅访问 `127.0.0.1`，不依赖
任何外部账号或服务。
