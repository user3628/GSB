# MSMQ Local：消息投递语义与格式互操作组件

这是一个完全本地运行的 SQLite 消息中间件实验组件。它提供两种持久化格式、显式版本兼容矩阵、至少一次/恰好一次投递、原子导入、并发领取与强制终止恢复，并附带确定性的 HTTP 验收工具。

- 运行边界：HTTP 只允许监听 `127.0.0.1`、`::1` 或 `localhost`。
- 外部依赖：仅 Python 包索引中的 FastAPI、Uvicorn、pytest、httpx；不使用 Docker、云服务或真实账号数据。
- 持久化：SQLite WAL，显式 `BEGIN IMMEDIATE` 写事务，`synchronous=FULL`。
- 数据模型：`queues`、`messages`、`deliveries`、`acks`、`intents`、`queue_snapshots`、`recovery_reports`、`import_reports`。

## Windows 快速开始

前置条件：Windows、Python 3.11+，并且 `python` 与 `pip` 在 PATH 中可用。

```powershell
python --version
python -m pip --version
Set-ExecutionPolicy -Scope Process Bypass
.\setup.ps1
```

`setup.ps1` 会创建 `.venv`、安装固定依赖、生成确定性样本并运行 pytest。若只准备环境：

```powershell
.\setup.ps1 -SkipTests
```

启动仅监听回环地址的服务：

```powershell
.\run_service.ps1 -Port 8765 -Db artifacts\msmq.sqlite3
```

浏览器打开 `http://127.0.0.1:8765/docs` 可查看 OpenAPI。运行完整验收：

```powershell
.\validate.ps1
```

在 PowerShell 中手工执行等价命令：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\generate_samples.py
.\.venv\Scripts\python.exe scripts\run_validation.py
```

## 项目结构

```text
msmq/
  api.py          HTTP 契约、统一错误体、回环监听入口
  service.py      队列、投递、确认、导入、导出业务规则
  db.py           SQLite schema、事务、意图与版本快照
  formats.py      紧凑二进制与 JSONL 编解码
  compat.py       显式兼容矩阵与格式容量
  recovery.py     重启时基于持久化事实恢复
  models.py       消息、队列与语义模型
scripts/
  generate_samples.py  确定性样本生成器
  run_validation.py    本地 HTTP 验收管线
  start_service.py     回环地址服务入口
tests/           27 项 pytest 覆盖
artifacts/
  samples/       固定样本、SHA-256 清单
  validation/    验收报告与本次导出文件
docs/            格式、兼容、恢复和 API 技术说明
```

## HTTP 接口

所有业务错误统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_CONFLICT_AMBIGUOUS",
    "message": "import conflict cannot be resolved deterministically",
    "details": {"rejected_index": 0, "state_changed": false}
  }
}
```

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 健康与数据库路径 |
| `GET` | `/v1/compatibility` | 返回显式兼容矩阵 |
| `POST` | `/v1/queues` | 创建队列并声明投递语义 |
| `GET` | `/v1/queues/{queue}` | 队列元数据 |
| `POST` | `/v1/queues/{queue}/migrate-semantics` | 唯一允许的语义迁移入口 |
| `POST` | `/v1/queues/{queue}/messages` | 入队 |
| `POST` | `/v1/queues/{queue}/claim` | 单写者串行领取 |
| `POST` | `/v1/queues/{queue}/ack?consumer_id=...` | 单条确认；重复时 HTTP 409 + 业务码 |
| `POST` | `/v1/queues/{queue}/acks` | 批量确认，逐条结果，允许部分成功 |
| `GET` | `/v1/queues/{queue}/snapshot` | 一致快照、计数与最新版本快照 |
| `GET` | `/v1/queues/{queue}/versions` | 持久化版本快照历史 |
| `GET` | `/v1/export/compact?queue=...` | 紧凑二进制导出 |
| `GET` | `/v1/export/jsonl?queue=...` | 分片 JSON 行导出 |
| `POST` | `/v1/import/compact?sub_ms_policy=truncate` | 二进制导入 |
| `POST` | `/v1/import/jsonl?sub_ms_policy=truncate` | JSONL 导入 |
| `GET` | `/v1/import/report/latest` | 最近一次导入报告 |
| `GET` | `/v1/recovery/report` | 最近一次启动恢复报告 |

更完整的请求/响应示例见 [docs/API.md](docs/API.md)。

## 关键语义

### 至少一次

领取后若在可见性超时前未确认，下一次领取会重新投递同一消息并递增 `delivery_count`。可见性超时是队列默认值，也可在单次领取时覆盖。

### 恰好一次

确认幂等键是 `(message_id, consumer_id)`。同一消费者重复确认返回 `E_DUPLICATE_ACK`，不新增确认行、不重复扣减待投递状态；另一消费者确认已确认消息返回 `E_ALREADY_ACKED`。两种模式共享存储，但队列不能随导入或运行而静默切换；只有显式迁移接口可以改语义，且有活跃租约时返回 `E_QUEUE_BUSY`。

### 原子导入与冲突

同一消息 ID 的冲突按以下顺序裁决：

1. 投递状态优先级：`acked > leased > pending > expired`。
2. 更高 `record_version` 胜出。
3. 更晚 `last_write_at_us` 胜出。
4. 三项完全相同时，载荷一致则跳过；载荷不同则 `E_CONFLICT_AMBIGUOUS` 拒绝整批。

插入、覆盖、跳过与拒绝均写入导入报告。任一条拒绝时事务整体回滚，返回 `rejected_index` 与业务码，绝不留下部分导入状态。

### 强制终止恢复

入队、领取、确认与导入都先单独提交一条 `pending` 意图，再在效果事务中同时提交业务变更和 `committed` 意图状态。进程在效果事务中被 `os._exit` 强杀时，未提交的 SQLite 效果由 WAL 丢弃；重启扫描 `pending` 意图并写入只基于 SQLite 已提交行与意图行的恢复报告。报告明确声明 `runtime_memory_used=false`、`crash_exit_report_available=false`。

## 本次实际验证结果

执行环境：Python 3.14.4，Linux 容器外工作区；核心代码只使用跨平台 Python API，Windows 入口为上述 PowerShell 脚本。当前环境没有 PowerShell，因此本次实测使用等价的 `python` 命令。

```text
$ .venv/bin/pytest -q
27 passed

$ .venv/bin/python scripts/run_validation.py
[PASS] format_roundtrip_equivalence
       compact_inserted=6, jsonl_inserted=6, messages=6
[PASS] compatibility_capacity_atomicity
       legacy_decision=degrade_accept
       legacy_fields_filled=6
       legacy_time_normalizations=1
       optional_fields_preserved=11
       future_rejection_code=E_FORMAT_INCOMPATIBLE
       oversize_batch_code=E_BATCH_SPLIT_REQUIRED
       oversize_body_code=E_RECORD_TOO_LARGE
       conflict_rejected_index=0
       atomic_conflict_inserted_after=0
[PASS] delivery_semantics_concurrency
       at_least_once_delivery_count=2
       exactly_once_duplicate_code=E_DUPLICATE_ACK
       exactly_once_ack_count=1
       batch_succeeded=1, batch_failed=1
       concurrent_claims=8, concurrent_winners=1
[PASS] forced_termination_recovery
       claim_crash_exit_code=97
       ack_crash_exit_code=97
       rolled_back_claims=1
       rolled_back_acks=1
       runtime_memory_used=false
       post_recovery_ack_succeeded=1
       final_ack_count=1
```

机器可读报告：`artifacts/validation/validation_report.json`。固定样本清单与 SHA-256：`artifacts/samples/manifest.json`、`artifacts/samples/checksums.sha256`。

## 技术说明

- [格式规范与兼容矩阵](docs/FORMAT_COMPATIBILITY.md)
- [恢复协议、并发与投递语义](docs/RECOVERY_AND_SEMANTICS.md)
- [HTTP API 与调用示例](docs/API.md)

## 已知边界

- 服务仅面向本机验证，不包含 TLS、鉴权或多主机复制。
- 消息体在 HTTP 层使用 UTF-8 文本；二进制格式仍以“长度前缀 + UTF-8 字节”存储。
- SQLite 使用单库写事务，因此队列领取通过全局 SQLite 写锁得到强于“队列单写者”的串行保证。
- 强制终止恢复不声称生成完整退出报告；它只报告重启后可证明的事实。

