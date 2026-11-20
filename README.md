# MVCC 可见性裁决与编辑会话引擎

这是一个只使用 Python 标准库与 SQLite3 的本地领域数据编辑服务。它把行版本、事务提交序号、编辑会话、保存点、行写权等待和恢复意图都放在工作区内，不依赖 Docker、外部数据库或第三方 Python 包。

核心目标是让“打开编辑面板”与“保存”之间的版本校验、事务可见性快照和锁等待裁决保持一致：页面读取不会绕开事务快照直接读当前状态，保存也不会静默覆盖别人已提交的新版本。

## 快速开始

Windows PowerShell：

```powershell
cd <工作区>
powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows.ps1
.\.venv\Scripts\python.exe .\scripts\run_demo.py
```

WSL / Linux：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m unittest discover -s tests -v
python scripts/run_demo.py
```

启动服务：

```powershell
.\.venv\Scripts\python.exe .\scripts\run_server.py --port 0
```

服务只允许绑定 `127.0.0.1`、`::1` 或 `localhost`。使用端口 `0` 时，真实端口写入 `data/server-runtime.json`，同时标准输出会打印一行 `SERVER_READY {...}`。默认数据库为 `data/mvcc.sqlite3`，意图审计日志为 `data/intent-log.jsonl`。

## 目录

```text
mvcc_engine/
  engine.py        MVCC、编辑会话、保存点、锁等待、死锁与恢复
  storage.py       SQLite schema、固定样本初始化、意图持久化
  http_api.py      仅回环的 JSON HTTP 路由
  client.py        标准库 HTTP 客户端
  config.py        参数与工作区路径
samples/
  seed.json        固定业务表样本
  demo_params.json 超时、队列容量、事务上限等参数
  edit_example.json
scripts/
  setup_windows.ps1      创建 Windows 虚拟环境并跑测试
  run_server.py          服务入口
  editor_a.py            并发编辑器 A
  editor_b.py            并发编辑器 B
  concurrent_editors_demo.py
  force_termination_demo.py
  run_demo.py            全场景可重复演示
tests/
  test_engine.py
  test_http_api.py
artifacts/
  demo-report.json       最近一次完整演示的机器可读结果
  verification.md        实际验证记录
```

## 数据与可见性

`row_versions` 保留每个逻辑行版本的以下事实：

- `version_id`：物理版本主键。
- `table_name`、`row_id`：逻辑行标识。
- `row_version`：同一逻辑行内单调递增、供编辑面板携带的版本号。
- `created_txn`、`deleted_txn`：创建与删除该版本的事务。
- `commit_seq`：创建事务提交后写入的提交序号。
- `row_data_json`：完整 JSON 行值。

`committed_transactions` 单独记录 `txn_id -> commit_seq`。读取时同时连接创建事务与删除事务的提交事实，因此只有“创建已提交且删除未提交或尚未进入快照”的版本可见。

隔离级别：

| 级别 | 快照规则 | 冲突处理 |
| --- | --- | --- |
| `READ_COMMITTED` | 每条读取语句取最新提交序号 | 行写权被占时立即 `E_WRITE_CONFLICT` |
| `REPEATABLE_READ` | 事务首次读取时固定提交序号 | 有界等待，可超时或死锁牺牲 |
| `SNAPSHOT` | 事务首次读取时固定提交序号 | 有界等待，可超时或死锁牺牲 |

本事务自己的未提交版本对自身可见；其他事务只能看到完整提交版本。更新时，新版本插入与旧版本 `deleted_txn` 标记在同一 SQLite 写事务中完成；提交时，`commit_seq`、已提交事务表、事务状态和删除事实也在同一个写事务中落盘。因此读者不可能看到“新版本已提交但旧版本删除标记尚未写入”的中间状态。

## 编辑会话

`POST /txns/{txn_id}/edit-sessions` 返回：

- 当前可见行值与 `row_version` / `version_id`。
- 固定或语句级 `snapshot_id`。
- 当前行锁持有者和等待队列长度。
- 打开时间、过期时间与交互预算。

保存必须带打开时取得的 `row_version`。服务端先检查打开到保存的服务端时间差；超时返回 `E_EDIT_EXPIRED`，上下文包含 `opened_at`、`save_observed_at`、`elapsed_ms`、`budget_ms` 与判定依据。未超时则取得行排他写权，再以“当前完整提交状态”比较面板打开时的 `version_id`。若已被其他事务提交覆盖或删除，返回 `E_EDIT_STALE`，并附 `latest` 可编辑版本；删除后的最新版本为 `null`。

编辑会话本身也持久化在 SQLite 中。保存成功后同一会话会指向新版本，可继续编辑；事务提交或恢复时关闭会话。

## 保存点

保存点保存以下不可分割状态：

- 可见性快照序号。
- 写操作序号高水位。
- 完整编辑会话快照。
- 当时持有的全部行锁。

回滚到保存点时，在同一临界区和 SQLite 写事务中执行：

1. 逆序撤销保存点后的新版本插入与删除标记。
2. 删除保存点后新增的编辑会话，恢复已有会话状态。
3. 释放保存点后取得的锁，保留保存点前已持有的锁。
4. 保持事务可见性快照不变。
5. 将保存点后的开放意图标记为 `rolled_back`。

不会出现“版本回滚了但会话仍指向旧版本”或“快照被替换但事务仍在读取”的中间状态。

## 行锁、等待与死锁

- 锁粒度是 `(table_name, row_id)` 的排他写权。
- 全局 `threading.Condition` 保护锁授予、等待图、提交和保存点回滚。
- 等待使用 `Condition.wait(monotonic_remaining)`，锁释放、事务取消、牺牲或超时会 `notify_all()` 真唤醒；没有固定延迟轮询。
- `max_waiters` 限制全局等待队列容量，满时 `E_LOCK_QUEUE_FULL`。
- 等待超时返回 `E_LOCK_TIMEOUT`，包含锁对象、持有者、已等时长和等待原语。
- 等待图出现环时，选择环中 `txn_seq` 最大的事务作为确定性牺牲者，回滚其未提交版本、会话和锁，返回 `E_DEADLOCK_VICTIM`；其他等待者立即被唤醒并继续。

资源配置在 `samples/demo_params.json`：

```json
{
  "max_active_txns": 8,
  "max_waiters": 3,
  "lock_wait_ms": 900,
  "edit_budget_ms": 2500
}
```

## 意图与恢复

事务开启、编辑会话打开、行版本写入、锁授予、保存点与提交都会先写 `intents`，其中关键数据写入还会同步追加 `intent-log.jsonl` 并 `fsync`。意图状态在事务完成前保持开放。

进程被外部强制终止后，下一次启动先执行恢复，再绑定 HTTP：

- 扫描 `transactions.status='active'` 与未完成意图。
- 删除这些事务创建的行版本，清除其删除标记。
- 将相关编辑会话标为 `recovered_aborted`。
- 依据开放的锁意图释放全部逻辑写权。
- 将未完成事务标为 `recovered_aborted`，未完成意图标为 `recovered_rolled_back`。
- 保留所有已提交事务、提交序号、版本与意图。
- 写入 `data/recovery-report.json`，报告字段 `graceful_exit_report_available` 固定为 `false`，明确说明强制终止无法生成完整退出报告。

如果进程在 SQLite 提交事务中途被终止，SQLite 原子性保证要么看到完整提交，要么看到未提交前状态；恢复不会猜测未持久化事实。

## HTTP 摘要

所有失败响应统一为：

```json
{
  "ok": false,
  "code": "E_EDIT_STALE",
  "message": "the version shown in the edit panel is no longer the current committed version",
  "context": {
    "txn_id": "...",
    "latest": {"row_version": 2, "data": {"...": "..."}}
  }
}
```

主要路由：

| 方法与路由 | 用途 |
| --- | --- |
| `POST /txns` | 开启事务；可传 `txn_id`、`isolation` |
| `GET /txns/{id}` | 事务状态、快照与锁 |
| `POST /txns/{id}/read` | 事务内读取单行 |
| `POST /txns/{id}/query` | 事务内查询整表 |
| `POST /txns/{id}/edit-sessions` | 打开编辑会话 |
| `POST /txns/{id}/edit-sessions/{sid}/save` | 带版本保存 |
| `POST /txns/{id}/edit-sessions/{sid}/delete` | 带版本删除 |
| `POST /txns/{id}/savepoints` | 设置保存点 |
| `POST /txns/{id}/rollback-to` | 回滚到保存点 |
| `POST /txns/{id}/commit` | 单写者提交；请求体参与幂等指纹 |
| `POST /txns/{id}/rollback` | 回滚事务并唤醒等待者 |
| `GET /locks` | 锁、队列与等待图 |
| `GET /stats` | 资源与恢复摘要 |
| `GET /recovery/report` | 启动恢复报告 |

重复提交同一 `txn_id` 且请求体哈希一致时复用原提交结果并获得 `"replayed": true`；同一事务标识以不同隔离级别打开，或提交请求体不同，返回 `E_CONFLICTING_TXN`。

## 演示与验证

完整演示：

```bash
python scripts/run_demo.py
```

脚本会：

- 在独立数据目录启动回环 HTTP 服务。
- 并发运行 `editor_a.py` / `editor_b.py` 打开同一版本，A 提交后 B 保存得到 `E_EDIT_STALE` 与最新版本。
- 展示保存点后的第二次写入与会话被撤销，而保存点前写入、快照和锁保留。
- 展示 RC 立即 `E_WRITE_CONFLICT`、RR 真实等待超时 `E_LOCK_TIMEOUT`。
- 填满等待队列得到 `E_LOCK_QUEUE_FULL`，释放锁后三个等待者均被唤醒。
- 构造等待环，确定性牺牲年轻事务并唤醒另一事务。
- 展示不考虑版本的过期保存返回 `E_EDIT_EXPIRED`。
- 覆盖 `E_TXN_LIMIT`、`E_INVALID_ISOLATION`、`E_CONFLICTING_TXN`。
- 对独立服务进程执行外部 `kill`，重启后只基于持久化事实恢复并校验提交数据仍存在、未提交数据消失。

机器可读结果写入 `artifacts/demo-report.json`，实际验证摘要见 `artifacts/verification.md`。

## 说明与边界

- 面向单机、单服务进程；不提供跨进程分布式锁。
- HTTP 仅限回环地址，无认证，不应暴露到公网。
- 业务表数据以 JSON 对象保存，不建立每表列级 DDL；逻辑表名与行标识均经过白名单字符校验并参数化查询。
- 行写权只覆盖逻辑行；长事务仍可在不冲突的行上并发推进。
- 交互预算由服务端恢复的打开时间和保存到达时间计算，不信任客户端时钟。

