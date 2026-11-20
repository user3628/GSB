# 单机训练检查点管理与恢复组件

这是一个面向单机训练编排器的本地检查点组件。它使用 SQLite/WAL 持久化检查点状态、迁移历史、写请求和裁剪意图，使用本地文件系统保存参数分片、优化器动量分片与清单文件，并通过 FastAPI 暴露回环 HTTP API。

组件不依赖 Docker、外部对象存储或网络服务。训练进程可以进程内集成 `CheckpointManager`，也可以通过本地 HTTP API 接入。

## 快速开始（Windows PowerShell）

要求 Python 3.11 或更高版本。

```powershell
cd <项目目录>
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"

# 运行自检
python -m pytest

# 运行完整演示：固定样本、HTTP 服务、裁剪、强杀、恢复
python -m checkpoint_recovery.demo
```

演示会自动生成/重建：

- `artifacts/samples/`：固定随机种子 `20261120` 的 6 轮参数与动量二进制样本。
- `artifacts/demo_run/`：SQLite 数据库、检查点目录、临时目录和意图目录。
- `artifacts/logs/`：三个 HTTP 服务子进程的日志。
- `artifacts/demo_output.json`：演示全过程的可核验结构化事实。

也可以单独启动服务：

```powershell
python -m checkpoint_recovery.server `
  --data-dir .\local-checkpoints `
  --port 8765 `
  --max-writing 2 `
  --max-checkpoints 3 `
  --min-round-interval 1
```

服务只允许绑定 `127.0.0.1`、`localhost` 或 `::1`。启动后可访问 `http://127.0.0.1:8765/docs` 查看 OpenAPI 文档。

## 交付内容

```text
src/checkpoint_recovery/
  manager.py         单写者队列、异步提交、幂等复用、裁剪、恢复
  state_machine.py   合法状态迁移与迁移记录
  db.py              SQLite schema/WAL、元数据与意图日志
  storage.py         临时分片、清单、摘要复核、原子目录提交
  api.py             FastAPI 路由与统一错误 JSON
  server.py          本地回环服务 CLI
  samples.py         确定性样本生成与 HTTP payload 装载
  demo.py            可重复端到端演示
tests/               14 项自动化测试
artifacts/           实际运行输出
```

## 持久化布局

```text
<data-dir>/
  checkpoints.db
  checkpoints/
    ckpt-<uuid>/
      manifest.json
      params/shard-00000.bin
      params/shard-00001.bin
      optimizer/shard-00000.bin
      optimizer/shard-00001.bin
  tmp/
    ckpt-<uuid>/...        # 未提交写入
  intents/
    prune-<uuid>.json      # 裁剪意图的独立耐久副本
```

`manifest.json` 记录轮次、调度器位置、数据游标、每类分片集合、相对路径、大小与 SHA-256。SQLite 同时保存清单摘要和分片摘要，恢复时会双重复核。

SQLite 表：

- `checkpoints`：检查点事实与当前状态；裁剪后保留 PRUNED tombstone 供审计。
- `checkpoint_shards`：COMMITTED 检查点的分片摘要；裁剪后删除文件元数据行。
- `transitions`：每次状态迁移的旧状态、新状态、原因、UTC 时间与详情。
- `write_requests`：写请求幂等结果，`PENDING/SUCCESS/ERROR`。
- `intent_log`：裁剪意图，`PENDING/DONE/ABORTED`。
- `recovery_runs`：启动恢复扫描事实。

## 检查点状态机

```text
                 commit protocol
WRITING ───────────────────────────────> COMMITTED
   │                                         │
   │ write/verify failure                    ├──> INVALID  (恢复复核失败/隔离)
   └──────────────────────────────────────> INVALID
                                             │
                                             └──> PRUNED (意图落盘后)

INVALID：终态
PRUNED ：终态
```

合法迁移：

| From | To |
| --- | --- |
| `WRITING` | `COMMITTED`, `INVALID` |
| `COMMITTED` | `INVALID`, `PRUNED` |
| `INVALID` | 无 |
| `PRUNED` | 无 |

创建检查点时另记录一次 `NULL -> WRITING` 审计事件。非法迁移返回 `E_INVALID_TRANSITION`，事务回滚，原状态和迁移记录均不变。手工 HTTP 隔离接口只允许把检查点转为 `INVALID`，不能绕过摘要协议直接提交或裁剪。

## 写入与提交协议

1. 请求进入后先校验 JSON、分片序号、Base64 与客户端声明摘要。声明摘要不符立即返回 `E_CHECKSUM_MISMATCH`，不创建检查点。
2. 在一个 SQLite 事务中原子创建 `WRITING` 检查点、`PENDING` 写请求和 `NULL -> WRITING` 迁移；同时检查当前 `WRITING` 数量是否超过 `max_writing`。
3. 请求进入进程内 FIFO 单写者队列后立即返回 HTTP 202。默认异步；编排器可传 `?wait=true` 等待最终结果。
4. 写者按提交顺序写临时分片，再写临时清单，随后重读全部文件并复核大小与 SHA-256。
5. 全部复核通过后，用同卷目录原子改名把临时目录发布为正式检查点目录。
6. 最后一个 SQLite 事务写入分片元数据，执行 `WRITING -> COMMITTED`，并同步把写请求置为 `SUCCESS`。
7. 在步骤 6 提交前，任何查询都只能看到 `WRITING`；不会出现清单已发布但分片未复核的 `COMMITTED` 查询结果。

崩溃窗口处理：

- 临时文件写一半、清单未完成：重启后 `WRITING -> INVALID`，删除临时产物。
- 目录已改名但 DB 未提交：正式目录没有 COMMITTED 元数据，重启扫描时作为 orphan 清理，恢复基点仍为旧检查点。
- DB 已提交但进程随后退出：DB 中的检查点和写请求结果都已完整，重启可直接作为恢复基点。

## 异步、幂等与并发

- 所有文件写入、提交、裁剪和恢复核验通过单写者队列和互斥域串行。
- 只读请求使用独立 SQLite 连接；WAL 允许多个查询并发。
- 同一 `request_id` 只创建一个检查点。首个提交默认立即返回；并发重复提交共享同一个 `Future`，即使未传 `wait=true` 也会等待原执行并复用终态结果，标记 `reused=true`。进程被强杀后的 `PENDING` 写请求会在重启时变成 `ERROR/E_PROCESS_TERMINATED`；重试应使用新的 `request_id`。
- `max_writing` 由 SQLite `BEGIN IMMEDIATE` 中的计数与插入原子保证，不依赖易失内存计数。

## 保留策略裁剪

自动裁剪算法按轮次从新到旧扫描：

1. 最新 COMMITTED 检查点永远保留，它是恢复基线。
2. 最多选择 `max_checkpoints` 个保留点。
3. 新保留点与上一个保留点的轮次间隔必须至少为 `min_retain_round_interval`。
4. 其余 COMMITTED 检查点进入裁剪候选。

每个候选的裁剪顺序：

1. 先把 `prune-<uuid>.json` 意图文件 `fsync` 到磁盘。
2. 在 SQLite 写入同 ID 的 `PENDING` 意图记录。
3. 执行 `COMMITTED -> PRUNED`，从此刻起恢复路径不再选择该检查点。
4. 删除正式目录中的清单与全部分片。
5. 清除 DB 中的 manifest 路径和分片行，保留 PRUNED tombstone 与状态历史。
6. 把意图标记为 `DONE`。

恢复扫描会根据持久化意图把中断的裁剪推进到安全终态。显式请求裁剪最新恢复基线会返回 `E_PRUNE_CONFLICT`；自动模式在 `selection.protected` 中明确列出最新基线及其他保留原因。

## 强制终止恢复与差异报告

新进程构造 `CheckpointManager` 时、开始接收流量前执行启动恢复：

1. 完成或中止遗留的 PENDING 裁剪意图。
2. 把所有无主 `WRITING` 检查点迁移为 `INVALID`，将写请求置为 `E_PROCESS_TERMINATED`。
3. 清理临时目录、未提交的正式目录、孤立意图文件和无关正式目录。
4. 写入一条 `recovery_runs` 记录，包含被隔离检查点、完成意图、清理对象与错误。

恢复基线计算会按轮次从新到旧复核清单摘要、分片存在性、大小、路径和 SHA-256：

- 最新 COMMITTED 完整：直接作为基线。
- 最新 COMMITTED 损坏：迁移为 `INVALID`，继续回退，并在报告中列出拒绝原因和最终回退点。
- 全部 COMMITTED 均不完整：返回 `E_CHECKPOINT_NOT_FOUND`。

差异报告包含恢复轮次、目标轮次、需重放轮次数、调度器步差、优化器动量可用性、参数/动量分片复核数量、数据游标是否需回退、回退链与最新启动恢复事实。报告明确设置：

```json
{
  "persisted_facts_only": true,
  "exit_report_available": false
}
```

即只承诺从已持久化事实恢复到完整检查点边界，不承诺强杀前的内存状态或完整退出报告。

## HTTP API

| Method | Path | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/checkpoints` | 提交写入；默认 202 异步，`?wait=true` 等待 |
| `GET` | `/write-requests/{request_id}` | 查询幂等写请求结果或错误 |
| `GET` | `/checkpoints?status=COMMITTED&limit=100` | 查询检查点及状态 |
| `GET` | `/checkpoints/{id}` | 查询单个检查点 |
| `GET` | `/checkpoints/{id}/transitions` | 查询状态迁移轨迹 |
| `POST` | `/checkpoints/{id}/transition` | 管理隔离为 INVALID；不允许手工提交/裁剪 |
| `POST` | `/prune` | 自动裁剪，或传 `checkpoint_ids` 显式裁剪 |
| `GET` | `/recovery/baseline` | 复核并返回最新完整基线 |
| `GET` | `/recovery/report?target_round=...` | 返回恢复差异报告 |
| `GET` | `/recovery/startup` | 返回本次启动恢复事实 |

写请求核心结构：

```json
{
  "request_id": "trainer-epoch-4-step-800",
  "round": 4,
  "scheduler_position": 153,
  "data_cursor": {"epoch": 1, "batch": 4, "sample_index": 512},
  "params": [
    {"ordinal": 0, "name": "shard-000.bin", "data_base64": "...", "sha256": "..."}
  ],
  "optimizer": [
    {"ordinal": 0, "name": "shard-000.bin", "data_base64": "...", "sha256": "..."}
  ]
}
```

统一错误响应：

```json
{
  "ok": false,
  "code": "E_CHECKSUM_MISMATCH",
  "message": "temporary checkpoint digest verification failed; commit was not attempted",
  "details": {}
}
```

| 业务码 | HTTP | 场景 |
| --- | --- | --- |
| `E_CHECKSUM_MISMATCH` | 409 | 声明摘要或落盘后摘要不一致 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移 |
| `E_WRITER_BUSY` | 429 | 达到 `max_writing` |
| `E_CHECKPOINT_NOT_FOUND` | 404 | 检查点/请求不存在或没有完整恢复基线 |
| `E_PRUNE_CONFLICT` | 409 | 试图裁剪保护中基线或非 COMMITTED 对象 |
| `E_INVALID_REQUEST` | 422 | 请求结构、状态过滤或字段不合法 |
| `E_WRITE_FAILED` | 500 | 不可恢复的文件写入错误 |
| `E_PROCESS_TERMINATED` | 410 | 强杀后遗留的未完成请求 |

## 样本与测试

样本由 `numpy.random.RandomState(20261120 + round * 1009)` 生成，每轮包含 2 个参数分片和 2 个优化器动量分片，另含调度器位置和数据游标。生成逻辑不接触网络。

测试覆盖：

- 固定样本可重复性与摘要。
- 异步提交后 WRITING 可见，最终 COMMITTED。
- 同一请求 6 线程并发提交只落盘一次并复用结果。
- 声明摘要错误和临时文件损坏均不破坏旧检查点。
- 非法迁移不改变状态或迁移历史。
- 最大保留数/最小轮次间隔与最新基线保护。
- 显式裁剪基线的 `E_PRUNE_CONFLICT`。
- 子进程临时分片写入后 `os._exit(91)` 强杀，重启隔离 WRITING 并恢复旧基线。
- 遗留 PENDING 裁剪意图在重启后完成。
- 最新检查点分片损坏后自动回退。
- HTTP 错误契约、写者忙、异步查询和幂等等待。

本次实际验证结果：

```text
14 passed, 1 warning in 3.56s
```

完整演示的关键实际输出：

```text
[write r1] submit=HTTP 202 immediate=WRITING ... trace=WRITING -> COMMITTED
[write r2] submit=HTTP 202 immediate=WRITING ... trace=WRITING -> COMMITTED
[write r3] submit=HTTP 202 immediate=WRITING ... trace=WRITING -> COMMITTED
[write r4] submit=HTTP 202 immediate=WRITING ... trace=WRITING -> COMMITTED
[prune] pruned=2,1 retained=4,3
[phase 1] persisted states=r4:COMMITTED, r3:COMMITTED, r2:PRUNED, r1:PRUNED
[phase 2] r5 accepted as WRITING ...; waiting for forced termination
[phase 2] writer process forcibly exited with code 91
[phase 3] persisted states=r5:INVALID, r4:COMMITTED, r3:COMMITTED, r2:PRUNED, r1:PRUNED
[phase 3] baseline round=4 state=COMMITTED fallback=False
[report] rounds_to_replay=2 optimizer_available=True digest=PASS cursor_rewind=True
[phase 3] crashed checkpoint trace=WRITING -> INVALID
```

本次验证运行在仓库提供的 WSL Ubuntu 环境，Python 3.14.4。实现仅使用跨平台 Python API、SQLite WAL、`os.replace` 与路径无关的 HTTP 回环绑定；上面给出的 PowerShell 命令为目标 Windows 本地运行方式。Windows 上目录 `fsync` 不可用时组件会安全降级为文件级 flush/fsync 加原子改名，不依赖 POSIX 专用命令。

## 边界与设计取舍

- 面向单机、单进程单写者；HTTP 服务必须以一个 Uvicorn worker 运行。多个独立写进程不在本组件承诺范围内。
- 只支持本地同卷文件系统，不支持网络文件系统上的跨卷原子改名。
- PRUNED 状态需要可查询审计，因此 DB 保留最小 tombstone；被移除的是可恢复文件、清单路径和分片行。
- 恢复只选择摘要完整且元数据已 COMMITTED 的边界；宁可回退到更早完整检查点，也不采用未提交或半写入目录。
- 故障注入参数仅用于演示/测试，生产运行不要启用 `--fault-crash-after-temp` 或 `--fault-corrupt-after-temp`。
