# 单机混合精度检查点管理与精度策略组件

这是一个只依赖 Python 标准库、NumPy 和 Flask 的本地组件。所有张量由固定种子生成，不访问 GPU、外部数据集或网络服务；HTTP 服务只绑定回环地址。

核心实现位于 `mpc/service.py`，SQLite schema 位于 `mpc/db.py`，确定性数值契约位于 `mpc/numeric.py`，样本生成位于 `mpc/samples.py`。本次实验的完整事实记录在 `artifacts/demo_report.json`。

## 1. 环境与安装

目标环境为 Windows + Python 3.11 及以上；本次实际验证运行于 WSL/Linux 工作区，使用 Python 3.14.4。程序未使用平台专属的数据库或数值库。

`sqlite3` 随 Python 标准库提供，不需要也不应通过 pip 安装；额外第三方依赖只有 NumPy 与 Flask。实验全程不使用 Docker 或其他容器。

### Windows PowerShell（可复现安装）

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 生成固定种子样本分片（含一个未写满分片，正式清单不会包含它）
.\.venv\Scripts\python.exe scripts\generate_samples.py --output data\seed_20261122 --seed 20261122

# 占用并立即释放本机回环端口，确认端口可用
.\.venv\Scripts\python.exe scripts\check_port.py --host 127.0.0.1 --port 8765
```

也可运行 `powershell -ExecutionPolicy Bypass -File scripts\setup_windows.ps1` 完成上述步骤。安装清单另有 `requirements.lock`。

本次实际安装命令与版本：

```text
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install numpy flask

Python 3.14.4
numpy==2.5.5
Flask==3.1.3
Werkzeug==3.1.9
```

若端口已被占用，先关闭冲突进程。Windows 可用：

```powershell
Get-NetTCPConnection -LocalPort 8765 -State Listen | Select-Object OwningProcess
Get-Process -Id <OwningProcess>
```

本工作区的 8765/8766 端口已有其他进程监听，验证时没有终止未知进程，而是由演示脚本自动选择空闲回环端口；另外用 `scripts/check_port.py --port 19234` 实际确认了回环端口可绑定。

## 2. 启动 HTTP 服务与实验

```powershell
.\.venv\Scripts\python.exe -m mpc.server --host 127.0.0.1 --port 8765
```

另开终端执行完整可复现实验：

```powershell
.\.venv\Scripts\python.exe scripts\run_demo.py
```

演示脚本会：生成固定种子数据，经真实 HTTP API 完成全量策略与检查点提交，提交只改 `s1` 精度的增量策略，验证派生差异，然后启动 `mpc.crash_worker`，在 SQLite 事务写入途中的 `mid_rebuild` 点强制终止进程，重启服务扫描持久化意图并输出恢复报告。脚本最后写 `artifacts/demo_report.json`。

运行自动化测试：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -t . -v
```

## 3. HTTP API

成功响应统一为：

```json
{"ok": true, "data": { }}
```

失败响应统一为（同时保留顶层 `code` 便于客户端快速分派）：

```json
{
  "ok": false,
  "code": "E_CHECKSUM_MISMATCH",
  "error": {
    "code": "E_CHECKSUM_MISMATCH",
    "message": "fp16 checksum mismatch; checkpoint was not submitted",
    "details": {"shard_id": "s0"}
  }
}
```

接口清单：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/health` | 服务、策略、最新检查点状态 |
| GET | `/api/policies/current` | 当前完整策略 |
| POST | `/api/policies/snapshots` | 提交完整、带版本号的策略替换 |
| POST | `/api/policies/increments` | 提交仅含受影响分片/损失缩放字段的增量 |
| POST | `/api/policies/load-snapshot` | 从本地策略快照文件原子加载 |
| GET | `/api/policies/<version>/derived` | 按策略版本查询所有检查点派生摘要 |
| POST | `/api/checkpoints` | 提交检查点 |
| GET | `/api/checkpoints/latest` | 最新正式清单 |
| GET | `/api/versions/<version>/derived` | 按检查点版本查询分片派生摘要与偏差 |
| GET | `/api/checkpoints/<id-or-version>/derived` | 同上，提交 ID 或版本均可 |
| GET | `/api/checkpoints/<ref>/shards/<shard_id>/derived` | 单分片派生结果 |
| GET | `/api/recovery/report` | 最近一次恢复报告 |
| POST | `/api/recovery/scan` | 重新扫描 pending 意图 |
| GET | `/api/recovery/intents` | 意图持久化状态 |
| GET | `/api/commits` | 已持久化幂等提交索引 |

### 完整策略

`shards` 必须是全部正式分片，防止新旧精度标记混合。`version` 必须大于当前版本，否则 `E_STALE_SNAPSHOT`。

```json
{
  "commit_id": "policy-full-1",
  "format_version": 1,
  "version": 1,
  "loss_scale": 64.0,
  "loss_scale_initial": 64.0,
  "growth_factor": 2.0,
  "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"}
}
```

### 增量策略

`base_version` 必须等于当前版本，否则 `E_STALE_BASE` 且不产生任何状态变化。`version` 必须严格递增，否则 `E_STALE_SNAPSHOT`。可以只改一个分片，也可以只改损失缩放字段。

```json
{
  "commit_id": "policy-inc-2",
  "base_version": 1,
  "version": 2,
  "shards": {"s1": "FP16"},
  "loss_scale": 32.0
}
```

### 检查点

`shard_ids` 省略时使用全部正式分片。不会自动带入 `s_partial`；显式引用未写满分片返回 `E_SHARD_NOT_FULL`。每个分片可附带 `fp16_digest` / `fp32_master_digest` / `momentum_digest` 期望值，任何不一致返回 `E_CHECKSUM_MISMATCH`，事务不会执行。

```json
{
  "commit_id": "checkpoint-r1",
  "round": 1,
  "policy_version": 1,
  "loss_scale": 64.0,
  "scheduler_position": {"epoch": 0, "step": 12, "lr_index": 12},
  "data_cursor": {"epoch": 0, "batch": 3, "sample_offset": 384},
  "shard_ids": ["s0", "s1", "s2", "s3"]
}
```

检查点事务会保存主参数、FP16 副本、动量的真实张量字节、三个内容摘要、精度标记、调度位置、数据游标、损失缩放和策略版本。正式清单只包含 `full=true` 且 `written == capacity` 的分片。

### 快照文件

格式：

```json
{
  "format_version": 1,
  "snapshot_seq": 1,
  "generated_at": "2026-11-22T00:00:00+00:00",
  "policy": {
    "version": 2,
    "loss_scale": 64.0,
    "loss_scale_initial": 64.0,
    "growth_factor": 2.0,
    "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"}
  }
}
```

提交 `{"path": "C:\\...\\snapshot.json"}` 到 `/api/policies/load-snapshot`。加载前校验格式版本、快照序号单调性和策略版本单调性；只有全部校验通过才在一个 SQLite 事务内切换策略指针与派生结构。提交成功后才释放旧的内存快照资源；失败时旧策略、旧派生结构和旧资源均保持原样。

## 4. 事务、失效与恢复语义

### 单写者与多读者

- 写路径先独立提交一条 `intents(status='pending')`，之后才进入 `BEGIN IMMEDIATE` 主事务。
- 进程内写操作由 `RLock` 串行化；跨进程再加 SQLite `BEGIN IMMEDIATE` 与 30 秒 busy timeout。
- 读路径使用独立的 `query_only` 连接和显式读事务，在 WAL 下并发读取同一完整提交快照。
- 检查点、策略、各分片派生行和聚合派生行在同一事务写入；提交指针也在该事务最后更新。查询不会观察到“检查点已提交但某分片派生摘要仍旧”的中间状态。
- `commit_operations` 以 `commit_id` 为主键保存请求哈希和响应。相同 ID + 相同内容复用响应；相同 ID + 不同内容返回 `E_CONFLICTING_COMMIT`。

### 依赖图与级联失效

每片显式写入四类依赖边：`master_derived_summary`、`consistency_result`、`recovery_baseline`、`memory_estimate`，并另写指向聚合产物的依赖边。依赖指纹包含：

- 主参数、FP16 副本、动量摘要；
- 精度标记；
- 当前损失缩放、初始损失缩放、增长因子；
- 数值契约版本、形状与 dtype。

精度或任一损失缩放字段变化时，受影响分片重新计算，聚合产物同事务重建。旧策略版本的派生行保留作审计但被标记 `invalidated=true`；当前策略版本的行必须完整且未失效，否则查询返回内部一致性错误而不是部分旧视图。

### 强制终止

`scripts/run_demo.py` 与测试会在主事务写入部分派生行后向子进程发送强制终止。SQLite WAL 回滚整个未提交事务；持久化意图仍为 `pending`。重启时恢复扫描：

1. 若对应 `commit_operations` 已持久化，标记为已完成；
2. 否则标记为 `aborted`，不修改策略/检查点指针；
3. 核验最后完整检查点在当前策略版本下派生行齐全且未失效；
4. 写入只含 SQLite 已持久化事实的恢复报告。

恢复报告明确包含 `guarantees_complete_exit_report: false`。被强制终止的进程无法提供正常退出报告，恢复过程也不会伪造该事实。

## 5. 混合精度数值契约

样本中的 FP16 副本是 `master * loss_scale` 的确定性 FP16 表示。对本策略为 `FP16` 的分片：

1. `fp16` 先无损转成 float64；
2. 除以当前 `loss_scale`；
3. 使用 `numpy.rint`（round-half-to-even）显式舍入到 `2^-14` 固定点网格；
4. 转成 float32，作为还原参数。

策略为 `FP32` 的分片直接使用 FP32 主参数。每片输出：

- `max_absolute`：逐元素最大绝对偏差；
- `max_relative`：以 `max(|master|, 1e-12)` 为分母的最大相对偏差；
- `mean_absolute`；
- `cumulative_rounding_error`：该片逐元素绝对偏差之和；
- 聚合累计误差为各片贡献的确定性浮点求和。

一致性容差为绝对/相对均不超过 `1e-2`。摘要输入使用规范 JSON、显式 float64 十六进制标识和 C-order 小端数组字节，因此相同检查点与相同策略版本重复计算得到相同的摘要、偏差和还原摘要。

显存估算对每片计入 `4*numel` 主参数、`4*numel` 动量和计算副本（FP16 为 `2*numel`，FP32 为 `4*numel`），另加 256 字节元数据。因此一片从 FP32 改为 FP16 会减少 `2*numel` 字节。

## 6. 业务码

必选业务码均已实现并有测试：

| 业务码 | 触发条件 |
|---|---|
| `E_CHECKSUM_MISMATCH` | 请求期望摘要或样本文档摘要与真实内容不一致；不提交检查点 |
| `E_STALE_BASE` | 增量或检查点引用的基础策略版本不是当前版本 |
| `E_STALE_SNAPSHOT` | 策略/快照文件版本或快照序号不递增 |
| `E_CONFLICTING_COMMIT` | 同一提交 ID 携带不同内容 |
| `E_INVALID_PRECISION` | 精度标记不是 FP16/FP32，或检查点精度与当前策略冲突 |

另有 `E_SHARD_NOT_FULL`、`E_SNAPSHOT_VERSION`、`E_INVALID_SNAPSHOT`、`E_INVALID_LOSS_SCALE`、`E_UNKNOWN_SHARD`、`E_POLICY_REQUIRED`、`E_LOSS_SCALE_MISMATCH`、`E_INVALID_REQUEST`、`E_NOT_FOUND`、`E_INTERNAL`。

## 7. 本次实际验证结果

固定种子：`20261122`。测试命令执行结果：`Ran 14 tests ... OK`。

演示实验：

- 全量策略 v1：`s0=FP16, s1=FP32, s2=FP16, s3=FP32`，`loss_scale=64`，`growth_factor=2`。
- 全量检查点版本 1：4 个正式分片；未写满的 `s_partial` 未进入正式清单。
- 单分片增量 v2：只把 `s1` 改为 FP16；派生摘要变化分片严格为 `["s1"]`，其余三片摘要不变。
- 聚合最大绝对偏差：`0.0002421736717224121 -> 0.0002421736717224121`。
- 聚合累计舍入误差：`0.0014091059565544128 -> 0.002542119473218918`。
- 显存估算：`1832 -> 1784` 字节（`s1` 有 24 个元素，减少 48 字节）。
- 相同版本重复查询：响应逐位一致。
- 强制终止：崩溃点 `mid_rebuild`，进程返回码 `-9`；重启前待处理意图 1 个；恢复扫描中止 1 个，完成 0 个。
- 恢复后策略仍为 v2，最后完整检查点仍为 v1，派生聚合摘要与崩溃前一致，`derived_consistency.consistent=true`。
- 恢复报告：`guarantees_complete_exit_report=false`。

演示还探测并确认 `E_STALE_BASE`、`E_STALE_SNAPSHOT`、`E_CHECKSUM_MISMATCH`、`E_INVALID_PRECISION`、`E_CONFLICTING_COMMIT` 均按约定返回，且失败探测不改变策略版本。

逐片偏差（v1 -> v2）：

| 分片 | v1 精度 / max_abs | v2 精度 / max_abs |
|---|---|---|
| `s0` | FP16 / `0.0002421736717224121` | FP16 / `0.0002421736717224121` |
| `s1` | FP32 / `0.0` | FP16 / `0.00017940998077392578` |
| `s2` | FP16 / `0.00009417533874511719` | FP16 / `0.00009417533874511719` |
| `s3` | FP32 / `0.0` | FP32 / `0.0` |

## 8. 目录

```text
mpc/
  api.py          Flask 路由与统一错误信封
  db.py           SQLite schema、WAL、单写者/并发读事务
  numeric.py      还原、偏差、摘要、依赖指纹、聚合
  samples.py      固定种子 PCG64 样本与确定性 NPZ
  service.py      策略、检查点、幂等提交、级联重建、恢复
  server.py       仅回环地址的本地 HTTP 入口
  crash_worker.py 强制终止实验的单操作进程
scripts/
  generate_samples.py
  write_policy_snapshot.py
  run_demo.py
  check_port.py
  setup_windows.ps1
tests/            unittest API、事务、并发、快照、崩溃恢复测试
data/seed_20261122/
artifacts/demo_report.json
```
