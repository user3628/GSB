# SHARD/BUDGET

面向分布式对象存储读路径的**副本读修复裁决与纠删码重建预算引擎**。项目从可重复的本地样本出发，完成布局归一化、分片可读性核验、三种读修复策略裁决、GF(256) 纠删码重建、并发/字节/时间三重预算调度、确定性预算降级、SQLite 意图持久化与强制终止后的启动对账，并提供 FastAPI HTTP 接口和本地交互控制台。

本项目不依赖 Docker、模型、外部数据集或公网目标。所有样本、分片内容和故障状态均在 `data/` 中确定性生成。

## 快速开始（Windows 10+）

目标运行时为 Python 3.11 或 3.12。在 PowerShell 中执行：

```powershell
cd <项目目录>
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup_windows.ps1
.\scripts\run_server.ps1
```

打开：

- 控制台：<http://127.0.0.1:8765/>
- OpenAPI：<http://127.0.0.1:8765/docs>

一键运行确定性演示和真实强制终止恢复演示：

```powershell
.\scripts\run_demo.ps1
```

也可以手动执行：

```powershell
.\.venv\Scripts\python.exe scripts\demo.py --output artifacts\demo_evidence.json
.\.venv\Scripts\python.exe scripts\crash_and_recover.py
.\.venv\Scripts\python.exe -m pytest -q
```

Linux/WSL 本地验证命令等价：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/build_samples.py
python -m pytest -q
python scripts/demo.py --output artifacts/demo_evidence.json
python scripts/crash_and_recover.py
python -m uvicorn app.main:app --host 127.0.0.1 --port 8765
```

## 核心设计

```text
布局 JSON ──严格归一化──┐
轨迹 JSON ──严格归一化──┤
                        ├─ 可读性核验（节点/磁盘/存在性/摘要）
                        ├─ 直接读 / 副本补齐 / EC 重建 / 快速失败裁决
                        ├─ 优先级 + 到达顺序 + 并发 + 字节 + 时间预算调度
                        ├─ ThreadPoolExecutor 并行 GF(256) 重建
                        └─ 稳定归并 + 轮次原子提交 + 预算账本
                                      │
                                      └─ SQLite WAL + 意图先行 + 启动对账
```

关键实现位置：

| 能力 | 文件 |
|---|---|
| 布局解析与归一化 | `app/layout.py` |
| 轨迹解析、位图与时间戳校验 | `app/trace.py` |
| 可读性和原因码 | `app/health.py` |
| GF(256)、Cauchy 矩阵、矩阵求逆 | `app/gf.py` |
| 分片重建和摘要核验 | `app/codec.py` |
| 确定性裁决与三重预算调度 | `app/planner.py` |
| 轮次执行、并行任务、归并 | `app/engine.py` |
| SQLite 意图协议、账本、恢复 | `app/store.py` |
| HTTP API 与控制台 | `app/main.py`, `app/static/*` |
| 确定性样本生成 | `scripts/build_samples.py` |
| 重复演示与强制终止脚本 | `scripts/demo.py`, `scripts/crash_and_recover.py` |

## 输入契约

### 布局

完整样本见 `data/sample_layout.json`。简化结构：

```json
{
  "layout_version": "layout-2026-11-22-v1",
  "max_k_plus_m": 8,
  "engine": {
    "rebuild_concurrency": 2,
    "rebuild_queue_capacity": 4,
    "round_byte_budget": 2048,
    "round_time_budget_us": 20000
  },
  "nodes": {"node-a": true},
  "disks": [{"node_id": "node-a", "disk_id": "disk-a", "status": "online"}],
  "objects": [
    {
      "object_id": "obj-a",
      "k": 4,
      "m": 2,
      "shards": [
        {
          "shard_index": 0,
          "size_bytes": 512,
          "digest": "sha256:<64 hex>",
          "node_id": "node-a",
          "disk_id": "disk-a",
          "present": true,
          "content_b64": "..."
        }
      ]
    }
  ]
}
```

规则：

- `k`、`m` 为正整数，`k+m <= max_k_plus_m <= 255`。
- 每个对象必须提供恰好 `k+m` 个分片占位；分片可乱序输入，但索引必须唯一并归一化为 `0..k+m-1`。
- 每个逻辑分片的 `size_bytes` 一致，取值为非负且不超过 64 位有符号整数范围。
- 摘要只接受 `sha256:<64 位小写十六进制>`（兼容输入 `sha-256:` 前缀）。
- `present=true` 时必须提供 `content_b64`/`content`/`content_hex`，内容长度必须等于声明大小。
- 分片默认有一个主副本；也可提供 `replicas` 数组表示多个完整副本。副本位置由节点与磁盘标识确定。
- 节点缺失于 `nodes` 时默认健康；磁盘缺失于 `disks` 时默认 `online`。磁盘只有 `online` 被视为可读。
- 任一布局错误返回 `E_LAYOUT_INVALID`，整批不落库。
- 同一 `layout_version` 内容一致时幂等复用；内容不一致返回 `E_LAYOUT_CONFLICT`。

为保持样本完全本地可运行，分片内容直接放在布局 JSON 中。生产接入可将内容读取替换为对象存储/磁盘读取，而保持其余裁决逻辑不变。

### 轨迹

完整样本见 `data/sample_trace.json`：

```json
{
  "trace_id": "main-budget-shed-demo",
  "layout_version": "layout-2026-11-22-v1",
  "options": {"round_byte_budget": 2048},
  "requests": [
    {
      "request_id": "req-alpha-ec",
      "arrival_ms": 1001,
      "object_id": "obj-alpha",
      "shard_bitmap": "0x3f",
      "priority": 9,
      "tolerable_latency_us": 10000
    }
  ]
}
```

- `arrival_ms` 必须单调不减；任一回退会整批返回 `E_TRACE_INVALID`。
- `shard_bitmap` 支持整数、`0x` 十六进制、`0b` 二进制；也支持 `requested_shards` 整数数组。
- 位图必须非空，且不能包含对象分片范围外的位。
- `priority` 为 `0..100`，也接受 `low/normal/high/critical`。
- `tolerable_latency_us` 为正整数。
- 同一轨迹内 `request_id` 不得重复。
- `options` 可按轮次覆盖布局中的预算参数。

## 裁决算法

### 1. 分片可读性

每个逻辑分片按副本依次检查，稳定原因码为：

- `NODE_UNAVAILABLE`：节点不健康。
- `DISK_FAULT`：磁盘不是 `online`。
- `SHARD_MISSING`：副本声明缺失或没有内容。
- `DIGEST_MISMATCH`：实际内容 SHA-256 与布局声明不一致。

只要一个完整副本通过检查，该逻辑分片即可读。输出包含对象范围的 `readable_bitmap`、选中副本、每个副本的状态与原因码。

### 2. 策略优先级

按请求顺序归并输出，但重建入场顺序按 `priority DESC, arrival_ms ASC, seq ASC, request_id ASC`：

1. 请求分片全部可读：`DIRECT_READ`。
2. 主副本不可读但其他完整副本可读：`REPLICA_FILL`，不消耗重建预算。
3. 存在不可读请求分片且可读分片数 `< k`：`DIRECT_FAIL + E_UNRECOVERABLE`，绝不创建重建任务。
4. 可重建但即使立即执行也超过该请求延迟上限：`DIRECT_FAIL + E_LATENCY_GUARD`。
5. 队列容量不足：`DIRECT_FAIL + E_REBUILD_QUEUE_FULL`。
6. 字节或时间预算不足：`DIRECT_FAIL + E_REBUILD_SHED`，按优先级和已消耗预算确定性降级。
7. 否则：`EC_REBUILD`，所有任务并行执行并按摘要核验。

若已经重建过的分片结果已提交，后续请求会复用持久化结果，不重复占用预算。

### 3. 三重预算

- **并发上限**：任务按确定顺序切成并发波次，每波最多 `rebuild_concurrency` 个任务。
- **等待队列**：单轮最多接纳 `rebuild_concurrency + rebuild_queue_capacity` 个任务；超出的重建请求快速失败。
- **字节预算**：单轮所有新任务字节数之和不得超过 `round_byte_budget`。
- **时间预算**：使用本地确定性成本模型计算虚拟 `start_us/end_us/waves`，波次 makespan 不得超过 `round_time_budget_us`。

时间预算采用确定性虚拟时钟而非 wall clock，目的是保证同一布局和轨迹在不同机器、不同进程中得到相同的裁决和摘要；实际重建仍由 `ThreadPoolExecutor` 并行执行。

任务时延模型：

```text
duration_us = rebuild_setup_us
            + ceil(shard_bytes / rebuild_bandwidth_bytes_per_us)
            + rebuild_per_shard_latency_us
```

请求端到端估计：

```text
read_overhead_us + max(相关任务 estimated_end_us)
<= tolerable_latency_us
```

## 纠删码重建

- Galois 域：GF(256)，本原多项式 `0x11d`。
- 系统矩阵：数据分片为前 `k` 行单位矩阵；校验行采用确定性 Cauchy 系数 `1/(x_i XOR y_j)`，`x_i = k+i`、`y_j = j`。
- 任意 `k` 个存活分片的行子矩阵均可逆。
- 重建时固定选择排序后的前 `k` 个可读分片作为独立方程，结果稳定可复现。
- 重建结果计算实际 SHA-256，并与布局声明比较。不一致返回 `E_RECONSTRUCT_MISMATCH`，包含对象、分片索引、期望摘要、实际摘要、源分片与矩阵指纹。
- 矩阵维数或运算前提不满足返回 `E_MATRIX_INVALID`。

## SQLite 持久化与恢复

数据库启用 WAL、`synchronous=FULL` 与 `BEGIN IMMEDIATE`。所有业务写入遵循两阶段意图协议：

1. 独立事务写入 `intents(state=PENDING)` 并提交。
2. 应用事务写入实际事实，并在同一事务末尾把意图改为 `APPLIED`。

因此强制终止后只会出现两种事实：

- 实际事实完整可见，意图 `APPLIED`；
- 实际事实未提交，留下可扫描的 `PENDING` 意图。

持久化内容：

- 布局版本与原始文档摘要；
- 轮次、轨迹、选项和最终裁决结果；
- 每请求结构化裁决；
- 重建任务 `READY/RUNNING/COMMITTED/FAILED`；
- 预算账本 `RESERVED/COMMITTED/RELEASED`；
- 已提交、可复用分片结果；
- 启动对账报告。

启动恢复流程：

1. 先写 `RECOVERY_RECONCILE` 意图。
2. 扫描未完成意图和 `BUILDING` 轮次。
3. 未提交任务回到 `READY`；其 `RESERVED` 预算改为 `RELEASED`。
4. 已提交任务和分片结果保持 `COMMITTED/REUSABLE`。
5. 轮次标记 `ABORTED`，不伪造最终裁决。
6. 按已落盘账本重算 `remaining = byte_budget - committed`。
7. 生成并持久化只包含事实的启动对账报告。

任何只读查询只返回 `COMMITTED` 轮次；`BUILDING/ABORTED` 轮次不会暴露“重建成功但预算尚未归并”的中间状态。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/layouts` | 加载并归一化布局 |
| `GET` | `/api/v1/layouts` | 列出布局版本 |
| `GET` | `/api/v1/layouts/current` | 当前布局摘要 |
| `POST` | `/api/v1/traces` | 提交轨迹并同步完成轮次裁决 |
| `GET` | `/api/v1/rounds` | 列出轮次 |
| `GET` | `/api/v1/rounds/{id}` | 提交后的完整轮次结果 |
| `GET` | `/api/v1/rounds/{id}/decisions` | 逐请求裁决轨迹 |
| `GET` | `/api/v1/rounds/{id}/budget` | 已提交预算摘要与持久化账本事实 |
| `GET` | `/api/v1/rounds/{id}/replay-signature` | 重放摘要与裁决 |
| `GET` | `/api/v1/recovery` | 最新启动对账报告 |
| `GET` | `/api/v1/recovery/{report_id}` | 指定恢复报告 |
| `POST` | `/api/v1/admin/arm-crash` | 仅回环：武装下一次强制退出故障点 |
| `GET` | `/api/v1/demo/fixtures` | 返回全部确定性演示样本 |

业务失败统一为：

```json
{
  "error": {
    "code": "E_REBUILD_SHED",
    "message": "单轮重建字节预算不足，请求降级快速失败",
    "details": {"remaining_bytes": 0, "required_bytes": 512}
  }
}
```

逐请求不可恢复、降级、队列满或重建摘要不一致属于轮次裁决结果，随 `HTTP 201` 的轮次一起返回，位于 `decisions[].error`；批次结构、布局或轨迹非法则直接返回对应 HTTP 错误。

至少覆盖的业务码：`E_LAYOUT_INVALID`、`E_TRACE_INVALID`、`E_UNRECOVERABLE`、`E_RECONSTRUCT_MISMATCH`、`E_REBUILD_QUEUE_FULL`、`E_REBUILD_SHED`。其他稳定码包括 `E_LATENCY_GUARD`、`E_CONFLICTING_REQUEST`、`E_MATRIX_INVALID`、`E_LAYOUT_CONFLICT`、`E_ROUND_NOT_COMMITTED`。

## 确定性样本与预期结果

`data/sample_layout.json` 包含 10 个对象，覆盖：

- 节点不可用、磁盘离线、分片缺失、内容摘要不一致；
- 主副本缺失但其他完整副本可读；
- 存活分片少于 `k`；
- 用于预算降级和延迟保护的多对象轨迹。

主轨迹 `data/sample_trace.json` 的基准结果：

| 请求 | 策略 | 结果 |
|---|---|---|
| `req-gamma-unrecoverable` | `DIRECT_FAIL` | `E_UNRECOVERABLE` |
| `req-alpha-ec` | `EC_REBUILD` | 成功重建 shard 2、4 |
| `req-beta-replica` | `REPLICA_FILL` | 使用 shard 1 的完整副本 |
| `req-delta-direct` | `DIRECT_READ` | 成功 |
| `req-epsilon-ec` | `EC_REBUILD` | 成功 |
| `req-iota-digest-repair` | `EC_REBUILD` | 摘要损坏分片修复成功 |
| `req-zeta-shed` | `DIRECT_FAIL` | `E_REBUILD_SHED` |
| `req-eta-shed` | `DIRECT_FAIL` | `E_REBUILD_SHED` |
| `req-theta-latency` | `DIRECT_FAIL` | `E_LATENCY_GUARD` |

合计：9 请求，5 成功，4 失败，4 个重建任务，消耗 2048/2048 字节，虚拟 makespan 2412 μs。每请求还输出稳定归并的 `read_result`（按分片索引列出 `PRIMARY_READ`、`REPLICA_FILL`、`EC_REBUILD`、`PERSISTED_REBUILD_REUSE` 或失败原因）。基准 `decision_digest` 为：

```text
sha256:48c06165e7a8fb067cf5ac50afc492dbfd23ca2c25d395bc3e6aa47c808badeb
```

同一轨迹在同一数据库重复提交时，所有 `request_id` 复用原裁决，轮次不新增重建任务和字节消耗，但 `decision_digest` 保持一致。

其他场景：

- `data/queue_trace.json`：并发 1、队列 0，后两个请求返回 `E_REBUILD_QUEUE_FULL`。
- `data/mismatch_trace.json`：目标分片声明摘要被故意破坏，重建后返回 `E_RECONSTRUCT_MISMATCH`，同时预算仍记录实际消耗 384 字节。
- `data/recovery_trace.json`：第一次分片结果提交后真实 `os._exit(42)`；重启后 1 个分片结果可复用、1 个任务回到 READY、512 字节保留为已消耗、512 字节预留释放、剩余 3584 字节。

## 验证

实际验证结果、命令和关键输出见 [`VERIFICATION.md`](VERIFICATION.md)。生成的可审计证据：

- `artifacts/demo_evidence.json`：主轨迹、重放、队列满和摘要不一致的完整结果。
- `artifacts/forced_kill_recovery.json`：真实退出码 42 后的启动对账报告。

## 文件结构

```text
app/                 引擎、算法、SQLite、HTTP 与静态控制台
data/                确定性布局与四类轨迹
scripts/             样本生成、演示、强制终止、Windows 启动脚本
tests/               单元、集成、HTTP、并发读与恢复测试
artifacts/           实际运行证据 JSON
requirements.txt     FastAPI/Uvicorn/NumPy/pytest/httpx
pytest.ini
```

## 边界说明

- 服务按单 Uvicorn worker、进程内单写者锁设计；不要对同一数据库启动多个写进程。
- 时间预算采用确定性成本模型，不是 wall-clock 限时器；这是逐位一致重放的必要设计。
- 内容直接内置在演示布局中，便于离线复现；接入真实存储时应替换内容装载层。
- 管理故障注入接口只接受本机回环地址；不要将服务暴露到不可信网络。
