# 本地配置中心热更新中间件

这是一个面向本地多服务测试环境的配置中心。它提供带版本与校验摘要的配置快照、客户端订阅、逐客户端独立超时的 HTTP 推送、确认状态机、确定性数据管线、结果一致性校验，以及基于 SQLite 意图日志的崩溃恢复。

实现不依赖 Docker、云服务、外部数据集或非回环网络。配置中心 API 与客户端测试端点默认只绑定 `127.0.0.1`。

## 交付内容

- `confighub/`：可复用 Python 包。
- `confighub/service.py`：快照、推送、确认、一致性校验和恢复逻辑。
- `confighub/storage.py`：SQLite WAL 持久层、意图记录、可见性不变量。
- `confighub/pipeline.py`：固定种子样本与确定性加权求和/归一化/阈值管线。
- `confighub/client_endpoint.py`：本地 HTTP 客户端测试端点，支持延迟与滞后拾取模式。
- `confighub/api.py`：配置中心 HTTP API。
- `demo.py`：自行选择空闲回环端口、拉起两端服务并执行完整演示。
- `scripts/crash_worker.py`：在推送已分发、结果未完成时以 `os._exit(37)` 强制终止进程。
- `scripts/verify_results.py`：独立结果一致性校验工具。
- `scripts/recover.py`：独立恢复扫描工具。
- `tests/`：pytest 验收测试。
- `artifacts/`：实际运行输出与机器可读报告。

## 环境准备

要求 Python 3.11+。本次实际验证在新建 `.venv` 中完成，使用 Python 3.14.4、SQLite 3.46.1、FastAPI 0.165.0、NumPy 2.5.5、pytest 9.2.0、httpx 0.28.1。

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup.ps1
.\scripts\run_demo.ps1
.\scripts\run_tests.ps1
```

### Linux / WSL

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest
python demo.py
```

演示脚本会先选择两个空闲回环端口，再启动客户端测试端点和配置中心 API。它不需要预先清理固定端口，也不会访问非回环地址。SQLite 文件在首次启动时创建于 `data/demo.sqlite`；崩溃恢复演示使用独立的 `data/crash.sqlite`。

## 快速演示

```bash
python demo.py --report artifacts/demo-report.json
```

演示按以下顺序执行：

1. 注册 `fast`、`lagging`、`slow` 三个客户端，全部指向回环 HTTP 端点。
2. 提交并推送快照 v1，三个客户端确认并应用。
3. 提交 v2；`fast` 正常前进，`lagging` 回传其实际拾取的 v1，`slow` 的 HTTP 读取超时。
4. 输出 v2 降级清单与不一致明细；最终结论只吸收已确认且等于目标版本的 `fast` 结果。
5. 提交并推送 v3；重复触发 v3 推送时直接复用原批次，不再访问客户端。
6. 用不同 `content_tag` 重复推送 v3，返回 `E_CONFLICTING_PUSH`。
7. 重复确认保持幂等，冲突确认返回 `E_CONFLICTING_ACK`，v2 迟到确认返回 `E_STALE_ACK` 且不覆盖 v3。
8. 子进程在 v2 推送记录已落库、网络结果未完成时强制退出；重启后扫描意图并将批次/推送标记为 `ABORTED`，客户端仍保留最后一个完整版本 v1。

实际输出保存在 `artifacts/demo-output.txt`，完整 JSON 保存在 `artifacts/demo-report.json`。最近一次运行中，演示脚本的 17 项断言全部通过。

## 配置与订阅模型

### 客户端注册

```http
POST /clients
Content-Type: application/json

{
  "client_id": "fast",
  "config_keys": ["w0", "w1", "w2", "normalization_scale", "threshold"],
  "endpoint_url": "http://127.0.0.1:19090/push/fast",
  "timeout_seconds": 0.5,
  "sample_seed": 7,
  "sample_size": 16,
  "feature_count": 3
}
```

端点只接受 `http/https`、显式端口和 `127.0.0.1`、`localhost`、`::1` 主机名；其他目标返回 `E_EXTERNAL_TARGET`。

### 提交快照

```http
POST /snapshots
Content-Type: application/json

{
  "version": 1,
  "generated_at": "2026-01-01T00:00:00+00:00",
  "config": {
    "w0": 1.2,
    "w1": -0.5,
    "w2": 0.25,
    "normalization_scale": 1.5,
    "threshold": 0.1
  },
  "digest": "..."
}
```

摘要计算规则：

```text
sha256(canonical_json({
  "version": version,
  "generated_at": generated_at,
  "config": {所有配置值规范化为 float64}
}))
```

`digest`/`checksum` 可省略，由服务计算；一旦提供就必须完全一致。

- 版本不大于当前已提交版本：`E_STALE_SNAPSHOT`。
- 摘要不一致：`E_SNAPSHOT_INVALID`，校验发生在任何快照或版本元数据写入之前。
- 配置必须是有限数值对象的非空集合，否则返回 `E_VALIDATION`。

## 确定性数据管线

本地样本由 `SampleSpec(seed, sample_size, feature_count)` 唯一生成：

```python
rng = np.random.Generator(np.random.PCG64(seed))
samples = rng.uniform(-1.0, 1.0, size=(sample_size, feature_count))
```

不调用 `np.random.seed`、模块级 RNG 或任何隐式全局随机状态。固定输入重复执行时，样本张量逐位一致。

计算过程：

```text
weights       = [w0, ..., wN-1]
weighted_sum  = samples @ weights
normalized    = weighted_sum / normalization_scale
flags         = normalized >= threshold
```

`normalization_scale` 也接受别名 `scale` 或 `ratio`。结果摘要包含：

- `sample_digest`、`weighted_sum_digest`、`normalized_digest`、`threshold_flags_digest`
- 配置摘要、配置版本、样本种子/规模
- `stats.weighted_sum`、`stats.normalized`、阈值命中数量/比例
- 覆盖全部规范输入与数组字节的 `result_digest`

数组摘要使用 little-endian float64 的原始字节，版本、配置与样本也参与最终摘要；因此相同参数但不同配置版本仍有不同 `result_digest`。

## 推送与确认状态机

```text
PENDING ──> PUSHED ──> ACKED ──> APPLIED
   │           │          │
   └───────────┴──────────┴────> ABORTED

APPLIED 与 ABORTED 为终态。
```

合法迁移集合在 `confighub/models.py` 中显式定义。非法迁移抛出 `E_INVALID_TRANSITION`；迁移检查与状态更新位于同一 SQLite 事务，异常时回滚，因此业务状态不变。

关键规则：

- 每个 `(client_id, version)` 只有一条推送记录。相同内容重复推送返回原批次报告并设置 `reused=true`，不会再次访问客户端。
- 同版本但客户端契约、订阅键、端点、样本或 `content_tag` 变化，返回 `E_CONFLICTING_PUSH`。
- `PUSHED -> ACKED -> APPLIED` 在同一个确认事务中执行，同时更新 `clients.applied_version`。任何读事务只能看到应用前或应用后的完整状态。
- `APPLIED` 不可回退。相同结果重复确认幂等返回 `DUPLICATE_ACK`；同版本不同结果返回 `E_CONFLICTING_ACK`。
- 若对应版本已被更新版本取代，或推送已中止，迟到确认只写入 `ack_records` 历史并返回 `E_STALE_ACK`。
- 客户端回传的 `result.config_version` 与目标版本不一致时，服务记录该观测结果但标记为不合格，不推进客户端版本。
- 服务会用快照和客户端样本独立重算 `result_digest`；不一致返回 `E_RESULT_INVALID`，不应用结果。

## 独立超时与降级

`POST /pushes` 同时向批次内客户端发起请求。每个客户端使用自己的 `timeout_seconds`，也可由请求级 `timeout_seconds` 统一覆盖。

每次请求使用新的 `httpx.AsyncClient`，配置零连接复用、`Connection: close` 和严格读取超时。超时发生时：

1. 抛出并捕获 `httpx.TimeoutException`；
2. `AsyncClient` 上下文关闭真实 TCP 连接；
3. 推送记录迁移到 `ABORTED`，`last_error=E_PUSH_TIMEOUT`；
4. 报告包含 `connection_closed=true` 与实际耗时；
5. 只有收到有效确认的其他客户端推进版本。

测试 `test_push_timeout_really_closes_underlying_connection` 使用一个永不响应的原始 TCP 回环服务端，验证客户端超时后服务端观察到 EOF，而不仅是本地停止等待。

`POST /pushes` 的降级报告示例：

```json
{
  "advanced": [{"client_id": "fast", "picked_version": 2}],
  "not_advanced": [
    {"client_id": "lagging", "code": "E_STALE_ACK", "reason": "..."},
    {"client_id": "slow", "code": "E_PUSH_TIMEOUT", "connection_closed": true}
  ],
  "degraded": true,
  "degradation_policy": "advance only acknowledged clients; retain last committed version for others"
}
```

## 并发、可见性与事务边界

- 快照提交由进程内快照锁串行化，并在 SQLite `BEGIN IMMEDIATE` 内再次校验当前版本。
- 同一版本的推送由异步版本锁串行化；并发重复触发会等待首个执行者，之后复用持久化报告。
- 跨进程竞争由 `batches.target_version` 唯一约束兜底。一个快照版本对应一次批次尝试；若该轮中止，重试应提交新版本，而不是复用旧版本。
- 所有写事务使用 `BEGIN IMMEDIATE`，由进程内写锁和 SQLite 写锁共同保证单写者。
- 所有查询使用独立连接、`PRAGMA query_only=ON` 和 WAL 读事务；多个查询不阻塞写者，也不会观察到未提交的 `ACKED` 中间态。
- `storage.visibility_invariant_violations()` 会检查两个不变量：
  - 不存在已提交的 `ACKED` 且客户端版本仍旧；
  - 不存在客户端版本低于其 `APPLIED` 推送版本。

## SQLite、意图与恢复

数据库表：

- `snapshots`、`clients`
- `batches`、`push_records`、`push_transitions`
- `ack_records`、`results`
- `intents`、`recovery_reports`
- `meta.current_version`

写操作顺序：

1. 在独立事务中提交 `STARTED` 意图；
2. 在 `BEGIN IMMEDIATE` 事务中修改领域表；
3. 提交领域事务；
4. 将意图标记为 `COMMITTED` 或 `ABORTED`。

如果进程在步骤 3 与 4 之间退出，恢复扫描会依据已提交的快照/结果/批次事实将意图标记为 `RECONCILED`；如果领域事务尚未提交，则标记为 `ABORTED`。

启动时 `ConfigService.recover()` 或 `POST /recovery/scan` 会：

1. 扫描 `RUNNING` 批次；
2. 将其中 `PENDING/PUSHED/ACKED` 推送迁移到 `ABORTED` 并写迁移历史；
3. 将批次标记为 `ABORTED`，原因 `RECOVERY_INCOMPLETE_BATCH`；
4. 不修改客户端已持久化的 `applied_version`；
5. 解析所有 `STARTED` 意图；
6. 输出仅基于已提交 SQLite 事实的恢复报告，并持久化到 `recovery_reports`。

恢复结论会列出未完成意图、已中止批次、每个客户端最后完整版本、剩余非终态推送和可见性违规。不会使用内存中的未落库推送结果。

独立工具：

```bash
python scripts/recover.py --db data/crash.sqlite
python scripts/verify_results.py --db data/demo.sqlite --version 2
python scripts/verify_results.py --db data/demo.sqlite --batch-id <batch-id>
```

默认退出码表达“最终合格子集是否一致”；加 `--strict` 时还要求客户端覆盖完整且整体观测版本/摘要一致。

## 结果一致性校验

`GET /batches/{batch_id}/consistency` 或 `verify_results.py` 的输出包含：

- 每个观测结果的客户端、拾取版本、结果摘要、是否确认、是否可进入最终结论；
- 与目标版本参考结果相比的 `difference_fields`；
- 被排除的结果及原因；
- 未确认客户端及原因；
- `coverage_complete` 与 `degraded`；
- `observed_conclusion`、`observed_versions`、`mixed_versions`：整体观测结果是否混用版本；
- `final_consistent`：仅对最终合格子集的判断；
- `consistent`：整批一致性，只有覆盖完整、观测一致且最终合格子集一致时才为 `true`；
- `final_version`、`final_result_digest` 和 `conclusion`。

最终结论的纳入条件是：

1. 快照版本存在；
2. 有 `accepted=1` 的确认记录；
3. 推送当前状态为 `APPLIED`；
4. 拾取版本等于批次目标版本。

因此，滞后客户端已确认但拾取旧版本的结果只用于不一致明细，不会混入最终结论。若最终合格集合为空，`conclusion` 为 `INSUFFICIENT_CONFIRMATIONS`；合格集合内部摘要不一致则为 `INCONSISTENT`；全部一致则为 `CONSISTENT`。与此同时，只要批次观测到多个版本或多个结果摘要，`observed_conclusion` 就会独立标记为 `INCONSISTENT`，避免把合格的子集结论误解为整批一致。`coverage_complete=false` 明确表示这只代表退化后的合格子集。

## HTTP API 摘要

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 当前版本、数据库路径、可见性违规 |
| POST/GET | `/clients` | 注册/列出订阅 |
| GET | `/clients/{id}` | 查询客户端与应用版本 |
| POST | `/snapshots` | 提交快照 |
| GET | `/snapshots/latest` | 最新快照 |
| GET | `/snapshots/{version}` | 指定快照 |
| POST | `/pushes` | 执行或复用版本推送 |
| POST | `/acks` | 外部/迟到确认 |
| GET | `/batches/{id}` | 批次持久化报告 |
| GET | `/batches/{id}/consistency` | 一致性校验 |
| POST | `/recovery/scan` | 执行恢复扫描 |
| GET | `/recovery/latest` | 最近恢复报告 |

客户端测试端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 端点健康检查 |
| POST | `/push` 或 `/push/{profile}` | 接收配置、执行管线并回传确认 |
| POST | `/admin/delay` | 设置测试延迟 |
| POST | `/admin/mode` | `normal` 或 `lagging` 拾取模式 |
| GET | `/stats` | 请求计数与最后拾取版本 |

## 实际验证记录

本次执行命令：

```bash
python -m pytest
python demo.py
```

验证覆盖：

- 快照摘要错误与版本回退均不改变已提交状态；
- 全局 NumPy RNG 状态变化不影响固定种子结果，重复执行逐位一致；
- 真实 TCP 超时断连；
- 滞后版本结果差异与最终结论过滤；
- 重复推送不重放网络、内容冲突拒绝；
- 重复/冲突/迟到确认；
- 非法状态迁移不改变状态；
- 写事务未提交时读取端仍看到旧 `PUSHED` 状态，提交后原子显示 `APPLIED`；
- 子进程在推送中强制退出后重启恢复，批次中止、客户端保留 v1。

详细机器可读输出见：

- `artifacts/demo-report.json`
- `artifacts/demo-output.txt`
- `artifacts/test-output.txt`
- `artifacts/verification-summary.md`

## 已知边界

- 这是本地测试中间件，不是跨网络分布式配置中心；不提供 TLS、认证、租户隔离或跨主机复制。
- 一个快照版本只执行一次推送批次。需要重试同一配置时，应提交更高版本；这可防止重复网络副作用与旧确认覆盖。
- 客户端测试端点的 `lagging` 模式用于构造版本混合场景，不参与生产语义。
- 结果数组本身不全部持久化，持久化的是原始字节摘要和统计摘要；逐位一致性通过摘要验证。

