# 主从复制延迟治理本地应用

这是一个可在 Windows 本地运行、仅监听 `127.0.0.1` 的主从复制延迟治理策略编辑器与确定性裁决服务。应用使用 FastAPI + SQLite，不依赖外部数据库或容器；SQLite 数据库默认位于工作区 `data/replication_guard.sqlite3`。

## 快速开始（Windows PowerShell）

要求 Python 3.11 或以上版本。

```powershell
cd <工作区>
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts\make_samples.py
python -m replication_guard --host 127.0.0.1 --port 8765 --db data\replication_guard.sqlite3
```

也可以直接执行：

```powershell
.\setup.ps1
.\run.ps1 -Port 8765
```

若 PowerShell 阻止本地脚本，可仅在当前窗口执行 `Set-ExecutionPolicy -Scope Process Bypass` 后重试。

服务启动后：

- 接口文档：`http://127.0.0.1:8765/docs`
- 健康检查：`http://127.0.0.1:8765/healthz`
- 兼容矩阵：`http://127.0.0.1:8765/api/compatibility`
- 状态机说明：`http://127.0.0.1:8765/api/state-machine`

可用以下命令通过真实本地 HTTP 接口加载确定性样本：

```powershell
python scripts\load_samples.py --base-url http://127.0.0.1:8765 --observe
```

## 固定样本

| 文件 | 用途 |
| --- | --- |
| `samples/policies.json` | 规范化策略基准样本，包含 4 个从库 |
| `samples/policies.jsonl` | 当前 JSON Lines 分片格式样本 |
| `samples/policies_v2.bin` | 当前紧凑二进制 v2 样本 |
| `samples/observations.json` | 确定性延迟观测，含同一采样时刻重复记录与无策略从库 |
| `samples/policies_legacy_v1.bin` | 旧二进制 v1 降级样本 |
| `samples/policies_legacy_export_meta.json` | v1 导出时声明的秒级截断事实 |
| `samples/policies_future_v2.jsonl` | 声明未知必填字段 `tenantRoutingKey` 的高版本样本 |

`python scripts/make_samples.py` 可重新生成所有派生二进制和 JSONL 样本；输入固定、输出确定。

## 核心数据

策略字段：

- `slave_id`：从库标识
- `replication_group`：复制组
- `delay_threshold_ms`：延迟判定阈值，非负整数
- `tolerable_delay_ms`：可容忍延迟，非负整数且不小于判定阈值
- `expected_version`：预期从库版本段字符串，例如 `8.0.32-24`
- `degradation_action`：非空大写动作标识；标准值包括 `NONE`、`MONITOR`、`THROTTLE_WRITES`、`READ_ONLY`、`FAILOVER`、`PAUSE_REPLICATION`
- `priority`：非负整数，数值越小优先级越高；0-33 为高优先级段，34-66 为中优先级段，67 以上为低优先级段
- `forward_compatible`：是否允许从库上报版本高于预期时继续自动裁决
- `policy_version`：策略修订号，用于冲突裁决
- `updated_at_ms`：最后写入时间，毫秒

观测字段：`slave_id`、`sampled_at_ms`、`binlog_gap`、`delay_ms`、`reported_version`。时间也接受 ISO-8601 字符串，进入领域层后统一为毫秒整数。

版本字符串按数字段解析并保留原始字符串回显。例如 `8.0.32-24` 解析为 `(8, 0, 32, 24)`；比较时对缺失的尾部段补零，因此 `8.0.32` 与 `8.0.32.0` 相等。无法解析时返回 `E_VERSION_MALFORMED`。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/policies` | 读取已提交策略与数据集版本 |
| `GET` | `/api/policies/{slave_id}` | 读取单条策略 |
| `POST` | `/api/policies` | 新建/覆盖策略，支持 `baseDatasetVersion` |
| `PUT/PATCH` | `/api/policies/{slave_id}` | 编辑策略，基础版本过期返回 `E_STALE_BASE` |
| `DELETE` | `/api/policies/{slave_id}` | 删除策略，支持基础版本校验 |
| `POST` | `/api/policies/import` | 导入二进制或 JSONL，支持自动识别 |
| `GET/POST` | `/api/policies/export` | 导出二进制或 JSONL，可指定版本与分片数 |
| `POST` | `/api/observations` | 原子导入观测批次 |
| `GET` | `/api/observations` | 读取观测 |
| `POST` | `/api/recalculate` | 按观测窗口重算状态、动作与裁决 |
| `GET` | `/api/states` | 读取已持久化状态 |
| `POST` | `/api/states/{slave_id}/transition` | 手工状态迁移，非法路径返回 `E_INVALID_TRANSITION` |
| `GET` | `/api/verdicts` | 读取裁决运行列表 |
| `GET` | `/api/verdicts/{recalc_id}` | 读取单次可编辑裁决 |
| `PATCH` | `/api/verdicts/{recalc_id}/rows/{slave_id}` | 写入人工动作裁决与备注 |
| `POST` | `/api/verdicts/{recalc_id}/replay` | 从持久化运行快照确定性回放 |
| `GET` | `/api/recovery-report` | 最近一次恢复扫描报告 |
| `POST` | `/api/recovery/scan` | 主动扫描未完成意图与数据集完整性 |
| `GET` | `/api/intents` | 读取意图日志 |
| `GET` | `/api/batches/{batch_id}` | 读取成功、拒绝或中止批次的持久化结果 |
| `GET` | `/api/compatibility` | 读取显式兼容矩阵 |
| `GET` | `/api/state-machine` | 读取状态机合法迁移与动作规则 |

导入二进制有两种传法：

1. 原始二进制请求体：`Content-Type: application/octet-stream`，URL 使用 `?format=binary&batch_id=...`。
2. JSON 包装：`{"format":"binary","encoding":"base64","data":"...","batchId":"..."}`。

JSONL 可直接以 `Content-Type: application/x-ndjson` 提交，也可以提交 `{"format":"jsonl","data":"..."}`；多分片可用 `shards` 数组。导出 `shards > 1` 时返回分片名到内容的 JSON 映射。

成功响应包含 `ok: true`。失败响应统一为：

```json
{
  "ok": false,
  "code": "E_STALE_BASE",
  "message": "策略编辑基础版本已过期",
  "details": {"baseDatasetVersion": 1, "currentDatasetVersion": 2}
}
```

必需业务码均已覆盖：`E_POLICY_INVALID`、`E_VERSION_MALFORMED`、`E_FORMAT_INCOMPATIBLE`、`E_STALE_BASE`、`E_INVALID_TRANSITION`。另有 `E_FORMAT_MALFORMED`、`E_IMPORT_CONFLICT`、`E_BATCH_ABORTED`、`E_POLICY_MISSING`、`E_VERSION_AHEAD` 等显式裁决码。

`E_VERSION_AHEAD` 是“重算成功但某从库需人工复核”的裁决码，因此重算接口返回 HTTP 200，同时把该码放入结果行的 `businessCode`、顶层 `businessCode` 与 `X-RG-Business-Code` 响应头；其余失败均返回非 2xx JSON。

## 紧凑二进制格式

所有多字节整数为大端。

文件头：

| 偏移 | 长度 | 内容 |
| --- | --- | --- |
| 0 | 4 | 魔数 `RGPB` |
| 4 | 1 | 格式版本，当前 `1` 或 `2` |
| 5 | 1 | 文件头标志，当前必须为 `0` |
| 6 | 4 | 记录数 `u32` |

每条记录：

1. `u32` 记录长度，包含后续载荷与 4 字节 CRC32。
2. `u16` 可选字段存在位图。
3. 必填字段：`slave_id`、`replication_group`、`delay_threshold_ms:u32`、`tolerable_delay_ms:u32`、`expected_version`、`priority:u16`。
4. 可选字段按位图顺序：更新时间、降级动作、前向兼容布尔、策略修订号。字符串为 `u16` 字节长度加 UTF-8。
5. `u32` CRC32，覆盖同一记录中除 CRC 本身的载荷。

位图：

- bit 0：更新时间存在
- bit 1：降级动作存在
- bit 2：前向兼容标志存在（仅 v2）
- bit 3：策略修订号存在（仅 v2）

版本 1 的更新时间是 `u32` 秒；版本 2 是 `u64` 毫秒。导入 v1 时按 `seconds * 1000` 归一化；导出 v1 时毫秒值按 `floor(ms / 1000) * 1000` 截断，并在导出元数据与导入报告中列出每条截断，不静默处理。v1 缺少前向兼容位与策略修订号，导入时补齐为 `false` 与 `1`，逐条写入降级明细。

## JSON Lines 分片格式

每行一个 JSON 对象。第一行可含元数据：

```json
{"_meta":{"format":"policy-jsonl","version":1,"timePrecision":"milliseconds","shardIndex":1,"shardCount":1}}
```

策略行包含：

```json
{
  "policyId":"replica-east-01",
  "replicationGroup":"east",
  "delayThresholdMs":500,
  "tolerableDelayMs":2000,
  "expectedVersion":"8.0.32-24",
  "degradationAction":"THROTTLE_WRITES",
  "priority":10,
  "forwardCompatible":false,
  "policyVersion":3,
  "updatedAtMs":1763000000000
}
```

JSONL v1 的时间精度为毫秒。旧记录缺少可选字段时补齐声明默认值并记录降级明细；未知必填字段、未知高版本字段或无法确认字段可选性时返回 `E_FORMAT_INCOMPATIBLE`，整批不导入。

## 兼容矩阵

| 格式/版本 | 结论 | 处理 |
| --- | --- | --- |
| binary v1 | 降级接受 | 补 `forwardCompatible=false`、`policyVersion=1`，秒级时间乘 1000 |
| binary v2 | 接受 | 完整字段、毫秒时间 |
| JSONL v1 | 接受 | 完整字段、毫秒时间 |
| JSONL v2 且声明未知必填字段 | 拒绝 | `E_FORMAT_INCOMPATIBLE`，整批回滚 |
| JSONL 高版本但无未知必填字段 | 降级接受 | 仅按 v1 已知字段保守读取并记录 |
| binary 高于 v2 | 拒绝 | 可能含未知必填字段，整批回滚 |

协商结论、矩阵版本、输入/目标版本、默认补齐项、时间归一化与未知字段明细均随导入报告输出。

## 导入冲突与原子性

同一 `policyId` 冲突时按以下顺序比较：

1. 更高 `policyVersion` 优先；
2. 优先级数值更小（优先级更高）者优先；
3. `updatedAtMs` 更新者优先。

高者写 `OVERWRITE`，低者写 `SKIP`；三者完全并列但内容不同则写 `REJECT`，因为无法确定性裁决。只要批次内出现一条 `REJECT`，整个批次不写入任何策略，错误详情仍包含完整 `importReport`。新策略记为 `ADDED`。

批次标识全局幂等：相同 `batchId` 的后续请求等待首个请求结束后复用其结果，不会重复执行。失败批次也会持久化，并复用相同业务失败。

## 观测裁决与状态机

同一从库同一 `sampledAtMs` 的多条观测先按 `(binlogGap, delayMs, reportedVersion 数字段, 原始版本)` 取最大值，再按采样时间排序。因此到达顺序不会影响结果，位点差最大的观测一定胜出。

状态与合法迁移：

```text
HEALTHY  -> DELAYED, DEGRADED
DELAYED  -> DEGRADED, RESTORED
DEGRADED -> RESTORED
RESTORED -> HEALTHY, DELAYED, DEGRADED
```

迁移由连续观测窗口 `windowSize` 驱动：

- 连续窗口内延迟超过 `delayThresholdMs`，进入 `DELAYED`；
- 连续窗口内延迟超过 `tolerableDelayMs`，进入 `DEGRADED`；
- `DELAYED`/`DEGRADED` 经过完整健康窗口后进入 `RESTORED`；
- `RESTORED` 再经过一个完整健康窗口后回到 `HEALTHY`；
- 窗口未填满时保持原状态，并在 `trigger` 中输出原因和计数。

上报版本低于或等于预期时按上述延迟状态裁决；高于预期且 `forwardCompatible=false` 时冻结自动迁移、动作改为 `MANUAL_REVIEW`，并返回 `E_VERSION_AHEAD`。高于预期但声明前向兼容时继续按延迟窗口裁决。

动作与状态、优先级一致：

- `HEALTHY` 应用 `NONE`；
- `DELAYED` 时高优先级（0-33）应用策略声明动作，否则 `MONITOR`；
- `DEGRADED` 时应用策略声明动作，若声明 `NONE` 则提升为 `READ_ONLY`；
- `RESTORED` 应用 `RESTORE_SERVICE`。

每次重算还输出 `actionArbitration`，把所有非 `NONE` 动作按优先级数值、状态严重度与从库标识确定性排序，并附触发码，便于核对优先级一致性与执行顺序。

非法手工迁移返回 `E_INVALID_TRANSITION`，事务不写入状态，响应明确标记 `persistedStateUnchanged: true`。

## 意图、事务与中断恢复

每个写操作先在独立事务写入 `intents` 中的 `PREPARED` 意图，再执行 SQLite `BEGIN IMMEDIATE` 业务事务。策略变更、意图完成状态、幂等批次结果与数据集完整快照在同一事务提交。所有写操作由一个进程内可重入写锁和 SQLite 单写事务串行化；读取使用 WAL 快照并可与写入并发。

启动时扫描未完成意图：

1. 将 `PREPARED/IN_PROGRESS` 意图标记为 `ABORTED`；
2. 为对应批次持久化 `E_BATCH_ABORTED` 结果；
3. 校验当前策略表与最后完整数据集快照，不一致则回滚到快照版本；
4. 写入只包含已持久化事实的恢复报告。

恢复报告明确设置 `scope: PERSISTED_FACTS_ONLY` 与 `forcedTerminationExitReportAvailable: false`，不承诺强制终止时生成完整退出报告。

演示脚本以子进程启动 uvicorn，在导入事务提交前调用 `os._exit(92)`：

```powershell
python scripts\kill_recovery_demo.py
```

脚本会验证基线数据集版本仍为 2、策略数仍为 4、崩溃批次被标记为中止，并验证同批次重提返回 `E_BATCH_ABORTED`。也可设置：

```powershell
$env:RG_CRASH_POINT = "after_intent"   # 意图持久化后、业务写入前
$env:RG_CRASH_POINT = "before_commit"  # 业务事务提交前
```

## 验证

```powershell
python -m pytest -q
python scripts\http_smoke.py
python scripts\kill_recovery_demo.py
.\verify.ps1
```

当前实际验证结果见 `VERIFICATION.md`。

