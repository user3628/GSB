# 本地配置一致性协调器

面向本地多组件平台的确定性配置一致性组件。输入是带版本号和 SHA-256 校验摘要的 JSON 快照；协调器在固定点数值域内计算配额占比、阈值余量、整型上限利用率和跨组件加权一致度，并以单一 SQLite 事务提交快照、组件指标、一致度结论与误差说明。

项目不依赖外部服务。样本、随机种子、HTTP 服务、演示与测试均在本机运行。

## 快速开始（Windows）

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts\generate_samples.py
python -m pytest -q
python scripts\demo.py
```

启动 HTTP 服务：

```powershell
$env:CONSISTENCY_DB = "$PWD\var\consistency.sqlite3"
.\.venv\Scripts\python.exe -m uvicorn app.api:app --host 127.0.0.1 --port 8000
```

也可以直接使用：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_demo.ps1
powershell -ExecutionPolicy Bypass -File scripts\run_api.ps1
```

本次实际验证运行在 WSL/Linux 的 Python 3.14 环境；代码和依赖版本均兼容 Python 3.11，Windows 专有分支仅使用标准库 `msvcrt` 实现进程锁。SQLite 使用 Python 内置 `sqlite3`，无需单独安装。

## 样本与摘要

`scripts/generate_samples.py` 使用固定种子 `20261120` 生成：

- `data/baseline.json`：版本 1，全量基线。
- `data/adjusted.json`：版本 2，只修改 `api` 的配额与用量。
- `data/crash_candidate.json`：版本 3，用于强制终止恢复演示。
- `data/mixed_sum.json`：大数/小数混合累计样本。

摘要定义为：移除 `checksum` 字段后，对键排序、Decimal 规范化、JSON 紧凑序列化，再计算 SHA-256。组件对象键顺序变化不改变摘要。空值、非有限数字、重复 JSON 键、缺失字段或不合法数值范围均返回 `E_SNAPSHOT_INVALID`。

快照核心结构：

```json
{
  "schema_version": "1.0",
  "snapshot_version": 1,
  "generated_at": "2026-11-20T00:00:00Z",
  "policy": {
    "relative_tolerance": 0.000010,
    "min_weighted_consistency": 0.900000,
    "default_min_headroom_ratio": 0.100000
  },
  "components": {
    "api": {
      "quota": 400,
      "ratio": 0.400000,
      "ratio_tolerance": 0.050000,
      "threshold": 120,
      "usage": 41.830299,
      "min_headroom_ratio": 0.100000,
      "limit": 1000,
      "weight": 0.400000
    }
  },
  "checksum": "sha256:..."
}
```

## 定点精度契约

生产判定路径不依赖二进制浮点。

- 内部表示：有符号整数微单位，`1.000000` 表示为 `1_000_000`。
- 小数位：固定 6 位；输出数值一律序列化为 `"0.393939"` 这样的 6 位字符串，并同时保留整数 `micros`。
- 舍入：每次十进制转换、除法和无量纲乘法都使用 `ROUND_HALF_EVEN`。
- 量纲：配额和权重先归一化；阈值绝对余量仅用于展示，所有判定使用无量纲 `headroom_ratio`；上限判定使用 `usage / limit`，不混用配额、阈值和权重单位。
- 顺序：组件按 `component_id` 字典序排序后计算，输入对象顺序不影响结果。
- 累计：使用 Kahan 补偿结构的整数累计。整数微单位加法本身精确，补偿项固定为 0；该结构防止未来替换为不精确累加器时静默改变结论。

派生指标：

```text
quota_share_i     = quota_i / sum(quota)
headroom_abs_i    = threshold_i - usage_i
headroom_ratio_i  = headroom_abs_i / threshold_i
limit_utilization = usage_i / limit_i
normalized_weight = weight_i / sum(weight)
share_score_i     = clamp(1 - abs(quota_share_i - ratio_i) / ratio_tolerance_i, 0, 1)
weighted_consistency = sum(normalized_weight_i * share_score_i)
```

误差说明包含每次操作的 `rounding_mode`、结果微单位、单步误差增量、累计误差上界和量纲。误差上界按输入误差、一阶传播和单次半微单位舍入残差保守累计，并向上取整到微单位。

### 混合累计对比

`data/mixed_sum.json` 固定样本为 `1e12 + 20 × 0.1 - 1e12`。本次运行结果：

| 路径 | 结果 | 实测误差 | 保守上界 |
| --- | ---: | ---: | ---: |
| 定点整数 Kahan 路径 | `2.000000` | `0.000000` | `0.000000` |
| binary64 朴素求和 | `1.999512` | `0.000489` | `0.009326` |
| binary64 Kahan 求和 | `2.000000` | `0.000000` | `0.018652` |

浮点上界公式为 `gamma_(n-1) * sum(abs(x))`，Kahan 路径使用 `2 * gamma_(n-1) * sum(abs(x))`。生产判定只使用定点整数路径。

### 相对容差与平局

区间判定不是裸浮点比较。对边界 `b` 和观测值 `v`，允许量为：

```text
ceil(relative_tolerance * max(abs(b), abs(v), 1.0))
```

零附近以 `1.000000` 作为相对尺度，避免容差退化。容差内视为一致。组件按失败优先、`share_score` 升序、`component_id` 字典序排列；分数相同时使用稳定字典序，`critical_component` 取该顺序首项。

## 事务、并发与恢复

SQLite 使用 WAL、`synchronous=FULL`、`busy_timeout=30000`。提交串行化由两层保证：

1. 进程内 `RLock`。
2. 数据库旁路文件锁，POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`，崩溃时由操作系统释放。

提交流程：

1. 完整解析并验证摘要。
2. 检查 `base_version`、版本单调性和重复版本。
3. 在独立事务中持久化 `started` 意图记录。
4. 在内存中完成全部定点指标和一致度计算。
5. 在一个 `BEGIN IMMEDIATE` 事务中写入快照、全部组件指标、一致度结论、误差说明、提交响应和当前版本指针，同时把意图标记为 `committed`。

查询只读取 `status='committed'` 且指标、结论、误差说明、响应均完整的版本。因此不会看到某一组件已更新而加权一致度仍旧的中间状态。历史版本继续按版本查询，但当前视图只指向最后一次完整提交。

- 同版本、同摘要的重复提交复用已持久化结果并返回 `"reused": true`。
- 同版本、不同摘要返回 `E_DUPLICATE_VERSION`。
- 携带过期 `base_version` 返回 `E_STALE_BASE`，且在任何意图写入前退出。
- 并发的相同快照提交只有一个执行计算，其余等待后复用结果。

重启恢复扫描 `started` 意图：

- 若不存在快照行，标记意图为 `recovered_aborted`，当前版本不变。
- 若存在不完整快照，删除其指标/结论/误差/响应，将版本标为 `aborted`，必要时把当前指针回退到最近的完整版本。
- 若完整快照已经持久化，则保留完整数据。

恢复报告只包含 SQLite 中已持久化的观测和动作，明确声明强制终止进程无法产生完整优雅退出报告。

## HTTP API

所有错误响应统一为：

```json
{
  "error": {
    "code": "E_SNAPSHOT_INVALID",
    "message": "snapshot checksum does not match canonical payload",
    "details": {}
  }
}
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/snapshots?base_version=1` | 提交完整快照；body 也可为 `{"base_version": 1, "snapshot": {...}}` |
| `GET` | `/api/v1/snapshots/{version}/metrics` | 查询完整版本的组件派生指标 |
| `GET` | `/api/v1/snapshots/{version}/consistency` | 查询判定、差异报告与误差覆盖 |
| `GET` | `/api/v1/snapshots/{version}/errors` | 查询每步舍入与累计误差说明 |
| `GET` | `/api/v1/recovery/report` | 查询最近恢复报告及历史报告 |
| `POST` | `/api/v1/recovery/scan` | 手工触发恢复扫描 |
| `GET` | `/api/v1/current` | 查询当前完整提交版本 |
| `GET` | `/health` | 健康检查 |

至少覆盖的业务码：`E_SNAPSHOT_INVALID`、`E_STALE_BASE`、`E_DUPLICATE_VERSION`；另有 `E_VERSION_NOT_FOUND`、`E_BAD_REQUEST`、`E_INTERNAL`。

PowerShell 提交示例：

```powershell
$body = Get-Content data\baseline.json -Raw
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/api/v1/snapshots?base_version=0" `
  -ContentType "application/json" -Body $body
```

## 可复现演示

`scripts/demo.py` 执行并写入 `artifacts/demo_result.json`：

1. 基线 v1 全量提交、误差说明和两次重算逐位一致性。
2. 修改单个 `api.quota` 后提交 v2，展示配额占比和下游加权一致度级联变化，同时确认 v1 历史视图未混入 v2。
3. 子进程提交 v3，在意图记录持久化后执行 `os._exit(91)`；重启扫描将意图标为中止，当前版本仍为 v2，v3 不可见。
4. 输出混合大数/小数的朴素浮点、Kahan 浮点、定点整数路径误差对比。

本次实测结论：

- `pytest`：`25 passed`。
- v1：`CONSISTENT`，加权一致度 `1.000000`，累计误差 `0.000000`。
- v2：`CONSISTENT`，`api` 占比 `0.393939`，加权一致度 `0.914142`，累计误差上界 `0.000007`。
- 强制终止：子进程退出码 `91`，存在 1 条 `started` 意图；恢复后当前版本仍为 `2`，完整版本为 `[1, 2]`，崩溃版本 `3` 返回 `E_VERSION_NOT_FOUND`。
- 所有样本的误差上界均被相对容差覆盖，重复计算与组件重排后的结果逐位一致。

## 项目结构

```text
app/fixed.py       定点运算、舍入、误差传播、Kahan/浮点对比
app/snapshot.py    严格 JSON 解析、摘要、快照结构校验
app/engine.py      派生指标、容差判定、差异报告、舍入账本
app/db.py          SQLite schema、事务、跨进程文件锁
app/service.py     单写者提交、幂等复用、查询、恢复
app/api.py         FastAPI HTTP 接口与统一错误响应
data/              固定种子生成的确定性 JSON 样本
scripts/           样本生成、崩杀子进程、端到端演示
tests/             数值、快照、引擎、服务、API、恢复测试
artifacts/         实测结果与临时 SQLite（数据库被 .gitignore 排除）
```

## 已知边界

- 误差上界是保守界，不等同于实际误差；它用于证明容差覆盖关系，不替代业务容差配置。
- 输入数值超过 6 位小数时会在固定点转换处舍入，并在误差账本中记录；若业务要求拒绝超精度输入，应在快照生成侧加更严格限制。
- 本实现面向单机本地服务。跨进程写者通过文件锁串行化，不使用分布式协调。
- 不承诺进程被强制终止时生成优雅退出报告；恢复事实只来自重启后 SQLite 中的持久化记录。
