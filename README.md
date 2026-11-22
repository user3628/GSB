# Kubernetes 风格多租户资源治理工作台

这是一个面向 Windows 本地环境、从零实现的确定性多租户资源治理后端。它不依赖真实
Kubernetes、Docker、外部账号或网络服务，使用 Python 3.11+、FastAPI、SQLite（WAL）
和 pytest。所有策略、请求、迁移与故障恢复样本都位于 `samples/`。

## 1. Windows 本地启动

在 PowerShell 或 Windows Terminal 中：

```powershell
cd <项目目录>
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
pytest
python scripts\demo.py --output artifacts\demo_report.json
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

若 `py` 不存在，可使用已安装的 `python`。服务默认数据库为 `data/governance.db`，可用
`GOVERNANCE_DB` 指定其他 SQLite 文件：

```powershell
$env:GOVERNANCE_DB = "data\my-workbench.db"
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

OpenAPI 文档：`http://127.0.0.1:8000/docs`。本项目只使用 Python 标准库 `sqlite3`，不需要
单独安装 SQLite 服务。

## 2. 目录

```text
app/
  policy.py      策略解析、父子校验、摘要、规范化
  migration.py   v1-flat -> v2-hierarchical 迁移裁决器
  storage.py     SQLite schema、WAL、读写事务
  service.py     配额/令牌/层级/版本/状态机/恢复核心
  api.py         FastAPI 路由与统一错误体
  errors.py      业务错误码
samples/
  policies_flat_v1.json
  policies_hierarchical_v2.json
  requests_batch.json
  migration_conflict.json
scripts/
  build_samples.py  重新生成固定样本及校验摘要
  demo.py           端到端演示（迁移差异、分页、强杀恢复）
  crash_probe.py    受控强杀探针：意图落盘后 os._exit
tests/              37 个 pytest 用例
```

## 3. 固定样本

| 文件 | 用途 | 校验摘要 |
|---|---|---|
| `policies_flat_v1.json` | 8 个旧版扁平租户，包含可映射废弃字段 | `sha256:454585a882f780200fbe84fddad754e33f3917facd6e1f1c266f5f56717d9868` |
| `policies_hierarchical_v2.json` | 同一层级树的 8 个新版租户 | `sha256:3e7e75ea060463723c1288a7793a9feee930623f4d963abfcbe43b74ed7ff9fb` |
| `requests_batch.json` | 11 条、按毫秒到达的确定性申请批次 | 无摘要（请求内容由请求 ID 幂等哈希保护） |
| `migration_conflict.json` | 可接受租户 + 冲突废弃字段租户，用于整批回滚 | `sha256:f2bc0aea614c4c06fa5c9c7d4b42a86b64b7787858350c1a18b3ab9a667836ac` |

时间基准为 `1767225600000`（2026-01-01T00:00:00Z），生命周期到 2027-01-01。重新生成样本：

```powershell
python scripts\build_samples.py
```

摘要覆盖“去掉顶层 `checksum` 后、按对象键排序、租户按 `tenant_id` 排序”的规范 JSON。
因此空白、键顺序和租户数组顺序不影响摘要，任何业务字段变化都会导致摘要不匹配。

## 4. 策略模型与校验

新版分层租户示例：

```json
{
  "tenant_id": "alice",
  "parent_id": "team-alpha",
  "quota": {
    "cpu_millicores": 4000,
    "memory_bytes": 10737418240,
    "burst": {"cpu_millicores": 500, "memory_bytes": 2147483648}
  },
  "rate_limit": {"refill_per_second": 20, "capacity": 40, "initial_tokens": 40},
  "lifecycle": {"start_ms": 1767225600000, "end_ms": 1798761600000}
}
```

- 有效资源上限 = 基础硬配额 + 突发额度。CPU 与内存都按有效上限检查。
- 基础 CPU/内存、令牌补充速率、桶容量必须为正整数；突发额度可为 0，初始令牌须在
  `[0, capacity]`；生命周期 `end_ms > start_ms`。
- 租户 ID 全批唯一，父租户必须存在，禁止自引用/环；根深度为 0，最大父链深度为 4。
- 旧版扁平字段：`cpu_millicores_limit`、`memory_bytes_limit`、
  `requests_per_second_limit`、`lifecycle`，以及
  `deprecated_burst_tokens`、`deprecated_initial_tokens`、
  `deprecated_burst_cpu_millicores`、`deprecated_burst_memory_bytes`、
  `deprecated_parent_tenant_id` 等可映射废弃字段。
- 任一校验失败返回 `E_SPEC_INVALID`。全量策略在写库前完整解析，不会存入半批数据。

## 5. 配额与令牌联合裁决

内部使用 `1000` 个 token-unit 表示 1 个请求令牌，避免每个区间向下取整丢失亚令牌精度：

```text
delta_ms         = arrival_ms - last_refill_ms        # 整数毫秒，必须非负
refill_units     = refill_per_second * delta_ms
available_units  = min(capacity * 1000,
                       tokens_scaled + refill_units)
cost_units       = token_cost * 1000                  # 默认 1 个令牌
```

亚毫秒规则：API 只接受整数毫秒时间戳；时间中的亚毫秒部分不进入计算（按 0 截断，
非整数毫秒输入返回 `E_SPEC_INVALID`）。令牌余额保存到 1/1000 令牌，计算本身不逐区间
截断；对外展示的整数令牌使用 `floor(tokens_scaled / 1000)`。

一次申请按固定顺序检查：

1. 整条祖先链的生命周期与阻塞状态（`SUSPENDED`、`MIGRATING`）。
2. 直属租户 CPU/内存有效配额。
3. 从最近父租户向上，祖先聚合 CPU/内存配额；第一个不足者返回
   `E_PARENT_QUOTA_EXCEEDED`，含祖先 ID、余量与差额。
4. 直属租户令牌桶。
5. 祖先令牌桶。

配额不足返回 `E_QUOTA_EXCEEDED`；直属/祖先令牌不足返回 `E_RATE_LIMITED`。拒绝结果
都带当前余量和差额。拒绝不扣配额或令牌；为保持“当前状态”准确，配额/限流拒绝会把
令牌推进到本次到达时刻并持久化，同时将直属租户标记为 `QUOTA_BLOCKED` 或 `THROTTLED`。
后续成功准入会在同一事务中自动恢复为 `ACTIVE`。

子租户成功申请会同时扣除自身和全部祖先的配额、令牌；直属占用与祖先聚合占用分列保存。
每次策略提交都按新父链从直属占用重建祖先聚合，避免改父后出现漂移。整条申请要么全部
扣减，要么完全不扣减。

## 6. 状态机

状态：`ACTIVE`、`THROTTLED`、`QUOTA_BLOCKED`、`SUSPENDED`、`MIGRATING`。

| 当前状态 | 允许迁移到 |
|---|---|
| `ACTIVE` | `THROTTLED`, `QUOTA_BLOCKED`, `SUSPENDED`, `MIGRATING` |
| `THROTTLED` | `ACTIVE`, `QUOTA_BLOCKED`, `SUSPENDED`, `MIGRATING` |
| `QUOTA_BLOCKED` | `ACTIVE`, `THROTTLED`, `SUSPENDED`, `MIGRATING` |
| `SUSPENDED` | `ACTIVE`, `MIGRATING` |
| `MIGRATING` | `ACTIVE`, `SUSPENDED` |

自迁移和表外路径返回 `E_INVALID_TRANSITION`，事务回滚，不修改已持久化状态。
`POST /api/tenants/{id}/state` 可同时释放直属配额；状态、直属占用、全部祖先占用、
令牌余额、运行态快照和状态事件在同一 SQLite 事务中提交。

## 7. 版本、增量与读者一致性

- 全量快照带单调递增 `version` 和 `sha256` 摘要；版本必须大于当前版本，否则
  `E_VERSION_CONFLICT`。当前快照整体保存，不存在按租户混读新旧版本。
- 增量提交携带 `base_version` 和受影响租户片段。片段对已有租户做递归字段合并；新租户
  必须给完整定义。`base_version` 不是当前版本时返回 `E_STALE_BASE`，且不创建业务写入。
- 增量版本固定为 `base_version + 1`；服务写锁和 SQLite `BEGIN IMMEDIATE` 保证同一基础
  版本的两个并发增量只有一个成功，另一个得到 `E_STALE_BASE`。
- 每次业务提交分配一个单调 `revision`。策略、运行态、裁决、状态事件、迁移报告和意图
  完成标记在同一事务中提交。
- SQLite 使用 WAL。读者使用独立连接和 `BEGIN DEFERRED`，并按 `revision` 固定读取。
  `runtime_snapshots` 保存每个完整 revision 的租户运行态，因此历史 revision 可重复读取。
- 分页按 `decisions.id ASC` 做 `COUNT + LIMIT/OFFSET`，整个查询在同一读事务内。客户端
  应把首次响应的 `revision` 传给后续页；这样即使期间有新写者提交，分页拼接也与固定
  revision 的整取结果逐项一致、不重不漏。

## 8. 迁移 v1-flat -> v2-hierarchical

`POST /api/migrations`：

```json
{
  "legacy": { "schema_version": "governance.policy/v1-flat", "...": "..." },
  "base_version": 2,
  "parent_map": {"legacy-child": "platform"},
  "confirm_lifecycle_overlap": false,
  "generated_at_ms": 1767225600000
}
```

迁移逐租户输出：

- `ACCEPTED`：字段完整映射，包含逐字段 `mapping_basis`。
- `PENDING_REVIEW`：未知废弃字段、废弃别名冲突、生命周期与当前策略重叠但未显式确认。
  整批返回 `E_MIGRATION_REVIEW`。
- `REJECTED`：必填字段非法、父租户不存在、深度超限等。整批返回
  `E_MIGRATION_REJECTED`，并计入迁移报告。

任何 `PENDING_REVIEW` 或 `REJECTED` 都会使整批回滚：当前策略版本、运行态、配额和令牌
不变；报告作为审计记录单独提交。全部接受时创建新版本快照并原子激活。
`GET /api/migrations/{id}` 会重新计算报告摘要并返回 `replayable`，报告含输入摘要、输出
版本、逐项映射依据和 `report_checksum`。`POST /api/migrations/{id}/replay` 提供只读审计
回放：校验持久化输入与报告摘要并返回相同报告，明确标注 `AUDIT_ONLY_NO_STATE_CHANGE`，
不会重新激活策略。

## 9. 意图日志与强杀恢复

每个写操作：

1. 单独事务插入 `intents`，状态 `PENDING`，记录类型、基础 revision、规范 payload 和摘要。
2. 开启 `BEGIN IMMEDIATE`，业务变更、`runtime_snapshots` 和 intent 的 `COMMITTED` 标记
   同事务提交。
3. 启动时扫描仍为 `PENDING` 的意图，统一标记 `ABORTED`，写入 `recovery_runs`。

SQLite 事务原子性保证未提交的配额/令牌扣减不会落盘。恢复报告只引用已持久化事实：
pending 意图、基础/观测 revision、最后完整 revision、快照版本、运行态摘要和逐意图动作。
可使用 `scripts/crash_probe.py` 复现“intent 已落盘、业务事务未开始，进程 `os._exit(91)`”。

## 10. API 与错误体

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/api/meta` | 当前 revision / 快照版本 |
| GET/PUT | `/api/snapshot` | 读取/全量替换策略快照 |
| PATCH | `/api/snapshot` | 增量片段提交 |
| POST | `/api/decisions` | 单条准入裁决 |
| POST | `/api/decisions/batch` | 批量裁决 |
| GET | `/api/decisions` | 分页；`page`、`page_size`、`revision` |
| GET | `/api/decisions/{request_id}` | 按请求 ID 查询 |
| GET | `/api/tenants` | 固定 revision 查看策略、占用、令牌、状态 |
| POST | `/api/tenants/{id}/state` | 状态迁移，可同时释放配额 |
| POST | `/api/migrations` | 旧版迁移裁决 |
| GET | `/api/migrations` | 迁移列表 |
| GET | `/api/migrations/{id}` | 可校验迁移报告 |
| POST | `/api/migrations/{id}/replay` | 只读审计回放，不改变状态 |
| GET | `/api/recovery/latest` | 最近恢复报告 |

所有业务错误统一为：

```json
{
  "error": {
    "code": "E_QUOTA_EXCEEDED",
    "message": "tenant quota is insufficient",
    "details": {
      "tenant_id": "alice",
      "resources": {
        "cpu_millicores": {
          "requested": 1600,
          "used": 3000,
          "effective_limit": 4500,
          "remaining": 1500,
          "shortfall": 100
        }
      }
    }
  }
}
```

单条拒绝裁决另附 `decision` 审计对象；批量接口每个 item 同时带 `decision` 和 `error`
（接受时 `error` 为 `null`）。重复提交同一请求内容会复用原裁决，并带
`Idempotent-Replayed: true`；内容不同返回 `E_CONFLICTING_REQUEST`。

## 11. 实际验证结果

当前环境：Python 3.14.4（代码要求 3.11+），FastAPI 0.115.6，uvicorn 0.34.0，
pytest 8.3.4。执行：

```text
python -m compileall -q app scripts tests
pytest
37 passed, 42 warnings in 2.22s
```

这些 warning 均是 Python 3.14 下 Starlette/FastAPI 的内部弃用提示，不是测试失败。
另外已实际启动 `uvicorn app.main:app --host 127.0.0.1 --port 8765`，`GET /health`
返回 `{"status":"ok"}`。

`python scripts/demo.py --output artifacts/demo_report.json` 输出 `DEMO RESULT: PASS`，实际结果：

- 旧扁平策略：`OK=6`、`E_RATE_LIMITED=3`、`E_QUOTA_EXCEEDED=1`、
  `E_PARENT_QUOTA_EXCEEDED=1`。
- 新分层策略：`OK=7`、`E_RATE_LIMITED=2`、`E_QUOTA_EXCEEDED=1`、
  `E_PARENT_QUOTA_EXCEEDED=1`。
- 旧版迁移：8/8 `ACCEPTED`，`APPLIED`；迁移前后同一批次裁决差异为 0，说明可完整映射。
- 旧扁平 vs 新版分层：`req-alice-quota` CPU 差额由 1100 变 100；
  `req-dave-parent` 祖先 CPU 差额由 800 变 200；`req-edge-2` 由限流拒绝变为接受。
- 三个策略场景的分页拼接均与整取结果逐项一致。
- 强杀探针退出码 91；恢复发现并中止 1 个 PENDING 意图，最后完整 revision 保持为 2，
  starter 的配额占用与令牌状态逐字段未变，未提交请求未出现在裁决表中。结论：
  `UNCOMMITTED_INTENTS_ABORTED_LAST_COMPLETE_REVISION_PRESERVED`。

完整机器可读输出见 `artifacts/demo_report.json`。

## 12. 设计假设

- “突发额度”作为基础资源上限之外的额外硬上限，有效上限为两者之和。
- 祖先配额取子树聚合占用；祖先不足时从最近父租户向上报告第一个不足者。
- 配额/限流拒绝会推进并持久化令牌补充，但不扣令牌；状态标记可由后续成功准入自动恢复。
- 全量替换保留已有租户的直属占用；策略提交会按新层级重建祖先聚合。仍有直属占用的
  租户不能被删除。
- 推荐以一个 uvicorn 进程运行。跨进程写入仍由 SQLite `BEGIN IMMEDIATE` 串行化；当前
  幂等预处理针对单服务实例，避免多 worker 的重复裁决竞争。
