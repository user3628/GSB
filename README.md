# 本地推理网关路由组件

面向 Windows 本地模型服务的确定性推理网关。组件以 FastAPI 接收推理请求，以带版本与摘要的路由表选择后端，在全局并发、后端并发、后端显存与每轮时间预算共同约束下并行调度，并用 SQLite 保存请求状态、后端预留、轮次轨迹、意图日志与恢复报告。

本仓库包含可运行的本地确定性模型后端、两版路由样本、混合租户请求批次、固定随机种子张量、pytest 测试、结构化验证器和强制终止恢复端到端脚本。不使用 Docker 或任何容器。

## 快速开始（Windows PowerShell）

要求 Python 3.11 及以上。本项目实际验证环境为 Python 3.14.4。

```powershell
cd <repo>
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

若没有 `py` 启动器，使用 `python -m venv .venv`。需要严格复现时可安装锁定版本：

```powershell
python -m pip install -r requirements.lock.txt
```

Linux/macOS/WSL 对应命令：

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

本次实际安装版本：

| 包 | 版本 |
|---|---:|
| Python | 3.14.4 |
| fastapi | 0.165.0 |
| uvicorn | 0.54.0 |
| numpy | 2.5.5 |
| pytest | 8.4.2 |
| requests | 2.34.2 |
| pydantic | 2.14.4 |
| httpx | 0.28.1 |

## 本地端口与启动

约定本地回环端口：

- 网关 API：`127.0.0.1:19080`
- 确定性模型后端：`127.0.0.1:19101`
- SQLite：工作区中的 `gateway.db` 或命令行 `--db` 指定路径

启动确定性模型后端：

```powershell
python scripts/mock_backend.py --host 127.0.0.1 --port 19101 --behaviors samples/backend_behaviors.json
```

新终端启动网关：

```powershell
$env:GATEWAY_DB_PATH = ".\gateway.db"
$env:GATEWAY_GLOBAL_CONCURRENCY = "2"
$env:GATEWAY_QUEUE_LIMIT = "20"
$env:GATEWAY_ROUND_BUDGET_MS = "600"
python -m gateway.api --host 127.0.0.1 --port 19080 --db .\gateway.db
```

网关按单进程设计（uvicorn 默认 1 worker）。不要以多个 worker 同时打开同一 SQLite 文件；强制终止恢复假定同一时刻只有一个网关所有者。

健康检查：`GET http://127.0.0.1:19080/health`。交互式 OpenAPI：`/docs`。

样本 `samples/route_v1.json` 与 `samples/route_v2.json` 已面向 `19101` 端口生成正确摘要。若修改端点或字段，必须重算摘要。可调用 `POST /routes/checksum`，或使用：

```powershell
python -c "import json; from gateway.domain import compute_route_checksum as c; d=json.load(open('samples/route_v1.json',encoding='utf-8')); d.pop('checksum',None); print(c(d))"
```

## 一键验收

```powershell
python e2e/run_e2e.py
```

脚本自行分配空闲回环端口、启动后端与网关，完成：

1. 提交 v1 路由并验证过期路由拒绝。
2. 批量提交 `samples/requests_batch.json`。
3. 第一轮并行派发、容量跳过。
4. 第二轮并行派发、时间预算耗尽 SHED、挂起请求 250ms 超时释放。
5. 后端失败统计、熔断与 `E_BACKEND_DEGRADED`。
6. 在请求处于 `DISPATCHED` 时强杀网关，重启后扫描持久化事实恢复。
7. 重排恢复请求、取消并真实关闭 HTTP 连接、释放并发与显存。
8. 运行结构化验证器，要求全部检查 PASS。

输出目录 `artifacts/e2e/`：

- `round1_trace.json`：第一轮完整调度轨迹。
- `round2_trace.json`：第二轮完整调度轨迹。
- `validation_report.json`：最终验证报告。
- `e2e_report.json`：全部断言、请求结局、恢复报告与两轮轨迹。
- `e2e_summary.json`：便于快速查看的验收摘要。

测试命令：

```powershell
python -m pytest -q
```

独立验证持久化数据库：

```powershell
python -m gateway.validation --db .\gateway.db
```

## 路由表

规范字段示例：

```json
{
  "version": 1,
  "generated_at": "2026-11-22T03:00:00Z",
  "global_concurrency_limit": 2,
  "backends": {
    "backend-a": {
      "endpoint": "http://127.0.0.1:19101",
      "vram_limit_mb": 256,
      "concurrency_limit": 2,
      "vram_per_sample_mb": 2.0,
      "estimated_latency_ms": 80,
      "failure_threshold": 2
    }
  },
  "models": {
    "alpha": {"backend_id": "backend-a", "estimated_latency_ms": 80}
  },
  "checksum": "sha256:..."
}
```

摘要计算对象是提交的原始 JSON，去除 `checksum`、`digest`、`sha256`、`route_checksum` 字段后，以 UTF-8 规范 JSON（键排序、无多余空白）做 SHA-256，前缀为 `sha256:`。

提交规则：

- 摘要错误：`E_ROUTE_INVALID`，不落库。
- 版本不大于当前版本：`E_STALE_ROUTE`，当前路由不变。
- 新路由在单个事务内整体写入并切换当前版本。
- 每个请求在准入时固化 `route_version`、`backend_id`、`endpoint` 与显存需求，后续路由替换不会混用新旧规格。
- 只允许回环地址作为后端端点；拒绝 URL 凭据。

兼容输入别名：`vram_budget_mb`/`vram_budget`、`max_concurrency`、`vram_mb_per_sample`、`latency_ms`，但持久化使用上表规范字段。

## 请求状态机

业务状态：`PENDING`、`QUEUED`、`DISPATCHED`、`COMPLETED`、`FAILED`、`SHED`。恢复专用终态：`ABORTED`。

| 源状态 | 合法目标 |
|---|---|
| PENDING | QUEUED, FAILED, SHED |
| QUEUED | DISPATCHED, FAILED, SHED, ABORTED |
| DISPATCHED | COMPLETED, FAILED；强制恢复时为 QUEUED 或 ABORTED |
| COMPLETED / FAILED / SHED / ABORTED | 终态 |

非法迁移返回 `E_INVALID_TRANSITION`，事务回滚且不改变已持久化状态。人工迁移接口不允许从 `DISPATCHED` 直接改写资源状态，也不允许手工进入 `DISPATCHED`；这些迁移只能由调度器或恢复器完成。

## 准入与调度

请求字段：`request_id`、`model_id`、`sample_count`、`latency_cap_ms`、`priority`（越大越优先）、`tenant_id`。兼容延迟字段名 `expected_latency_ms`、`latency_ms`、`timeout_ms`。

准入同时检查：

- 有效全局并发上限 `min(配置上限, 路由上限)`。
- 目标后端并发上限。
- 显存需求 `sample_count * vram_per_sample_mb` 不超过后端显存预算。

普通准入容量暂满时进入有界等待队列；绝对显存需求超限立即返回 `E_CAPACITY_EXCEEDED` 并附当前余量。`POST /requests?strict_capacity=true` 要求当前余量立即满足，否则返回 `E_CAPACITY_EXCEEDED` 并留下 FAILED 请求事实。队列满返回 `E_QUEUE_FULL`。

每轮调度在决策锁内冻结资源事实，按以下确定性顺序选请求：

1. 优先级降序。
2. 同优先级内按租户历史派发次数、历史已消耗估算预算、历史派发序号和提交序号做租户公平轮转。
3. 同租户内按提交序号。

每轮用 `estimated_latency_ms` 累加判断时间预算。`POST /rounds` 请求体：

```json
{"time_budget_ms": 200, "degrade_excess": true, "max_dispatches": null}
```

- `degrade_excess=false`：容量或预算暂不满足的请求保持 `QUEUED`，并记录跳过原因。
- `degrade_excess=true`：按同一确定性顺序将不能进入本轮的请求迁移为 `SHED`。
- 跳过/降级原因包括 `GLOBAL_CONCURRENCY`、`BACKEND_CONCURRENCY`、`VRAM_BUDGET`、`TIME_BUDGET`、`BACKEND_DEGRADED`、`QUEUE_FULL`、`MAX_DISPATCHES`。
- 已 `DISPATCHED` 的请求不会被后续轮次中断。
- 每轮轨迹包含派发顺序、每个请求派发前剩余预算、跳过清单、策略、轮次摘要与轮末后端快照。

派发事务同时完成 `QUEUED -> DISPATCHED`、后端 `in_flight + 1`、显存预留增加、租户使用量更新和请求绑定；不存在“已派发但未计数”的可见中间状态。

## 超时、取消与熔断

每次派发使用独立短连接和请求级读超时 `latency_cap_ms`。超时或取消时：

- 主动 `shutdown` 并关闭底层 HTTP 连接。
- 工作线程在释放事务中同时完成请求终态、后端在途减一、显存预留清零和失败统计。
- 查询显示 `FAILED + E_BACKEND_TIMEOUT`；不会把“跳过”误记为超时。
- 网关只关闭自身 HTTP 连接并释放网关侧资源，不宣称已终止后端进程中可能仍在执行的代码。

同一后端连续失败达到 `failure_threshold` 后进入 `DEGRADED`。新请求返回 `E_BACKEND_DEGRADED`；已排队请求在轮次中被标记为 FAILED 并附业务码。管理端可用 `POST /admin/backends/reset` 在后端无在途时恢复健康状态。

## SQLite 一致性、幂等与恢复

数据库启用 WAL、`synchronous=FULL` 与事务级串行访问。资源变更采用两阶段意图：

1. 单独事务写入 OPEN 意图并提交。
2. 在下一事务中原子提交“请求状态 + 后端计数 + 显存预留 + 关闭意图”。

进程在步骤 1 与 2 之间退出时，请求仍处于旧的一致状态；在步骤 2 后退出时，状态与资源已一起闭合。

启动恢复扫描已持久化事实：

- 所有 OPEN 意图标记为 ABORTED。
- 所有 `DISPATCHED` 请求在路由/后端仍存在、未取消且恢复次数未超限时回退为 `QUEUED`；否则标记 `ABORTED`。
- 从请求事实重建每个后端的在途计数与显存预留。
- 运行中的轮次标记 `INTERRUPTED`。
- 写入恢复报告，字段注明 `source=persisted_facts_only`。报告不声称被强杀进程已优雅退出。

幂等：相同 `request_id` 与完全相同内容复用已有请求，不重复入队；内容不同返回 `E_CONFLICTING_REQUEST`。查询与批量回放只读取完整事务结果。

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/routes/checksum` | 计算原始路由 JSON 摘要 |
| POST | `/routes` | 原子提交新路由版本 |
| GET | `/routes/current` | 当前路由 |
| GET | `/routes` / `/routes/{version}` | 路由历史/指定版本 |
| POST | `/requests` | 提交单个请求；支持 `strict_capacity`、`wait` |
| POST | `/requests/batch` | 批量回放，接受数组或 `{requests: [...]}` |
| GET | `/requests/{id}` | 请求完整状态；`include_transitions=true` 返回迁移历史 |
| POST | `/requests/{id}/cancel` | 取消；排队请求立即终态，派发中请求关闭连接后释放 |
| POST | `/requests/{id}/transition` | 人工迁移校验接口，不允许资源绕过 |
| POST | `/rounds` | 执行一轮确定性调度 |
| GET | `/rounds` / `/rounds/{id}` | 轮次列表/轨迹与后端快照 |
| GET | `/backends` | 在途、显存、熔断与连接状态 |
| GET | `/recovery/report` | 最近恢复报告 |
| GET | `/validation/report` | 结构化一致性验证 |
| GET | `/health`、`/config` | 健康与运行限制 |

统一失败响应：

```json
{"ok": false, "code": "E_CAPACITY_EXCEEDED", "message": "...", "details": {}}
```

至少覆盖：`E_STALE_ROUTE`、`E_ROUTE_INVALID`、`E_CAPACITY_EXCEEDED`、`E_QUEUE_FULL`、`E_CONFLICTING_REQUEST`、`E_INVALID_TRANSITION`、`E_BACKEND_DEGRADED`、`E_BACKEND_TIMEOUT`。

其它结构化业务错误包括 `E_ROUTE_UNAVAILABLE`、`E_ROUND_IN_PROGRESS`、`E_ROUND_CONFLICT`、`E_REQUEST_CANCELLED`、`E_TIME_BUDGET_EXHAUSTED`、`E_SHED_CAPACITY`。

## 验证工具

`GET /validation/report` 与 `python -m gateway.validation --db ...` 检查：

- 路由来源摘要与已提交摘要一致。
- 每个请求的迁移链连续、合法，最后状态与迁移记录一致。
- 每个后端 `in_flight` 等于该后端 `DISPATCHED` 请求数。
- 每个后端 `vram_reserved_mb` 等于在途请求预留之和。
- 全局/后端并发与显存不超过预算。
- 每个请求只绑定提交时的一个路由版本、后端端点与显存语义。
- 轮次估算预算未越界，同一轮无重复派发，轨迹有对应持久化派发迁移。
- 稳定快照中不存在未关闭意图，也不存在无主 `DISPATCHED` 状态。

验证结论为 `PASS` 或 `FAIL`，每项检查均包含 `id`、`status`、说明与失败明细。

## 本次实际验证结果

- `python -m pytest -q`：20 个测试全部通过。
- `python e2e/run_e2e.py`：全部端到端断言通过，最终验证 `PASS`。
- 第一轮派发：`r-001 -> backend-a`、`r-002 -> backend-b`，其余因全局并发满保持排队并记录 `GLOBAL_CONCURRENCY`。
- 第二轮派发：`r-003`（80ms）与 `r-007`（120ms），估算预算恰好用尽 200ms；其余 4 个请求按顺序 SHED，原因 `TIME_BUDGET`。
- `r-002` 注入后端失败，结局 `FAILED + E_BACKEND_FAILURE`。
- `r-007` 挂起响应在 250ms 真实超时，结局 `FAILED + E_BACKEND_TIMEOUT`；后端在途与显存均归零，HTTP 连接注册表为空。
- `backend-b` 连续失败达到阈值，状态 `DEGRADED`，后续提交返回 `E_BACKEND_DEGRADED`。
- 强杀网关时 `crash-hang` 为 `DISPATCHED`；重启恢复报告 `requeued_count=1`、`open_intent_count=0`、`source=persisted_facts_only`，后端计数重建为零。
- 恢复请求重新派发后取消，结局 `FAILED + E_REQUEST_CANCELLED`，资源闭合。
- 最终验证 9 项检查全部 PASS：路由摘要、状态机、预留闭合、资源上限、路由绑定、轮次轨迹、意图日志、恢复一致性等。

更完整的持久化表、事务与锁设计见 `docs/DESIGN.md`。固定样本位于 `samples/`，测试位于 `tests/`，完整验收脚本位于 `e2e/run_e2e.py`。
