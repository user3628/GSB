# DeployLens 多环境一致性工作台

DeployLens 是一个完全本地运行的多环境部署可视化与诊断工作台。它维护开发、预发、生产等环境的部署单元与不可变快照版本，在 SQLite 单写者事务中重建部署计划、拓扑分层、期望状态摘要和跨环境一致度评分，并对镜像、配置键、期望版本和依赖差异按稳定严重级别输出诊断清单。

项目不连接真实集群、镜像仓库或外部网络。HTTP 服务只绑定回环地址，所有数据来自 `samples/` 下的确定性 JSON 样本。

## 已交付能力

- 环境与部署单元模型：服务名唯一、依赖引用存在、`sha256:<64 hex>` 镜像摘要校验；无效规格返回 `E_SPEC_INVALID`，整份快照不入库。
- 全局单调快照版本：全量快照整体替换、局部增量基于当前版本、陈旧基线拒绝、重复提交幂等、同 ID 异内容冲突。
- 原子级联重建：镜像、配置键或依赖变化时，计算旧图与新图的反向依赖闭包，在同一事务中重建部署计划、拓扑、期望状态和所有环境的评分。
- 显式引用关系：每个部署单元和每个配置键均记录到计划、拓扑、期望状态和评分产物的引用；可通过 API 查询。
- 跨环境只读诊断：镜像、配置、期望版本、依赖差异及环境版本级差，按 `critical → high → medium → low`、环境、服务、差异类型稳定排序。
- 依赖环降级：返回 `E_DEPENDENCY_CYCLE`、环服务、未污染分层和受阻单元；不写入污染状态。
- 并发一致：写提交串行化；读请求固定到单一不可变快照版本并在一个 SQLite 读事务内完成；WAL 支持多读者并发。
- 意图持久化与崩溃恢复：先提交 `pending` 意图，再进入重建事务；强制终止后重启扫描未完成意图，回滚到最后一个完整快照并记录仅基于已落盘事实的报告。
- 可视化工作台：环境卡片、一致度、拓扑层、部署计划、差异表、级联轨迹、引用关系和恢复报告；全部静态资源本地提供。
- HTTP 端到端演示与独立恢复演练脚本。

## 环境准备

要求 Python 3.11 或以上。Windows PowerShell 示例：

```powershell
cd <项目目录>
python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import fastapi, uvicorn, numpy, pytest, sqlite3, hashlib, httpx; print('imports ok')"
```

当前工作区已创建 `.venv`，使用 Python 3.14.4，已确认 FastAPI 0.165.0、uvicorn 0.54.0、NumPy 2.5.5、pytest 9.2.0、SQLite 3.46.1、httpx 0.28.1 可导入。

## 启动工作台

```powershell
.\.venv\Scripts\python -m app --host 127.0.0.1 --port 8765 --db data/workbench.db
```

Linux / WSL 使用 `.venv/bin/python`。浏览器打开 `http://127.0.0.1:8765`。页面顶部按钮依次执行固定样本流程：

1. 提交全量 `v1`
2. 全量替换为 `v2`
3. `prod/inventory` 单镜像摘要增量
4. 注入 `dev` 依赖环（预期被拒绝并显示未污染拓扑）
5. 刷新或手动扫描恢复意图

## 演示与恢复演练

端到端 HTTP 演示会自动启动回环端口上的 uvicorn，提交固定样本，并打印级联轨迹、差异清单和最终摘要：

```powershell
.\.venv\Scripts\python scripts/demo.py --port 0 --db data/demo.db
```

`--port 0` 自动选择空闲回环端口；也可指定 `--port 8765`。

强制终止演练会启动独立写进程，在重建事务中执行 `os._exit(137)`，然后由新进程扫描 SQLite 事实并恢复：

```powershell
.\.venv\Scripts\python scripts/recovery_drill.py --db data/recovery.db --fault mid_rebuild
```

## 自动化测试

```powershell
.\.venv\Scripts\python -m pytest -q
```

覆盖 18 项：无效规格、全量版本替换、幂等与冲突、同基线并发增量、并发重复提交、固定版本并发读、三类级联触发、引用关系、只读诊断、稳定排序、滞后标记、环降级、版本绑定、事务中途强制终止与两种故障点恢复。

## 数据模型与版本语义

平台使用两类版本号：

- **快照版本 `snapshot_version`**：全局单调递增，是查询绑定和事务一致性的单位。每次全量或增量提交生成一个不可变 revision。
- **环境部署代号 `env_version`**：环境自身的发布代号。全量快照显式声明；增量将目标环境代号加 1，其他环境不变。基线环境代号减去某环境代号得到版本级差，超过该环境声明的 `version_lag_tolerance` 时单独标记 `ENVIRONMENT_LAG`。

全量快照必须使用大于当前快照版本的新版本；它会整体替换当前环境集合。增量携带 `base_version`，必须等于当前全局快照版本，否则返回 `E_STALE_BASE` 且不写入状态。

历史 revision 永久保留：查询 `?version=N` 会读取该版本的完整不可变产物。请求不存在或从未提交的版本返回 `E_VERSION_RETIRED`。因此读请求在写入并发发生时，要么在固定旧版本上完整完成，要么得到明确的版本错误，不会看到半新半旧结果。

## 原子提交协议

```text
请求校验（E_SPEC_INVALID / E_DEPENDENCY_CYCLE）
  ↓
检查 commit_id 幂等与内容冲突
  ↓
提交 pending intent（独立落盘）
  ↓
BEGIN IMMEDIATE                    ← 单写者事务开始
  再次校验 base/current 版本
  写入不可变 units、environment_specs、artifacts、refs、cascade_events
  写入 commit 成功记录
  将 intent 标记 completed
COMMIT                             ← 以上全部同时可见
```

若进程在 intent 落盘后、事务提交前终止，SQLite 自动回滚未提交行。重启时的恢复扫描会把该 intent 标记为 `aborted`，并以已提交的 `dataset_revisions` / `commits` / `meta.current_revision` 为准生成报告。报告明确 `basis=sqlite_committed_rows_only`、`exit_report_guaranteed=false`，不声称强制终止前能够输出退出报告。

`Store` 使用进程内可重入写锁，并叠加 SQLite `BEGIN IMMEDIATE`、WAL、`busy_timeout`。同一进程内重复提交相同 `commit_id` 的请求会等待首个写者结束并复用结果。读操作不获取写锁，每个查询使用独立连接和单一只读事务。

## 级联与引用

对目标环境的变更按以下步骤处理：

1. 比较旧/新单元，识别镜像、配置、依赖、期望版本变化。
2. 在旧依赖图与新依赖图分别计算反向依赖闭包并取并集。
3. 计划项携带完整传递依赖的 `dependency_fingerprints`，因此上游单元变化会真实改变所有下游计划内容。
4. 标记并重建：变化单元计划、下游单元计划、目标环境拓扑、期望状态，以及所有环境的跨环境一致度评分。
5. 对每个部署单元和配置键写入到计划/拓扑/期望状态/评分的 `artifact_refs`。
6. 将完整轨迹写入 `cascade_events`，与产物和提交同事务提交。

评分使用 NumPy 点积：镜像 40%、期望版本 20%、配置 20%、依赖 20%。基线环境自身评分为 100。

## 依赖环降级

拓扑通过稳定的 Kahn 分层计算。无法继续的节点组成受阻集合，再用 Tarjan SCC 找出实际环。提交接口返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_DEPENDENCY_CYCLE",
    "details": {
      "environment": "dev",
      "cycle": ["auth", "identity-db"],
      "partial_layers": [["ledger"], ["inventory", "payments"], ["orders"]],
      "blocked_units": ["auth", "gateway", "identity-db"]
    }
  }
}
```

该错误在写入状态前完成计算；不会出现环状态已部分落库的情况。

## HTTP API

所有成功响应为 `{"ok": true, "data": ...}`；所有失败响应为：

```json
{"ok": false, "error": {"code": "E_...", "message": "...", "details": {}}}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/commits/full` | 提交整份快照 |
| POST | `/api/commits/incremental` | 提交局部增量 |
| GET | `/api/commits/{commit_id}` | 查询提交记录 |
| GET | `/api/commits/{commit_id}/cascade` | 查询级联轨迹 |
| GET | `/api/environments?version=` | 环境列表与当前评分 |
| GET | `/api/environments/{env}/bundle?version=` | 单事务读取计划、拓扑、摘要、评分和产物哈希 |
| GET | `/api/environments/{env}/plan?version=&service=` | 按环境与应用/服务查询计划 |
| GET | `/api/environments/{env}/topology?version=` | 查询拓扑分层 |
| GET | `/api/environments/{env}/applications/{application}?version=` | 应用视图：计划、依赖/被依赖者、拓扑与评分 |
| GET | `/api/environments/{env}/references?version=&subject=` | 查询单元/配置键引用关系 |
| GET | `/api/diagnostics?version=&baseline=` | 只读差异诊断 |
| POST | `/api/diagnostics/run?version=` | 执行只读一致性校验 |
| GET | `/api/recovery` | 查询恢复报告 |
| POST | `/api/recovery/scan` | 再次扫描未完成意图 |
| GET | `/api/recovery/pending-intents` | 查询 pending 意图 |
| GET | `/api/samples/{name}` | 读取本地固定样本 |

主要错误码：`E_SPEC_INVALID`、`E_STALE_BASE`、`E_CONFLICTING_COMMIT`、`E_DEPENDENCY_CYCLE`、`E_VERSION_RETIRED`、`E_ENV_NOT_FOUND`、`E_SERVICE_NOT_FOUND`、`E_COMMIT_NOT_FOUND`、`E_NO_CHANGES`、`E_SAMPLE_NOT_FOUND`、`E_INTERNAL`。

交互式 OpenAPI 文档位于 `http://127.0.0.1:8765/docs`。

## 固定样本

- `samples/snapshot_v1.json`：三环境、每环境 7 个部署单元、基线环境 `prod`，含预置配置/镜像差异。
- `samples/snapshot_v2.json`：全局快照 v2、三环境部署代号 2 的整体替换。
- `samples/increment_image.json`：`base_version=2`，只修改 `prod/inventory` 镜像摘要。
- `samples/cycle_increment.json`：`base_version=3`，令 `dev/identity-db` 依赖 `auth`，形成 `auth ↔ identity-db` 环。
- `samples/generate_samples.py`：可重复生成上述 JSON，摘要由 SHA-256 稳定派生。

## 项目结构

```text
app/
  domain.py       校验、拓扑、环检测、依赖闭包、评分、诊断
  models.py       Pydantic 请求模型与规格校验
  store.py        SQLite schema、事务提交、级联、查询、恢复
  main.py         FastAPI 路由、统一错误响应、静态资源
  chaos.py        强制终止演练写进程
  static/         本地可视化工作台
samples/          确定性快照、增量与生成器
scripts/          HTTP 演示与恢复演练
tests/            18 项集成/并发/恢复测试
docs/verification.md  本次实际运行结果
```

## 实际验证结果

本工作区已用 `.venv` 完成 `18 passed`、HTTP 演示 `DEMO OK`、强制终止恢复 `RECOVERY DRILL OK`。关键输出与复现命令见 [docs/verification.md](docs/verification.md)。

## 本地安全边界

应用默认只绑定 `127.0.0.1`，没有认证/多租户能力，不应暴露到非回环地址。演示脚本只访问本机回环端口，不访问外部网络。
