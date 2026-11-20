# 本地 Kubernetes 调度与亲和性判定工作台

这是一个完全离线、确定性的调度诊断项目。它读取本地 JSON 中的节点与待调度 Pod，按本地实现的 Kubernetes 调度语义执行资源累计、节点选择器、节点亲和性、污点/容忍、Pod 间亲和/反亲和、抢占与加权打分，并通过 FastAPI 页面或脚本输出可复核的过滤轨迹、余量账本、约束传播和恢复报告。

运行时不连接真实集群，不依赖 kubelet、容器运行时、Docker 或外部镜像。HTTP 服务和演示脚本只需要 Python 3.11+ 与 `requirements.txt` 中的本地依赖。

## 已实现能力

- CPU 统一为毫核整数，内存统一为字节整数；支持 `500m`、`2`、`1.5`、`512Mi`、`1Gi` 等写法，并对非法单位、负数和 signed 64-bit 溢出返回 `E_INVALID_QUANTITY`。
- 节点初始分配账本逐 Pod 扣减，每步保存扣减前/后的整数余量和拒绝快照；任何被拒候选都不改变状态。
- 支持节点选择器，以及 required node affinity 的 `In`、`NotIn`、`Exists`、`DoesNotExist`、`Gt`、`Lt` 和表达式组合。
- 支持 `NoSchedule`、`PreferNoSchedule`、`NoExecute`；`NoExecute` 通过样本逻辑时钟、taint `timeAddedSeconds`、toleration `tolerationSeconds` 计算剩余时间窗。
- 支持 required/preferred Pod affinity 与 anti-affinity，依据已放置 Pod 标签和拓扑键逐 Pod 重算。
- 软约束使用整数 micro-point 定点累计，总分固定 6 位小数；诊断中包含各分项、half-up 舍入残差、资源平衡度、污点降权与 tie-break。
- 可选优先级抢占：仅驱逐低优先级、`preemptable: true` 的已分配 Pod，并在放置前重算余量与硬约束。
- SQLite 两阶段持久化：先写 `intent`，再写最终结论；启动扫描未完成意图并按持久化规格重算，失败则标记 `aborted`。
- 本地静态工作台展示节点余量、Pod 过滤/打分轨迹、否决原因、节点账本和恢复报告；所有资源来自同源本地文件。

## Windows 快速开始

前置条件：安装 64 位 **Python 3.11**，并在安装器中勾选 “Add Python to PATH”。确认以下命令可用：

```bat
py -3.11 --version
```

在工作区根目录执行：

```bat
setup.bat
run.bat
```

浏览器打开 <http://127.0.0.1:8000/>。页面中的“运行原版 / 修改版对比”会分别提交 `samples/original.json` 与 `samples/modified.json`。

运行固定样本端到端演示：

```bat
demo.bat
```

也可以手动创建环境和运行：

```bat
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pytest
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## Linux / WSL 快速开始

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pytest
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

演示脚本：

```bash
.venv/bin/python scripts/demo.py
```

## 固定样本与对比

- `samples/original.json`：3 个节点、2 个已分配 Pod、8 个待调度 Pod。
- `samples/modified.json`：只把 `node-b.labels.accelerator` 从 `none` 改为 `nvidia`。
- `samples/preemption.json`：高优先级 Pod 抢占一个可抢占低优先级 Pod，同时保留 `preemptable: false` 的 Pod。

实际演示结果：

| 批次 | 放置 | 否决 | 关键结论 |
| --- | ---: | ---: | --- |
| `original` | 4 | 4 | `accelerator-job` 因 `accelerator=none` 返回 `E_UNSATISFIED_AFFINITY` |
| `modified` | 5 | 3 | 仅 `accelerator-job` 改放 `node-b`；`node-b` CPU 余量由 `1800m` 变为 `1000m` |

原版中其他关键诊断：

- `web-b`：`E_POD_ANTI_AFFINITY_VIOLATION`，`web-a` 已放在同一 hostname 拓扑。
- `maintenance-b`：`E_TAINT_NOT_TOLERATED`，NoExecute 容忍窗 `10s`，逻辑时钟已过 `20s`，剩余 `-10s`。
- `batch-a`：`E_INSUFFICIENT_RESOURCES`，`node-b` 扣减前余量为 `1800m` CPU / `4.50GiB` 内存。
- `cache-a`：`PreferNoSchedule` 污点未容忍，但不硬否决，打分扣 `5.000000`。

完整结果保存在 `artifacts/demo-results.json`。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/health` | 离线健康检查 |
| `GET` | `/api/samples` | 列出本地样本 |
| `GET` | `/api/samples/{name}` | 读取本地样本原文 |
| `POST` | `/api/batches` | 提交 `sample` 或内联 `spec` |
| `GET` | `/api/batches/{batchId}` | 查询批次状态与完整结论 |
| `GET` | `/api/batches/{batchId}/nodes/{node}` | 查询节点余量、约束、过滤和打分明细 |
| `GET` | `/api/recovery` | 查询仅基于持久化事实的恢复报告 |
| `POST` | `/api/recovery/scan` | 主动扫描未完成意图 |
| `GET` | `/api/openapi.json` | 本地 OpenAPI 描述；默认 CDN Swagger 页已关闭 |

提交样本：

```json
{
  "batchId": "demo-001",
  "sample": "original"
}
```

提交内联规格时，把 `"sample"` 替换为 `"spec": { ... }`。`sample` 与 `spec` 必须且只能提供一个。

所有 API 错误统一为：

```json
{
  "error": {
    "code": "E_INVALID_QUANTITY",
    "message": "invalid CPU quantity or unit",
    "details": {
      "path": "nodes[0].allocatable.cpu",
      "value": "2Xi"
    }
  }
}
```

主要业务码：`E_INVALID_QUANTITY`、`E_INVALID_SPEC`、`E_UNSATISFIED_AFFINITY`、`E_POD_AFFINITY_UNSATISFIED`、`E_POD_ANTI_AFFINITY_VIOLATION`、`E_TAINT_NOT_TOLERATED`、`E_INSUFFICIENT_RESOURCES`、`E_NODE_ACCOUNTING_OVERFLOW`、`E_BATCH_NOT_FOUND`、`E_BATCH_EXISTS`。

## 判定顺序与确定性约定

每个节点按固定顺序执行：

1. `NodeAccounting`
2. `NodeSelector`
3. `NodeAffinity`
4. `Taints`
5. `PodAffinity`
6. `PodAntiAffinity`
7. `NodeResourcesFit`

待调度 Pod 按“优先级降序，同优先级按 JSON 样本顺序”处理。通过过滤的节点按“`scoreUnits` 降序，节点名升序”选择。相同输入重复运行会得到逐位相同的 `result`，时间戳只存在于批次持久化元数据中，不参与调度结果。

软约束满分构成：节点 preferred affinity 35、Pod affinity 20、Pod anti-affinity 15、资源平衡度 30；每个未容忍的 `PreferNoSchedule` 扣 5。分项以 `1/1,000,000` 点为整数单位计算，仅在分项边界执行 half-up；最终输出固定 6 位小数，并记录每项舍入残差。批次结果还包含 `scoreContract.roundingAudit`，以精确分数累计所有候选评分的舍入残差，最后才转换为展示小数。

## 持久化与恢复边界

批次状态机为：

```text
intent -> completed
       -> recovering -> completed
       -> recovering -> aborted
```

正常提交在完整解析、数量归一化成功后才写 `intent`；随后计算并写最终结论。进程被强制终止时可能只留下 `intent`，下次启动扫描后使用持久化规格重算。恢复报告只引用 SQLite 中已持久化的事实，并明确声明强制终止前不会存在完整的优雅退出报告。

## 验证结果

本工作区实际使用 CPython 3.11.18 与固定版本依赖验证：

```text
$ .venv/bin/python -m pytest
48 passed, 1 warning
```

唯一警告来自 Starlette `TestClient` 对 anyio 内部别名的弃用提示，不影响本项目行为。

```text
$ .venv/bin/python scripts/demo.py
original: placed=4 rejected=4 preempted=0, allNodesConsistent=True
modified: placed=5 rejected=3 preempted=0, allNodesConsistent=True
accelerator-job: rejected -> node-b
node-b remaining: CPU 1800m -> 1000m, memory 4.50GiB -> 3.50GiB
rounding audit: web-a = 5/11 micro-point residual = 0.000000454545 point
```

测试覆盖数量解析/溢出、资源累计与拒绝快照、全部节点亲和操作符、节点选择器与亲和性联合、NoExecute 时间窗、软硬 Pod 亲和性、抢占余量重算、定点确定性、意图恢复与统一 API 错误。

## 项目结构

```text
app/
  main.py        FastAPI 应用、统一错误响应、静态工作台入口
  service.py     批次提交、样本加载、恢复编排、节点明细
  storage.py     SQLite 意图/结论/恢复事件持久化
  models.py      输入模型、归一化领域模型
  quantity.py    CPU/内存精确解析与溢出检查
  matcher.py     标签选择器、拓扑和容忍匹配
  engine.py      过滤、顺序放置、账本、约束传播、抢占
  scoring.py     整数 micro-point 打分与舍入诊断
  static/        无外部依赖的 HTML/CSS/JS 工作台
samples/         原版、标签对比版、抢占样本
scripts/demo.py  固定样本端到端演示
tests/           验收测试
artifacts/       实际演示诊断输出
```

## 语义边界

这是面向本地诊断的 Kubernetes 调度语义子集，不是完整 kubelet 或 API Server：不支持 namespace 隔离、Pod 自亲和例外、拓扑分布约束、卷绑定、端口冲突、抢占 PDB 或 scheduler framework 的真实插件扩展。Pod affinity 只检查“已放置 Pod”；缺失拓扑键时 required affinity 视为不可满足，required anti-affinity 不会凭空制造冲突。所有限制均在结果诊断中保持可解释性。

更详细的实现契约见 `docs/TECHNICAL.md`。
