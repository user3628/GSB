# GraySwitch：本地灰度路由与流量镜像中间件

面向本地微服务测试环境的灰度路由 / 流量镜像中间件。所有比例类计算运行在**显式
定点数**（9 位小数、整数分子）之上，禁止隐式二进制浮点混用，并对每一步舍入、
累计误差与理论上界输出可审计轨迹。仅监听回环地址、只读写本地 SQLite，无任何
外部网络依赖。

## 目录结构

```
grayswitch/
├── app/
│   ├── fixedpoint.py       # 定点数、显式舍入、归一化、补偿求和
│   ├── numeric_contract.py # 运算轨迹、误差上界、容差比较、采样、float64 对照
│   ├── models.py           # 快照/事件解析、摘要校验、领域模型
│   ├── engine.py           # 赤字加权分流、灰度容差、镜像采样与去重
│   ├── storage.py          # SQLite 持久化、意图日志、崩溃恢复
│   ├── service.py          # 单写者提交、幂等去重、统计重建
│   ├── errors.py           # 统一业务码
│   └── api.py              # FastAPI 本地 HTTP 接口
├── scripts/
│   ├── generate_samples.py # 固定种子确定性样本
│   ├── run_experiment.py   # 四场景可重复实验入口
│   └── _crash_worker.py    # 强制终止注入工作进程
├── tests/                  # 35 个 pytest 用例
├── samples/                # 生成的确定性样本
└── data/                   # SQLite 库与实验报告（运行期生成）
```

## Windows 本地准备（Python 3.11+）

```powershell
python --version              # 确认 >= 3.11
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r grayswitch/requirements.txt
```

> 本仓库的 `.venv` 已在工作区根目录创建；Linux/WSL 下激活命令为
> `source .venv/bin/activate`。SQLite 使用标准库 `sqlite3`，无需额外安装。

## 生成确定性样本

```powershell
cd grayswitch
python -m scripts.generate_samples
```

样本固定随机种子 `20261122`，比例字段全部使用十进制字符串，策略快照带
SHA-256 规范化摘要；另含坏摘要、坏事件、零权重策略等负面样本。

## 启动本地服务（固定回环端口 8077）

```powershell
# Windows PowerShell
$env:GRAYSWITCH_DB = "$PWD\data\grayswitch.db"
python -m uvicorn app.api:app --host 127.0.0.1 --port 8077
```

```bash
# Linux / WSL / macOS
GRAYSWITCH_DB=$(pwd)/data/grayswitch.db \
  python -m uvicorn app.api:app --host 127.0.0.1 --port 8077
```

交互文档：`http://127.0.0.1:8077/docs`。

## 一键可重复实验

```powershell
python -m scripts.run_experiment
```

脚本自动启停服务，依次完成：全量策略裁决、镜像采样与去重、越过灰度上限、
强制终止后恢复，并把完整报告写到 `data/experiment_report.json`。

## 运行测试

```powershell
python -m pytest -q
```

## HTTP API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/policies` | 提交策略快照（摘要 + 单调版本号校验） |
| POST | `/events` | 提交事件批次并裁决（幂等 / 冲突检测） |
| GET | `/stats/routing` | 按版本查询分流占比与灰度使用 |
| GET | `/contracts/errors` | 数值契约与逐步舍入轨迹 |
| GET | `/recovery` | 崩溃恢复报告 |
| GET | `/batches` | 批次列表 |
| GET | `/verdicts/{request_id}` | 单请求裁决轨迹 |
| POST | `/admin/rebuild` | 派生统计重建 + 逐位一致性自检 |
| POST | `/admin/replay-check` | 重放逐位一致性校验 |
| POST | `/experiments/float64` | 混合量级 float64 误差对照 |
| GET | `/health` | 健康检查 / 启动恢复结果 |

失败响应统一为：

```json
{"error": {"code": "E_...", "message": "...", "context": {...}}}
```

覆盖业务码：`E_SNAPSHOT_INVALID`、`E_EVENT_INVALID`、`E_WEIGHT_INVALID`、
`E_GRAY_LIMIT_EXCEEDED`、`E_CONFLICTING_REQUEST`、`E_NOT_FOUND`。

## 数值契约要点

* 定点基数 `SCALE = 10^9`，比例只以整数分子存储；从 JSON `float` 构造直接抛
  `ImplicitFloatError`，比例必须以十进制字符串 / 整数传入。
* 权重归一化采用 half_even 定点化 + 最大余数法残差再分配，归一化比例之和
  **逐位等于 1**，单版本误差上界 1 ulp，平局按版本字典序稳定打破。
* 分流使用定点**赤字加权路由**（deficit routing），长期实际占比收敛到声明比例；
  请求权重为整数，路由键累计走 Kahan 补偿求和（整数定点路径补偿项恒为 0）。
* 灰度上限比较为纯整数分子比较：容差带 `limit * rel_tol`（一次显式舍入），
  `usage <= limit + tol` 含等号判未越界，越界响应附使用值 / 上限 / 容差 / 阈值，
  全部保留 9 位小数。
* 镜像采样用 `sha256(seed|seq)` 生成 `[0,1)` 定点分位，左闭右开 `[0, rate)`；
  去重窗口两端包含，采样与分流读取同一不可变策略版本与同一累计状态。

## 崩溃恢复语义

写事务先持久化 `PENDING` 意图并提交，再在后续事务里写业务数据并把意图置为
`COMMITTED`。进程在两步之间被 `kill -9` 时，重启扫描到 `PENDING` 意图即标记
`ABORTED`（其业务数据从未提交，随事务边界不存在），灰度累计停留在最后一个
完整策略版本。恢复报告只引用库内可核验行，不承诺强制终止后生成完整退出报告。
