# xfpipe — 表格特征交叉组合与降维管线

本地可运行的四阶段管线：列类型推断 → 交叉组合生成 → 降维投影 → 结果汇总。
全程不依赖外部服务、GPU 或真实数据集；同一输入与配置在任意查询顺序、任意
缓存状态下产生逐字节一致的结果。原始任务说明见 `docs/TASK.md`。

## 环境准备

```bash
python3 -m venv .venv --system-site-packages   # Python 3.11+
.venv/bin/pip install numpy fastapi uvicorn pytest httpx
```

## 快速开始

```bash
# 生成确定性样本（640 行、4 个类别列、含缺失列、二分类标签）与 JSON 配置
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb init

# 分阶段运行 / 查询 / 校验 / 停止与恢复
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb run
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb query explained_variance
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb verify
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb run --stop-after-stage cross
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb recover

# 本地 HTTP 接口（127.0.0.1，无外部服务）
PYTHONPATH=src .venv/bin/python -m xfpipe --workdir /tmp/wb serve --port 8077

# 一键确定性演示（覆盖全部错误码与恢复场景）
PYTHONPATH=src .venv/bin/python scripts/demo.py

# 测试
.venv/bin/python -m pytest
```

## 架构

```
src/xfpipe/
  inference.py   阶段1：非空取值数 + 可解析数值比例 → numeric/categorical/missing/label；
                 数值列等宽分箱，缺失单元显式标注 __MISSING__；
                 NaN/Inf → E_NUMERIC（附列名与行号），全缺失列 → E_EMPTY_FEATURE
  crossing.py    阶段2：按组合阶数上限生成类别列交叉，确定性排序 (order, columns)，
                 超出数量上限截断并记录被截断组合与理由；零类别列/零阶数 → E_INVALID_CONFIG；
                 类别列基数全为 1 → E_EMPTY_FEATURE；计数矩阵定点量化（scale=1，精确）
  reduce.py      阶段3：PCA（显式特征协方差 + eigh）、截断 SVD（样本矩阵）、
                 固定种子随机投影（记录投影矩阵指纹）；主成分符号确定性归一
                 （最大绝对值载荷为正）；目标维度越界/非有限成分 → E_DIM_INVALID
  summary.py     阶段4：确定性汇总 + pipeline_digest
  quantize.py    定点量化（int64 + 每列缩放因子）与误差界估计
  checkpoint.py  哈希链检查点日志：原子写（临时文件 + os.replace）、文件锁
                 （POSIX fcntl / Windows msvcrt）、序号连续推进、回退/篡改识别
  cache.py       LRU 查询缓存；键含样本指纹 + 配置版本 + 阶段指纹
  query.py       交互查询：延迟预算内返回，超时 → E_LATENCY_BUDGET（附查询标识与已耗时）
  pipeline.py    编排器：阶段指纹链、增量调度、优雅停止、恢复、全量一致性校验
  server.py      FastAPI 接口；cli.py 命令行接口；workbench.py 共享装配
```

## 确定性设计

- 所有指纹为规范 JSON（键排序、紧凑分隔符）的 SHA-256。
- 阶段指纹 = hash(阶段名, 样本指纹, 该阶段配置作用域摘要, 上游阶段指纹)，
  构成版本链；配置变化仅使最早受影响阶段及其下游失效。
- 主成分/投影在缩放整数域累加（int64 精确），产物以定点保存并记录每列缩放因子；
  与 float64 参考结果比较的误差界随产物记录（实测误差 ≤ 理论界）。
- 检查点记录含阶段名、样本指纹、配置版本、产物摘要、单调序号与前序摘要
  （哈希链）；任意位置中断不会留下半写记录。

## 错误码

| 代码 | 触发条件 |
| --- | --- |
| `E_INVALID_CONFIG` | 组合阶数/交叉上限/目标维度/缓存容量/延迟预算非法，零类别列 |
| `E_DIM_INVALID` | 目标维度大于特征数，或主成分非有限（附实际维度） |
| `E_LATENCY_BUDGET` | 查询超出延迟预算（附查询标识与已耗时），不阻塞等待 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与全量重建不一致（附首个差异位置） |
| `E_STALE_CHECKPOINT` | 检查点回退/篡改/重复或落后发布（区分 rollback/tamper） |
| `E_INVALID_STAGE` | 缺失前置阶段产物或未知阶段 |
| `E_NUMERIC` | 样本含 NaN/Inf（附列名与行号） |
| `E_EMPTY_FEATURE` | 全缺失列、类别列基数全为 1、空特征空间 |

## HTTP 接口

`POST /samples/import`、`GET/PUT /config`、`POST /pipeline/run`、
`POST /pipeline/recover`、`POST /pipeline/stop`、`POST /pipeline/stop/clear`、
`GET /pipeline/status`、`POST /query/{feature_summary|explained_variance|combinations}`、
`GET /cache/stats`、`GET /query/traces`、`GET /checkpoints`、`POST /verify`。
结构化错误以 HTTP 422 + `{"error": {code, message, details}}` 返回。

## 停止与恢复语义

停止请求（`stop` 命令或 HTTP）在阶段边界生效：当前不可拆分单元（单个阶段）
完成后退出，不承诺强制终止后生成退出报告；强制终止后的恢复完全依赖已持久化
的检查点记录。恢复后最终产物与一次从零完整运行逐字节一致，且序号连续推进
（测试：`tests/test_recovery.py`）。

## 并发发布保护

多工作进程发布阶段产物时经文件锁串行化并比较序号：仅允许序号连续推进；
落后或重复发布返回 `E_STALE_CHECKPOINT`；日志整体替换（序号回退、指纹变化、
摘要不匹配）在加载时区分为 rollback / tamper / normal，仅 normal 被接受。
Windows 上使用 `msvcrt.locking`，POSIX 上使用 `fcntl.flock`；原子重命名统一
使用 `os.replace`。

## 验证结果（本机实测）

- `pytest`：**45 passed**（推断/交叉/降维/缓存/增量/检查点/恢复/量化/HTTP 九个维度）
- `scripts/demo.py`：完整输出见 `docs/demo_output.txt`，要点：
  - 完整运行固定摘要 `pipeline_digest=613f1c08…8c6ec2a2`（两次独立运行逐字节一致）
  - 交叉组合 8 保留 / 2 截断（理由记录）/ 110 维特征
  - 注入越界维度 → `E_DIM_INVALID`；非法阶数 → `E_INVALID_CONFIG`；
    零延迟预算 → `E_LATENCY_BUDGET`
  - 配置变更仅重建受影响阶段（reused=['infer']），增量与全量逐字节校验 `match`
  - 阶段间停止后恢复：序号 `[1,2,3,4]` 连续，恢复产物与全量运行 `match`
  - 量化误差：max_abs_error ≤ 2.6e-06，理论界 1.1e-05（实测误差均在界内）
- 真实 uvicorn 服务冒烟：run/query(缓存命中)/stop/recover/verify 全部符合预期
