# qopt — 面向本地 SQLite 的查询计划与索引选择组件

从统计快照（行数、不同值数、等深直方图、空值比例）估算谓词选择率与
多表连接基数，在候选索引集合中为等值/范围/组合谓词选择索引与左深
连接顺序，并通过本地 HTTP API 返回带代价分解、误差上界与假设来源的
确定性计划。设计与取舍详见 `docs/DESIGN.md`，原始任务书见 `docs/TASK.md`。

## 环境准备

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # numpy==2.5.5（仅用于样本生成）
```

要求 Python 3.11+（开发验证使用 3.14）；标准库 `sqlite3`、`http.server`
即可运行服务与样本库，无 Docker、无外部数据库服务。

## 快速开始

```bash
# 1. 生成确定性样本库与统计快照（data/，固定种子，逐字节可复现）
.venv/bin/python scripts/make_sample.py

# 2. 启动本地 HTTP API（回环 127.0.0.1:8787）
.venv/bin/python -m qopt.server --port 8787 --snapshot data/snapshot_v1.json

# 3. 提交查询（另开终端）
curl -s -X POST http://127.0.0.1:8787/v1/plan \
  -H 'Content-Type: application/json' \
  -d '{"snapshot_version":"v1","query":{"tables":["orders"],
       "predicates":[{"type":"eq","table":"orders","column":"customer_id","value":42},
                     {"type":"range","table":"orders","column":"total","low":100,"high":500}]}}'

# 4. 端到端示例（内嵌服务，输出三种典型查询 + 版本/错误码核验）
.venv/bin/python scripts/demo.py

# 5. 测试
.venv/bin/python -m unittest discover -s tests
```

## 样本库与快照

`scripts/make_sample.py` 以固定种子 `20261120` 生成：

- `data/sample.db`：customers 2500 / orders 25000 / order_items 75000 行，
  含偏斜状态分布、zipf 产品分布、city→region 函数依赖、空值列；
- `data/snapshot_v1.json` / `snapshot_v2.json`：两版统计快照（16 桶等深
  直方图、声明的函数依赖与相关系数、候选索引集合），用于版本切换验证；
- `data/snapshot_broken.json`：故意损坏的快照，用于 `E_SNAPSHOT_INVALID` 核验。

重复生成的文件 sha256 完全一致（含 sample.db）。

## 实际验证结果（本仓库当前代码）

- `python -m unittest discover -s tests`：**64 个测试全部通过**
  （Kahan 误差上界与逐位一致、选择率边界与 [0,1] 截断、相关性修正、
  对数域连接基数与笛卡尔上限、前缀规则与排除原因、快照校验/原子替换、
  HTTP API 错误码与计划字节级确定性）。
- `python scripts/demo.py`：**ALL DEMO CHECKS PASSED**，关键输出：
  - Q1（等值+范围组合谓词）：`idx_orders_cust_total` 以前缀
    `customer_id,total` 中选（total=12.186891），`idx_orders_status`、
    `idx_orders_total`、`seq_scan` 均附代价排除原因；
  - Q2（两表连接 + city→region 函数依赖）：联合选择率取 max(0.025,
    0.125)=0.125 替代独立估计 0.003125，连接顺序 customers→orders，
    中间基数 698.184；
  - Q3（三表连接 + IN + IS NOT NULL）：customers→orders→order_items，
    中间基数 7528 → 16836.1，对数域累加；
  - Kahan 演示：对抗序列 `[1e16, 1×1000, -1e16]` 补偿求和=1000.0、
    朴素求和=0.0；真实直方图误差上界 kahan≤4.44e-12 vs naive≤3.33e-11；
  - 同一查询两次求值输出逐字节一致（5311 == 5311 bytes）；
  - 错误码核验：`E_SNAPSHOT_VERSION_MISMATCH`、`E_SNAPSHOT_INVALID`
    （加载失败后旧快照 v2 保持有效）、`E_STATS_MISSING`；
  - 笛卡尔积：显式上限策略 cap=1e15 并在计划中标注。

## 精度契约摘要

代价分项 JSON 输出固定 6 位小数（ROUND_HALF_EVEN），附累计舍入误差
说明（≤1.5e-6）；计划比较使用 rel_tol=1e-9 的容差比较，容差内平局按
稳定键字典序定胜负，保证计划选择可复现。完整契约见 `docs/DESIGN.md`。
