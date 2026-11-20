# 搜索精排与重排并行调度工作台（rerank-bench）

本地可运行的搜索精排 / 重排并行调度工作台。输入为固定随机种子生成的
JSONL 分片与一份 YAML 调度配置；不依赖真实排序模型、GPU 或外部服务。
精排使用本地哈希特征 + 线性打分（定点量化），重排使用带窗口的贪心多样
性选择。管线四阶段：特征装配 → 精排打分 → 重排选择 → 结果汇总，每阶段
产物可查询、可失效、可恢复。

## 环境与版本

- Python 3.14.4（要求 3.11+，使用标准库 unittest / json / threading /
  multiprocessing）
- numpy 2.5.5（精排打分的 int64 定点向量运算）
- PyYAML 6.0.3（调度配置解析）
- 不使用 Docker 或任何容器；HTTP 接口绑定 127.0.0.1 非特权端口（默认 8791）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # 依赖来自本地包索引 / wheel
```

## 快速开始

```bash
# 1. 生成确定性样本（固定种子，4 个分片 + schedule.yaml，可重复产出相同文件）
.venv/bin/python -m rerank_bench generate --data-dir data

# 2. 导入工作区并运行完整管线（默认并行）
.venv/bin/python -m rerank_bench import --data-dir data
.venv/bin/python -m rerank_bench run

# 3. 查看状态 / 预算 / 重排轨迹 / 指纹 / 导出
.venv/bin/python -m rerank_bench status
.venv/bin/python -m rerank_bench budget
.venv/bin/python -m rerank_bench trace shard-000
.venv/bin/python -m rerank_bench fingerprints shard-000
.venv/bin/python -m rerank_bench export --out summary.json

# 4. 停止 / 恢复（分片为不可拆分单元，恢复后与从零运行逐字节一致）
.venv/bin/python -m rerank_bench run --stop-after 2
.venv/bin/python -m rerank_bench resume

# 5. 本地 HTTP 接口（127.0.0.1:8791）
.venv/bin/python -m rerank_bench serve --port 8791

# 6. 一键演示（清空产物 → 全量 → 停止恢复 → 注入超预算配置验证错误码）
bash scripts/demo.sh
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/import` | 导入分片与配置 `{"data_dir": "data"}` |
| POST | `/run` | 触发执行，支持 `{"serial":bool,"stage":...,"stop_after":N}` |
| POST | `/stages/run` | 分阶段执行（assemble/score/rerank/aggregate） |
| POST | `/stop` | 请求优雅停止（完成当前分片后退出） |
| POST | `/resume` | 清除停止标记并恢复执行 |
| GET | `/status` | 各分片各阶段状态与版本指纹 |
| GET | `/fingerprints/{shard}` | 单分片版本指纹 |
| GET | `/budget` | 预算消耗与未处理分片原因 |
| GET | `/shards/{shard}/trace` | 重排贪心决策轨迹 |
| GET | `/shards/{shard}/artifacts/{stage}` | 查询指定阶段产物 |
| GET | `/errors` | 结构化错误对象 |
| GET | `/export` | 导出排序结果汇总 |

## 数据与配置格式

每个 JSONL 分片包含查询组记录与候选记录：

```json
{"type":"query_group","query_id":"q-s0-0","locale":"zh-CN","channel":"web"}
{"type":"candidate","query_id":"q-s0-0","doc_id":"doc-s0-g0-c000","coarse_score":0.41,"category":"news","language":"zh","features":{"ctr":0.5,"dwell":120.0,"freshness":0.9,"authority":3.0}}
```

`schedule.yaml` 描述精排特征权重、重排窗口大小、去重与多样性惩罚系数、
并行工作进程上限、全局候选评估预算、单进程评估配额与每分片执行时间预算
（见 `rerank_bench/generator.py:default_config`）。演示样本：4 个分片、
中英混排、4 种类目（news/video/docs/shopping）。

## 设计要点

- **定点量化打分**：所有特征、权重、分数均为 1e6 刻度的整数（micro
  units），精排用 numpy int64 向量点积，输出 6 位小数字符串，同一输入与
  配置逐字节一致。
- **窗口贪心重排**：在窗口内累计类目 / 语言命中，惩罚系数为零时退化为
  纯精排序；窗口大于候选数时截断并记录 `window_truncated`；同一候选不会
  重复出现；每步决策的候选与惩罚依据写入 trace。
- **预算调度**：按分片实际候选数预估消耗，在全局预算、单进程配额、并行
  上限三重约束下接纳分片；超预算分片记录为未处理并给出原因；接纳前置决
  策保证任何时刻累计评估次数不超过全局预算。
- **确定性归约**：分片结果按分片序号与组内排序位全局排序后归约，并行与
  串行汇总逐字节一致；工作进程异常以结构化错误对象记录，归约继续。
- **检查点**：每分片每阶段原子写入（临时文件 + fsync + os.replace），记
  录分片标识、阶段名、输入内容指纹、配置版本、产物摘要与单调序号；中断
  不留半写记录。
- **增量失效**：版本指纹由分片内容摘要与各阶段配置摘要链式构成；任一输
  入变化定位最早受影响阶段并使其及下游失效，上游产物继续复用；失效原因
  与指纹在 status / fingerprints 接口可观测。
- **发布保护**：多进程发布共用文件锁（Windows 用 msvcrt.locking，POSIX
  用 flock），序号仅允许连续推进，落后 / 重复 / 错位发布返回
  E_STALE_CHECKPOINT。

## 稳定业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_MISSING_FEATURE` | 空候选组或缺失特征，附 query_id 与字段名 |
| `E_STALE_CHECKPOINT` | 落后 / 重复 / 跨分片错位的检查点发布 |
| `E_INVALID_STAGE` | 缺失前置阶段产物或非法阶段跳转 |
| `E_LATENCY_BUDGET` | 分片执行时间预算耗尽，附已耗时与分片标识 |
| `E_BUDGET_EXCEEDED` | 分片接纳会使累计评估次数超过全局预算 |
| `E_QUOTA_EXCEEDED` | 分片预估消耗超过单工作进程配额 |

## 测试与验证

```bash
.venv/bin/python -m unittest discover -s tests   # 43 个用例
.venv/bin/python tools/verify_run.py --check      # 固定摘要比对
```

实际验证结果（2026-11-21，本机 Python 3.14.4 / numpy 2.5.5 / PyYAML 6.0.3）：

- 43 个 unittest 用例全部通过（打分确定性、重排窗口与退化、预算裁决、
  检查点原子性与发布保护、并行=串行、停止恢复、增量失效、错误码、HTTP
  socket 往返）。
- 固定摘要：并行 / 串行 / 停止恢复三种路径的 `export/summary.json` 均为
  `5ea2975baf96a60ba2a6072920503eceb8b07425d62eecc41812a0fa2f176e13`
  （记录于 `tests/fixtures/expected_summary.sha256`）。
- 异常注入：全局预算 250 → shard-002/003 返回 E_BUDGET_EXCEEDED；延迟预
  算 0ms → E_LATENCY_BUDGET（附 elapsed_ms 与 shard_id）；缺失特征 →
  E_MISSING_FEATURE（附 query_id 与字段名）。

## 目录结构

```
rerank_bench/       核心包（阶段、调度、执行器、检查点、HTTP、CLI）
tools/verify_run.py 结果验证工具与固定摘要比对入口
tests/              unittest 测试与黄金摘要
scripts/demo.sh     端到端演示脚本
data/               生成的确定性样本（分片 + schedule.yaml）
docs/TASK.md        原始任务说明
```

## Windows 说明

路径读写均使用 UTF-8；文件锁在 Windows 上使用 `msvcrt.locking`（见
`rerank_bench/locking.py`），POSIX 上使用 `fcntl.flock`；工作目录与临时
目录需可写；HTTP 绑定 127.0.0.1 非特权端口。
