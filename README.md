# 本地确定性 TopK / 核采样分块算子库

本项目从零实现了一个只依赖 Python 标准库与 NumPy 的本地并行算子库，覆盖：

- 按行块、词表列块扫描的确定性 TopK 内核；
- 对数域稳定归一化、温度缩放、可复现采样；
- 核采样（top-p）确定性截断与截断诊断；
- 全局元素预算、单工作进程元素配额、峰值内存上限三重约束下的分片接纳；
- 本地进程池执行、按分片序号与行号确定性归约；
- 四阶段原子检查点、优雅停止与恢复；
- 版本指纹驱动的增量失效、文件锁和跨进程发布保护；
- 参数注入验证、固定摘要演示和自动化验收脚本。

不访问网络，不使用 GPU、Docker 或在线服务。

## 快速开始

要求 Python 3.10+、NumPy 1.24+。

```bash
python3 scripts/generate_demo_data.py --out data

python3 -m topk_pipeline run \
  --manifest data/manifest.json \
  --workspace artifacts/demo/full \
  --workers 2 \
  --summary artifacts/demo/full/summary.json

python3 -m topk_pipeline run \
  --manifest data/manifest.json \
  --workspace artifacts/demo/resume \
  --workers 1 \
  --stop-after-shards 2 \
  --reset-stop

python3 -m topk_pipeline run \
  --manifest data/manifest.json \
  --workspace artifacts/demo/resume \
  --workers 2 \
  --reset-stop \
  --summary artifacts/demo/resume/resumed-summary.json

python3 -m topk_pipeline inspect \
  --manifest data/manifest.json \
  --workspace artifacts/demo/resume
```

完整验收：

```bash
python3 -m unittest discover -s tests -v
python3 scripts/verify_demo.py
```

`scripts/verify_demo.py` 会重新生成本地样本、执行完整/串行/停止恢复运行、跑全部注入用例、做增量失效和并发检查点竞争，并写入 `artifacts/verification.json`。

## 目录

```text
topk_pipeline/
  topk.py                 行块/列块 TopK 内核、并列裁决、来源列块与比较次序
  normalize_sampling.py   对数域归一化、温度校验、top-p、确定性采样
  scheduler.py            三重预算约束接纳、并发度计算
  checkpoints.py          原子状态文件、文件锁、序号/阶段校验、增量失效
  worker.py               单分片四阶段执行与结构化异常
  runner.py               进程池、有界停止、确定性归约、摘要导出
  validation.py           形状、分值区间、概率和、采样一致性校验
  demo_data.py            固定随机种子样本与注入清单生成器
  cli.py                  generate/run/inspect 命令
scripts/
  generate_demo_data.py   样本生成入口
  verify_demo.py          端到端验收入口
  checkpoint_publish_probe.py  并发发布竞争探针
tests/                    26 项单元与端到端测试
data/                     已生成的 .npy 样本、算子清单和注入清单
artifacts/verification.json  最近一次实际验收结果
```

## 演示数据

固定生成种子：`20261123`。主要正常分片如下：

| 分片 | 形状 | K | 温度 | 核采样 | top-p | 用途 |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| `shard_00_small` | 12 × 16 | 3 | 0.8 | 是 | 0.85 | 小词表 |
| `shard_01_large` | 32 × 4096 | 12 | 1.1 | 是 | 0.92 | 大词表、非整除列块 |
| `shard_02_tie_max` | 10 × 32 | 4 | 0.7 | 否 | - | 每行 6 个并列最大分数 |
| `shard_03_nucleus_ties` | 16 × 64 | 12 | 1.0 | 是 | 0.40 | 等概率跨越 top-p 边界 |

异常/注入样本包括 NaN、`+Inf`、全 `-Inf` 行、全并列最大分数、非法 K、预算和内存越界。

算子清单逐分片声明 `k`、`temperature`、`nucleus_enabled`、`top_p`、`seed`、`expected_shape`、`expected_score_range`、`row_block`、`col_block`。全局预算位于 `budget`。

## 核心规则

### TopK

扫描顺序固定为：行块 → 词表列块 → 分值降序 → token id 升序。每个候选记录：

- `source_column_block`：首次进入候选集时所在词表列块；
- `admission_turn`：按列块依次引入候选时的确定性接纳序号；
- `rank` / `comparison_order`：最终唯一名次。

并列分数的裁决规则是“分值高者优先；同分时 token id 小者优先”。相同输入和配置产生逐字节一致的候选清单。`K <= 0` 或 `K > vocab_size` 返回带 `shard_id` 与 `k` 的 `E_INVALID_K`。

### 归一化与采样

每行 TopK 分值先减去行最大值，再按 `(score - max_score) / temperature` 做稳定 log-softmax。温度为 0、负数或非有限值返回 `E_INVALID_TEMP`。

采样随机源固定为：

```text
PCG64(SeedSequence([seed_u64, shard_ordinal, row]))
```

因此每个分片、每一行独立于并行度，串行和并行会得到完全相同的采样 token、均匀随机数和采样对数概率。

### 核采样

按名次累积概率，选择首个满足 `cumulative >= top_p` 的最短前缀。若多个等值概率跨越边界，采用“名次前缀、低 token id 在前”的固定规则，不按进程完成顺序、哈希顺序或浮点近似分组取舍。边界标记写入 `equal_probability_boundary`，所有被截断候选保留在 `truncated_candidates` 诊断中。截断后概率质量为零或非有限时返回 `E_NUMERIC`，并带行号和截断位置；不会静默返回首候选。

### 三重预算

每个分片的保守估计为：

```text
elements = rows * vocab + 3 * rows * K
memory   = rows * vocab * dtype_bytes + rows * K * 320 + 65536
```

调度器按 `priority` 降序、声明序号升序接纳分片：

- 累计估计元素超过 `global_elements`：`E_BUDGET_EXCEEDED`；
- 单分片超过单工作进程配额 `worker_elements`：`E_BUDGET_EXCEEDED`；
- 单分片保守内存超过 `peak_memory_bytes`：`E_MEMORY_BUDGET`；
- 并发度同时受 worker 数、已接纳分片数和最大安全并发内存限制约束。

分片是调度与执行的最小单元，每个分片由一个工作进程任务处理。接纳结果与请求的并行度无关，所以串行与并行摘要可逐字节比较。内存估计在执行前完成，不使用 OOM 兜底。

### 检查点与增量失效

每个分片依次发布 `filter`、`normalize`、`sampling`、`validation` 四个阶段记录。记录包含：

- `shard_id`、`stage`；
- `input_fingerprint`（阶段输入版本指纹）、`content_fingerprint`（logits 内容指纹）；
- `config_version`；
- `artifact` 与 `artifact_digest`；
- 分片内严格递增的 `sequence`。

状态文件采用“同目录临时文件 + fsync + `os.replace`”原子发布。状态文件是唯一引用来源；任意写入位置中断最多留下未引用的临时文件，不会产生半条记录。分片状态锁在 POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`。

版本链的失效最早阶段：

| 变化 | 最早失效阶段 |
| --- | --- |
| logits 内容、K、行列块配置 | `filter` |
| 温度 | `normalize` |
| seed、top-p、核采样开关 | `sampling` |
| 预算配置、期望形状/分值区间 | `validation` |

每个下游指纹包含全部上游指纹。`prepare` 在文件锁内比较有效前缀，只截断最早失效阶段及其下游并保留高水位序号；上游产物直接复用。重复或落后发布返回 `E_STALE_CHECKPOINT`；跨阶段或序号错位返回 `E_INVALID_STAGE`。

### 停止与恢复

停止请求由 `STOP_REQUESTED` 文件表示。工作进程只在完整行块结束后观察 stop 请求，已完成阶段仍保留检查点；父进程使用有界派发，不再提交未开始分片。恢复时按版本链复用已完成阶段，最终摘要与从零完整运行逐字节一致。

工作进程的 `PipelineError` 会转换为结构化错误对象；其他分片的成功结果仍保留。实现没有用线程超时来推断工作进程状态。

## 业务码

| 场景 | 业务码 | 阶段 |
| --- | --- | --- |
| K 非正或大于词表 | `E_INVALID_K` | `filter` / 调度预检 |
| 温度非正或非有限 | `E_INVALID_TEMP` | `normalize` |
| NaN、`+Inf` logits | `E_NUMERIC` | `filter` |
| 全 `-Inf` 行无有限最大值 | `E_NUMERIC` | `normalize` |
| 截断概率质量为零 | `E_NUMERIC` | `sampling` |
| 全局预算/单 worker 配额不足 | `E_BUDGET_EXCEEDED` | `schedule` |
| 峰值内存估计不足 | `E_MEMORY_BUDGET` | `schedule` |
| 重复/落后检查点发布 | `E_STALE_CHECKPOINT` | 检查点发布 |
| 跨阶段或序号错位 | `E_INVALID_STAGE` | 检查点发布 |

全并列最大分数是合法输入：本实现以 token id 升序稳定选出前 K 个，而不是报错；注入用例验证其确定性裁决。

## 实际验证结果

最近一次运行环境：Python 3.14.4、NumPy 2.2.1、Linux WSL2 x86_64。没有 GPU 或网络访问。

```text
python3 -m unittest discover -s tests -v
Ran 26 tests ... OK

python3 scripts/verify_demo.py
all checks: true
passed: true
```

固定摘要：

| 项目 | 值 |
| --- | --- |
| 输出摘要 `output_sha256` | `6233da2b6545b234e3d80470df416691ac6600c49839b4ac16324f1ba16de1af` |
| `summary.json` 文件 SHA-256 | `9ced3d2a6d8e3d6039e98d6481a19684d73a0926e2c0121877e88544fd422197` |
| 数据语料 SHA-256 | `04e92fa206b9eef9c301244e852d504bbd5b82f7243695b95d456e2af6496b4d` |
| 元素评估 | 134,564 / 300,000 |
| 单 worker 配额 | 200,000 |
| 计划峰值内存 | 1,372,160 / 8,388,608 bytes |

验证结论：

- 2 worker 与 1 worker 的 `summary.json` 逐字节一致；
- 完成 2 个分片后停止，再恢复到 4 个分片，结果与完整运行逐字节一致；
- 温度变化只重建 `normalize` 及下游，`filter` 序号保持不变；
- 预算变化只重建 `validation`，前三个阶段的序号保持不变；
- 两个进程并发发布同一阶段时恰好一个成功，另一个返回 `E_STALE_CHECKPOINT`；错误阶段返回 `E_INVALID_STAGE`；
- 模拟半写临时文件不影响已发布状态；
- 行块停止在第 8 行完成后才触发；
- 全部注入用例业务码与阶段匹配，详见 `artifacts/verification.json`。

## Windows 说明

Windows 路径使用 `spawn` 进程池和 `msvcrt.locking` 文件锁；POSIX 使用 `fork` 与 `fcntl.flock`。本次实际验收在 WSL2 Linux 环境完成，Windows 分支已实现但未在 Windows 实机执行。
