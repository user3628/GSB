# 依存句法分析并行调度与资源预算工作台 — 技术说明

## 1. 设计总览

工作台是一个纯本地、零外部服务的 Python 3.11 应用，分层如下：

```
scripts/                 样本生成、演示、启动脚本
depworkbench/
  hashing.py             FNV-1a / blake2b / sha256 确定性哈希与指纹
  tokenizer.py           空白+标点切分、CJK 逐字切分、确定性词性标注
  config.py              YAML 加载、字段校验、模型/预算/配置指纹
  parser.py              arc-standard 转移式贪心依存解码器（numpy 点积打分）
  shards.py              分片与清单加载、输入指纹
  budget.py              全局预算 + 工作线程配额 + 单句预算的接纳规划
  locking.py             fcntl 咨询式文件锁
  checkpoint.py          原子写、校验和、单调序号、并发发布保护
  engine.py              线程池并行、优雅停止、恢复、版本失效、确定性归约
  workspace.py           服务门面（导入/规划/运行/停止/恢复/统计/导出）
  cli.py                 命令行接口
  http_app.py            标准库 http.server 的本地 HTTP 接口
tests/                   42 个 pytest 用例
samples/                 固定种子生成的 3 个分片、清单、配置与错误注入配置
```

## 2. 确定性解码（业务要求 1、7）

- 分词：ASCII 字母数字按最长连续序列成词，CJK 假名/谚文/汉字逐字成词，
  标点与空白仅作分隔，不产生 token。
- 词性：规则表 + 基于 FNV-1a 的哈希回退，相同 token 永远得到相同标注。
- 转移动作：arc-standard 系统，合法动作为 `shift / left / right`。
  `ROOT <- 非根` 的 right 弧只允许在最后一步建立，从机制上保证根节点唯一，
  buffer 清空后仅允许归约，保证终止于栈 `[ROOT]`。
- 打分：每个动作 6 维特征（s1/s0/b0 的哈希符号特征、浮点特征、栈/缓冲
  规模特征），与 YAML 权重用 numpy float64 点积；同分时按
  `left < right < shift` 固定优先级打破平局。
- 输出：每 token 的中心词下标（0 = ROOT）、关系标签、依存距离，句级总分
  与动作序列。解码后校验无环、单根、距离上限；非有限分数触发
  `E_NUMERIC`，结构违规触发 `E_TREE_INVALID`，均带句号与 token 下标。
- 哈希全部为显式实现（FNV-1a/blake2b），不依赖 `PYTHONHASHSEED`，
  因此同输入同配置逐字节一致（见 `tests/test_determinism.py`）。

## 3. 预算接纳（业务要求 2）

接纳在任何解析工作之前完成（`budget.plan_admission`）：

1. 分片按序号轮询分配工作线程 `worker = shard_index % worker_threads`。
2. 单句 token 数超过 `per_sentence_max_tokens` → `E_BUDGET_SENTENCE`。
3. 工作线程累计 + 预估超过 `worker_token_quota` → `E_BUDGET_WORKER`。
4. 全局累计 + 预估超过 `global_total_tokens` → `E_BUDGET_GLOBAL`。
5. 仅全部满足才接纳并记账，因此任意时刻累计消耗不超过全局预算。

预算字段进入配置指纹；预算变化时旧检查点的配置指纹不匹配，
`invalidate_stale_checkpoints` 使旧决策失效并重新规划。

## 4. 并行与确定性归约（业务要求 3）

- `ThreadPoolExecutor` 并行解析分片；主线程按分片序号顺序消费 future，
  乱序完成的 future 进入缓冲，保证发布严格连续。
- 归约前按 `(分片序号, 句内序号)` 全局排序，统计仅依赖排序后的结果，
  并行与单线程顺序执行导出文件 sha256 相同。
- 工作线程不使用超时杀线程；停止通过协作式标志位在分片边界生效。
- 任何异常都转成结构化 `StructuredError`（未知异常包装为
  `E_PARSE_FAILED`），已完成分片的检查点保留。

## 5. 检查点、停止与恢复（业务要求 4）

- 每个检查点先写同目录临时文件，`fsync` 后 `os.replace` 原子改名并
  fsync 目录，崩溃不会留下半写记录。
- 记录体含分片序号、单调序号、输入/模型/配置指纹、结果摘要，
  sha256 校验和存于信封层。
- 恢复时读取最长连续且校验通过的前缀；损坏记录返回
  `E_CORRUPT_CHECKPOINT` 警告并从最近一致点继续。
- 停止标志位为工作区 `STOP` 文件；顺序与并行路径都在分片边界检查，
  当前不可拆分分片完成后退出，恢复后最终统计与一次完整运行一致。

## 6. 版本失效传播（业务要求 5）

三类指纹分别覆盖：

- 输入指纹：分片序号 + 文件名 + 句子列表的 sha256。
- 模型指纹：权重向量 + 最大依存距离 + 单根约束。
- 配置指纹：模型指纹 + 预算指纹 + 线程数。

恢复时找到第一个不匹配的检查点，原因记为 `INPUT_CHANGED /
MODEL_CHANGED / BUDGET_OR_CONFIG_CHANGED`，删除该分片及其下游的检查点
与结果文件后重建；未受影响分片继续复用。`status` 接口对每个分片暴露
指纹与失效原因，统计只从当前版本检查点聚合，禁止旧版本数据填充。

## 7. 并发发布与错误码（业务要求 6）

- `fcntl.flock` 互斥锁保护发布临界区，配合 `sequence.json` 记录
  最新分片序号，只允许 `last + 1` 发布；落后/重复发布返回
  `E_STALE_CHECKPOINT`（`tests/test_concurrency.py` 覆盖跨进程场景）。
- 缺失前置分片即请求聚合、非法阶段跳转返回 `E_INVALID_STAGE`。
- 越界距离上限、非法/非有限权重、配额大于全局预算等返回
  `E_INVALID_CONFIG`，错误对象含字段名与实际值。

## 8. 错误码清单

| 错误码 | 触发场景 |
| --- | --- |
| `E_INVALID_CONFIG` | 距离越界、权重非法/非有限、预算字段非法 |
| `E_BUDGET_GLOBAL` | 全局 token 预算不足 |
| `E_BUDGET_WORKER` | 单工作线程配额不足 |
| `E_BUDGET_SENTENCE` | 单句 token 超上限 |
| `E_TREE_INVALID` | 多根、有环、距离超限、空句 |
| `E_NUMERIC` | 动作分数 NaN/Inf（注入权重或 force_numeric_at） |
| `E_STALE_CHECKPOINT` | 重复/乱序/落后的检查点发布 |
| `E_INVALID_STAGE` | 缺失前置分片聚合、非法阶段跳转 |
| `E_CORRUPT_CHECKPOINT` | 检查点校验和不符或 JSON 损坏 |
| `E_NOT_FOUND` / `E_SHARD_MISMATCH` / `E_PARSE_FAILED` | 文件缺失、清单不符、未知工作异常 |

## 9. 接口

CLI（`scripts/run_cli.sh` 或 `python -m depworkbench.cli`）：
`import / plan / run / resume / stop / status / stats / export / reset`。

HTTP（`scripts/run_http.sh`，默认 `127.0.0.1:18765`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/import` | 导入分片目录、清单、配置 |
| GET | `/plan` | 接纳规划与预算消耗 |
| POST | `/run` | 并行/顺序运行 |
| POST | `/resume` | 停止或失效后恢复 |
| POST | `/stop` | 协作式优雅停止 |
| GET | `/status` | 分片状态、指纹、失效原因 |
| GET | `/stats` | 依存弧与预算聚合统计 |
| GET | `/export?output=` | 导出完整结果 JSON |
| POST | `/reset` | 清空检查点与运行状态 |

## 10. 复现实验

```bash
uv venv --python 3.11 .venv && uv pip install -r requirements.txt
.venv/bin/python scripts/generate_samples.py   # 固定种子样本
.venv/bin/python -m pytest tests/ -q           # 42 个测试
.venv/bin/python scripts/demo.py              # 端到端演示
```

演示覆盖：并行/顺序逐字节一致、分片间停止恢复、输入与模型/预算失效、
全局与线程预算超限、非法配置、数值与树诊断、并发发布与阶段保护，
固定摘要写入 `artifacts/demo_summary.json`。
