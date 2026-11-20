# LoRA 参数高效微调并行调度工作台

面向小规模 Transformer 语言模型适配器训练实验的本地工作台。全部数据、模型权重与
训练过程均由固定随机种子生成并在 CPU 上运行，**不依赖任何外部预训练权重、GPU 或在线
服务**，不使用 Docker。

## 1. 环境与固定版本

要求 Python 3.11（开发验证版本：3.11.18）。

```bash
python3.11 -m venv .venv
.venv/bin/pip install torch==2.2.2 --index-url https://download.pytorch.org/whl/cpu
.venv/bin/pip install -r requirements-lock.txt
```

版本清单（`requirements-lock.txt`，`pip freeze` 实测）：

| 包 | 版本 |
| --- | --- |
| torch | 2.2.2+cpu |
| numpy | 1.26.4 |
| fastapi | 0.110.0 |
| uvicorn | 0.29.0 |
| pyyaml | 6.0.1 |
| pytest | 8.1.1 |
| httpx | 0.27.0（测试依赖） |

## 2. 目录结构

```
src/lora_bench/
  errors.py        结构化业务错误码与错误对象（含 task_id/step/field 定位字段）
  determinism.py   固定种子 RNG、SHA256 指纹、原子写、跨平台文件锁
  model.py         小型 Transformer 编码器与 LoRA 注入（A 固定种子、B 置零）
  tokenizer.py     本地 UTF-8 字节级分词器（无外部词表资产）
  data.py          JSONL 加载、样本指纹、确定性批次与样本游标
  config.py        YAML 实验配置解析、校验与任务/预算版本指纹
  training.py      仅 LoRA 参数更新的确定性训练步（SGD，梯度累积）
  checkpoint.py    原子检查点、单调序号、损坏识别与失效删除
  scheduler.py     三重预算约束下的任务接纳与原因分类
  runner.py        可被 spawn 进程池 pickle 的 worker 入口与恢复逻辑
  engine.py        接纳、并行池、确定性归约、停止恢复、配置失效重建
  state.py         持久化工作区状态与停止标志（查询全部读磁盘）
  generator.py     固定种子的样本/YAML/模型权重生成器与校验清单
  http_app.py      FastAPI 本地 HTTP 工作台
  cli.py           命令行工作台
tests/             23 个 pytest 用例
scripts/           演示、HTTP 启动脚本（.sh 与 Windows .ps1）
```

## 3. 确定性与 LoRA 训练核心

- `LoRALinear` 包装冻结的 `nn.Linear`：`lora_a` 用按模块名派生的固定种子初始化，
  `lora_b` 置零；前向输出为 `base(x) + (alpha/rank) * B(A(x))`。
- 注入后基础权重全部 `requires_grad=False`，只有 `lora_a/lora_b` 参与梯度与优化器更新。
- 训练使用 SGD、确定性批次顺序与显式样本游标；同配置、同样本、同种子的两次运行产出
  逐位相同的损失序列与适配器张量 SHA256 摘要（见
  `tests/test_determinism_and_lora.py`）。

## 4. 预算接纳调度

接纳在训练前一次性完成，约束为：并行工作进程上限 `max_workers`、单任务样本配额
`per_task_sample_quota`、全局样本上限 `global_sample_cap`。单任务预估消耗为
`batch_size * grad_accum * max_steps`（循环复用样本同样计消耗）。超预算任务不会启动，
并在 `admission.json` 与任务状态中记录原因分类：

- `per_task_quota_exceeded`
- `global_cap_exceeded`
- `no_samples_selected`

任何时刻全局累计消耗不超过上限，进程池大小取 `min(max_workers, 已接纳任务数)`。

## 5. 并行执行与确定性归约

- 进程池统一使用 `spawn` 启动方式（Windows 唯一可用，同时避免继承父进程随机状态）。
- worker 返回纯字典结果；引擎按任务在配置中的全局序号排序后再归约并生成
  `summary.json`，并行与串行（`--workers 1`）的汇总指纹逐字节一致。
- worker 内异常被捕获为结构化 `TaskError`（含任务标识与步序号），其余任务继续执行，
  不使用超时/线程术语描述失控计算。

## 6. 检查点、停止与恢复

- 每个检查点是独立目录，依次 fsync 写入 `adapter.pt`、`checkpoint.json`，最后写
  `.committed` 标记；任意位置中断都只会留下可识别的未提交目录，恢复时忽略并清理。
- 记录含任务标识、已完成步数、单调序号、样本游标、样本指纹、配置指纹、适配器摘要、
  损失历史与种子。
- 发布在文件锁内比较任务内序号，仅允许序号严格连续推进；落后/重复/错位发布返回
  `E_STALE_CHECKPOINT`。
- 停止标志为工作区文件，worker 在不可拆分的训练步之间观察，完成当前步后退出。
- 恢复时从最新一致检查点继续；若最新检查点损坏则回退到前置一致检查点，若无任何一致
  检查点则整任务从零重建，并复用样本游标保证与一次从零运行逐字节一致。

## 7. 配置变更失效与预算重调度

- 任务配置指纹覆盖 rank、alpha、目标模块、学习率、批大小、梯度累积、最大步数与样本 id；
  样本内容变化由样本指纹识别。
- 重新导入时，未变化任务与已完成检查点继续复用；变化任务从最早不匹配检查点开始失效并
  重建，失效原因、指纹与删除清单可在任务状态 `invalidation` 字段查询。
- 预算指纹变化触发重新接纳；曾因预算不足判为未启动的任务不会复用旧训练记录，其检查点
  目录会被清空。

## 8. 业务错误码

| 错误码 | 触发场景 | 定位字段 |
| --- | --- | --- |
| `E_INVALID_TASK` | 非法 rank、负学习率、恢复无前置检查点/未接纳任务 | `task_id`, `field` |
| `E_BUDGET_EXCEEDED` | 超单任务配额或全局预算 | `task_id`, 预算明细 |
| `E_STALE_CHECKPOINT` | 序号落后、重复或跨任务错位发布 | `task_id`, `step` |
| `E_WEIGHT_MISMATCH` | 适配器张量摘要与记录不一致 | `task_id`, `field` |
| `E_INVALID_PHASE` | 非法阶段跳转（如未运行时停止） | `field=run_phase` |
| `E_CORRUPT_CHECKPOINT` | 检查点元数据/权重不可读 | `task_id` |
| `E_NOT_FOUND` | 工作区/任务不存在 | `task_id` |

## 9. HTTP 与 CLI

HTTP 启动：

```bash
# Linux/macOS
scripts/serve.sh
# Windows PowerShell
scripts/serve.ps1
```

接口：`POST /import`、`POST /run`、`POST /stop`、`POST /resume`、
`GET /state`、`GET /tasks/{id}`、`GET /budget`、`GET /summary`、
`GET /export/loss`、`GET /health`。所有读接口均从工作区磁盘加载持久化状态。

CLI：

```bash
PYTHONPATH=src .venv/bin/python -m lora_bench.cli generate --data-dir data/demo
PYTHONPATH=src .venv/bin/python -m lora_bench.cli import --workspace data/ws --config data/demo/experiment.yaml
PYTHONPATH=src .venv/bin/python -m lora_bench.cli run --workspace data/ws
PYTHONPATH=src .venv/bin/python -m lora_bench.cli stop --workspace data/ws
PYTHONPATH=src .venv/bin/python -m lora_bench.cli resume --workspace data/ws
PYTHONPATH=src .venv/bin/python -m lora_bench.cli budget --workspace data/ws
PYTHONPATH=src .venv/bin/python -m lora_bench.cli summary --workspace data/ws
PYTHONPATH=src .venv/bin/python -m lora_bench.cli export-loss --workspace data/ws
```

## 10. 确定性演示与错误注入参数

一键演示（生成 3 个任务、中英混排 24 条样本、1 个必然超预算任务，覆盖停止/恢复/损坏/
全部错误码）：

```bash
scripts/demo.sh            # Linux/macOS
scripts/demo.ps1           # Windows
```

错误注入参数清单：

| 注入手段 | 预期结果 |
| --- | --- |
| 任务 `rank: 0` 或负数 | `E_INVALID_TASK`（field=rank） |
| `learning_rate: -0.1` | `E_INVALID_TASK`（field=learning_rate） |
| `per_task_sample_quota` 调到小于任务预估 | `E_BUDGET_EXCEEDED`，任务 not_started |
| 训练前写入停止标志 | 完成当前步后 `stopped`，再 resume 结果一致 |
| 删除/改写检查点 `.committed` 或 JSON | 损坏检查点被忽略，从前置一致点恢复或重建 |
| 重复发布相同序号检查点 | `E_STALE_CHECKPOINT` |
| 篡改适配器摘要 | `E_WEIGHT_MISMATCH` |

## 11. 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

实测 23 个用例全部通过，覆盖 LoRA 注入与逐位确定性、预算约束、检查点序号与损坏恢复、
并行/串行摘要一致、停止恢复、配置变更增量重建、预算重调度、HTTP 接口与全部业务错误码。
