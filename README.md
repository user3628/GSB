# 本地确定性模型评测与选择引擎

本项目从零实现一个不依赖真实模型、API、GPU 或线上数据的多模型多基准评测引擎。输入为本地 JSONL 分片，固定随机种子生成器负责产出样本、伪推理输出、参考资料与异常样本。引擎按四个阶段完成判分、聚合、能力画像与排序，并通过内容摘要依赖链支持可验证的增量重建。

## 环境与快速开始

已验证环境：

- Python 3.14.4（代码要求 Python >= 3.11）
- numpy 2.5.5
- pytest 9.2.0
- WSL2 Ubuntu，Linux 本地文件系统

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

python -m eval_engine generate --workspace . --force
python -m eval_engine build --workspace . --verify --pretty
python -m eval_engine status --workspace .
python -m eval_engine query --workspace . --stage ranking --pretty

pytest -q
# 或
python -m unittest discover -v
```

生成器默认种子为 `20261121`，产出 180 条记录、36 个唯一基准样本、5 个模型、3 个基准和 3 个 JSONL 分片。生成过程不使用网络。

## 工作区结构

```text
samples/
  shard-000.jsonl              # 输入评测分片
  shard-001.jsonl
  shard-002.jsonl
  replacements.json            # 批次替换覆盖层（首次替换后出现）
configs/
  engine.json                  # 判分、剔除、分层与排序配置
  answers.json                 # 按基准/样本的答案覆盖（可增量更新）
work/
  .lock                        # 跨进程文件锁
  STOP                         # 可选优雅停止请求
  checkpoints/00000001.json    # 单调递增的原子检查点
  artifacts/stage1.json        # 四阶段规范化产物
  artifacts/stage2.json
  artifacts/stage3.json
  artifacts/stage4.json
eval_engine/                   # 引擎、生成器与 CLI
tests/                         # unittest/pytest 测试
scripts/generate_samples.py    # 生成器便捷入口
```

### 输入记录

```json
{
  "run_id": "run-model-a-batch-reasoning-zh",
  "model_id": "model-a",
  "benchmark_id": "bench-reasoning",
  "sample_id": "bench-reasoning-zh-000",
  "output": "答案 蓝色圆形 7",
  "reference_answer": "答案 蓝色圆形 7",
  "language": "zh",
  "source_batch": "batch-reasoning-zh",
  "question_type": "reasoning"
}
```

兼容字段别名：`raw_output`/`output_text`/`prediction` 作为原始输出，`batch_id` 作为来源批次，`task_type` 作为题型。同一 `benchmark_id + sample_id` 被不同模型复用时，其参考答案必须一致，或通过 `configs/answers.json` 显式覆盖。

## 四阶段语义

### 阶段 1：规范化与自动判分

- 文本依次执行 Unicode NFKC、全半角统一、空白折叠、首尾裁剪和小写化。
- 精确匹配为 0/1；token 重叠度采用多重集合 token F1。中文/CJK 按字符切分，拉丁词按词切分。
- 精确匹配与重叠度权重在配置中指定，合成分数使用百万分之一整数定点计算。
- 空输出、参考答案缺失、零重叠均返回确定性 0 分和原因码；缺失参考答案的记录标记为 `rule_applicable=false`。
- 相同输入和规则产生逐字节一致的阶段产物。

### 阶段 2：分项聚合与异常剔除

- 按 `model_id × benchmark_id` 聚合。
- 输出有效样本数、总样本数、剔除数、平均分、P10/P50/P90，以及逐条有效样本明细和剔除清单。
- 剔除原因包括：`scoring_not_applicable`、`language_mismatch`、`missing_language`、`output_length_above_limit`、`output_length_below_limit`、`unsupported_question_type`、`empty_output_excluded`。
- 每条剔除项均保留 `sample_id`、`run_id`、模型、基准、来源批次和原因列表。
- 平均值和分位数只从 `valid_records` 计算，测试会按有效样本逐项复算。

### 阶段 3：分层加权能力画像

- 每个能力维度由若干 `language:question_type` 层组成，配置中给出维度和层权重。
- 权重先转成百万分之一整数，再用最大余数法归一化；层均值、维度得分和总分均在定点域累加。
- 某层有效样本数低于 `min_samples_per_layer`，或维度有效样本低于 `min_samples_per_dimension` 时，该维度状态为 `insufficient_evidence`，得分为 `null`，不按 0 分参与总分。
- 输出每层样本数/均值/是否受支持、维度有效权重、支撑层数、样本覆盖说明和模型总分。
- 部分维度可用时，总分会基于所有证据充分的维度重新归一化权重，并标记 `evidence_status=partial`。

### 阶段 4：排序、并列裁决与选择

- 先排除无总分、有效样本不足或基准覆盖不足的模型。
- 以最高总分为锚点，总分差不超过 `score_tolerance` 的连续模型组成并列组。
- 并列组内依次按有效样本数降序、基准覆盖数降序、稳定 `model_id` 字典序升序裁决。
- 输出最终排名、并列组、裁决使用条件、选择结论、每个模型的决策原因，以及所有被挤出模型的落选原因。

## 增量依赖与失效传播

四阶段使用摘要链，而不是时间戳或文件修改时间：

```text
stage1 = H(effective_samples, effective_answers, scoring_rules)
stage2 = H(stage1, exclusion_rules)
stage3 = H(stage2, profiling_weights)
stage4 = H(stage3, ranking_rules)
```

实际检查点同时保存各组件摘要、上游依赖指纹、输入分片摘要、配置版本、产物摘要与检查点序号。

| 变化内容 | 最早失效阶段 | 可复用上游 |
|---|---:|---|
| 参考答案、判分规则、样本集合 | 1 | 无 |
| 剔除规则 | 2 | 阶段 1 |
| 分层权重/维度映射 | 3 | 阶段 1、2 |
| 排序容差、最小样本、top-k | 4 | 阶段 1、2、3 |

`status` 查询实时展示：

- `earliest_invalid_stage`、`invalid_stages`、`valid_stages`
- 失效原因，如 `answers_changed`、`exclusion_rules_changed`
- 受影响模型列表；批次替换可精确缩小到实际被改动模型
- 当前依赖指纹、分片摘要、最后检查点序号

阶段查询默认拒绝把失效的旧产物冒充最新结论，返回 `E_INVALID_STAGE`。诊断时可显式使用 `--allow-stale`，响应会带 `stale=true`。

### 单基准答案更新

```bash
cat > /tmp/answers.json <<'JSON'
{"bench-reasoning-zh-000": "新的参考答案"}
JSON
python -m eval_engine update-answers \
  --workspace . \
  --benchmark bench-reasoning \
  --answers-file /tmp/answers.json
```

答案值未变化时不会触发无意义重建；值变化会从阶段 1 起失效。

### 单批次样本替换

```bash
python -m eval_engine replace-batch \
  --workspace . \
  --batch batch-reasoning-zh \
  --records-file /tmp/replacement.jsonl
```

替换记录写入 `samples/replacements.json` 覆盖层，原始分片保持不可变；加载时以批次覆盖层解析最终有效样本集合。覆盖写入使用原子替换，并会先验证重复键、参考答案一致性与批次归属。

### 增量/全量一致性校验

`build --verify` 或 `verify` 会在临时工作区从同一输入完整重建，并将阶段 2、3、4 产物逐字节比较。若不一致返回：

```json
{
  "ok": false,
  "code": "E_INCREMENTAL_MISMATCH",
  "details": {
    "first_difference_stage": 2,
    "model_id": "model-a"
  }
}
```

## 检查点、并发与优雅恢复

- 每个阶段完成时，对阶段产物使用同目录临时文件、`fsync` 和 `os.replace` 原子发布。
- 检查点文件名即八位序号，例如 `00000013.json`；序号必须从 1 开始连续递增。
- 发布在跨进程文件锁内执行：POSIX 使用 `flock`，Windows 使用 `msvcrt.locking`。
- 发布前再次读取当前输入和配置摘要；若构建期间数据发生变化，拒绝发布。
- 序号落后、重复、跨阶段错位、无待发布阶段或检查点断裂均返回 `E_STALE_CHECKPOINT`。
- `work/STOP` 存在时，引擎在完成当前不可拆分单元（样本判分、模型-基准聚合、模型画像或模型排序单元）后停止；未完成阶段不写检查点。
- 恢复时复用有效前缀，继续下一个失效阶段；最终排序产物与从零完整运行逐字节一致。`STOP` 文件需在恢复前由操作者移除。

检查点包含以下核心字段：

```json
{
  "checkpoint_seq": 3,
  "stage": 3,
  "stage_name": "build_capability_profiles",
  "input_digest": "...",
  "shard_content_digest": "...",
  "config_version": "2026.11.1",
  "config_digest": "...",
  "dependency_fingerprint": "...",
  "components": {"upstream": "...", "profiling": "..."},
  "artifact_digest": "...",
  "artifact_path": "artifacts/stage3.json"
}
```

## 稳定错误码

所有 CLI 错误和查询接口返回同一 JSON 错误结构：

```json
{"ok": false, "code": "E_INVALID_STAGE", "message": "...", "details": {}}
```

| 错误码 | 含义 | CLI 退出码 |
|---|---|---:|
| `E_INVALID_STAGE` | 非法阶段、缺失前置产物、旧产物冒充当前结论、未知基准/样本 | 64 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与全量重建不一致 | 65 |
| `E_STALE_CHECKPOINT` | 检查点落后、重复、断号、跨阶段或并发发布失败 | 73 |
| `E_STOPPED` | 优雅停止，当前阶段未完整发布 | 75 |
| `E_INVALID_CONFIG` | 配置缺失或结构非法 | 78 |

## Python API

```python
from eval_engine import Engine

engine = Engine(".")
status = engine.status()
result = engine.build(verify=True)
ranking = engine.query_ranking()
engine.update_answers("bench-reasoning", {"bench-reasoning-zh-000": "新答案"})
engine.replace_batch("batch-reasoning-zh", replacement_records)
engine.verify_consistency()
```

## 实际验证结果

以下结果来自本次交付环境：

- `pytest -q`：16 项测试通过。
- `python -m unittest discover -q`：16 项测试通过。
- 固定种子全量构建：180 条记录、5 个模型、3 个基准、3 个分片。
- 默认排序：`model-c` 第一，`model-a` 第二；两者被选入 top-2，`model-b`、`model-d`、`model-e` 记录为 `outside_top_k`。
- 阶段检查点序号：`1, 2, 3, 4`，每个检查点均含阶段名、输入摘要、配置版本、依赖指纹和产物摘要。
- `build --verify`：`verification.status=consistent`。
- 答案变更测试：从阶段 1 起重建，序号连续推进为 `5,6,7,8`。
- 剔除规则变更：保留阶段 1，仅从阶段 2 重建。
- 分层权重变更：保留阶段 1、2，仅从阶段 3 重建。
- 排序规则变更：保留阶段 1、2、3，仅重建阶段 4。
- 单批次替换：受影响模型精确为实际改动的 `model-c`，随后增量与全量结果一致。
- 停止恢复：在阶段 2 完成后停止，恢复只发布阶段 3、4；最终阶段 4 字节与完整运行完全相同。
- 原子中断：在阶段 1 产物写入后、检查点写入前模拟异常，未出现半写检查点；恢复后序号仍为 `1..4`。
- 双进程并发发布：一个进程成功推进到序号 5，另一个稳定得到 `E_STALE_CHECKPOINT`。
- Windows 挂载文件系统烟测（WSL 视角 `/mnt/c`）：全量构建与增量重建均 `consistent`，检查点连续推进到 8。

当前执行主机为 WSL2。上述测试和 `/mnt/c` 烟测均由 Linux Python 进程执行；实现包含 Windows `msvcrt` 文件锁分支和对目录 `fsync` 的安全降级，但 Windows 原生 Python 进程的锁分支未在本机直接执行。
