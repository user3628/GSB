# 本地对话语音情感与对话理解引擎

这是一个不依赖真实语音模型、预训练权重、GPU 或外部服务的本地引擎。它从固定随机种子生成的 WAV 分片和 JSON 对话清单出发，完成逐轮声学特征、四类情感打分、对话行为识别、显式上下文状态演进、阶段检查点、优雅停止恢复、版本失效传播、并发发布保护和业务指标汇总。

## 快速开始

环境要求：Python 3.11+、NumPy、SciPy；当前环境实测 Python 3.14.4、NumPy 2.5.5、SciPy 1.18.1。

```bash
python -m pip install -e .

# 生成 3 个 WAV 分片和 manifest.json
python -m dialogue_engine generate --out demo_data --force

# 完整运行
python -m dialogue_engine run --workspace demo_workspace --data demo_data

# 查询逐轮四个阶段的结果
python -m dialogue_engine query-turn --workspace demo_workspace --data demo_data --shard shard_01 --turn 1

# 查询版本指纹和阶段发布状态
python -m dialogue_engine fingerprints --workspace demo_workspace --data demo_data --shard shard_01 --turn 1

# 在 2 个完整轮次后优雅停止，再恢复
python -m dialogue_engine run --workspace demo_resume --data demo_data --stop-after-turns 2
python -m dialogue_engine resume --workspace demo_resume --data demo_data

# 完整演示：生成数据、完整运行、停止恢复、错误码和双进程并发发布
python -m dialogue_engine demo --workspace demo_runtime --force
```

`demo` 会写出 `demo_runtime/demo_report.json`，并在 `all_checks_passed=true` 时返回 0。

## 已验证结果

- 演示数据 3 个分片、15 个轮次、3 个说话人（S1/S2/S3）、中英混排元数据、四种情感和四种对话行为；包含情感转折和 3 次打断。
- 完整运行固定摘要：`85426e8b14762d98932065fdc1484584869d442820d98fc90b9d662e6afa6947`。
- 情感准确率 1.0，行为准确率 1.0，打断召回率 1.0；状态轨迹平均偏差约 0.1345。
- 在 2 个轮次后停止时为 8 条连续检查点记录；恢复后为 60 条，摘要字节、全部阶段产物和 60 条日志记录均与一次完整运行逐字节一致。
- 声明样本数不符返回 `E_INPUT_LENGTH`；重叠轮次返回 `E_INVALID_TURN`；缺失阶段返回 `E_INVALID_STAGE`。
- 两个独立工作进程同时发布序号 1 时，恰好一个成功，另一个返回 `E_STALE_CHECKPOINT`（`kind=duplicate`）。
- 自动化测试：`pytest -q`，27 项通过。

## 数据契约

清单示例（完整生成结果见 `demo_runtime/demo_data/manifest.json`）：

```json
{
  "schema_version": 1,
  "seed": 20261121,
  "sample_rate": 16000,
  "shards": [
    {
      "shard_id": "shard_01",
      "wav_file": "shard_01.wav",
      "sample_count": 113824,
      "turn_count": 2,
      "turns": [
        {
          "turn_index": 0,
          "speaker_id": "S1",
          "start_ms": 0,
          "end_ms": 1650,
          "text": "大家好，今天聊一下本地引擎。Let's begin.",
          "ref_emotion": "平静",
          "ref_dialogue_act": "陈述"
        },
        {
          "turn_index": 1,
          "speaker_id": "S2",
          "start_ms": 1890,
          "end_ms": 3370,
          "text": "真的吗？Can it resume after a stop?",
          "ref_emotion": "高兴",
          "ref_dialogue_act": "提问"
        }
      ]
    }
  ]
}
```

导入时校验：

- WAV 必须是 16 kHz、单声道、16 位 PCM。
- 实际样本数必须等于 `sample_count`，否则 `E_INPUT_LENGTH`。
- `turn_count` 必须等于轮次数组长度，否则 `E_INPUT_LENGTH`。
- 轮次必须按 `turn_index` 连续、位于音频范围内、非空且不重叠，否则 `E_INVALID_TURN`。
- 合法边界但短于一个分析帧，在 `features` 阶段返回 `E_EMPTY_TURN`。
- 参考情感和对话行为必须是 `平静/高兴/愤怒/悲伤` 与 `陈述/提问/确认/打断`。

## 处理阶段

1. `features`：按可配置帧长/帧移做汉宁窗分帧，逐帧计算对数能量、过零率、频谱质心和梅尔频带能量。默认向量维度为 `16 + mel_bands = 42`，包含能量统计、语音段能量趋势、过零率、频谱质心、停顿比例、浊音比例和梅尔均值。
2. `emotion`：构造固定声学描述子，经固定 SHA-256 哈希生成的正负投影矩阵映射到 64 维，与四个固定声学原型分别计算余弦相似度，再经温度 softmax 输出四类分数、标签和置信度。没有学习过程或外部权重。
3. `behavior`：使用轮次起止、轮间间隔、停顿比例、语音段能量斜率/变化、上一轮情感分数、上一轮行为、说话人切换和可配置转移权重，判定 `陈述/提问/确认/打断`。
4. `state`：从显式前序状态演进，维护当前主导情感、衰减累计情感、情感趋势、打断次数、参考打断次数、说话人变化和逐步轨迹。首轮只接受显式初始状态；第 N 轮缺前序状态直接返回 `E_INVALID_STAGE`。

每轮的四个阶段结果分别写入不可变、按输入指纹版本化的产物文件。

## 检查点与恢复

工作区结构：

```text
<workspace>/
  checkpoints/
    journal/000000000001.json ...   # 连续编号、链式摘要的检查点日志
    artifacts/features/...          # 按输入指纹版本化，避免重建覆盖旧引用
    artifacts/emotion/...
    artifacts/behavior/...
    artifacts/state/...
    head.json                       # 当前序号、链头、活跃分片、重建/最近失效信息
    checkpoint.lock                 # 跨进程文件锁
    quarantine/                     # 恢复时隔离的损坏/重复记录
    recovery/last_recovery.json
  exports/summary.json              # 确定性对话理解摘要
  diagnostics/timings.json          # 运行期耗时诊断，不进入确定性摘要
  run/run_state.json                # completed/stopped 运行状态
  run/stop.request                  # 优雅停止请求
```

检查点记录包含：分片标识、轮次、阶段、输入内容指纹、配置版本、全局连续序号、产物摘要、阶段摘要、`prev_hash` 和 `record_hash`。写入顺序是“产物原子写 → 检查点记录原子写 → 头指针原子写”，临时文件使用同目录 `.tmp`，通过 `fsync + os.replace` 提交；中断只会留下可忽略的临时文件或无害的孤立产物，不会留下半写记录。

恢复时扫描日志前缀，校验连续序号、记录摘要和链式前序摘要。损坏、不完整、序号重复及其后缀会移到 `quarantine/`，从最近一致前缀继续。头指针落后于有效日志时会从日志恢复；头指针超前或指向不同链头会报告 `rollback`/`tamper`。

## 停止与恢复一致性

- `--stop-after-turns N` 或 `request_stop()` 只在完整轮次单元结束后生效，绝不在某一阶段的半轮状态中停止。
- 恢复后未受影响的阶段产物和检查点直接复用，序号继续前进，不回退也不跳跃。
- 已验证完整运行和“停止后恢复”具有相同的摘要字节、相同阶段产物字节和相同的 60 条日志字节。

## 版本指纹与增量失效

每个阶段的输入指纹由显式上游摘要和该阶段配置构成：

- `features`：WAV 样本摘要、样本数、轮次样本边界、特征参数。
- `emotion`：特征产物摘要、分类器投影参数和固定原型版本。
- `behavior`：特征/情感摘要、当前及前一轮显式边界与说话人、前序情感/行为摘要、规则和转移权重。
- `state`：情感/行为摘要、当前清单真值（含参考标签）、前序状态摘要、状态和指标配置。

运行或 `status()` 比较当前期望指纹与已发布指纹，找到最早不匹配阶段并进入重建。最早阶段的上游继续复用，该阶段和全部下游重建；重建检查点追加到同一连续序号之后。`status()` 的 `checkpoints.last_invalidation` 可看到原因、最早阶段、分片、轮次、新旧指纹和新旧配置版本。实测：分类器变化从 `emotion` 失效；行为阈值变化从 `behavior` 失效；状态配置或参考标注变化从 `state` 失效；特征参数变化从 `features` 失效。

## 并发发布

发布检查点在 `checkpoint.lock` 文件锁内完成，先读取并校验当前日志，再要求期望序号等于 `当前最大序号 + 1`。重复、落后或跨分片错位发布返回 `E_STALE_CHECKPOINT`，并在 `details.kind` 中给出 `duplicate`、`sequence_gap`、`cross_shard`、`fingerprint_mismatch`、`rollback` 或 `tamper`。检查点分片必须先完成当前活跃分片，最后一个 `state` 记录才允许切换到下一分片。

## 业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_INPUT_LENGTH` | 清单声明样本数/轮次数不符、WAV 格式或采样率不符、无法读取输入 |
| `E_INVALID_TURN` | 轮次边界越界、空采样范围、轮次重叠、参考标签非法 |
| `E_INVALID_STAGE` | 请求不存在的前置阶段、非法阶段跳转、缺少前序轮次状态 |
| `E_STALE_CHECKPOINT` | 序号重复/落后/跳跃、跨分片错位、指纹不匹配、回退或篡改 |
| `E_NUMERIC` | 分数或特征出现 NaN/Inf，或非有限配置导致数值结果非法 |
| `E_EMPTY_TURN` | 轮次短于一个完整分析帧或没有高于语音阈值的帧 |
| `E_FEATURE_DIM` | 特征维度与当前配置不一致 |

所有业务错误都通过 `EngineError.to_dict()` 携带 `code`、`message`、`shard_id`、`turn_index`、`stage` 和 `details`，不会静默跳过或写出该阶段的检查点。

## 库接口

```python
from dialogue_engine import DialogueEngine, EngineConfig, ClassifierConfig

engine = DialogueEngine(
    "workspace",
    config=EngineConfig(classifier=ClassifierConfig(projection_seed=20261121)),
)

engine.import_dataset("demo_data")
full = engine.run()

turn = engine.query_turn("shard_01", 1)
stage = engine.query_stage("shard_01", 1, "emotion")
fingerprints = engine.version_fingerprints("shard_01", 1)
status = engine.status()
summary = engine.export_dialogue_summary("exports/custom_summary.json")

engine.request_stop("operator requested")
stopped = engine.run()
resumed = engine.resume()
```

公开导出包括 `DialogueEngine`、`CheckpointStore`、配置数据类、`load_dataset`、`generate_demo_dataset`、错误码和 `EngineError`。

## 确定性说明

- 摘要只包含业务结果、清单真值、指标和指纹，不含时间戳、耗时或临时路径。
- 阶段耗时单独写入 `diagnostics/timings.json`，避免破坏跨进程逐字节一致性。
- 数值按各阶段配置的小数位规范化，JSON 使用 UTF-8、键排序和紧凑分隔符。
- 音频生成、投影矩阵和规则评分为纯本地确定性计算；不访问网络、账号或付费资源。

## 测试

```bash
pytest -q
```

测试覆盖：种子生成与数据覆盖、特征/情感逐字节确定性、完整运行、停止恢复产物一致、增量失效阶段、所有业务错误码、缺失阶段、非法阶段发布、并发双进程发布、产物篡改、记录篡改、序号重复和恢复隔离。

实现是本地领域组件与演示引擎，不包含真实 ASR/TTS；清单中的中英文文本是对话元数据，不参与声学识别。

