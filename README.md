# 本地确定性流式 TTS 推理引擎

这是一个从零实现、完全本地运行、可复现的流式神经语音合成实验引擎。它不加载真实 TTS 模型、不下载预训练权重、不访问外部推理服务；文本哈希嵌入、声学层权重、波形相位与噪声都由固定种子生成。

当前交付包含：

- 文本前端：Unicode NFKC、空白折叠、全半角统一、中英 token 化、标点优先切块、显式音素表和字符偏移。
- 流式声学推理：按全局音素位置生成固定帧，任何切分参数下逐帧浮点字节不变。
- 波形合成：16 位 PCM，单声道/双声道，固定相位包络、确定性噪声、钳位统计。
- 预算调度：本机校准的耗时口径、比例块拒绝降级、线程数/预处理深度、可复现 trace。
- 一致性校验：逐块拼接与一次性整体推理严格比较。
- 原子检查点：文件锁、单调序号、前置阶段校验、过期发布拒绝、阶段内容指纹失效。
- 优雅停止/恢复：恢复后的 WAV 与确定性 trace 和完整运行逐字节一致。
- CLI、FastAPI、本地停止/恢复脚本、错误注入脚本和自动化测试。

详细算法与协议见 [`docs/design.md`](docs/design.md)。

## 快速开始

```bash
# 1. 创建 Python 3.11 虚拟环境并安装固定依赖
bash scripts/bootstrap.sh

# 若需要完全按记录版本安装传递依赖：
.venv/bin/pip install -r requirements.lock.txt
.venv/bin/pip install -e .

# 2. 资源已纳入工作区；如需重新生成：
.venv/bin/python scripts/generate_assets.py

# 3. 完整确定性演示（3 条中英混排语句）
scripts/run_demo.sh main

# 4. 与仓库内固定摘要逐字段核验
.venv/bin/python scripts/verify_demo.py

# 或直接使用 CLI
.venv/bin/python -m stream_tts.cli --run-id main demo
```

主运行结束后：

```text
output/main/*.wav                     # 完整语句 WAV
output/main/chunks/<utt>/*.wav        # 逐块 WAV 分片
output/main/acoustic/<utt>.npy        # 逐帧声学参数，float64
output/main/markers/<utt>.json        # 韵律标记序列
runtime/main/latency_trace.jsonl      # 确定性逐块预算追踪
runtime/main/timing_audit.jsonl       # 本机实测耗时审计
runtime/main/run_summary.json         # 固定摘要
runtime/main/status.json              # 运行状态
runtime/main/checkpoints/             # 原子阶段检查点与产物
data/demo_reference_summary.json      # 固定摘要（aggregate/WAV/acoustic/trace）
```

## 本地 HTTP 服务

```bash
scripts/serve.sh                       # 127.0.0.1:18765（可用 PORT 覆盖）
```

常用接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 服务、校准和版本健康检查 |
| `GET` | `/config` | 当前公开配置 |
| `GET` | `/version` | 全局版本指纹和逐语句四阶段指纹 |
| `POST` | `/samples/import` | 导入 JSONL 文本样本 |
| `PUT` | `/settings/split` | 注入切分参数 |
| `PUT` | `/settings/budget` | 注入首包/续包预算与并发参数 |
| `POST` | `/runs` | 启动流式合成，可 `wait=true` 同步返回 |
| `GET` | `/runs/{run_id}` | 查询运行状态 |
| `GET` | `/runs/{run_id}/stages` | 查询各语句四阶段记录、序号、指纹与产物完整性 |
| `GET` | `/status?run_id=main` | 状态查询别名 |
| `GET` | `/runs/{run_id}/trace` | 确定性逐块延迟追踪 |
| `GET` | `/runs/{run_id}/timing` | 实测耗时审计 |
| `GET` | `/runs/{run_id}/degrade` | 只看降级块及原因 |
| `GET` | `/runs/{run_id}/summary` | 固定运行摘要 |
| `POST` | `/runs/{run_id}/stop` | 请求在当前不可拆分块完成后停止 |
| `POST` | `/runs/{run_id}/resume` | 恢复 |
| `GET` | `/runs/{run_id}/exports` | 导出文件列表 |
| `GET` | `/runs/{run_id}/exports/{utt_id}.wav` | 下载 WAV |

示例：

```bash
curl -s http://127.0.0.1:18765/health

curl -s -X POST http://127.0.0.1:18765/runs \
  -H 'content-type: application/json' \
  -d '{"run_id":"main","wait":false}'

curl -s http://127.0.0.1:18765/runs/main
curl -s http://127.0.0.1:18765/runs/main/degrade
curl -s http://127.0.0.1:18765/runs/main/trace
```

## 输入数据

`data/samples.jsonl` 每条记录：

```json
{"utt_id":"utt_mix_01","text":"你好，欢迎使用 Stream TTS 流式语音合成。今天天气很好，我们去 park 散步吧。","language":"zh","speaker":2,"prosody":{"rate":0.965,"energy":1.055,"pitch":0.968,"complexity":1.0}}
```

- `utt_id`：工作区内唯一。
- `language`：`zh` 或 `en`，允许文本本身混排。
- `speaker`：非负整数。
- `prosody`：可选；`rate`、`energy`、`pitch`、`complexity` 为有限数值。
- `complexity` 同时进入声学生成和预算成本模型；演示中的 `utt_budget_stress_03` 使用 `complexity=10` 触发续包降级。

显式音素表位于 `data/phoneme_table.json`。表中没有的字符不会被静默丢弃，而是返回 `E_INVALID_TEXT`。`data/invalid_samples.jsonl` 提供空文本和未知字符样例。

## 配置

主配置：`config/default.yaml`。关键分组：

- `model`：固定种子、嵌入维度、帧移、频带数、隐藏层结构、激活函数。
- `split`：首块最小字符数、后续块字符数、标点强制切分、预算拆分最大深度。
- `budget`：首包/续包毫秒预算、最大线程数、预处理深度、确定性成本单位。
- `synthesis`：相位种子、谐波滚降、确定性噪声幅度。
- `output`：采样率、16 位深、1/2 声道。
- `validation`：严格一致性模式。
- `paths`：样本、音素表、权重、冻结校准和参考 WAV。

非法切分参数不会在 YAML 加载时被吞掉：前置检查会以 `E_INVALID_TEXT` 返回 `utt_id`、`chunk_index=0` 和 `char_position=0`，并指出具体字段。

## CLI

```bash
.venv/bin/python -m stream_tts.cli --run-id main run
.venv/bin/python -m stream_tts.cli --run-id main run --resume
.venv/bin/python -m stream_tts.cli --run-id main run --stop-after-chunks 3
.venv/bin/python -m stream_tts.cli --run-id main status
.venv/bin/python -m stream_tts.cli --run-id main trace
.venv/bin/python -m stream_tts.cli --run-id main trace --timing
.venv/bin/python -m stream_tts.cli --run-id main version
.venv/bin/python -m stream_tts.cli --run-id main stages
.venv/bin/python -m stream_tts.cli import --file path/to/samples.jsonl
.venv/bin/python -m stream_tts.cli serve --port 18765
```

安装后也可直接使用 `stream-tts` 命令。

## 停止与恢复

```bash
# 方式 A：外部停止请求。引擎完成当前不可拆分块后退出。
scripts/stop.sh main
.venv/bin/python -m stream_tts.cli --run-id main run       # 观测到 stopped
scripts/resume.sh main                                     # 清除停止标记并恢复

# 方式 B：本地验证参数，前 3 个完整块后停止。
.venv/bin/python -m stream_tts.cli --run-id checkpoint_demo run --stop-after-chunks 3
scripts/resume.sh checkpoint_demo
```

停止运行不承诺生成完整退出报告，也不写最终 trace；阶段检查点保留。恢复后最终 WAV 和 `latency_trace.jsonl` 与一次完整运行逐字节一致。实测结果见下文。

## 错误与降级注入

```bash
.venv/bin/python scripts/error_cases.py invalid-split --first-chunk-min-chars -1
.venv/bin/python scripts/error_cases.py empty
.venv/bin/python scripts/error_cases.py unknown
.venv/bin/python scripts/error_cases.py over-budget
```

也可通过 HTTP：

```bash
curl -X PUT http://127.0.0.1:18765/settings/split \
  -H 'content-type: application/json' \
  -d '{"first_chunk_min_chars":-1}'
```

主要错误码：

- `E_INVALID_TEXT`：空文本、未知字符、非法切分参数。
- `E_NUMERIC`：帧级 NaN/Inf，带 `utt_id`、`frame_index`、`parameter_name`。
- `E_STREAM_MISMATCH`：逐块与整体结果不一致，带首个偏差块、帧、采样点和两侧摘要。
- `E_STALE_CHECKPOINT`：重复、落后、跨语句错位或非连续发布。
- `E_INVALID_STAGE`：缺失前置阶段记录或产物、产物摘要损坏。

## 确定性设计摘要

- 音素拥有全局索引；每帧声学值只依赖固定种子、说话人、音素、全局索引、音素内帧号和韵律参数，不依赖块边界。
- 线性层使用显式固定顺序累加，避免不同聚合顺序导致浮点差异。
- 波形相位基于全局采样位置，不按块重置；噪声使用 SplitMix64 由全局采样点和固定种子确定。
- 每个调度块记录 `frame_start/end`、`sample_start/end`、`start_time_ms/end_time_ms`、预算、估算耗时、线程数、预处理深度和降级原因。
- `latency_trace.jsonl` 只含确定性字段；真实 wall-clock 耗时写入独立的 `timing_audit.jsonl`。因此恢复后 trace 可逐字节一致，同时保留实测审计。
- 预算估算使用 `data/calibration.json` 中冻结的本机中位数测速。该文件与配置模型结构绑定；缺失或不匹配时本地重新校准并原子写入。
- 超预算时按 `ceil(estimated_ms / budget_ms)` 计算拒绝/拆分因子；原候选块被拒绝并替换为成比例的更小块，绝不通过线程超时取消计算。
- 阶段指纹：前端含文本/语言/切分/音素表；声学含前端、调度、权重、说话人、韵律；波形含声学与合成参数；校验含波形、预算调度与严格算法版本。
- 检查点写入采用 `临时文件 + fsync + os.replace`；发布在 `flock` 内完成序号比较、前置产物摘要校验、产物写入、记录写入和 ledger 推进。

## 实际验证结果

环境：Python 3.11.18，NumPy 2.4.6，SciPy 1.17.1，FastAPI 0.165.0，Uvicorn 0.54.0，PyYAML 6.0.3，pytest 9.2.0。

```text
$ .venv/bin/pytest -q
17 passed

$ .venv/bin/python scripts/verify_demo.py
ok=true，fixed aggregate/trace/WAV/acoustic/markers 全部一致
```

正式三次 utterance 运行 `run_id=main`：

```text
utterance                 WAV SHA-256 (前 16 位)  frames  blocks  degraded  reference
utt_mix_01                fecbc58ef6fe6561        140     2       false     true
utt_mix_02                2bbb940320edc43a        107     2       false     true
utt_budget_stress_03      2407ae33ea696f7b        226     8       true      true

aggregate_sha256: cdef2ab742c22bd6868baddc57871723c499b1f94a855a07680dbc579e87c9a1
trace_sha256:     a1468862a38130b17298c696cfc9980492b83f6cadc394eedb29b6482e989bc5
trace_records:    12
config_version:   11e02bc529d1383ef26963185bba24ae157771225e96b8ff7f76e9d3e2b7b277
calibration:      fb325cd2e70b6679a6ff6951e7e643785768ecb02479f2401a7cfd46c2da8039
```

停止/恢复实测：

```text
scripts/stop.sh scriptstop               -> stop_requested=true
run scriptstop                           -> stopped
scripts/resume.sh scriptstop             -> complete
三份 WAV 与 main 对比                     -> 全部逐字节一致
latency_trace.jsonl 与 main 对比          -> 逐字节一致
```

真实端口冒烟（`127.0.0.1:18765`）：

```text
GET /health                 -> 200
GET /runs/main/stages       -> 200；三条语句四阶段均 complete
GET /runs/main/exports/...  -> 200；WAV SHA-256 与本地文件一致
GET /runs/main/trace        -> 200；返回确定性逐块记录
```

自动化测试还验证了：任意两套切分参数下声学 float64 字节和 PCM 字节一致；NaN 不产生 WAV；预算/文本/合成参数变化命中的最早失效阶段；并发重复发布只有一个成功；HTTP 停止、恢复、trace、degrade 和 WAV 导出。

## 复现边界

- 本引擎是确定性实验引擎，不是自然语音质量模型；输出为合成谐波 PCM。
- `latency_trace` 的降级决策可复现；`timing_audit` 的 wall-clock 数值会随主机负载变化，这是刻意分层。
- 冻结校准与参考 WAV 在默认配置和当前环境中生成。修改模型/合成参数后，参考 WAV 只作为对照记录，`reference_match` 可为 `false`，不会阻断合法的重建运行。
- 运行时不需要 GPU、Docker、容器、预训练权重或网络服务。
