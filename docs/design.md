# 设计说明：确定性流式 TTS 实验引擎

## 1. 数据流总览

```text
JSONL utterance ──► text frontend ──► configured chunks
                                      │
                                      ▼
                             budget scheduler
                     (calibration + proportional reject)
                                      │
                                      ▼
                          execution blocks (global phone ranges)
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
          acoustic frames (float64)             prosody markers
                   │
                   ▼
             PCM frames (int16)
                   │
                   ├── chunk WAVs
                   ├── concatenated utterance WAV
                   └── strict comparison with one-shot path
```

四类持久化产物分别对应 `frontend`、`acoustic`、`waveform`、`validation` 阶段。检查点产品被内容摘要保护，记录被单调序号保护。

## 2. 文本前端

### 2.1 规范化

1. 对完整文本执行 Unicode `NFKC`，统一全角字母、数字、空格等兼容字符。
2. 用 `\s+` 将任意连续 Unicode 空白折叠为一个 ASCII 空格。
3. 偏移量统一指向规范化文本，而非原始输入；API 错误中的 `char_position` 也是规范化位置。

### 2.2 Token 与音素

- 每个 CJK 字符是一个 token。
- 连续 ASCII 字母是一个 Latin token；连续 ASCII 数字是一个数字 token。
- 标点、空格和未知字符分别成为独立 token。
- 音素表是显式 JSON，不存在“未知 CJK 自动兜底”。查找失败立即产生 `E_INVALID_TEXT`，并记录 `utt_id`、块索引、字符位置、字符和 Unicode 名称。
- 每个音素条目包含 `phone`、`frames`、`marker`。Latin token 内部仍逐字符映射音素。

### 2.3 切块

候选边界只能落在 token 之间。算法按首个块最小字符数和后续块字符数累计可见字符；标点强制切分时优先选择满足最小字符数后的标点边界，若目标位置尚未遇到标点，则在有限前瞻窗口内寻找下一个标点。最后对 `"".join(chunk.text)` 与规范化全文做覆盖断言，禁止静默丢字符。

调度器允许在预算超限时进一步把音素范围拆小；这属于执行块切分，不改变文本前端阶段产物和全局音素序列。

## 3. 确定性声学模型

### 3.1 帧函数

每个音素 `p` 有全局索引 `g` 和固定帧数 `n_p`。音素内第 `k` 帧的输入为：

```text
embedding = SHAKE256(seed | speaker | phone | g | k) -> text_embedding_dim floats
x = embedding + prosody_vector + phone_kind_one_hot
h_0 = x
h_l = tanh(W_l h_{l-1} + b_l)
raw = W_last h_last + b_last
bands_i = sigmoid(raw_i * 1.7) * energy * spectral_tilt(i)
f0       = 70 * exp(log(520/70) * sigmoid(raw_f0)) * speaker_scale * pitch
envelope = sigmoid(raw_env * 1.4) * energy_scale
```

所有矩阵向量乘法使用显式 `i=0..n_in-1` 单线程累加，固定浮点运算顺序。

### 3.2 切分不变性证明要点

块只选择连续的全局音素区间。帧输入不含“块首/块尾”上下文，也不使用局部帧号；音素内帧号只相对于该全局音素。因此：

```text
concat(predict(block_0), ..., predict(block_m)) == predict(all_phones)
```

块级 `frame_start` 仅用于波形全局采样位置和追踪；不进入声学哈希输入。

权重由 `numpy.random.default_rng(model.seed)` 按固定层顺序生成。权重摘要按排序后的 tensor 名称、shape 和规范化 `<f8` 字节计算，不依赖 `.npz` 容器 ZIP 时间戳。若配置声明的层形状与现有权重不兼容（例如 `n_bands` 改变）或权重文件缺失，引擎会用固定种子重建权重；同形状的既有权重则保留，以便外部权重修订通过摘要触发失效。

## 4. 波形合成

每一帧生成 `hop = sample_rate * frame_shift_ms / 1000` 个采样点。对全局采样位置 `s`：

```text
signal(s) = envelope * sum_i bands_i * sin(2π * f0 * (i+1) * s / sample_rate + channel_phase)
          + noise_level * envelope * splitmix64(seed, s, channel)
```

- 相位基于全局 `s`，不会在块边界重置。
- 单声道/双声道均支持；双声道使用第二个固定相位偏移和不同噪声流。
- 超出 `[-1, 1]` 时钳位并计数。
- 先检查整个声学数组有限性，再生成 PCM；NaN/Inf 返回 `E_NUMERIC`，不返回部分 PCM。
- 用户可见 WAV 只在严格一致性检查通过后写出。

## 5. 预算调度

### 5.1 本地校准

`scripts/generate_assets.py` 在本地测量多次前向 pass 的每帧中位耗时，写入 `data/calibration.json`。运行时只复用与“模型结构 + 权重摘要”绑定的冻结校准，保证决策可复现。

### 5.2 成本估算

```text
work_units = base
           + per_visible_char * visible_chars
           + per_frame * frame_count
           + per_complexity * prosody.complexity
           + per_depth * preprocess_depth
estimated_ms = calibration.unit_ms * work_units
```

首块使用 `first_packet_ms`，其余块使用 `continuation_ms`。

### 5.3 降级策略

若估算超预算：

1. 先把预处理深度降到 0 并重估。
2. 若仍超预算，计算 `factor = ceil(estimated_ms / budget_ms)`。
3. 原计划块被拒绝，按音素边界拆成 `factor` 个块；记录 `rejected_blocks = factor - 1`。
4. 若已达最大拆分深度或单音素不可再拆，则完整处理并标记 `cannot_split_further`。

调度器从未使用线程 `join(timeout)`、future 取消或超时终止来表示计算结束。

### 5.4 线程与预处理深度

`worker_count` 用于哈希文本特征的并行预取。`preprocess_depth` 决定预取 pass 数；pass 重复相同确定性工作但不改变任何帧值。并行结果按提交顺序合并，浮点计算仍固定顺序。检查点提交与 PCM 发布串行化。

## 6. 严格一致性

对每条语句分别构建：

- 流式路径：调度块声学值、标记、PCM 顺序拼接。
- 一次性路径：`front_end_single_chunk` + 全体音素一次前向 + 一次波形合成。

比较顺序：

1. 帧数；
2. 声学 float64 数组逐字节；
3. 韵律标记序列；
4. 每声道采样数；
5. PCM 字节；
6. 钳位计数。

失败返回 `E_STREAM_MISMATCH`，包含：

- `first_deviating_block`
- `frame_index`
- `sample_index`
- 可选 `pcm_byte_index` 和参数名
- `chunked_summary` 与 `full_summary`

## 7. 检查点协议

### 7.1 存储结构

```text
runtime/<run>/checkpoints/
  .publish.lock
  ledger.json
  records/<sha256(key)>.json
  products/<utt>/<stage>/<chunk>.<suffix>
```

发布顺序：

1. 在 `flock` 内读取 ledger。
2. 校验 `seq == next_seq`、key 不重复、前置 key 存在。
3. 校验前置产品文件存在且摘要匹配。
4. 原子写产品。
5. 原子写记录。
6. 原子推进 ledger 的 `next_seq`。

任一写入使用同目录临时文件、`fsync` 和 `os.replace`，中断不会留下可被当成完整记录的半写 JSON。

### 7.2 顺序与并发

- 记录 `seq`/`monotonic_sequence` 全局单调递增。
- 重复 key、落后 seq、非连续 seq 返回 `E_STALE_CHECKPOINT`。
- 缺失前置阶段或产物返回 `E_INVALID_STAGE`。
- 新语句只能在 `utt_order` 中更早语句完成 validation 后开始，跨语句错位返回 `E_STALE_CHECKPOINT`。
- 多个发布者通过同一个文件锁串行比较并推进 ledger。

### 7.3 停止与恢复

引擎在每个 acoustic+waveform 完整执行块和检查点提交之后检查停止标记。停止时：

- 完成当前不可拆分块；
- 保留已提交阶段；
- 不写最终 `latency_trace.jsonl` 或完整运行摘要；
- 状态记录 `no_exit_report=true`。

恢复时清除停止标记，从已有检查点读取阶段产物，继续缺失块。最终输出和确定性 trace 由同一计划和同一内容生成，因此与完整运行逐字节一致。

## 8. 版本指纹与失效

| 变更内容 | 最早失效阶段 | 上游复用 |
|---|---|---|
| 文本、语言、音素表、切分参数 | frontend | 无 |
| 权重、模型结构、说话人、韵律、预算/调度/校准 | acoustic | frontend |
| 合成参数、采样率、位深、声道 | waveform | frontend、acoustic |
| 严格校验算法、预算 trace 语义 | validation | frontend、acoustic、waveform |

每个阶段记录自身指纹。运行开始扫描已有记录，一旦发现某阶段指纹不一致，就从该阶段删除本语句及其全部下游产品和记录，然后重建。全局 `config_version` 同时写入每条阶段记录。

## 9. 两种追踪文件

`latency_trace.jsonl`（确定性）：

- 块文本、字符偏移、音素范围；
- 帧和采样起止；
- 派生时间戳；
- 首包/续包预算；
- 估算耗时、是否超预算、降级原因、拒绝块数；
- 线程数和预处理深度；
- 声学/PCM 摘要和钳位计数。

`timing_audit.jsonl`（实测）：

- acoustic/waveform 阶段真实 `perf_counter_ns` 耗时；
- cache hit/miss；
- 不参与最终 trace 字节一致性。

这种分层同时满足“预算判定可复现”和“有可核对的本地实测耗时记录”。

## 10. 安全与边界

- 所有文件写入限制在工作区内；HTTP 的 `samples_file` 不能逃逸 workspace。
- 不启动 Docker、容器、GPU 服务或外部 TTS 服务。
- 音素表显式有限；未知字符是错误，不是忽略。
- 校准、参考 WAV 和演示摘要基于默认配置。修改模型或合成参数是合法实验，但参考 WAV 的 `reference_match` 会变为 false，除非重新生成资源。
