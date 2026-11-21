# 技术设计与确定性说明

## 1. 数据与导入契约

每个 WAV 是单声道、16 kHz、16 位 PCM。一个 WAV 内按主驾、副驾、后排左、后排右依次拼接四段音区麦克风信号。清单声明每段采样偏移，但处理时每条音区信号使用自己的局部毫秒时间轴，因此四个音区可以在同一毫秒上重叠活动。

清单核心字段：

```json
{
  "schema_version": 1,
  "sample_rate": 16000,
  "duration_upper_bound_ms": 2000,
  "default_query_budget_ms": 20,
  "shards": [{
    "shard_id": "shard-001",
    "wav": "wavs/shard-001.wav",
    "total_samples": 128000,
    "zones": [
      {"zone_id": "driver", "channel": 0, "start_sample": 0, "end_sample": 32000}
    ],
    "reference_activity_ms": {"driver": [[220, 620]]},
    "priorities": {"driver": 1},
    "word_hits": [
      {"zone_id": "driver", "word": "打开空调", "start_ms": 300, "end_ms": 520, "strength": 0.9}
    ]
  }]
}
```

导入时进行检查：

- WAV 头为 16 kHz / 单声道 / 16-bit PCM，且不是压缩格式。
- WAV 帧数等于 `total_samples`。
- 四音区 `start_sample` 从 0 连续推进，`end_sample - start_sample` 之和等于 `total_samples`，最后一段完整覆盖 WAV。
- 每段时长与所有参考活动/词表命中终点不超过 `duration_upper_bound_ms`。
- 四音区 ID、声道、优先级、区间和强度均在合法范围内。

失败统一为 `E_IMPORT_INVALID`，详情携带 `shard_id`、`zone_id`、样本或毫秒位置等上下文。

## 2. VAD 阶段

对每音区独立执行：

1. 按配置的 `frame_ms` 和 `hop_ms` 转成样本数；帧数按确定性整数公式计算。
2. 尾部帧越界时显式补零，记录 `padded_frame_count` 与 `padding_samples`。
3. 应用 `np.hanning(frame_samples)` 窗。
4. 帧对数能量：`10 * log10(mean(frame^2) + eps)`。
5. 频谱平坦度：`geometric_mean(power) / arithmetic_mean(power)`，使用 rFFT 非 DC 频点。
6. 特征得分：能量归一化值和 `1 - flatness` 的配置加权平均。
7. 滞回：非活跃态达到 `enter_threshold` 才进入活跃；活跃态低于 `exit_threshold` 才退出。
8. 后处理：删除短于 `min_speech_ms` 的活动段，填平短于 `min_silence_ms` 的静音间隙。
9. 输出逐帧 `energy_db`、`flatness`、`score`、`active`，并生成半开毫秒活动区间。

同一输入、配置和 NumPy 版本产生相同的帧标记；测试对两次计算结果做规范化 JSON 字节比较。

## 3. 仲裁阶段

词表命中按 `(word, start_ms)` 在可配置聚类窗口内聚合。每个聚类：

- 从各音区有效活动区间中选取与命中窗口重叠最多、起点最近的一段作为观测到达起点。
- 参与音区包含窗口内活跃音区和命中音区。
- 对候选责任音区 `h` 和两两音区 `(a,b)`：
  - 观测时延：`onset_a - onset_b`。
  - 几何预测：`(distance(h,a) - distance(h,b)) / speed_of_sound`。
  - 误差与兼容度：`exp(-abs(observed - expected) / tolerance)`。
- 每个候选音区的分量：
  - 优先级分数：参与音区中优先级数字越小得分越高。
  - 命中强度：该音区聚类内最大参考命中强度。
  - TDOA 兼容度：所有音区对兼容度均值。
- 总分是三类分量的配置加权和。按总分、优先级、固定音区顺序确定性排序。
- 若最高与第二名分差小于 `review_margin`，状态为 `review` 且 `responsible_zone` 为空；否则给出唯一责任音区。

输出保留参与音区、每个候选的全部三组分、时延差明细、排序、分差和源命中 ID。

## 4. 统一时间轴与编辑模型

有效活动区间由 VAD 基础区间按活动操作日志顺序重放得到。操作只有：

- `split`：必须严格落在某区间内部。
- `merge`：必须恰好连接两个相邻区间。

优先级、完整优先级映射、词表命中 upsert/delete、活动切分/合并、配置修改都产生一条 `edit_version` 单调递增的审计记录，包含操作类型、影响分片/音区/区间、前后值和配置版本变化。

非法区间返回 `E_INVALID_INTERVAL`，详情至少包含：

```json
{"shard_id": "...", "zone_id": "...", "start_ms": 0, "end_ms": 0}
```

VAD 帧参数变化会使已应用的活动操作失效，服务在一次配置编辑中显式清空全部相关活动操作，并在审计记录中标记 `invalidated_activity_ops`。

## 5. 阶段指纹与增量重算

阶段输入指纹只包含会影响该阶段的内容：

- `vad`：清单音区布局、WAV 内容 SHA-256、采样率、VAD 配置摘要。
- `arbitrate`：VAD 产物摘要、有效活动摘要、优先级、词表命中、仲裁配置摘要。

因此优先级或词表编辑只改变仲裁指纹，VAD 快照原样复用。帧参数或 VAD 阈值变化会改变 VAD 指纹，从而重算 VAD 与其下游仲裁。仲裁间距、声速、几何或权重只影响仲裁。

服务按 `vad -> arbitrate` 顺序识别首个不匹配阶段，仅计算该阶段及下游。阶段完成后在状态锁内重新读取工作区；如编辑版本、配置版本或清单指纹在计算期间变化，拒绝提交并返回 `E_STALE_VERSION`，避免覆盖并发编辑。

增量完成后，服务以当前清单、配置和编辑模型从零计算一份全量结果，并对增量与全量结果执行规范化 JSON 字节比较。若不同：

- 返回 `E_INCREMENTAL_MISMATCH`。
- 附 `first_difference.offset`、两侧长度、左右上下文及两侧摘要。

测试中的优先级编辑实际结果为 `reused_stages=["vad"]`、`recomputed_stages=["arbitrate"]`，且 `result_digest == verified_full_digest`。

## 6. 查询与缓存

查询类型：

- `active`：某毫秒的活跃音区、区间、重叠标记。
- `tdoa`：覆盖该毫秒的候选指令时延差明细。
- `assignment`：完整分量得分、排序与责任音区。

缓存键为以下内容的 SHA-256：

- 查询类型、分片、毫秒和预算外的参数。
- 全部分片内容指纹与清单摘要。
- 当前编辑版本。
- VAD 与仲裁配置指纹。
- VAD/仲裁阶段产物摘要。

缓存文件保存 `result` 和 `result_digest`；读取时重新校验摘要，损坏时视为未命中。响应中的 `query_id`、`elapsed_ms`、`cache_hit` 是传输元数据，`result` 及其摘要不包含这些值，因此缓存命中与未命中的业务结果逐字节一致。

查询不获取发布锁。服务在计算或缓存读取后测量实际耗时；超过请求预算或配置预算时返回 HTTP 504 与：

```json
{"code": "E_LATENCY_BUDGET", "details": {"query_id": "...", "elapsed_ms": 2.39, "budget_ms": 0}}
```

`budget_ms=0` 是确定性的超限演示入口。

## 7. 快照链

每阶段完成后写一条快照记录：

- `seq`：从 1 开始严格连续。
- `stage`：`import`、`vad` 或 `arbitrate`。
- `shard_fingerprint`：分片内容指纹聚合。
- `config_version`、`edit_version`。
- `artifact` 与 `artifact_digest`。
- `previous_chain_digest`、`record_digest` 与头部 `chain_digest`。

写入使用同目录临时文件、`fsync`、`os.replace` 和目录 `fsync`，不会出现可见的半写 JSON。后续追加前先验证既有链，只接受 `head.seq + 1`。

校验规则：

- 记录序号重复、缺失或摘要/链不一致：`E_SNAPSHOT_TAMPERED`。
- 最高记录序号低于头部，或头部落后于已提交记录：`E_SNAPSHOT_ROLLBACK`。
- 当前工作区阶段产物摘要与最后一条对应阶段快照不一致：`E_SNAPSHOT_TAMPERED`。

## 8. 并发编辑发布

所有状态读取-修改-写回、快照追加和状态提交都在 `fcntl.flock` 排他文件锁内完成。编辑事务流程：

1. `prepare` 在当前版本验证操作并写入 `pending/<edit_id>.json`。
2. `publish` 在锁内重新加载工作区和待提交记录，要求 `base_version == current_version` 且分片等于当前编辑作用域。
3. 落后、重复、已丢弃或跨分片错位返回 `E_STALE_VERSION`；该事务保留为 `stale`。
4. `retry` 可显式把基线推进到当前版本后重新校验发布；`discard` 显式丢弃。

多进程测试使用 `spawn` 启动真实子进程，验证一个进程推进版本后另一个进程的发布得到 `E_STALE_VERSION`，随后可丢弃或重试。

## 9. 停止与恢复

停止请求写入独立 `STOP` 标志文件。VAD 和仲裁在每处理一个分片前检查标志；当前不可拆分单元结束后退出。阶段只有完整计算后才会获得快照和状态提交，因此停止不会留下半阶段产物。恢复时清除标志，从首个缺失/过期阶段继续，并对最终结果再次做全量字节校验。

## 10. 确定性边界与限制

- 演示信号由固定种子 PCG64 随机数、固定谐波和固定包络生成；WAV 文件逐字节固定。
- 结果 JSON 使用排序键、紧凑分隔符和 UTF-8，浮点字段统一四舍五入到固定小数位。
- TDOA 使用帧级毫秒区间起点，符合本任务契约，但不是真实声学时延估计。
- 工作区路径、快照创建时间等运行元数据不进入固定结果摘要。
- 所有 HTTP 服务只绑定回环地址；程序运行期无外部网络调用。
