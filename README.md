# 车内多音区语音活动仲裁与指令归属引擎

这是一个从零构建、完全本地运行的车内四音区（主驾、副驾、后排左、后排右）语音活动检测与指令归属领域组件。项目附带确定性 WAV/清单生成器、CLI、标准库 HTTP 工作台、增量失效传播、原子检查点、强制终止恢复和逐字节全量一致性校验。

本仓库不下载外部数据、不依赖容器或外部服务。运行时只依赖 Python 3.11+、NumPy；HTTP 接口基于标准库 `http.server`；并发发布使用 POSIX `fcntl.flock` 文件锁。

## 快速开始

```bash
# Python 3.11+
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[test]'

# 生成固定种子 demo：3 个分片、4 个音区、越界清单、重叠指令清单
python -m cabin_voice generate --output demo

# 导入清单并做全量构建；--verify 会另起干净工作区做全量参考并逐字节比对
python -m cabin_voice build \
  --workspace demo/workspace \
  --manifest demo/manifest.json \
  --mode full --verify

python -m cabin_voice status --workspace demo/workspace
python -m cabin_voice fingerprints --workspace demo/workspace
python -m cabin_voice result --workspace demo/workspace

# 一次运行完整验收链路并写 validation_report.json
python -m cabin_voice validate --output validation_run

# 单元/集成测试
pytest
```

如果只想使用当前环境，可以省略虚拟环境步骤；项目根目录可直接作为包路径运行。

## 交付内容

```text
cabin_voice/
  audio.py          # WAV 切片、汉宁窗分帧、对数能量 + 频谱平坦度、滞回状态机
  attribution.py    # 跨音区到达时延、几何先验、对数域归属分、待复核仲裁
  config.py         # 默认配置、启动期严格校验、E_INVALID_CONFIG
  engine.py         # 四阶段流水线、增量缓存、失效链路、恢复、最终产物
  http_api.py       # 标准库 ThreadingHTTPServer 工作台
  manifest.py       # 清单校验、WAV 导入、音区偏移越界检查
  samplegen.py      # 固定随机种子样本/真值/注入清单生成器
  storage.py        # 原子检查点、flock 发布、内容寻址缓存、工作区
  timeline.py       # 毫秒区间编辑、重叠标记、编辑版本轨迹
  validation.py     # 端到端可重复验收
  cli.py            # 命令行接口
tests/              # 24 个 pytest 场景
demo/               # 已生成的确定性 3 分片样本与三份清单
validation_run/     # validate 命令生成的完整证据（可重新生成）
VALIDATION.md       # 实际验证结果记录
```

## 业务流水线

每个分片严格按四个阶段推进，每个阶段完成后发布一个检查点：

1. **detection**：按清单 `offset_samples` / `sample_count` 切出音区信号；使用可配置帧长、帧移和 Hann 窗分帧；逐帧计算 `10log10(E)` 与频谱平坦度，合成为 `score = log_energy_db - flatness_weight * flatness_db`；以高阈值进入、低阈值退出的滞回状态机判定活跃帧，再执行最短语音段和最短静音段后处理。
2. **delay**：对活跃区间有重叠的音区两两计算起点差 `observed_delay_ms`；根据音区坐标、麦克风坐标和可配置声速得到 `expected_delay_ms = (distance_b - distance_a) / speed * 1000`；输出残差和归一化惩罚。
3. **attribution**：按同一词条聚合候选音区；归属分在对数域合成：`log(strength) + priority_weight*log(priority/100) - mean(delay_penalty)`；排序后得到责任音区。若第一、第二名差值小于 `review_gap_threshold`，状态为 `needs_review`、`responsible_zone=null`，并保留两名候选的完整证据，绝不静默取第一名。
4. **summary**：构造统一毫秒时间轴、重叠活动标记、语音来源音区、指令归属记录和累计会话摘要；最后一个分片的摘要即完整会话摘要。

### 默认 demo 内容

| 分片 | 场景 | 预期仲裁 |
| --- | --- | --- |
| `chunk_001` | 主驾、副驾、后排左先后/部分重叠，指令词条不同 | 3 条唯一归属 |
| `chunk_002` | 主驾与副驾同时命中 `open_window`，开始时间相同 | `needs_review`，保留主驾/副驾双方依据 |
| `chunk_003` | 副驾、后排左、后排右三音区重叠；后段主驾指令 | 4 条唯一归属，多个重叠标记 |

`demo/manifest.overlap.json` 是仅保留重叠指令分片的独立用例；`demo/manifest.invalid_offset.json` 注入越界偏移。

## 清单与配置

### 清单

```json
{
  "schema_version": 1,
  "session_id": "cabin-demo-20261123",
  "sample_rate": 16000,
  "zones": [
    {"zone_id": "driver", "label": "主驾", "priority": 100, "x_m": -0.42, "y_m": 0.05}
  ],
  "microphone": {"x_m": 0.0, "y_m": 0.38},
  "lexicon": [{"term": "open_ac", "display": "打开空调"}],
  "chunks": [
    {
      "chunk_id": "chunk_001",
      "wav": "audio/chunk_001.wav",
      "segments": [
        {
          "zone_id": "driver",
          "offset_samples": 0,
          "sample_count": 38400,
          "reference_active_ms": [[210, 870]],
          "lexicon_hits": [{"term": "open_ac", "strength": 0.97, "at_ms": 245}]
        }
      ]
    }
  ]
}
```

- WAV 必须为 16 kHz、单声道、16 位 PCM。
- `reference_active_ms` 和词表命中是参考真值/观测输入；检测器不读取活动真值。
- 所有命中词条必须存在于顶层 `lexicon`，否则返回 `E_LEXICON_REFERENCE_MISSING`。
- 任一 `offset_samples + sample_count` 超过 WAV 帧数，返回 `E_ZONE_OFFSET_OUT_OF_BOUNDS`。

### 配置

默认配置由 `cabin_voice/config.py` 提供，也可以通过工作区 `.cabin_voice/config.json` 覆盖。核心参数：

```json
{
  "detection": {
    "frame_ms": 25.0,
    "hop_ms": 10.0,
    "window": "hann",
    "energy_floor_db": -100.0,
    "flatness_weight": 0.55,
    "enter_threshold_db": -43.0,
    "exit_threshold_db": -51.0,
    "min_speech_ms": 70,
    "min_silence_ms": 110
  },
  "arrival": {"speed_of_sound_mps": 343.0, "delay_sigma_ms": 18.0},
  "attribution": {
    "review_gap_threshold": 0.20,
    "priority_weight": 0.02,
    "lexicon_floor": 1e-9
  }
}
```

启动构建前会校验全部音区。非法配置返回 `E_INVALID_CONFIG`，`details` 至少包含 `zone_id` 与 `parameter`，例如：

```json
{
  "code": "E_INVALID_CONFIG",
  "details": {"zone_id": "driver", "parameter": "detection.hop_ms"},
  "message": "frame hop must be greater than zero"
}
```

## 编辑模型

`EditModel` 对以下操作维护单调 `edit_version` 和不可变轨迹：

- `set_priority(zone_id, priority)`
- `set_hits(chunk_id, zone_id, hits)`：编辑命中词表条目
- `split_activity(chunk_id, zone_id, at_ms)`
- `merge_activity(chunk_id, zone_id, left_start_ms, right_start_ms)`：只合并相邻、接触、同类区间

每次编辑记录 `before_summary`、`after_summary`、操作目标和编辑版本。相同操作序列会产生逐字节一致的 `edits.json` 与版本轨迹。区间边界在显式 split 后不会因为常规归一化被自动抹掉，只有显式 merge 才重新合并。

## 检查点、停止与恢复

每个阶段检查点写入 `.cabin_voice/checkpoints/<seq>.json`：

```json
{
  "seq": 6,
  "position": 6,
  "build_id": "...",
  "chunk_id": "chunk_002",
  "stage": "delay",
  "stage_index": 2,
  "content_fingerprint": "...",
  "config_version": "...",
  "edit_version": 1,
  "input_fingerprint": "...",
  "fingerprint_components": {"detection_fingerprint": "...", "detection_output": "..."},
  "artifact_summary": "...",
  "artifact": {"pair_delays": []},
  "checksum": "..."
}
```

写入使用同目录临时文件、`fsync`、`os.replace` 和目录 `fsync`，因此进程在任意位置中断都不会留下可被识别的半写 JSON 检查点。恢复时：

- 校验 JSON 可读性、必填字段和记录校验和；
- 丢弃损坏、重复序号、跨分片/跨阶段错位以及连续前缀之后的记录；
- 只从连续一致前缀继续，检查点序号保持 `1..N` 连续、无跳跃、无重复；
- 强制终止后不生成退出报告，恢复只依赖已持久化记录。

`stop` 请求在最近一个不可拆分阶段检查点完成后生效；未完成时不写 `output/final.json`，`recover` 从持久化前缀继续。

## 版本指纹与增量失效

每个阶段指纹由自身内容组件和上游阶段指纹共同构成，确保失效沿链路传递：

- detection：样本 SHA-256、音区偏移布局、帧/滞回配置、当前分片区间编辑
- delay：detection 阶段指纹、detection 产物摘要、几何先验、声速/时延参数
- attribution：delay 阶段指纹、delay 产物摘要、音区优先级、命中词表、归属配置
- summary：attribution 阶段指纹、attribution 产物摘要、前序会话摘要

因此：

- 样本内容改变 → 最早失效阶段为 detection；
- 帧长/帧移/滞回阈值改变 → detection；
- 音区优先级、命中词表、归属间隔阈值改变 → attribution；
- 上游即使产出物碰巧相同，只要阶段指纹变化，所有下游仍会重建；
- 未受影响的上游阶段从内容寻址缓存复用。

`fingerprints` 查询与 `invalidation` 报告会展示每个阶段的 `input_fingerprint`、`components`、`artifact_summary`、来源（`checkpoint` / `cache` / `computed`）、最早失效阶段和原因码。

## CLI

```bash
python -m cabin_voice generate --output demo --seed 20261123
python -m cabin_voice validate --output validation_run

python -m cabin_voice import --workspace ws demo/manifest.json
python -m cabin_voice build --workspace ws --mode full --verify
python -m cabin_voice build --workspace ws --mode incremental --verify
python -m cabin_voice stop --workspace ws
python -m cabin_voice recover --workspace ws --verify

python -m cabin_voice status --workspace ws
python -m cabin_voice fingerprints --workspace ws
python -m cabin_voice result --workspace ws
python -m cabin_voice verify-full --workspace ws

python -m cabin_voice replace-sample --workspace ws --chunk-id chunk_001 --wav new.wav
python -m cabin_voice set-config --workspace ws --path attribution.review_gap_threshold --value 0.1 --build --verify

python -m cabin_voice set-priority --workspace ws --zone-id passenger --priority 95
python -m cabin_voice set-hits --workspace ws --chunk-id chunk_003 --zone-id rear_right \
  --hits '[{"term":"play_music","strength":0.95,"at_ms":735}]'
python -m cabin_voice split-activity --workspace ws --chunk-id chunk_001 --zone-id driver --at-ms 500
python -m cabin_voice merge-activity --workspace ws --chunk-id chunk_001 --zone-id driver \
  --left-start-ms 200 --right-start-ms 500

python -m cabin_voice serve --workspace ws --host 127.0.0.1 --port 8765
```

## 本地 HTTP 工作台

启动：

```bash
python -m cabin_voice serve \
  --workspace demo/http_workspace \
  --manifest demo/manifest.json \
  --port 8765
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/import` | 导入清单 `{"manifest_path":"..."}` |
| POST | `/build` | `{"mode":"full|incremental","verify":true}` |
| POST | `/stop` | 请求优雅停止 |
| POST | `/recover` | `{"verify":true}` 从持久化前缀恢复 |
| GET | `/status` | 运行状态、检查点、损坏/重复记录 |
| GET | `/fingerprints` | 各阶段版本指纹与最近失效报告 |
| GET | `/checkpoints` | 连续前缀记录与无效记录 |
| GET | `/result` | 完整最终产物 |
| POST | `/samples/replace` | `{"chunk_id":"chunk_001","wav_base64":"...","verify":true}` |
| POST | `/config` | `{"path":"detection.hop_ms","value":10,"verify":true}` 或 `{"patch":{...}}` |
| POST | `/edits/priority` | `{"zone_id":"passenger","priority":95}` |
| POST | `/edits/lexicon-hits` | 编辑命中词表条目 |
| POST | `/edits/split-activity` | 切分区间 |
| POST | `/edits/merge-activity` | 合并相邻同类区间 |
| POST | `/verify/full` | 当前最终产物与干净全量构建逐字节比对 |

示例：

```bash
curl -s -X POST http://127.0.0.1:8765/build \
  -H 'Content-Type: application/json' \
  -d '{"mode":"full","verify":true}'

curl -s -X POST http://127.0.0.1:8765/config \
  -H 'Content-Type: application/json' \
  -d '{"path":"attribution.review_gap_threshold","value":0.05,"verify":true}'

curl -s http://127.0.0.1:8765/fingerprints
```

`/samples/replace` 和 `/config` 默认执行增量重建并调用全量参考比对；不一致返回 `E_INCREMENTAL_MISMATCH`，`details.first_difference` 包含首个差异字节位置和两侧十六进制上下文。

## 稳定错误码

| 错误码 | 含义 |
| --- | --- |
| `E_INVALID_CONFIG` | 配置非法；带 `zone_id` 和 `parameter` |
| `E_ZONE_OFFSET_OUT_OF_BOUNDS` | 音区样本偏移/长度超过 WAV 帧数 |
| `E_LEXICON_REFERENCE_MISSING` | 命中词条不在参考词表 |
| `E_NONFINITE_SCORE` | 输入或评分出现 NaN/Inf，或强度非正 |
| `E_STALE_CHECKPOINT` | 落后、重复、跨分片/跨阶段错位检查点发布 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与干净全量构建不一致 |
| `E_RUN_STATE_CHANGED` | 恢复时输入状态与持久化运行不一致 |
| `E_BUILD_IN_PROGRESS` | 同工作区已有构建进程 |
| `E_INVALID_EDIT` | 非法区间切分/合并或编辑目标 |
| `E_INVALID_WAV` | WAV 格式或采样率不符合要求 |

HTTP 错误返回 HTTP 400/404/409/422/500 与统一 JSON 结构 `{ok, code, message, details}`。

## 并发发布保护

`CheckpointLog.publish` 在 `fcntl.flock` 排他锁内重新扫描连续前缀，计算下一序号与计划中的 `(chunk_id, stage, stage_index)`，只有完全匹配才原子写入。两个进程同时发布同一序号时只有一个成功，另一个收到 `E_STALE_CHECKPOINT`；跨分片错位发布同样被拒绝。测试中使用真实多进程竞争验证该行为。

## 实际验证结果

验证环境：Python 3.11.18、NumPy 2.4.6、pytest 9.2.0。同一套测试也已在 Python 3.14.4 / NumPy 2.2.1 上通过；代码只使用 Python 3.11+ 语法和 API。

```text
$ pytest
24 passed

$ python -m cabin_voice validate --output validation_run
all_checks_passed: true
```

关键实测数据：

- 10 个有参考活动的音区区间全部检出，区间数量差异 0；最大起点偏差 10 ms，最大终点偏差 5 ms。
- 8 条指令唯一归属；`chunk_002:open_window` 正确标记 `needs_review`，保留主驾/副驾两个候选证据。
- 3 个分片共 6 个重叠活动标记。
- 在第 5 个检查点停止后恢复，序号 `1..12` 连续，最终 JSON 与一次完整构建逐字节一致。
- 音区优先级变化只重建 attribution/summary，detection/delay 从缓存复用；SHA-256 与全量参考一致。
- 单分片样本替换从 detection 开始失效，下游全部重建；SHA-256 与全量参考一致。
- 损坏 JSON 与重复序号记录被忽略并隔离，恢复后仍与全量构建逐字节一致。

完整报告见 `validation_report.json` 与 `VALIDATION.md`。

## 设计取舍

- 参考活动区间只用于验收与真值展示，VAD 不读取真值，避免“用答案检测答案”。
- 归属分使用对数域词表强度、优先级对数修正和时延残差惩罚；不同词条不会互相抢占责任音区。
- 同词条多音区重合并列第一、第二且分差不足时，不会产生静默选择，必须由业务复核。
- 检查点以阶段为不可拆分单元；优雅停止在当前阶段原子提交后生效。
- 增量校验通过复制源输入到临时干净工作区完成全量参考构建，不依赖缓存，便于发现任何增量路径错误。
