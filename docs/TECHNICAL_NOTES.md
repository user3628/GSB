# VoiceLine 技术验证说明

## 1. 交付范围

本实现是一个可被上层语音助手集成的本地流式语音转换验证组件，包含：

- 固定种子的 16 kHz / 单声道 / PCM16 WAV 分片与 JSON 清单生成器。
- 汉宁窗分帧、采样点换算、自相关 F0、梅尔三角滤波器组对数能量。
- 目标说话人映射、F0 偏移、梅尔域包络弯折、IFFT 与 overlap-add 波形重建。
- 全局并发组上限、单组帧配额、每轮预估时间预算的确定性调度。
- 配额回收、并行计算、按帧块序号归约，输出与串行路径共用同一纯函数。
- 流式逐块推送、PCM 逐块返回和 `E_STREAM_MISMATCH` 首差异定位。
- SQLite 事务阶段检查点、块产物连续发布、条件更新、Unix `flock` 文件锁。
- SIGTERM/SIGINT 优雅停止、SIGKILL 后仅凭已提交事务恢复。
- 单调策略版本、`E_STALE_BASE`、快照正常推进/整体替换/回退分类。
- 块级指纹链、最早阶段/最早块定位和增量缓存重建。
- F0 参考区间对齐诊断与 `E_TOLERANCE_EXCEEDED`。
- `voiceline` CLI 与本地 HTTP 策略接口。

SQLite 驱动使用纯 Go 的 `modernc.org/sqlite`；已验证 `CGO_ENABLED=0 go build ./...` 和 `CGO_ENABLED=0 go test ./tests`。

## 2. 常用命令

```bash
go version
sqlite3 --version

# 首次拉取依赖；当前环境 proxy.golang.org 超时，使用可达镜像
GOPROXY=https://goproxy.cn,direct go mod tidy

# 生成确定性样本
go run ./cmd/voiceline generate --out samples/local

# 帧级参数表
go run ./cmd/voiceline analyze \
  --manifest samples/local/manifest.json --segment 0 \
  --out artifacts/seg-000.frames.json

# F0 真值诊断
go run ./cmd/voiceline validate --manifest samples/local/manifest.json

# SQLite 检查点转换
go run ./cmd/voiceline convert \
  --manifest samples/local/manifest.json \
  --out artifacts/run/out \
  --db artifacts/run/checkpoints.db \
  --parallel --recycle-quota --frames-per-group 6

# 串并行/流式内置自检
go run ./cmd/voiceline verify --out artifacts/verify

# 本地 HTTP 策略服务
go run ./cmd/voiceline serve --addr 127.0.0.1:18091

# 全量测试
GOPROXY=off go test ./... -count=1
CGO_ENABLED=0 GOPROXY=off go test ./tests -count=1
```

## 3. 确定性设计

### 3.1 样本生成

生成器使用内置常量种子和每分片独立的 SplitMix64 状态，不依赖 Go map 遍历、当前时间、主机名或全局随机源。输出目录首次运行即可复现：

- 采样率：`16000 Hz`
- 声道/位深：单声道、PCM16
- 每片样本数：`7200`
- 帧移：`10 ms => 160` 个整数采样点
- 帧长：`20 ms => 320` 个采样点
- 默认转换块：`4` 帧，共 `12` 块

### 3.2 帧块所有权

第 `i` 帧拥有输出区间 `[i * hop, (i+1) * hop)`，最后一帧截断到清单声明的 `expected_samples_upper_bound`。重建时虽然使用长度 `2 * hop` 的 periodic-Hann 窗和 overlap-add，但 `RenderBlock` 只输出该块帧拥有的样本区间，并把前一帧的重叠贡献计算在内。

因此：

- 串行整段转换 = 按序调用所有块渲染器并拼接。
- 并行转换 = 并发调用相同渲染器，结果写入各自槽位，再按块序拼接。
- 流式转换 = 逐块调用相同渲染器并立即返回该块 PCM。
- 恢复转换 = 从 SQLite 读取已提交 PCM，缺失块仍调用相同渲染器。

所有路径的量化、钳制和浮点运算顺序在同一块内一致，所以 WAV/PCM/摘要可逐字节比较。

### 3.3 稳定摘要

所有参与哈希的结构均为固定字段顺序的 struct/slice，不序列化不确定的 map。稳定摘要包括：

- 输入 WAV 内容指纹。
- 帧参数表摘要。
- 逐帧映射后 F0 与 Mel 摘要序列。
- PCM 摘要与 WAV 摘要。
- 块指纹链：`chain_i = SHA256(chain_{i-1} | analysis | mapping | dispatch | render)`。

调度组 ID、工作线程 ID、耗时和传输明细放在调度报告中，不进入波形稳定摘要。因此配额回收路径不同，最终摘要仍逐字节相同。

## 4. 调度与预算

`internal/dispatch/dispatch.go` 在分析完成后根据以下项估算块消耗：

- 清单期望样本总数和块样本数。
- 帧数量、帧移和自相关 lag 范围。
- Mel 频带数。
- 固定成本、每采样点成本、每频带/每 lag 成本。

调度约束：

1. `MaxGroups`：同时处于运行态的转换组上限，执行器使用信号量保证。
2. `FramesPerGroup`：单组基础帧配额。
3. `RoundBudgetNanos`：本轮预估时间预算；接纳前比较块估算值与剩余预算。

预算不足时，从第一个不可接纳块开始写入 `unprocessed`，每项包含块序号、原因、估算消耗和当时预算余量；不会静默丢弃。

当某组因整块不可再放入剩余配额而提前结束时，剩余帧配额按固定规则移交给编号最小的等待/新组：

```text
earliest_waiting_group_receives_remaining_quota
```

移交明细包含步骤、源组、目标组、帧数、触发块和规则名。关闭回收时配额作废，但不影响块纯函数结果。

## 5. SQLite 检查点与并发发布

核心表：

- `stage_checkpoint(segment_id, stage PRIMARY KEY(segment_id, stage))`：同一分片同一阶段仅一条有效记录，重复完成在同一行事务内更新 revision 和最新 monotonic sequence。
- `global_sequence`：所有检查点共享单调序号。
- `block_cursor(segment_id PRIMARY KEY, next_block_index)`：下一个允许发布的连续块号。
- `block_artifact(segment_id, block_index, ... PRIMARY KEY(segment_id, block_index))`：已提交 PCM 产物和摘要。
- `block_assignment(global_seq PRIMARY KEY, segment_id, block_index)`：用于识别跨分片错位发布。

`PublishBlock` 同时执行：

1. 获取数据库旁的真实 `flock(LOCK_EX)` 文件锁。
2. 开启事务并读取 `next_block_index`。
3. 仅当 `block_index == next_block_index` 时执行条件更新：
   `WHERE segment_id=? AND next_block_index=?`。
4. 插入块产物并提交。

落后、重复、乱序、条件更新失败或全局 assignment 中的分片与发布分片不一致，均返回 `E_STALE_CHECKPOINT`，错误详情含 `reason`、期望块号和实际块号。

## 6. 停止与恢复

- 第一次 SIGINT/SIGTERM 调用 `StopController.RequestStop()`。
- 工作器在每个不可拆分块开始前检查停止标记；已经开始的块（包括测试用 `--block-delay`）执行到渲染和发布完成。
- 主归约器只按块序发布连续事实；遇到未完成缺口立即停止，不跨缺口发布。
- 强制 SIGKILL 不执行退出处理；只有已经提交的 SQLite 块和阶段记录可用于恢复。
- 重启后读取 `block_cursor.next_block_index`，复用已提交块，仅渲染缺失后缀，再按序装配 WAV 和摘要。

`TestForceKillRestartProducesContinuousWaveform` 会真实构建 CLI、启动子进程、等待至少一个块提交、发送 `SIGKILL`，再重启并比较 WAV 字节。

## 7. 策略与失效传播

HTTP 接口：

- `GET /healthz`
- `GET /v1/policy`
- `POST /v1/policy/patches`
- `POST /v1/policy/snapshots`

增量片段必须携带 `base_version`。若与当前版本不符，返回 HTTP 409 和：

```json
{"error":{"code":"E_STALE_BASE","current_version":2,...},"ok":false}
```

快照分类：

- 同 lineage、版本前进：`normal_advance`。
- lineage 改变：`full_replacement`，所有分析、映射、调度、重建和校验产物失效。
- 同 lineage、版本降低：`rollback`；默认拒绝，显式 `allow_rollback=true` 后应用并全量重建。

字段影响：

- `frame_shift_ms`：最早命中 `analysis` 和块 0，帧网格变化导致全量。
- `f0_shift`、`envelope_warp`：最早命中 `mapping` 和块 0。
- `block_length`：最早命中 `dispatch`，块边界和重建区间变化。
- `max_groups`、`frames_per_group`、`round_budget_nanos`：最早命中 `dispatch`，但块 PCM/WAV 内容摘要可复用。
- `output_gain`：命中 `reconstruction`。

增量引擎用块指纹判断内容是否可复用；确定最早内容差异块后，重建从该块开始的连续区间，前缀直接命中缓存。

## 8. 错误码

| Code | 触发点 | 关键字段 |
| --- | --- | --- |
| `E_PARAM_INVALID` | F0 偏移为零/非正，包络系数超出 `[0.5,2.0]` | `segment_id`, `frame_index` |
| `E_SAMPLE_RATE_CONFLICT` | WAV 采样率与清单不一致 | `segment_id`, expected/actual |
| `E_FRAME_SHIFT_ZERO` | 清单帧移为零或换算采样点非正 | `segment_id` |
| `E_STREAM_MISMATCH` | 流式与整段结果不一致 | `segment_id`, `first_difference_frame`, expected/actual |
| `E_STALE_BASE` | 增量基础版本不等于当前版本 | `current_version` |
| `E_STALE_CHECKPOINT` | 落后/重复/乱序/跨分片块发布 | `segment_id`, `frame_index`, details |
| `E_TOLERANCE_EXCEEDED` | F0 区间比例或最大偏差超容差 | `max_deviation`, outlier frames |
| `E_BUDGET_EXHAUSTED` | 块估算成本超过剩余时间预算 | budget remaining |

## 9. 代码入口

- CLI：`cmd/voiceline/main.go`
- 样本生成：`internal/sample/generate.go`
- WAV：`internal/wav/wav.go`
- DSP：`internal/dsp/`
- 调度：`internal/dispatch/dispatch.go`
- 转换/流式/增量：`internal/pipeline/`
- SQLite/锁：`internal/checkpoint/`
- 策略/指纹：`internal/policy/policy.go`
- HTTP：`internal/httpapi/server.go`
- F0 诊断：`internal/diagnostics/f0.go`
- 验收测试：`tests/`
