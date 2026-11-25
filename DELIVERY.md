# 说话人分离与识别领域组件交付说明

本交付是一个可离线运行、可被上层转写服务以 Go API / HTTP / CLI 集成的确定性领域组件。它不使用真实分离模型、预训练权重、GPU、Docker 或外部音频/模型依赖；唯一第三方依赖是 pure-Go 的 `modernc.org/sqlite`，并已通过 `vendor/` 固定，测试和演示可完全离线运行。

## 1. 快速验证（Windows PowerShell）

```powershell
go version          # 需要 go1.22 或以上
$env:GOPROXY = "off"
go test ./...
go vet ./...

# 生成样本、清单、SQLite 检查点、快照和 outputs/*.json
go run ./cmd/speakerdiar run --workdir .\demo-workspace --verify-incremental
go run ./cmd/speakerdiar verify --workdir .\demo-workspace

# 可选：本地 HTTP 集成端点
go run ./cmd/speakerdiar serve --workdir .\demo-workspace --addr 127.0.0.1:8088
```

也可直接执行 `scripts\verify.ps1`。首次运行若没有 `vendor/`，可使用 `$env:GOPROXY="https://goproxy.cn,direct"; go mod download`。

## 2. 确定性样本与产物

首次运行会在工作区生成：

```text
manifest.json                 # 会话清单：分片顺序、参考分段、样本上界、参数
samples/chunk-000.wav          # 16 kHz / mono / PCM16
samples/chunk-001.wav
samples/chunk-002.wav
state.db                      # SQLite 事务检查点与序号事实
snapshots/<chunk>/...json     # 原子阶段快照和内容寻址版本
outputs/segments.json         # 统一毫秒分段清单
outputs/overlaps.json         # 重叠说话段清单
outputs/voiceprints.json      # 声纹向量、归类和相似度
outputs/final.json            # 含时间轴版本/修订记录的总产物
diagnostics/                  # 业务异常诊断上下文
```

固定种子生成 3 个分片，说话人数分别为 2、3、4；内容包含独立基频区间、共振峰包络、静音、同时重叠和跨分片说话人 `S1..S4`。

当前默认参数下的一次典型完整运行输出为：

```text
chunks=3 segments=29 overlaps=11 voiceprints=9
```

测试 `TestLocalDeterministicAcceptance` 进一步断言：

- 每片识别出的说话人集合与清单真值集合一致；
- 分片内编号连续且唯一；
- 清单中的每对参考重叠区间都与至少一个检出的重叠段有时间交集；
- 全量两次计算以及增量结果的分段、重叠、声纹字节一致。

## 3. 四段流水线

1. `frame_spectrum`：`model.go` 将 25 ms / 10 ms 显式换算为 400 / 160 个采样点；`dsp.go` 实现汉宁窗、基 2 FFT、复数谱、对数能量、RMS 和频谱平坦度。末尾不足一帧时零填充并记录 `padded_frames`；NaN/Inf 返回 `E_NUMERIC`，错误含分片和帧索引。
2. `mask_separation`：`separation.go` 按初始说话人数初始化谐波/共振峰谱轮廓，执行确定性 soft-amplitude mask、锐化、归一化、轮廓/激活更新和 Hann OLA 重建。每轮写入每帧平均掩码变化量；零掩码、声源全零或人数超上限返回 `E_SEPARATION`，含分片、声源和轮次。
3. `embedding_arbitration`：`embedding.go` 与 `embedding_pipeline.go` 计算 Mel 频带能量、局部均值/方差统计、块归一化和固定 SplitMix 哈希投影；与清单参考段质心做余弦相似度，并用确定性贪心完成声源到说话人的一一映射。低间隔帧记录邻居/全局声源仲裁依据；投影维度不匹配或无依据并列返回 `E_EMBEDDING`。
4. `summary_timeline`：`summary.go` 与 `timeline.go` 根据分离轨帧能量判定双声源活跃帧，合并同组短间隙，输出毫秒重叠段和分段；时间轴支持新增、切分、相邻同说话人合并、跨分片身份绑定/冲突检测，每次编辑递增 `Version` 并记录来源。

## 4. 增量重算

`pipeline.go` 的 `ComputeFingerprints` 为内容、分帧、分离、声纹和汇总分别计算 SHA-256 版本指纹。`service.go` 只从最早失配阶段重算，并在发布该阶段时事务删除其下游有效检查点。

已覆盖的变更：

- 参考分段标注；
- 清单说话人数；
- 掩码最大轮数；
- 余弦间隔阈值；
- Mel/投影维度、种子等声纹投影参数。

`VerifyIncrementalEqualsFull` 会从零重算并逐阶段比较摘要，再逐字节比较：

- `segments.json`
- `overlaps.json`
- `voiceprints.json`
- `final.json`

不一致时返回 `E_INCREMENTAL_MISMATCH`，详情包含首个差异分片和阶段。

## 5. SQLite、恢复与并发发布

`store.go` 使用以下核心表：

- `checkpoints`：`(chunk_id, stage)` 主键，保证同片同阶段只有一条有效记录；
- `checkpoint_revisions`：只追加的提交历史和全局连续序号；
- `chunks`：分片内单调 `high_water`；
- `global_sequence`：已提交全局序号。

发布请求携带调用方计算时看到的 `ExpectedChunkSequence`。事务内执行条件更新：

```sql
UPDATE chunks
SET high_water = high_water + 1, ...
WHERE chunk_id = ? AND high_water = ?;
```

影响行数为 0 即返回 `E_STALE_CHECKPOINT`。发布前还会获取 `locks/publish.lock` 真实文件锁；Unix 使用 `flock`，Windows 使用 `kernel32!LockFile`。

已验证：

- 优雅停止：轮询到 `chunk-000/frame_spectrum` 后调用 `RequestStop`，服务完成第 1 片并在边界退出；重启后最终 `final.json` 与连续运行字节一致。
- `kill -9`：真实二进制在首个检查点提交后被强杀，库中保留 1 条已提交事实；重启后序号从 1 连续到 12，恢复产物与连续运行 SHA-256 完全相同。
- 双进程并发：两个 CLI 同时发布同一工作区，一个成功，另一个稳定收到 `E_STALE_CHECKPOINT`。
- 外部记录变化：`ClassifyRecordChange` / `HandleObservedRecord` 区分 `normal_progression`、`whole_replacement`、`rollback`、`duplicate`；整体替换删除该阶段及下游有效检查点以重建，回退返回 `E_STALE_CHECKPOINT` 并保留原状态。

## 6. 原子快照与异常注入

`snapshot.go` 先写同目录临时文件、`Sync`、再 `Rename`，同时写最新快照和内容寻址版本。快照 envelope 携带 `version_fingerprint`、`config_version`、`artifact_digest`。读取时会规范化 JSON 并校验载荷摘要。

| 注入值 | 稳定错误码 | 触发阶段 |
| --- | --- | --- |
| `length_mismatch` | `E_INPUT_LENGTH` | WAV/清单长度校验 |
| `rate_mismatch` | `E_RATE_MISMATCH` | 采样率校验 |
| `zero_speakers` | `E_INVALID_PARAM` | 掩码分离参数 |
| `negative_mask_rounds` | `E_INVALID_PARAM` | 掩码分离参数 |
| `projection_dim_mismatch` | `E_EMBEDDING` | 声纹投影 |
| `embedding_tie` | `E_EMBEDDING` | 并列相似度仲裁 |
| `numeric_spectrum` | `E_NUMERIC` | 频谱数值检查 |
| `zero_masked_source` / `too_many_speakers` | `E_SEPARATION` | 掩码分离 |

Go 测试 `TestInjectionFailureLeavesCommittedSnapshotsReadable` 验证所有主要注入失败后，旧 `final.json` 与阶段快照仍可读取且字节不变。

CLI 示例：

```powershell
go run ./cmd/speakerdiar run --workdir .\demo-workspace --inject rate_mismatch
go run ./cmd/speakerdiar run --workdir .\demo-workspace --inject projection_dim_mismatch
```

HTTP 示例：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8088/v1/run `
  -ContentType 'application/json' `
  -Body '{"verify_incremental":true}'
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8088/v1/inject/length_mismatch
Invoke-RestMethod -Uri http://127.0.0.1:8088/v1/artifacts/segments
```

## 7. 主要集成 API

```go
svc, err := speakerdiar.NewService("./workspace", "transcription-service", speakerdiar.InjectNone)
if err != nil { return err }
defer svc.Close()

report, err := svc.Run()
if err != nil {
    if code := speakerdiar.CodeOf(err); code != "" {
        // 稳定业务码处理
    }
    return err
}

// 上层服务可直接读取 report.Final.Segments / Overlaps / Voiceprints，
// 或读取 outputs/*.json。需要优雅停止时调用 svc.RequestStop()。
_ = report
```

## 8. 实际验证记录

本次交付已实际执行：

- `gofmt -w .`
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 go build ./...`：通过。
- `GOPROXY=off go test -count=1 ./...`：全部通过，末次记录 `ok speakerdiar 74.215s`（含验收、增量、恢复、并发发布、注入、分片顺序和 HTTP 测试）。
- 真实二进制 `kill -9` 后恢复：`checkpoint_revisions` 为 12 条，`min(sequence)=1`、`max(sequence)=12`；连续运行与恢复运行的 `outputs/final.json` SHA-256 相同。
- 两个真实 CLI 进程并发运行：一个退出码 0，另一个输出 `E_STALE_CHECKPOINT`；成功者的 `verify` 通过。

完整测试名称包括：

- `TestFramingSpectrumDeterminismAndNumericCode`
- `TestSeparationDeterminismAndDiagnosticCodes`
- `TestEmbeddingProjectionTieAndStableSpeakerSet`
- `TestTimelineSplitMergeAndConflict`
- `TestLocalDeterministicAcceptance`
- `TestClusterDigestsIndependentOfChunkOrder`
- `TestIncrementalRecomputePlanAndByteEquality`
- `TestGracefulStopAndRecoveryMatchesContinuous`
- `TestCommittedStageFailureRecovery`
- `TestConcurrentConditionalPublishAndMutationClassification`
- `TestInjectionFailureLeavesCommittedSnapshotsReadable`
- `TestHTTPAPIEndpointsAndInjection`
