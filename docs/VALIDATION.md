# 实际验证记录

验证时间：2026-11-26（Asia/Shanghai）

## 环境

```text
go version go1.27.4 linux/amd64
SQLite CLI: 3.46.1 2024-08-13 ...
CGO_ENABLED=0
驱动: modernc.org/sqlite v1.63.0（纯 Go）
```

首次拉取依赖时 `proxy.golang.org` 连接超时，改用：

```bash
GOPROXY=https://goproxy.cn,direct go get modernc.org/sqlite@latest
```

依赖完成后，所有测试和示例均在本地运行；测试内 CLI 构建使用 `GOPROXY=off`，不访问网络服务。

## 最终命令与结果

### 1. 格式和静态检查

```bash
gofmt -w cmd internal tests
test -z "$(gofmt -l cmd internal tests)"   # 通过，无输出
GOPROXY=off go vet ./...                  # 通过，无诊断
```

### 2. 全量测试

```bash
GOPROXY=off go test ./... -count=1
```

结果：

```text
?       voiceline/cmd/voiceline       [no test files]
?       voiceline/internal/canonical  [no test files]
?       voiceline/internal/checkpoint [no test files]
?       voiceline/internal/diagnostics [no test files]
?       voiceline/internal/dispatch   [no test files]
?       voiceline/internal/dsp        [no test files]
?       voiceline/internal/httpapi    [no test files]
?       voiceline/internal/model      [no test files]
?       voiceline/internal/pipeline   [no test files]
?       voiceline/internal/policy     [no test files]
?       voiceline/internal/sample     [no test files]
?       voiceline/internal/wav        [no test files]
ok      voiceline/tests       1.484s
```

详细日志：`artifacts/validation/go-test-verbose.txt`。13 个验收测试全部 PASS：

```text
TestSQLiteConditionalPublication                         PASS (0.10s)
TestGracefulStopAndRecoveryByteIdentity                  PASS (0.43s)
TestFrameTableDeterminismAndBusinessErrors               PASS (0.03s)
TestSerialParallelStreamByteIdentity                     PASS (0.04s)
TestBudgetAdjudicationAndQuotaTransfer                   PASS (0.02s)
TestF0ToleranceErrorCode                                 PASS (0.02s)
TestClampCountRecorded                                   PASS (0.03s)
TestCrossProcessPublicationAllowsOneContiguousWriter      PASS (0.11s)
TestForceKillRestartProducesContinuousWaveform           PASS (1.16s)
TestPolicyStoreStaleBaseAndSnapshotClassification        PASS (0.00s)
TestHTTPStaleBase                                        PASS (0.00s)
TestIncrementalRebuildReuse                              PASS (0.03s)
TestBlockFingerprintChainFindsEarliestLocalBlock         PASS (0.00s)
```

### 3. 无 CGO 构建/测试

```bash
CGO_ENABLED=0 GOPROXY=off go build ./...
CGO_ENABLED=0 GOPROXY=off go test ./... -count=1
```

结果：

```text
ok      voiceline/tests       1.484s
```

### 4. 固定样本生成与 F0 真值

```bash
go run ./cmd/voiceline generate --out samples/local
go run ./cmd/voiceline validate --manifest samples/local/manifest.json
```

实际结果：4 个分片均为 45 帧，其中 44 帧落在清单参考 F0 区间，比例 `0.977777...`；末帧为加零填充边界帧，存在小幅越界但最大偏差小于清单容差，所有分片 `passed: true`。

### 5. 串并行、配额回收和流式自检

```bash
go run ./cmd/voiceline verify --out artifacts/validation/verify
```

实际报告：

```json
{
  "clamp_count": 0,
  "observed_parallel": 2,
  "ok": true,
  "parallel_matches": ["no_recycle", "recycle"],
  "pcm_digest": "9afd2bfe5832c41c01d021181c64f8ac684a1a702233158a9a43e02815cd307b",
  "recycle_transfers": 4,
  "segment_id": "seg-000",
  "stream_matches": true,
  "wav_bytes": 14444,
  "wav_digest": "78ab1775da7c73bc10d8e719a986da4094626ad78cf32f62471ef9000c63ab57"
}
```

该报告确认：

- 未回收配额的并行结果与串行 PCM/WAV/摘要逐字节一致。
- 发生 4 次配额移交的回收路径仍与串行逐字节一致。
- 实际观察到并发度 `2`，符合 `MaxGroups=2`。
- 流式逐块拼接结果通过 `VerifyConsistency`。

### 6. SQLite 检查点实跑

示例命令：

```bash
go run ./cmd/voiceline convert \
  --manifest samples/local/manifest.json \
  --out artifacts/run-001/out \
  --db artifacts/run-001/voiceline.db \
  --parallel=true --recycle-quota=true --frames-per-group 6
```

SQLite 查询到每分片四条阶段记录：

```text
seg-000|analysis
seg-000|dispatch
seg-000|reconstruction
seg-000|validation
...
```

每分片 12 个连续块产物：

```text
seg-000|12|0|11
seg-001|12|0|11
seg-002|12|0|11
seg-003|12|0|11
```

### 7. 强制终止恢复

`TestForceKillRestartProducesContinuousWaveform` 执行以下真实流程：

1. 构建 `artifacts/voiceline` 等价 CLI 二进制。
2. 连续运行一次转换，保存 `seg-000.wav`。
3. 以 `--block-delay 80ms` 启动第二个进程。
4. 轮询 SQLite `block_cursor.next_block_index >= 1`。
5. 发送 `SIGKILL`。
6. 用同一数据库重启进程。
7. 比较恢复后的 WAV 与连续运行 WAV。

实测结果（最终完整日志中的 SIGKILL 用例为 0.89s）：

```text
--- PASS: TestForceKillRestartProducesContinuousWaveform (1.16s)
```

字节比较使用 `bytes.Equal(recoveredWAV, continuousWAV)`。

### 8. HTTP 陈旧基版本

实测首次基于 `version=1` 的 patch 成功并前进到 `version=2`；重复提交 `base_version=1` 返回：

```http
HTTP/1.1 409 Conflict
X-Error-Code: E_STALE_BASE
```

```json
{
  "error": {
    "code": "E_STALE_BASE",
    "message": "patch base_version does not match current policy version",
    "current_version": 2,
    "details": {"base_version": 1}
  },
  "ok": false
}
```

## 验收点到测试的映射

| 验收点 | 自动化验证 |
| --- | --- |
| 参数表确定性、采样率冲突、帧移零 | `TestFrameTableDeterminismAndBusinessErrors` |
| `E_PARAM_INVALID` 分片/帧索引 | `TestFrameTableDeterminismAndBusinessErrors` |
| 串并行、回收/未回收逐字节一致 | `TestSerialParallelStreamByteIdentity` |
| 流式一致性和 `E_STREAM_MISMATCH` | `TestSerialParallelStreamByteIdentity` |
| 预算裁决和余量 | `TestBudgetAdjudicationAndQuotaTransfer` |
| 配额移交明细 | `TestBudgetAdjudicationAndQuotaTransfer`、`verify` 报告 |
| 钳制次数 | `TestClampCountRecorded` |
| SQLite 条件更新、重复/乱序/跨分片 | `TestSQLiteConditionalPublication`、`TestCrossProcessPublicationAllowsOneContiguousWriter` |
| 优雅停止与恢复摘要一致 | `TestGracefulStopAndRecoveryByteIdentity` |
| SIGKILL 后恢复 WAV 一致 | `TestForceKillRestartProducesContinuousWaveform` |
| `E_STALE_BASE` 与当前版本 | `TestPolicyStoreStaleBaseAndSnapshotClassification`、`TestHTTPStaleBase` |
| 快照正常推进/替换/回退 | `TestPolicyStoreStaleBaseAndSnapshotClassification` |
| 增量复用/重建和最早块指纹 | `TestIncrementalRebuildReuse`、`TestBlockFingerprintChainFindsEarliestLocalBlock` |
| F0 区间统计与 `E_TOLERANCE_EXCEEDED` | `TestF0ToleranceErrorCode` |
