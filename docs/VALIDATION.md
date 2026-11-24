# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）。

本机执行环境：

- `go version go1.27.4 linux/amd64`
- 模块声明：`go 1.22.0`
- SQLite：`modernc.org/sqlite v1.29.0`，依赖已 vendor。
- 工作样本、SQLite DB 和演示目录均位于 `mktemp -d`/`$env:TEMP`，未把 WAV/DB 写入源码目录。

## 1. 离线全量测试

命令：

```bash
GOPROXY=off GOFLAGS=-mod=vendor go test -count=1 -v ./...
```

结果：

```text
--- PASS: TestFourZoneVADAndAttribution (0.04s)
--- PASS: TestEditSplitMergePriorityAndHit (0.03s)
--- PASS: TestQueryCacheAndBudget (0.02s)
--- PASS: TestLocalVerificationAndConcurrency (0.50s)
--- PASS: TestInjectionErrorCodes (0.05s)
--- PASS: TestIncrementalMismatchReturnsFirstInterval (0.03s)
--- PASS: TestGracefulStopAndReopenRecovery (0.15s)
--- PASS: TestHardExitRecoverySubprocess (0.15s)
--- PASS: TestStoreSequenceCASAndTamperClassification (0.02s)
--- PASS: TestSnapshotRollbackClassification (0.02s)
--- PASS: TestConcurrentPublishSameSequenceOnlyOneWins (0.02s)
ok  	cockpitaudio	1.055s
```

`GOPROXY=off GOFLAGS=-mod=vendor go vet ./...` 无输出并以 0 退出。

## 2. 四音区检测与归属

固定生成 3 个分片，每片包含 `driver`、`passenger`、`rear_left`、`rear_right` 四个存储段。默认帧参数的换算值为：

```text
frame_length_ms=25 -> frame_length_samples=400
frame_shift_ms=10  -> frame_shift_samples=160
fft_samples=512
```

每个分片四音区均检出 2 个稳定语音区间，共 24 个统一时间轴区间、36 个跨音区重叠标记、6 条指令。

归属结果：

- `*-cmd-open-window`：明确归属 `driver`，与第二名分差约 `1.841`，阈值 `0.18`。
- `*-cmd-temperature`：默认配置标记 `ambiguous=true`，最高分 `rear_left`、第二名 `rear_right`，分差 `0.060 < 0.18`；4 个候选、6 对时延和全部对数证据均保留。

## 3. 增量与并发一致性

命令：

```bash
go run ./cmd/cockpit-audio verify --workdir <temp-verify>
```

实际 `verification.json` 已复制到 `artifacts/verification.json`。10 个验收用例全部 `ok=true`：

```text
deterministic_full_build           ok
priority_change                    ok  earliest=attribution
vocabulary_hit_change              ok  earliest=attribution
frame_parameters                   ok  earliest=vad
hysteresis_thresholds              ok  earliest=vad
attribution_gap_threshold          ok  earliest=attribution
speed_parameter                    ok  earliest=attribution
query_cache_byte_consistency       ok  earliest=query
latency_budget_structured_error    ok  earliest=query
concurrent_edit_settlement         ok  earliest=query
```

并发审计实际规模：

```text
queries_checked=84
edits_applied=14
unique_versions=[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14]
```

每个并发响应都按其 `edit_version` 在结算侧重放编辑并全量计算；查询规范 JSON 与结算侧逐条一致。

## 4. 延迟预算

一次真实 CLI 查询：

```bash
cockpit-audio query --workdir <golden> --type active_zones --at 250
```

实际输出摘要：

```text
elapsed_ms=0.033003
zones=[driver passenger rear_left rear_right]
cache_key=type=active_zones;at=250;range=0-0;command=;shard=;cfg=07d3cba268b68c1c;edit=0;digest=8d11824f30671d05
```

将 `--budget-ms 0.000001` 时返回结构化错误：

```json
{
  "code": "E_LATENCY_BUDGET",
  "stage": "query",
  "query_id": "cli-query",
  "elapsed_ms": 0.071709,
  "budget_ms": 0.000001
}
```

## 5. 硬退出、`SIGKILL` 与恢复

连续执行、`--crash-after 4` 后 `os.Exit(137)`、以及真实 `kill -9` 后恢复，三份 `report.json` 的 SHA-256 完全相同：

```text
f9e66e8ea8ad9d4d99a2a15744ba7decd43c536e0a995a8fde5afbe708c6c31d  continuous/report.json
f9e66e8ea8ad9d4d99a2a15744ba7decd43c536e0a995a8fde5afbe708c6c31d  hard-exit/report.json
f9e66e8ea8ad9d4d99a2a15744ba7decd43c536e0a995a8fde5afbe708c6c31d  kill9/report.json
```

`go test` 中的 `TestHardExitRecoverySubprocess` 也会启动子进程，在第 4 个事务提交后直接 `os.Exit(137)`，再重开 SQLite 恢复并与连续结果做 canonical JSON 字节比较。

## 6. SQLite 发布保护

测试覆盖并通过：

- 同分片显式序号重复发布：仅第 1 个成功，其余返回 `E_STALE_CHECKPOINT` / `duplicate_or_behind`。
- 序号 `1,2,4`：跳号请求返回 `future_gap`。
- `ExpectedShardID` 与实际分片不符：返回 `cross_shard`。
- 12 个发布者竞争同一序号：恰好 1 个成功、11 个收到 `E_STALE_CHECKPOINT`。
- 直接改写产物摘要：`VerifySnapshot` 返回 `classification=tamper`。
- 将旧序号记录恢复为有效：返回 `classification=rollback`。

## 7. 注入错误码

实际 CLI 输出的稳定码：

```text
E_SAMPLE_COUNT_MISMATCH  ingest       shard-001
E_RATE_CONFLICT          ingest       shard-001
E_INVALID_PARAM          ingest       shard-001  reason=offset_overlap
E_INVALID_PARAM          attribution  shard-001  max_speakers=0
E_INVALID_PARAM          vad          shard-001  mask_iterations<0
E_INVALID_PARAM          attribution  shard-001  speed_mps<=0
```

## 8. Windows 交叉构建

命令：

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
GOPROXY=off GOFLAGS=-mod=vendor \
go build -o /tmp/cockpit-audio-validation.exe ./cmd/cockpit-audio
```

结果：成功生成约 11 MB 的 Windows amd64 PE 文件。文件锁实现分别为 Unix `flock` 与 Windows `LockFileEx`。
