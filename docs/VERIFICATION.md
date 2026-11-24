# 实际验证记录

- 运行时间（报告 started_at）：`2026-11-24T17:13:13.692713344Z`
- 工具链：`go version go1.27.4 linux/amd64`；另已执行 `GOOS=windows/amd64 go build ./...` 交叉编译通过
- 模块声明：`go 1.22`；SQLite 驱动：`modernc.org/sqlite v1.34.5`（纯 Go）
- 完整机器报告：`artifacts/verification-report.json`
- 测试日志：`artifacts/go-test-final.log`
- Race 日志：`artifacts/go-test-race-final.log`（`go test -race ./...` 全部通过）
- 端到端控制台日志：`artifacts/verify-final-console.txt`

## 执行命令

```bash
go test ./...
go test -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
go build -o artifacts/pipeline ./cmd/pipeline
./artifacts/pipeline verify --report artifacts/verification-report.json
```

端到端验证包含 `16` 个场景和 `11` 项检查，报告 `ok=True`。

## 场景结果

| 结果 | 场景 | HTTP | 期望 | 实际/错误码 |
|---|---|---:|---|---|
| PASS | `accept_current_binary` | 200 | `trusted` | `trusted` |
| PASS | `accept_current_jsonl` | 200 | `trusted` | `trusted` |
| PASS | `identity_case_normalization_merge` | 200 | `trusted` | `trusted` |
| PASS | `degraded_accept_legacy_jsonl_v1` | 200 | `trusted` | `trusted` |
| PASS | `degraded_accept_binary_v1` | 200 | `trusted` | `trusted` |
| PASS | `direct_connection_replay_endpoint` | 200 | `trusted` | `trusted` |
| PASS | `abnormal_termination_not_trusted` | 200 | `insufficient_evidence` | `insufficient_evidence` |
| PASS | `missing_root_is_chain_invalid_not_mitm` | 200 | `insufficient_evidence` | `insufficient_evidence` |
| PASS | `untrusted_root_pin_miss_is_mitm` | 200 | `suspected_mitm` | `suspected_mitm` |
| PASS | `hot_update_event_level_binding_mixed` | 200 | `suspected_mitm` | `suspected_mitm` |
| PASS | `switch_endpoint_initial_trusted` | 200 | `trusted` | `trusted` |
| PASS | `http_policy_switch_and_replay` | 200 | `suspected_mitm` | `suspected_mitm` |
| PASS | `reject_future_format_unknown_required` | 400 | `E_FORMAT_UNSUPPORTED` | `E_FORMAT_UNSUPPORTED` |
| PASS | `reject_corrupt_crc_record_index` | 400 | `E_RECORD_CORRUPT` | `E_RECORD_CORRUPT` |
| PASS | `reject_normalized_identity_conflict` | 409 | `E_IDENTITY_CONFLICT` | `E_IDENTITY_CONFLICT` |
| PASS | `recovery_batch_reimport_after_abort` | 200 | `suspected_mitm` | `suspected_mitm` |

## 检查项

- [x] **force_kill_recovery**：SIGKILL subprocess left one durable started intent; HTTP recovery aborted it without exposing crashed connection
- [x] **identity_normalization**：lowercase connection id, uppercase SNI root dot, and hex serial normalize to same entity
- [x] **normalized_http_identity_lookup**：lowercase path ID resolves to canonical connection entity
- [x] **degradation_matrix**：legacy v1 accepted with default protocol and low precision listed
- [x] **replay_api**：POST /connections/{id}/replay re-read durable record and wrote a complete conclusion version
- [x] **chain_invalid_error_code**：missing root replay returns E_CHAIN_INVALID with persisted insufficient_evidence conclusion
- [x] **event_level_policy_binding**：seq3 trusted under v1; seq4 MITM under replacement v2; final result not backfilled
- [x] **retired_version_query**：old policy retrospective query returns E_VERSION_RETIRED with current range
- [x] **active_version_query**：
- [x] **policy_switch_api**：runtime whole-policy replacement switches judgment on replay
- [x] **whole_batch_atomicity**：failed format/corrupt/identity imports did not advance complete commit or change visible connections

## 强制终止恢复差异

- 子进程：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-235/artifacts/pipeline`
- SIGKILL 观测：`True`；退出码：`-1`
- 恢复前未完成意图：`1`；可见连接：`['ACCEPT-20251124-0001']`
- 恢复扫描中止意图：`['intent-2-1795540393726']`
- 最后完整提交：`1`
- 恢复 facts：
  - aborted unfinished intent intent-2-1795540393726 for commit 2
  - last complete commit is 1
  - durable connection fact visible: ACCEPT-20251124-0001
- 恢复后未完成意图：`0`
- 重新导入后可见连接数：`9`，包含 `CRASH-RECOVERY-0001`

恢复报告中的 facts 只包含已落盘的意图、完整提交和连接 ID。

## 降级与舍入抽样

- 降级明细条数：`6`（含 binary/JSONL v1 的协议补全与精度降级）。
- 热更新连接 `HOT-UPDATE-0001` 累计舍入误差（truncate）：

| event_seq | raw_unix_ns | timestamp_ms | remainder_ns | rounding_bit | applied_delta_ns | cumulative_ns |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1763971200001000000 | 1763971200001 | 0 | 0 | 0 | 0 |
| 2 | 1763971200010000000 | 1763971200010 | 0 | 0 | 0 | 0 |
| 3 | 1763971200020123456 | 1763971200020 | 123456 | 0 | -123456 | -123456 |
| 4 | 1763971200030650499 | 1763971200030 | 650499 | 1 | -650499 | -773955 |
| 5 | 1763971200040999999 | 1763971200040 | 999999 | 1 | -999999 | -1773954 |
