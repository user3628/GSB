# 实际验证记录

验证时间环境：Linux/WSL，Go `go1.27.4 linux/amd64`（`go.mod` 声明 `go 1.22`），SQLite `3.46.1`，CGO 可用。

复现命令：

```bash
./scripts/verify.sh
```

脚本的完整原始输出保存在 `artifacts/verification-results.txt`。最近一次运行以 `VERIFICATION_OK` 结束；`go test ./... -count=1`、`go test -race ./... -count=1` 和定向并发测试全部通过。

## 验收点与结果

| 验收点 | 实际结果 |
| --- | --- |
| 固定样本规则数 | `demo` 命名空间编译出 9 条规则、18 个选择器索引条目、18 条包含/相等关系、3 个冲突对 |
| 端口合并 | `demo` 原始 9 段合并为 5 段；TCP `7 -> 4`，UDP `2 -> 1`，并输出 overlap/adjacent 事件 |
| 逐端口裁决 | `8083=allow`、`8087=allow`、`8092=deny`；8087 同时命中 allow/deny，同优先级按声明次序由 `demo-allow-edge` 决定 |
| 顺序无关 | 规则随机重排后 `compiledDigest` 与 `decisionDigest` 不变，见 `TestCompilationIsOrderIndependentAndStable` |
| 非法规则首字段 | `E_RULE_INVALID`，`field=rules[0].action` |
| 陈旧快照 | 重复提交 v1 返回 `E_STALE_SNAPSHOT`，上下文含 `currentVersion=1/submittedVersion=1` |
| 增量提交 | 固定增量以 base v1 成功生成 v2，`demo` 规则数变为 10 |
| 同 base 并发增量 | `TestConcurrentIncrementsSameBaseOnlyOneSucceeds`：1 个成功、1 个 `E_STALE_BASE` |
| 同请求 ID 并发 | 8 个调用方全部得到同一成功结果，`snapshots` 只有 v1/v2 两行，见 `TestSameRequestIDConcurrentExecutesOnce` |
| 内容冲突幂等 | 同 ID 不同内容返回 `E_CONFLICTING_REQUEST`，见 `TestConflictingRequestID` |
| 陈旧读拒绝 | 重建窗口内 `GET /v1/views/demo?observationVersion=3` 返回 `E_STALE_READ`，上下文为 `viewVersion=2 observedVersion=3 targetVersion=3 lagVersions=1`；observation v2 仍可读旧完整版 |
| 定点一致度 | 示例分数：alpha/beta/gamma `840000`，demo `786667`；每命名空间输出 Q6 分数、逐步舍入和累计误差分数 |
| 容差边界 | `TestThresholdToleranceBoundaryIsDistinguishable`：Q6 边界内与相差 1 Q6 的边界外状态不同 |
| SIGKILL 恢复 | `--crash-point=mid_compile` 下进程被 `SIGKILL`，curl 退出码 52；重启后恢复报告为 `recovered_unfinished_intents`，`lastCompleteVersion=3`，1 个意图 aborted，`snapshotPresent=false readyViews=0` |
| 崩溃后重试 | 同一 `X-Request-ID: verify-crash-v4` 重试成功提交 v4，视图 usedVersion=4 |
| 数据竞争 | `go test -race ./... -count=1` 通过 |

## 手动 HTTP 验证

```bash
go run ./cmd/netpol-compiler --addr 127.0.0.1:18080 --data-dir ./.localdata --seed-sample

curl -sS 'http://127.0.0.1:18080/v1/views/demo?observationVersion=1'
curl -sS -X POST 'http://127.0.0.1:18080/v1/adjudications?observationVersion=1' \
  -H 'Content-Type: application/json' \
  --data @docs/examples/adjudication.json
curl -sS 'http://127.0.0.1:18080/v1/consistency?observationVersion=1&threshold=0.75&relativeTolerancePpm=50000'
curl -sS 'http://127.0.0.1:18080/v1/recovery'
```

## 崩溃恢复手动复现

```bash
go build -o /tmp/netpol-compiler ./cmd/netpol-compiler
DATA=$(mktemp -d /tmp/netpol-manual.XXXXXX)

/tmp/netpol-compiler --addr 127.0.0.1:18080 --data-dir "$DATA/db" --seed-sample
# 另一个终端提交示例；随后用 v2 样本和 --crash-point=mid_compile 重启并提交。

/tmp/netpol-compiler --addr 127.0.0.1:18080 --data-dir "$DATA/db" --crash-point=mid_compile
curl -sS "$BASE/v1/sample/snapshot?version=2" > /tmp/snapshot-v2.json
curl -X POST http://127.0.0.1:18080/v1/snapshots \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: manual-crash-v2' \
  --data @/tmp/snapshot-v2.json

/tmp/netpol-compiler --addr 127.0.0.1:18080 --data-dir "$DATA/db"
curl -sS http://127.0.0.1:18080/v1/recovery
```
