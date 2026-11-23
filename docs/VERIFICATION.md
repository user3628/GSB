# 实际验证记录

记录时间：2026-11-24（固定样本标记仍为 `SAMPLE-FIXED-2024-01-01T00:00:00Z`，报告不读取墙钟）。

## 工具链

```text
$ go version
go version go1.27.4 linux/amd64
```

项目 `go.mod` 声明 `go 1.22.0`，并 vendored `modernc.org/sqlite v1.34.5`。

## 无网络测试与静态检查

```sh
GOPROXY=off go test ./...
GOPROXY=off go vet ./...
GOPROXY=off go test -race ./internal/engine ./internal/app ./internal/snapshot
```

结果：全部通过；`go vet` 无输出；race 检查通过。

测试覆盖：

- 秒/分钟/天/小时归一，FILETIME 1601 纪元换算与亚秒截断。
- 分值六位截断、乘法/加法/缩放溢出。
- 未知单位、非法标识、重复标识、截断长度块与已成功条目保留。
- 确定性偏差、区间重叠不确定性、误差传播上界。
- 严格/容错误差策略、预算部分结果、会话隔离、并发等待后全部完成。
- SQLite 连续版本、整体替换识别、强制跳号拒绝、加载跳号回退和诊断回放。
- 样本与报告重复运行字节级一致。

## Windows 构建

```sh
GOPROXY=off GOOS=windows GOARCH=amd64 go build -o /tmp/baselinebench.exe ./cmd/baselinebench
```

结果：成功生成约 11 MiB 的 Windows/amd64 可执行文件。CLI 只使用 `path/filepath` 与相对路径，示例见 README 的 PowerShell 命令。

## 一键演示（严格模式）

```sh
GOPROXY=off go run ./cmd/baselinebench demo --workdir work/demo
```

摘要输出：

```text
Baseline Workbench summary
sample_marker=SAMPLE-FIXED-2024-01-01T00:00:00Z mode=strict reference=packages/reference
packages=8 complete=3 failed=5 deterministic=1 uncertain=26

RANK  PACKAGE                              SCORE          ERROR_UPPER    SNAPSHOT  CODE
   1  packages/normal                     -64800.000000  0.000000       v1        OK
   2  packages/error-bound                0.000000       1.000002       -         E_ERROR_BOUND
   3  packages/filetime-overflow          0.000000       0.000000       -         E_NUMERIC_OVERFLOW
   4  packages/mixed-units                0.000000       0.000000       v1        OK
   5  packages/overflow-multiply          0.000000       0.000000       -         E_NUMERIC_OVERFLOW
   6  packages/reference                  0.000000       0.000000       v1        OK
   7  packages/truncated-text             0.000000       0.000000       -         E_POLICY_MALFORMED
   8  packages/unknown-unit               0.000000       0.000000       -         E_UNIT_UNKNOWN
```

稳定摘要：

```text
sample_sha256=6a5a765586a84e538311bfe362c9af902b5f3712501c60368294b67ea1c72c95
report_sha256=25ca84d784be48b64c9227c8224d026d95e2f53fc84373d18113e548a615402f
```

重复运行结果：

```text
report_run1=25ca84d784be48b64c9227c8224d026d95e2f53fc84373d18113e548a615402f
report_run2=25ca84d784be48b64c9227c8224d026d95e2f53fc84373d18113e548a615402f
summary_run1=bfd21fcaaa9328481dc4499724e2e2980123e2b9dec52f080f03229e69f2e9f7
summary_run2=bfd21fcaaa9328481dc4499724e2e2980123e2b9dec52f080f03229e69f2e9f7
```

## 五类异常注入

```sh
GOPROXY=off go run ./cmd/baselinebench verify --workdir work/verify
```

```text
Anomaly injection verification
PASS unknown-unit       expected=E_UNIT_UNKNOWN     actual=E_UNIT_UNKNOWN     ledger=rolled_back snapshot=v1->v1
PASS mul-overflow       expected=E_NUMERIC_OVERFLOW actual=E_NUMERIC_OVERFLOW ledger=rolled_back snapshot=v1->v1
PASS filetime-overflow  expected=E_NUMERIC_OVERFLOW actual=E_NUMERIC_OVERFLOW ledger=rolled_back snapshot=v1->v1
PASS version-gap        expected=E_SNAPSHOT_VERSION actual=E_SNAPSHOT_VERSION ledger=committed   snapshot=v1->v1
     fallback=E_SNAPSHOT_VERSION version=v1 score=777 passed=true
PASS error-bound        expected=E_ERROR_BOUND      actual=E_ERROR_BOUND      ledger=rolled_back snapshot=v1->v1
all_passed=true
```

```text
verification_sha256=a10c0cc81eeda0096f283ded3622e54cf058ce160d6ebd8b0218de6306891b89
```

重复运行验证报告哈希保持一致：`a10c0cc81eeda0096f283ded3622e54cf058ce160d6ebd8b0218de6306891b89`。

## 容错模式演示

```sh
GOPROXY=off go run ./cmd/baselinebench demo --workdir work/demo-tolerant --mode tolerant
```

结果：`packages/error-bound` 在误差上界 `1.000002` 下被接受并提交 v1；未知单位、截断块、数值溢出仍保留业务码且不提交快照。该模式报告哈希为 `1262a8364cd6df30436362ba5aefca67b75461ba785b8f3ef4f28b6b84a8a70d`。
