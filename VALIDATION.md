# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）。

## 环境

```text
go version go1.27.4 linux/amd64
gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0
pkg-config --modversion sqlite3 = 3.46.1
CGO_ENABLED=1
```

`go.mod` 最低语言版本固定为 `go 1.22`，依赖已放入 `vendor/`，可使用 `-mod=vendor` 离线构建和测试。

## 已执行命令

```bash
go vet -mod=vendor ./...
go test -mod=vendor -count=1 ./...
go run -mod=vendor ./cmd/workbench verify --out artifacts/verification
go run -mod=vendor ./cmd/workbench demo --out artifacts/demo
go build -mod=vendor -tags libsqlite3 -o /tmp/regworkbench-libsqlite3 ./cmd/workbench
ldd /tmp/regworkbench-libsqlite3 | grep sqlite3
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor ./internal/filelock
```

结果：

- `go vet`：通过，日志 `artifacts/validation/go-vet.log`。
- `go test -mod=vendor -count=1 ./...`：全部包通过，完整日志 `artifacts/validation/go-test.log`。
- 一键验收：small 与 medium 两档各 15 项检查全部 `ok: true`，报告 `artifacts/verification/verification.json`。
- 固定演示：两档均 `state=completed`、检查点序号 7，摘要 `artifacts/demo/demo-summary.json`。
- 动态 SQLite 标签构建：通过；`ldd` 显示链接 `libsqlite3.so.0`。
- Windows 文件锁包交叉编译：通过。
- 另用 `GOTOOLCHAIN=go1.22.12 go test -mod=vendor ./internal/... ./cmd/...` 验证最低支持版本：全部包通过（SQLite C 编译仅有上游 const 限定警告）。

## 验收覆盖的关键性质

- 两个独立从零会话的 `coefficients.json` 与 `validation_summary.json` 逐字节相同。
- 查询缓存首次计算、第二次命中、删除后重算的 `result` 字节相同。
- `budget_ms=0` 稳定返回 `E_LATENCY_BUDGET`，并带 `query_id` 与 `elapsed_ms`。
- 轮次钩子中可读取实时 trajectory，并同时验证预算错误；随后优雅停止。
- 第一折检查点后请求停止，恢复后的系数/摘要与未中断从零运行逐字节相同。
- 正则化网格增量运行复用未变更的逐折路径点；增量产物与全新全量构建逐字节相同。
- 检查点序号重复/落后/跨折错位返回 `E_STALE_CHECKPOINT`，非法阶段跳转返回 `E_INVALID_STAGE`。
- NaN 指标返回 `E_INVALID_FOLD`，NaN 数值输入返回 `E_NUMERIC`。

## 固定摘要（快速演示配置）

完整机器可读摘要见 `artifacts/demo/demo-summary.json`。其中：

- small：600 行、12 列；选中点 `lambda=0.01`、`l1_ratio=0.5`。
- medium：6000 行、40 列；选中点 `lambda=0.01`、`l1_ratio=0`。

这些数值来自固定种子和固定求解配置；不要在代码中加入运行时间、随机 map 顺序或绝对路径序列化，以维持逐字节复现。
