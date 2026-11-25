# qaudit — 分层量化标定与误差传播审计工作台

面向轻量图像分类器的纯 Go 工作台：内置固定种子生成权重、灰度 PNG 分片、量化策略与
高精度参考输出，按「校准样本选取 → 逐层定点标定 → 量化推理 → 误差传播审计」四阶段
推进，并以 SQLite 事务维护单调版本快照，支持强制终止恢复、版本替换识别、并发发布
保护与外部参数注入验证。

## 环境要求

- Go 1.22+（验证环境：Go 1.27.4，`CGO_ENABLED=0` 静态构建）
- SQLite 3 命令行工具（用于人工检查 `snapshots.db`）
- 模块代理：默认代理不可用时执行 `go env -w GOPROXY=https://goproxy.cn,direct`
- 无需 Docker、GPU、外部账号或在线数据集

## 快速开始

```bash
go mod tidy
go build ./...
go test ./... -race

# 一键确定性验证：测试、逐字节一致性、崩溃恢复、注入业务码
./scripts/verify.sh

# 手动演示
D=$(mktemp -d)
go run ./cmd/qaudit generate -data "$D"
go run ./cmd/qaudit run      -data "$D"
go run ./cmd/qaudit status   -data "$D"
go run ./cmd/qaudit audit    -data "$D"
go run ./cmd/qaudit versions -data "$D"
go run ./cmd/qaudit export   -data "$D"
sqlite3 "$D/snapshots.db" "SELECT version, seq, layer_name FROM snapshots;"

# 强制终止后恢复（发布第 2 层后模拟 kill -9）
go run ./cmd/qaudit run     -data "$D" -crash-after-layer 2 || true
go run ./cmd/qaudit recover -data "$D"

# 本地 HTTP（默认随机空闲回环端口，也可 -addr 127.0.0.1:PORT）
go run ./cmd/qaudit serve -data "$D" -addr 127.0.0.1:0
```

## 稳定业务码

| 代码 | 触发阶段 | 典型场景 |
| --- | --- | --- |
| `E_CALIB_SAMPLE` | 阶段 1 | 比例为零/越界、分片尺寸不符、非有限特征 |
| `E_QUANT_PARAM` | 阶段 2 | 非法位宽、全零通道、NaN/Inf 权重、零尺度 |
| `E_NUMERIC_MISMATCH` | 阶段 3 | 显式抽样位置逐层偏差超出可配置容差 |
| `E_AUDIT_INPUT` | 阶段 4 | 参考输出缺失、量化权重全零 |
| `E_STALE_VERSION` | 持久化/版本 | 回退、重复/错位/落后发布、恢复指纹不符 |

## 目录

- `cmd/qaudit`：CLI 与 HTTP 入口
- `internal/`：分领域实现（见 `docs/DESIGN.md` 的包职责表）
- `scripts/verify.sh`：一键确定性验证脚本
- `docs/DESIGN.md`：架构、量化公式、误差上界推导、版本与恢复模型

## 验证结论

`scripts/verify.sh` 断言：两次独立运行的版本指纹/校准清单/逐层产物/导出报告逐字节
一致；发布第 2 层后强制终止再恢复，报告与连续执行完全一致；五类注入分别在所属阶段
返回稳定业务码；`go test ./... -race` 全部通过。
