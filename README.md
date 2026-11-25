# configaudit — 基础设施配置一致性数值审计

基于 Go + SQLite（纯 Go 驱动 `modernc.org/sqlite`）的配置一致性数值审计服务。
对多节点、多来源通道的整数/定点配置做单位换算、误差传播、优先级叠加，
并与本地高精度有理数（`big.Rat`）参考逐项比对，输出可复核的误差上界。

完整设计、误差模型、错误码与验收记录见 [docs.md](docs.md)。

## 快速开始

```bash
go version            # 需要 Go 1.21+（本仓库在 go1.27 上验证，go.mod 声明 1.22）
go test ./...         # 全量测试
go vet ./...

go run ./cmd/configaudit generate -dir data      # 固定种子生成 JSONL 样本与参考
go run ./cmd/configaudit audit    -dir data      # 全量审计并落快照
go run ./cmd/configaudit crash-demo -dir run -db demo.db  # 强制终止后恢复演示
go run ./cmd/configaudit serve    -dir data -addr 127.0.0.1:8080
```

## 代码导航

- `internal/fixedpoint`：int64 定点运算、换算链、误差传播、错误码
- `internal/units`：单位表与新旧两代来源描述归一化
- `internal/model`、`internal/parser`：模型与带行号/字段的样本校验
- `internal/sample`：固定种子确定性样本与高精度参考生成
- `internal/audit`：审计引擎、优先级叠加/遮蔽/冲突、参考比对、服务编排
- `internal/store`：SQLite 快照、审计结果、误差记录、意图日志与崩溃恢复
- `internal/httpapi`：本地审计提交与查询端点
- `cmd/configaudit`：CLI 入口

## 关键约束

- 不使用 Docker 或容器；样本完全离线、确定性生成。
- 所有比对按 节点 × 配置项 × 来源通道 × 换算链 完整遍历，禁止抽样。
- 单写者串行提交、多读者并发查询、request_id 幂等，读者不会看到中间状态。
