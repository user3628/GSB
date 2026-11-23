# 快速上手

前置：Go 1.22+（已在 go1.27 验证）。依赖通过纯 Go SQLite 驱动获取，无需
外部数据库或网络账号。

```bash
go mod tidy
go build ./...

# 端到端：在临时目录确定性生成样本并输出 report.json
go run ./cmd/injectctl run -workdir ./work

# 五类异常注入自检（all_passed=true）
go run ./cmd/injectctl selfcheck -workdir ./work-sc

# 强制终止与恢复
go run ./cmd/injectctl crash   -workdir ./work-crash   # exit 9
go run ./cmd/injectctl recover -workdir ./work-crash

# 资源控制
go run ./cmd/injectctl run -workdir ./work-b -maxsamples 3 -maxencoded 12 -concurrency 4 -queue 16
```

输出 `report.json` 包含：批次样本摘要、裁决与归一化前后字节偏移、单调时钟
延迟统计（固定标记）、快照版本链与全部业务码，按批次标识与样本序号稳定
排序，重复运行字节级一致。详细设计见 `DESIGN.md`，验证记录见 `VALIDATION.md`。
