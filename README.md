# mitmpin

中间人（MITM）检测与证书固定（Certificate Pinning）校验的数据处理管线与结果验证工具。
全部样本由本地确定性生成器产出，校验在内存完成，不发起任何真实网络握手；
持久化采用纯 Go 的 `modernc.org/sqlite`，无需 CGO，可直接在 Windows 构建运行。

## 能力概览

- 观测记录与固定策略解析：策略唯一性、哈希算法、事件连续性、证书链结构校验
- SPKI 固定与受信根路径验证，MITM/SNI/协议/链断裂独立业务码
- 策略快照热更新：单事务整体替换、内容哈希识别、连接级版本绑定可追溯
- 按事件检查点与中断恢复：跳号 `E_SNAPSHOT_VERSION`、回退最近连续版本
- 并发调度与资源预算：加权排队、等待队列、`E_EVENT_BUDGET` / `E_GLOBAL_BUDGET`
- 五类异常注入验证，不修改源码，正常/异常样本同批隔离
- 确定性 JSON 报告：稳定排序、固定 marker、字节级一致、交互侧重放逐条核对

## 快速开始

```bash
export GOTOOLCHAIN=local
go test ./...

go run ./examples/quickstart

go build -o bin/mitmpin ./cmd/mitmpin
./bin/mitmpin generate -out data/sample.json
./bin/mitmpin run -in data/sample.json -db data/run.db -report data/report.json
```

## 目录

- `cmd/mitmpin`：命令行（generate / run / inject）
- `examples/quickstart`：一键运行示例
- `internal/`：sample、certutil、pin、policy、store、engine、scheduler、replay、report、generator、pipeline
- `facade.go`：宿主服务集成的导出接口
- `DESIGN.md`：架构、业务码映射、关键设计与实际验证结果

数据文件默认写入项目 `data/` 目录（已在 `.gitignore` 忽略），运行前无需预置外部资源。
