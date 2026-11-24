# TLS/MITM 检测与证书固定管线

纯 Go + 纯 Go SQLite 驱动（`modernc.org/sqlite`）实现的本地数据处理管线。它从确定性样本导入二进制/JSONL 连接观测，完成版本裁决、时间与身份归一化、证书链与固定策略判定、策略热更新的事件级版本绑定、状态机重放、结论/证据落盘，以及强制终止后的意图恢复。

## 快速运行

```bash
go version          # 开发验证使用 go1.27.4；go.mod 声明 go 1.22
go mod tidy
go test ./...
go build -o artifacts/pipeline ./cmd/pipeline

# 一键验收：固定样本、HTTP 回环、SIGKILL 子进程、恢复与报告
./artifacts/pipeline verify --report artifacts/verification-report.json

# 启动可配置端口的本地 HTTP 服务
./artifacts/pipeline serve --addr 127.0.0.1:18080 --db artifacts/pipeline.db

# 生成固定样本到目录
./artifacts/pipeline gen-samples --dir artifacts/samples
```

`verify` 默认监听 `127.0.0.1:0`（随机回环端口），数据库和样本放入系统临时目录；完整 JSON 报告默认写入 `artifacts/verification-report.json`。可重复执行，证书由固定种子生成，不访问网络或真实证书。

Windows 请安装 Go 1.22+ 官方 MSI，在新终端确认 `go version`，然后使用 PowerShell/CMD 执行：

```powershell
go build -o artifacts\pipeline.exe .\cmd\pipeline
.\artifacts\pipeline.exe verify --report artifacts\verification-report.json
.\artifacts\pipeline.exe serve --addr 127.0.0.1:18080 --db artifacts\pipeline.db
```

当前实际运行记录来自 Linux/WSL；代码已通过 `GOOS=windows/amd64 go build ./...` 交叉编译检查，强制终止在 Windows 分支使用 `Process.Kill()`。

## 已验证结果

最近一次实际运行：

- `go test ./...`：`codec`、`normalize`、`pki`、`replay`、`service`、`store` 全部通过。
- `go test ./internal/service -race`：并发读者测试通过。
- `go vet ./...`：无输出。
- `./artifacts/pipeline verify`：16 个场景、11 项检查全部 `PASS`（另含交叉签名/环检测等单元测试）。
- 强制终止演练：子进程在持久化意图后被 `SIGKILL`（退出码 `-1`）；重启扫描到 1 条 `started` 意图，最后完整提交版本为 1，崩溃批次连接 `CRASH-RECOVERY-0001` 在恢复前不可见；恢复将意图/提交版本 2 标记为 `aborted`，随后重新导入成功。

详见 `docs/VERIFICATION.md` 与 `artifacts/verification-report.json`。

## HTTP 接口

失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_RECORD_CORRUPT",
    "message": "...",
    "context": {"record_index": 1}
  }
}
```

主要接口：

- `GET /healthz`
- `GET /api/v1/compatibility`
- `POST /api/v1/import?rounding=truncate|round|ceil&batch_id=...`
- `GET /api/v1/conclusions?commit=N`
- `GET /api/v1/connections/{id}?commit=N`
- `POST /api/v1/connections/{id}/replay?rounding=...`
- `GET /api/v1/policy-versions`
- `POST /api/v1/policy-versions/switch`
- `GET /api/v1/policy-versions/{version}`
- `GET /api/v1/policy-versions/{version}/conclusions`
- `GET /api/v1/recovery`
- `POST /api/v1/recovery`

连接 ID 在入口按大写/去空白规则归一化；SNI 按小写、去末尾根点归一化。

## 错误码

- `E_RECORD_CORRUPT`：结构、长度、CRC、JSON 行或事件序列不合法，上下文含 `record_index`。
- `E_FORMAT_UNSUPPORTED`：高版本包含无法解释的必填字段或未知必需位图。
- `E_IDENTITY_CONFLICT`：同一规范连接 ID 的 SNI/证书序列号冲突。
- `E_CHAIN_INVALID`：缺根、成环、签名失败、有效期异常或签名算法不受支持；不等同于 MITM。
- `E_VERSION_RETIRED`：查询已替换的旧策略版本；上下文附当前可用版本和 revision 区间。
- `E_INVALID_TRANSITION`：事件违反握手状态机。
- `E_NOT_FOUND`、`E_CONFLICT`、`E_INTERNAL`。

## 代码地图

- `internal/domain`：稳定错误码与 API/持久化共享模型。
- `internal/codec`：二进制 v1/v2、JSONL v1/v2、魔数/CRC/位图/兼容矩阵。
- `internal/normalize`：身份归一化、纳秒/RFC3339 到毫秒、舍入位与累计误差。
- `internal/pki`：PEM 解析、乱序/交叉路径构建、环检测、有效期、SPKI/根固定。
- `internal/replay`：策略时间线、事件级版本绑定、状态机和结论聚合。
- `internal/store`：SQLite schema、意图/提交事务、版本快照、恢复扫描。
- `internal/service`：导入、身份冲突、重放和策略切换编排。
- `internal/httpapi`：本地 HTTP JSON API。
- `internal/sample`：固定种子证书与验收样本。
- `internal/verify`：可复现验证器和真实 `SIGKILL` 子进程演练。
- `cmd/pipeline`：`serve`、`verify`、`crash-import`、`recover`、`gen-samples`。

更多二进制布局、状态机和恢复语义见 `docs/TECHNICAL.md`。
