# Reg Workbench

确定性的线性模型正则化训练交互工作台，使用 Go 1.22+、SQLite、CGO 和标准库 HTTP 服务实现。输入由固定种子生成，覆盖约 `600×12` 小样本与 `6000×40` 中样本，支持 L1、L2、弹性网逻辑回归、分层 CV、正则化路径、延迟预算查询、缓存、增量复用、检查点恢复、文件锁发布保护、CLI 与本地 Web 控制台。

## 快速开始

```bash
go version
go test -mod=vendor -count=1 ./...

# 生成两档固定种子 CSV 与 manifest.json
go run -mod=vendor ./cmd/workbench generate --out samples

# 一键端到端验收：逐字节一致性、延迟预算、缓存重算、增量、停止恢复
go run -mod=vendor ./cmd/workbench verify --out artifacts/verification

# 固定摘要演示（两档样本均完整训练）
go run -mod=vendor ./cmd/workbench demo --out artifacts/demo

# CLI 训练/查询/停止/恢复/导出
go run -mod=vendor ./cmd/workbench train \
  --manifest samples/manifest.json --key small --dir sessions/small
go run -mod=vendor ./cmd/workbench query --dir sessions/small --kind selection
go run -mod=vendor ./cmd/workbench query --dir sessions/small --kind state --budget-ms 0
go run -mod=vendor ./cmd/workbench stop --dir sessions/small
go run -mod=vendor ./cmd/workbench resume --dir sessions/small
go run -mod=vendor ./cmd/workbench export --dir sessions/small --kind trajectory --out trajectory.json

# HTTP + 可视化控制台，默认仅监听本机
go run -mod=vendor ./cmd/workbench serve --addr 127.0.0.1:8787 --root artifacts/http
# 浏览器打开 http://127.0.0.1:8787/
```

Windows PowerShell：

```powershell
go version                 # 需要 go1.22 或更高
gcc --version              # 需要 MSYS2 UCRT64 mingw-w64 GCC
$env:CGO_ENABLED="1"
go build -mod=vendor -o workbench.exe .\cmd\workbench
.\workbench.exe verify --out artifacts\verification
```

若要链接系统/动态 SQLite，而不是驱动自带 amalgamation，可安装 SQLite 开发包后使用：

```bash
go build -mod=vendor -tags libsqlite3 -o workbench ./cmd/workbench
ldd workbench | grep sqlite3
```

Windows 动态链接时将 `sqlite3.dll` 放在 `PATH` 或可执行文件同目录，并使用 `-tags libsqlite3` 构建。

## 主要 HTTP API

- `POST /api/datasets/generate`：固定种子生成两档样本和声明文件。
- `GET /api/datasets`：读取 manifest。
- `POST /api/sessions`：创建会话和配置。
- `POST /api/sessions/{id}/run`：同步执行分阶段训练。
- `POST /api/sessions/{id}/start`：后台训练。
- `POST /api/sessions/{id}/stop`：写入优雅停止请求；当前求解轮次结束后停止。
- `POST /api/sessions/{id}/resume`：清理停止请求并从 SQLite/原子 JSON 检查点恢复。
- `POST /api/sessions/{id}/incremental`：按新配置增量复用未受影响路径点/折。
- `GET /api/sessions/{id}/query?kind=state|trajectory|metrics|current_coefficients|selection|final_model`：查询；支持 `fold`、`lambda`、`l1_ratio`、`budget_ms`、`query_id`。
- `GET /api/sessions/{id}/export?kind=coefficients|summary|trajectory`：导出产物。

## 确定性与一致性

- 随机源固定为 `20261125`（可由 `--seed` 覆盖），分折使用独立派生种子。
- 所有对外 JSON 使用结构体字段顺序或排序键，不写时间戳、主机名、随机 map 迭代值。
- 系数、截距和 CV 摘要在训练完成后写入 `exports/coefficients.json` 与 `exports/validation_summary.json`；两次从零运行逐字节一致。
- 查询缓存键包含样本 SHA-256、配置版本、折号、查询类型和正则化点；命中、删除后重算结果字节一致。
- 增量会话与全量重建使用 `CompareRunDirectories` 比较；不一致时返回 `E_INCREMENTAL_MISMATCH`、首个差异折和字段。

## 文档

- `DELIVERY.md`：需求与实现追踪、错误码、持久化/并发设计。
- `VALIDATION.md`：实际命令和验证结果。
- `docs/TASK-SPEC.md`：原始任务说明。
