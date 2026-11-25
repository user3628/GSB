# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）  
执行环境：`linux/amd64`，`go version go1.27.4 linux/amd64`  
目标兼容：`go 1.22`，并已通过 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...`  
SQLite：纯 Go 驱动 `modernc.org/sqlite v1.35.0`，本机 `sqlite3` CLI 可读取数据库。

> 说明：本次实际执行和强杀恢复发生在当前 Linux 工作区；Windows 侧完成了纯 Go 交叉编译。目标 Windows 机器请运行 `.\scripts\verify.ps1` 做原生复验。

## 1. 命令结果

| 命令 | 实际结果 |
|---|---|
| `go mod tidy` | 通过；固定 `modernc.org/sqlite v1.35.0` |
| `go build ./...` | 通过 |
| `go test ./...` | 通过：`ok grayrouter`，两个 cmd 包无测试文件 |
| `go test -race ./...` | 通过：`ok grayrouter 1.410s` |
| `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...` | 通过 |
| `go vet ./...` | 通过 |
| `go run ./cmd/verify` | `PASS: 13 checks`，生成 `artifacts/verification-report.json` 和 `.md` |

仓库包含 `vendor/`。依赖已整理后，可使用 `GOPROXY=off go build ./...` 与 `GOPROXY=off go test ./...` 离线重复执行。

## 2. 13 项验收检查

1. `E_GRAPH_INVALID` 返回节点/字段上下文，且非法图不写入快照或模型表。
2. 图签名重复计算逐位一致，并检测全部五类差异。
3. 在灰度切换前绑定在途请求 `verify-pin-0001` -> `demo-op@1.0.0`。
4. 提交固定基线快照 v1（6 个模型版本）。
5. 提交增量 v2/base1（5 个受影响模型），生成类别驱动的迁移/重建清单。
6. head 切到 v2 后，绑定请求仍完成于 `demo-op@1.0.0`。
7. 灰度定点总和为 `1,000,000`，同请求同快照裁决 JSON/digest 逐位一致。
8. 同模型/种子两次推理的完整结果 JSON 逐位一致，并输出量化舍入步骤和累计误差。
9. 24 个并发同 request id 推理只产生 1 次图执行，所有等待者复用同一结果。
10. 同 base2 的两个并发增量一成一冲突：`E_CONFLICTING_UPDATE`；旧 base1 返回 `E_STALE_BASE`。
11. 32 个读协程 × 10 次 State/Artifacts/CommitReport 查询全部成功。
12. 真实子进程在级联重建 `after_stage` 检查点被 `Process.Kill` 后恢复：head 仍为 1，中止 1 个意图、1 个 staging 快照、20 行 staging 派生产物。
13. 真实子进程在推理绑定 `after_binding` 检查点被强杀后恢复：head 仍为 1，中止 1 个意图和 `crash-inflight-0001` 绑定。

## 3. 可核对摘要

灰度样例（request `req-0001`，snapshot 2，route `demo-op-gray`）：

- 目标：`demo-op@1.1.0`
- bucket：`807092 / 1000000`
- digest：`901955bb941177346749d1edd5f04506956a3de8df7817275c621d974c4b02a4`

量化推理样例（`demo-quant@1.1.0`，seed `20261125`）：

- 输出：`out`，shape `[1,4]`，dtype `float32`
- digest：`sha256:972d9d6543d154b8e31f71aea8cf4e1d6fa956bd31977b35a046dcf4c96fce64`

主验证库：`artifacts/verify-work/main.db`

```text
head_state: snapshot_version=3
snapshots:
1 full_snapshot committed 6 models
2 delta_commit/base1 committed 5 models
3 delta_commit/base2 committed 1 model
derived_artifacts: 12 routing_table / 12 prepost_cache / 12 inflight_binding_index / 12 gray_plan, all ready
inflight_bindings: verify-pin-0001 -> demo-op@1.0.0 snapshot=1 completed
```

SQLite CLI 核对：

```bash
bash scripts/check-sqlite.sh artifacts/verify-work/main.db
sqlite3 artifacts/verify-work/crash-rebuild.db \
  "select version,status from snapshots; select count(*) from derived_artifacts where status='staging';"
sqlite3 artifacts/verify-work/crash-inflight.db \
  "select request_id,model_key,status from inflight_bindings;"
```

Windows PowerShell：

```powershell
.\scripts\verify.ps1
.\scripts\check-sqlite.ps1 -Db artifacts\verify-work\main.db
```

## 4. HTTP 工作台复验

```powershell
go run ./cmd/server --addr 127.0.0.1:18080 --db .\data\grayrouter.db --samples-dir .\samples
```

打开 <http://127.0.0.1:18080/>，可操作样本载入、图校验、快照/增量提交、迁移清单、按模型推理、重复摘要比对、灰度裁决、请求轨迹和恢复报告。服务启动地址强制校验为 loopback；非 `127.0.0.1`/`[::1]`/`localhost` 的 `--addr` 会拒绝启动。
