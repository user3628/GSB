# DriftLoom 实际验证记录

验证日期：2026-11-24。工作区为 `/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-200`。

## 1. 验证环境

| 项 | 实际值 |
|---|---|
| 执行系统 | Linux amd64 |
| Go | `go1.27.4 linux/amd64` |
| 模块 | `driftconverge`, `go 1.22` |
| SQLite 驱动 | `modernc.org/sqlite v1.34.5`（纯 Go） |
| 网络模式 | `GOPROXY=off GOSUMDB=off` |
| Windows 目标 | `GOOS=windows GOARCH=amd64 CGO_ENABLED=0` |
| 容器 | 未使用 Docker 或其他容器 |

说明：本会话没有 Windows 实机或浏览器界面。已实际完成 Windows amd64 无 CGO 交叉构建并检查 PE 头；Windows 原生运行需在目标机执行同一批 `driftctl` 命令。Web 控制台通过嵌入式 HTML 与 `httptest` API 测试验证，未生成截图。

## 2. 复现命令

```bash
export GOPROXY=off GOSUMDB=off

go test ./...
go test -race ./...
go vet ./...

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go build -trimpath -ldflags '-s -w' \
  -o artifacts/bin/driftctl-windows-amd64.exe ./cmd/driftctl

CGO_ENABLED=0 go build -trimpath -o artifacts/bin/driftctl-linux-amd64 ./cmd/driftctl

./artifacts/bin/driftctl-linux-amd64 demo --out artifacts/demo
./artifacts/bin/driftctl-linux-amd64 verify --out artifacts/verification.json
./artifacts/bin/driftctl-linux-amd64 stress \
  --count 20000 --fanout 6 --exact-limit 5000
```

Windows PowerShell 等价入口为 `scripts/build.ps1` 与 `scripts/demo.ps1`。

## 3. 构建与静态检查

### 3.1 离线测试

```text
?    driftconverge/cmd/driftctl        [no test files]
?    driftconverge/internal/apperr     [no test files]
ok   driftconverge/internal/batch
ok   driftconverge/internal/detect
ok   driftconverge/internal/engine
ok   driftconverge/internal/fixed
ok   driftconverge/internal/model
ok   driftconverge/internal/plan
ok   driftconverge/internal/simulator
ok   driftconverge/internal/store
ok   driftconverge/internal/web
```

结果：全部包通过。原始输出见 `artifacts/go-test.txt`。

### 3.2 竞态与静态检查

```text
GOPROXY=off GOSUMDB=off go test -race ./...
# 全部包通过，无 data race，见 artifacts/go-test-race.txt

GOPROXY=off GOSUMDB=off go vet ./...
# 无输出，退出码 0，见 artifacts/go-vet.txt
```

### 3.3 Windows 交叉构建

```text
-rwxr-xr-x 11866624 bytes  artifacts/bin/driftctl-windows-amd64.exe
PE32+ executable for MS Windows 10.00 (console), x86-64, 8 sections
sha256 61d70f71521ab297d38a90c5a8386cba1722283a7c13715a7de9f1b51202a819
```

结论：纯 Go SQLite 驱动在 `CGO_ENABLED=0` 下成功生成 Windows amd64 可执行文件，无网络下载。

## 4. 样本演示

命令：

```bash
./artifacts/bin/driftctl-linux-amd64 demo --out artifacts/demo
```

关键实际输出：

```text
analysis snapshot v7 objects=10 drift=7 path=exact reconciled=true batchVersion=1
path batch signatures: exact=sha256:4449c7323d0cbad955097538caade7d372db4ca217018c58bbb42cb73f025af0 bucketed=sha256:4449c7323d0cbad955097538caade7d372db4ca217018c58bbb42cb73f025af0 reconciled=true
batch signature=sha256:4449c7323d0cbad955097538caade7d372db4ca217018c58bbb42cb73f025af0 batches=4 clean=3 cyclic=0
cost estimate=3.436037ms budgetOccupancy=0.052582 roundingError=-0.244000
round demo-round-7 state=completed endpoint=http://127.0.0.1:35793 reused=false converged=7 unresolved=0 failed=0
idempotency replay: reused=true state=completed convergedObjects=7
refused-endpoint scenario: state=failed errorCode=E_ENDPOINT_REFUSED failedObjects=7
recovery: scanned=0 rolledBack=0 pendingWrites=0 abortedRounds=0
```

验证点：

- 10 个对象中 7 个漂移、3 个干净；四类漂移均出现。
- 精确路径与分桶路径的逐对象结论一致，批次 SHA-256 也完全一致。
- 4 个批次均满足批次上限 2 和依赖先后约束。
- 定点计划包含非零舍入传播：总误差 `-0.244000`（误差单位为 `1e-6us`）。
- 正常端点写回 7 个对象，全部读回复核收敛。
- 相同轮次 ID 重放 `reused=true`，写回记录数不增加。
- 未监听回环端口返回 `E_ENDPOINT_REFUSED`，7 个对象均记录失败。

原始产物：`artifacts/demo/analysis.json`、`execution.json`、`reused.json`、`refused.json`、`startup-recovery.json`、`recovery.json`、`summary.json` 和 `console.txt`。

## 5. 端到端验收场景

命令：

```bash
./artifacts/bin/driftctl-linux-amd64 verify --out artifacts/verification.json
```

结果：**9/9 场景 PASS，33 项检查 PASS**。

| # | 场景 | 检查数 | 实际结论 |
|---|---:|---:|---|
| 1 | `drift-batching-adaptive-convergence` | 10 | 双路径 10 对象一致；双批次签名一致；四类漂移齐全；来源冲突首要；浮点容差内不漂移；干净对象不写回；批次/依赖约束成立；舍入误差非零；7 对象写回收敛；轮次 ID 幂等复用 |
| 2 | `dependency-cycle-partial-batches` | 3 | 返回 `E_DEPENDENCY_CYCLE`；环成员为 `cycle/a,cycle/b,cycle/c`；环外 `outside/d,outside/e` 批次保留 |
| 3 | `connection-refused` | 2 | 返回 `E_ENDPOINT_REFUSED`；7 个失败写回均留痕 |
| 4 | `readback-unresolved` | 3 | HTTP 成功但 7 对象复核未消除；保留残留类别（如 `prod/config/legacy=[field_extra]`）；返回 `E_CONVERGENCE_INCOMPLETE` |
| 5 | `write-timeout` | 2 | 返回 `E_WRITE_TIMEOUT`；7 个超时对象计入失败 |
| 6 | `bounded-write-queue` | 2 | 返回 `E_WRITE_QUEUE_FULL`；已启动 1 批结果保留，6 批进入跳过清单 |
| 7 | `round-time-budget-skip` | 2 | 4 批确定性跳过，跳过顺序为 `batch-0001...batch-0004`；返回 `E_CONVERGENCE_INCOMPLETE` |
| 8 | `forced-process-crash-recovery` | 5 | 子进程以 91 强制退出；写回回滚为 `pending_writeback`；staging 批次标记 `aborted`；running 轮次中止；最后完整批次版本保留为 1 |
| 9 | `scale-adaptive-detection` | 4 | 12,000 对象选择分桶路径；双路径全部一致；双批次划分一致；生成 24 批 |

控制台原文见 `artifacts/verification-console.txt`，机器可读报告见 `artifacts/verification.json`。

## 6. 20,000 对象压力验证

命令：

```bash
./artifacts/bin/driftctl-linux-amd64 stress \
  --count 20000 --fanout 6 --exact-limit 5000
```

实际输出：

```text
stress objects=20000 fanout=6 selected=bucketed reconciled=true compared=20000
timings exact=44091us bucket=56916us drift=8492 clean=11508
  bucket source_conflict  objects=1319
  bucket value_offset     objects=4000
  bucket field_missing    objects=2858
  bucket field_extra      objects=1819
batches=25 signature=sha256:495173ad3f54f981fdb954aafb1ff7b78b5f68f396236016062e04ee9f394477 exactSignature=sha256:495173ad3f54f981fdb954aafb1ff7b78b5f68f396236016062e04ee9f394477 bucketSignature=sha256:495173ad3f54f981fdb954aafb1ff7b78b5f68f396236016062e04ee9f394477 reconciled=true
cost=315.297943ms occupancy=0.035363 rounding=-203.826000
wall=0.41 s maxrss=141168 KB
```

验证点：

- 20,000 对象在精确路径和分桶路径逐对象对账一致。
- 精确、分桶、选中三条批次签名完全一致。
- 8,492 个漂移对象生成 25 个依赖批次。
- 累计定点舍入误差 `-203.826000`，证明大规模累加没有退回浮点估算。
- 本次环境总墙钟约 0.41 秒，峰值 RSS 约 138 MiB。

原始输出见 `artifacts/stress-console.txt`。

## 7. Web 控制台验证

自动化覆盖：

- `TestDashboardAndStatusAPI`：加载真实样本、启动本地模拟器、请求 `/` 与 `/api/status`，校验产品名、快照版本、对象数、双路径一致性、批次计划和定点计划。
- `TestNonLoopbackClientRejected`：非回环客户端访问返回 HTTP 403。
- `internal/web` 包在普通测试和 `-race` 下均通过。

实际回环 smoke test：

```text
web-smoke PASS
index-bytes=17048
status-bytes=15806
status-fields: snapshotVersion objectCount pathReconciled batchPathReconciled batchSignature totalRoundingError

DriftLoom dashboard: http://127.0.0.1:18787
embedded simulator: http://127.0.0.1:36375 mode=normal
```

本次实际请求了真实监听进程的 `/` 与 `/api/status`，首页包含 `DriftLoom`，状态 JSON 包含双路径与批次一致性字段。证据为 `artifacts/web-index.html`、`artifacts/web-status.json` 和 `artifacts/web-console.txt`。

手工启动方式：

```bash
./artifacts/bin/driftctl-linux-amd64 web --addr 127.0.0.1:8787
```

浏览器打开 `http://127.0.0.1:8787` 可查看路径一致性、四类分桶、漂移明细、批次流程、定点成本、执行结果、恢复扫描和原始 JSON。当前无桌面浏览器，未提供截图。

## 8. 异常注入说明

所有异常均由启动参数、样本字段或策略字段注入，未修改源码制造失败：

| 异常 | 注入方式 |
|---|---|
| 写回超时 | 模拟器 `--mode timeout --write-delay-ms 900`，策略 `writeTimeoutMs=500`；验证器使用 80ms/220ms 隔离组合 |
| 连接拒绝 | 使用已关闭/未监听回环端口 |
| 复核未消除 | 模拟器 `--mode unresolved` |
| 队列满 | 策略 `maxParallelBatches=1`、`writeQueueCapacity=0`、`batchSizeLimit=1` |
| 预算跳过 | 策略 `roundTimeBudgetMs=1`、相对容差 0 |
| 中断恢复 | 子进程写入 pending 意图后 `os.Exit(91)`，父进程重新打开数据库执行恢复 |

## 9. 证据文件

| 文件 | 内容 |
|---|---|
| `artifacts/go-test.txt` | 离线全量测试 |
| `artifacts/go-test-race.txt` | 竞态检测 |
| `artifacts/go-vet.txt` | 静态检查（空文件表示无报告） |
| `artifacts/bin/driftctl-windows-amd64.exe` | Windows amd64 构建产物 |
| `artifacts/demo/*.json` | 演示的分析、执行、复用、拒绝连接、恢复和汇总 |
| `artifacts/verification.json` | 9 场景结构化验收报告 |
| `artifacts/verification-console.txt` | 验收控制台原文 |
| `artifacts/stress-console.txt` | 20,000 对象压力输出 |
| `artifacts/web-index.html` | 实际回环 Web 首页 |
| `artifacts/web-status.json` | 实际 `/api/status` 响应 |
| `artifacts/web-console.txt` | 嵌入式模拟器与 Web 启动日志 |

校验和：

```text
61d70f71521ab297d38a90c5a8386cba1722283a7c13715a7de9f1b51202a819  driftctl-windows-amd64.exe
a70b3fc997c2d41b6212816e9ef86ddb0f67262efdc8fd197fe08e8459093c78  verification.json
dd8738e5ad18aa28a8abbbce8be41bc4866b0062d3338d713b3549d7e0b1eead  demo/summary.json
c43727069edd7a596e56e1047db9a2904c40146422b44791aedf1cd7826f9a46  stress-console.txt
```

## 10. 未覆盖项与风险

- 本会话未在 Windows 实机启动 `.exe`，只验证了交叉构建产物格式与无 CGO 编译。Windows 原生运行仍需目标机验收。
- 未做真实 Kubernetes API Server 集成；模拟器只服务本地确定性写回/读回契约。
- 未做跨主机多写者协调；单进程写者串行化、SQLite WAL/事务和 busy timeout 是当前并发边界。
- 双路径影子计算使判定 CPU 约为单路径两倍，这是验收期一致性优先的明确取舍。

