# 实际验证记录

验证日期：2026-11-24（Asia/Shanghai）  
执行环境：WSL2 Ubuntu 26.04 / Linux x86_64 / 非 root 用户 `zsl`  
最低工具链验证：Go 1.22.12  
交叉编译目标：`windows/amd64`，`CGO_ENABLED=0`  
依赖来源：仓库内 `vendor/`，`GOPROXY=off`，无网络

> 当前宿主未启用 Windows 互操作（`cmd.exe` 不存在），无法直接启动 Windows 可执行文件。Unix 权限位探测已真实执行；Windows `icacls` 适配代码已完成交叉编译。该限制不影响离线测试、版本链、闭包、报告和注入链路验证。

## 1. 离线测试

```bash
export PATH=/home/zsl/goroot/go/bin:$PATH
export GOTOOLCHAIN=local GOFLAGS=-mod=vendor GOPROXY=off GOSUMDB=off CGO_ENABLED=0
go test ./...
```

结果：

```text
ok  sandboxpolicy/engine   0.413s
ok  sandboxpolicy/policy   (cached)
ok  sandboxpolicy/probe    0.016s
ok  sandboxpolicy/report   0.055s
ok  sandboxpolicy/tests    0.186s
```

附加静态与竞态检查均通过：

```text
go vet ./...                         # 无输出
go test -race ./policy ./engine ./report ./tests
ok  sandboxpolicy/policy  1.025s
ok  sandboxpolicy/engine  1.550s
ok  sandboxpolicy/report  1.081s
ok  sandboxpolicy/tests   1.314s
```

测试覆盖：

- 数组逆序、大小写与集合顺序变化后规范形/摘要不变。
- 重复标识、引用缺失、规则依赖环、命名空间越权、动作层不匹配、目录层级非法均返回具体条目。
- 删除目录的直接/传递闭包、进程组收紧闭包、历史闭包读取与规则溯源。
- 事务失败回滚、旧快照保留、跳号回退、同版本摘要替换拒绝。
- 同名并发串行化、在途上限、队列/冲突事件、写锁等待阈值。
- 真实文件读写与子进程启动探测、隔离目录清理、依赖失效与探测失败分类。
- Windows 盘符根路径规范化、变更集操作标识裁剪，以及已取消/超时进程探测归入 `E_PROBE_FAILED`。
- 报告字节级稳定和五类外部异常注入。

## 2. 构建与交叉编译

```bash
go build -o artifacts/sandboxctl-go122 ./cmd/sandboxctl
GOOS=windows GOARCH=amd64 go build -o artifacts/sandboxctl-go122.exe ./cmd/sandboxctl
```

结果：Windows 可执行文件成功生成，大小 `10,232,320` 字节；SQLite 为纯 Go 驱动，无 CGO。

此外执行 `GOOS=windows GOARCH=amd64 go test -c -o probe.test.exe ./probe`，已成功生成探测测试二进制，确认 `icacls` Windows 实现分支可交叉编译。

完整脚本 `./scripts/verify.sh` 也已通过，输出：

```text
[1/8] offline unit and integration tests
[2/8] offline Linux build and Windows cross-build
[3/8] baseline and four incremental snapshots
[4/8] historical closure and dependency trace
[5/8] real file/process probes and byte-stable report
[6/8] four malformed/conflict input injections
[7/8] version-gap fallback and digest replacement
[8/8] interactive shell smoke test
verification OK
```

## 3. 基线到版本 5

产物目录：`artifacts/verification-go122/`。

```bash
./sandboxctl-go122 init  --db state.db --policy samples/policies/baseline.json
./sandboxctl-go122 apply --db state.db --changeset samples/changes/01-add-cache.json
./sandboxctl-go122 apply --db state.db --changeset samples/changes/02-delete-directory.json
./sandboxctl-go122 apply --db state.db --changeset samples/changes/03-tighten-process.json
./sandboxctl-go122 apply --db state.db --changeset samples/changes/04-delete-registry.json
./sandboxctl-go122 current --db state.db
```

连续版本链（`current.json`）：

| 版本 | 父版本 | 摘要 |
| --- | --- | --- |
| 1 | 0 | `05def53c8e6e6638542d0a28d74302a9d89cd21dc93699821ba3a487af24cc52` |
| 2 | 1 | `db66ed13981d651d34493b481c4a560e9026a36dcc9445b2a8858186acdda0b1` |
| 3 | 2 | `8165f46804d7b03a2ecdf430fa62ae57b8c342dedf764d2f6605219e1691e5d0` |
| 4 | 3 | `c31f98a1ebb4dfb8ebd529b8d35fc5b5ad6a2f8751f4b77da2726cdffab2e8bc` |
| 5 | 4 | `a845131fcc372d9b3bc870f768d7ab014533003075f96b3ff58cf78e48274e77` |

当前版本为 5，无版本链警告。

## 4. 失效闭包与溯源

版本 3 删除 `dir.data` 后：

- 直接失效：`rule.file.data.read`、`rule.file.data.write`，业务码 `E_DEP_INVALIDATED`。
- 传递失效：`rule.file.audit.read`、`rule.file.cache.read`。
- 规则依赖传递链示例：`rule.file.audit.read -> rule.file.data.read -> dir.data`。

版本 4 收紧 `pg.trusted`（移除 `tool`）后：

- 直接失效：`rule.proc.trusted`，业务码 `E_DEP_RESTRICTED`。
- 传递失效：`rule.proc.audit`。
- 规则依赖传递链：`rule.proc.audit -> rule.proc.trusted -> pg.trusted`。

上述结果保存在 `closure-v3.json`、`closure-v4.json`。`state_issues` 在版本 4 仍保留版本 3 遗留的 4 条 `E_DEP_INVALIDATED`，证明历史失效可由当前快照重新发现。

单规则溯源 `trace-v3.json` 输出 `rule.file.audit.read -> rule.file.data.read -> dir.audit -> dir.root`，规则依赖与实体父链均可见。

## 5. 真实探测

版本 2 同时包含允许和拒绝的文件读写/进程启动边界：

```text
total: 10, matched: 10, mismatched: 0, failed: 0, skipped: 0
business_codes: []
```

其中 8 条为文件/进程真实探测（基线 5 个文件规则 + 3 个进程规则），版本 2 新增 2 个文件规则，因此 10 条；2 条注册表规则按需求不执行真实写入。

拒绝探测使用 Unix 权限位：文件读 `0000`、文件写 `0444`、可执行副本 `0644`；结束后恢复并删除独立临时根目录。测试确认临时父目录无残留。探测自身不可实施的拒绝（例如 root 绕过权限位）会归入 `E_PROBE_FAILED`，不会写成策略匹配。

## 6. 确定性报告

```bash
./sandboxctl-go122 report --db state.db --version 2 --out report-v2.json
./sandboxctl-go122 report --db state.db --version 2 --out report-v2-repeat.json
cmp report-v2.json report-v2-repeat.json
sha256sum report-v2.json report-v2-repeat.json
```

结果：

```text
ab3554fa1fdbf49174a482255f714186ce4b07eab1a6c3577b44872001660f95  report-v2.json
ab3554fa1fdbf49174a482255f714186ce4b07eab1a6c3577b44872001660f95  report-v2-repeat.json
```

报告为 3632 字节，包含截至版本 2 的摘要链、变更闭包、状态问题、受影响规则、10 条探测结果与空业务码数组。两次独立导出字节完全一致。

## 7. 异常注入

每类注入均从新基线开始，失败后再次读取 `current` 并与注入前摘要比较。证据位于 `artifacts/verification-go122/injections/results.txt`。

独立畸形策略 `samples/invalid/policy-duplicate-id.json` 在 `init` 阶段返回退出码 2、业务码 `E_POLICY_MALFORMED`、原因 `duplicate_identifier`，状态库没有留下版本 1 快照。

```text
missing-reference.json        rc=2 expected=2 committed_version=1 code=E_POLICY_MALFORMED
dependency-cycle.json         rc=2 expected=2 committed_version=1 code=E_POLICY_MALFORMED
cross-namespace-reference.json rc=7 expected=7 committed_version=1 code=E_NAMESPACE_SCOPE
delete-then-add-conflict.json rc=6 expected=6 committed_version=1 code=E_CHANGE_CONFLICT
version-gap                    apply_rc=3 fallback_version=2 warning=E_SNAPSHOT_VERSION
```

此外，`inject digest-mismatch --version 2` 后：

- 当前版本回退到 1。
- 版本链警告为 `E_SNAPSHOT_VERSION`，消息为 `version 2 content digest mismatch`。
- 显式读取版本 2 返回退出码 3。
- 版本 1 摘要保持不变。

所有前四类失败应用均未推进终端版本，也未改写版本 1 的已提交内容。版本跳号注入后，后续应用被 `E_SNAPSHOT_VERSION` 阻止，连续前缀仍为 1、2。

## 8. 并发行为

自动化测试并发提交 12 个同命名空间请求，在途上限 3、等待阈值 2 秒：

- 1 个请求成功提交版本 2。
- 11 个请求因过期基线返回 `E_CHANGE_CONFLICT`。
- 事件表包含 `queued` 与 `conflict`，且无交错版本。
- 连续链仍为 1 → 2。

另一测试持有 SQLite 写锁并配置 80ms 等待阈值，应用在约一个短 busy 周期内返回 `E_CHANGE_CONTENTION`，版本仍为 1，证明不会无限阻塞或部分提交。

## 9. Windows 限制说明

已用 Go 1.22.12 与 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0` 编译主程序和探测测试二进制。Windows 探测实现：

- 使用独立临时目录。
- 文件/进程拒绝通过 `icacls <fixture> /deny <current-user>:(R|W|RX)` 实施。
- 探测结束前 `/remove:d` 恢复 ACE。
- 只向隔离目录中的夹具传递路径。

由于本宿主没有 `cmd.exe`/Windows 互操作，无法记录 Windows 本机的 `icacls` 运行结果；这是本次验证唯一未实机执行的项目。建议在 Windows 命令行执行 `go test ./...` 与 `sandboxctl.exe report --db <db> --version 2 --out report.json` 完成实机复核。

