# pkitrust

`pkitrust` 是一套可嵌入宿主 PKI 服务的本地证书链与信任管理领域组件。它只使用 Go 标准库与 `github.com/mattn/go-sqlite3`，不访问系统根证书池、网络或真实证书源。

## 交付内容

- `truststore/`：可集成 Go 包，包含证书链重建/验证、分层信任模型、增量变更集、依赖失效闭包、SQLite 版本快照、并发控制、恢复和确定性报告。
- `cmd/pkitrust/`：一键异常场景与强制终止恢复验证 CLI。
- `examples/quickstart/`：最小嵌入式调用示例。
- `truststore/truststore_test.go`：领域、并发、版本回退、报告稳定性和强制终止恢复测试。
- `artifacts/verification/`：一次完整运行的汇总、控制台结果和关键报告样本。

## 环境准备（Windows）

要求 Go 1.22+、CGO 和可用的 C 编译器（MinGW-w64 即可）。

```powershell
go version
go env CGO_ENABLED
# 应为 1；若不是：
$env:CGO_ENABLED = "1"
gcc --version
```

新目录初始化示例：

```powershell
go mod init pkitrust
go get github.com/mattn/go-sqlite3@v1.14.28
go mod tidy
```

仓库已经包含该驱动的 `vendor/` 副本，离线环境直接构建无需再次下载。工作目录请使用不含空格和中文的路径。

## 一键运行

```powershell
go test ./...
go run ./cmd/pkitrust run -scenario all -workdir .\_pkitrust_run -out .\_pkitrust_run\summary.json
```

也可单独运行：

```powershell
go run ./cmd/pkitrust run -scenario revoke-root -workdir .\_pkitrust_run -out .\_pkitrust_run\revoke-root.json
go run ./examples/quickstart
```

`-scenario all` 会依次执行：

| 场景 | 断言重点 |
|---|---|
| `baseline` | 有效链 `OK`，断链、自签名非根、路径长度超限业务码 |
| `missing-ca` | 变更集引用缺失 CA 返回 `E_TRUST_INVALID`，版本不推进；样本断链返回 `E_CHAIN_BROKEN` |
| `revoke-intermediate` | 中间 CA 吊销后的闭包、直接/传递失效与 `E_DEP_INVALIDATED` |
| `revoke-root` | 受信根删除后全部依赖链失效，独立根链保持 `OK` |
| `tighten-scope` | 收紧根/中间 CA 白名单后依赖链返回 `E_SCOPE_DENIED`，闭包标记受影响链 |
| `cross-replace` | 交叉签名替换后重建新路径，版本递增但不判为整体替换 |
| `whole-replace` | 受信根集合完全不重叠时识别整体替换，尽管版本只加一 |
| `version-gap` | 加载返回 `E_SNAPSHOT_VERSION`，回退到最近连续版本并拒绝后续写入 |
| `contention` | 超过等待阈值的同命名空间请求返回 `E_TRUST_CONTENTION`，被拒请求不改变信任状态 |
| `recovery` | 强制终止未提交事务完全回滚；已提交事务与版本在重启后保留 |

## 作为 Go 包集成

```go
samples, _ := truststore.GenerateSamples()
content, _ := samples.InitialContent()

store, _ := truststore.Open(ctx, truststore.StoreOptions{
    DBPath:      `C:\pki-state\trust.db`,
    MaxInflight: 4,
    WaitTimeout: 250 * time.Millisecond,
})
defer store.Close()

snapshot, _ := store.Bootstrap(ctx, "production", content)

result, err := store.Apply(ctx, "production", truststore.Changeset{
    Operations: []truststore.Operation{{
        Type:   truststore.OperationRevoke,
        CertID: "dept-ca",
        Reason: "key compromise",
    }},
}, truststore.ApplyOptions{RequestID: "change-2026-001"})
if err != nil {
    switch truststore.CodeOf(err) {
    case truststore.CodeTrustInvalid:
        // 引用非法，事务已回滚
    }
}

verdict, _ := store.Verify(ctx, "production", "chain-team")
reportJSON, _ := store.ExportReportJSON(ctx, "production")
_ = snapshot
_ = result
_ = verdict
_ = reportJSON
```

主要导出入口：

- `Open` / `Close`：打开 SQLite 状态库。
- `Bootstrap`：原子创建版本 1。
- `Load` / `LoadSnapshotVersion`：加载最新连续快照或回放旧版本。
- `Apply`：单事务应用四类变更。
- `ValidateChain` / `Verify` / `VerifyAll`：纯函数或状态库链验证。
- `ExportReport` / `ExportReportJSON`：导出可复核 JSON。
- `GenerateSamples`：生成确定性的本地样本 PKI。

## 信任模型

`SnapshotContent` 包含：

- `Certificates`：完整证书记录，ID 为 DER 的 SHA-256 指纹。
- `RootIDs`：显式受信根。
- `IntermediateIDs`：显式受信中间 CA。
- `RevokedIDs` / `SupersededIDs`：吊销和交叉签名替换记录。
- `Scopes`：命名的受信根/中间 CA 白名单。
- `HostChains`：待裁决的叶证书、链候选和范围声明。

链验证只使用 `RootIDs` 作为信任锚，按 Subject/Issuer、AKID/SKID 重建路径，验证 CA 标志、KeyUsage、签名、有效期和路径长度约束。交叉签名变体拥有相同 Subject/Key/SKID，但证书指纹、Issuer 和签名不同。

业务码：

| 业务码 | 含义 |
|---|---|
| `OK` | 成功/链有效 |
| `E_CHAIN_BROKEN` | 签发关系断开或 CA 不受信 |
| `E_SELF_SIGNED_NONROOT` | 链终止于未受信的自签名证书 |
| `E_PATH_LEN_EXCEEDED` | BasicConstraints pathLen 超限 |
| `E_SIGNATURE_INVALID` | 证书签名或算法校验失败 |
| `E_CERT_EXPIRED` / `E_CERT_NOT_YET_VALID` | 不在有效期内 |
| `E_TRUST_INVALID` | 变更引用非法、操作非法或整体回滚 |
| `E_DEP_INVALIDATED` | 依赖闭包内的吊销/替换失效 |
| `E_SNAPSHOT_VERSION` | 版本链断裂、哈希不符或跳号 |
| `E_TRUST_CONTENTION` | 同命名空间串行化或在途配额等待超时 |
| `E_SCOPE_DENIED` | 路径超出受信范围 |
| `E_NOT_FOUND` | 命名空间、链或版本不存在 |

## 快照、事务与恢复

- `Bootstrap` 创建版本 1，父版本为 0。
- 每次成功 `Apply` 递增一版，写入完整内容、内容哈希、信任相关哈希、整体替换标记和审计 JSON。
- 所有变更与快照写入在同一个 `BEGIN IMMEDIATE` SQLite 事务内；任一步失败均回滚，版本不推进。
- 加载检查版本号连续、父版本链接、内容哈希和信任哈希。发现跳号时返回 `E_SNAPSHOT_VERSION`，以最近连续快照作为回退视图，并拒绝后续写入。
- 新旧受信根集合完全不重叠且均非空时，标记 `replacement=true`，即使版本号只是递增。
- 竞争拒绝记录写入 `<db>.contention.jsonl`，不写 SQLite，因此被拒请求不会改变信任状态或版本。

## 并发语义

- 同一命名空间使用独立 gate 串行化。
- 全局在途事务由容量为 `MaxInflight` 的信号量限制。
- 等待超过 `WaitTimeout`（可在单次请求覆盖）返回 `E_TRUST_CONTENTION`。
- 不同命名空间不共享 gate；SQLite 自身仍按单写者模式串行提交。
- 审计记录包含命名空间、请求 ID、等待毫秒数、结果和业务码。

## 强制终止验证

`recovery` 场景通过重新执行当前二进制完成：

1. `uncommitted`：子进程在事务内写入后写 marker，父进程 `Kill`；重启后版本仍为 1，变更完全丢弃。
2. `committed`：子进程提交后写 marker，父进程 `Kill`；重启后版本为 2，变更完整保留。

SQLite WAL/事务日志负责可见性恢复；测试不修改源码制造失败，只通过 `ApplyDiagnostics` 输入参数定位终止点。

## 报告确定性

`ExportReportJSON` 仅输出持久化快照和审计内容，不含当前时间、随机值或映射迭代顺序。快照版本、链、闭包路径、受影响链、竞争记录和业务码均稳定排序。相同输入和相同状态重复导出时字节一致；测试同时比较了独立全新状态库的输出。并发审计中的实际等待毫秒数按运行记录保存，重复导出同一状态仍稳定。

## 本地验证

```text
$ go version
go version go1.27.4 linux/amd64

$ go env CGO_ENABLED
1

$ go test ./...
ok  pkitrust/truststore

$ go test -race ./truststore
ok  pkitrust/truststore

$ go vet ./...
(no output)

$ go run ./cmd/pkitrust run -scenario all -workdir ./_pkitrust_run -out ./_pkitrust_run/summary.json
[PASS] baseline ... [PASS] recovery
summary=./_pkitrust_run/summary.json passed=true
```

完整控制台输出、场景汇总和代表报告见 `artifacts/verification/`。

当前执行环境为 Linux amd64，未安装 MinGW-w64，因此未做 Windows 交叉编译；Windows 上需保持 `CGO_ENABLED=1` 并安装 MinGW-w64。实现仅使用跨平台 Go/SQLite API，强制终止使用 `os.Process.Kill`。
