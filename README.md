# 离线 TLS 握手与证书链审计工作台

这是一个仅使用 Go 标准库实现的本地 PKI 审计工作台。输入为确定性样本目录，输出为可字节级重复比对的 JSON 报告。审计过程不访问网络，不读取系统根证书，`roots.pem` 是唯一信任锚来源。

## 交付内容

- `internal/samplegen`：固定种子的 Ed25519 证书与畸形样本生成器。
- `internal/sampleio`：`chain.pem`、`roots.pem`、`meta.json` 加载与输入摘要。
- `internal/engine`：链拓扑、路径约束、扩展、SNI、TLS 版本、签名算法与 `x509.Verify` 审计。
- `internal/scheduler`：固定并发上限的工作池、深度加权队列和确定性调度计量。
- `internal/checkpoint`：逐样本快照、递增版本、原子落盘、恢复与 `E_SNAPSHOT_GAP` 检测。
- `internal/workbench`：预算准入、全局降级、计数守恒与规范 JSON 报告。
- `internal/injection`：四类异常的参数化注入与断言入口。
- `cmd/pkiaudit`：`audit`、`generate`、`verify-injections` 三个命令。

## 环境

要求 Go 1.22 或以上。本工作区实际使用 `go1.27.4 linux/amd64` 验证；代码未使用第三方依赖。

```bash
go version
go mod init internalpki/workbench   # 本仓库已初始化，无需重复执行
go test -count=1 ./...
```

如果本机未把 Go 放入 `PATH`，可将本工作区下载的工具链加入路径：

```bash
export PATH="$PWD/.tools/go/bin:$PATH"
```

## 快速运行

生成确定性样本（13 个：5 个正常样本、8 个异常样本）：

```bash
go run ./cmd/pkiaudit generate --out samples --anomalies=true
```

执行正常审计：

```bash
go run ./cmd/pkiaudit audit \
  --samples samples \
  --report artifacts/audit-report.json \
  --checkpoint artifacts/checkpoint.json \
  --concurrency 4 \
  --sample-cpu 5ms \
  --global-cpu 500ms \
  --memory 134217728 \
  --at 2030-01-01T00:00:00Z
```

执行异常注入验证：

```bash
go run ./cmd/pkiaudit verify-injections \
  --out injection-samples \
  --report artifacts/injection-report.json
```

`audit` 支持直接以 `pkiaudit --samples ...` 形式调用；未写子命令时会自动补为 `audit`。

## 样本格式

每个样本目录必须包含：

```text
chain.pem   # 叶证书在前，随后是中间 CA，可选包含根 CA
roots.pem   # 唯一受信根集合
meta.json   # 期望 SNI、目标 TLS 版本、服务端声明版本和期望结果
```

示例 `meta.json`：

```json
{
  "sni": "svc.normal-chain-depth-2.internal",
  "tls_version": "tls1.3",
  "server_tls_versions": ["tls1.2", "tls1.3"],
  "expected_result": "OK",
  "fixture_schema": "pki-audit-fixture/v1",
  "certificate_profile": "ed25519-fixed-seed"
}
```

正常样本中包含根直签、1/2/3 个中间 CA，以及一个 `chain.pem` 不包含根的样本，用于验证从 `roots.pem` 补齐完整叶到根链。异常样本固定包含需求中的四类：

- `anomaly/missing-intermediate` → `E_CHAIN_BROKEN`
- `anomaly/sni-mismatch` → `E_HOSTNAME_MISMATCH`
- `anomaly/expired` → `E_EXPIRED`
- `anomaly/untrusted-root` → `E_CHAIN_UNTRUSTED`

另外提供 `E_EXT_KEY_USAGE`、`E_UNTRUSTED_SELF_SIGNED`、`E_PATH_LEN_EXCEEDED`、`E_TLS_VERSION_MISMATCH` 的扩展样本。

## 审计核心

验证使用 `crypto/x509` 的内存等价流程，而不是实际网络握手，因为样本不包含服务端私钥：

- `x509.VerifyOptions.Roots` 仅指向样本 `roots.pem` 构造的池。
- 中间证书池只来自样本链，不加载系统池。
- `DNSName` 使用 `meta.sni`，`KeyUsages` 强制 `ExtKeyUsageServerAuth`。
- 固定 `--at` 作为验证时刻，不使用墙钟。
- 失败结果保留失败证书的 Subject 与序号。

链拓扑在 `x509.Verify` 前独立审计：

- 相邻证书 issuer/subject 是否衔接，签名是否有效。
- 根是否存在于受信集合；链未携带根时只从 `roots.pem` 查找签发根。
- 中间 CA 与根是否 `CA:TRUE`。
- 是否违反 `pathLenConstraint`。
- 输入链序与追加的受信根统一输出为叶到根摘要。

握手参数审计包括：

- 目标 TLS 版本是否在 `server_tls_versions` 中。
- 叶证书 SAN 是否匹配 SNI。
- 链中每个签名算法是否与签发者公钥类型兼容。
- 叶证书 EKU、KeyUsage、未知关键扩展是否适合 TLS 服务端用途。

结构化偏差包含 `sample_path`、`code`、`field`、`expected`、`actual`、`detail` 和失败证书身份。相同 `code + field + 证书身份` 的偏差会在序列化前合并，因此不会被重复计数。

## 调度与预算模型

参数：

- `--concurrency`：工作池上限，超出的任务留在队列中，不会拒绝。
- `--sample-cpu`：单样本 CPU 预算。
- `--global-cpu`：批次总 CPU 预算。
- `--memory`：批次总内存预算（字节）。

为了满足“输入不变时报告字节级一致”，预算采用确定性归一化计量，而不是墙钟采样：

```text
CPU units    = 120 + 80 * chain_depth
Memory bytes = 32768 + 16384 * chain_depth
```

Duration 参数向上取整为微秒等价的 CPU units。该模型避免同一输入因机器负载不同而得到不同的 `E_SAMPLE_BUDGET_EXCEEDED` 结论，同时所有预算消耗和等待计量均可审计。

准入规则：

1. 单样本估算 CPU 超过 `--sample-cpu`：直接标记 `E_SAMPLE_BUDGET_EXCEEDED`，状态为 `budget_terminated`，CPU/内存计量值归零，表示资源已释放。
2. 剩余样本按链深度从浅到深尝试占用全局 CPU/内存预算。
3. 一旦某个样本无法装入，当前及后续更深样本标记 `E_GLOBAL_BUDGET_EXHAUSTED`，但仍作为完整样本行进入报告。
4. 实际执行队列按链深度从深到浅、路径升序调度；等待计量由前置深度权重累计得到，因此不随并发度变化。

报告汇总始终满足：

```text
completed + failed + degraded == total
```

## 检查点与恢复

检查点文件包含全局 `generation`、连续 `commit_log` 和逐样本快照。样本状态为：

```text
pending / verifying / completed / failed / budget_terminated
```

- 初始快照版本为 1；每次 `verifying` 或终态提交递增全局版本。
- `completed`、`failed`、`budget_terminated` 中已提交且输入摘要相同的样本会被恢复。
- `pending`、`verifying` 和输入摘要变化的样本整体重做。
- `commit_log` 必须严格为 `1..generation`；跳号、重复路径、无效版本或终态缺少结果会输出 `E_SNAPSHOT_GAP`（格式错误输出 `E_SNAPSHOT_INVALID`），并停止依赖该检查点恢复。
- 检查点通过临时文件加 rename 替换；Windows 下使用兼容的替换分支。

## 确定性保证

- 报告不含绝对路径、墙钟时间、并发度或随机 ID。
- `sample_marker` 固定为 `deterministic-sample-v1`。
- 样本按相对路径排序，失败业务码作为第二排序键。
- findings、diagnostics、SAN、EKU、指纹等切片均规范排序。
- 所有结构均为定序 struct，不使用 map 参与 JSON 序列化。
- JSON 编码使用两空格缩进并以单个换行结尾。

## 实际验证结果

本工作区的 `artifacts/` 保存了最终运行记录：

| 验证项 | 结果 |
| --- | --- |
| `go test -count=1 -v ./...` | 全部通过，见 `artifacts/test-output.txt` |
| 正常审计 | total=13, completed=5, failed=8, degraded=0, findings=8, accounted=true |
| concurrency=4 报告 | SHA-256 `0710f0251b3c5ffb28c7259df3434b51889e173ca09c96037c392bd0fd9d8495` |
| concurrency=1 报告 | 与 concurrency=4 字节级一致，同 SHA-256 |
| concurrency=8 检查点恢复报告 | 与首次报告字节级一致，同 SHA-256 |
| 完整恢复后检查点 | 未被追加提交，文件字节不变 |
| 全局预算 1000us | total=13, completed=1, failed=2, degraded=10, accounted=true |
| 单样本预算 1ns | total=13, failed=13，全部 `E_SAMPLE_BUDGET_EXCEEDED`，计量值归零 |
| 四类异常注入 | `injection checks: passed=true`，正常控制样本全部通过 |
| 快照跳号测试 | 输出 `E_SNAPSHOT_GAP`，随后对全部样本重新审计 |
| 并发上限测试 | 18 个任务、上限 3，峰值不超过 3，全部任务完成 |

主要产物：

- `artifacts/audit-report.json`：正常审计报告。
- `artifacts/budget-report.json`：全局预算降级报告。
- `artifacts/sample-budget-report.json`：单样本预算释放报告。
- `artifacts/injection-report.json`：异常注入后的同管线审计报告。
- `artifacts/injection-checks.txt`：四类异常、正常控制与计数断言。
- `artifacts/test-output.txt`：完整测试输出。

## 备注

- 未使用 Docker 或其他容器。
- 未添加第三方依赖，未访问真实站点。
- `samples/` 与 `injection-samples/` 中的 PEM 是固定的本地测试夹具，可由 `generate` 或 `verify-injections` 随时重建。
