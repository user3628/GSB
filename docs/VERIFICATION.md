# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）

环境：

- `go version go1.27.4 linux/amd64`
- SQLite：`modernc.org/sqlite v1.29.10`，纯 Go 驱动，无 CGO、无外部数据库进程
- 依赖通过 `GOPROXY=https://goproxy.cn,direct go mod tidy` 预取；服务运行期不访问外部网络
- 未使用 Docker 或其他容器

完整原始输出：

- `docs/test-output.txt`
- `docs/demo-output.txt`

## 静态检查与单元测试

执行：

```bash
go vet ./...
go test ./... -count=1
go test -race ./internal/engine -count=1
```

实际结果：

```text
vet: PASS
ok  nonceguard/internal/engine  0.430s
ok  nonceguard/internal/engine  2.614s   # -race
```

测试覆盖：

- 非法 Nonce、非正权重、超大整数溢出和整批解析失败。
- 十条固定样本的全部预期业务码。
- 偏移早到/晚到所附 offset 与 boundary。
- Nonce 重放的首次占用时刻和剩余窗口。
- 窗口左闭右开、精确到期回收和过期回收。
- 配置版本陈旧、跳号、摘要损坏及旧配置继续可用。
- 快速模式采样率、未预检状态、不写持久化表和快速/完整对比。
- 8 个并发相同批次下的单写者串行与请求结论数量。
- `batch_interrupt` 后的意图恢复、批次状态和关闭重开数据库。

## 端到端演示

执行：

```bash
./scripts/demo.sh
```

脚本实际选择空闲回环端口，构建 `cmd/gate` 与 `cmd/peer`，完成以下断言：

```text
codes: OK, OK, E_SIGNATURE_MISMATCH, E_TIMESTAMP_EARLY, E_TIMESTAMP_LATE,
       E_NONCE_REPLAYED, OK, OK, E_CONFLICTING_REQUEST, OK
comparison: selected=6 match=6 not_prechecked=4
decisions=8 occupancy=4
issues: 3
E_STALE_SNAPSHOT / E_CONFIG_VERSION_GAP
timeout  E_PEER_TIMEOUT elapsed≈250
refused  E_PEER_REFUSED elapsed=0
late     E_PEER_LATE_ACK elapsed=801
E_BATCH_INTERRUPTED open_intents= 10
```

强制终止恢复片段：

```text
== 7/7 SIGKILL 强制终止、重启恢复与续跑 ==
已对服务进程执行 kill -9
run_id=3 open_intents=120 aborted_batches=1
 - found 120 intent row(s) in status=open
 - marked 1 batch job(s) interrupted_recovered
 - removed 0 signature, 0 occupancy, 0 decision orphan row(s) tied to open intents
 - last complete active config version is 2
 - committed SQLite transactions remain durable; uncommitted force-killed transactions were replayed/rolled back by SQLite
resumed decisions=120 accepted=120
Demo passed.
```

`removed 0 ... orphan row(s)` 是预期结果：裁决事务和意图完成状态在同一个 SQLite 事务中提交；`kill -9` 发生在事务提交前时，SQLite/WAL 已回滚未提交的签名、占用和结论写入，只留下先前已持久化的 open intent。

## 额外手工 HTTP 验证

- `GET /` 返回完整交互控制台，抽取页面 JavaScript 后 `node --check` 通过。
- gate 内嵌 `/peer/v1/signature-check` 与 `/peer/v1/nonce-query` 可真实响应延迟 ACK。
- `POST /v1/nonces/reclaim?as_of_ms=1795507800000` 在窗口右端点回收 4 条占用；更早时刻回收 0 条，验证右开区间。

## 复核入口

```bash
./scripts/demo.sh --stay
# 打开脚本输出的 http://127.0.0.1:<port>/
```

也可使用固定默认端口：

```bash
go run ./cmd/gate
open http://127.0.0.1:18090/
```
