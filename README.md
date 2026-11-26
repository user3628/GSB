# 本地回环 DNS 递归解析缓存

这是一个面向 Linux/WSL 本地回环环境的 DNS 缓存组件与验证工具。它同时监听 UDP 与 TCP 的真实 DNS 报文，向配置的本地确定性权威服务器查询 A、AAAA、CNAME、MX、TXT、NS、SOA 记录，以 SQLite 持久化正缓存、负缓存、区域版本、派生索引、意图日志与幂等结果。

默认监听地址全部在 `127.0.0.1`，不访问公网，不使用 Docker。代码使用 Go 1.22 语法，DNS 编解码与调度器只依赖标准库；SQLite 使用 `github.com/mattn/go-sqlite3`（系统 libsqlite3，CGO）。

## 已实现能力

| 要求 | 实现位置 | 关键点 |
| --- | --- | --- |
| DNS 编解码与监听 | `internal/dnsmsg`、`internal/resolver/listener.go` | 7 类记录、压缩指针、标签/域名上限、UDP/TCP、FORMERR + EDE |
| 正负缓存 TTL | `internal/store/cache.go` | 毫秒剩余 TTL、SOA 负缓存 TTL、过期清理、`E_NEGATIVE_TTL_UNDEFINED` |
| 快照/增量提交 | `internal/store/zone.go` | 单调版本、校验摘要、`E_STALE_BASE`、批次幂等 |
| 级联失效与原子重建 | `internal/store/zone.go` | 单事务更新区版本、RRset、派生索引并删除缓存依赖 |
| 并行解析调度 | `internal/scheduler` | 全局/权威并发上限、优先级队列、轮次预算、同键合并、长尾分流 |
| 上游超时/错误 | `internal/resolver/upstream.go` | 每跳独立超时、真实关闭 UDP/TCP 套接字、稳定错误码 |
| 单写者/多读者/幂等 | `internal/store` | `writeMu` 串行提交，SQLite 读事务，查询 ID/批次 ID 幂等 |
| 持久化恢复 | `internal/store/recovery.go` | 意图先行、启动扫描、未提交缓存回滚、区域批次中止或事实重放 |

## 目录

```text
cmd/dnscache       DNS 缓存服务（UDP/TCP + 管理 API）
cmd/authserver     本地确定性权威测试服务器（支持延迟/丢包/坏响应注入）
cmd/dnsprobe       结果验证工具（UDP/TCP、TTL 递减、EDE 业务码断言）
cmd/zonehash       计算快照/增量的 canonical SHA-256 校验摘要
internal/dnsmsg    DNS wire 编解码
internal/store     SQLite 缓存、区域、幂等与恢复
internal/scheduler 上游并发调度、队列、合并与预算
internal/resolver  递归 CNAME 会话、上游客户端、监听器、指标
internal/admin     JSON 管理 API
samples            本地确定性区域与增量样本
scripts            构建、运行、端到端冒烟脚本
docs/TECHNICAL.md  事务边界、并发状态机、恢复窗口与验证矩阵
```

## 环境与构建

要求 Go 1.22+、CGO 编译器、SQLite 开发头文件。Ubuntu/WSL 可执行：

```bash
sudo apt-get update
sudo apt-get install -y build-essential libsqlite3-dev sqlite3 curl
go version
go env CGO_ENABLED   # 需要 1
./scripts/build.sh
```

产物位于 `bin/`：`dnscache`、`authserver`、`dnsprobe`、`zonehash`。如 Go module 下载受限，可临时设置可用的 `GOPROXY`。

## 快速运行

终端一：启动本地权威测试服务器。

```bash
./bin/authserver \
  -zone samples/zone-v1.json \
  -udp 127.0.0.1:15354 -tcp 127.0.0.1:15354
```

终端二：启动缓存组件。端口绑定失败会阻止启动，不会静默降级。

```bash
./bin/dnscache \
  -udp 127.0.0.1:15353 -tcp 127.0.0.1:15353 \
  -admin 127.0.0.1:18080 \
  -db ./data/dnscache.db \
  -upstream 127.0.0.1:15354 \
  -snapshot samples/zone-v1.json \
  -global-upstream-limit 8 \
  -per-auth-limit 2 \
  -upstream-queue-limit 64 \
  -round-budget 2s \
  -upstream-timeout 500ms
```

查询并验证：

```bash
./bin/dnsprobe -name www.example.test -type A
./bin/dnsprobe -name alias.example.test -type A
./bin/dnsprobe -network tcp -name txt.example.test -type TXT
./bin/dnsprobe -name www.example.test -type A -repeat 2 -interval 1100ms \
  -require-noninc-ttl
```

也可以直接运行一键脚本：

```bash
make run      # 启动并等待 Ctrl-C
make smoke    # 完整回环冒烟并断言结果
```

`scripts/smoke.sh` 使用 `25353`/`25354`/`28080`，可通过 `DNS_PORT`、`AUTH_PORT`、`ADMIN_PORT` 覆盖。脚本会构建、启动两个进程、验证七类记录、缓存 TTL、NXDOMAIN/NODATA、畸形 FORMERR、区域 v1→v2 失效、陈旧增量 409、上游超时和 SQLite pending 意图。

## 主要配置

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-udp` / `-tcp` | `127.0.0.1:15353` | DNS 监听地址；传空字符串可禁用对应传输 |
| `-admin` | `127.0.0.1:18080` | 管理 HTTP 地址；传空字符串禁用 |
| `-db` | `./data/dnscache.db` | SQLite 文件 |
| `-upstream` | `127.0.0.1:15354` | 本地权威服务器；当前解析会话使用第一个地址 |
| `-upstream-network` | `udp` | 上游传输 `udp` 或 `tcp` |
| `-global-upstream-limit` | `8` | 全局上游并发名额 |
| `-per-auth-limit` | `2` | 每个权威服务器并发名额 |
| `-upstream-queue-limit` | `64` | 有界等待队列；满时 `E_UPSTREAM_QUEUE_FULL` |
| `-round-budget` | `2s` | 每轮解析调度时间预算 |
| `-upstream-timeout` | `500ms` | 每跳上游独立超时 |
| `-detach-after` | `250ms` | 合并查询的长尾独立发起阈值 |
| `-snapshot` | 空 | 启动时提交的区域快照；已有更新版本时忽略 |

## 管理 API

所有接口只应在本地回环使用，未内置管理认证。

| 方法/路径 | 用途 |
| --- | --- |
| `GET /healthz` | 就绪检查 |
| `GET /api/zones` / `GET /api/zones/{zone}` | 已提交区域版本与 RRset |
| `GET /api/zones/{zone}/derived` | 签名域与派生负缓存区间引用 |
| `POST /api/zones/snapshot` | 提交完整区域快照 |
| `POST /api/zones/delta` | 提交增量批次 |
| `GET /api/cache?name=...&type=A` | 缓存查询 |
| `GET /api/cache` | 列出缓存，可加 `include_expired=true` |
| `DELETE /api/cache?name=...&type=A` | 删除单个缓存键 |
| `POST /api/cache/cleanup` | 清理过期条目 |
| `GET /api/metrics` | DNS、缓存、调度器、SQLite 统计及最近错误 |
| `GET /api/recovery` | 最近一次启动恢复报告 |

增量示例：

```bash
curl -sS -H 'Content-Type: application/json' \
  -H 'X-Request-ID: delta-v1-to-v2' \
  --data-binary @samples/delta-v1-to-v2.json \
  http://127.0.0.1:18080/api/zones/delta
```

同一 `X-Request-ID`（或增量 JSON 的 `delta_id`）重复提交且内容一致时返回 `idempotent:true`；内容不一致返回 HTTP 409 `E_CONFLICTING_REQUEST`。基础版本不是当前版本返回 HTTP 409 `E_STALE_BASE`，且不改变任何已提交区域或缓存状态。

## 区域格式与校验摘要

快照和增量都携带 `checksum`，计算时排除 checksum 字段本身。修改样本后使用：

```bash
go run ./cmd/zonehash -file samples/zone-v2.json -out samples/zone-v2.json
go run ./cmd/zonehash -file samples/delta-v1-to-v2.json -out samples/delta-v1-to-v2.json
```

增量 `changes` 中设置 `"delete": true` 可删除一个 RRset；否则按 `owner + type` 原子替换该 RRset。任何 RRset 新增、删除或 TTL 变化都会在该区提交事务中清理依赖缓存并重建派生索引。

## 业务错误码

DNS 错误应答同时使用标准 rcode，并通过 EDNS0 EDE option 15 携带稳定业务码，例如畸形查询返回 `FORMERR` + `E_MESSAGE_MALFORMED`。管理 API 直接返回 JSON `code`。

| 业务码 | 典型场景 |
| --- | --- |
| `E_MESSAGE_MALFORMED` | 截断、非法压缩指针、超长标签/域名、RDATA 长度错误 |
| `E_TCP_FRAME_INVALID` | TCP 长度前缀声明与实际帧不一致；连接关闭并计入指标 |
| `E_NEGATIVE_TTL_UNDEFINED` | NXDOMAIN/NODATA 没有 SOA，无法得到负缓存 TTL |
| `E_STALE_BASE` | 增量基础版本过期，或查询期间区域已变更 |
| `E_CONFLICTING_REQUEST` | 相同查询/批次 ID 携带不同内容 |
| `E_UPSTREAM_QUEUE_FULL` | 有界上游等待队列已满 |
| `E_UPSTREAM_TIMEOUT` | 单跳上游真实超时；套接字已关闭、并发名额已释放 |
| `E_UPSTREAM_REFUSED` | 连接被拒绝或权威返回 REFUSED |
| `E_UPSTREAM_BAD_RESPONSE` | 坏事务 ID、畸形响应、非法 framing、异常 rcode |
| `E_ROUND_BUDGET_EXCEEDED` | 本轮调度预算耗尽 |
| `E_UNSUPPORTED_TYPE` | 非 A/AAAA/CNAME/MX/TXT/NS/SOA 查询 |
| `E_CHECKSUM_REQUIRED` / `E_CHECKSUM_MISMATCH` | 区域批次摘要缺失或不匹配 |

## 持久化与恢复

SQLite 在 WAL 与 `synchronous=FULL` 下运行。核心表：

- `cache_entries` / `cache_deps`：缓存结果与区域/RRset 依赖。
- `zones` / `rrsets`：已提交区域版本和 RRset 索引。
- `zone_derived`：签名域、负缓存区间及版本引用。
- `intention_log`：意图先行记录，状态为 `pending`、`committed`、`replayed` 或 `aborted`。
- `idempotency` / `zone_batches`：查询/批次幂等结果与批次生命周期。
- `recovery_reports`：每次启动扫描的事实报告。

写缓存的顺序是：在独立事务提交 `pending` 意图 → 在一个数据事务中写缓存、完成意图、写幂等结果并提交。因此不存在“缓存已可见但意图未落盘”的窗口。区域提交同理，区版本、RRset、派生索引、缓存失效、批次/意图/幂等结果处于同一事务边界。

启动时只依据 SQLite 已落盘事实恢复：未完成缓存意图对应的缓存行被删除并标记中止；未完成区域批次若目标版本和摘要均已落盘则重建派生结构并标记 `committed_replayed`，否则标记中止。报告不猜测内存中的上游请求或尚未提交的写。

## 实际验证结果

在 2026-11-26 的本机 WSL Ubuntu 环境（Go 1.27.4 编译，模块目标 Go 1.22；SQLite CLI 3.46.1）执行：

```bash
go test ./... -count=1 -timeout=120s
go test -race ./internal/... -count=1 -timeout=180s
./scripts/smoke.sh
```

结果：

- 全部存储、DNS、调度、解析与管理 API 测试通过，`-race` 无数据竞争报告。
- 冒烟查询 A/AAAA/CNAME/MX/TXT/NS/SOA 与 TCP 均成功。
- 同一缓存键 1.1 秒后 TTL 从 `60` 变为 `59`，未访问权威。
- NXDOMAIN、NODATA 使用 SOA minimum `5s` 负缓存；NODATA 应答 0 条 answer。
- 畸形压缩指针得到 `FORMERR` + EDE `E_MESSAGE_MALFORMED`。
- 区域 v1 的 `www` 为 `192.0.2.10`；提交 v1→v2 增量并切换权威后返回 `192.0.2.11`。
- 基础版本 `1` 的陈旧增量返回 HTTP 409 `E_STALE_BASE`，区域仍保持版本 `2`。
- 丢弃应答权威使查询在约 300ms 后返回 `SERVFAIL` + EDE `E_UPSTREAM_TIMEOUT`。
- 冒烟结束 SQLite 统计：`zones=1`、`pending_intents=0`、`committed_batches=2`。
- 测试内使用真实子进程 `SIGKILL`：缓存写中断仅留下 pending 意图，重启后缓存不可见；区域增量中断保持旧版本并中止批次。

## 适用边界

本交付针对题目规定的本地确定性权威环境：解析会话直接查询配置的本地权威服务器并沿 CNAME 继续，不实现根区迭代、委派发现、DNSSEC 验证、DoH/DoT、EDNS 分片或公网递归。管理 API 默认无认证，只能绑定回环地址。恢复保证覆盖 SQLite 已提交事实，不覆盖 SIGKILL 时仍在内存中的上游请求、客户端应答或未提交事务。
