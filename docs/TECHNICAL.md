# 技术说明：事务边界、并发与恢复

## 1. 数据流

```text
UDP/TCP client
    |
    v
resolver.Server  -- parse / FORMERR+EDE / TCP framing
    |
    v
resolver.Resolver -- cache lookup (single read transaction)
    |                    |
    | miss               | hit: decrement TTL from written_ms
    v                    v
scheduler.Do          DNS response
    |  global + per-server limits, priority FIFO, round budget
    |  in-flight key coalescing, deterministic long-tail detach
    v
upstreamClient.exchange -- one UDP socket or TCP connection per hop
    |  ID/question/QR/rcode validation, independent hop timeout
    v
SQLite single writer -- intention -> cache/zone transaction -> idempotency
```

监听器只负责协议边界；解析器只负责会话和缓存策略；调度器只负责并发/预算/合并；SQLite 层是唯一提交权威。

## 2. DNS wire 边界

`internal/dnsmsg/wire.go` 的解析约束：

- 域名标签长度最多 63；展开后的 wire 长度最多 255。
- 压缩指针必须指向报文内且严格向前；访问偏移去重，循环或前向/越界指针立即失败。
- 标签高两位为 `01`/`10` 是保留类型，直接失败。
- 每个 RR 的 `rdlength` 必须落在报文内；A/AAAA 长度严格为 4/16；TXT 的每个 character-string 不越过 RDATA；CNAME/NS/MX/SOA 中的压缩域名必须恰好消耗其 RDATA。
- 所有 section 解析完还有尾随字节时视为畸形，避免边界歧义。
- 编码端同样拒绝超长标签/域名、非法 IP 和过大的 TXT/帧。

UDP 解析失败时，监听器从报文前两字节尽量取回事务 ID，返回 `FORMERR`。业务码通过 EDNS0 EDE option 15 携带，例如 `E_MESSAGE_MALFORMED`。TCP 使用 2 字节大端长度前缀；声明长度后提前 EOF 或零长度帧产生 `E_TCP_FRAME_INVALID`，计入 `/api/metrics`，并立即关闭该 TCP 连接（此时没有可信的应用层应答可写）。

## 3. 缓存模型

`cache_entries` 以 `qname|qtype` 唯一，记录：

- `written_ms`、`ttl_ms`：写入时刻与原始生存期毫秒数。
- `status`：`positive`、`nxdomain`、`nodata`。
- `source_type`：正向为请求类型；负向为 `SOA`。
- `zone`、`zone_version`：源区及写入时的区版本。
- `payload_json`：rcode/answer/authority/additional 的规范化内容。

命中时 `remaining_ms = written_ms + ttl_ms - now_ms`。wire TTL 按已流逝整秒递减；剩余不足一秒但尚未过期时输出 1，避免把仍存活的条目呈现为 0。过期条目不命中，并同步按 cache id 删除。

负缓存 TTL 取 `min(SOA RR TTL, SOA.MINIMUM)`。NXDOMAIN 和“NOERROR 但没有请求 RRset”的 NODATA 均适用。CNAME 链只含 CNAME 而没有最终请求类型时仍归类为 NODATA，尽管 answer section 保留 CNAME 以便客户端诊断。若响应没有 SOA，写入拒绝并返回 `E_NEGATIVE_TTL_UNDEFINED`，错误文本包含 answer/authority 数量等诊断信息。

`cache_deps` 为缓存中每个相关 owner 和 CNAME/NS/MX target 记录 `zone + zone_version + owner + rrtype`。读取时在同一个 SQLite 读事务中同时获取缓存、源区版本和全部依赖区版本；任一版本不一致即视为未命中。这样读者不会看到“区索引已更新而缓存仍为旧值”的混合快照。

## 4. 区域提交与级联失效

### 快照

快照必须提供：

- 规范区名、正整数版本、生成时间毫秒。
- 按 owner/type/records canonical 内容计算的 SHA-256 摘要。
- 所有 RRset 的 owner 必须在区内且类型受支持。

提交前在单写者锁内检查当前版本，只有更高版本才能提交。事务内依次：

1. upsert `zones` 版本、生成时间、摘要。
2. 删除并重建该区全部 `rrsets`。
3. 删除依赖该区的缓存（源区、CNAME target 依赖，以及区创建前 zone 为空的同名缓存）。
4. 删除并重建 `zone_derived` 的两类派生行：`signature_domain` 与 `negative_range`，`refs_json` 引用区版本、source owner/type、TTL 和记录数。
5. 递增 SQLite `state.zone_epoch`。
6. 将批次、意图、幂等结果标记为完成。

以上步骤同一事务提交。读者要么看到完整旧版本，要么看到完整新版本。

### 增量

增量必须携带 `base_version`、更高的 `version`、生成时间、摘要和受影响 RRset。基版本不等于当前版本时，在写任何业务状态前返回 `E_STALE_BASE`。提交时以 `owner + type` 原子替换或删除 RRset，然后执行与快照相同的整区缓存失效、派生重建和 epoch 递增。

`writeMu` 把所有区域提交串行化。两个基于同一版本的并发增量只有一个能通过当前版本检查，另一个得到 `E_STALE_BASE`。测试同时覆盖批次 ID 相同（幂等复用）和批次 ID 不同（一胜一 stale）两种情况。

### 防止旧上游结果复活

仅靠“提交时删除缓存”仍存在竞态：查询在区提交前读取版本并向上游发出请求，区提交后旧答案才返回，若直接写缓存会被错误标记为新版本。解析器因此在发起上游前记录：

- 查询名所属最长已提交区和版本。
- 全局 `zone_epoch`。

写缓存事务再次校验两者。任何区域在查询期间提交都会使 `E_STALE_BASE` 生效，旧答案仍可服务当前请求，但不会进入缓存。该检查覆盖 CNAME 目标区与源区两类依赖。

## 5. 调度器

`scheduler` 内部状态：

- `queue`：按 `Priority` 升序、同优先级按严格递增 arrival sequence 排序。
- `activeGlobal`：全局在跑任务数。
- `activeByServer`：按权威地址计数的在跑任务数。
- `flights`：相同 key 的 leader/等待者集合。

新任务先尝试进入可启动状态，否则进入有界队列。每完成一个任务，调度器在同一把锁内释放全局和权威名额、广播 flight 结果，并从队列选择下一个“当前可启动”的最高优先级任务；被单权威上限阻塞的高优先级任务不会阻止其他权威上的低优先级任务。

队列已满且当前无法启动时立即返回 `E_UPSTREAM_QUEUE_FULL`。每个调用有本轮 deadline；排队等待使用独立计时器，超时任务从队列移除并返回 `E_ROUND_BUDGET_EXCEEDED`。

### 同键合并与长尾确定性规则

相同 `qname|qtype` 的第一个任务成为 leader，后续调用不进入队列，而是等待同一个结果。长尾决策完全确定：

1. leader 尚未启动、或已运行时间小于 `DetachAfter`：等待。
2. 等待者本轮剩余预算小于 `DetachAfter`：等待。
3. 当前没有额外全局或该权威并发名额：等待。
4. 以上均不满足：等待者作为独立任务发起自己的上游查询。

独立任务不覆盖 leader flight；其完成结果只返回给该等待者。调度统计中的 `joined` 与 `detached` 可用于观测该策略。

任务 context 只携带本轮 deadline。每跳上游超时在 `upstreamClient.exchange` 上独立施加，因此一个 CNAME 链的第一跳消耗 70ms、第二跳再消耗 70ms 时，即使单跳超时为 120ms，也不会把总耗时误判为单跳超时。

## 6. 上游真实超时与资源释放

每跳处理流程：

1. 用 `net.Dialer.DialContext` 建立 UDP connected socket 或 TCP connection。
2. 为该跳创建独立 timeout context，设置 socket deadline。
3. UDP 发送一个报文并读取一个应答；TCP 写 2 字节长度、报文，再读完整帧。
4. 校验事务 ID、QR、question、rcode；畸形或错 ID 映射 `E_UPSTREAM_BAD_RESPONSE`，REFUSED 映射 `E_UPSTREAM_REFUSED`。
5. `defer conn.Close()` 在任何成功、超时、校验失败路径都执行；函数返回后调度器才释放并发名额。

因此超时路径不存在“连接未关闭但名额已释放”的窗口。监听器把上游错误转换为 `SERVFAIL`，EDE 中带稳定错误码。

## 7. 单写者、幂等和口径一致性

所有写操作经过 `Store.writeMu`：缓存写、清理、区域快照/增量、恢复重放。SQLite 使用 WAL，读事务可并发；每个读事务固定在一个快照上。

缓存写的意图流程：

```text
tx0: INSERT intention_log(status=pending)   -- 独立提交
tx1: cache row + cache_deps + intent=committed + idempotency=completed
     COMMIT                                 -- 原子可见
```

若 tx1 未提交，tx0 的 pending 意图保留供重启扫描；若 tx1 提交，客户端和其他读者只在提交后看到缓存和幂等结果。区域批次同样使用 `zone_batches(status=pending)` 与意图先行；业务数据和完成状态在同一事务。

缓存查询 ID（`CacheWrite.QueryID`）和区域批次 ID 都有内容摘要。相同 ID + 相同摘要复用首次结果，`Idempotent=true`；相同 ID + 不同摘要返回 `E_CONFLICTING_REQUEST`。被拒绝的 stale 结果也会记录，因此同一失败批次重试保持相同的可复现结果。

## 8. 崩溃恢复

启动顺序是先 `Open` SQLite、执行 schema 和恢复，再绑定任何监听端口。

恢复扫描所有 `intention_log(status=pending)`：

- `cache_write`：删除带该 `intent_id` 的缓存行（正常崩溃时通常为 0 行），标记意图 `aborted`。
- 区域意图：查对应批次。若区表已存在 `version >= target_version` 且 checksum 匹配，认为区域事实已落盘：删除该区缓存依赖、重建派生结构、递增 epoch，标记 `committed_replayed`；否则标记批次和意图 `aborted`。

之后扫描任何没有对应意图的 pending 批次，执行同样的事实判定。最后把报告写入 `recovery_reports`，并可通过 `GET /api/recovery` 查询。

报告字段只来自 SQLite 可见行：pending 意图数、实际回滚缓存行数、中止/重放批次数、明细。限制明确列出：SIGKILL 时仍在上游或内存中的请求、未提交缓存写、未写入 SQLite 的客户端应答无法重建；SQLite 自身提交保证之外的存储损坏不在恢复范围。

测试 `TestSIGKILLCacheWriteAndZoneBatchRecovery` 通过子进程真实 `SIGKILL`：

- 缓存写：意图已落盘，数据事务未开始；重启报告 pending=1、缓存不可见。
- 区域增量：快照 v1 已落盘，v2 批次意图刚提交即被杀；重启保持 v1 并中止批次。

另有测试构造“目标区版本已落盘但批次仍 pending”的场景，验证重启将其标记为 `committed_replayed`。

## 9. 验证矩阵

| 类别 | 测试 |
| --- | --- |
| 全部 7 类 RR + 压缩指针 | `internal/dnsmsg` round-trip |
| 畸形/循环/越界/超长 | `TestMalformedPackets`、`TestMalformedRDataLengths` |
| TCP 帧不匹配 | `TestTCPFraming`、监听器指标测试 |
| 正缓存毫秒 TTL/过期清理 | `TestPositiveCacheTTLMillisAndExpiredCleanup` |
| SOA 负缓存/缺失 SOA | `TestNegativeCache...`、`TestCNAMEOnlyChain...` |
| 快照/增量/stale/并发 | `TestZoneDelta...`、`TestConcurrentDeltaSameBaseOnlyOneWins` |
| 崩溃恢复 | 真实 SIGKILL、pending batch replay 测试 |
| 队列上限/优先级/并发上限 | `internal/scheduler` 测试 |
| 同键合并/长尾分流/预算 | `internal/scheduler`、resolver 并发测试 |
| UDP/TCP 上游超时与坏响应 | resolver 子测试 |
| 每跳独立超时 | `TestEachCNAMEHopGetsIndependentUpstreamTimeout` |
| 区域竞态不复活旧缓存 | `TestZoneCommitDuringUpstreamDiscardsStaleCache` |
| UDP/TCP 监听与 FORMERR EDE | `TestUDPAndTCPListenersAndMalformedResponses` |
| 管理 API 幂等/冲突 | `internal/admin` 测试 |

## 10. 已知边界

- 仅支持题目规定的 7 类记录；未知类型只保留原始 RDATA，不作为解析目标。
- 上游使用配置列表首项；本地权威样本为单区确定性服务器。调度器本身已实现按任意 server key 的并发上限。
- 不实现根区迭代、委派、DNSSEC、EDNS 分片、DoH/DoT。
- UDP 应答未按客户端 EDNS 缓冲区裁剪；本地样本均小于 512 字节。若扩展到公网需补 TC/重试 TCP。
- 管理 API 无认证，默认仅绑定 `127.0.0.1`；不要暴露到非受信网络。
- 进程被 SIGKILL 后，SQLite 保证已提交事实；但无法还原已发送给客户端但未进入事务的状态，也无法保留内存队列中的任务。
