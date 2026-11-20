# 技术说明：报文一致性与恢复边界

## 1. 为什么不回放缓存报文

缓存保存的是 `Record` 语义对象，不保存带压缩指针和固定 OPT 的完整 UDP 报文。原因是同一语义结果在不同客户端条件下必须重新编码：

- 客户端 A 无 EDNS，UDP 上限 512；
- 客户端 B 带 EDNS 1232 或 4096；
- 客户端 C 使用 TCP；
- 客户端 D 使用大小写混写查询名。

若回放旧报文，会出现错误的 TC、错误的 OPT 载荷、错误 ID/QUESTION 或把 UDP 截断报文返回给 TCP 客户端。`render_response()` 每次重新生成 header、question、RR、OPT 和压缩指针。

## 2. 截断策略

UDP 完整编码超过上限时，当前实现不保留部分 RR。返回报文包含：

- 原 ID、RD；
- `QR=1`、`RA=1`、`AA=0`、`TC=1`；
- 原 question；
- 如客户端带 EDNS，则包含回显 OPT。

这样避免客户端观察到不完整 RRset。TCP 查询重新从语义对象编码，完整返回全部 CNAME 与最终记录。

## 3. CNAME 链与 TTL 独立递减

示例：

```text
chain.example.test. 300 CNAME mid1.example.test.
mid1.example.test.  120 CNAME mid2.example.test.
mid2.example.test.   30 A     192.0.2.44
```

缓存写入时刻记录 `written_at`；命中时对每条记录分别计算：

```text
remaining = max(0, original_ttl - floor(now - written_at))
```

整个缓存版本在 `min(original TTL)` 到期时失效。这样既不会用链首 300 秒覆盖链尾 30 秒，也不会让过期链尾继续返回。

## 4. 负缓存规则

本任务要求 NXDOMAIN 与 NODATA 使用不同负缓存 TTL 来源：

- NXDOMAIN：`SOA.MINIMUM`；
- NODATA：`min(SOA.TTL, SOA.MINIMUM)`。

负缓存 payload 记录 RCODE、CNAME 前缀、SOA 和负 TTL。命中时：

- 保持 NXDOMAIN 或 NOERROR-NODATA 语义；
- CNAME RR 按自身 TTL 递减；
- SOA TTL 替换为负缓存剩余 TTL。

## 5. SQLite 状态机

表：

- `resolution_sessions`：一次 leader 解析的生命周期、链进度、最后上游名、错误码；
- `cache_commit_intents`：`pending -> committed/abandoned` 的提交意图；
- `cache_entries`：完整语义缓存版本，外键指向 session 与 intent；
- `recovery_reports`：每次启动恢复的事实报告。

提交顺序刻意分成两个事务：

1. 创建解析会话时立即提交 `pending` intent。若进程在链式解析中途死亡，重启可以证明“曾准备写入但未完成”；
2. `BEGIN IMMEDIATE` 后同时写 cache row、置 intent committed、置 session committed，再 COMMIT。

内存可见性只在第 2 步成功之后更新，因此读线程看不到部分链。

## 6. 恢复判定

恢复不根据名称猜记录，也不尝试重新查询。它只处理数据库行：

- running session -> interrupted；
- pending intent -> abandoned；
- 未同时满足 committed session + committed intent 的 cache row -> 删除；
- 已过期 row -> 删除；
- committed 数据 -> 保留。

因此即使缓存 payload 来自旧版本，恢复报告仍只陈述“哪些持久化记录被清理/标记”。

## 7. 套接字生命周期

`UpstreamClient` 每个上游交换创建独立 socket，`finally` 中无条件 `close()`：

- UDP 超时后关闭；
- TCP connect/read/send 任一阶段超时后关闭；
- 上游 TC 触发 TCP 回退时，两个 socket 分别关闭；
- 统计 `sockets_opened`、`sockets_closed`、`active_sockets`，演示中验证活动套接字回零。

每跳使用自身 deadline；不会通过“继续 sleep 等应答”冒充已经终止的请求。

## 8. 并发的两把边界

- `RecursiveResolver._inflight`：同一规范化键只有一个 leader；
- `CacheStore._writer_lock`：所有会话/意图/缓存写入串行；
- `CacheStore._mem_lock`：缓存快照原子替换；
- `CacheStore._db_lock`：同一 SQLite 连接的多线程访问串行化。

waiter 只复用语义 `Resolution`，不会复用 leader 的请求 ID、原始报文、EDNS 载荷或传输协议。

## 9. 业务码的线上表达

DNS 标准响应只有 4-bit RCODE（EDNS 再扩展 8-bit），不存在任意业务字符串字段。本项目采用三层表达：

1. 标准 DNS RCODE：FORMERR、SERVFAIL、NOTIMP 等；
2. 请求带 EDNS 时，响应 OPT option `65001` 携带 ASCII 业务码；
3. 无 EDNS/结构性畸形请求通过结构化日志和 `/stats.errors` 暴露业务码。

这样既让本地测试客户端能读取业务码，又不向没有 EDNS 的客户端注入非标准附加记录。

## 10. Windows 回环注意点

- 所有监听地址默认 `127.0.0.1`，不绑定 `0.0.0.0`；
- UDP 与 TCP 使用同一高位端口，`SO_REUSEADDR` 由 `socketserver` 设置；
- 每个请求由 daemon 线程处理，主进程退出不会无限等待慢上游；
- 强制终止使用 `Popen.kill()`：Windows 走 `TerminateProcess`，POSIX 走 `SIGKILL`；
- SQLite 使用 WAL，重启时由 SQLite 自行回放/丢弃未完成事务，再由应用层扫描 running/pending 记录；
- 演示端口固定，启动前同时探测 UDP/TCP/HTTP 端口；若被占用会明确失败，不会静默换端口。
