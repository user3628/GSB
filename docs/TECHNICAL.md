# 技术设计与不变量

## 模块边界

```text
UDP/TCP DNS listeners ─┐
HTTP diagnostics ──────┼─> DiagnosticService ─> Resolver ─> UpstreamClient ─> loopback authorities
                       │          │                 │
                       │          ├─ FlightRegistry / ConcurrencyGate
                       │          └─ Storage (SQLite WAL, single writer)
                       └─ Authority fixture (UDP/TCP)
```

`DiagnosticService` 是唯一编排入口。HTTP 与 DNS 线协议只负责输入解析和输出编码，不直接修改缓存或会话。

## 单写者与提交版本

所有写操作通过 `Storage._write` 进入同一线程锁和同一个 SQLite 写连接，使用 `BEGIN IMMEDIATE`。读操作每次创建只读连接，WAL 保证读取到完整事务版本。

缓存提交分两阶段：

1. `intent_log` 先以独立事务落盘 `pending`。
2. 缓存条目、leader 会话完成和意图 `committed` 在同一事务内提交。

因此写者在任何瞬间都不会让读者看到“会话已完成但缓存尚未写入”或反向的状态。合并 follower 只有在 leader 的两阶段提交完成后才被唤醒。

## 并发闸门

`ConcurrencyGate` 在同一条件变量下同时检查：

- 全局活动上游查询数 `< global_upstream_limit`；
- 当前权威活动数 `< per_authority_limit`。

两项同时满足才原子增加计数，避免持有全局名额等待某个权威名额而造成全局饥饿。每个 CNAME 跳只在一个上游交换期间持有名额，跳与跳之间释放。

闸门同时执行全局 `queue_limit`：当没有可用名额且现有等待者已达到上限时，新任务立即以 `E_QUEUE_FULL` 失败。该计数跨轮共享，避免多个并发 HTTP 批次各自通过准入后无限堆积在闸门前。

## 调度准入

同一批次先按缓存键分组，组首为最早到达请求，组优先级为组内最高优先级。新组按优先级降序、到达顺序升序准入，容量为全局上限加队列上限。准入后才启动任务，避免被拒绝任务创建 flight。已有跨轮 flight 的组不占新准入容量。

## 合并快照

leader 完成解析后计算一次 `SharedResolution`，其中包含应答记录的原始 TTL、`ttl_base_ms` 和已渲染应答。future 完成时该对象已不可变使用，所有 follower 复制同一答案快照，不会各自重新计算 TTL。

## 预算裁决

预算裁决只决定“继续等待共享结果”还是“为该长尾请求单独派发”。`max_splits_per_flight` 防止同一热点产生拆分风暴。拆分请求独立占用闸门名额，但不写缓存；规范 leader 继续完成唯一缓存提交。

活动上游查询使用剩余轮次预算截断。预算截断与真实上游超时使用不同业务码和不同上下文字段，避免把调度等待伪装成网络超时。

## 恢复状态机

会话状态：

```text
pending -> resolving -> complete
                     -> failed
pending/resolving --crash--> aborted (startup recovery)
```

轮次状态：

```text
running -> complete
       -> interrupted (startup recovery)
```

恢复事务只读取 SQLite 已提交事实。报告不包括猜测的退出原因、未落盘应答或“完整退出摘要”。

