# 实现说明

## 代码结构

- `pkg/edgeconfig/model.go`：API/持久化共享领域模型。
- `pkg/edgeconfig/codes.go`：业务错误码与 JSON 错误体。
- `pkg/edgeconfig/validate.go`：键路径、节点唯一性、端点、订阅、依赖环和策略校验。
- `pkg/edgeconfig/digest.go`：规范化 JSON、快照摘要、节点期望摘要和请求哈希。
- `pkg/edgeconfig/affected.go`：订阅索引、区域分桶、传播图、跳数和期望视图构造。
- `pkg/edgeconfig/numeric.go`：整数权重、定点舍入、累计误差、相对容差和字典序平局处理。
- `pkg/edgeconfig/store.go`：pure-Go SQLite 驱动、schema 和 PRAGMA。
- `pkg/edgeconfig/write.go`：单写者幂等包装、意图记录、结果复用和冲突检测。
- `pkg/edgeconfig/service.go` / `commits.go`：拓扑、快照、增量版本事务。
- `pkg/edgeconfig/reports.go`：节点上报裁决。
- `pkg/edgeconfig/probe.go`：真实 HTTP 探测、独立超时、连接跟踪/强制关闭、并发名额。
- `pkg/edgeconfig/convergence.go`：收敛状态、区域/全局一致度和事务内重算。
- `pkg/edgeconfig/recovery.go`：未完成意图扫描、回滚/中止和恢复报告。
- `pkg/edgeconfig/server.go`：`net/http` JSON API。
- `pkg/edgeconfig/endpoints.go`：可配置摘要、拒绝状态码和挂起的 loopback 节点。
- `internal/verify/`：固定样本验证器和真实强杀子进程编排。
- `cmd/edge-dist/`：`verify`、`serve`、`node-endpoint`、`hash-snapshot`、`__crash_child`。

## 可集成 API

```go
import "edgeconfig/pkg/edgeconfig"

store, _ := edgeconfig.OpenStore("./edgeconfig.db")
defer store.Close()
svc := edgeconfig.NewService(store)

_ = svc.SaveTopology(ctx, topology)
commit, err := svc.SubmitIncrement(ctx, increment)
decision, err := svc.SubmitReport(ctx, report)
round, err := svc.RunProbeRound(ctx, edgeconfig.ProbeRequest{
    RoundID: "round-1", RequestID: "req-1", TimeoutMS: 200, Concurrency: 4,
})
recovery, err := svc.Recover(ctx)
```

也可以用 `edgeconfig.NewHTTPHandler(svc)` 挂载到现有 `net/http` 服务。

## 影响面策略

`Graph.AnalyzeImpact` 先对变更键去重，然后按策略选择：

- `per_key_exact`：查询 `key -> subscribers` 索引。只访问变更键的订阅者，不扫描全部节点。
- `region_bucket`：先由键索引得到受影响区域，再只扫描这些区域的节点一次，用订阅集合做 O(min(changed, subscriptions)) 匹配。

选择依据包含：

- 变更键数量与声明阈值；
- 全订阅节点数量；
- `naive_scan_product = changed_keys * total_nodes`；
- 实际索引查询数、订阅访问数、区域桶数和候选节点扫描数。

直接订阅/订阅变更节点为第 1 跳；沿 `depends_on` 反向边传播，下游为第 2、3... 跳。拓扑校验拒绝依赖环。

## 期望摘要与级联

节点期望摘要是以下内容的规范化 SHA-256：

- 该节点订阅的键路径、类型、值和顺序号；
- 其直接上游依赖的期望摘要。

因此上游节点的键变化会改变上游摘要，并继续改变所有下游摘要。若新摘要与上一完整版本相同，节点保留旧的 `expected_version`；否则提升到当前提交版本。每次提交都向 `expected_views(commit_version, node_id)` 写入完整节点行，形成可查询的完整版本。

## 陈旧读规则

对每个节点持久化：

- `expected_version` / `expected_digest`
- 最近一次 `observed_version` / `applied_digest`

裁决规则：

1. `observed_version < expected_version`：返回 HTTP 409 和 `E_STALE_READ`，详情附 `expected_version`、`current_view_version`；上报事实仍写入 `node_reports`，但状态置为 `STALE_READ`，不产生收敛结论。
2. 观察版本足够且摘要相等：`CONVERGED`。
3. 观察版本足够但摘要不同：`DIVERGED` / `E_DIGEST_MISMATCH`，这是新鲜视图上的分歧结论，不是陈旧读。

## 探测超时与名额

`RunProbeRound` 为每轮创建容量为 `concurrency` 的信号量。goroutine 取得名额后才创建独立 `context.WithTimeout`，所以排队时间不消耗探测超时。

超时或网络错误时：

- 取消 context；
- 自定义 `trackedDialer` 关闭该探测跟踪到的底层 `net.Conn`；
- 关闭 transport 的空闲连接；
- `defer` 从信号量释放名额；
- 返回 `E_PROBE_TIMEOUT`。

自动化测试 `TestProbeTimeoutReleasesConcurrencySlots` 使用 4 个长挂起端点、并发 2、超时 120ms；若前两个超时后不释放名额，后两个探测将永远无法开始，测试会在整体超时中失败。当前用例约 0.25 秒完成。

## 数值契约

`NumericPolicy.Scale` 默认 `1_000_000`，即六位定点。权重为正整数，缺省权重可配置。

计算过程：

1. 按 `(region, node_id)` 稳定排序。
2. 求 `total_weight` 和 `matched_weight`。
3. 对每个匹配节点计算 `weight * scale / total_weight` 的整数商和余数。
4. 先分配整数底账，再按余数从大到小补齐；余数相同按区域、节点 ID 字典序。
5. 全局使用 `matched_weight * scale / total_weight` 的半入舍入确定权威分数。
6. 输出每步商、余数、舍入后单元和误差分子；累计误差以有理数和 `1e9` 显示定点值表示。

阈值比较不使用 `float64`：

```text
abs(score - threshold) * 1,000,000 <= relative_tolerance_ppm * max(score, threshold)
```

容差内标记 tie，并使用稳定的 `(region, node_id)` 顺序给出边界节点和可复现的 `TIE_BROKEN_PASS/FAIL`。

## 单写者与幂等

所有变更入口经过 `runWrite[T]`：

1. 获取进程内 `writeMu`。
2. 按 `<kind>:<request_id>` 查 `idempotent_requests`。
3. 相同 ID 且请求哈希相同：反序列化首次响应并返回。
4. 相同 ID 但哈希不同：`E_CONFLICTING_REQUEST`。
5. 先自动提交一条 `PENDING` 意图。
6. 执行业务；成功时业务行、幂等响应、意图完成标记在同一 SQLite 事务提交。
7. 业务拒绝时保留意图并标记 `ABORTED`，同时缓存相同错误响应。

SQLite 使用 WAL。读请求不获取写互斥锁，只读取已提交事务；期望摘要、节点状态和一致度在同一事务更新，因此不会出现“状态已收敛但期望摘要缺失”的中间读。

## 崩溃恢复

意图先于业务事务持久化。强杀可能留下 `PENDING`：

- `increment` / `snapshot`：业务版本事务是原子的；恢复将意图标记为中止。
- `affected_recompute` / `probe_round`：最终结果事务未提交则探测记录/状态/一致度不可见；恢复从最大的完整 `expected_views.commit_version` 重建期望字段，依据已落盘上报/探测事实重算状态和一致度。

验证器会启动真实子进程，在写入 `READY` 意图后由父进程调用 `Process.Kill()`。在 Windows 上这对应 `TerminateProcess`，不执行 `defer`。随后父进程重新打开 SQLite 并调用 `Recover`，断言恰好扫描到 1 条 pending intent、最后完整版本为 1、回滚目标为 1。
