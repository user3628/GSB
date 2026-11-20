# 设计说明与一致性不变量

## 1. 核心层次

```text
JSON snapshot / increment
        │
        ▼
RouteConfig (validated, immutable)
        │
        ▼
compile_state() ──> CompiledState (routes + 4 artifacts/route + 5 aggregates)
        │                         │
        │                         ├── reverse/forward matching indexes
        │                         ├── pool groups
        │                         ├── health plans
        │                         └── forward rule caches
        ▼
SQLite intent -> complete snapshot -> Runtime pointer swap -> health generation swap
        │
        ▼
request binds one CompiledState and uses only its objects until completion
```

## 2. 关键不变量

1. **单一发布点**：`Runtime._current = compiled` 是唯一配置可见性转换。赋值前，所有路由、索引、产物和一致性检查已构建完成；赋值后请求才能读取。
2. **只读快照**：`CompiledState` 发布后不修改。更新永远构建新对象，不原地替换某个路由或某个产物。
3. **编译自检**：`CompiledState.assert_atomic()` 检查每条路由四项产物、索引指纹和所有聚合产物都存在且互相引用当前路由。
4. **请求绑定**：请求解析完请求头后取得一次 `CompiledState` 引用，路由匹配、产物选择、上游候选和转发版本都来自该对象。
5. **版本锁**：增量基础版本比较和快照版本单调比较都在同一个 `asyncio.Lock` 内完成。
6. **持久化先行**：只有完整快照事务提交成功后才发布内存状态；SQLite 失败不会产生新的可见配置。
7. **健康 generation**：每次发布前取消旧探测任务并递增 generation；旧任务即使返回结果也不能写入新计划状态。

## 3. 级联失效模型

路由规范化 JSON 的 SHA-256 指纹变化即视为路由变化。变化类型不限于上游、请求头和超时；重试、故障策略、健康策略和匹配规则同样进入全部四项路由产物。

每条路由产物 ID：

```text
match:<route_id>
pool:<group>:<route_id>
health:<route_id>
rules:<route_id>
```

聚合产物均依赖当前完整路由指纹集合，因此在任何路由变化时进入失效集合。这有意选择“稍宽但安全”的级联范围：宁可重建无关方向的匹配聚合，也不允许旧上游残留在任一索引。

未进入 `rebuilt_artifacts` 且内容哈希未变的路由产物可复用，报告为 `reused_artifacts`。

## 4. 更新事务顺序

```text
acquire update lock
  parse and validate payload
  compare base_version / version monotonicity
  apply increment or full replacement
  compile complete immutable state in memory
  COMMIT intent(prepared)
    [failpoint：此处强杀只会留下 intent]
  COMMIT snapshot(complete=1) + current_version + history
  mark intent committed
  cancel old health plans (generation++)
  Runtime._current = new state       # atomic publication
  start new health plans
release update lock
```

如果进程在“完整快照已提交、意图仍为 prepared”时退出，恢复报告将操作标记为 `commit_already_durable`，因为完整快照是已持久化事实。进程不会猜测未提交状态。

## 5. 请求状态机

```text
receive headers
  bind CompiledState
  match route
  read body
  for each retry attempt:
      abort policy generation check
      choose first non-unhealthy candidate
      real HTTP exchange
      classify error / 5xx / success
      update health from real result
      if abort policy and generation changed: E_CONFIG_CHANGED
  write upstream status/body or stable JSON error
```

`continue` 与 `abort` 的区别只影响已绑定的在途请求，不影响其他请求从哪个快照读取。最终响应写出前仍有极短的调度窗口；本实现将确定性边界定义在“上游响应已取得并更新健康后”，此后若尚无更新发生，请求按绑定版本完成。

## 6. 健康状态机

```text
unknown --probe success threshold--> healthy
unknown --probe/forward fail threshold--> unhealthy
healthy --probe/forward fail threshold--> unhealthy
unhealthy --probe success threshold--> healthy
```

真实转发成功会立即恢复 `healthy`，因为该结果直接证明当前上游可用。探测成功按配置的 `success_threshold` 累计；探测和转发失败都按 `fail_threshold` 累计。候选选择永不主动发送新请求给 `unhealthy` 上游。

## 7. 退出差异

- 安全退出：关闭监听 socket，设置 `accepting=false`，等待已接纳任务；持久化 `process_state=stopped` 和完成/取消数。
- 强杀：没有机会更新进程状态，`process_state` 保持 `running`；重启扫描 pending intent 并基于完整快照重放恢复。
- 两种路径都只能从 SQLite 完整快照恢复配置，不依赖内存日志。

