# 设计与数值契约

## 1. 数据与派生关系

每条队列声明 `(CIR, PIR, CBS, PBS, priority, match, parent)`。有效速率按父先子后的顺序计算：

```text
effective_cir(q) = min(q.cir, effective_cir(parent))
effective_pir(q) = min(q.pir, effective_pir(parent))
```

根的声明值就是有效值。每个来源记录为：

```json
{"kind": "declared", "self_value": 8000000}
{"kind": "parent_limit", "self_value": 12000000, "parent_queue_id": "video", "parent_effective": 1234567}
```

因此父队列速率下调会导致整棵子树重新计算，即使某个后代原声明值本身未变。依赖边变化也重算该队列子树。

派生结构包括：

1. `effective_rates`：有效 CIR/PIR、来源、父链、深度、计算版本与有效至版本。
2. `token_bucket_view`：有效速率、CBS/PBS、优先级层、父链和补充/舍入契约。
3. `consistency`：自身字段及完整父链引用，加确定性评分。

未受影响队列不重算上述内容。提交时只推进其 `valid_through_version`；其 `calculation_version` 保留上一次真实计算版本。接口同时返回两者。

## 2. 增量影响集合

| 变更 | 重算范围 |
|---|---|
| CIR/PIR 改变 | 本队列及后代 |
| 父依赖改变 | 本队列及后代 |
| 优先级改变 | 本队列及后代（后代一致度引用父链优先级） |
| CBS/PBS/Match 改变 | 本队列 |
| 新增队列 | 新队列 |
| 删除队列 | 删除其整棵子树，不重算保留队列 |

返回值中 `recomputed_queue_ids` 是真实重算集合，`unchanged_queue_ids` 没有重算，`removed_queue_ids` 是级联删除结果。

## 3. 一致度评分

评分从 100 开始，确定性扣分：

- 有效 CIR 因父上限低于自身声明：-10。
- 有效 PIR 因父上限低于自身声明：-10。
- 每个祖先优先级高于子队列，每个反转 -5，最多 -20。
- 任何祖先有效速率小于后代有效速率（正常情况下不会发生）：每条父链再 -40。

评分不是模糊模型，而是可复核的规则。`references` 明确保存自身 CIR/PIR/CBS/PBS/priority 与每个祖先的 effective CIR/PIR/priority。

## 4. 整数令牌桶

### 4.1 单位与初始化

- 速率单位：`bit/s`。
- 突发容量与令牌单位：`bit`。
- 在队列首包时间戳，承诺桶置满 `CBS*8`，峰值桶置满 `PBS*8`。
- 包按 `(timestamp_ms, priority DESC, queue_id ASC, sample_index ASC)` 形成确定性调度序列。各队列拥有独立令牌桶；父树影响通过有效速率继承体现。

### 4.2 补充算法

对每个非首包：

```text
delta_ms = current_timestamp_ms - last_timestamp_ms
exact_millibits = rate_bps * delta_ms
total = carry_millibits + exact_millibits
whole_bits = total / 1000
carry_millibits = total % 1000
bucket = min(capacity, bucket + whole_bits)
capacity_overflow_bits = whole_bits - added_bits
```

时间戳为整数毫秒，子毫秒部分在进入算法前截断；当前样本本身就是整数毫秒，因此首包与每包 `ms_truncation_ms` 均为 0。

余数不丢弃，而是跨包携带，所以累计截断误差始终小于 1 bit。每个补充事件记录：

```json
{
  "bucket": "committed",
  "rate_bps": 1234567,
  "delta_ms": 5,
  "raw_millibits": 6172835,
  "carry_before_millibits": 0,
  "whole_bits": 6172,
  "carry_after_millibits": 835,
  "cumulative_error_millibits": 835,
  "capacity_overflow_bits": 0,
  "explanation": "1234567 bps * 5 ms = 6172835 millibits; (0 carry + 6172835 raw)/1000 = 6172 whole bits, carry 835 millibits; ..."
}
```

队列汇总中的 `cumulative_error_millibits` 是仿真结束时的余数；`capacity_overflow_bits` 是桶满后未加入的整令牌。

### 4.3 三色判定

包大小 `bits = bytes * 8`。

- 承诺桶与峰值桶都足够：绿色，两个桶都扣 `bits`，全部发送。
- 仅峰值桶足够：黄色，只扣峰值桶，全部发送；之后若承诺令牌恢复，仍可变绿。
- 峰值桶不足：红色，不扣令牌，整包丢弃。

队列结论：

- 无红、无黄：`CONFORMANT`
- 有黄、无红：`EXCEEDS_COMMITTED_RATE`
- 有红：`NON_CONFORMANT`

同一策略版本 + 同一流量文件重复运行，包序列、令牌、颜色、舍入字段完全相同。仿真落库时也满足这一性质；只有 `created_at`、`simulation_id` 这类请求元数据不同。

## 5. 事务与可见性

### 提交顺序

1. 获取进程内单写者互斥锁。
2. 检查 `intents.request_id` 幂等键。
3. 先以独立自动提交写入 `intents(status='pending')`。
4. 完成纯内存校验/影响分析，写入 intent phase。
5. 开启 SQLite 事务；再次确认当前版本。
6. 在同一事务中更新策略、受影响派生视图、未受影响视图的 `valid_through_version`、历史视图、提交记录、`current_version` 与 intent 完成状态。
7. 提交事务。

任何第 3 步之后的崩溃都会留下 pending intent；策略和派生数据仍在第 5 步事务中，进程退出后由 SQLite 整体回滚。

### 读取

`GET /v1/policy/queues/...`、仿真诊断和列表都先开启 `ReadOnly` 事务，再在同一事务内读取 `meta.current_version`、队列行和视图。因此：

- 写入进行中：读者看到提交前的完整版本。
- 写入刚提交：读者要么仍看到旧完整版本（若读事务已建立快照），要么看到新完整版本的 `current_version` 并据此裁决观察版本。
- 不存在“父已更新、子仍旧”或“令牌视图已更新、一致度仍旧”的可见中间态。

观察版本语义：

```text
observe_version != current_version          -> E_STALE_READ
valid_through_version < observe_version     -> E_STALE_READ
calculation_version > observe_version       -> E_STALE_READ
otherwise                                   -> OK + calculation_version + valid_through_version
```

未受影响视图的 `calculation_version` 可以小于观察版本，但其 `valid_through_version` 等于观察版本，故可安全返回。

## 6. 崩溃恢复

服务启动、开始监听之前执行恢复：

1. 读取 `meta.current_version` 与 `MAX(commits.version)`；如有超前则恢复到最后一个完整提交。
2. 扫描所有 pending intent。
3. 将每个 intent 标为 `aborted`，保存 `E_ABORTED` 与阶段。
4. 清理未来版本的 `queue_view_history`、仿真/令牌相关残留。
5. 写入 `recovery_runs`，包括请求 ID、kind、phase、回滚版本和派生/令牌回滚标志。

演示中的故障注入位于事务内、派生行写入之后、`COMMIT` 之前。外部 `Process.Kill()` 后重启，报告示例：

```json
{
  "current_version": 2,
  "pending_intents": 1,
  "reports": [{
    "request_id": "demo-crash-v3",
    "kind": "increment",
    "phase": "increment_rebuild",
    "status": "ABORTED",
    "rollback_version": 2,
    "derived_rollback": true,
    "token_rollback": false,
    "message": "uncommitted policy/derived/simulation state rolled back to the last complete version"
  }]
}
```

崩溃提交修改的 `bulk` CIR 不可见；`bulk@v2` 仍为旧有效值。若故障阶段为 `simulation`，报告中的 `token_rollback` 为 true。

## 7. 实际验证记录

执行环境：Linux 开发容器，`go1.27.4`；模块声明 `go 1.22.0`，并额外用 `GOOS=windows GOARCH=amd64` 编译全部包。依赖锁定为 `modernc.org/sqlite v1.34.5`。

```text
$ go test ./... -count=1
ok  qosshaper/internal/qos
ok  qosshaper/internal/httpapi

$ GOOS=windows GOARCH=amd64 go test ./... -run '^$'
全部包编译通过

$ go run ./cmd/demo
== full snapshot ==
code=OK version=1 recomputed=[bulk guest video video-rtp voice voice-sip wan]
video@v1 effective_cir=20000377 calculation_version=1 valid_through=1

== simulation v1 ==
queue=video eff_cir=20000377 sent=146853 dropped=0 green=72 yellow=0 red=0 final_tokens=242176 carry=835 conclusion=CONFORMANT
queue=video-rtp eff_cir=12000123 sent=150305 dropped=0 green=72 yellow=0 red=0 final_tokens=109784 carry=665 conclusion=CONFORMANT

== incremental parent CIR change ==
code=OK version=2 recomputed=[video video-rtp] unchanged=[bulk guest voice voice-sip wan]
video@v2 eff_cir=1234567 calc=2 valid=2
video-rtp@v2 eff_cir=1234567 source=parent_limit score=90 calc=2 valid=2
voice@v2 eff_cir=5000123 calc=1 valid=2

== stale read ==
HTTP=409 code=E_STALE_READ observed=1 view=2 current=2

== forced termination ==
killed pid while derived rebuild was uncommitted
recovery current_version=2 aborted=1
  request=demo-crash-v3 kind=increment phase=increment_rebuild status=ABORTED rollback_version=2
bulk@v2 after recovery eff_cir=10000111 calc=1 valid=2
DEMO PASS
```

以上结果与声明版本一致：父队列 CIR 下调只重算 `video` 子树；其他分支保留旧计算版本但有效至版本 2；旧观察版本被拒绝；强制终止后的新 CIR 没有进入版本 2 的可见状态。
