# 设计说明

## 1. 核心不变量

对每个完整文档版本 `V`，以下对象必须在同一 SQLite 事务内同时可见：

- `documents(V)`
- 每个组件的 `materializations(V, component)`
- 每个组件的 `impacts(V, component)`
- 每个组件的 `refs(V, component)`
- `meta.current_version = V`

因此读者不可能观察到“物化已更新但影响集合仍是旧值”的组合。历史版本数据不被覆盖，读请求可以按观察版本读取旧快照。

## 2. 组件有效配置

定义组件 `C` 的候选集合 `Candidates(C, key)`：

1. `C` 所属分组的默认值，距离 0，来源种类 `group`。
2. `C` 的显式键值，距离 0，来源种类 `component`。
3. 对每个依赖 `D`，把 `Candidates(D, key)` 整体提升一层：距离加一，来源路径前插当前组件。

提升依赖候选时同时提升其全量来源链，而不是只传胜出值。这样最终 `sources` 可以完整展示分组继承、各条直接/间接依赖路径和冲突来源。

排序键为：

1. `order` 降序。
2. 来源优先级降序：本地显式 30，直接上游显式 29，远层上游显式递减；本地默认 10，上游默认随距离递减。
3. 依赖距离升序。
4. `owner`、来源种类、`via` 字典序。

胜出候选复制为 `winner`，全部候选保留在 `sources`。`resolution` 根据第一、第二候选的 order/优先级/距离差异生成稳定原因文本。

`undefined_keys` 由组件监听键减去有效值键集合得到。系统没有“默认零值”分支。

## 3. 影响面算法

令 `changedKeys` 为补丁中所有键。

直接种子：

```text
Seed = { C | changedKeys ∩ C.watches != ∅ }
     ∪ { C | C.config[key] 被补丁修改或新增 }
     ∪ { C | C.group == G, G.defaults[key] 被补丁修改或新增 }
```

构造反向依赖图 `Dependents: component -> direct consumers`。从所有种子同时 BFS：

- 种子跳数为 0。
- 每个依赖边加一。
- 多路径到达时保留最短跳数，但合并所有触发键和原因种子。
- 每个受影响组件再独立 BFS，输出其自身可达下游集合。

这样既覆盖“监听键直接命中”，也覆盖“分组继承直接命中”和“上游有效配置变化导致的级联重物化”。

## 4. 物化路径

对组件 `C`：

```text
ratio = |unique(triggerKeys(C))| / max(1, |C.watches|)
```

`ratio < threshold` 选择增量，否则选择全量。首次物化强制全量；未受影响组件选择复用。

增量路径以触发键集合作为 resolver 过滤条件：

- 依赖递归只返回这些键的候选。
- 旧物化中的其他键直接复制。
- 触发键先删除旧值，再写入新解析结果；若新版本无定义则保持缺失。
- `materialized_version` 更新为新版本。

全量路径不加过滤，解析全部键。

由于补丁只修改/新增同一路径的键，其他键的候选集合和胜出规则不受影响，所以复用旧键与全量重算等价。测试 `TestIncrementalAndFullMaterializationAreEquivalent` 对两个独立数据库、相同补丁、不同阈值的结果做逐键深度比较。

## 5. 读可见性与陈旧读

`Read(component, observed)` 在只读事务中执行：

1. 读取当前完整版本 `committed`。
2. `observed == 0` 时绑定 `committed`。
3. `observed > committed` 或无对应完整快照：`E_STALE_READ`。
4. 读取 `(observed, component)` 的物化行。
5. 若行缺失或 `materialized_version > observed`：`E_STALE_READ`。
6. 否则返回配置，并同时标注快照版本和真实物化版本。

未受影响的组件在版本 2 快照中可以有 `materialized_version = 1`，这是合法的复用；读结果明确标注 1。历史版本请求读取该版本自己的完整快照，不会拿最新值冒充旧版本。

提交过程中，最终快照事务尚未提交，因此新版本对读者不可见；旧版本快照仍完整可读。测试 `TestReadersUseBoundVersionDuringCommit` 在意图已落盘、最终事务未开始时并发读取，验证：

- 旧版本读成功。
- 新版本读返回 `E_STALE_READ`。
- 新版本影响和引用记录不可见。
- 提交完成后物化、影响、引用同时可见。

## 6. 持久化模型

| 表 | 作用 | 主键/关键索引 |
|---|---|---|
| `documents` | 完整文档 JSON 与哈希 | `version` |
| `materializations` | 每版本每组件有效配置、真实物化版本、路径决策 | `(snapshot_version, component_id)` |
| `impacts` | 每版本每组件影响条目 | `(snapshot_version, component_id)` |
| `refs` | 监听键、配置键、分组默认值、上游组件引用 | `(snapshot_version, component_id, kind, target, key)` |
| `patch_log` | 补丁内容哈希、状态和幂等结果 | `patch_id` |
| `intents` | 提交前意图与中止/完成事实 | 自增 `id` |
| `meta` | 当前完整版本、schema 版本 | `key` |

所有版本行与 `meta.current_version` 在最终事务内一起推进。SQLite 使用 WAL、`synchronous=FULL`、busy timeout 和外键开关。

## 7. 崩溃恢复协议

```text
validate patch
    |
    v
TX1: patch_log=in_progress + intents=pending   <-- durable intent
    |
    |  (process can be killed here or during computation)
    v
compute impact + materializations + refs
    |
    v
TX2: documents + materializations + impacts + refs + meta + complete states
```

TX2 是恢复边界。若进程在 TX1 后、TX2 提交前退出，重启时：

1. 扫描 `pending`/`in_progress` 意图。
2. 标记意图 `aborted`，补丁日志 `aborted`。
3. 验证每个文档版本的物化与影响组件集合是否完整；不完整版本及其快照行被丢弃并写入恢复报告。
4. 删除不存在完整文档行的孤立快照行。
5. 取剩余文档中的最大版本作为最后完整版本并写回 `meta.current_version`。
6. 生成只含数据库事实的恢复报告。

真实强杀测试在子进程的意图事务提交后直接 `os.Exit(91)`，不执行 defer、关闭连接或回滚代码。父进程验证退出码、恢复报告、文档行数量、补丁状态和不可读的新版本。

## 8. 并发模型

- `writerMu` 将同一 Engine 的文档加载、补丁提交和物化重建串行化。
- SQLite `BEGIN` 后的版本校验防止外部写者推进版本后仍提交旧基础。
- 多读者使用独立连接和只读事务。
- 重复补丁调用在写者锁上等待；第一个提交后，后续调用从 `patch_log.result_json` 返回同一结果并标记 `duplicate=true`。
- 相同 ID 不同规范 JSON 哈希返回 `E_CONFLICTING_PATCH`。

当前假设一个数据库只有一个 Engine 写者进程；同进程并发写已满足单写者串行语义。
