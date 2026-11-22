# 本地嵌入式键值索引与过期管理组件

这是一个无外部服务依赖的本地 KV 组件，面向领域数据编辑器的导入、增量编辑、快照切换和异常恢复场景。运行时只使用 Python 标准库：段文件保存记录，SQLite 保存事务意图、导入报告和恢复报告；没有数据库服务、Docker、网络下载或第三方运行时依赖。

## 1. 快速运行

环境要求：Python 3.11+，开发/验收工具为 pytest 与 requests。

```bash
python3 -m pip install -r requirements-dev.txt
python3 scripts/generate_samples.py
python3 -m pytest -q
python3 scripts/demo_e2e.py
```

启动本地 HTTP 服务：

```bash
python3 -m kvstore --data-dir ./runtime/store --host 127.0.0.1 --port 8765
```

也可以用 `--port 0 --port-file ./runtime/port.json` 让系统分配回环端口。服务只监听本地回环地址。

## 2. 目录结构

```text
kvstore/
  formats.py       KVSG 二进制与分片 JSONL 编解码、协商、时间归一化
  heap.py          以 key 去重的索引最小堆
  model.py         Record / KeyRef / Tombstone / State 等不可变读状态模型
  segments.py      不可变段文件、旁车摘要、随机偏移读取、摘要复验
  db.py            SQLite 意图日志、批次占位、导入/恢复报告
  snapshot.py      快照文档、版本/序号/校验和校验
  store.py         单写者事务、COW 状态发布、导入冲突、快照、恢复
  api.py           本地 HTTP API 与统一错误响应
scripts/
  generate_samples.py  生成固定样本、损坏样本与快照样本
  crash_worker.py      确定性 SIGKILL 演示工作进程
  demo_e2e.py          双格式、增量、过期、快照、段损坏、强杀恢复、HTTP 演示
tests/                 28 个 pytest 验收测试
samples/               脚本生成的固定本地样本
artifacts/             实际演示输出与验证日志
```

## 3. 一致性模型

### 3.1 内存读取状态

`StoreState` 包含：

- `index: key -> KeyRef(version, segment_id, offset, length, write_ts, expires_at, token)`；
- `expiry`：以 `(expires_ms, key, token)` 排序的索引最小堆；
- `tombstones: key -> Tombstone`；
- 当前可见段摘要 `segments`；
- `state_version`、最大段序号、最近快照序号。

已发布的 `StoreState` 不会被原地修改。写者先克隆状态，在克隆上只替换受影响键的派生条目，完整构建后一次指针交换发布。读者持有的旧 `ReadView` 继续绑定旧状态，因此写事务进行中仍看到写前一致快照。段文件不可变，`SegmentManager` 保留已加载段字节缓存，旧读视图不依赖后续文件覆盖。

### 3.2 单写者与增量派生结构

- 所有写入由进程内 `RLock` 串行化；普通写默认等待，导入和快照切换使用非阻塞获取，冲突返回 `E_CONFLICTING_BATCH`。
- 同一 key 的新写会先移除旧堆条目，再插入新条目；因此堆中每个 key 最多一个条目，不会积累旧过期时刻的僵尸项。
- 堆条目携带当前 `KeyRef.token`。清理时同时检查 key、token、过期时刻，旧条目即使被外部重建也无法误删新值。
- 写入、导入、删除只替换受影响 key 的 `index`、堆条目和墓碑；未受影响 key 的 `KeyRef` 对象保持不变。
- 读记录严格按索引中的段偏移和长度读取，并校验记录 CRC、key 与 version。偏移和版本在同一个 `KeyRef` 中原子发布，不会出现“新偏移 + 旧版本”的可见中间态。

### 3.3 持久化事务顺序

每次写事务遵循以下顺序：

1. SQLite 写入 `prepared` 意图，含事务 ID、批次 ID、目标段 ID、快照序号和摘要上下文；
2. 完整构造临时段并 `fsync`，同时构造旁车摘要；`os.replace` 原子改名为最终段；
3. 意图标为 `segment_durable`；
4. 在旁路构造新状态并一次性发布；
5. 意图标为 `committed`，更新 SQLite 元数据。

若进程在步骤 1 后、最终段出现前被杀，重启会清理临时文件并将意图标为 `rolled_back`。若最终段已完整落盘，但尚未发布或提交，重启会扫描完整段、重建状态并标为 `recovered`。恢复报告只陈述持久化事实，并明确 `process_exit_report_available=false`：SIGKILL 不承诺产生有序退出报告。

## 4. KVSG 紧凑二进制格式

所有多字节整数遵守文件头中的端序标记。魔数与端序标记本身是固定字节。

| 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 4 | 魔数 `KVSG` |
| 4 | 1 | 格式版本，当前为 `1` |
| 5 | 1 | 端序标记：`L` 小端，`B` 大端 |
| 6 | 2 | 保留 flags，当前 `0` |
| 8 | 4 | 记录数 `u32` |
| 12 | 可变 | 连续记录帧 |

每条记录：

| 长度 | 字段 |
|---:|---|
| 4 | `frame_length`：payload 长度 + 4 字节 CRC，不含长度前缀本身 |
| 1 | 操作：`0=put`，`1=delete` |
| 1 | 过期标志：`0=无`，`1=有` |
| 4 | key 字节长度 |
| 4 | value 字节长度 |
| 8 | 版本号 `u64`，要求至少 1 |
| 8 | 写入时间戳 `i64`，内部统一为毫秒 |
| 0 或 8 | 可空绝对过期时间 `i64` 毫秒 |
| 可变 | UTF-8 key |
| 可变 | 原始 value 字节 |
| 4 | 前置 payload 的 CRC32/IEEE，按文件端序存放 |

段文件内部始终使用小端 KVSG；外部导入/导出支持大小端。解码时逐条检查长度、字段边界、UTF-8 key、版本、过期标志和 CRC。失败返回 `E_RECORD_CORRUPT`，上下文包含 `record_index`、`byte_offset`、`record_position` 以及校验值。

## 5. 分片 JSON Lines 格式

默认每行一个 JSON 对象，关键字段：

```json
{
  "key": "alpha",
  "value": "domain-alpha-v1",
  "value_encoding": "utf8",
  "version": 1,
  "write_timestamp_us": 1900000000123456,
  "expires_at_us": 1900086400123000,
  "shard_id": 1,
  "crc32": "1234abcd"
}
```

- `value_encoding` 支持 `utf8` 与 `base64`；不可解码为 UTF-8 的原始字节导出为 base64。
- `crc32` 可选；存在时按去掉 `crc32` 后的规范化 JSON 计算并校验。
- `shard_id` 与 `shard_count` 由 `crc32(key) % shard_count` 产生；`GET /export/jsonl?shards=N` 每行仍含完整字段与分片号。
- 也支持单文档分片包：`{"format":"kv-jsonl-shards","format_version":1,"shards":[{"shard_id":0,"records":[...]}]}`。
- 接受别名 `write_timestamp`、`write_ts`、`timestamp`，默认单位微秒；也可通过 `*_unit=ms|us|s` 或 `*_ms` 字段名声明。
- 绝对过期接受 `expires_at_us`、`expires_at_ms`、`expires_at`；相对 TTL 接受 `ttl_us`、`ttl`、`ttl_ms`、`ttl_seconds`、`expires_in_us`、`expires_in_ms`。
- TTL 基准优先取记录自带的 `base_time_us` / `base_time` / `base_timestamp_us`，缺失时取该记录写入时间。

### 缺失字段降级规则

- `key` 必填；缺失或空串为 `E_RECORD_CORRUPT`。
- `value` 缺失为空字符串。
- `version` 缺失为 `1`。
- `write_timestamp` 缺失为导入时刻。
- 绝对过期缺失为 `null`。
- 相对 TTL 基准缺失为记录写入时间。

### 时间归一化与舍入报告

内部统一为毫秒。声明规则为“向负无穷截断到毫秒”：

```text
normalized_ms = floor(source_us / 1000)
rounding_remainder_us = source_us - normalized_ms * 1000  # 0..999
rounding_error_us = normalized_ms * 1000 - source_us       # -(remainder)
```

导入报告的 `rounding_report[]` 逐条输出写入时间和过期时间的 `source_us`、`normalized_ms`、`rounding_remainder_us`、`rounding_error_us` 及累计误差 `cumulative_rounding_error_us`。二进制输入已是毫秒，舍入误差为 0。

## 6. 导入协商与冲突规则

导入前通过魔数/首个非空白 JSON 字符检测格式，并与显式 `format=auto|binary|jsonl` 协商；显式声明与检测不符返回 `E_FORMAT_MISMATCH`。

同 key 比较使用确定的全序：

1. 版本号高者优先；
2. 版本号相同，写入时间戳较新者优先；
3. 版本号与时间戳均相同：内容完全一致时跳过；内容不同则拒绝；
4. 文件内重复 key 使用同一规则选出一个胜者，其余记录标为 `skip`；无法决出的重复组标为 `reject`；
5. 当前 key 不存在但存在删除墓碑时，候选版本更高可重建；同版本需更新时间戳更晚；否则墓碑获胜。

报告中的每条输入都有 `overwrite`、`skip` 或 `reject` 结局及原因。只要有一条 `reject`，整批不写入段、不更新时间索引/堆，返回 `E_IMPORT_REJECTED` 并保留原状态；报告仍会持久化供查询。

## 7. 过期清理

- 堆顶 `expires_ms <= now_ms` 的条目进入候选清理集；
- 对每个候选复核当前索引 token 和过期时刻；
- 为仍有效的到期 key 写一组删除记录到新段，删除记录同时成为持久化墓碑；
- 原子发布新状态后从索引与堆移除 key；
- 返回 `cleaned_count`、`cleaned_keys`、`tombstones_recorded` 以及陈旧候选检测结果。

删除后重建、过期后重写都会产生新 token 和更高版本；旧堆条目已从索引堆删除，旧 `KeyRef` 不再出现在当前状态。

## 8. 段摘要、级联失效与压缩

每个 `.kvs` 段都有独立 `.kvs.meta.json` 旁车摘要，包含 SHA-256、记录数、序号、类型、批次与快照序号。加载和 `POST /maintenance/verify-segments` 会重新计算磁盘摘要并逐记录校验 CRC。

摘要失败或段记录损坏时，当前状态引用的该段全部 key 映射被判定失效。组件在旁路跳过无效段、从其余有效段重建索引/堆/墓碑，然后一次发布；不会原地修改半个映射。汇总返回失效 key、变化条目及前后状态版本。

压缩会生成一个完整的新段（墓碑在前、活记录在后），以原子发布替换当前状态中的段映射。旧段文件保留在磁盘，用于崩溃恢复和旧读视图；重启时更高序号段和最新快照能重建相同状态。

## 9. 快照

快照是 JSON 文档，包含：

- `magic = KVSTORE-SNAPSHOT`；
- `format_version = 1`；
- 单调递增的 `snapshot_seq`；
- `base_snapshot_seq`、`generated_at_ms`、`state_version`；
- 墓碑与活记录数组（值支持 utf8/base64）；
- 对除 `checksum` 外规范 JSON 的 SHA-256。

加载前完整解析并校验；任一格式、版本、字段或校验失败返回 `E_SNAPSHOT_INVALID`。序号不大于当前已加载序号返回 `E_SNAPSHOT_STALE`；调用方给出过时 `expected_base_snapshot_seq` 时返回 `E_STALE_BASE`。快照携带的 `base_snapshot_seq` 作为来源信息保留，但整体替换允许把更高来源基线的快照装入更旧或空的目标状态。通过校验后按普通事务顺序写意图、落完整快照段、一次切换状态指针；失败路径完全不触碰旧状态。并发切换由非阻塞写锁保证只有一个成功，其余返回 `E_CONFLICTING_BATCH`。

## 10. SQLite 表与恢复报告

SQLite 使用 WAL、`synchronous=FULL`，表包括：

- `intents`：事务意图及 `prepared/segment_durable/committed/recovered/rolled_back/aborted` 状态；
- `import_reports`：按批次保存导入结局与冲突明细；
- `recovery_reports`：每次启动扫描生成的持久化恢复报告；
- `batch_claims`：导入批次与快照序号占位；
- `meta`：最大段序号、状态版本、最近快照序号等。

启动恢复报告包含扫描问题、有效段数、当前活动段、逐意图动作、恢复/回滚计数、已提交但段缺失告警及状态摘要。它不补写“退出报告”，且 `facts_only=true`。

## 11. 错误码

所有 HTTP 失败响应统一为：

```json
{"ok": false, "code": "E_...", "message": "...", "context": {}}
```

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_RECORD_CORRUPT` | 422 | 记录长度/CRC/字段/JSON 行损坏，含记录位置 |
| `E_IMPORT_REJECTED` | 409 | 至少一条冲突被拒绝，整批回滚 |
| `E_CONFLICTING_BATCH` | 409 | 并发导入/快照或批次/快照序号占位冲突 |
| `E_SNAPSHOT_INVALID` | 422 | 快照格式、版本、字段或校验和失败 |
| `E_SNAPSHOT_STALE` | 409 | 快照序号非单调 |
| `E_STALE_BASE` | 409 | 写/导入的基准状态版本或快照基线过时 |
| `E_VERSION_CONFLICT` | 409 | 本地写/删除版本不递增 |
| `E_KEY_NOT_FOUND` / `E_KEY_EXPIRED` | 404 | key 不存在或已到期 |
| `E_FORMAT_MISMATCH` / `E_FORMAT_UNSUPPORTED` | 415 | 格式声明不符或不支持 |
| `E_SEGMENT_INVALID` | 422 | 段/偏移依赖已不可读 |
| `E_BAD_REQUEST` | 400 | 参数或请求体非法 |

## 12. HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/stats` | 状态版本、key/墓碑/堆/段统计 |
| PUT/POST | `/keys/{key}` | 写入；支持 value、version、时间、过期、expected_version、base_state_version |
| GET | `/keys/{key}` | 读取；可选 `now_ms` 用于确定性过期判断 |
| DELETE | `/keys/{key}` | 删除并写墓碑 |
| GET | `/export/binary?endian=little\|big` | 导出 KVSG |
| GET | `/export/jsonl?shards=N[&bundle=1]` | 导出 JSONL 或分片包 |
| POST | `/import` | 原始字节或 JSON wrapper；`format=auto\|binary\|jsonl` |
| POST | `/import/binary` | 强制二进制导入 |
| POST | `/import/jsonl` | 强制 JSONL 导入 |
| GET | `/imports/{batch_id}` | 查询导入报告 |
| GET | `/snapshot/export?snapshot_seq=N` | 生成快照文档 |
| POST | `/snapshot/switch` | 校验并原子切换快照 |
| GET | `/recovery/report` | 最近启动恢复报告 |
| POST | `/maintenance/expire` | 执行到期清理，可传 `now_ms` |
| POST | `/maintenance/verify-segments` | 复核摘要并级联重建 |
| POST | `/maintenance/compact` | 压缩当前状态为新段 |
| GET | `/segments` | 段摘要与依赖 key |
| GET | `/debug/heap` | 堆一致性、stale token 检测 |

JSON wrapper 导入示例：

```json
{"batch_id":"b-1","format":"binary","data_base64":"S1ZTRwE..."}
```

## 13. 实际验证结果

执行日期：2026-11-23。当前环境 Python 3.14.4、SQLite 3.46.1、pytest 8.3.4、requests 2.34.2。

```text
$ python3 -m pytest -q
28 passed
```

测试覆盖：

- 大小端 KVSG 往返、CRC 错误记录位置、JSONL CRC/坏行、格式协商；
- 微秒截断/累计误差、TTL 基准换算、缺省字段；
- 堆 replacement 无僵尸、删除重建、过期清理墓碑；
- 导入覆盖/跳过/拒绝、整批回滚、批次占位与 stale base；
- 同 key 更新仅替换对应索引条目、未受影响引用不变；
- 段摘要损坏后 key 级联失效、原子重建、旧读视图仍可读；
- 压缩后偏移重建与重启恢复；
- 快照成功切换、校验失败旧状态可用、单调序号、版本校验；
- 写事务进行中读者仍见旧状态、并发导入冲突；
- 真实 `SIGKILL`（子进程返回码 `-9`，shell 显示 `137`）后完整段恢复与仅意图回滚；
- HTTP 全部主路径及 `E_RECORD_CORRUPT`、`E_CONFLICTING_BATCH`、`E_SNAPSHOT_INVALID`、`E_STALE_BASE`、`E_SNAPSHOT_STALE`。

`artifacts/demo_output.json` 记录的端到端结果摘要：

- 二进制固定样本导入 5 条；JSONL 固定样本导入后语义索引与二进制完全一致；
- JSONL 5 条记录累计截断误差 `-3169 µs`，逐条 remainder 已输出；
- 单键 `alpha` 增量写后，变化 key 集合仅为 `["alpha"]`，其余 4 个引用保持相同；
- 清理陈旧记录 2 条（`gamma`、`epsilon`）并写入 2 个墓碑，堆仍一致；
- 段摘要破坏后 `a` 级联失效，`b` 保留，`state_rebuilt=true`；
- `after_segment` 强杀恢复批次 2 个 key；`after_intent` 强杀批次回滚；
- HTTP 校验覆盖 5 个要求的业务错误码，快照切换成功且重复序号返回 stale；
- `/recovery/report` 返回 `facts_only=true` 和 `process_exit_report_available=false`。

`artifacts/acceptance_checks.json` 对上述关键结果做机器可读断言，当前 12 项检查全部为 `true`；原始 pytest 输出见 `artifacts/pytest.txt`。

固定样本 SHA-256 见 `samples/manifest.json`；演示生成时间、事务 ID 每次运行可能变化，但记录内容、时间与结果规则固定。

