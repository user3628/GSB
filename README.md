# 本地分段键值索引与过期管理组件

面向本地嵌入式场景的 Python 组件。它使用追加式段文件保存记录，在内存中维护 `key -> segment offset` 索引和过期时间最小堆，并提供紧凑二进制与 JSON Lines 两种互操作格式以及本地 HTTP API。

## 已实现能力

- 段文件记录包含键、值、版本、写入时间戳、绝对过期时间、墓碑标记和未知字段元数据；读取时重新校验目标帧 CRC32。
- `.kvsb` 紧凑二进制与 `.jsonl` 分片 JSON 行格式的完整编解码；二进制支持大/小端并逐记录校验 CRC32。
- 导入格式协商、毫秒/微秒归一化、相对 TTL 到绝对过期时间换算、字段补全和未知字段报告。
- 确定性的版本/写入时间冲突规则，输出 `overwrite`、`skip`、`reject` 三种结果；拒绝不会回滚同批次已接受的键。
- 单写者非阻塞锁、读路径无写锁、写前后不可变状态引用；同一导入批次或并发快照切换只有一个请求成功。
- 意图日志、提交标记、段截断回滚和重启恢复报告；真实子进程硬终止演示。
- 快照文件带格式版本、快照序号和生成时间；索引、过期堆与活动段通过一次状态根替换原子生效。

## Windows 快速开始

要求 Python 3.10+ 和可用的 pip。无需数据库、Docker、网络服务或额外 CRC32 依赖，`zlib` 属于 Python 标准库。

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe scripts\interop_demo.py
```

也可直接运行封装脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

启动本地 HTTP 服务：

```powershell
.\.venv\Scripts\python.exe -m kvstore.server --data-dir .\local-data --host 127.0.0.1 --port 5050
```

重建固定样本：

```powershell
.\.venv\Scripts\python.exe scripts\generate_samples.py
```

## HTTP API

所有 JSON 响应都带 `ok` 字段；失败统一返回：

```json
{"ok":false,"error":{"code":"E_KEY_NOT_FOUND","message":"key does not exist","details":{"key":"missing"}}}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查与当前活动段摘要 |
| `POST` | `/v1/keys` | 写入，JSON body 必须含 `key` 和 `value` |
| `PUT` | `/v1/kv/<key>` | 写入指定键 |
| `GET` | `/v1/kv/<key>` | 读取；支持 `read_timeout_ms` 或 `X-Read-Timeout-Ms` |
| `DELETE` | `/v1/kv/<key>` | 写墓碑并删除键 |
| `GET` | `/v1/export/binary` | 导出紧凑二进制，默认小端；可用 `?endian=big` |
| `GET` | `/v1/export/jsonl` | 导出 JSON Lines |
| `POST` | `/v1/import/binary` | 导入二进制；支持原始 body 或 multipart `file` |
| `POST` | `/v1/import/jsonl` | 导入 JSON Lines；支持原始 body 或 multipart `file` |
| `POST` | `/v1/import` | 自动协商格式，可用 `?format=binary|jsonl|auto` |
| `POST` | `/v1/import/negotiate` | 只做格式检测、字段映射与冲突前预览，不修改存储 |
| `POST` | `/v1/expire` | 清理到期键，body 可含 `limit` |
| `POST` | `/v1/snapshot/switch` | 无 `path` 时创建并切换新快照；有 `path` 时校验并加载本地快照 |
| `GET` | `/v1/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/v1/index` | 活动段、快照序号、索引摘要与键清单 |

写入 body 可选字段：`version`、`written_at_us`、`expires_at_us`、`ttl_ms`、`extras`。未提供版本时取当前版本 +1。导入请求可通过 `X-Batch-Id` 声明批次；同一批次只允许成功一次。

业务错误码：

| 错误码 | 含义 |
|---|---|
| `E_KEY_NOT_FOUND` | 键不存在，或已到期但尚未物理写墓碑 |
| `E_FORMAT_INVALID` | 魔数、头部、长度、CRC32、JSON 或格式协商失败 |
| `E_CONCURRENT_WRITE` | 已有写事务，或同一导入批次已执行 |
| `E_SNAPSHOT_INVALID` | 快照版本、序号、CRC 或加载校验失败 |
| `E_VERSION_CONFLICT` | 普通写入请求的版本不高于当前版本 |
| `E_READ_TIMEOUT` | 读取超过声明的本地预算 |
| `E_INVALID_ARGUMENT` | 参数或 JSON value 不合法 |
| `E_STORAGE` / `E_INTERNAL` | 本地存储或未预期内部错误 |

## 项目结构

```text
kvstore/
  store.py         事务、索引、堆、快照、恢复、冲突解决
  segment.py       内部段文件与 CRC32 帧
  codec_binary.py  紧凑二进制格式
  codec_jsonl.py   JSON Lines 格式
  formats.py       格式检测与协商入口
  api.py           Flask HTTP API
  server.py        服务入口
  crash_worker.py  强杀演示 worker
  recover.py       离线恢复报告入口
samples/           固定二进制、JSONL 与预期冲突数据
scripts/           样本生成、Windows 启动、端到端互操作演示
tests/             编解码、冲突、过期、快照、并发、HTTP、强杀恢复测试
docs/              格式与架构说明
artifacts/         实际验证结果
```

## 关键设计

- 内部段文件为追加式日志，头部与每帧都有 CRC32。每个内存索引项记录活动段路径、帧偏移与帧长度；读取不信任索引缓存值，而是按偏移读段并校验 CRC。
- 每次写入、导入、删除、过期清理或快照切换都走同一事务路径：先 `fsync` 意图文件，再写段和 `fsync`，随后写提交标记，最后原子替换内存状态根并更新 meta。
- 读请求捕获一个不可变 `StoreState` 引用。普通写只创建新索引字典和堆副本，因此旧读者继续看到写前快照；快照切换后旧状态在最后一个读者释放引用时才清理内存资源。
- 写锁使用非阻塞获取。HTTP 层的并发写不会排队等待，直接返回 `E_CONCURRENT_WRITE`；只读请求不获取该锁。
- 恢复以提交标记和段头为事实：有 `.commit` 的意图验证段头与长度后视为已应用；无 `.commit` 且无 `.abort` 的意图按 `pre_size` 截断段文件；正常进程内失败已写 `.abort`，不会在下次启动误回滚后续成功事务。

字节级格式见 [docs/FORMATS.md](docs/FORMATS.md)，事务与原子切换细节见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 实际验证结果

本工作区实际执行结果：

```text
$ .venv/bin/python -m pytest
37 passed

$ .venv/bin/python scripts/interop_demo.py
1. Binary sample import: 4 committed; CRC verified = True
2. Binary export: artifacts/demo/exported-segment.kvsb 243 bytes, 4 records
3. JSONL import: overwrite=3 skip=2 reject=2 committed=3 rejected_keys=['dup:key']
   - record 1 device:threshold: overwrite (newer_write_time)
   - record 2 sensor:temp: overwrite (higher_version)
   - record 3 config:region: skip (lower_version)
   - record 5 dup:key: reject (unresolved_same_version_and_write_time)
   - record 6 dup:key: reject (unresolved_same_version_and_write_time)
   - record 7 device:mode: skip (duplicate_record)
4. Snapshot switch: seq 1 live keys 5
5. Forced termination: killed pid with persisted intent; process return code -9
6. Restart recovery: rolled back 1 transaction: txn-000000000004
   applied transactions: [1, 2, 3]
7. Final index: live keys 5 snapshot seq 1
   crash:pending HTTP response: E_KEY_NOT_FOUND
8. All assertions: PASS
```

完整机器可读报告见 [artifacts/demo/interop-report.json](artifacts/demo/interop-report.json)，验证摘要见 [artifacts/verification.md](artifacts/verification.md)。

## 边界与说明

- 组件定位为单进程本地写者。它不提供多个操作系统进程同时写同一 `data-dir` 的协调；HTTP 服务内部保证单写者。
- 二进制格式的时间精度为毫秒，导出时会向下取整；JSON Lines 保留微秒。导入报告明确给出转换统计。
- 未知 JSON 字段保存在内部 `extras` 中，并在 JSON 导出时写回；紧凑二进制没有未知字段槽位。
- 快照和意图文件默认保留以便审计。快照切换释放的是内存索引/堆等旧状态资源，活动文件不会被提前删除，避免 Windows 上读者仍持有文件时删除失败。
- Windows 不提供目录 `fsync`；文件本身使用 `flush + os.fsync`，快照和 meta 使用 `os.replace` 原子替换。进程硬杀恢复依赖持久化意图、提交标记与段头事实。
