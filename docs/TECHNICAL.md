# TM Sync 技术说明

本地译后编辑翻译记忆库同步服务。Go 1.22+、SQLite、CGO；不依赖 Docker、GPU 或外部数据集。

## 快速运行

```bash
go test ./...
./scripts/verify.sh

go run . demo --dir demo-data --db demo-data/tm.db
go run . serve --db demo-data/tm.db --addr 127.0.0.1:18080
```

浏览器或 curl：

```bash
curl 'http://127.0.0.1:18080/api/v1/suggestions?q=%E6%8F%90%E4%BA%A4&invalid=1'
curl 'http://127.0.0.1:18080/api/v1/stages'
curl 'http://127.0.0.1:18080/api/v1/units/export?format=jsonl'
curl -X POST 'http://127.0.0.1:18080/api/v1/recover'
go run ./cmd/tm-sync stop --addr 127.0.0.1:18080
```

`scripts/verify.sh` 会运行单元/端到端测试、确定性演示、两次真实 `SIGKILL` 恢复、16 进程并发发布竞争和 HTTP 冒烟。
`serve` 启动时会自动执行一次恢复扫描；空数据库会跳过 `E_NO_SOURCE_IMPORTED`。

## 代码结构

- `internal/tm/text.go`：NFKC、Unicode 空白折叠、全半角兼容、段落保留和带 rune 偏移切句。
- `internal/tm/align.go`：字符 n-gram F1 覆盖、长度比、语言标签相容性、对数域几何合成、阈值和确定性互斥仲裁。
- `internal/tm/merge.go`：记忆单元归并、出现计数、质量/序号/分数仲裁、建议生成、规范导出和全量差异比较。
- `internal/tm/generator.go`：固定种子 `2026112601` 样本，生成 3 个分段源文件和 2 个增量文件。
- `internal/store/store.go`：SQLite schema、WAL、真实 `flock` 文件锁、事务辅助和单调检查点序号。
- `internal/service/service.go`：导入、版本判定、增量应用、最小失效集合和原因链。
- `internal/service/sync.go`：基于已提交检查点的分阶段/恢复循环。
- `internal/service/units.go`：单元事务化重建、规范导出、建议和原因链查询。
- `internal/service/publish.go`：段内序号条件更新与 `E_STALE_CHECKPOINT`。
- `internal/service/stage.go`：显式阶段推进、非法跳转和缺失前置产物错误。
- `internal/api/server.go`：本地 HTTP JSON/JSONL 接口。
- `internal/cli/cli.go`：HTTP 服务与全部 CLI 子命令；根目录 `main.go` 和 `cmd/tm-sync/main.go` 都是薄入口。

对齐阈值可通过 `TM_ALIGN_THRESHOLD` 覆盖；默认值为 `0.16`。

## 数据流和一致性边界

1. 导入分段源后，`segments.raw_json` 保存原始事实，`segment_versions` 保存版本历史。
2. `segmentation`、`alignment`、`merge`、`invalidation` 阶段按段写活动检查点；`unit` 阶段按单元写活动检查点。
3. 检查点包含 `seq_no`、阶段名、段/单元 ID、段内容指纹、生效版本、产物摘要和 JSON 明细。
4. 同一 `(stage, segment_id)` 或 `(stage, unit_id)` 只允许一条 `status='active'` 记录；重跑时在同一事务内把旧记录置为 `superseded` 再插入新记录。
5. 一个记忆单元的 `units` 更新、建议更新/插入和 `unit` 检查点在同一个 SQLite 事务中提交。
6. 增量导入在一个事务内校验 `base_version`、更新段、标记共享单元/对等段/旧建议、写原因链并推进生效版本。
7. 恢复不依赖退出报告：新进程把遗留 `running` 运行标记为 `interrupted`，随后只补齐缺失或指纹过期的检查点和未完成单元。
8. 增量路径只用数据库中的当前语料做只读全量 oracle 比对；oracle 结果不会覆盖增量状态。不一致时返回 `E_INCREMENTAL_MISMATCH`。

## 版本判定

- 新版本号大于当前生效版本：`normal_advance`。
- 相同版本号且内容摘要变化：`whole_replacement`，所有活动单元、建议和检查点失效，记录 `previous_digest`。
- 版本号小于当前生效版本：`E_VERSION_ROLLBACK`，详情含 `rejected_version` 与 `current_version`。
- 增量的 `base_version` 不等于当前生效版本：`E_STALE_BASE`，详情含 `current_version` 与 `base_version`。

## 并发发布

发布通道以 `segment_id` 为 lane，段内序号从 1 连续递增。`Service.WithTx` 先获取数据库同名 `.lock` 上的 `flock(LOCK_EX)`，事务内执行：

```sql
INSERT INTO publish_cursors(segment_id,last_seq) VALUES(?,?)
ON CONFLICT(segment_id) DO UPDATE SET last_seq=excluded.last_seq
WHERE publish_cursors.last_seq=?;
```

- `seq < current`：`behind`。
- `seq == current`：`duplicate`。
- `seq > current+1`：`cross_segment_or_gap`。
- 可选 `checkpoint_seq` 会验证检查点存在、有效且属于同一段，跨段检查点返回 `cross_segment_checkpoint`。

## HTTP 接口

- `POST /api/v1/sources/import?version=1`：body 为 JSONL 或 JSON 数组。
- `POST /api/v1/increments/import`：导入并应用增量。
- `POST /api/v1/sync`：`{"mode":"incremental|full","segment_id":""}`。
- `POST /api/v1/sync/stage`：显式推进单个阶段。
- `POST /api/v1/recover`：标记中断运行并按已提交事实恢复。
- `GET /api/v1/status`、`GET /api/v1/stages`、`GET /api/v1/versions/fingerprint`。
- `GET /api/v1/checkpoint?stage=alignment&segment_id=...`：查看活动检查点及完整仲裁明细。
- `GET /api/v1/suggestions?q=...&invalid=1`。
- `GET /api/v1/units`、`GET /api/v1/units/export`、`GET /api/v1/units/export?format=jsonl`。
- `GET /api/v1/export`：单元和建议的规范字节导出。
- `POST /api/v1/publish`。
- `POST /api/v1/versions/records`。
- `POST /api/v1/admin/stop`。

CLI 子命令包括 `generate`、`demo`、`serve`、`stop`、`import-source`、`import-increment`、`sync`、`stage`、`recover`、`status`、`suggest`、`export-units`、`publish`、`version-record`。

## 稳定业务码

| Code | 触发场景 | 关键详情 |
| --- | --- | --- |
| `E_SEGMENT_EMPTY` | 空目标、仅标点或规范化后无有效句段 | `segment_id`, `stage=segmentation`, `side` |
| `E_ALIGN_INVALID` | 对齐分非有限值或长度比为 0 | `segment_id`, `source_offset`, `target_offset` |
| `E_STALE_BASE` | 增量基础版本不匹配 | `base_version`, `current_version` |
| `E_INCREMENTAL_MISMATCH` | 增量重建与只读全量 oracle 不一致 | `first_unit_id`, `incremental_digest`, `full_digest` |
| `E_VERSION_ROLLBACK` | 拒绝旧版本号 | `rejected_version`, `current_version` |
| `E_STALE_CHECKPOINT` | 落后、重复、断号或跨段发布 | `submitted_seq`, `current_seq`, `reason` |
| `E_INVALID_STAGE_TRANSITION` | 显式阶段跳转不合法 | `segment_id`, `from_stage`, `to_stage` |
| `E_MISSING_PREREQUISITE` | 缺少前置阶段产物或检查点 | `segment_id`, `stage`, `prerequisite` |
| `E_SEGMENT_NOT_FOUND` | 增量或阶段引用未知段 | `segment_id` |

## 确定性样本

固定种子生成 12 个段，写入：

- `sources/source-001.jsonl`
- `sources/source-002.jsonl`
- `sources/source-003.jsonl`
- `increments/increment-001.jsonl`：修改跨 3 段重复的“请立即提交。”
- `increments/increment-002.jsonl`：修改跨 2 段重复的“请及时保存。”

样本包含中英混排、空目标、仅标点目标、重复源文、40+ 编号长句段以及越界质量标记 `quality=4096`。两个无有效译文的段按预期返回 `E_SEGMENT_EMPTY`，其余单元和建议可完成同步。

## 中断测试钩子

`sync` 和 `recover` 支持隐藏验收参数 `--crash-after N`：第 N 个检查点事务提交后，进程立即向自身发送 `SIGKILL`。这模拟强制终止；不会执行 defer 或退出报告。重启后仅依据 SQLite 已提交记录恢复。
