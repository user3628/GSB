# 本地特征迁移平台实现说明

## 1. 交付概览

- Go 1.22+，模块名 `featureplatform`。
- 嵌入式 SQLite 驱动：`modernc.org/sqlite`，无需 CGO 或外部数据库服务。
- 文件锁：`github.com/gofrs/flock`，Linux/macOS/Windows 均可使用真实文件锁。
- Unicode 处理：`golang.org/x/text/unicode/norm`。
- CLI：`cmd/featurectl`。
- HTTP 服务：`internal/httpapi`。
- 确定性演示：`featurectl demo`，包含三任务样本、一次跨任务版本增量、一次真实强杀恢复和一次 HTTP 优雅停止。

当前固定种子为 `20261125`，生成 3 个任务分片共 43 条样本：

- `text_classification`：14 条。
- `short_text_sentiment`：15 条。
- `intent_annotation`：14 条。

其中包含空文本、不可映射标签、低覆盖声明、旧任务版本降级、覆盖分母为零等确定性分支。

## 2. 快速验证

```bash
go mod download
gofmt -w cmd internal
go vet ./...
go test -count=1 ./...
go build -o bin/featurectl ./cmd/featurectl

# Linux/macOS
./bin/featurectl demo --dir .demo

# Windows PowerShell
go build -o bin\featurectl.exe .\cmd\featurectl
.\bin\featurectl.exe demo --dir .demo
```

演示会分别建立连续执行库与中断恢复库。它启动子进程执行迁移，在提交 2 个检查点后挂起，然后调用 `Process.Kill`；在 Linux 上即真实 `SIGKILL`。重启后仅凭 SQLite 已提交事实恢复，并比较两个库的规范导出摘要。

本次实测固定摘要：

```text
fixed_summary_digest=ca005fc1c0f8058c3014d68118ad61a17185c89b074404889dc1e12d3b7c3076
continuous=e4aec178a1c95a7b6b3d42b17e84198f37ba3b34d280a231749ef31bd4db7bd9
recovery=e4aec178a1c95a7b6b3d42b17e84198f37ba3b34d280a231749ef31bd4db7bd9
final_after_increment=fbf560ace768a9099b015ba06d981b87ae235469179d0ea1a3cfd261bf565114
```

固定摘要只包含逻辑结果，不包含工作目录、时间戳、SQLite 自增序号或进程信息；使用 `.demo-a` 与 `.demo-b` 两个不同目录运行，得到相同 `fixed_summary_digest`。

## 3. 手动 CLI / HTTP 流程

```bash
./bin/featurectl generate --dir data
./bin/featurectl import --db migration.db --shards data/shards.json
./bin/featurectl config put --db migration.db --file data/config.json
./bin/featurectl migrate --db migration.db --budget 100

./bin/featurectl increment --db migration.db --file data/increment.json
./bin/featurectl status --db migration.db --units
./bin/featurectl migrate --db migration.db --budget 100

./bin/featurectl read --db migration.db --sample SA-0001 --version 1
./bin/featurectl read --db migration.db --sample SA-0001 --version 2
./bin/featurectl summary --db migration.db
./bin/featurectl rejections --db migration.db
./bin/featurectl invalidations --db migration.db
./bin/featurectl export --db migration.db --out records.canonical.json

./bin/featurectl serve --db migration.db --addr 127.0.0.1:8080
./bin/featurectl stop --url http://127.0.0.1:8080
```

也可分阶段推进：

```bash
./bin/featurectl migrate --db migration.db --stage normalize
./bin/featurectl migrate --db migration.db --stage label
./bin/featurectl migrate --db migration.db --stage feature_map
./bin/featurectl migrate --db migration.db --stage coverage
./bin/featurectl migrate --db migration.db --stage record
```

主要 HTTP 路由：

- `POST /api/v1/demo/seed`
- `POST /api/v1/shards/import`
- `POST /api/v1/config/snapshots`
- `POST /api/v1/config/fragments`
- `POST /api/v1/migrate`
- `GET  /api/v1/status?include_units=1`
- `GET  /api/v1/fingerprints`
- `GET  /api/v1/read?sample_id=...&version=N`
- `GET  /api/v1/records`
- `GET  /api/v1/rejections`
- `GET  /api/v1/invalidations`
- `GET  /api/v1/summary`
- `GET  /api/v1/export`
- `POST /api/v1/admin/stop`

## 4. 特征映射规则

规范化顺序：

1. Unicode NFKC，兼容分解会完成全角 ASCII/半角等兼容字符统一。
2. `strings.Fields` 按 Unicode 空白切分并折叠为单个 ASCII 空格。
3. 切词：
   - 每个汉字输出 `han_unigram`。
   - 相邻两个汉字输出 `han_bigram`。
   - 最长连续拉丁字母串输出为小写 `latin`。
   - 其他字符作为分隔符。

特征位使用 `SHA-256("unified-feature/v1\n" + mapped_term)` 的前 8 字节对 `hash_dim` 取模。跨任务的同一统一词项落在同一特征位；每条 `FeatureBit` 记录：

- `feature_index`
- `source_task`
- `source_token`
- `mapped_term`
- `mapping_basis`
- `mapping_version`

记录使用结构体字段顺序和 `encoding/json` 的稳定 map key 排序生成规范 JSON，再计算 SHA-256。空数组固定输出为 `[]`，避免序列化/反序列化后 `nil` 与空切片摘要不同。

业务错误码均带 `sample_id` 与 `task`：

- `E_HASH_DIM_ZERO`
- `E_TASK_MAPPING_MISSING`
- `E_EMPTY_TEXT`
- `E_LABEL_UNMAPPED`
- `E_COVERAGE_LOW`
- `E_COVERAGE_DECL_INVALID`
- `E_TASK_VERSION_UNSUPPORTED`

## 5. 版本降级与覆盖口径

配置中每个任务可保留多版本映射规则。样本 `task_version` 命中旧规则时，使用旧映射版本并标记：

- `version_status=downgraded_historical`
- `W_TASK_VERSION_DOWNGRADE`

找不到对应规则时按 `version_policy` 处理：`use_fallback` 会在版本差不超过 `max_skew` 时使用显式 `fallback_versions`；`reject` 或超差则输出 `E_TASK_VERSION_UNSUPPORTED`。

覆盖声明采用显式分母：

```json
"coverage_declaration": {"denominator": 32, "declared_hit_bits": 22}
```

- `hit_bits`：样本实际激活的去重哈希位数。
- `coverage_ratio = hit_bits / denominator`。
- `threshold_margin = coverage_ratio - task_threshold`。
- 无声明或 `denominator=0` 时不推断覆盖率，标记 `coverage_applicable=false` 并计入 `not_applicable_sample_count` / `not_applicable_groups`。
- 声明的命中数或比率与实际不一致时产生警告；分母为负输出 `E_COVERAGE_DECL_INVALID`。

标签不可映射或覆盖不足的样本进入拒绝清单，但管线继续执行。有效记录数始终等于输入样本数；拒绝样本也是一条带状态和原因的当前有效产物。

## 6. 增量失效传播

配置快照版本必须单调加一。增量片段使用 JSON Pointer 风格路径提交局部改动，例如：

```json
{
  "fragment_id": "inc-sentiment-task-v2",
  "base_version": 1,
  "new_version": 2,
  "changes": [
    {"op": "replace", "path": "/task_versions/short_text_sentiment", "value": 2}
  ]
}
```

阶段指纹：

- `normalize`：规范化器版本。
- `label`：标签表版本与该任务的重编码表。
- `feature_map`：哈希维度、该任务版本、版本策略与该任务映射规则。
- `coverage`：该任务生效阈值。

选择重建集合时逐样本核对：

- 是否存在当前有效记录。
- 五个阶段检查点是否齐全。
- 检查点产物摘要是否与当前规范记录一致。
- 阶段配置指纹是否与当前配置一致。
- 规范化检查点中的样本内容指纹是否与导入样本一致。

只有不满足条件的样本进入重建集合；未受影响样本的旧检查点和当前记录直接复用。演示中的单任务版本变化得到 `15 affected / 28 reused / 15 rebuilt`。

以下情形设置 `pending_full_rebuild_version` 并触发全量：

- 增量显式 `force_full_rebuild`。
- `/task_mappings` 或 `/label_mappings` 被整体替换。
- 任务当前版本号回退。
- `hash_dim` 变更。
- 未知配置路径采用保守全量策略。

## 7. SQLite 检查点与恢复

关键表：

- `meta`：当前生效版本、最低可读版本、待全量重建版本。
- `config_snapshots` / `config_fragments` / `invalidations`。
- `samples`：样本规范 JSON、内容指纹、任务内连续序号。
- `checkpoints`：自增 `seq`、阶段、单元、样本指纹、配置版本、产物摘要、阶段指纹、状态。
- `record_versions`：记录的历史版本与 `is_valid` 标记。

部分唯一索引保证：

```sql
CREATE UNIQUE INDEX idx_checkpoints_one_valid
ON checkpoints(stage, unit_id) WHERE status='valid';

CREATE UNIQUE INDEX idx_record_versions_one_valid
ON record_versions(sample_id) WHERE is_valid=1;
```

重建时在事务内把旧有效检查点/记录置为无效，再插入新版本。最终记录与 `record` 检查点在同一事务提交。强杀后所有未提交事务回滚，已提交检查点保留；重新运行 `migrate` 或 `recover` 时通过 `PlanMigration` 找到缺失/失配单元并续跑。

规范导出只包含业务记录，不包含 SQLite `seq`、行 ID、文件路径或时间字段，因此恢复后的导出摘要与一次连续执行相同。

## 8. 陈旧读裁决

`ReadAdjudicated(sample_id, observed_version)` 的顺序：

1. `observed_version < current_version`：返回 `E_STALE_READ`，附带 `current_version` 与 `minimum_acceptable_version`。
2. `observed_version > current_version`：返回 `E_STALE_VERSION`。
3. 版本相等但没有当前记录：返回 `E_STALE_CHECKPOINT`，`missing_stage=record`。
4. 任一必需阶段检查点缺失、产物摘要不匹配或配置指纹不匹配：返回带 `missing_stage`、`sample_id`、期望/实际摘要的 `E_STALE_CHECKPOINT`。
5. 全部通过才返回记录。

因此当前版本下，未重建的受影响任务会被拒绝，而依赖未变化的跨任务记录可直接返回。

## 9. 并发提交

每个 `Store` 写事务：

1. 获取进程内互斥锁。
2. 获取 SQLite 文件旁的真实 `*.lock` 文件锁。
3. 开启事务。
4. 读取当前版本并执行条件更新，例如：

```sql
UPDATE meta
SET value=?
WHERE key='current_config_version' AND value=?;
```

配置版本必须等于当前版本加一。特征记录首次提交必须满足任务内序号 `last_sequence+1`；重建必须携带正确的当前有效记录 ID。落后、重复、跨任务或序号错位均返回 `E_STALE_VERSION` 并附 `current_version`。

实测 8 个独立 CLI 进程并发提交同一初始配置：

```text
success=1 stale=7 other=0
```

Go 测试 `TestConcurrentConfigCommitsUseConditionalVersion` 也覆盖了多个独立 `Store` 实例的竞争提交。

## 10. 覆盖汇总一致性

`BuildSummary` 先从 SQLite 读取当前有效记录，再显式按 `sample_id` 排序，然后累加：

- `sample_count`
- `hit_feature_bits`
- `coverage_hit_bits`（仅适用样本的分子）
- `rejected_records`
- `coverage_denominator`
- `applicable_sample_count`
- `not_applicable_sample_count`
- `coverage_ratio`
- `coverage_threshold`
- `threshold_margin`

分组键为 `(task, unified_label, mapping_version)`。当组内分母总和为零时输出 `coverage_applicable=false`，省略比率和余量，并增加 `not_applicable_groups`。汇总的 `input_sample_count == valid_record_count == 43` 且 `conservation_ok=true`。

## 11. 实测结果

验证环境：`go version go1.27.4 linux/amd64`；`go.mod` 目标指令为 `go 1.22`，依赖版本已选到支持 Go 1.22 的系列。

```text
go vet ./...: exit 0
go test -count=1 ./...: all packages ok
GOTOOLCHAIN=go1.22.10 go test -count=1 ./...: ok
GOOS=windows GOARCH=amd64 go build ./cmd/featurectl: ok
demo: exit 0
recovery_matches_continuous=true
increment=15 affected / 28 reused / 15 rebuilt
summary=43 input / 43 valid / 9 rejected / 4 N/A samples / 1 N/A group
```

自动化测试覆盖：

- NFKC/全半角、中文 unigram/bigram、Latin token 与逐字节确定性。
- `E_HASH_DIM_ZERO`、`E_TASK_MAPPING_MISSING`、`E_EMPTY_TEXT`、`E_LABEL_UNMAPPED`。
- 三任务全量迁移与计数守恒。
- 单任务增量最小失效集合。
- `E_STALE_READ` 与 `E_STALE_CHECKPOINT`。
- 上下文中断后的恢复摘要一致性。
- 并发配置版本条件提交。
- 跨任务错位和过期记录版本提交。
- 版本回退与整表替换的全量计划。

原始测试日志见 `artifacts/go-test.log` 与 `artifacts/go1.22-test.log`，完整演示 JSON 见 `artifacts/demo-summary.json`。
