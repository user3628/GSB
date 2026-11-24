# rewardpack

`rewardpack` 将历史偏好标注加工为可训练、可审计、可增量重建的奖励特征包。实现使用 Go 1.22、纯 Go SQLite 驱动 `modernc.org/sqlite` 与本地文件锁，不依赖 CGO、Docker 或网络。依赖已放入 `vendor/`，可直接离线构建；仓库内生成代码针对当前 Linux/amd64 与目标 Windows/amd64 保留。

## 快速开始

```bash
go build -o rewardpack ./cmd/rewardpack

# 仓库内已有 sample/，也可以重新生成
./rewardpack generate --out sample --seed 42

./rewardpack build \
  --db /tmp/reward.sqlite \
  --config sample/config.yaml \
  --shards sample/shards \
  --out /tmp/reward-artifacts

./rewardpack verify --db /tmp/reward.sqlite
./rewardpack status --db /tmp/reward.sqlite
```

`sample/` 包含多来源中文、英文、中英混排、同 prompt 多偏好对以及 `stress.jsonl` 中的空回复、纯标点、越界/负一致度样本。`sample/annotations.jsonl` 故意混合了非法文本，直接构建会按契约返回 `E_EMPTY_TEXT`；常规完整构建使用 `sample/shards/`。

## 处理链路

五个产物阶段按固定顺序发布，非法跨阶段发布会返回 `E_ILLEGAL_STAGE` 或 `E_MISSING_ARTIFACT`：

1. `normalize`：Unicode NFKC、空白折叠、小写、中文单字/拉丁连续串分词；每个 token 同时记录符文和字节半开区间。
2. `score`：长度惩罚、prompt 词项重叠方向、标注一致度三个乘性因子在对数域求和。
3. `features`：词项差分特征投影到配置维度，使用 256 位 `big.Float` 做单位归一化和定点量化参考计算。
4. `budget`：source 配额、全局 token 上限、语言覆盖与确定性并列仲裁。
5. `report`：逐对归因、特征摘要、保留/淘汰依据和空来源层。

另有 `verify` 操作：增量结果与从零全量构建逐字节比较。阶段顺序常量为 `normalize, score, features, budget, report`，定义在 `types.go`。

### 规范化

`NormalizePair` 的输出完全由输入和配置决定。`TextFeatures` 包含：

- `normalized`：NFKC、空白折叠、小写后的文本。
- `tokens`：`text/start/end/byte_start/byte_end`，偏移均基于规范化文本。
- `terms`：按词项排序的 `{term,count}` 集合。
- `token_count`、`char_count`、`language`：`zh/en/mixed/und`。

chosen 或 rejected 在规范化后没有 token 时返回：

```json
{"code":"E_EMPTY_TEXT","pair_id":"...","field":"chosen","stage":"normalize","message":"..."}
```

### 因子打分

令 `c/r` 为 chosen/rejected token 数，`p` 为 prompt，`w(t)` 为配置词项权重：

- 长度因子：`L = exp(-length_penalty * abs(log((1+c)/(1+r))))`
- 重叠因子：`O = exp(clamp(overlap_scale * (coverage(chosen,p)-coverage(rejected,p)), -1, 1))`
- 一致度因子：`C = consistency`；越界时使用 `factor_floors.consistency` 并标记原因。
- 最终强度：`strength = exp(log(L)+log(O)+log(C))`

每个因子输出 `value`、`log_contribution`、`fallback`、`fallback_reason`。最终值按 `strength_significant_digits` 舍入，并同时输出 `strength_text` 与 `effective_digits`。因子不可计算时回退到配置下限；NaN/Inf 返回带 `pair_id` 与 `factor` 的 `E_NUMERIC`，不会静默置零。

### 投影与量化

基础向量将 chosen/rejected 词项计数差、长度方向和强度散列到 `features.dimension` 个桶，再按矩阵投影。投影与 L2 单位归一化使用 256 位精度。量化使用对称全局缩放：

`scale = (2^(bits-1)-1) / max(abs(reference))`

每一维输出 `scale`、`reference`、`quantized`、`error_bound=0.5/scale` 与 `observed_error`。以下情况返回带 `row/column` 的 `E_DIM_MISMATCH`：

- 投影行宽不等于 `features.dimension`；
- 投影矩阵包含 NaN/Inf；
- 量化缩放因子为零或不可有限表示。

### 预算裁决

`DecideBudget` 按 source 配置配额做整数分配；总请求超过全局预算时使用最大余数法按 source 名称稳定仲裁。选择顺序：

1. 按 `required_languages` 和 `language_minimums` 预留语言覆盖；
2. 其余候选按 `strength` 降序、`token_cost` 升序、`pair_id` 升序填充；
3. 同时检查 source 剩余配额与全局剩余 token。

`token_cost = chosen.token_count + rejected.token_count`。每个来源都出现在 `source_layers` 中：可用对为零为 `EMPTY`，配额低于 `min_retention_unit` 为 `QUOTA_BELOW_MIN_UNIT`，不会被静默跳过。每个淘汰对都有 `reason` 和数值化 `detail`。

## 增量与失效

每个阶段保存可独立复用的分片摘要：

- normalize 分片输入摘要：标注原始 JSON 摘要 + normalization 配置摘要；
- score 分片输入摘要：normalize 产物摘要 + scoring 配置摘要；
- features 分片输入摘要：score 产物摘要 + features 配置摘要；
- budget/report：上游全部摘要 + 对应配置摘要。

单条编辑只重算该 pair 的 normalize/score/features，再重算全局 budget/report。source 整片替换只替换该 source 的逐对产物；其他 source 的 `pair_artifacts` 保持原摘要。build manifest 记录全部 pair/source/配置摘要，并用于识别：

- `initial_build` / `append` / `annotation_edit` / `source_replace` / `rollback` / `config_change` / `noop`

`rollback` 包括内容回退到已提交历史快照；配置版本号回退则返回 `E_VERSION_ROLLBACK`。失效记录存入 `invalidations`，包含 `earliest_stage`、`invalidated_stages`、`affected_sources`、`affected_pairs` 与 `first_diff_pair_id`。`status` 命令可查询。

`verify` 会先执行增量构建，再在临时空数据库中从零构建，并对 normalize、score、features、budget、report 的规范 JSON 逐字节比较。不一致返回 `E_INCREMENTAL_MISMATCH`，附带首个差异 `pair_id` 与两路取值。

## 检查点、并发与恢复

SQLite 表：

- `config_snapshots`：不可变配置版本，拒绝版本回退和同版本内容变化。
- `sources` / `annotations` / `pending_changes`：当前输入与变更日志。
- `stage_state`：每阶段当前有效序号、输入指纹、配置版本、产物摘要。
- `checkpoints`：检查点历史；每个 `(stage,input_fingerprint)` 仅一条 `valid=1`。
- `pair_artifacts`：逐 pair 可复用产物及输入摘要。
- `invalidations` / `manifest_history` / `meta`：失效范围、历史快照与当前构建清单。

发布在一个 SQLite 事务内执行条件更新 `WHERE stage=? AND seq=?`，并校验上一阶段产物摘要。落后、重复或跨阶段错位发布返回 `E_STALE_CHECKPOINT`。构建器额外持有真实 OS 文件锁（Linux `flock`，Windows `LockFileEx`），因此多进程不会同时推进发布；CLI 的配置/分片同步与构建序列另有命令级进程锁，SQLite busy 还会在事务层做有界重试并最终返回 `E_LOCK_TIMEOUT`。

收到取消时，当前不可拆分 pair 单元完成后返回 `E_STOPPED`，未完成阶段不会发布。下次只使用已提交事实恢复；恢复产物与完整运行逐字节一致。

## 稳定错误码

| 错误码 | 场景 |
| --- | --- |
| `E_EMPTY_TEXT` | chosen/rejected 规范化后为空或仅标点 |
| `E_NUMERIC` | 因子、标注一致度或归一化出现 NaN/Inf |
| `E_DIM_MISMATCH` | 投影维度/有限性/量化缩放因子错误，附行列 |
| `E_STALE_CHECKPOINT` | 序号落后、重复输入、跨阶段错位、并发落败 |
| `E_MISSING_ARTIFACT` | 缺失前置阶段或逐对产物 |
| `E_ILLEGAL_STAGE` | 未知阶段或非法跳转 |
| `E_VERSION_ROLLBACK` | 配置版本回退 |
| `E_VERSION_CONFLICT` | 同版本不同配置内容 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量产物不一致 |
| `E_STOPPED` | 优雅停止，可恢复 |
| `E_LOCK_TIMEOUT` | 文件锁或 SQLite busy 超过等待上限 |
| `E_INVALID_CONFIG` / `E_INVALID_INPUT` | 配置或输入非法 |

错误均以 JSON 输出，包含稳定 `code` 和可用诊断字段。

## 命令行

```text
rewardpack generate --out DIR [--seed 42]
rewardpack validate --config FILE --input JSONL
rewardpack config --db FILE --config FILE
rewardpack build --db FILE [--config FILE] [--shards DIR] [--out DIR]
rewardpack edit --db FILE --file ANNOTATION.json
rewardpack replace-source --db FILE --source NAME --file SHARD.jsonl
rewardpack verify --db FILE [--out DIR]
rewardpack status --db FILE
```

单条编辑示例：

```bash
cat > /tmp/pair.json <<'JSON'
{"pair_id":"a-001","prompt":"解释一下缓存穿透","chosen":"使用布隆过滤器并缓存空值。","rejected":"直接忽略。","source":"shard-a","lang":"zh","consistency":0.42}
JSON
./rewardpack edit --db /tmp/reward.sqlite --file /tmp/pair.json
./rewardpack build --db /tmp/reward.sqlite --out /tmp/reward-artifacts
./rewardpack verify --db /tmp/reward.sqlite
```

单 source 整片替换：

```bash
./rewardpack replace-source \
  --db /tmp/reward.sqlite \
  --source shard-b \
  --file /tmp/new-shard-b.jsonl
./rewardpack build --db /tmp/reward.sqlite
```

导出的 `scores.jsonl`、`features.jsonl`、`retained.jsonl`、`eliminated.jsonl`、`budget.json`、`report.json` 和 `bundle.json` 均为确定性 JSON。

## 测试与验证

```bash
go test ./...
go test -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

当前实现的实际验证结果：

- `go test ./...`：通过。
- `go test -race ./...`：通过。
- `go vet ./...`：通过。
- Windows amd64 无 CGO 交叉编译：通过。
- 确定性样本 `seed=42`：12 个有效 pair，覆盖 4 个有效 source、1 个配置空来源、1 个低于最小保留单位的来源。
- 单条 `a-001` 编辑：仅 `normalize/score/features` 的 `a-001` 被重建；`verify` 返回 `byte_identical=true`。
- source 整片替换：仅目标 source 的逐对摘要变化，其他 source 摘要不变；`verify` 通过。
- 停止后恢复：resume 与从零全量构建的 score/feature/budget/report 字节一致。
- 两个独立 CLI 进程并发构建同一数据库：两个进程均成功、report digest 相同、`verify` 字节一致，阶段序号只推进一次。

测试文件：`core_test.go`、`checkpoint_test.go`、`invalidation_test.go`。

## Windows 注意事项

- 确认 GOPATH、模块缓存和数据库目录可写。
- `modernc.org/sqlite` 为纯 Go，不要求安装 GCC 或启用 CGO。
- 数据库目录不要被编辑器/同步工具长期占用；WAL 会创建 `-wal`/`-shm` 伴随文件。
- 构建锁使用独立 `<db>.lock` 文件与 Windows `LockFileEx`，不会依赖 SQLite 文件句柄锁。
- SQLite CLI 可直接检查：`sqlite3 reward.sqlite '.tables'`。

依赖已 vendored（linux/amd64 + windows/amd64）；如删除 `vendor/`，离线环境需要预先准备 Go 模块缓存。
