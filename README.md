# imretrace：确定性图文对齐、量化误差追踪与恢复工作台

这是一个无外部数据、无 Docker、可离线构建的最小完整演示。程序用固定种子生成 9 个图文分片，
完成视觉/文本特征、跨模态投影、8/4-bit 定点量化、双向检索、误差上界诊断、SQLite 事务检查点、
强制终止恢复、并发发布互斥、策略增量/整体替换/回退，以及 CLI/HTTP 工作台。

原始任务草案保留在 `docs/REQUIREMENTS.md`，实际验收记录见 `docs/VERIFICATION.md`。

## 环境

- 已验证：Go `go1.27.4 linux/amd64`；`go.mod` 声明 `go 1.22.0`。
- SQLite CLI：`3.46.1`。
- `libsqlite3-dev` 已安装；驱动使用纯 Go 的 `modernc.org/sqlite`，运行期不依赖 C 编译参数。
- 模块依赖已在本地缓存。已设置 `GOFLAGS=-mod=mod`，可用 `GOPROXY=off` 离线构建。

```bash
go version
sqlite3 --version
go env -w GOFLAGS=-mod=mod
GOPROXY=off go build ./...
```

## 30 秒演示

```bash
make build
./bin/imretrace prepare
./bin/imretrace build
./bin/imretrace search-text --q 'red circle 红色圆形 blue gradient' --k 5
./bin/imretrace search-image --item item-shard-0001 --k 5
./bin/imretrace serve --addr 127.0.0.1:8765
```

工作区当前已生成演示状态：v1=8-bit，v2=增量 4-bit，v3=整体替换（grid=4、权重重置）后重建。
诊断报告在 `artifacts/verification/workspace-diagnostics-report.json`。

一键完整验收：

```bash
./scripts/acceptance.sh
```

脚本会在临时目录验证测试、双向检索、异常码、4-bit、零权重、整体替换、回退、SQLite 唯一性、
真实 `SIGKILL` 后逐字节恢复、报告导出、HTTP 检索和优雅停止；日志写入
`artifacts/verification/acceptance-run.log`。

## 目录与数据位置

所有运行数据默认在工作区 `data/`：

- `data/imretrace.sqlite`：恢复事实来源，WAL 模式。
- `data/config/default_params.json`：固定种子生成参数。
- `data/config/policy.v*.json`、`policy.current.json`：策略快照。
- `data/demo/manifest.jsonl`：全部图文对清单。
- `data/demo/shards/shard-0001/...`：9 个分片、PNG 与分片 JSONL。
- `data/exports/diagnostics.json`：默认诊断报告。
- `data/locks/publish.lock`：真实 `flock(2)` 发布锁。

仓库内 `config/default_params.json` 与 `config/policy.default.json` 是默认模板。

## CLI

```bash
./bin/imretrace prepare
./bin/imretrace generate
./bin/imretrace import --path data/demo/manifest.jsonl
./bin/imretrace import --path data/demo/shards
./bin/imretrace build [--crash-after-stage 1|2|3|4]
./bin/imretrace recover
./bin/imretrace status
./bin/imretrace state-hash
./bin/imretrace report --out artifacts/diagnostics.json
./bin/imretrace search-text --q 'green triangle 绿色三角' --k 5
./bin/imretrace search-image --item item-shard-0003 --k 5
./bin/imretrace policy show
./bin/imretrace policy patch --file patch.json
./bin/imretrace policy replace --file full-policy.json
./bin/imretrace policy replace --next-demo
./bin/imretrace serve --addr 127.0.0.1:8765
```

全局可加 `--root /path/to/workspace`；默认当前目录。

增量片段必须且只能包含一个可调字段，例如：

```json
{"base_version": 1, "quant_bits": 4}
```

```json
{"base_version": 2, "grid": 4}
```

```json
{"base_version": 3, "fusion_weights": {"cosine": 0.5, "term": 0.5}}
```

```json
{"base_version": 4, "candidate_depth": 4}
```

整体替换请求使用 `"kind":"full"` 和更高 `version`；低版本返回 `E_STALE_VERSION`。

## HTTP 工作台

启动 `serve` 后访问根路径。主要 API：

- `GET /api/status`：策略、版本指纹、分片序号、活动检查点、规范状态哈希。
- `POST /api/prepare`、`POST /api/generate`、`POST /api/build`、`POST /api/recover`。
- `POST /api/search/text`：body `{"query":"...","k":5}`。
- `POST /api/search/image`：body `{"item_id":"...","k":5}`。
- `POST /api/policy/patch`、`POST /api/policy/replace`。
- `GET /api/report`、`POST /api/export`。
- `POST /api/stop`：返回响应后优雅关闭 HTTP server。

错误响应为 `{"ok":false,"error":{"code":"E_...", ...}}`；查询类错误在 `error.partial`
中保留已计算候选和分量。

## 实现要点

**特征阶段**

- 视觉：`grid × grid` 切块，每块拼接 RGB 均值 3 维、RGB 标准差 3 维、8 bin 梯度方向直方图，
  固定为 `14·grid²` 维；默认 grid=3，即 126 维。
- 文本：显式全角/半角处理、Unicode NFKC、Unicode 空白折叠；提取拉丁词、CJK unigram/bigram、
  词袋计数和字符 2/3-gram，稳定哈希到 512 维。
- 空 caption、空白/常量图像、`grid<=0` 返回 `E_FEATURE_INVALID`，含 `item_id` 和
  `stage=feature_extract`。

**投影与量化**

- 共享空间默认 24 维。前 16 个语义轴对应颜色、形状、明暗，其余为稳定保留轴。
- 投影矩阵由确定性样本和语义目标通过核岭回归闭式拟合，矩阵摘要写入阶段 2 产物；即席查询使用
  SQLite 中恢复出的同一矩阵。
- 支持 8-bit/4-bit、逐维 `dim` 或整向量 `vector` 粒度；记录 `range_min/range_max/scale/step/
  zero_point/quant_scale/param_digest`。
- 矩阵维度不符、缩放因子为零、量化向量全零返回 `E_QUANT_INVALID`，含 `item_id` 与 `dim`。

**误差传播**

对参考向量 `x`、反量化向量 `x̂` 和参数表，逐维计算：

```text
outside_d = max(range_min_d - x_d, 0, x_d - range_max_d)
δ_d       = outside_d + step_d / 2
||e||₂    <= sqrt(sum δ_d²)
```

若视觉/文本两路界为 `E_v/E_t`，高精度余弦为 `c_ref`，则：

```text
B_cos <= (E_v + E_t + E_vE_t)(1 + |c_ref|) / ((1-E_v)(1-E_t))
B_final = w_cos/(w_cos+w_term) · B_cos
```

每条候选输出视觉路/文本路的 L2 实际误差与上界、`log_norm_contribution`、
`log_error_contribution`、有效 bit、有效十进制位、余弦参考值/量化值、词项分、融合分和来源。
语料级诊断给出 1…1024 样本规模的 union/sum/RMS/聚合 L2 估计。

**检查点与并发**

每个分片按 1→4 连续发布：

1. `feature_extract`
2. `project_quantize`
3. `index_build`
4. `recall_verify`

`checkpoints` 表保存阶段、`item_id`、输入指纹、策略版本/摘要、量化参数指纹、产物摘要、
单调序号和完整 canonical payload。部分唯一索引保证活动记录中 `(shard_id,stage)` 与
`(shard_id,seq)` 均只有一条。

发布时先获取 `flock(LOCK_EX)`，再开启 SQLite 立即事务，读取 `shards.current_seq`，用
`UPDATE ... WHERE shard_id=? AND current_seq=?` 做条件推进。重复、落后、跳号或跨分片发布返回
`E_STALE_CHECKPOINT` 和 `current_seq/expected_seq`。

策略整体替换会在同一事务中把旧检查点置为 `superseded` 并把分片序号归零，因此必须重建。

## 主要业务码

- `E_FEATURE_INVALID`：空 caption、空白图像、grid 非法。
- `E_QUANT_INVALID`：矩阵维度、零缩放、全零量化向量。
- `E_NUMERIC_MISMATCH`：量化/参考偏差超过策略容差，附最大绝对/相对偏差。
- `E_QUERY_INVALID`：空查询、`k` 超过索引规模、融合权重全零；保留 partial。
- `E_STALE_VERSION`：策略回退或版本内容冲突，附当前版本和最小可接受版本。
- `E_STALE_CHECKPOINT`：序号落后/重复/跳号/跨分片，附当前序号。

## 代码地图

- `cmd/imretrace/main.go`：CLI 与服务启动。
- `internal/model`：策略、错误码、路径、canonical hash。
- `internal/dataset`：固定种子 PNG/JSONL 生成器。
- `internal/features`：视觉描述子与 NFKC 文本特征。
- `internal/quant`：投影矩阵、标定、量化、误差界。
- `internal/reference`：Kahan float64 高精度参考实现。
- `internal/search`：双向检索、融合、分数分解、语料误差估计。
- `internal/store`：SQLite schema、事务、文件锁、条件发布。
- `internal/pipeline`：分阶段构建、恢复、策略切换、报告。
- `internal/web`：HTTP API 与内嵌单页工作台。
- `scripts/acceptance.sh`：端到端验收脚本。
