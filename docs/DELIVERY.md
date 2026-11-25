# 多路召回融合实验组件 — 交付说明

首个可运行版本使用 Go 标准库、`golang.org/x/text` 做 Unicode NFKC，以及 `github.com/mattn/go-sqlite3` 做事务化检查点。模块指令为 `go 1.22`，已在本机 `go1.27.4 linux/amd64` 验证。

## 快速验证

```bash
# 若默认 proxy.golang.org 超时，使用已验证可用的镜像
export GOPROXY=https://goproxy.cn,direct

go test -count=1 ./...
bash scripts/verify.sh
```

`scripts/verify.sh` 会在 `mktemp -d /tmp/multirecall-verify-XXXXXX` 中：

1. 运行全部 Go 测试；
2. 构建 CLI；
3. 首次运行写出确定性物品库、查询集和配置快照；
4. 比较顺序与 4 worker 并行报告字节；
5. 在第 4 个检查点事务提交后向自身发送 `SIGKILL`，重开 SQLite 恢复并与连续执行结果逐字节比较；
6. 用 `sqlite3` 检查有效检查点；
7. 在 `127.0.0.1:18080` 检查健康、召回、`E_EMPTY_QUERY` 和 `E_STALE_BASE`；
8. 通过 HTTP 批量接口写入并读取检查点。

最近一次实际运行结果保存在 `artifacts/verification/verify-run.log`，结论为 `ALL_VERIFICATION_CHECKS_PASSED`。

## 常用命令

```bash
go run . init --data data  # 或 go run ./cmd/multirecall init --data data
go run .                  # 无子命令默认在 127.0.0.1:18080 启动服务
go run ./cmd/multirecall run --data data --out sequential.json --mode sequential
go run ./cmd/multirecall run --data data --out parallel.json --mode parallel --workers 4
cmp sequential.json parallel.json

go run ./cmd/multirecall run \
  --data data --db data/checkpoints.db --out recovered.json \
  --mode sequential

go run ./cmd/multirecall serve --data data --addr 127.0.0.1:18080
```

强制终止演练：

```bash
go run ./cmd/multirecall run --data data --db cp.db \
  --kill-after-commits 4 --mode sequential
# shell 中可见 Killed，退出码为 137；随后用同一 --db 正常运行即可恢复
```

## HTTP 接口

- `GET /healthz`
- `GET /v1/config`
- `POST /v1/config/fragment`：局部片段，校验 `base_version`
- `POST /v1/config/replace`：完整快照替换/回退识别
- `GET /v1/dataset`
- `GET /v1/budget?query_count=10`
- `GET /v1/recall?query_id=Q001`
- `POST /v1/recall`：可用 `query_id` 或临时 `raw_query`
- `POST /v1/recall/batch`：body 如 `{"workers":4,"use_checkpoint":true}`
- `GET /v1/checkpoints?query_id=Q001`

错误响应使用统一结构，错误码包括 `E_EMPTY_QUERY`、`E_VECTOR_INVALID`、`E_NO_WEIGHT`、`E_NUMERIC_MISMATCH`、`E_STALE_BASE`、`E_STALE_VERSION`、`E_CHECKPOINT`。

## 代码地图

- `internal/sample/sample.go`：固定种子 `30820261126`，生成 24 个物品、10 个查询和默认配置。
- `internal/textnorm/textnorm.go`：NFKC、全半角兼容、空白折叠、小写化、中文单字/相邻二字/拉丁连续串切分。
- `internal/recall/lexical.go`：倒排、IDF/饱和度定点权重、命中词项明细和误差界。
- `internal/recall/category.go`：历史点击路径的公共前缀深度与近因整数加权。
- `internal/recall/vector.go`：显式 `vector_scale` 还原、维度/NaN/零范数校验、缩放定点坐标与余弦。
- `internal/recall/fusion.go`：`weighted_sum` 与 `reciprocal_rank`、多路来源、对数贡献、排序、去重和截断。
- `internal/recall/audit.go`：误差传播、稳定性裁决、`big.Float` 256-bit 融合参考和固定抽样位。
- `internal/recall/budget.go`：千分比深度、全局评估次数和确定性成本/时间缩减。
- `internal/recall/engine.go`：无 SQLite 的工作池、查询边界取消和按 `query_id` 排序归约。
- `internal/store/checkpoint.go`：SQLite schema、单事务旧记录失效+新记录插入、部分唯一索引。
- `internal/orchestrator/orchestrator.go`：分阶段指纹、检查点复用、并行检查点编排。
- `internal/configx/config.go`：配置摘要、片段合并、版本推进/替换/回退识别和最早失效阶段。
- `internal/server/server.go`：回环 HTTP API。
- `tests/acceptance_test.go`：验收测试，包含真实 CLI `SIGKILL` 后重启比对。

## 关键语义

### 数值与稳定性

- 词法和向量路分数使用 `1e9` 定点尺度；融合贡献使用 `1e9` 定点尺度。
- 向量坐标另以 `1e8` 缩放后累加定点点积。
- 每个候选输出原始域误差界和归一化传播误差界；每个融合命中输出总误差上界、有效十进制位数和推导字符串。
- 融合参考使用 `big.Float` 256-bit 精度累加；默认在第 `1,2,3,5` 位（rank positions）抽样，并比较 6 位小数字面量。
- 相邻融合分差不超过两侧误差界与 `similarity_tolerance` 之和时，上位命中标记为 `unstable=true`，同时给出 `crossing_magnitude`。
- 最近样本的最大融合偏差为 `2.2025043566742397e-08`（`Q007`，来源标记 `fusion`），36/36 个固定抽样逐位一致，45 个 top-k 命中中有 3 个不稳定相邻位次。

### 预算一致性

预算先计算千分比后的每查询统一深度，再按配置中的确定性顺序（默认 `vector -> category -> lexical`）响应全局评估次数和估算时间预算。缩减原因和被淘汰路写入 `budget.reductions`。

`TestBudgetReductionMatchesReducedConfig` 将全局预算压到 40，得到 `lexical=4,category=0,vector=0`，再用固化后的缩减深度、1000‰ 配额和宽松全局预算从零运行；通过 `recall.CanonicalResultBytes` 比较，召回候选、融合分、稳定性和审计字节一致。配置回显和预算裁决轨迹属于配置事实，不纳入“召回结果投影”。

### 并行确定性

报告只使用结构体字段和稳定排序，不把 map、时间戳、goroutine 调度信息写入结果。所有查询完成后按 `query_id` 排序，再计算 `result_digest`。轮次 deadline 只阻止尚未越过查询边界的任务开始；已经开始的查询及其检查点事务会执行完成，不会声称杀死了工作线程。

### SQLite 恢复

`checkpoint` 表包含全局单调 `seq INTEGER PRIMARY KEY AUTOINCREMENT`、`stage`、`query_id`、`input_fingerprint`、`config_version`、`output_digest`、`output_json`、`status` 和时间戳。部分唯一索引保证每个 `(query_id, stage)` 至多一条 `status='valid'` 记录。

提交时在同一事务内把旧有效记录置为 `invalid`，再插入新记录。恢复时重新计算链式输入指纹并校验摘要；匹配则复用已提交的阶段 JSON。默认样本完整检查点为 65 条有效记录：9 个有效查询各 7 个阶段，空查询只有 `budget` 和 `normalize`。

### 配置版本

- 片段的 `base_version` 不等于当前版本：`E_STALE_BASE`（HTTP 409）。
- 更低版本或同版本异摘要的整体替换：`E_STALE_VERSION`（HTTP 409）。
- 候选深度、千分比或预算变化最早失效 `budget` 及其下游。
- 权重、融合方法或截断规则变化最早失效 `fuse` 及其下游。
- `TestConfigFragmentInvalidatesAndRebuildsCheckpoints` 验证权重片段使 18 条 `fuse/audit` 有效记录失效（65→47），再次批量运行后重建为 65 条且全部位于配置 v2。

## 实际验收摘要

```text
Go: go1.27.4 linux/amd64 (go.mod: go 1.22)
SQLite CLI: 3.46.1
gcc: 15.2.0
go test -count=1 ./...: ok multirecall/tests
sequential/parallel bytes: 231609
result_digest: a56094ef439b3d41aa1ee8d48700b934a7580af7cdaa7e951d71586faa9309e5
BYTE_MATCH_PARALLEL=yes
KILL_RC=137
BYTE_MATCH_RECOVERY=yes
SQLite valid_records=65
ALL_VERIFICATION_CHECKS_PASSED
```

上述 digest 对应当前固定样本和默认配置；任何算法或配置变更都会按设计改变摘要。
