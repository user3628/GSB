# SampleEngine：训练推理一体化的样本回填引擎

纯 Go、无 CGO、无 Docker、无 GPU、无外部服务。样本和事件由固定种子生成，SQLite 使用
`modernc.org/sqlite` 纯 Go 驱动；Windows/amd64 已用
`CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build` 交叉验证。

## 一键验收

要求 Go 1.22+。若首次拉模块遇到默认代理超时，可执行：

```powershell
go env -w GOPROXY=https://goproxy.cn,direct
```

在仓库根目录运行：

```powershell
go mod download
go test ./... -count=1
go run . demo --dir .demo --summary artifacts/demo-summary.json --report artifacts/rebuild-report.json
```

演示会：

- 用固定种子生成 4 个批次、共 24 条样本；
- 启动真实子进程，提交前两个批次后调用进程强杀（Windows 下为 `TerminateProcess`）；
- 重新打开 SQLite，仅凭已提交事务恢复并续导第 3、4 批；
- 对跨批次样本 `s-0002` 做标签 `1 -> 0` 更正并补充 `source=cross-batch-backfill`；
- 定位特化单元 `[0 1 3 4 5 6 7]`，复用单元 `2`；
- 重建受影响单元、全局模型版本和推理缓存；
- 比较局部重建与从零全量训练的规范化检查点字节；
- 输出定点分数、高精度参考值、逐维误差界和固定摘要。

期望结果中的关键字段：

```json
{
  "ok": true,
  "batch_count": 4,
  "sample_count": 24,
  "recovered_batches": ["batch-01", "batch-02"],
  "byte_equal": true,
  "first_diff_byte": -1,
  "old_model_version": 24,
  "new_model_version": 25,
  "after_within_bound": true,
  "config_digest": "8c36b7970e1bbcb16cb14601a08927faa7b7fc145b29bd46c110b479da7600d6",
  "fingerprint": "f26e8e829819ef726f2662a51fe62e74e472dd219e54226ace09329b02787394",
  "summary_digest": "f1580c2da67d48acaf675c6b444c315d394f80b1579a35bc7e19ee05c69ce9d7"
}
```

`summary_digest` 对不含本机临时路径的确定性字段计算，因此在不同临时目录重复运行仍应得到
`f1580c2da67d48acaf675c6b444c315d394f80b1579a35bc7e19ee05c69ce9d7`。

## Windows 构建与 CLI

```powershell
$env:CGO_ENABLED=0
go build -o sampleengine.exe .
.\sampleengine.exe demo --dir .demo
.\sampleengine.exe status --db .demo\sampleengine.db
```

常用命令：

```powershell
# 导入内置固定种子批次
.\sampleengine.exe import --db .demo\se.db --demo

# 推理打分，0 表示当前有效模型版本
.\sampleengine.exe score --db .demo\se.db `
  --request-id req-0001 --model-version 0 `
  --text "退款 invoice 延迟 vip" --field source=online-query

# 只查看固定哈希向量、缺失桶和冲突计数
.\sampleengine.exe featurize --id sample-1 --text "退款 invoice vip"

# 查看某单元最新参数状态及该版本的缩放因子/增量记录
.\sampleengine.exe unit-version --db .demo\se.db --unit 0

# 跨批次回填：s-0002 原始固定标签为 1，更正为 0
.\sampleengine.exe backfill --db .demo\se.db `
  --sample s-0002 --label 0 --field source=cross-batch-backfill

# 版本状态和报告导出
.\sampleengine.exe status --db .demo\se.db
.\sampleengine.exe report --db .demo\se.db --id latest --out rebuild-report.json

# 异常注入
.\sampleengine.exe inject --case hash_dim_zero
.\sampleengine.exe inject --case illegal_feature_dim --dim 999999
.\sampleengine.exe inject --case label_non_binary
.\sampleengine.exe inject --case learning_rate_non_positive
.\sampleengine.exe inject --case numeric_nan --request-id rq-nan --dim 7

# 多进程连续发布；可同时启动多个相同命令
.\sampleengine.exe publish-loop --db .demo\race.db --unit 5 --iterations 20 --worker-id p1
```

也可以从 JSON 导入：文件格式为 `{"batches":[{"index":1,"id":"batch-01","samples":[...]}]}`，
或直接传批次数组。

## 库接口

核心包为 `sampleengine/engine`：

- `engine.DefaultConfig`：128 个哈希维、8 个特化单元、3 轮、学习率 `1/20`。
- `engine.GenerateDemoData(seed)`：固定种子样本和跨批次更正。
- `engine.Featurize` / `engine.NormalizeText` / `engine.Tokenize`：固定哈希特征化。
- `engine.OpenDB` / `Engine.ImportBatch`：SQLite 事务化批次和逐样本参数版本。
- `Engine.Backfill`：摘要定位、依赖单元重放、全局版本/缓存失效、全量字节比对和报告。
- `Engine.Score` / `engine.ScoreCheckpoint`：定点打分、高精度 `big.Rat` 参考和逐维误差界。
- `Engine.PublishUnit`：文件锁 + SQLite CAS，推进单元版本。
- `Engine.RegisterVersion` / `Engine.FullRebuildAndPublish` / `Engine.ReplaceConfig`：
  正常推进、整体替换、回退拒绝和替换后重建。
- `engine.InjectParam`：稳定业务异常注入入口。
- `engine.RunDemo`：确定性端到端演示，CLI 只是薄封装。

最小调用示例：

```go
package main

import (
    "fmt"
    "sampleengine/engine"
)

func main() {
    e, _ := engine.OpenDB("se.db", engine.DefaultConfig())
    defer e.Close()

    data := engine.GenerateDemoData(engine.DefaultConfig().Seed)
    for _, batch := range data.Batches {
        if _, err := e.ImportBatch(batch); err != nil { panic(err) }
    }

    report, err := e.Backfill(engine.BackfillRequest{
        SampleID: data.Correction.SampleID,
        Label:    data.Correction.Label,
        Fields:   data.Correction.Fields,
    })
    if err != nil { panic(err) }
    fmt.Println(report.ByteEqual, report.IncrementalFingerprint)

    score, err := e.Score(engine.ScoreRequest{
        RequestID: "req-1",
        Text:      data.QueryText,
        Fields:    data.QueryFields,
    })
    if err != nil { panic(err) }
    fmt.Println(score.Score, score.ObservedError <= score.TotalBound)
}
```

## 实现要点

**固定特征空间**

- 规范化顺序为 NFKC、空白折叠、小写化。
- 输出 `h:<中文单字>`、`b:<相邻中文二字>`、`l:<拉丁连续串>`、`f:<补充字段>` 和
  `m:<缺失字段>`。
- 普通特征用 FNV-1a 32 位映射到维度 `1..Dim-1`；维度 `0` 是显式缺失桶。
- 输出稀疏向量、缺失计数和不同 token 落入同维的冲突计数；结果带摘要。

**定点训练**

- 标签极性为 `2*label-1`，每轮把特征值累加到整数梯度累加器。
- 更新量为
  `deltaW = trunc(Scale[d] * LRNum * Gradient / LRDen)`，权重和每维缩放因子落库。
- 高精度参考权重用 `math/big.Rat` 同步执行未量化更新。
- 每个样本写 `model_versions`，每个受影响单元写 `unit_versions`，并绑定批次和样本序号。

**特化单元与回填等价性**

- `unit = dim % Units`。坐标更新互不耦合，因此单元可独立按原始样本顺序重放。
- 更正标签或补充特征后，合并旧/新稀疏向量得到受影响单元集合。
- 只查询 `sample_units` 中依赖这些单元的样本并重放；未受影响单元直接复用检查点字节。
- 发布前用 `RebuildAll` 从全部当前样本从零训练，调用 `EnsureCheckpointsEqual`
  逐字节比较规范化检查点。不一致返回 `E_INCREMENTAL_MISMATCH` 和 `first_diff_byte`。

**误差界**

- 每次定点更新的量化误差小于 `1/Scale[d]`，`N` 次更新的参数误差界为 `N/Scale[d]`。
- 权重转换到公共 `ScoreScale` 时每维另有小于 `|feature|/ScoreScale` 的舍入传播。
- `DimBound` 同时给出缩放因子、特化路径放大系数、更新次数、定点贡献、`big.Rat`
  参考贡献、实际误差和保守边界。

**SQLite、恢复和并发**

- `batches`、`samples`、`sample_revisions`、`sample_units`、`units`、`unit_versions`、
  `model_versions`、`inference_cache`、`inference_dependencies` 和 `rebuild_reports`
  均在 schema 中事务化维护。
- `PRAGMA synchronous=FULL` 与 rollback journal 保证已提交事实可恢复；未提交批次在强杀后回滚。
- `unit_versions(unit_id, version)` 主键、`(unit_id, version) WHERE valid=1`
  唯一索引和 `unit_id WHERE is_current=1` 唯一索引防止重复有效参数和双当前版本。
- 发布先获取跨平台 `flock` 文件锁，再用
  `UPDATE ... WHERE current_version=?` 条件更新；版本必须恰好 `+1`。

**版本识别**

- `ClassifyVersion` 用版本序号、代际和内容摘要区分：
  - `normal_advance`：同代际、序号连续；
  - `full_replacement`：显式替换、代际 `+1` 或配置摘要变化；
  - 回退、重复和版本空洞：`E_STALE_CHECKPOINT`。
- 替换会把全部推理缓存置为无效并清空当前单元指针，随后可调用
  `FullRebuildAndPublish` 或 CLI `rebuild` 重建。

## 稳定业务码

| 场景 | 代码 |
| --- | --- |
| NaN/Inf 或定点溢出/误差界违背 | `E_NUMERIC` |
| 局部重建与全量不一致 | `E_INCREMENTAL_MISMATCH` |
| 落后、重复、版本空洞、跨单元错位、回退 | `E_STALE_CHECKPOINT` |
| 特征维度越界 | `E_FEATURE_DIM_INVALID` |
| 标签不是 0/1 | `E_LABEL_INVALID` |
| 学习率非正 | `E_LEARNING_RATE_INVALID` |
| 哈希维度为 0 | `E_HASH_DIM_ZERO` |
| 其他非法哈希维度 | `E_HASH_DIM_INVALID` |

## 测试与实际结果

`engine/engine_test.go` 覆盖特征确定性、续跑字节一致、回填/全量一致、误差界、
错误码、版本分类、替换失效重建、8×5 goroutine 并发发布和真实子进程强杀。
`cli_test.go` 会实际 `go build` CLI，并启动 4 个操作系统进程并发执行
`publish-loop`，验证跨进程文件锁和数据库唯一当前版本。

最终一次完整验证的命令和结果记录在 `artifacts/verification.txt`。
