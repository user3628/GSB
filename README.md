# 确定性流式 TTS 分帧检查点组件

这是一个可在 Windows 空白工作区直接运行、可嵌入 Go 服务的流式 TTS 领域组件与完整演示。它不加载真实声学模型，也不依赖 GPU、容器或外部服务；文本、YAML 配置、声学帧、16 位 PCM 和全部检查点都由本地固定种子与内容摘要确定性生成。

核心验收点是中断安全：每个不可拆分的发音单元完成后，四个阶段都在同一个帧段上留下可查询的 SQLite 事实。优雅停止、进程被强制终止、配置变化或文本变化之后，恢复过程只复用仍然匹配当前阶段指纹的前缀帧段，最终声学字节、PCM 字节、逐帧统计和固定摘要与从零全量运行完全一致。

## 快速开始（Windows PowerShell）

前置条件：Go 1.22 或更高版本；工作目录与 Go 模块缓存可写。SQLite 为纯 Go 驱动 `modernc.org/sqlite`，无需安装 CGO SQLite。

```powershell
go version
go mod download
go test ./...

go run ./cmd/ttsctl init --dir .ttsflow
go run ./cmd/ttsctl run --dir .ttsflow
go run ./cmd/ttsctl status --dir .ttsflow --utt utt-001 --segment 0
go run ./cmd/ttsctl versions --dir .ttsflow
go run ./cmd/ttsctl fingerprint --dir .ttsflow --utt utt-001 --segment 0
go run ./cmd/ttsctl report --dir .ttsflow --out artifacts/diagnostic.json
```

如果本机访问默认 Go 模块代理较慢，可临时指定可用代理，例如：

```powershell
$env:GOPROXY = "https://goproxy.cn,direct"
go mod tidy
```

也可以先构建本地可执行文件：

```powershell
go build -o ttsctl.exe ./cmd/ttsctl
.\ttsctl.exe init --dir .ttsflow
.\ttsctl.exe run --dir .ttsflow
```

## 本地演示

以下命令会执行三类运行并自动比对：

1. 从零全量运行。
2. 启动子进程，在其提交至少一个完整帧段后由父进程真实 `Kill()`，再恢复并比对。
3. 已完整运行后修改声学权重种子，执行增量重建，再与同配置的全新全量运行比对。

```powershell
go run ./cmd/ttsctl demo --dir artifacts/demo
```

结果写入 `artifacts/demo/demo-report.json`。演示目录是生成的运行数据，可安全删除后重跑。

## 固定样本与配置

`init` 首次运行会生成：

- `synthesis.yaml`：声学层维度链、`embedding_dim`、`frame_shift_samples`、`bands`、权重种子、切分参数、音素映射、采样率、位深、声道和检查点间隔。
- `texts.jsonl`：4 条固定种子语句，覆盖中英混排、标点和较长上下文。每行含 `utt_id`、`text`、`language`、`speaker_id`、`prosody` 与 `source_rank`。
- `state.db`：首次合成为 SQLite WAL 检查点库；另有 `state.db.publish.lock` 真实文件锁。

固定样本：

| utt_id | 语言 | 内容特征 |
| --- | --- | --- |
| `utt-001` | zh | 中文、全角标点、强制切分 |
| `utt-002` | en | 英文、拉丁字母串、句末标点 |
| `utt-003` | zh | 中英混排、`Go 1.22`、数字与句点 |
| `utt-004` | zh | 多标点长句，用于增量定位 |

切分与归一化规则：

- Unicode NFKC，并显式执行全角/半角折叠；连续空白折叠为单个 ASCII 空格。
- 中文按单字、拉丁文本按连续字母/数字串形成 token，标点与符号单独形成 token。
- 首个块至少达到 `first_min_chars`；后续块达到 `following_chars`；`force_at_punctuation` 开启时在标点后立即封块。
- 块携带归一化文本中的 rune/byte 偏移，块序列无损覆盖整段归一化文本。
- 每个字形经 `frontend.graphemes` 映射为音素，再经 `frontend.phoneme_units` 映射为不可拆发音单元。中文优先 `zh`，拉丁优先 `en`，标点优先 `common`。

## 四阶段与确定性

每个发音单元对应一个递增的 `segment_seq`，四个阶段依次发布：

| 阶段 | 产物 | 检查点内容 |
| --- | --- | --- |
| `frontend` | 块、偏移、字形、音素、发音单元 | 前端指纹、单元摘要、前端产物摘要 |
| `acoustic` | 每帧频带能量与 F0 | 声学指纹、帧号范围、声学字节摘要 |
| `wave` | 16 位 PCM、采样范围、钳制次数 | 波形指纹、PCM 摘要、起止采样点 |
| `consistency` | 帧/采样统计、峰值、RMS、钳制数 | 一致性指纹与统计摘要 |

确定性实现要点：

- 固定哈希嵌入只依赖发音单元、说话人与韵律控制。
- 逐层线性变换的权重由 YAML 中 `weight_seed` 和层形状确定性生成；改变权重种子会改变全部声学帧。
- 帧值不包含全局帧号或块序号，因此任意合法切分参数下，按块拼接的声学字节与整体预测逐字节一致。
- 声学帧以显式 little-endian float64 编码；PCM 以显式 little-endian int16 编码。
- 波形使用固定相位与帧内包络；全局采样相位由绝对采样位置决定，避免块边界重置。
- 非有限频带能量或 F0 立即返回 `E_NUMERIC`，并附 `utt_id` 与绝对 `frame_no`。
- PCM 样本数强制等于“帧数 × 帧移”，不符返回 `E_LENGTH_MISMATCH` 并附声明值与实际值。

## 检查点模型

SQLite 主表 `checkpoints` 对每个 `(utt_id, segment_seq)` 只保留一条有效记录，由部分唯一索引强制。`stage_mask` 的四位依次表示 frontend/acoustic/wave/consistency；只有一致性阶段完成后，帧段才算完整提交。

每个阶段都会向 append-only 的 `checkpoint_events` 写入一条带自增单调序号的记录，包含：

- `utt_id`、`segment_seq`、阶段、阶段后的位掩码；
- 文本与配置内容摘要；
- 阶段指纹、发音单元摘要/摘要值；
- 产物摘要与写入后的活动快照摘要。

发布路径同时使用两层互斥：

1. 进程内互斥，防止同一 `Store` 的 goroutine 重入同一 `flock` 文件描述符。
2. `state.db.publish.lock` 上的真实 OS 文件锁，`modernc.org/sqlite` 本身不提供跨进程业务互斥。

进入 SQLite 事务后，还会验证整份活动检查点快照的摘要，并执行带旧序号/旧位掩码条件的更新。因此落后、重复、跳号或跨 `utt_id` 错位发布会返回 `E_STALE_CHECKPOINT`，错误中带 `current_seq`。

快照检查区分：

- `normal`：活动快照与流头一致，可正常推进下一阶段/帧段。
- `replacement`：同序号或更高活动快照被替换，序号未回退但指纹/摘要变化。
- `rollback`：已完成帧段或活动记录回退，当前流头指向的记录已不存在。

只有正常推进会被接受。

## 停止与恢复

- 库内停止：`Engine.RequestLocalStop()`；跨进程停止：`Store.RequestStop(ctx, uttID)` 或 CLI `stop`。
- 停止检查发生在完整一致性检查点提交之后，因此不会留下半个发音单元。
- 恢复：`Engine.Run(ctx, RunOptions{ClearStops:true})` 或 CLI `recover`；它清除上一次运行的停止请求，从已提交事实继续。
- 强制终止不保证退出报告。已提交事务保留；未提交事务由 SQLite 回滚。重启时从持久化 `stage_mask`、指纹和产物摘要继续。

## 增量指纹与版本规则

每个帧段保存四级指纹链：

- `frontend`：块文本/偏移与发音单元，不包含整段文本的全局摘要，因此末尾编辑不会误伤前缀。
- `acoustic`：前端指纹、声学配置摘要、说话人与韵律控制。
- `wave`：声学指纹、采样率/位深/声道、全局帧起止范围。
- `consistency`：波形指纹与检查点间隔。

引擎逐帧段比较持久化指纹与当前计划的指纹，找到最早的 `(segment_seq, stage)`，只裁剪该点及之后的活动状态；若失效从 acoustic 开始，同一帧段的 frontend 位仍可复用。

| 变化 | 最早失效阶段 |
| --- | --- |
| 某条文本变化 | 实际改变的首个 `frontend` 帧段 |
| 切分参数变化 | 首个块元数据变化的 `frontend` 帧段 |
| 音素映射变化 | 首个受影响单元的 `frontend` 帧段 |
| 声学权重/韵律/说话人变化 | `segment 0` 的 `acoustic` |
| 采样率/位深/声道变化 | `segment 0` 的 `wave` |

版本表 `dataset_versions` 记录文本集和配置的内容摘要、递增版本与状态：

- 文本正常追加：`append`。
- 同 ID 内容编辑：`update`，由引擎做局部失效。
- 显式整体替换：`replace`，立即停用全部活动帧段并删除流头，首次恢复不会复用旧状态。
- 配置正常修订：`update`，由阶段指纹决定局部失效。
- 配置显式替换或修订号跳跃：`replace`，全量失效。
- 新摘要命中历史旧版本，或配置变更未提高 `revision`：拒绝并返回 `E_ROLLBACK`。

修改配置时应同时增加顶层 `revision`。例如把 `acoustic.weight_seed` 从 `20261125` 改为 `20261126` 时，将 `revision: 1` 改为 `revision: 2`。整体替换使用：

```powershell
go run ./cmd/ttsctl replace-config --dir .ttsflow --file new-synthesis.yaml
```

`ttsflow.CompareSynthesisResults(actual, full)` 会逐发音单元比较实际声学字节与 PCM 字节，再比较逐帧统计、聚合摘要和最终 manifest；任何差异返回 `E_INCREMENTAL_MISMATCH`，并填 `first_diff_segment`。

## 库接口示例

```go
paths, cfg, raw, samples, store, err := ttsflow.OpenWorkspace(".ttsflow")
if err != nil { /* handle */ }
defer store.Close()

engine, err := ttsflow.NewEngine(cfg, raw, store, samples)
if err != nil { /* handle */ }

// 正常流式运行；也可用 MaxSegments 做确定性的优雅停止。
result, err := engine.Run(ctx, ttsflow.RunOptions{})
if err != nil && !errors.Is(err, ttsflow.ErrGracefulStop) { /* handle */ }

status, found, err := engine.Status(ctx, "utt-001", 0)
fingerprint, err := engine.VersionFingerprint(ctx, "utt-001", 0)
report, err := engine.DiagnosticReport(ctx, nil)

// 从已提交事实恢复。
recovered, err := engine.Run(ctx, ttsflow.RunOptions{ClearStops: true})
_ = ttsflow.CompareSynthesisResults(recovered, result)
_ = paths
```

主要导出入口：

- `ttsflow.OpenWorkspace`、`ttsflow.EnsureWorkspace`、`ttsflow.OpenStore`
- `ttsflow.NewEngine`、`(*Engine).Run`、`(*Engine).Status`、`(*Engine).VersionFingerprint`
- `(*Engine).DiagnosticReport`、`(*Engine).ExportReport`
- `(*Store).RequestStop`、`(*Store).ObserveTextSet`、`(*Store).ObserveConfig`
- `ttsflow.CompareSynthesisResults`

## CLI 命令

| 命令 | 作用 |
| --- | --- |
| `init` | 用固定种子生成 YAML 与 JSONL |
| `run` | 四阶段流式合成；支持 `--stop-after`、`--delay-ms`、`--force-rebuild` |
| `recover` | 清理旧停止请求并从持久化事实恢复 |
| `stop` | 写入跨进程优雅停止请求 |
| `status` | 按 `utt_id + segment` 查询阶段位、产物摘要和版本指纹 |
| `fingerprint` | 计算当前输入下指定帧段的四阶段指纹 |
| `versions` | 查看文本/配置当前版本与内容摘要 |
| `report` | 导出不含大体积 PCM 的诊断 JSON |
| `import-texts` | 以 `append`、`update`、`replace` 模式导入 JSONL |
| `replace-config` | 整体替换配置并使全部派生状态失效 |
| `demo` | 全量、真实强杀恢复、增量重建三条路径自动比对 |

## 业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_EMPTY_TEXT` | 归一化后为空文本 |
| `E_PHONEME_MAPPING` | 字形或音素缺少映射 |
| `E_SPLIT` | 文本块无法形成或没有发音单元 |
| `E_NUMERIC` | 出现 NaN/Inf，附 `utt_id` 与 `frame_no` |
| `E_LENGTH_MISMATCH` | PCM 样本数与帧数/帧移不符，附声明值与实际值 |
| `E_STALE_CHECKPOINT` | 发布落后/重复/跳号/快照替换或回退，附 `current_seq` |
| `E_INCREMENTAL_MISMATCH` | 增量与全量产物不一致，附首个差异帧段 |
| `E_ROLLBACK` | 文本或配置回退到历史摘要 |
| `E_CONFIG_INVALID` / `E_DATASET_INVALID` | 配置/输入结构非法 |
| `E_LOCK` | 发布文件锁不可获得 |

## 本次实际验证

验证日期：2026-11-25。开发容器为 Linux，默认工具链 Go 1.27.4，并额外用 Go 1.22.0 完整跑过测试；模块指令固定为 `go 1.22.0`，并通过 `GOOS=windows GOARCH=amd64` 交叉编译。应用不依赖 CGO、GPU 或网络服务。

执行的验证：

```text
go test ./... -count=1                         PASS
GOTOOLCHAIN=go1.22.0 go test ./... -count=1    PASS
go test -race ./ttsflow -count=1               PASS
go vet ./...                                   PASS
GOOS=windows GOARCH=amd64 go build ./...       PASS
GOOS=windows GOARCH=amd64 go test -c ./ttsflow PASS
go run ./cmd/ttsctl demo                       PASS
```

最终演示结果：

```text
full_manifest              = db9896ddd04940604e4d9d19be86e7e528c7b11179531e64f6ce65645bc62769
recovered_manifest         = db9896ddd04940604e4d9d19be86e7e528c7b11179531e64f6ce65645bc62769
incremental_manifest       = ce7684c14a9068ffef48aab4951846767580b0808e3deda3f75fabbd53d1d639
incremental_full_manifest  = ce7684c14a9068ffef48aab4951846767580b0808e3deda3f75fabbd53d1d639
hard_kill_observed         = true
recovery_equal             = true
incremental_equal          = true
incremental_first_stage    = acoustic
incremental_first_segment  = 0
```

固定全量摘要由 `TestGoldenManifestDigest` 锁定。演示总计 97 个不可拆发音单元帧段；每个完整帧段产生 4 个阶段事件，共 388 个阶段检查点记录。恢复测试在子进程至少提交一个完整帧段后真实强杀，恢复摘要与全量摘要一致。

机器可读结果见 `artifacts/validation-report.json`。

## 说明与边界

- 声学网络是确定性合成替身，用于验证分帧、持久化、失效与恢复语义，不代表真实语音质量。
- `checkpoint_interval_frames` 是 YAML 声明的诊断间隔，并参与一致性指纹；由于“不可拆分发音单元”约束优先，每个单元仍会落库，不会为了间隔而截断单元。
- SQLite 采用 WAL、10 秒 busy timeout、部分唯一索引和条件更新；发布业务互斥另由真实文件锁保证。
- 强制终止后如果没有完整帧段提交，恢复会从最后一个完整或部分阶段事实安全重算；不承诺生成退出报告。
