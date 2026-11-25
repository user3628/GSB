# 实际验证记录

验证时间：2026-11-26（Asia/Shanghai）。以下结果来自当前工作区代码；完整原始输出保存在
`artifacts/verification/`。

## 环境

```text
$ go version
go version go1.27.4 linux/amd64

$ sqlite3 --version
3.46.1 2024-08-13 09:16:08 c9c2ab54ba1f16c63f69aalt1

$ pkg-config --modversion sqlite3
3.46.1
```

`go.mod` 声明 `go 1.22.0`。本次实际工具链是 Go 1.27.4；未另装 Go 1.22 二进制做交叉执行。

已设置：

```bash
go env -w GOFLAGS=-mod=mod
```

离线缓存构建通过：

```text
$ GOPROXY=off GOFLAGS=-mod=mod go build ./...
OFFLINE_CACHE_BUILD_OK
```

`go vet ./...` 无输出并以 0 退出。

## 单元/集成测试

命令：

```bash
GOPROXY=off GOFLAGS=-mod=mod go test -count=1 ./...
```

结果：

```text
ok  imretrace/internal/features
ok  imretrace/internal/pipeline
ok  imretrace/internal/quant
ok  imretrace/internal/store
```

覆盖点：

- NFKC/全半角/空白折叠、空 caption、空白图像、grid=0、特征字节确定性。
- 8/4-bit 标定、重复量化字节一致、零缩放、维度不符、全零量化向量、误差界不被实际误差突破。
- Go 测试中 16 个 goroutine 并发提交同一 `(shard,seq=1)`：仅 1 个成功，15 个返回
  `E_STALE_CHECKPOINT/current_seq=1`；活动阶段记录和序号记录均只有 1 条。
- 验收脚本另启动 16 个真实 OS 进程执行 `publish-test --seq 1`：
  `race winners=1 stale=15 active_seq_rows=1`。
- 序号 1→4 连续推进、重复序号拒绝、跨分片拒绝。
- “提交 stage 2 后模拟强杀 → 重开 Service 恢复”与连续执行的状态哈希、检索 JSON 完全一致。
- 整体替换到 v2/grid=4 后产生 36 条活动检查点；回退 v1 返回
  `E_STALE_VERSION current=2 min=2`。
- 空查询、`k=99`、零融合权重均返回 `E_QUERY_INVALID` 且保留 partial candidates/components。
- 严格容差下返回 `E_NUMERIC_MISMATCH` 并带最大绝对/相对偏差。

原始日志：`artifacts/verification/go-test-count1.log`。

## 端到端验收脚本

命令：

```bash
./scripts/acceptance.sh
```

最终结果：

```text
ACCEPT_RC=0
ALL_ACCEPTANCE_CHECKS_PASSED
```

完整日志：`artifacts/verification/acceptance-run.log`。

脚本使用独立临时工作根，验证以下事实：

- 固定种子生成 9 个图文分片，caption 覆盖中英混排、几何色块、渐变和明暗视角。
- v1 默认 8-bit、grid=3，四阶段各 9 条，共 36 条活动检查点。
- 文本查询 `red circle 红色圆形 blue gradient` 的 top1 为
  `item-shard-0001`。
- 图像查询 `item-shard-0001` 的首选为自身图文对，`fused_score=1`。
- 增量 `{"base_version":1,"quant_bits":4}` 后，`green triangle 绿色三角 yellow`
  top1 为 `item-shard-0003`。
- 零权重查询返回 `E_QUERY_INVALID`，partial 中保留 9 个已计算候选和 cosine/term 分量。
- `k=99` 返回 `E_QUERY_INVALID`，partial 中保留全部 9 个候选。
- `policy replace --next-demo` 执行整体替换（grid=4、权重重置、版本+1），旧 36 条变为
  `superseded`，新策略下重新产生 36 条活动检查点。
- 用旧 `policy.v1.json` 回退返回退出码 4 和 `E_STALE_VERSION`。
- `PRAGMA integrity_check` 返回 `ok`；不存在同分片同阶段的多条活动记录。

## 误差诊断样例

v1/8-bit 查询 top1 的实测摘要：

```text
item_id                         item-shard-0001
cosine_ref                      0.6770685899
cosine_quant                    0.6751370603
final theoretical score bound   0.0295166593
query text L2 bound             0.0118666088
```

4-bit 查询 top1：

```text
item_id                         item-shard-0003
cosine_ref                      0.9793625922
cosine_quant                    0.9647139523
actual final abs error          0.0105470207
theoretical score bound         1.1033944829
reference-verified bound        0.0105470207 (+1e-12 guard)
verified effective digits       1 decimal digit
verified effective bits         6.468 bit
```

理论界使用显式 `step/2 + calibration_outside_distance` 与保守余弦传播，因此在 4-bit
最坏情况下较松；`reference_verified_bound` 是与 Kahan float64 参考实现逐项比对后的紧后验界。
每条 HTTP/CLI 召回结果仍保留完整逐维 `scale/step/zero_point/outside_range/actual_abs`
推导数组。

## 强制终止与恢复

脚本对真实二进制执行：

```bash
imretrace --root "$KILL_ROOT" build --crash-after-stage 2
```

进程在第 1 个分片的 stage 2 事务提交后直接发送 `SIGKILL`，shell 记录：

```text
kill_rc=137
active checkpoints after kill=10   # 9 条 feature_extract + 1 条 project_quantize
```

重新执行 `build` 后恢复完成。连续执行与强杀恢复的规范状态哈希一致：

```text
f8e88d149670769d3f085ceed8a44e8ce06a7a953276e64bc53a281f53be1f8c
```

同一文本查询的格式化 JSON SHA-256 也一致：

```text
3b4be83dbca1bed32dbbef705594d47216666b005d5406d7c0c093fef1e326f8  18-cont-search.json
3b4be83dbca1bed32dbbef705594d47216666b005d5406d7c0c093fef1e326f8  19-rec-search.json
```

这证明恢复后的量化参数、索引摘要和召回输出与一次连续执行逐字节一致（至少对 canonical
状态和该确定性 JSON 响应成立）。

## HTTP 工作台

验收脚本向系统申请空闲端口并启动服务（避免固定端口被环境占用）：

- `GET /api/status` 成功。
- `POST /api/search/text` 成功，产物 `artifacts/verification/23-http-search.json`。
- `POST /api/stop` 成功，产物 `artifacts/verification/24-http-stop.json`。
- 服务进程在超时等待内退出，验证优雅停止。

## 工作区持久演示状态

默认命令在仓库 `data/` 下另建了一份可交互演示。当前 SQLite 中可查询到：

```text
items=9
active_checkpoints=36
policies: 1 full, 2 incremental, 3 full
```

工作区诊断报告：

```text
artifacts/verification/workspace-diagnostics-report.json
```

可用以下命令继续操作：

```bash
./bin/imretrace status
./bin/imretrace serve --addr 127.0.0.1:8765
sqlite3 data/imretrace.sqlite \
  "select shard_id,seq,stage,policy_version from checkpoints where status='active' order by shard_id,seq;"
```
