# layoutread — 文档版面阅读顺序分析

`layoutread` 是一个可直接运行的 Go 1.22+ 文档版面阅读顺序引擎与本地演示应用。它使用纯 Go SQLite 驱动（`modernc.org/sqlite`，不启用 CGO），包含确定性样本生成器、四阶段分析流水线、增量失效传播、事务化检查点、并发发布保护、延迟预算查询、HTTP 控制台和 CLI。

## 快速开始

```bash
# 构建与测试
CGO_ENABLED=0 go build -o layoutread ./cmd/layoutread
CGO_ENABLED=0 go test ./...

# 一次完整固定演示（临时数据库，不污染工作区）
./layoutread demo

# 生成固定种子样本/策略快照
./layoutread generate -out bundle.json

# 对持久化数据库执行全量或增量分析
./layoutread analyze -db demo.sqlite -demo
./layoutread status  -db demo.sqlite
./layoutread query   -db demo.sqlite -doc doc-double-en -scope blocks -budget-ms 50

# 启动本地 HTTP 控制台（浏览器打开 http://127.0.0.1:8080）
./layoutread serve -db demo.sqlite -demo
```

依赖已放在 `vendor/`，因此上述构建不需要网络。`vendor/` 中只为任务目标 `linux/amd64` 保留纯 Go SQLite/libc 生成文件；SQLite 命令行工具只用于查看事实，Go 侧不依赖它。

## 固定演示数据

内置生成器使用固定种子 `20261126`，立即产出 4 份文档和版本为 `3` 的策略快照：

| doc_id | 覆盖场景 |
|---|---|
| `doc-single-zh` | 单栏、中文正文、中英混排、页眉页脚 |
| `doc-double-en` | 双栏、英文/中文混排、页眉页脚 |
| `doc-span-title-zh-en` | 双栏正文 + 顶部通栏标题、页眉页脚 |
| `doc-dual-band-mixed` | 上下两个栏带 + 中部通栏小标题、相邻片段合并、页眉页脚 |

同一构建与配置下，`./layoutread demo` 两次输出逐字节一致。当前固定结果摘要：

```text
demo output SHA-256:    44e39848dd61ba4db43b6a37ede788b3d2dc1fadc26d8ee4089e77bed7161c45
document set digest:    9341a91ea9d44eefa46f6ccc4864e7f64c4a9e0bd07b0a8b215b8eda4b3279f3
```

`document set digest` 是导出结果中所有文档结果按 `doc_id` 排序后，与策略指纹一起计算的 SHA-256。

## 输入格式

`bundle.json` 的顶层结构：

```json
{
  "documents": [
    {
      "doc_id": "doc-single-zh",
      "page": {"width": 1200, "height": 1600},
      "boxes": [
        {
          "index": 0,
          "x": 0.06,
          "y": 0.015,
          "w": 0.88,
          "h": 0.025,
          "font_tier": 1,
          "text": "页眉文本",
          "lang": "zh",
          "column_hint": 0
        }
      ]
    }
  ],
  "strategy": {
    "version": 3,
    "merge_gap_px": 6,
    "margin_x_px": 60,
    "margin_y_px": 96,
    "header_footer_max_font_tier": 2,
    "column_histogram_bin_px": 16,
    "column_gap_px": 40,
    "column_min_support": 1,
    "span_width_ratio": 0.92,
    "cross_column_priority": "span_first",
    "confidence_vertical_weight": 0.6,
    "confidence_column_weight": 0.4,
    "latency_budget_ms": 25
  }
}
```

兼容输入别名：页面宽高也可写成顶层 `page_width/page_height`；框宽高可使用 `width/height`；字号档位可使用 `font_size`，支持数值或 `caption/body/title` 等字符串；省略 `index` 时按声明顺序补齐；省略 `lang` 为 `unknown`，省略 `column_hint` 为 `-2`（未知）。坐标均为页面归一化坐标。

策略版本必须单调递增；同一版本不允许内容变化，否则返回 `E_STRATEGY_INVALID`。

## 四阶段流水线

1. **`box_normalize_merge`**
   - 归一化坐标按页面像素宽高换算，并以取整后的左右/上下边界反推像素宽高，保证边界一致。
   - 校验有限值、`[0,1]` 边界和正宽高；失败返回 `E_BOX_INVALID`，带 `doc_id`、`box_index` 和诊断集合。
   - 按页边距和字号档位分类 `header`/`body`/`footer`。
   - 同类框仅在垂直/水平投影重叠、且另一轴间隙严格小于 `merge_gap_px` 时合并；列提示不同或通栏提示 `-1` 时作为合并屏障。
   - 合并组按源框序号稳定排序，文本、语言、包围盒与源框集合均确定性生成。

2. **`column_inference`**
   - 排除页眉页脚；正文宽度达到 `span_width_ratio × 有效宽度` 的框标记为通栏。
   - 对非通栏框的左起点建立直方图，结合框宽扩展形成栏区间。
   - 栏区间连续、互不重叠，并覆盖 `[margin_x_px, page_width-margin_x_px]`。
   - 无法归属的框返回 `E_COLUMN_UNASSIGNED`，不会静默并入相邻栏。

3. **`reading_order`**
   - 页眉最前、页脚最后。
   - 正文按通栏块分出纵向栏带；栏带内先左栏后右栏，同栏自上而下。
   - 通栏块与正文垂直重叠时，由 `cross_column_priority` 裁决；块序列严格去重。
   - 每条相邻顺序的置信度显式计算垂直间隙分数与栏归属分数。全部置信度为零或检测到裁决环时返回 `E_ORDER_CONFLICT` 和冲突框对。

4. **`result_summary`**
   - 固化块序列摘要、栏统计、字符数、占用面积比和最终结果指纹。
   - 输出字段使用 Go `encoding/json` 的稳定字段顺序，不含时间戳、路径或随机值。

## 错误码

| code | 含义 |
|---|---|
| `E_BOX_INVALID` | 框坐标越界、非有限值或宽高为零 |
| `E_COLUMN_UNASSIGNED` | 无法推断栏、栏数为零或框跨栏无归属 |
| `E_ORDER_CONFLICT` | 全零置信度、裁决环或重复源框 |
| `E_LATENCY_BUDGET` | 查询超过毫秒预算；包含 `query_id`、`elapsed_ms` |
| `E_STALE_CHECKPOINT` | 跳号、重复、回退、跨文档错位或并发发布落后 |
| `E_CHECKPOINT_ROLLBACK` | 快照序号回退 |
| `E_SNAPSHOT_REPLACED` | 相同序号下指纹/摘要整体变化 |
| `E_CHECKPOINT_CORRUPT` | 产物 JSON、摘要或指纹不匹配 |
| `E_STRATEGY_INVALID` | 策略参数无效或版本不单调 |

## SQLite 检查点

每次阶段完成时，在一个 `BEGIN IMMEDIATE` 事务内更新：

```sql
stage_checkpoints(
  doc_id, stage, stage_ordinal, seq,
  input_fingerprint, strategy_version,
  artifact_summary, artifact_fingerprint, artifact_json, created_at,
  PRIMARY KEY (doc_id, stage)
)
documents(
  doc_id, content_fingerprint, strategy_version, seq, updated_at
)
```

- 同一文档同一阶段只有一条有效记录。
- 文档内序号连续递增；条件 `UPDATE ... WHERE doc_id=? AND seq=?` 与事务内校验一起拒绝落后、重复、跳号和跨文档错位发布。
- 发布使用真实文件锁 `<db>.lock`（`flock`），并用事务保证任意失败点均不会留下半写记录。
- `<db>.guard.json` 保存已观察到的快照摘要与各文档序号，用于区分：
  - 序号更高且可解释：正常推进；
  - 任一文档序号回退：`E_CHECKPOINT_ROLLBACK`；
  - 相同序号但内容摘要变化：`E_SNAPSHOT_REPLACED`。
- `artifact_summary` 与 `artifact_fingerprint` 均为产物内容 SHA-256；读取时再次校验 JSON 与摘要。
- 导入会持久化 `analysis_pending=1`；只有一整轮分析成功结束才清除。进程重启后若仍为 pending，查询/导出会拒绝回退旧检查点，必须恢复执行。

可直接用 SQLite 核对：

```bash
sqlite3 demo.sqlite ".tables"
sqlite3 demo.sqlite "SELECT doc_id,stage,seq,substr(input_fingerprint,1,12) FROM stage_checkpoints ORDER BY doc_id,stage_ordinal;"
sqlite3 demo.sqlite "SELECT doc_id,seq,substr(content_fingerprint,1,12) FROM documents ORDER BY doc_id;"
./layoutread check -db demo.sqlite
```

### 停止与恢复

- `analyze` 监听 `SIGINT/SIGTERM`，调用停止请求；停止只发生在文档边界，当前文档四阶段完成后退出。
- `serve` 的 `POST /api/stop` 同样处理，并在信号退出时优雅关闭 HTTP 服务。
- `analyze -stop-file <path>` 可在文档边界轮询合作式停止文件；`./layoutread stop -db demo.sqlite` 会创建默认 `<db>.stop`。CLI 还可加 `-stage-delay-ms 50` 放慢阶段，便于实测停止边界。
- 恢复时先校验已提交检查点；阶段输入指纹相同则复用产物，否则从最早失效阶段重建。连续运行与“停止后恢复再导出”的 JSON 逐字节一致。

## 增量失效

阶段输入指纹分别包含：

- 规范化：文档内容 + 合并阈值 + 页边距 + 页眉页脚字号门限；
- 栏推断：规范化产物摘要 + 直方图/栏间隙/最小支持/通栏比例；
- 阅读顺序：栏产物摘要 + 跨栏优先级 + 两个置信度权重；
- 汇总：阅读顺序产物摘要。

因此可精确定位最早失效阶段。例如只修改栏参数时，规范化阶段继续复用，栏推断及其全部下游重建；未修改文档的四个阶段全部复用。服务运行报告中的 `invalidations`、`earliest_invalid_stage` 和 `reused_stages` 给出最小集合与复用事实。

## 延迟预算与查询缓存

查询范围：

- `blocks`：最终块序列摘要；
- `confidence`：相邻块置信度明细；
- `columns`：栏区间与栏统计。

缓存键包含文档内容指纹、策略版本、完整策略指纹、查询范围和结果指纹。导入新样本/策略时清空缓存；内容指纹或版本变化也必然产生新键，不会命中旧结果。

超时查询在独立 goroutine 中计算，HTTP/CLI 通过计时器立即返回：

```json
{
  "code": "E_LATENCY_BUDGET",
  "message": "query exceeded latency budget of 0 ms",
  "doc_id": "doc-double-en",
  "query_id": "budget-check",
  "elapsed_ms": 0.454
}
```

HTTP 查询可使用 `budget_ms=0` 稳定触发超时；也可附加 `simulated_delay_ms=30` 验证慢计算不会阻塞请求。

## HTTP API

启动：`./layoutread serve -db demo.sqlite -demo`。根路径 `/` 是内置的可交互控制台。

| 方法与路径 | 作用 |
|---|---|
| `GET /api/demo` | 获取固定样本与策略快照 |
| `POST /api/import` | 导入 bundle JSON |
| `POST /api/analyze[?stage_delay_ms=n]` | 全量/增量分析并返回运行报告；可选阶段延迟便于实测停止 |
| `POST /api/resume` | 与 analyze 相同，语义上用于恢复 |
| `POST /api/stop` | 请求在当前文档完成后停止 |
| `GET /api/status` | 文档、阶段、序号、产物摘要状态 |
| `GET /api/fingerprint` | 策略与各阶段版本指纹 |
| `GET /api/query?doc_id=...&scope=blocks&budget_ms=25` | 延迟预算查询 |
| `GET /api/export` | 导出确定性完整结果 |
| `POST /api/demo/run` | 导入固定演示、运行并导出 |

错误响应为顶层结构化 JSON；`E_LATENCY_BUDGET` 使用 HTTP 408，冲突类错误使用 409。

## 测试与实测结果

```text
$ CGO_ENABLED=0 go test ./...
ok  layoutread/cmd/layoutread
ok  layoutread/internal/demo
ok  layoutread/internal/engine
ok  layoutread/internal/httpapi
ok  layoutread/internal/model
ok  layoutread/internal/service
ok  layoutread/internal/store
```

已实际验证：

- 固定演示两次运行输出 SHA-256 均为 `44e39848dd61ba4db43b6a37ede788b3d2dc1fadc26d8ee4089e77bed7161c45`，`cmp` 无差异；
- 阅读顺序测试无重复源框，页眉在前、页脚在后，双栏为左栏到底后右栏，中段通栏标题正确分隔上下栏带；
- 全零置信度返回 `E_ORDER_CONFLICT` 并附冲突框对；
- `budget_ms=0` 与模拟慢查询均在预算到达后返回 `E_LATENCY_BUDGET`，缓存命中与重新计算数据逐字节一致；
- 栏参数变化时四份文档均从栏推断阶段失效且仅复用规范化；单文档内容变化时仅该文档失效，其余文档四阶段全部复用；
- 两种增量导出均与对应全量重建导出逐字节一致；
- 同一数据库第二次运行全部四阶段复用，首次与第二次 `export` 文件 SHA-256 均为 `59c58820659673d15f8272e3988158dec4c5a6873c9931f49e214e307a16f392`；
- 停止后恢复的导出与不间断运行逐字节一致；
- 失败注入不产生半写检查点；重复/跳号/跨文档/并发落后发布返回 `E_STALE_CHECKPOINT`；
- 快照回退与整体替换分别返回 `E_CHECKPOINT_ROLLBACK` 和 `E_SNAPSHOT_REPLACED`；
- HTTP 端到端导入、分析、状态、指纹、查询、停止及错误映射均通过测试；真实服务实测分析/查询/状态为 200，连续两次查询 `cache_hit` 从 `false` 变为 `true` 且数据逐字节一致，零预算查询返回 408，控制台首页 200。

另外执行了 `go test -race ./internal/service ./internal/httpapi ./internal/store`，未发现数据竞争。

## 目录

```text
cmd/layoutread/       CLI 与 HTTP 启动入口
internal/model/       输入/输出模型、结构化错误、稳定指纹
internal/engine/      四阶段引擎及阶段输入指纹
internal/store/       SQLite 事务、文件锁、快照守卫
internal/service/     增量编排、停止恢复、查询缓存与导出
internal/httpapi/     HTTP JSON API 与内嵌控制台
internal/demo/        固定种子样本生成器
```
