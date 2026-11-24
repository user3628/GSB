# Panoptic 掩码修订工作台

一个可本地完整演示的全景分割标注修订工作台。它不只是转换脚本：固定种子生成器会产出 4 个索引色 PNG 分片、v2 修订日志、v1 兼容日志与分叉夹具；工作台把每条修订逐条重放到像素级段 id 图和段属性表，提供历史快照、差异归因、撤销裁决、增量失效、SQLite 检查点恢复和多进程发布互斥，并附带本地 HTTP 工作台。

全部样本、日志和数据库均由程序在本地生成，不需要网络、外部数据集、Docker 或系统级 SQLite。

## 1. 快速开始

前置条件：

- Go 1.22 或以上
- `go version`、`go env GOPATH` 可用
- 不需要 CGO；SQLite 使用纯 Go 驱动 `modernc.org/sqlite`

Linux/macOS：

```bash
go version
go mod tidy
go test ./...
go run ./cmd/maskrev demo --work .maskrev-demo
go run ./cmd/maskrev serve --work .maskrev-demo --addr 127.0.0.1:8080
```

如本机到 `proxy.golang.org` 不可达，可先使用任一 Go 模块镜像完成一次依赖下载，例如：

```bash
GOPROXY=https://goproxy.cn,direct go mod tidy
```

Windows PowerShell：

```powershell
go version
go mod tidy
go test ./...
go run .\cmd\maskrev demo --work .maskrev-demo
go run .\cmd\maskrev serve --work .maskrev-demo --addr 127.0.0.1:8080
```

打开 `http://127.0.0.1:8080` 可查看分片列表、修订时间线、历史掩码、段属性表、差异归因、检查点、v1 升级和分叉/改动作/回退夹具裁决。

`demo` 与 `init` 会先清理指定工作目录，再生成数据、重建 SQLite 并执行重放。默认工作目录是 `.maskrev`，默认种子是 `20261125`。

## 2. 固定样本

生成器位于 `internal/generator`，每次生成：

- 4 个 `40×30` 索引色 PNG 分片：`shard-00` 至 `shard-03`
- 每个分片 12 个初始段，另含保留的忽略段 id `0`
- `manifest.json`：结构版本 v2，内含分片、图像摘要、初始段属性表、日志路径
- `manifest.v1.json`：旧清单结构副本
- `logs/*.v2.json`：原生 v2 日志
- `logs/shard-01.v1.json`：以自然语言短语表达动作、实例改判无目标段号、置信度为整数百分比的 v1 日志
- `fixtures/shard-00.fork.json`：撤销后以旧修订为父的新修订，用于分叉裁决
- `fixtures/shard-00.edited.json`：中间修订动作内容被改写
- `fixtures/shard-00.rollback.json`：日志被截断回退
- `legacy/v1-unrecoverable.json`：邻域无法推断改判目标时的不可恢复夹具

`shard-00` 的 8 条主日志包含连续撤销：rev 4 撤销 rev 3，rev 6 撤销 rev 5，rev 7 继续撤销 rev 2。最终 rev 2、3、5 为 undone，rev 4、6、7 是对应新修订点。分叉夹具在第 5 条修订声明 `parent_revision=2`，与当前线性头 rev 4 冲突。

## 3. CLI

```text
maskrev demo        [--work DIR] [--seed N] [--workers N]
maskrev init        [--work DIR] [--seed N] [--overlap POLICY] [--undo-policy POLICY]
maskrev generate    [--data DIR] [--seed N]
maskrev replay      [--work DIR] [--fresh] [--crash-after N] [--hard-kill]
maskrev serve       [--work DIR] [--addr 127.0.0.1:8080]
maskrev snapshot    [--work DIR] --shard ID --revision N [--png FILE]
maskrev diff        [--work DIR] --shard ID --from N --to N
maskrev upgrade     [--work DIR] --shard ID
maskrev classify    [--work DIR] --shard ID --fixture RELATIVE_PATH
maskrev publish     [--work DIR] --shard ID --expected N --revision N [--publisher NAME]
maskrev verify      [--work DIR]
```

可选配置：

- `--overlap last_write_wins|first_write_wins`
- `--undo-policy recycle|void`
- `--phrase-map FILE`：v1 短语映射，接受 `[{"contains":"重涂","action":"repaint_category"}]` 或包含 `phrase_rules` 的对象
- `--neighbor-radius N`
- `--strict-v1`：不可恢复 v1 动作立即失败

常用示例：

```bash
go run ./cmd/maskrev init --work .maskrev
go run ./cmd/maskrev snapshot --work .maskrev --shard shard-00 --revision 4 --png mask4.png
go run ./cmd/maskrev diff --work .maskrev --shard shard-00 --from 1 --to 4
go run ./cmd/maskrev upgrade --work .maskrev --shard shard-01
go run ./cmd/maskrev classify --work .maskrev --shard shard-00 --fixture fixtures/shard-00.fork.json
go run ./cmd/maskrev publish --work .maskrev --shard shard-00 --expected 1 --revision 2
```

`replay --crash-after N --hard-kill` 会在第 N 个新检查点事务提交后直接杀死当前进程，不生成退出报告；随后再次运行任意查询命令即可从已提交 SQLite 状态恢复。

## 4. 本地 HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 工作台页面 |
| GET | `/api/health` | 健康状态与工作区摘要 |
| GET | `/api/shards` | 4 个分片摘要 |
| GET | `/api/shards/{shard}` | 分片、修订、检查点联合视图 |
| GET | `/api/shards/{shard}/revisions` | 修订时间线及 active/undone 状态 |
| GET | `/api/shards/{shard}/snapshots/{rev}` | 可寻址掩码快照与段属性表 |
| GET | `/api/shards/{shard}/masks/{rev}.png` | 索引色掩码渲染 |
| GET | `/api/shards/{shard}/diff?from=A&to=B` | 差异像素、受影响段和首次来源 |
| GET | `/api/shards/{shard}/checkpoints` | 已提交检查点及单调序号 |
| POST | `/api/shards/{shard}/publish` | 事务 + 文件锁的发布闸门 |
| POST | `/api/shards/{shard}/apply-log` | 导入新日志并增量失效/重建 |
| POST | `/api/shards/{shard}/upgrade/export` | 导出升级 v2 并验证回读轨迹 |
| GET | `/api/fixtures?name=fork`（另支持 `edited`、`rollback`） | 日志变更裁决夹具 |
| GET | `/api/demo` | 读取 `demo` 生成的 `report.json` |

业务错误统一返回：

```json
{
  "error": {
    "code": "E_REVISION_UNDONE",
    "message": "revision 2 was undone by revision 7",
    "shard_id": "shard-00",
    "revision": 2,
    "reason": "revision 2 was undone by revision 7"
  }
}
```

主要业务码：

- `E_REVISION_OUT_OF_RANGE`：查询修订超出已重放范围
- `E_REVISION_UNDONE`：查询点指向被撤销的修订
- `E_UNDO_INITIAL`：无可撤销编辑，或尝试撤销初始状态
- `E_FORK_CONFLICT`：显式父修订与当前活动头不一致
- `E_REGION_OUT_OF_BOUNDS`：区域越界，附首个越界坐标
- `E_SEGMENT_NOT_FOUND`：目标段不存在或非 active
- `E_PIXEL_COUNT_MISMATCH`：段属性表像素总数与 id 图不一致，附首个差异像素
- `E_STALE_CHECKPOINT`：落后、重复、不连续或跨分片错位发布
- `E_LOCK_TIMEOUT`：发布文件锁超时
- `E_ACTION_UNRECOVERABLE`：v1 实例改判无法推断目标

## 5. 重放模型与语义

内部模型由两部分组成：

1. `Pixels []int`：行主序像素级段 id 图
2. `Segments map[int]*Segment`：类别、实例号、状态、像素总数、来源、创建/更新修订及历史事件

四类动作：

- `repaint_category`：将矩形区域写入目标段；未给目标段时按类别分配新实例段；给目标段时同步重涂该段类别
- `reassign_instance`：将矩形区域像素改判到已有目标段
- `ignore`：将矩形区域写为保留忽略段 id `0`
- `undo`：回退最近一个未撤销的非 undo 编辑

撤销规则：

- undo 动作本身不是下一次 undo 的候选目标
- 连续 undo 会继续寻找更早的、尚未撤销的编辑
- 目标修订的像素逆日志、段属性前像和派生段列表共同完成回滚
- `recycle`：派生段标为 `recycled`，加入空闲 id 池，可由后续新建段复用
- `void`：派生段标为 `voided`，永不复用
- 两种策略都会在段属性表中追加历史事件，保留撤销痕迹

分叉规则：

- 新修订若显式声明父修订且不等于当前活动头，记录 `ForkDecision{decision:"rejected"}` 并返回 `E_FORK_CONFLICT`
- “撤销后在中间修订点插入新修订”的夹具因此得到确定裁决，而不是静默近似合并
- 日志整体变化另由版本指纹分类为 `append`、`replacement`、`rollback`、`fork`

重叠策略：

- `last_write_wins`：新区域写入覆盖旧像素，默认策略
- `first_write_wins`：已被先前区域写过的像素冻结；undo 会恢复写归属，使像素可再次被写

一致性：

- 每次区域操作增量维护段像素数
- 每次动作后逐像素重建统计并校验段属性表总像素数
- 越界、缺失段、状态非法或计数偏差都返回带分片、修订号和首个差异坐标的业务码

## 6. v1 兼容升级

v1 导入支持：

- 从 `action_phrase` 或自然语言 `action` 按可配置规则识别动作
- 实例改判缺目标段时，在区域外邻域内统计不同段 id；排除区域内已有段 id，按“出现次数最多、相同则 id 最小”推断
- 置信度整数百分比除以 100，转为 `[0,1]` 浮点
- 每个动作记录 `inference_basis`
- 无法推断目标时生成显式 `noop` 动作并记录 `unrecoverable_reason`，仍可导出为 v2 并重放；默认同时报告，`--strict-v1` 则直接失败

`maskrev upgrade --shard shard-01` 会写出：

- `exports/shard-01.upgraded.v2.json`
- `exports/shard-01.upgrade-report.json`

并分别重放原 v1 规范化日志与导出 v2 日志，逐修订比较快照摘要。工作台内置样本结果为 `trace_equal=true`。

## 7. 差异与快照

每个重放修订点都持久化为可寻址快照：

- 修订号
- 输入分片指纹
- 日志前缀指纹
- 修订动作指纹
- 像素 id 图摘要
- 段属性表摘要
- 快照总摘要

差异查询返回：

- 完整差异像素集合（坐标、前段 id、后段 id）
- 受影响段清单及像素数变化
- 类别、实例、状态是否变化
- 每段的类别变化来源、实例变化来源、像素成员变化来源
- 全局首次造成差异的修订动作

如果修订号超出已重放范围，或修订已被后续 undo 撤销，查询拒绝返回近似结果，分别返回 `E_REVISION_OUT_OF_RANGE` / `E_REVISION_UNDONE`。

## 8. SQLite 检查点与恢复

`modernc.org/sqlite` 以纯 Go 方式链接。默认数据库为 `<work>/checkpoints.db`，核心表：

- `checkpoints`：每个修订一条有效记录，`UNIQUE(shard_id, revision)`；包含输入/日志/修订/段表/像素/快照摘要、压缩状态和快照 blob、单调自增 `seq`
- `publish_state`：每分片当前已发布的连续修订与行版本
- `publications`：发布记录，`UNIQUE(shard_id, revision)`
- `audit_events`：重放失败、分叉拒绝等审计
- `upgrade_reports`：v1 推断依据与不可恢复动作
- `shard_logs`：以原始日志摘要和输入分片指纹为键，持久化“实际导入的规范化日志”；自定义 v1 短语映射只需首次生效，后续恢复/查询不会因缺省参数不同而重解释

恢复流程：

1. 启动时读取当前日志前缀指纹
2. 从最新检查点向下找到与当前输入分片、修订指纹和日志前缀完全匹配的状态
3. 删除失效点及下游记录（如发生替换/回退/分叉）
4. 从最近有效检查点恢复完整重放状态
5. 仅应用剩余修订，每个修订完成后再提交下一个检查点

强制终止测试使用真实子进程 `SIGKILL`：删除退出清理机会后，仅凭已提交事务恢复。恢复后的每个修订快照与连续执行逐字节比较一致。

## 9. 增量失效与发布互斥

版本指纹由“输入分片内容摘要 + 每修订动作摘要构成的前缀摘要”组成：

- 旧序列是新序列前缀：`append`
- 第 1 条即不同或输入分片变化：`replacement`
- 新序列是旧序列截断：`rollback`
- 显式分支/父修订或撤销后旧父插入：`fork`

只删除最早受影响修订及其下游检查点，更早检查点行和 `seq` 保持原样。`apply-log` 可直接演示该过程；工作区测试验证了追加只重放新修订、动作编辑从 rev 5 起重建、截断保留更早检查点。

发布闸门同时使用：

- OS 级文件锁 `<work>/locks/<shard>.publish.lock`（Unix `flock`，Windows `LockFileEx`）
- SQLite 事务内读取当前发布修订
- `UPDATE ... WHERE revision=? AND row_version=?` 条件更新
- `UNIQUE(shard_id, revision)` 约束

只允许 `revision == current + 1` 且 `expected == current`。多进程竞争发布同一修订时恰好一个成功，其余返回 `E_STALE_CHECKPOINT`；重复、落后和跨分片借用快照摘要同样被拒绝。

## 10. 验证结果

当前实现已实际执行以下验证：

```text
go test ./...          PASS
go test -race ./...    PASS
go vet ./...           PASS
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...   PASS
maskrev demo --workers 4                                 PASS
```

两次在不同临时工作目录运行的完整 demo 得到相同摘要：

```text
workspace_digest = 8c02f34b3734a09be84d8d11f83d589ffd974f90194cba1d35435bc8974b8cfa
summary_digest   = 568c36147d71a6a27e08662acd72e97a8c8d6676386d690d325ce9a6d62c3f06
```

本次完整演示的关键结果：

```text
分片数量                         4
shard-00 主日志修订              8，最终撤销 2/3/5
v1 日志往返                      trace_equal=true
v1 推断目标                       rev 1 邻域段 2，basis 完整记录
不可恢复 v1 夹具                  unrecoverable_count=1
快照 shard-00 rev4               snapshot_digest=609b3976a65c73f3…
差异 shard-00 rev1 → rev4        25 像素，首次来源 rev2 reassign_instance
分叉夹具                         kind=fork，earliest=5，E_FORK_CONFLICT
动作编辑夹具                     kind=replacement，earliest=5，rev1–4 复用
回退夹具                         kind=rollback，earliest=5
强制终止恢复                     第 3 个检查点后 SIGKILL，恢复摘要与连续执行一致
发布竞争                         4 进程：1 成功，3 个 E_STALE_CHECKPOINT，最终 rev2
额外发布探针                     落后/重复/跨分片均 E_STALE_CHECKPOINT
```

`report.json` 中的 `generated_at_utc` 不参与 `summary_digest`；其余结果均为稳定内容，因此同一份代码在不同绝对路径下可复现固定摘要。

## 11. 目录结构

```text
cmd/maskrev/               CLI 与本地 HTTP 入口
internal/generator/        固定种子 PNG/日志/清单生成器
internal/revlog/           v2 解析、v1 升级与导出
internal/model/            像素图、段属性、快照、规范摘要、业务错误
internal/replay/           四类动作重放、撤销、分叉、差异归因
internal/reconcile/        版本指纹分类与最早失效修订
internal/store/            SQLite 检查点、恢复、发布锁与 CAS
internal/workspace/        工作区编排、查询、增量同步
internal/httpapi/          HTTP API 与内嵌工作台
internal/demo/             固定摘要端到端演示
```

## 12. 设计边界

- 区域是半开矩形；未引入任意多边形栅格化
- 索引色 PNG 以调色板索引承载段 id，因此该演示格式要求可见段 id 不超过 255；内部模型与 JSON 不依赖此限制
- 默认将撤销后旧父插入视为分叉并拒绝；裁决和原因完整留痕
- undo 动作和不可恢复 noop 不作为下一次 undo 的目标
- 发布闸门记录在独立 `publish_state/publications` 表；重放检查点在 `checkpoints` 表，两者通过分片、修订和快照摘要交叉校验

这些边界不影响题目要求的演示、恢复、差异、撤销与并发验证。
