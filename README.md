# depverify — 依赖闭包完整性核验引擎

在构建前校验内部组件依赖图是否被人为降级或替换的本机离线引擎。纯 Go 实现，
SQLite 通过纯 Go 驱动 `modernc.org/sqlite` 集成，无需系统数据库服务、无需网络。

## 环境准备

- Go 1.22+（开发机实测 go1.27.4，`go.mod` 声明 `go 1.22`）
- 依赖离线缓存：`go mod tidy` 后可用 `GOPROXY=off go build ./...` 完全离线构建
- Windows 下直接使用 `go build` / `go test`，无 CGO、无 shell 脚本依赖

```bat
go mod tidy
go build ./...
go test ./...
go build -o depverify.exe .\cmd\depverify
```

Linux/macOS：

```bash
go build -o depverify ./cmd/depverify
```

## 模块结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 业务错误码、组件/边/结论模型、版本序列比较、组件键规范化 |
| `internal/manifest` | 二进制清单流式编码/解析，魔数与长度前缀边界校验 |
| `internal/edges` | 依赖边 JSONL 解析、重复边与悬挂边校验 |
| `internal/closure` | 闭包 BFS、版本降级、Tarjan 环检测、约束冲突裁决 |
| `internal/engine` | 规模自适应双路径核验、摘要核验、结论规范化 |
| `internal/snapshot` | SQLite 版本化快照、单事务提交、版本链回退、内容哈希 |
| `internal/session` | 并发会话调度、等待队列、组件预算、中断恢复 |
| `internal/samples` | 内置确定性样本生成器（正常 + 六类畸形） |
| `internal/report` | 稳定排序的确定性 JSON 报告 |
| `cmd/depverify` | 命令行入口与配置文件参数注入 |

## 二进制清单格式

- 文件头：8 字节魔数 `DEPVRFY1`
- 每条记录：`uvarint 记录长度` + 记录体，记录体内字段均为 uvarint 长度前缀
  - 组件标识（UTF-8 字节）、版本段数量与各段整数、摘要算法 ID、摘要字节
- 解析严格保证记录边界不越出源缓冲，错误码：
  - `E_BAD_MAGIC`：魔数不符
  - `E_TRUNCATED`：头部/记录/字段截断（保留已解析条目数与最后偏移）
  - `E_LENGTH_OVERFLOW`：长度 uvarint 回绕或记录越界

依赖边为逐行 JSON：`{"parent","child","constraint":[...]}`。组件标识统一小写并
保留命名空间分隔符（`/`、`:`、`.`）；重复边报 `E_DUP_EDGE`，指向清单外组件报
`E_EDGE_DANGLING`，均带行号与字段名。

## 核验规则

- 版本比较：整数序列逐段比较，前缀一致时长者为大；实际版本低于约束报
  `E_VERSION_DOWNGRADE`，记录父子组件、实际与约束两条序列。
- 环检测：Tarjan 强连通分量，输出最小组件集合与闭环边序列，标记 `E_CYCLE`；
  BFS 以 visited 集合保证环边不无限展开。
- 约束冲突：同一子组件存在两个及以上互斥约束时报 `E_CONSTRAINT_CONFLICT`，
  输出约束并集与最小/最大约束两条证明边；所有裁决按字典序规范化，不依赖输入顺序。
- 双路径：闭包组件数低于阈值走 `PRECISE_EDGE` 逐边精算，达到阈值走
  `SORTED_BATCH` 排序索引批量核验；路径与阈值在输出中可观测，可由
  `--threshold` 或配置文件覆盖，两条路径业务结论逐字节一致。
- 摘要核验：对组件标识+版本序列的确定性字节串重算 SHA-256/SHA-512，与登记摘要比对。

## 快照与恢复

- 每轮健康核验在单个 SQLite 事务内提交递增版本快照，保存结论、业务码、算法路径
  与摘要登记来源；内容哈希覆盖完整结论，相同输入复用版本（报告可复现），
  整体替换式变化产生新哈希而非只看版本号。
- 加载时校验从版本 1 起的连续性，跳号返回 `E_SNAPSHOT_VERSION` 并回退到最近连续
  版本，回退目标写入报告（`snapshot_report.fallback_version`）。
- 并发会话独立闭包状态；超并发上限进入等待队列；单会话超组件预算报
  `E_ITEM_BUDGET` 终止且不影响其他会话。
- 进程强制终止后重启：已提交快照不回退，未提交事务由 SQLite 丢弃，`RUNNING`
  任务恢复为 `QUEUED` 重新入队。

## 命令用法

```bash
# 生成确定性样本（9 套：正常 + 截断/坏魔数/长度回绕/悬挂边/降级/环/冲突/跳号）
depverify generate --out samples

# 全量并发核验并导出确定性报告
depverify verify --db depverify.db --threshold 32 --concurrency 3 --out report.json

# 单类异常注入（六类畸形样本，不推进快照版本）
depverify inject --kind version_downgrade --db depverify.db
depverify inject --kind snapshot_gap --db depverify.db   # 制造版本空洞并验证回退

# 双路径逐字节一致性
depverify dualpath --components 64 --threshold 32

# 配置文件注入（JSON，字段与命令行参数同名）
depverify verify --config config.json
```

报告中时间字段仅使用固定采样标记 `SAMPLE@2026-01-01T00:00:00Z`，按任务 ID、
组件标识、边来源与业务码稳定排序，相同输入重复运行字节级一致。

## 业务错误码

| 码 | 触发场景 |
| --- | --- |
| `E_TRUNCATED` / `E_BAD_MAGIC` / `E_LENGTH_OVERFLOW` | 清单解析三类畸形 |
| `E_DUP_EDGE` / `E_EDGE_DANGLING` / `E_EDGE_MALFORMED` | 依赖边校验 |
| `E_VERSION_DOWNGRADE` | 实际版本低于约束 |
| `E_CYCLE` | 闭包内有向环 |
| `E_CONSTRAINT_CONFLICT` | 同一子组件互斥约束 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号 |
| `E_ITEM_BUDGET` | 单会话组件数超预算 |
| `E_ROOT_MISSING` / `E_DIGEST_MISMATCH` | 根缺失 / 摘要篡改 |

## 实测验证结果

- `go test ./...`：6 个测试包全部通过（含双路径逐字节一致、顺序无关、预算终止、
  快照跳号回退、强制终止恢复、重复报告字节一致）。
- `go test -race`：并发会话与引擎竞态检测通过。
- `GOPROXY=off go build ./...`：离线构建通过。
- `GOOS=windows GOARCH=amd64 go build`：Windows amd64 交叉编译通过。
- CLI 端到端：9 套样本各产出预期业务码且互不污染；两次 `verify` 报告 `cmp` 一致；
  `dualpath` 输出 `byte_identical=true`；跳号注入输出
  `code=E_SNAPSHOT_VERSION fallback=1 continuous=false`；畸形注入不推进快照版本。
