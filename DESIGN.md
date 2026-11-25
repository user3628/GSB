# 查询改写与前缀缓存收益裁决组件 — 技术说明

一套纯 Go（无 CGO、无容器、无外部数据）实现的查询改写 / 前缀缓存裁决流水线。
固定种子生成器在本地产出 JSONL 会话分片与 YAML 策略快照，四阶段结果以事务
检查点写入 SQLite，支持强制终止后逐字节一致的恢复。

## 1. 环境与依赖

- Go：本机 `go1.27.4`（`go.mod` 声明 `go 1.26.0`，满足任务要求的 ≥1.22）。
  Windows 安装官方 MSI 包后，在“系统属性→环境变量”确认 `GOROOT/bin` 与
  `GOPATH/bin` 在 `Path` 中即可，命令完全一致。
- SQLite：命令行 `sqlite3` 用于人工核查；程序侧使用纯 Go 驱动
  `modernc.org/sqlite`，`CGO_ENABLED=0 go build ./...` 可直接通过，无需 C 编译器。
- YAML：`gopkg.in/yaml.v3`；NFKC 规范化：`golang.org/x/text/unicode/norm`。
- HTTP 演示仅用标准库 `net/http`。
- 模块代理：官方 `proxy.golang.org` 在本机不可达，使用 `goproxy.cn`
  （`go env -w GOPROXY=https://goproxy.cn,direct`）。

## 2. 目录结构

```
cmd/qr            CLI 与 HTTP 入口（generate/run/status/report/patch/replace/recover/serve）
internal/spec     领域类型、稳定业务码（E_*）、阶段常量
internal/norm     NFKC/全半角/小写/空白折叠 + 词表最长匹配中文切分（带 rune 偏移）
internal/gen      固定种子生成器、内置同义词表、演示会话与基线策略
internal/engine   四阶段：generate / prefix / adjudge / schedule + 流水线/恢复/报告
internal/policy   策略校验、内容摘要、增量补丁、整体替换/回退分类、版本指纹
internal/store    SQLite（modernc 纯 Go）、事务检查点、条件发布、跨平台文件锁
internal/server   net/http 服务（服务级单例 Engine，互斥访问）
```

## 3. 四阶段流水线

1. 规范化与候选生成（`stage1_generate`，`engine/generate.go`）
   - NFKC（顺带统一全/半角）→ 小写 → 连续空白折叠为单空格。
   - 中文按本地同义词词表做最长匹配、未登录汉字回退单字；拉丁/数字按连续串切分，
     全部保留 `[start,end)` rune 偏移。
   - 两条策略：`history`（历史改写）与 `synonym`（同义词逐词位替换），按
     策略优先级 + 稳定序号去重排序，再按 `candidate_depth` 截断。
   - 输出候选文本、策略、与原查询的 Jaccard 词项重合度。
   - 空查询 `E_EMPTY_QUERY`、零候选 `E_NO_CANDIDATES`、空改写 `E_EMPTY_REWRITE`，
     错误均带 `session_id / turn / stage`；同输入同配置产出逐字节一致。

2. 前缀指纹与命中判定（`stage2_prefix`，`engine/prefix.go`）
   - 候选按 `min_prefix_runes` 截可缓存前缀，按 `prefix_block_runes` 逐块用固定
     FNV-1a 64 位哈希生成块指纹，记录块序号与 rune 跨度，再拼成全指纹。
   - 三态：`EXACT_HIT`（全块命中槽位）、`PARTIAL_HIT`（最长公共前缀块）、
     `NOT_REUSABLE`（无共享块）；前缀短于阈值为 `PREFIX_BELOW_THRESHOLD`。
   - 同轮同指纹多候选记录 `contention_with`；容量满时按 LRU（年龄降、槽位 id 升）
     确定性选择挤出对象并留痕。阶段二为纯只读，不修改缓存，保证可重放。

3. 收益与失效率逐候选裁决（`stage3_adjudge`，`engine/adjudge.go`）
   - 对数域收益：`log B = Wo·log(1+overlap) + Wh·log(hitFactor) − Wa·log(1+age)`。
   - 失效率上界由槽位年龄、前缀块数、命中情形显式推导：
     `agePart=age/(1+age)`、`blockPart=1−exp(−blocks)`、
     `reusePenalty=1−hitFactor`，
     `upper = agePart·(0.25+0.25·blockPart) + 0.5·reusePenalty`，天然落在 `[0,1]`，
     全部中间量写入 `derivation` 保留推导依据。
   - 决策：`benefit ≥ hit_gain_threshold 且 upper ≤ failure_upper_limit` 才 ACCEPT。
   - 权重 ≤0、对数项非有限、上界越界均返回带候选标识的结构化 `E_NUMERIC`。

4. 预算约束逐轮调度（`stage4_schedule`，`engine/schedule.go`）
   - 三重约束：策略千分比预算（换算为每轮各策略采纳上限
     `perMille/1000 × eval_budget`）、单轮评估次数 `eval_budget_per_turn`、
     缓存 `cache_capacity`（确定性 LRU 挤出）。
   - 确定性优先级：收益降序、失效率上界升序、候选 id 升序。
   - 千分比之和 ≠ 1000 时显式 `E_BUDGET_MILLE` 并按比例再平衡降级，绝不静默截断；
     每轮台账 `BudgetLedger` 记录评估消耗、各策略用量、容量、采纳与放弃原因，可事后复算。

## 4. 检查点与强制终止恢复（需求 5）

- 每轮每阶段在独立事务写 `checkpoints`，含 `stage/session_id/turn/input_digest/
  policy_version/product_digest/seq`，`UNIQUE(stage,session_id,turn)` 保证同会话
  同轮同阶段仅一条有效记录。
- 阶段四与槽位表通过 `CommitCheckpointWithSlots` 在**同一事务**原子提交：崩溃要么
  同时生效、要么都不生效。
- 重启仅凭已提交事实重建：已完成阶段从检查点装载；未完成阶段按确定性纯函数重算；
  缓存通过重放已提交阶段四（指纹派生的稳定槽位 id + 确定性 LRU）还原。
- 槽位 id 由指纹哈希派生（非单调计数器），连续执行与恢复路径生成相同 id。
- 已记录 stage1 业务错误的轮在恢复时短路，不再合成后续阶段。

## 5. 策略快照：增量 / 替换 / 回退（需求 6）

- 内容摘要 `policy.Digest`（规范化 JSON 的 SHA-256）+ 版本序号共同判定：
  - 增量：`version=current+1` 且单字段补丁，返回受影响的最早阶段（如改
    `candidate_depth` → stage1，改 `min_prefix_runes` → stage2）。
  - 整体替换：血缘断裂（`parent_version≠current` 或跳号）且 `kind=replacement`，
    清空全部槽位与裁决缓存重建。
  - 回退：`incoming.version < current.version`，拒绝并返回 `E_STALE_VERSION`，
    附 `current_version` 与当前摘要。

## 6. 并发互斥与陈旧读（需求 7）

- 真实文件锁：Unix `flock(2)`、Windows `LockFileEx`（按构建标签分文件）。
- SQLite WAL + `busy_timeout=10000`，写事务带 `SQLITE_BUSY` 有限重试。
- 条件更新：`turn_index` 仅允许会话内轮次连续推进；落后/重复/跨会话错位发布返回
  `E_STALE_CHECKPOINT`；同 `(stage,session,turn)` 重复检查点同样拒绝。
- 携带 `observed_version` 的读取低于当前生效版本时返回 `E_STALE_READ`（HTTP 409），
  明确要求重算，不返回陈旧结论。
- 槽位以全指纹唯一，同指纹争用只写一个槽位，不会被两个轮次重复占用。

## 7. 接口（需求 8）

CLI：`generate / run / status / report / patch / replace / recover / serve`，
`run -crash-after <stage>` 可模拟该阶段提交后立即 SIGKILL（退出码 137）。

HTTP（仅 net/http）：
`/healthz /import /policy /policy/patch /policy/replace /run /recover /status
/report /stop`，支持优雅停止。

## 8. 确定性演示数据

固定种子 `20261125`，3 个会话、9 个有效轮 + 1 个空查询错误轮，覆盖：
精确命中、部分前缀命中、不可复用、同轮前缀争用、容量满 LRU 挤出、空查询业务码。
完整运行产生固定报告摘要（见下“验证结果”）。

## 9. 快速复现

```bash
go env -w GOPROXY=https://goproxy.cn,direct
go build ./...
go test ./...                      # 单元 + 恢复 + 并发互斥
go run ./cmd/qr generate -out data
go run ./cmd/qr run -data data -db data/qr.db
go run ./cmd/qr report -data data -db data/qr.db -out data/report.json

# 强制终止恢复（任一阶段）
go run ./cmd/qr run -data data -db crash.db -crash-after stage2_prefix   # 137
go run ./cmd/qr recover -data data -db crash.db
```

## 10. 实际验证结果（本机 go1.27.4 / linux-amd64）

- `go test ./...` 与 `go test -race ./...`：全部包通过（norm / policy / engine /
  store），含确定性、业务码、E_NUMERIC、千分比降级、并发互斥、崩溃恢复用例。
- `CGO_ENABLED=0 go build ./...`：通过，确认纯 Go、无 C 编译器依赖。
- 两次 `generate` 产物 `diff -r` 完全一致（固定种子）。
- 演示固定报告摘要：
  `sha256:01594884fed552a6f8821f3f7df1df9ab6a33c09f4334148961549888789002e`
- 命中情形统计：`EXACT_HIT=1, PARTIAL_HIT=2, NOT_REUSABLE=42`；
  前缀争用事件 2；LRU 挤出留痕 38；持久槽 2（容量=2）。
- 空查询轮返回 `E_EMPTY_QUERY`（带 session/turn/stage），不产生后续阶段。
- 强制终止恢复：在 `stage1_generate / stage2_prefix / stage3_adjudge /
  stage4_schedule` 四个点分别注入 SIGKILL（退出码 137），`recover` 后报告与连续
  执行的报告 SHA-256 完全相同（逐字节一致），检查点 `product_digest` 全表一致。
- 策略：增量补丁返回最早受影响阶段；整体替换清空槽位；版本回退返回
  `E_STALE_VERSION` 并附当前生效版本；陈旧 observed_version 读取返回
  `E_STALE_READ`（HTTP 409，要求重算）。
- 并发：8 个独立连接竞争同一会话首轮，恰有 1 个成功、其余 7 个 `E_STALE_CHECKPOINT`。
