# 中文客服工单意图分类与优先级判定引擎

一个完全本地、确定性、无 CGO 依赖的四阶段数据处理管线。输入由内置固定种子生成器
产出 JSONL 工单分片与单调版本化策略快照；中间产物、检查点与统计写入 SQLite，支持
按版本回放、增量重训、强制终止恢复与并发发布互斥。

## 环境要求

- Go 1.26+（`go.mod` 声明 `go 1.26.0`，因纯 Go 驱动 `modernc.org/sqlite v1.63`
  需要该版本；项目本身不使用容器，亦不依赖 CGO）。
- SQLite 驱动为纯 Go 实现，`CGO_ENABLED=0` 即可构建，产出静态链接二进制。
- 仅监听 `127.0.0.1` 回环地址，不访问任何线上站点；样本全部本地确定性生成。

```bash
go mod tidy
CGO_ENABLED=0 go build -o ticketctl ./cmd/ticketctl
```

## 一分钟上手

```bash
mkdir -p data out
./ticketctl generate          # 生成 3 个分片(共 60 张工单)与 policy.v1.json
./ticketctl run               # 跑通规范化->意图->优先级->验证四阶段
./ticketctl status            # 查看各阶段检查点、代数、指纹、序号连续性
./ticketctl verify            # 仅依据 SQLite 事实回放校验不变量
./ticketctl report            # 打印验证报告(规范 JSON)
./ticketctl demo              # 端到端验收演示(含强制终止恢复)
```

工作目录可用环境变量覆盖：`TICKET_DATA_DIR`（默认 `data`）、`TICKET_OUT_DIR`
（默认 `out`）。

## 四阶段管线

| 阶段 | 包 | 职责 | 稳定业务码 |
| --- | --- | --- | --- |
| normalize | `internal/normalize` | NFKC、空白折叠、全半角统一、小写化；中文单字+相邻二字、拉丁字母数字连续串切分；带字符/字节偏移与渠道标记；FNV 特征哈希 | `E_EMPTY_TEXT`、`E_CONFIG` |
| intent | `internal/bayes` | 多项朴素贝叶斯，对数域累加，可配置平滑，未见词项回退显式未知标记，数值守卫 | `E_NUMERIC`、`E_CONFIG` |
| priority | `internal/priority` | 阈值分箱+标注一致度合成优先级；命中/漏判/误判明细；越界与缺失显式降级 | `E_LABEL_RANGE` |
| validate | `internal/validate` | 聚合规范报告，固定摘要，错误码统计，混淆计数 | 回放校验 |

## 确定性是如何保证的

- 随机源唯一：`internal/determ` 的 splitmix64 固定种子 PRNG，不依赖 `math/rand`
  默认源（其跨 Go 版本行为变化过）。
- 所有 JSON 产物经规范化编码（对象键排序、无多余空白），指纹统一用 SHA-256。
- 归并顺序显式固定：分片按 `shard_id`、工单按 `ticket_id` 排序；任何 map 都先
  取键排序后再聚合，从不依赖 map 迭代序或 goroutine 调度。
- 训练/采样划分是 `ticket_id` 与采样比例的纯函数（FNV 取模），重放即复现。
- 持久化时间戳采用逻辑时钟（按代数派生），数据库内不写入墙钟时间。
- 同一输入与配置产生逐字节一致的词项摘要、类别分数与报告摘要（已由测试断言）。

## 增量重训

策略快照按阶段拆分为「该阶段实际消费的配置子集」计算配置指纹。词表权重、平滑、
哈希维度、采样比例、阈值分别只影响对应阶段。运行时：

1. 比对每阶段检查点的输入指纹、配置指纹与阶段运行标记；
2. 找到最早失配阶段，仅清除该阶段及其下游，未受影响上游保留并复用；
3. 被重建阶段的 `stage_seq` 从 1 重新连续推进，上游代数保持不变；
4. 增量结果与从零全量运行逐字节一致（`demo` 第 4 步与引擎测试均有断言）。

## 检查点、恢复与并发发布

- 每条阶段记录在单个事务内提交，强制 kill 只会留下「完整提交」或「完全缺失」，
  重启仅凭已提交事实重建进度，不生成退出报告。
- `(stage, ticket_id)` 上有唯一约束，同一阶段同一工单只允许一条有效记录。
- 发布采用 SQLite 条件更新 + `flock`/`LockFileEx` 真实文件锁双重互斥，并比较
  阶段内序号：序号必须 `max+1` 连续推进；重复发布幂等；序号回退（rollback）、
  同序号指纹变化（replacement）、跨阶段错位均返回 `E_STALE_CHECKPOINT`。
- 恢复后若发现阶段检查点不完整（进程在阶段中途被杀），该阶段整体重建，保证
  词表与聚合同连续运行一致，序号无跳跃、无重复。

## 异常注入入口

```bash
./ticketctl inject zero-smoothing     # -> E_CONFIG @ intent
./ticketctl inject zero-hashdim       # -> E_CONFIG @ normalize
./ticketctl inject shard-mismatch     # -> E_SHARD_MISMATCH @ normalize
./ticketctl inject empty-text         # 数据层开关，运行期产出 E_EMPTY_TEXT
./ticketctl inject label-range        # 运行期产出 E_LABEL_RANGE
./ticketctl inject agreement-missing  # 显式降级 D_AGREEMENT_MISSING
```

所有业务错误都携带稳定错误码、阶段名、工单号与诊断上下文，从不用通用错误替代。

## 本地 HTTP 接口（仅回环）

```bash
./ticketctl serve 0          # 0 表示自动选择空闲回环端口；默认 18080
```

路由：`GET /healthz`、`POST /import`、`POST /run`、`GET /status`、
`GET /report`、`POST /change/policy`、`POST /change/shard`、`GET /stop`、
`POST /recover`。请求体与返回均为 JSON。

## 目录结构

```
cmd/ticketctl     命令行与内置验收演示
internal/model    数据模型与业务错误码
internal/determ   固定种子 PRNG、规范 JSON、指纹、跨平台文件锁
internal/gen      确定性样本与策略快照生成器
internal/normalize 阶段一：规范化与词项抽取
internal/bayes    阶段二：对数域朴素贝叶斯
internal/priority 阶段三：优先级阈值与降级裁决
internal/validate 阶段四：验证报告
internal/store    SQLite 事务化检查点与发布协议
internal/engine   四阶段编排、增量、恢复、变更
internal/httpx    回环 HTTP 服务
```

更多设计取舍与实际验证结果见 `docs/`。
