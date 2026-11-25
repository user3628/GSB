# 验证报告

生成时间（主机本地）：2026-11-25 11:47:15 CST

## 环境
```
go version go1.27.4 linux/amd64
CGO_ENABLED=0 build: OK (静态链接)
```

## 单元/集成测试（go test ./...）
```
?   	ticketpipeline/cmd/ticketctl	[no test files]
ok  	ticketpipeline/internal/bayes	(cached)
?   	ticketpipeline/internal/determ	[no test files]
ok  	ticketpipeline/internal/engine	(cached)
?   	ticketpipeline/internal/gen	[no test files]
ok  	ticketpipeline/internal/httpx	(cached)
?   	ticketpipeline/internal/model	[no test files]
ok  	ticketpipeline/internal/normalize	(cached)
ok  	ticketpipeline/internal/priority	(cached)
ok  	ticketpipeline/internal/store	(cached)
?   	ticketpipeline/internal/validate	[no test files]
```

## 竞态检测（仅测试期启用 cgo 的 race 工具，生产仍 CGO_ENABLED=0）
```
ok  	ticketpipeline/internal/store	1.214s
ok  	ticketpipeline/internal/engine	16.315s
```

## 命令行端到端验收（ticketctl demo）

内置演示覆盖：3 个分片、中英混排、同渠道/跨渠道重复文本、空文本、纯标点、优先级
越界、一致度缺失、一次子进程强制终止（exit 97）后恢复、单配置增量、三类注入与回环
HTTP 冒烟。实际输出：

```
[1] generate deterministic dataset (3 shards)
  dataset digest=eba9ec157ff3
[2] fresh full run through four stages
  report digest=ef361335c38b44021cb060969374860ae3474ea5f594040969374860ae3474ea5... accuracy=0.8793
  E_EMPTY_TEXT x2
  E_LABEL_RANGE x1
[3] force crash mid normalize then recover from committed facts
  forced termination observed (exit 97) at normalize:TKT-0011
  recovery replay matches fresh run; sequences contiguous
[4] single config change: only priority stage and downstream rebuild
  normalize gen 1->1 reused
  intent    gen 1->1 reused
  priority  gen 1->2 rebuilt
  validate  gen 1->2 rebuilt
  incremental == fresh digest=e91354623cb9...
[5] anomaly injections return stable business codes
  zero-smoothing -> E_CONFIG @ intent
  zero-hashdim   -> E_CONFIG @ normalize
  shard-mismatch -> E_SHARD_MISMATCH @ normalize
[6] loopback HTTP smoke test
  HTTP healthz OK on http://127.0.0.1:<ephemeral>/healthz
[7] fixed summary
  FINAL report digest=ef361335c38b...（连续多次运行保持不变）
```

## 异常错误码对照（实测）

| 注入/情形 | 阶段 | 业务码 | 携带工单号 |
| --- | --- | --- | --- |
| 空文本 / 仅标点 | normalize | `E_EMPTY_TEXT` | 是 |
| 标注优先级越界(9) | priority | `E_LABEL_RANGE` | 是 |
| 平滑强度=0 | intent | `E_CONFIG` | 阶段级 |
| 哈希维度=0 | normalize | `E_CONFIG` | 阶段级 |
| 分片声明行数≠实际 | normalize（装载） | `E_SHARD_MISMATCH` | 分片级上下文 |
| 对数域 NaN/下溢 | intent | `E_NUMERIC` | 是，含类别与词项 |
| 序号回退/整体替换/错位 | 任意阶段发布 | `E_STALE_CHECKPOINT` | 是 |
| 一致度缺失 | priority | 不报错，降级 `D_AGREEMENT_MISSING` 并封顶 1 | 是 |

## 确定性与增量一致性（实测）

- 两个独立工作区分别生成数据：`diff -r` 全部文件逐字节相同。
- 两个独立工作区分别全量运行：`normalize/intent/priority/validate` 产物目录
  `diff -r` 全部相同，报告 `digest` 相同。
- 阈值变更走增量（仅 priority、validate 进入第 2 代，normalize、intent 保留第 1 代），
  与「在干净工作区重放相同版本历史」的全量结果报告 `digest` 相同。
- `ticketctl verify` 依据 SQLite 事实校验四阶段 `stage_seq` 连续、每工单每阶段唯一，
  返回通过。

## 测试清单（共 28 个测试函数，全部通过）

- `internal/normalize`：NFKC/全半角、中文 uni/bi 与拉丁切分偏移、渠道敏感哈希、
  空/纯标点 `E_EMPTY_TEXT`、零哈希维度 `E_CONFIG`、逐字节摘要稳定性。
- `internal/bayes`：训练/打分确定性、未知标记回退、`E_NUMERIC` 数值守卫与诊断上下文、
  零平滑 `E_CONFIG`、采样划分稳定性。
- `internal/priority`：命中/漏判/误判三类、越界 `E_LABEL_RANGE` 且保留样本、
  一致度缺失/偏低两级降级、阈值单调性校验。
- `internal/store`：序号连续与幂等重复、replacement/rollback 识别、删除重建序号重置、
  每工单单有效记录。
- `internal/engine`：端到端确定性、增量与全量字节一致且代数正确、零配置业务码、
  重开幂等、强制终止恢复后摘要一致且序号连续。
- `internal/httpx`：回环 HTTP 的 healthz/run/status/report 全链路。

## 复现实验命令

```bash
go mod tidy
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build -o ticketctl ./cmd/ticketctl
./ticketctl demo
./ticketctl generate && ./ticketctl run && ./ticketctl verify && ./ticketctl status
```
