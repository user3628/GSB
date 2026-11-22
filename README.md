# 本地确定性对话式检索与生成式答案工作台（ragwb）

一个完全离线、无外部 API / GPU / 检索服务依赖的对话式检索—证据对齐—答案生成工作台。
检索打分、证据对齐、冲突消解与答案组装全部由本地确定性算法实现；Python 标准库
`http.server` 提供本地接口，NumPy 负责数值合成与非有限值校验。

## 设计目标

- 四个阶段产物都带内容指纹，任一上游变化可识别最早失效阶段并向下游传播。
- 每条答案都带可追溯证据链（原文偏移、版本、来源通道、裁决理由），证据链可重建同一答案。
- 运行目录与数据目录分离；检查点原子写盘 + 跨进程文件锁，崩溃/停止后恢复与一次完整运行逐字节一致。
- 全局证据评估预算为硬上限，规模分档在三种检索策略间确定性取舍。
- 固定随机种子生成演示数据，重复生成与重复运行均得到相同 SHA-256 摘要。

## 目录结构

```
ragwb/
  errors.py        稳定业务错误码与结构化异常
  io_utils.py      原子写盘、跨进程文件锁、规范化 JSON、SHA-256 指纹
  yaml_lite.py     标准库实现的 YAML 子集解析/导出（确定性、LF 换行）
  text.py          多语言分词、Jaccard、数值/日期事实抽取
  generator.py     固定种子的会话/知识库/配置生成器
  query.py         阶段1 查询改写与指代消解
  planner.py       规模分档、三策略选择、全局预算账本
  retrieval.py     阶段2 对数域检索打分与候选排序
  alignment.py     阶段3 实体对齐、冲突检测、可信度裁决
  answer.py        阶段4 答案组装、证据链导出与重建
  checkpoint.py    检查点原子发布、序号校验、文件锁
  incremental.py   版本变化检测、失效传播、逐字节比对
  pipeline.py      四阶段编排、优雅停止、恢复、固定摘要
  server.py        http.server 本地接口
  cli.py           命令行入口
tests/             32 个单元/集成测试（含错误码注入、并发、恢复、增量）
run_demo.sh        一键确定性演示
```

## 四个阶段

1. **查询改写与指代消解**（`query.py`）：维护对话焦点实体与上一轮引用 doc_id，
   替换“它/该条/这条/其/it”等指代并输出字符偏移；无焦点或无指代时回退原问题并记录
   `empty_dialogue_focus` / `no_anaphor_detected`。
2. **检索与打分**（`retrieval.py`）：对数域合成
   `w_overlap·log(Jaccard+ε) + w_fresh·log(版本新鲜度) + w_channel·log(exp(通道先验))`，
   按分数降序、同分 doc_id 升序输出；空查询/零候选抛 `E_RETRIEVAL_EMPTY`，
   NaN/Inf 抛 `E_NUMERIC`。
3. **对齐与冲突消解**（`alignment.py`）：按对齐容差 + 双语实体头聚类事实簇；
   簇内数值或日期不同即判冲突；可信度由版本、通道先验、语言一致性在对数域合成，
   并列时以 (可信度, 版本, 通道先验, doc_id) 确定性仲裁，全簇冲突也保留全部证据。
4. **答案组装与证据链**（`answer.py`）：模板渲染带连续 `[n]` 引用的答案，
   证据链记录原文偏移、版本、通道、裁决理由；槽位缺失或引用不连续抛
   `E_ANSWER_INVALID`，证据链可重建完全相同的答案与引用映射。

## 规模取舍与预算

- `exact_full`（小档）、`layered_truncate`（中档，截断深度 40）、
  `focus_first`（大档，截断深度 12）。
- 小档被显式禁止使用大档截断深度，决策记录入选理由与所有被淘汰策略原因。
- 全局预算在 `BudgetLedger` 中记账，恢复时从检查点重建账本；任何策略的实际评估数
  永不超过预算，耗尽后下一轮得到带会话/轮次/阶段的 `E_RETRIEVAL_EMPTY`。

## 检查点、恢复与增量

- 每条检查点含阶段名、会话、轮次、输入指纹、配置指纹、产物摘要与单调序号，
  经临时文件 + `os.replace` 原子发布，崩溃只可能留下旧文件或临时文件，不会半写。
- 发布在真实 OS 文件锁（POSIX `fcntl` / Windows `msvcrt`）下校验会话内轮次序号：
  落后/重复/跨会话错位 → `E_STALE_CHECKPOINT`，缺前置阶段 → `E_INVALID_STAGE`。
- 停止请求是文件标记，仅在不可拆分的会话轮次单元完成后响应；恢复后摘要、答案、
  证据链与从零完整运行逐字节一致，序号连续无跳跃重复。
- 会话问题、知识库段落内容/版本、检索深度、对齐容差、答案模板、规模阈值任一变化，
  都通过各阶段输入/产物指纹定位最早失效阶段并使下游全部失效；
  `verify-incremental` 与从零构建做逐字节摘要比对，不一致抛 `E_INCREMENTAL_MISMATCH`。

## 快速开始

```bash
python3 -m pip install -r requirements.txt   # 仅 numpy；其余均为标准库
./run_demo.sh                                # 生成数据 + 完整运行 + 打印固定摘要
python3 -m unittest discover -s tests        # 运行全部测试
```

CLI：

```bash
python3 -m ragwb.cli generate  --data-dir data/demo
python3 -m ragwb.cli run       --data-dir data/demo --run-dir runs/demo
python3 -m ragwb.cli stop      --data-dir data/demo --run-dir runs/demo
python3 -m ragwb.cli recover   --data-dir data/demo --run-dir runs/demo
python3 -m ragwb.cli status    --data-dir data/demo --run-dir runs/demo
python3 -m ragwb.cli fingerprints --data-dir data/demo --run-dir runs/demo --session-id sess-s1-aurora
python3 -m ragwb.cli evidence  --data-dir data/demo --run-dir runs/demo --session-id sess-s1-aurora --turn 1
python3 -m ragwb.cli export    --data-dir data/demo --run-dir runs/demo --session-id sess-s1-aurora
python3 -m ragwb.cli invalidate --data-dir data/demo --run-dir runs/demo
python3 -m ragwb.cli serve     --host 127.0.0.1 --port 8765 \
                               --data-dir data/demo --run-dir runs/demo
```

HTTP 路由：`POST /import`、`POST /run`、`POST /stop`、`POST /recover`、
`GET /status`、`GET /sessions`、`GET /sessions/{id}/fingerprints`、
`GET /sessions/{id}/evidence?turn=n`、`GET /sessions/{id}/plan?turn=n`、
`GET /answers/{id}`、`GET /export/{id}`、`POST /invalidation/detect`、
`POST /incremental/verify`。

## 演示数据覆盖

5 个会话分片、3 个知识库分片（24 段落，固定种子 20261123），覆盖：
中英混排（`sess-s3-bilingual`）、多轮指代消解（`sess-s1/s2`）、
同实体跨通道数值/日期冲突（极光电池、星河芯片、云岭手机、Nimbus 无人机）、
空焦点回退（`sess-s5-fallback`）、以及必然打满全局预算的会话（`sess-s4-budget`）。

## 平台说明

所有 JSON/YAML 以 UTF-8、LF 换行、排序键写出，Windows 与 POSIX 下产物逐字节一致；
文件锁与原子替换分别使用对应平台原语，未使用 Docker 或任何容器。
