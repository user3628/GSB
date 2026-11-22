# 实际验证结果

验证环境：Python 3.14.4、NumPy 2.2.1、Linux 本地工作区（代码仅使用 3.10+ 语法与标准库 API，Windows 下提供 `msvcrt` 锁回退）。

## 依赖校验

```text
$ python -V
Python 3.14.4
$ python -c "import numpy"
（无异常，numpy 2.2.1 可导入）
```

## 单元与集成测试

命令：`python -m unittest discover -s tests -v`

结果：23 个测试全部通过，覆盖：

- 像素面积 IoU、同类别一对一匹配、真值/候选不可重复分配、背景诊断保留
- 相同输入与配置下检测产物跨运行、跨进程逐字节一致
- NFKC/全半角/空白折叠与原始字符偏移、中英文多候选定位、`E_REF_PARSE`
- 期限/百分比/生效月定点换算、换算因子与精度、中文百分比、开闭区间与 `E_UNIT_UNRESOLVED`
- 同合同同条款定位冲突分组、唯一胜出、降级理由与输入顺序无关
- 证据五环导出/导入/回导、缺失环 `E_EVIDENCE_BROKEN`
- 原子检查点字段、连续序号、优雅停止后恢复与全量产物逐字节一致
- 真实 OS 文件锁互斥、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`
- 坐标/类别、IoU 阈值、单位字典、语言权重、比较精度、仲裁顺序变化的失效传播
- 增量与全量逐字节 parity 及 `E_INCREMENTAL_MISMATCH` 的首个阶段/ref 定位
- 本地 HTTP 接口

## 端到端演示

命令：`python demo.py`

样本：固定种子 20241123，3 页、11 条引证、28 个候选框。

关键输出：

```text
region detection : candidate_count=28 match_count=11 background_count=17
E_REF_PARSE      : ref-0004
E_UNIT_UNRESOLVED: ref-0005 duration/payment unit failures; ref-0009 inverted interval
conflict groups  : 2 conflict groups, 3 demoted citations
winner group 1   : grp-C100-a3-c2 -> ref-0001 (duration/effective_month/penalty)
winner group 2   : grp-C200-a9-c0 -> ref-0009 (effective_month/penalty)
evidence records : 11, reconstruction verdicts=11
coordinate change: earliest invalidated stage=detect, byte parity=true
language change  : earliest invalidated stage=resolve, byte parity=true
byte determinism : detect/parse/normalize/resolve all byte-identical
checkpoint seq   : [1, 2, 3, 4]
DEMO OK
```

## 跨进程确定性

两个独立 OS 进程分别从零运行同一固定种子样本，四阶段产物拼接后 `cmp` 一致：

```text
CROSS_PROCESS_BYTE_IDENTICAL
sha256=863a36695dffee324c9aca5607a312825a83a30df96eadcfae0aa1b2cf108444
```

## CLI / HTTP 实测

- CLI：`init/full/status/demotions/reconstruct/stop/resume/export/import-evidence` 均返回预期结果；
  停止后完成当前阶段，恢复后检查点序号仍为 `[1,2,3,4]`。
- HTTP：`POST /sample/import`、`POST /run/full`、`GET /stats/conflicts`、`GET /fingerprints`、
  `POST /stop`、`POST /resume`、`POST /evidence/export`、`GET /reconstruct` 均验证通过。
