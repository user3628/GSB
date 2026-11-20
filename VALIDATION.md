# 实际验证记录

日期：2026-11-20  
工作区：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-48`  
环境：Python 3.14.4、NumPy 2.3.5、pytest 9.0.2、Linux

## 1. 自动化测试

```text
$ pytest -o addopts='' -q
..............................                                           [100%]
30 passed in 12.59s
```

覆盖内容：

- 固定种子样本生成与声明 SHA-256。
- 小/中两档预处理、目标编码、缺失列、空特征 `E_EMPTY_FEATURE`。
- 分层折确定性、单类别折 `E_INVALID_CV`。
- L1/L2/弹性网、零正则化、坐标下降、近端梯度、数值异常。
- 四阶段检查点、连续序号、损坏记录隔离、跨进程并发发布。
- 配置/数据变化的最早失效阶段、上游缓存复用、结果变化。
- 折间停止、损坏尾部隔离、恢复后与全量运行逐字节一致。
- HTTP 应用、导入、训练、停止恢复、摘要、导出、错误码。
- 固定演示矩阵和浏览器 JavaScript 语法。

## 2. 固定演示

```bash
$ python3 scripts/run_demo.py --workspace runtime/demo --write-expected
$ python3 scripts/verify_demo.py
```

关键输出：

```text
ok = true
demo report digest = 24b0394d0a4d1dd815aad258d90d6d33d66eb1a3d5e5a4a960f01a8c9745e4ee

small result_digest  = e430a3c2533f8f6df1ee73258af5d0a9130f573125955f8941898ce11ba85e57
medium result_digest = 7ca7cee23831f8145f06ee1611f7866d0ab74bdf91a329453d2d7ed38cd2212b

determinism:
  first_digest  = 6e3f38f9...
  second_digest = 6e3f38f9...
  equal         = true

stop/resume:
  stopped status         = stopped
  checkpoints before     = 9
  checkpoints after      = 28
  sequences contiguous   = true
  prefix preserved       = true
  result equals full run = true

invalidation:
  changed field     = regularization.strengths
  earliest stage    = solve
  reusable stages   = preprocess, folds
  reused preprocess = true
  reused folds      = true
  old result digest = 6e3f38f9...
  new result digest = 481dfbab...

errors:
  invalid L1 ratio   = E_INVALID_HYPERPARAM
  single-fold CV     = E_INVALID_CV
  numeric divergence = E_NUMERIC
  empty feature      = E_EMPTY_FEATURE
  invalid stage jump = E_INVALID_STAGE
  concurrent publish = [E_STALE_CHECKPOINT, OK]
```

数值发散错误详情实际包含：

```json
{
  "code": "E_NUMERIC",
  "details": {
    "fold": 1,
    "iteration": 0,
    "strength": 0.01,
    "l1_ratio": 0.0,
    "objective": 0.6931471805599453,
    "reason": "objective ... exceeds configured limit 0.01"
  }
}
```

## 3. 文件哈希

```text
620f1f1d3757e28a3cfe114f30cfdb0f72aa2f20a92f8649a935d812c4964b02  data/small.csv
cdb30d262ec719ced4039b37a440f2d69354a1e04daf239a8a3844d12c074fa1  data/medium.csv
2512b6f9a76144142da98c2339b198a1f43f16dd972178e2075486798299c623  data/dataset_declaration.json

7d89b92e3efc6235ff53ff69f3a391a6e09e6c496a273cd5892a33f1e4f294b4  expected/small.summary.json
6c525c3457600f41f24f4d4e831f1672caa34fbf3df60a6078a06f650eed035e  expected/medium.summary.json
de1de34145777c2f4d2098b1aacf63a8837e3a1f06a229182547da74b986487a  expected/demo_report.json
```

复现：

```bash
python3 scripts/generate_samples.py
pytest -o addopts='' -q
python3 scripts/verify_demo.py
```
