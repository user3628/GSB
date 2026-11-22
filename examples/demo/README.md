# 完整本地示例

此目录由 CLI 生成，可直接检查或复制为集成起点。

```text
bundle/
  manifest.json               融合清单
  shards/*.npy                固定种子 NCHW 输入分片
  reference/*.npy             独立标量循环生成的高精度参考
  reference/metadata.json     样本和参考摘要
workspace/
  input/                      导入后的清单、分片和参考
  state/tasks/demo_fpn/       检查点、内容寻址产物和报告
  outputs/demo_fpn/           逐层输出、最终输出、逐元素量化数组和报告
demo_run.json                 完整 CLI 返回
validation.json               验证指标摘要
```

从零重放：

```bash
PYTHONPATH=../../src python3 -m multiscale_fusion demo \
  --dir /tmp/msfp-demo --force --verify-full
```

检查当前示例工作区：

```bash
PYTHONPATH=../../src python3 -m multiscale_fusion status \
  --workspace workspace --task demo_fpn

PYTHONPATH=../../src python3 -m multiscale_fusion verify-checkpoints \
  --workspace workspace --task demo_fpn

PYTHONPATH=../../src python3 -m multiscale_fusion verify-incremental \
  --workspace workspace --task demo_fpn
```

重新运行 `run` 会复用全部已提交阶段，不增加检查点序号；修改 `workspace/input/manifest.json` 中某个层的注意力或融合系数后再次运行，会只重建该层及下游。
