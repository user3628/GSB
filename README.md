# segtag：本地确定性中文分词与词性联合标注组件

面向其他 NLP 服务集成的纯本地库。输入若干 UTF-8 文本文件（每行一句，允许中英
混排、数字与全角标点），输出每个词的起止字符偏移、词面、词性标签以及该句的
联合标注对数概率。全程不依赖网络、账号或外部服务。

## 核心组件

| 模块 | 职责 |
| --- | --- |
| `segtag/decoder.py` | 字级 B/M/E/S + 词性联合标签的对数域 Viterbi 解码器，返回最优路径与联合对数概率 |
| `segtag/model.py` | 从本地标注样本统计初始/转移/发射计数，拉普拉斯平滑后以对数概率存储，生成版本指纹 |
| `segtag/batch.py` | 多文件多句批量解码与确定性汇总（同序累加 + 独立重解码交叉校验，容差 1e-9） |
| `segtag/validate.py` | 解码后按 B/M/E/S 重组词并校验连续性、单字词位置合法性、词性存在性 |
| `segtag/api.py` | 对外集成门面 `Segmenter`：训练、调平滑、单句/批量标注、缓存查询、测试注入入口 |
| `segtag/errors.py` | 稳定业务错误码与错误对象 |

## 环境与运行

要求 Python 3.11 与 NumPy（见 `requirements.txt`）。一键演示与测试：

```bash
bash run_demo.sh          # 自动建 .venv（python3.11 + numpy），跑 demo.py 与全部测试
```

分步执行：

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python demo.py                                   # 完整演示 + 生成 demo_summary.txt
.venv/bin/python -m unittest discover -s tests -v          # 21 个单元/集成测试
.venv/bin/python -m segtag --train data/train.txt \
    --inputs data/input1.txt data/input2.txt data/input3.txt --smoothing 0.1
```

## 库接口示例

```python
from segtag import Segmenter

seg = Segmenter.train("data/train.txt", smoothing=0.1)
r = seg.tag_sentence("我爱北京。")
# r.ok=True；r.words=(Word(0,1,'我','r'), Word(1,2,'爱','v'), Word(2,4,'北京','ns'), Word(4,5,'。','w'))
# r.logprob 为联合标注对数概率

report = seg.tag_files(["data/input1.txt", "data/input2.txt", "data/input3.txt"])
# report.total_logprob / report.total_words / report.consistent（交叉校验差 ≤ 1e-9）

seg.set_smoothing(0.5)     # 概率表重建，指纹变化，缓存失效（seg.fingerprint / seg.cache_info() 可观测）
```

构造类方法（`Segmenter.train`）在参数非法时抛出 `SegTagError`；逐句/批量接口不抛
业务异常，返回携带稳定错误码的结果对象，出错时 `words`/`logprob` 一律为 `None`，
不会部分写入输出。

## 数据格式

- 训练样本 `data/train.txt`：每行 `词/词性 词/词性 ...`，共 41 句，覆盖中英混排
  （Python/iPhone/AI/Google/GitHub）、数字（3/99/2024/5600）与全角标点（。，！？：；“”）。
- 输入 `data/input1.txt` ~ `input3.txt`：每行一句，共 10 句；`input3.txt` 含未登录字
  `囧`、`槑`（走未知标记 `<UNK>` 回退，被标为形容词性单字词）。

## 关键设计

- **对数域计算**：初始/转移/发射概率全部存对数，Viterbi 只做加法，禁止连乘；
  `safe_log` 统一拦截非正/非有限概率输入。
- **拉普拉斯平滑**：`P=(count+α)/(row_total+α·V)`，α 必须为正有限实数，否则 `E_SMOOTHING`。
- **未知标记回退**：未见字符回退到可配置未知标记（默认 `<UNK>`）的发射项；未见转移
  回退到前驱平滑回退值；未知前驱回退到均匀分布。
- **版本指纹**：概率表的规范 JSON（排序键）经 SHA-256 生成指纹；重复估计逐字节一致；
  平滑系数变化 ⇒ 指纹变化 ⇒ 解码缓存整体失效，`cache_info()` 中 `invalidations` 可观测。
- **数值哨兵**：每个字符的发射向量、每步 Viterbi 累积概率均做有限性检查，出现
  -inf/NaN 立即报 `E_NUMERIC` 并携带句号与字符位置，不静默返回空结果或伪造路径。
- **批量一致性**：总对数概率按文件与行序同序累加；随后逐句独立重解码同序累加做
  交叉校验，差值须 ≤ 1e-9（实测为 0.0）。

## 业务错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_NUMERIC` | 概率表或解码过程出现 -inf/NaN，或对数输入为非正概率（报句号与字符位置） |
| `E_INVALID_TAG` | B/M/E/S 结构冲突、词内词性不一致、词性不在词性表、标签数与字符数不符 |
| `E_EMPTY` | 空句输入 |
| `E_SMOOTHING` | 平滑系数非正、非有限或非数值 |
| `E_FORMAT` | 训练样本行格式非法 |
| `E_CONFIG` | 配置非法（如未知标记为空） |

## 实际验证结果（本地确定性样本，Python 3.11.18 + NumPy 2.4.6）

- 批量标注 3 个文件共 10 句全部成功，总词数 62，总对数概率 `-407.22750322526457`；
  逐句独立解码交叉校验差值 `0.0`（容差 1e-9），`一致=True`。
- 模型指纹（α=0.1）`cf00dda3…da163f0`；α 调整为 0.5 后变为 `7feff72c…25c3926`，
  缓存失效计数 +1，重建后同句 logP 由 `-45.391362949294695` 变为 `-63.30098030662772`，
  旧版本结果未被误用。
- 错误注入实测：非法平滑系数 `-0.5` → `E_SMOOTHING`；发射注入 `-inf` → `E_NUMERIC`
  （句号 7、字位 1）；发射注入 `NaN` → `E_NUMERIC`（句号 8、字位 2）；词内词性不一致
  与未知词性 → `E_INVALID_TAG`；空句 → `E_EMPTY`。
- `python -m unittest discover -s tests`：21 个测试全部通过。
- `demo.py` 连续两次运行生成的 `demo_summary.txt` 逐字节一致
  （SHA-256 `7e3fc544…7edae82`）。

## 目录结构

```
segtag/            # 库：errors / model / decoder / validate / batch / api / cli
data/              # train.txt 训练样本 + input1~3.txt 演示输入
tests/             # 21 个单元与集成测试
demo.py            # 完整演示：批量标注 + 平滑调优 + 错误注入
run_demo.sh        # 一键建环境、跑演示与测试
demo_summary.txt   # 演示生成的固定摘要（重复运行逐字节一致）
```

