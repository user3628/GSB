"""Fixed-seed demo corpus and configuration generation."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from .utils import atomic_write_bytes, canonical_bytes


SAMPLE_CONFIG = """\
seed: 20261122
runtime:
  min_body_chars: 40
  min_sentence_chars: 4
budget:
  global_sentence_evaluations: 260
  per_worker_sentence_evaluations: 95
  max_workers: 3
  latency_budget_ms: 12000
  estimated_overhead_ms: 60
  estimated_ms_per_evaluation: 0.7
  poll_interval_ms: 10
targets:
  short:
    min_chars: 70
    max_chars: 150
    candidate_count: 3
  medium:
    min_chars: 130
    max_chars: 270
    candidate_count: 3
  long:
    min_chars: 210
    max_chars: 430
    candidate_count: 3
extractor:
  max_sentences: 10
  min_sentences: 2
  mmr_lambda: 0.72
  position_decay: 0.72
  weights:
    term_frequency: 2.0
    position: 1.0
    title_overlap: 1.25
    length_fit: 0.55
compression:
  min_coverage: 0.62
  max_length_ratio: 1.02
  max_removed_ratio: 0.45
  removable_clause_patterns:
    - '，[^，。；!?！？]{0,48}?(?:表示|认为|指出|强调|据悉|据了解|分析人士称)[^，。；!?！？]{0,48}'
    - ',?\\s+(?:officials? said|analysts? said|according to reports?)[^,.;!?]{0,80}'
  synonym_replacements:
    '迅速': '快速'
    '表示': '称'
    '目前': '当前'
    '高度重视': '重点关注'
    '召开': '举行'
    'approximately': 'about'
    'announced': 'said'
    'rapidly': 'quickly'
    'in order to': 'to'
  required_replacement_terms:
    - '记者'
    - 'officials said'
template:
  min_coverage: 0.58
  max_points: 4
  templates:
    zh: '要点：{who}；{what}；{when_where}。'
    en: 'Key points: {who}; {what}; {when_where}.'
    mixed: '要点 / Key points: {who}; {what}; {when_where}.'
fusion:
  weights:
    coverage: 1.55
    length_fit: 0.75
    language: 0.5
    source_prior: 0.35
  route_priority:
    - extractive
    - compressed
    - template
  source_priors:
    wire: 1.0
    desk: 0.94
    social: 0.82
  epsilon: 1.0e-9
  fusion_candidate_evaluations: 3
checkpoint:
  publish_epoch: local
"""


def _doc(doc_id: str, title: str, body: str, language: str, source: str, target: str) -> dict[str, Any]:
    return {
        "doc_id": doc_id,
        "title": title,
        "body": body.strip(),
        "language": language,
        "source": source,
        "target_length": target,
    }


def fixed_documents(seed: int = 20261122) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    zh_long = _doc(
        "news-zh-001",
        "城市轨道交通新线开通：首周客流创新高",
        "城市轨道交通新线在清晨正式开通。"
        "首周累计客流达到一百二十万人次，较此前预测高出约百分之十八。"
        "交通部门表示，新线连接三个大型居住区与两个产业园。"
        "运营方在高峰时段将发车间隔缩短至三分钟，并增加八列备用车。"
        "多位乘客认为，换乘时间明显缩短，通勤体验得到改善。"
        "记者在现场记录了首班列车进站与乘客换乘情况。"
        "沿线商业机构反映，周末客流带动餐饮与零售销售增长。"
        "专家指出，客流快速上升也带来站台安全和应急疏散压力。"
        "市政府表示，将根据实时客流数据调整公交接驳与停车资源。"
        "新线二期工程已经完成土建招标，预计两年后投入试运行。"
        "相关部门承诺，后续评估将公开客流、准点率与投诉处理数据。",
        "zh",
        "wire",
        "long",
    )
    en_medium = _doc(
        "news-en-001",
        "Coastal city opens solar-powered transit hub",
        "The coastal city opened a solar-powered transit hub near the harbor on Tuesday. "
        "The facility combines bus bays, bicycle storage and a public charging plaza in one block. "
        "City officials said the hub is expected to serve approximately forty thousand passengers each day. "
        "Construction began two years ago and finished three months earlier than planned. "
        "Engineers installed rooftop panels that cover most of the building's daytime electricity demand. "
        "Local businesses announced a shared delivery plan in order to reduce curbside congestion. "
        "Residents said the new transfer point rapidly cut their daily commute by twenty minutes. "
        "Transport analysts said the project will be watched closely for cost controls and reliability. "
        "The city will publish monthly energy, ridership and maintenance reports.",
        "en",
        "desk",
        "medium",
    )
    mixed_short = _doc(
        "news-mixed-001",
        "跨境数据合作启动 / Cross-border data pact begins",
        "两地监管部门本周启动跨境数据合作试点。"
        "The pilot creates a shared review desk for privacy and security assessments. "
        "首批参与企业包括物流、支付与医疗数据服务商。"
        "Officials said the review desk will publish a common template next month. "
        "企业需在本地留存审计记录，并接受年度抽查。"
        "The program will be evaluated after six months, according to the joint statement.",
        "mixed",
        "social",
        "short",
    )
    zh_medium = _doc(
        "news-zh-002",
        "农业数字化试点扩大：合作社接入统一监测平台",
        "农业数字化试点本周扩大到十二个县，超过三百家合作社接入统一监测平台。"
        "平台汇总土壤湿度、气象预警与农机调度信息。"
        "技术人员表示，农户可以通过手机查看地块风险并预约设备。"
        "首批试点数据显示，灌溉用水平均下降百分之九。"
        "合作社负责人认为，统一平台减少了重复填报和人工巡检。"
        "研究机构提醒，偏远地区仍需保留离线登记与人工应急流程。"
        "主管部门计划在秋收后公开节水量、故障率与服务响应时间。"
        "下一阶段将重点验证数据质量和小农户的使用成本。",
        "zh",
        "desk",
        "medium",
    )
    filler = [
        "背景资料显示，该议题在过去五年经历多轮评估与公开征求意见。",
        "相关部门整理了历史客流、设备维护和应急演练记录。",
        "研究团队使用统一口径比较不同地区、不同季节的运行数据。",
        "报告列出风险清单、整改时限与责任单位的对应关系。",
        "公开材料还包括预算执行、采购流程和第三方审计结论。",
        "多位专家建议把长期运行成本纳入项目立项评审。",
        "监测系统将在每日凌晨生成前一日运行摘要和异常提示。",
        "所有原始记录按照固定编号归档，便于后续复核。",
        "若出现连续异常，平台将转交人工团队进行现场核查。",
        "阶段性结论需经过技术审查、合规审查与公众意见汇总。",
    ]
    oversize_sentences = [
        "本分片汇集超长背景材料，用于验证预算调度和未处理原因记录。",
        "材料覆盖规划、建设、运营、维护、审计与公众沟通等主题。",
    ]
    for index in range(45):
        oversize_sentences.append(f"{rng.choice(filler)}第{index + 1:02d}项记录已完成编号。")
    oversize = _doc(
        "news-zh-oversize",
        "超长背景资料分片：预算压力测试",
        "".join(oversize_sentences),
        "zh",
        "wire",
        "long",
    )
    documents = [zh_long, en_medium, mixed_short, zh_medium, oversize]
    return documents


def write_samples(directory: Path, config_path: Path | None = None, *, seed: int = 20261122) -> dict[str, Any]:
    sample_dir = Path(directory)
    sample_dir.mkdir(parents=True, exist_ok=True)
    docs_path = sample_dir / "documents.jsonl"
    payload = b"".join(canonical_bytes(doc) + b"\n" for doc in fixed_documents(seed))
    atomic_write_bytes(docs_path, payload)
    if config_path is None:
        config_path = sample_dir / "config.yaml"
    config_path = Path(config_path)
    atomic_write_bytes(config_path, SAMPLE_CONFIG.encode("utf-8"))
    return {
        "documents": str(docs_path),
        "config": str(config_path),
        "document_count": len(payload.splitlines()),
        "seed": seed,
    }


def load_documents(path: str | Path) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                from .errors import EngineError

                raise EngineError("E_INVALID_INPUT", f"Invalid JSON on line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                from .errors import EngineError

                raise EngineError("E_INVALID_INPUT", f"Line {line_number} must contain a JSON object")
            documents.append(value)
    return documents
