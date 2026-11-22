from __future__ import annotations

import random
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

from .config import default_config
from .errors import E_DURATION_PARSE, E_EMPTY_PROMPT, E_SEMANTIC_CONFLICT
from .matching import (
    METRIC_DURATION,
    METRIC_SCENE,
    METRIC_SCORE,
    METRIC_SUBJECT,
)
from .text import normalize_text
from .util import (
    atomic_write_json,
    canonical_bytes,
    ensure_dir,
    sha256_bytes,
    sha256_obj,
    write_jsonl,
)
from .version import GENERATOR_VERSION, PIPELINE_VERSION, SCHEMA_VERSION


def _unit_offsets(
    fragments: Iterable[tuple[str, str]],
) -> tuple[str, str, list[dict[str, Any]]]:
    fragment_list = list(fragments)
    if not fragment_list:
        return "", "", []
    separators = ["，", " and ", "；", ", ", "，"]
    raw = ""
    for index, (_, text) in enumerate(fragment_list):
        if index:
            raw += separators[(index - 1) % len(separators)]
        raw += text
    raw += "。"
    normalized = normalize_text(raw)
    cursor = 0
    units: list[dict[str, Any]] = []
    for index, (unit_type, text) in enumerate(fragment_list, 1):
        expected = normalize_text(text)
        start = normalized.find(expected, cursor)
        if start < 0:
            raise ValueError(
                f"generator could not locate normalized fragment {expected!r} after {cursor}"
            )
        end = start + len(expected)
        units.append(
            {
                "unit_id": f"u{index:04d}",
                "text": expected,
                "start": start,
                "end": end,
                "type": unit_type,
            }
        )
        cursor = end
    return raw, normalized, units


def _targets(
    *,
    subject_min: float = 1.0,
    scene_min: float = 1.0,
    duration_min: float = 1.0,
    score_min: float = 0.85,
    scene_applicable: bool = True,
    duration_applicable: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "must_subject_coverage": {"min": subject_min, "max": 1.0},
        "score": {"min": score_min, "max": 1.0},
    }
    if scene_applicable:
        result["scene_coverage"] = {"min": scene_min, "max": 1.0}
    if duration_applicable:
        result["duration_consistency"] = {"min": duration_min, "max": 1.0}
    return result


def _plan(
    *,
    shot_id: str,
    fragments: list[tuple[str, str]],
    duration_phrase: str,
    target_ms: int | None,
    target_source: str | None,
    target_precision: int | None,
    required_subjects: list[str],
    forbidden_subjects: list[str] | None = None,
    scenes: list[str],
    negatives: list[str] | None = None,
    targets: dict[str, Any] | None = None,
    expected_codes: list[str] | None = None,
    expected_verdict: str,
) -> dict[str, Any]:
    raw_prompt, normalized_prompt, units = _unit_offsets(fragments)
    return {
        "shot_id": shot_id,
        "fragments": fragments,
        "raw_prompt": raw_prompt,
        "normalized_prompt": normalized_prompt,
        "units": units,
        "duration_phrase": duration_phrase,
        "target_ms": target_ms,
        "target_source": target_source,
        "target_precision": target_precision,
        "required_subjects": list(required_subjects),
        "forbidden_subjects": list(forbidden_subjects or []),
        "scenes": list(scenes),
        "negatives": list(negatives or []),
        "targets": targets
        if targets is not None
        else _targets(scene_applicable=bool(scenes), duration_applicable=target_ms is not None),
        "expected_codes": sorted(expected_codes or []),
        "expected_verdict": expected_verdict,
    }


def _expected_coverage(plan: dict[str, Any]) -> dict[str, Any] | None:
    if E_EMPTY_PROMPT in plan["expected_codes"] or E_DURATION_PARSE in plan["expected_codes"]:
        return None
    config = default_config()
    weights = config["coverage"]["weights"]
    folded = lambda value: normalize_text(str(value)).casefold()
    subject_texts = [
        normalize_text(text)
        for unit_type, text in plan["fragments"]
        if unit_type == "subject_description"
    ]
    scene_texts = [
        normalize_text(text)
        for unit_type, text in plan["fragments"]
        if unit_type == "scene_description"
    ]
    subject_conflicted = E_SEMANTIC_CONFLICT in plan["expected_codes"]
    covered_subjects = sum(
        1
        for subject in plan["required_subjects"]
        if not subject_conflicted
        and any(folded(subject) in folded(text) for text in subject_texts)
    )
    covered_scenes = sum(
        1
        for scene in plan["scenes"]
        if any(folded(scene) in folded(text) for text in scene_texts)
    )

    def component(numerator: int, denominator: int) -> dict[str, Any]:
        if denominator <= 0:
            return {
                "numerator": numerator,
                "denominator": denominator,
                "value": None,
                "applicable": False,
            }
        value = (Decimal(numerator) / Decimal(denominator)).quantize(
            Decimal("0.000001"), rounding=ROUND_HALF_UP
        )
        return {
            "numerator": numerator,
            "denominator": denominator,
            "value": float(value),
            "applicable": True,
        }

    coverage = {
        METRIC_SUBJECT: component(covered_subjects, len(plan["required_subjects"])),
        METRIC_SCENE: component(covered_scenes, len(plan["scenes"])),
        METRIC_DURATION: component(
            1 if plan["target_ms"] is not None else 0,
            1 if plan["target_ms"] is not None else 0,
        ),
    }
    total_weight = Decimal(0)
    weighted = Decimal(0)
    for metric in (METRIC_SUBJECT, METRIC_SCENE, METRIC_DURATION):
        item = coverage[metric]
        weight = Decimal(str(weights[metric]))
        if not item["applicable"] or weight <= 0:
            continue
        total_weight += weight
        weighted += weight * Decimal(str(item["value"]))
    score = (
        None
        if total_weight == 0
        else float(
            (weighted / total_weight).quantize(
                Decimal("0.000001"), rounding=ROUND_HALF_UP
            )
        )
    )
    return {
        "coverage": coverage,
        "score": score,
        "na_components": [
            metric for metric, item in coverage.items() if not item["applicable"]
        ],
    }


def _special_plans(prefix: str) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    plans.append(
        _plan(
            shot_id=f"{prefix}-001",
            fragments=[
                ("subject_description", "黄色雨衣女孩站在月台边"),
                ("scene_description", "雨夜中霓虹灯闪烁"),
                ("scene_description", "neon glow reflects on wet asphalt"),
                ("duration", "镜头持续约三秒半"),
            ],
            duration_phrase="约三秒半",
            target_ms=3500,
            target_source="cn_quantifier",
            target_precision=1,
            required_subjects=["雨衣女孩"],
            scenes=["雨夜", "霓虹灯", "wet asphalt"],
            expected_verdict="pass",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-002",
            fragments=[
                ("subject_description", "黑猫蹲在旧书店门口"),
                ("scene_description", "旧书店里尘屑飞舞"),
                ("constraint", "不要出现红色气球"),
                ("duration", "镜头时长为2.5s"),
            ],
            duration_phrase="2.5s",
            target_ms=2500,
            target_source="arabic_unit",
            target_precision=1,
            required_subjects=["黑猫"],
            scenes=["旧书店"],
            negatives=["不要"],
            expected_verdict="pass",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-003",
            fragments=[
                ("subject_description", "红色气球漂浮在屋顶"),
                ("scene_description", "屋顶风声很紧"),
                ("constraint", "不要出现红色气球"),
                ("duration", "镜头只有八帧"),
            ],
            duration_phrase="八帧",
            target_ms=320,
            target_source="frames",
            target_precision=0,
            required_subjects=["红色气球"],
            scenes=["屋顶"],
            negatives=["不要"],
            expected_codes=["E_SEMANTIC_CONFLICT"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-004",
            fragments=[
                ("subject_description", "老船长凝视着海面"),
                ("scene_description", "海面泛起白浪"),
                ("duration", "镜头持续约三光年"),
            ],
            duration_phrase="约三光年",
            target_ms=None,
            target_source=None,
            target_precision=None,
            required_subjects=["老船长"],
            scenes=["海面"],
            expected_codes=["E_DURATION_PARSE"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-005",
            fragments=[],
            duration_phrase="2s",
            target_ms=2000,
            target_source="arabic_unit",
            target_precision=0,
            required_subjects=["雨衣女孩"],
            scenes=["雨夜"],
            expected_codes=["E_EMPTY_PROMPT"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-006",
            fragments=[
                ("subject_description", "白色机器人抬起手臂"),
                ("scene_description", "旧书店里尘屑飞舞"),
                ("scene_description", "dust motes float in warm light"),
                ("duration", "镜头持续3秒"),
            ],
            duration_phrase="3秒",
            target_ms=3000,
            target_source="arabic_unit",
            target_precision=0,
            required_subjects=["白色机器人", "银色自行车"],
            scenes=["旧书店", "dust motes"],
            targets=_targets(subject_min=1.0, scene_min=1.0, score_min=0.85),
            expected_verdict="review",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-007",
            fragments=[
                ("scene_description", "雨夜中霓虹灯闪烁"),
                ("scene_description", "slow dolly-in across the platform"),
            ],
            duration_phrase="八帧",
            target_ms=320,
            target_source="frames",
            target_precision=0,
            required_subjects=["黑猫"],
            scenes=["雨夜", "霓虹灯"],
            expected_codes=["E_EMPTY_PROMPT"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-008",
            fragments=[
                ("subject_description", "纸飞机掠过玻璃窗"),
                ("scene_description", "玻璃窗上雨滴滑落"),
                ("duration", "镜头持续0s"),
            ],
            duration_phrase="0s",
            target_ms=None,
            target_source=None,
            target_precision=None,
            required_subjects=["纸飞机"],
            scenes=["玻璃窗"],
            expected_codes=["E_DURATION_PARSE"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-009",
            fragments=[
                ("subject_description", "快递员推着银色自行车"),
                ("scene_description", "地铁站灯光忽明忽暗"),
                ("duration", "镜头约2秒"),
            ],
            duration_phrase="约2秒，3s",
            target_ms=None,
            target_source=None,
            target_precision=None,
            required_subjects=["快递员"],
            scenes=["地铁站"],
            expected_codes=["E_DURATION_PARSE"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-010",
            fragments=[
                ("subject_description", "paper plane drifts past the glass window"),
                ("scene_description", "rain drops streak across the glass window"),
                ("constraint", "without the paper plane"),
                ("duration", "1.25s"),
            ],
            duration_phrase="1.25s",
            target_ms=1250,
            target_source="arabic_unit",
            target_precision=2,
            required_subjects=["paper plane"],
            scenes=["glass window"],
            negatives=["without"],
            expected_codes=["E_SEMANTIC_CONFLICT"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-011",
            fragments=[
                ("subject_description", "黄色雨衣女孩站在月台边"),
                ("subject_description", "黑猫蹲在旧书店门口"),
                ("scene_description", "旧书店里尘屑飞舞"),
                ("duration", "10 frames"),
            ],
            duration_phrase="10 frames",
            target_ms=400,
            target_source="frames",
            target_precision=0,
            required_subjects=["雨衣女孩"],
            forbidden_subjects=["黑猫"],
            scenes=["旧书店"],
            expected_codes=["E_FORBIDDEN_SUBJECT"],
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-012",
            fragments=[
                ("subject_description", "白色机器人抬起手臂"),
                ("scene_description", "旧书店里尘屑飞舞"),
                ("duration", "1500ms"),
            ],
            duration_phrase="1500ms",
            target_ms=1500,
            target_source="arabic_unit",
            target_precision=0,
            required_subjects=["白色机器人", "快递员"],
            scenes=["旧书店", "地铁站"],
            targets=_targets(subject_min=1.0, scene_min=1.0, score_min=0.85),
            expected_verdict="rejected",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-013",
            fragments=[
                ("subject_description", "老船长凝视着海面"),
                ("duration", "两秒"),
            ],
            duration_phrase="两秒",
            target_ms=2000,
            target_source="cn_quantifier",
            target_precision=0,
            required_subjects=["老船长"],
            scenes=[],
            targets=_targets(scene_applicable=False),
            expected_verdict="pass",
        )
    )
    plans.append(
        _plan(
            shot_id=f"{prefix}-014",
            fragments=[
                ("subject_description", "雨衣女孩站在月台边"),
                ("scene_description", "雨夜中霓虹灯闪烁"),
                ("scene_description", "wet asphalt reflects the neon glow"),
                ("duration", "镜头持续２．５秒"),
            ],
            duration_phrase="２．５ｓ",
            target_ms=2500,
            target_source="arabic_unit",
            target_precision=1,
            required_subjects=["雨衣女孩"],
            scenes=["雨夜", "霓虹灯", "wet asphalt", "mist"],
            targets=_targets(),
            expected_verdict="review",
        )
    )
    return plans


_SUBJECT_CATALOG: list[dict[str, Any]] = [
    {
        "subject": "雨衣女孩",
        "fragment": "黄色雨衣女孩站在月台边",
        "scenes": ["雨夜", "霓虹灯"],
        "scene_fragments": ["雨夜中霓虹灯闪烁", "wet asphalt reflects the neon glow"],
    },
    {
        "subject": "白色机器人",
        "fragment": "白色机器人抬起手臂",
        "scenes": ["旧书店", "dust motes"],
        "scene_fragments": ["旧书店里尘屑飞舞", "dust motes float in warm light"],
    },
    {
        "subject": "黑猫",
        "fragment": "黑猫蹲在旧书店门口",
        "scenes": ["旧书店", "shadow"],
        "scene_fragments": ["旧书店里尘屑飞舞", "a long shadow crosses the floor"],
    },
    {
        "subject": "快递员",
        "fragment": "快递员推着银色自行车",
        "scenes": ["地铁站", "flickering light"],
        "scene_fragments": ["地铁站灯光忽明忽暗", "flickering light on tiled walls"],
    },
    {
        "subject": "红色气球",
        "fragment": "红色气球漂浮在屋顶",
        "scenes": ["屋顶", "wind"],
        "scene_fragments": ["屋顶风声很紧", "wind pulls at the camera strap"],
    },
    {
        "subject": "纸飞机",
        "fragment": "纸飞机掠过玻璃窗",
        "scenes": ["玻璃窗", "rain drops"],
        "scene_fragments": ["玻璃窗上雨滴滑落", "rain drops streak across the pane"],
    },
    {
        "subject": "老船长",
        "fragment": "老船长凝视着海面",
        "scenes": ["海面", "white waves"],
        "scene_fragments": ["海面泛起白浪", "white waves break beyond the rail"],
    },
    {
        "subject": "银色自行车",
        "fragment": "银色自行车靠在地铁站墙边",
        "scenes": ["地铁站", "tiled walls"],
        "scene_fragments": ["地铁站灯光忽明忽暗", "tiled walls carry a faint echo"],
    },
]

_DURATION_CATALOG = [
    ("约三秒半", 3500, "cn_quantifier", 1),
    ("2.5s", 2500, "arabic_unit", 1),
    ("八帧", 320, "frames", 0),
    ("3秒", 3000, "arabic_unit", 0),
    ("1.25s", 1250, "arabic_unit", 2),
    ("10 frames", 400, "frames", 0),
    ("1500ms", 1500, "arabic_unit", 0),
    ("两秒", 2000, "cn_quantifier", 0),
]


def _normal_plans(prefix: str, count: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    plans: list[dict[str, Any]] = []
    for index in range(count):
        catalog = _SUBJECT_CATALOG[index % len(_SUBJECT_CATALOG)]
        duration_phrase, target_ms, source, precision = _DURATION_CATALOG[
            index % len(_DURATION_CATALOG)
        ]
        fragments: list[tuple[str, str]] = [
            ("subject_description", catalog["fragment"]),
            ("scene_description", catalog["scene_fragments"][0]),
            ("scene_description", catalog["scene_fragments"][1]),
        ]
        negatives: list[str] = []
        # Deterministic shuffle of scene ordering and constraint placement keeps
        # the sample varied without moving away from the fixed seed.
        if index % 3 == 0:
            constraint = (
                "不要出现紫色烟雾"
                if catalog["subject"] == "红色气球"
                else "不要出现红色气球"
            )
            fragments.insert(1, ("constraint", constraint))
            negatives = ["不要"]
        elif index % 5 == 0:
            fragments.append(("constraint", "avoid harsh lens flare"))
            negatives = ["avoid"]

        duration_fragment = f"镜头持续{duration_phrase}"
        fragments.append(("duration", duration_fragment))

        # Use a seeded coin flip to reorder only the two decorative scene units.
        if rng.randrange(2):
            scene_indexes = [
                position
                for position, (unit_type, _) in enumerate(fragments)
                if unit_type == "scene_description"
            ]
            if len(scene_indexes) >= 2:
                first, second = scene_indexes[:2]
                fragments[first], fragments[second] = (
                    fragments[second],
                    fragments[first],
                )

        plans.append(
            _plan(
                shot_id=f"{prefix}-{index + 15:03d}",
                fragments=fragments,
                duration_phrase=duration_phrase,
                target_ms=target_ms,
                target_source=source,
                target_precision=precision,
                required_subjects=[catalog["subject"]],
                scenes=list(catalog["scenes"]),
                negatives=negatives,
                expected_verdict="pass",
            )
        )

    # A low scene coverage case intentionally lands in review rather than reject.
    plans.append(
        _plan(
            shot_id=f"{prefix}-{count + 15:03d}",
            fragments=[
                ("subject_description", "黑猫蹲在旧书店门口"),
                ("scene_description", "旧书店里尘屑飞舞"),
                ("duration", "2.5s"),
            ],
            duration_phrase="2.5s",
            target_ms=2500,
            target_source="arabic_unit",
            target_precision=1,
            required_subjects=["黑猫"],
            scenes=["旧书店", "shadow", "dust motes"],
            expected_verdict="review",
        )
    )
    return plans


def build_plans(tier: str, seed: int) -> list[dict[str, Any]]:
    if tier == "small":
        return _special_plans("small")
    if tier == "large":
        plans = _special_plans("large")
        plans.extend(_normal_plans("large", 36, seed))
        return plans
    raise ValueError(f"unknown tier: {tier}")


def _scripts(plans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "shot_id": plan["shot_id"],
            "prompt": plan["raw_prompt"],
            "duration_phrase": plan["duration_phrase"],
            "must_subjects": plan["required_subjects"],
            "must_scenes": plan["scenes"],
            "negative_terms": plan["negatives"],
        }
        for plan in plans
    ]


def _subjects(plans: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "shots": {
            plan["shot_id"]: {
                "required_subjects": plan["required_subjects"],
                "forbidden_subjects": plan["forbidden_subjects"],
            }
            for plan in plans
        },
    }


def _reference(tier: str, seed: int, plans: list[dict[str, Any]]) -> dict[str, Any]:
    shots: dict[str, Any] = {}
    for plan in plans:
        duration = None
        if plan["target_ms"] is not None:
            duration = {
                "target_ms": plan["target_ms"],
                "source": plan["target_source"],
                "precision_digits": plan["target_precision"],
            }
        shots[plan["shot_id"]] = {
            "normalized_prompt": plan["normalized_prompt"],
            "units": plan["units"],
            "duration": duration,
            "targets": plan["targets"],
            "expected_coverage": _expected_coverage(plan),
            "expected_error_codes": plan["expected_codes"],
            "expected_verdict": plan["expected_verdict"],
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "tier": tier,
        "seed": seed,
        "generator_version": GENERATOR_VERSION,
        "pipeline_contract_version": PIPELINE_VERSION,
        "shots": shots,
    }


def generate_tier(workspace: str | Path, tier: str, seed: int) -> dict[str, Any]:
    workspace_path = Path(workspace)
    inputs_dir = ensure_dir(workspace_path / "inputs")
    for directory in ("checkpoints", "objects", "exports", "locks"):
        ensure_dir(workspace_path / directory)

    plans = build_plans(tier, seed)
    scripts = _scripts(plans)
    subjects = _subjects(plans)
    reference = _reference(tier, seed, plans)
    config = default_config()

    scripts_bytes = "".join(
        canonical_bytes(record).decode("utf-8") for record in scripts
    ).encode("utf-8")
    subjects_bytes = canonical_bytes(subjects)
    reference_bytes = canonical_bytes(reference)
    config_bytes = canonical_bytes(config)

    from .util import atomic_write_bytes

    atomic_write_bytes(inputs_dir / "scripts.jsonl", scripts_bytes)
    atomic_write_bytes(inputs_dir / "subjects.json", subjects_bytes)
    atomic_write_bytes(inputs_dir / "reference.json", reference_bytes)
    atomic_write_bytes(inputs_dir / "config.json", config_bytes)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generator_version": GENERATOR_VERSION,
        "pipeline_contract_version": PIPELINE_VERSION,
        "tier": tier,
        "seed": seed,
        "shot_count": len(plans),
        "files": {
            "scripts.jsonl": sha256_bytes(scripts_bytes),
            "subjects.json": sha256_bytes(subjects_bytes),
            "reference.json": sha256_bytes(reference_bytes),
            "config.json": sha256_bytes(config_bytes),
        },
        "content_fingerprint": sha256_obj(
            {
                "tier": tier,
                "seed": seed,
                "scripts": scripts,
                "subjects": subjects,
                "reference": reference,
                "config": config,
            }
        ),
    }
    atomic_write_json(inputs_dir / "manifest.json", manifest)
    return manifest


def generate_all(root: str | Path, *, seed: int = 20261123) -> dict[str, Any]:
    root_path = Path(root)
    ensure_dir(root_path)
    manifests = {}
    for tier in ("small", "large"):
        manifests[tier] = generate_tier(root_path / tier, tier, seed)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "tiers": manifests,
    }
    atomic_write_json(root_path / "generation.json", summary)
    return summary
