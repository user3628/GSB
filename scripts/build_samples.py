from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def write_json(name: str, value: Any) -> None:
    (SAMPLES / name).write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def policy(version: int, generated_at_ms: int, *, threshold: int, tight: bool) -> dict[str, Any]:
    if tight:
        thresholds = {
            "error_rate": 0.02,
            "p90_latency_increase": 0.10,
            "success_rate_drop": 0.01,
        }
    else:
        thresholds = {
            "error_rate": 0.05,
            "p90_latency_increase": 0.25,
            "success_rate_drop": 0.03,
        }
    return {
        "version": version,
        "generated_at_ms": generated_at_ms,
        "window_ms": 60_000,
        "late_tolerance_ms": 5_000,
        "min_samples": 10,
        "thresholds": thresholds,
        "weights": {
            "error_rate": 0.40,
            "latency": 0.35,
            "success_drop": 0.25,
        },
        "risk_threshold": threshold,
        "auto_rollback_enabled": True,
        "cooldown_ms": 300_000,
        "fast": {
            "sample_cap": 12,
            "confidence_level": 0.95,
            "confidence_upper_bound": 5.0 if tight else 6.0,
        },
    }


def observation(
    sequence: int, timestamp_ms: int, latency_ms: float, *, success: bool = True
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "timestamp_ms": timestamp_ms,
        "metric_type": "CANDIDATE",
        "latency_ms": latency_ms,
        "success": success,
    }


def make_batch(
    batch_id: str,
    service: str,
    change_type: str,
    batch_size: int,
    started_at_ms: int,
    description: str,
) -> dict[str, Any]:
    return {
        "batch_id": batch_id,
        "service": service,
        "change_type": change_type,
        "batch_size": batch_size,
        "started_at_ms": started_at_ms,
        "policy_version": 1,
        "baseline": {
            "sample_count": 40,
            "success_count": 38,
            "latency_p90_ms": 180,
        },
        "description": description,
    }


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    v1 = policy(1, 1_763_800_000_000, threshold=50, tight=False)
    v2 = policy(2, 1_763_801_000_000, threshold=30, tight=True)
    write_json("policy-v1.json", v1)
    write_json("policy-v2.json", v2)

    config = {
        "policy_version": 1,
        "window_ms": v1["window_ms"],
        "late_tolerance_ms": v1["late_tolerance_ms"],
        "min_samples": v1["min_samples"],
        "fast": {
            **v1["fast"],
            "sampling_method": "systematic_even_by_sorted_sequence",
            "bootstrap_resamples": 400,
            "seed_rule": "sha256(policy_digest,batch,window,epoch,sampled_sequences)",
        },
        "full": {
            "sample_scope": "all observations in the selected window",
            "confidence": "exhaustive; no sampling error",
        },
        "comparison_batch_id": "B-COMPARE",
    }
    write_json("fast-full-config.json", config)

    batches = [
        make_batch(
            "B-PASS",
            "checkout-api",
            "CONFIG_APPLY",
            24,
            1_763_900_000_000,
            "低风险配置下发，用于完整模式放行。",
        ),
        make_batch(
            "B-ROLLBACK",
            "checkout-api",
            "IMAGE_UPGRADE",
            24,
            1_763_910_000_000,
            "高风险镜像升级，用于自动回滚与冷却期重触发。",
        ),
        make_batch(
            "B-COMPARE",
            "catalog-api",
            "IMAGE_UPGRADE",
            40,
            1_763_920_000_000,
            "完整窗口低风险但快速抽样跨阈值的模式对照批次。",
        ),
        make_batch(
            "B-INSUFFICIENT",
            "inventory-api",
            "SCALE",
            30,
            1_763_930_000_000,
            "仅 3 条观测，用于样本不足业务码。",
        ),
        make_batch(
            "B-CRASH",
            "payment-api",
            "CONFIG_APPLY",
            24,
            1_763_940_000_000,
            "高风险批次，用于回滚事务提交前强制终止恢复。",
        ),
    ]
    write_json("batches.json", batches)

    pass_latencies = [179, 181, 180, 182, 178, 183, 181, 180, 184, 179, 182, 180,
                      181, 179, 183, 180, 182, 181, 179, 184, 180, 181, 182, 180]
    pass_obs = [
        observation(
            index + 1,
            1_763_900_000_000 + index * 1_000,
            latency,
            success=index + 1 != 7,
        )
        for index, latency in enumerate(pass_latencies)
    ]

    rollback_latencies = [300, 310, 320, 305, 315, 325, 330, 302, 312, 322, 308, 318,
                          328, 306, 316, 326, 304, 314, 324, 332, 309, 319, 329, 311]
    rollback_failures = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19}
    rollback_obs = [
        observation(
            index + 1,
            1_763_910_000_000 + index * 1_000,
            latency,
            success=index + 1 not in rollback_failures,
        )
        for index, latency in enumerate(rollback_latencies)
    ]

    compare_pattern = [178, 180, 182, 184, 186, 188, 190, 192, 194, 196, 198, 200]
    compare_failures = {1, 5}
    compare_obs = [
        observation(
            index + 1,
            1_763_920_000_000 + index * 1_000,
            compare_pattern[index % len(compare_pattern)],
            success=index + 1 not in compare_failures,
        )
        for index in range(40)
    ]

    insufficient_obs = [
        observation(1, 1_763_930_000_000, 181),
        observation(2, 1_763_930_001_000, 183, success=False),
        observation(3, 1_763_930_002_000, 182),
    ]

    crash_latencies = [330, 340, 350, 335, 345, 355, 360, 332, 342, 352, 338, 348,
                       358, 336, 346, 356, 334, 344, 354, 362, 339, 349, 359, 341]
    crash_failures = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23}
    crash_obs = [
        observation(
            index + 1,
            1_763_940_000_000 + index * 1_000,
            latency,
            success=index + 1 not in crash_failures,
        )
        for index, latency in enumerate(crash_latencies)
    ]

    observation_bundle = {
        "seed_observations": {
            "B-PASS": pass_obs,
            "B-ROLLBACK": rollback_obs,
            "B-COMPARE": compare_obs,
            "B-INSUFFICIENT": insufficient_obs,
            "B-CRASH": crash_obs,
        },
        "seed_watermarks": {
            "B-PASS": 1_763_900_065_000,
            "B-ROLLBACK": 1_763_910_065_000,
            "B-COMPARE": 1_763_920_065_000,
            "B-INSUFFICIENT": 1_763_930_065_000,
            "B-CRASH": 1_763_940_065_000,
        },
        "fault_cases": {
            "identical_duplicate": {
                "batch_id": "B-PASS",
                "payload": {"observations": [pass_obs[0]]},
                "expected": "idempotent_duplicate",
            },
            "conflicting_duplicate": {
                "batch_id": "B-PASS",
                "payload": {
                    "observations": [
                        {
                            **pass_obs[1],
                            "latency_ms": pass_obs[1]["latency_ms"] + 5,
                        }
                    ]
                },
                "expected_code": "E_CONFLICTING_REPORT",
            },
            "late_sample_after_close": {
                "batch_id": "B-PASS",
                "payload": {
                    "watermark_ms": 1_763_900_065_000,
                    "observations": [
                        observation(99, 1_763_900_005_000, 181, success=True)
                    ],
                },
                "expected_code": "E_LATE_SAMPLE",
            },
            "insufficient_samples": {
                "batch_id": "B-INSUFFICIENT",
                "expected_code": "E_INSUFFICIENT_SAMPLES",
            },
        },
    }
    write_json("observations.json", observation_bundle)

    demo_script = {
        "description": "ChangeGuard 固定样本端到端演示时间线。",
        "policy_files": ["policy-v1.json", "policy-v2.json"],
        "batch_file": "batches.json",
        "observation_file": "observations.json",
        "scenarios": [
            {
                "id": "stale_policy",
                "action": "SUBMIT_STALE_POLICY",
                "policy_file": "policy-v1.json",
                "expected_code": "E_STALE_POLICY",
            },
            {
                "id": "release",
                "action": "FULL_DECISION",
                "batch_id": "B-PASS",
                "decision_time_ms": 1_763_900_070_000,
                "expected_status": "PASSED",
            },
            {
                "id": "auto_rollback",
                "action": "FULL_DECISION",
                "batch_id": "B-ROLLBACK",
                "decision_time_ms": 1_763_910_070_000,
                "expected_status": "ROLLED_BACK",
            },
            {
                "id": "cooldown_retrigger",
                "action": "FULL_DECISION",
                "batch_id": "B-ROLLBACK",
                "decision_time_ms": 1_763_910_100_000,
                "expected_code": "E_ROLLBACK_COOLDOWN",
                "expected_remaining_ms": 270_000,
            },
            {
                "id": "fast_full_compare",
                "action": "COMPARE",
                "batch_id": "B-COMPARE",
            },
            {
                "id": "policy_switch_same_window",
                "action": "RISK_UNDER_TWO_POLICIES",
                "batch_id": "B-COMPARE",
                "policy_versions": [1, 2],
            },
            {
                "id": "insufficient",
                "action": "RISK",
                "batch_id": "B-INSUFFICIENT",
                "expected_code": "E_INSUFFICIENT_SAMPLES",
            },
            {
                "id": "duplicate_idempotent",
                "action": "OBSERVATION_REPLAY",
                "fault_case": "identical_duplicate",
            },
            {
                "id": "duplicate_conflict",
                "action": "OBSERVATION_REPLAY",
                "fault_case": "conflicting_duplicate",
                "expected_code": "E_CONFLICTING_REPORT",
            },
            {
                "id": "late_sample",
                "action": "OBSERVATION_REPLAY",
                "fault_case": "late_sample_after_close",
                "expected_code": "E_LATE_SAMPLE",
            },
            {
                "id": "forced_exit_recovery",
                "action": "ARM_FAULT_THEN_DECISION",
                "batch_id": "B-CRASH",
                "operation": "RISK_DECISION_ROLLBACK",
                "decision_time_ms": 1_763_940_070_000,
                "expected_kill_exit_code": 137,
                "expected_status_after_restart": "OBSERVING",
                "expected_aborted_intents": 1,
            },
        ],
    }
    write_json("demo-script.json", demo_script)


if __name__ == "__main__":
    main()
