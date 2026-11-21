"""Independent local reference-score and diagnostics generation."""

from __future__ import annotations

import os
from typing import Any, Iterable

from .config import STAGES, config_version
from .data import load_tier
from .detectors import fit_detectors
from .diagnostics import build_diagnostics
from .pipeline import Pipeline, make_stage_artifact
from .preprocessing import matrix_from_preprocess, preprocess_tier
from .scoring import score_and_decide
from .util import atomic_write_json, file_lock, sha256_bytes


def generate_reference(
    root: str | os.PathLike[str], *, tiers: Iterable[str] | None = None
) -> dict[str, Any]:
    """Recompute both tiers through the pure functional path.

    This path intentionally bypasses checkpoint reuse and all invalidation
    bookkeeping.  It is a separate execution that starts from the raw NPZ
    shards and writes immutable numerical truth for later comparison.
    """

    pipeline = Pipeline(root)
    lock = file_lock(pipeline.root / "locks" / "pipeline.lock", blocking=False)
    try:
        lock.acquire()
    except BlockingIOError as exc:
        from .errors import WorkbenchError

        raise WorkbenchError(
            "E_BUSY", "cannot generate reference while the pipeline is executing"
        ) from exc
    try:
        pipeline.refresh()
        selected = pipeline._select_tiers(tiers)
        stage_configs = pipeline._stage_config_fingerprints()
        result: dict[str, Any] = {
            "schema_version": 1,
            "generator": "independent from-zero local reference",
            "config_version": config_version(pipeline.config),
            "tiers": {},
        }
        for tier in selected:
            data = load_tier(root, tier)
            preprocess_input = pipeline._preprocess_input(tier, data, pipeline.config)
            preprocess_bytes = make_stage_artifact(
                stage="preprocess",
                tier=tier,
                input_fingerprint=preprocess_input,
                stage_config_fingerprint=stage_configs["preprocess"],
                payload=preprocess_tier(data, pipeline.config),
            )
            preprocess_artifact = __import__("json").loads(
                preprocess_bytes.decode("utf-8")
            )
            preprocess_artifact["artifact_digest"] = sha256_bytes(preprocess_bytes)
            upstream = {
                "preprocess": {
                    "input_fingerprint": preprocess_input,
                    "artifact_digest": preprocess_artifact["artifact_digest"],
                }
            }

            fit_input = pipeline._next_input("fit", tier, pipeline.config, upstream)
            assert fit_input is not None
            fit_bytes = make_stage_artifact(
                stage="fit",
                tier=tier,
                input_fingerprint=fit_input,
                stage_config_fingerprint=stage_configs["fit"],
                payload=fit_detectors(
                    matrix_from_preprocess(preprocess_artifact),
                    pipeline.config["detectors"],
                ),
            )
            fit_artifact = __import__("json").loads(fit_bytes.decode("utf-8"))
            fit_artifact["artifact_digest"] = sha256_bytes(fit_bytes)
            upstream["fit"] = {
                "input_fingerprint": fit_input,
                "artifact_digest": fit_artifact["artifact_digest"],
            }

            score_input = pipeline._next_input("score", tier, pipeline.config, upstream)
            assert score_input is not None
            score_bytes = make_stage_artifact(
                stage="score",
                tier=tier,
                input_fingerprint=score_input,
                stage_config_fingerprint=stage_configs["score"],
                payload=score_and_decide(
                    preprocess_artifact,
                    fit_artifact,
                    pipeline.config,
                    data.declaration,
                ),
            )
            score_artifact = __import__("json").loads(score_bytes.decode("utf-8"))
            score_artifact["artifact_digest"] = sha256_bytes(score_bytes)
            upstream["score"] = {
                "input_fingerprint": score_input,
                "artifact_digest": score_artifact["artifact_digest"],
            }

            diagnostics_input = pipeline._next_input(
                "diagnostics", tier, pipeline.config, upstream
            )
            assert diagnostics_input is not None
            diagnostics_payload = build_diagnostics(
                score_artifact, data.declaration, pipeline.config
            )
            diagnostics_bytes = make_stage_artifact(
                stage="diagnostics",
                tier=tier,
                input_fingerprint=diagnostics_input,
                stage_config_fingerprint=stage_configs["diagnostics"],
                payload=diagnostics_payload,
            )
            diagnostics_artifact = __import__("json").loads(
                diagnostics_bytes.decode("utf-8")
            )
            diagnostics_artifact["artifact_digest"] = sha256_bytes(diagnostics_bytes)

            artifacts = {
                "preprocess": preprocess_artifact,
                "fit": fit_artifact,
                "score": score_artifact,
                "diagnostics": diagnostics_artifact,
            }
            result["tiers"][tier] = {
                "data_fingerprint": data.data_fingerprint,
                "stages": {
                    stage: {
                        "input_fingerprint": artifacts[stage]["input_fingerprint"],
                        "stage_config_fingerprint": artifacts[stage][
                            "stage_config_fingerprint"
                        ],
                        "artifact_digest": artifacts[stage]["artifact_digest"],
                    }
                    for stage in STAGES
                },
                "scores": {
                    "source_ids": score_artifact["payload"]["source_ids"],
                    "batch_ids": score_artifact["payload"]["batch_ids"],
                    "raw_scores": score_artifact["payload"]["raw_scores"],
                    "combined_scores": score_artifact["payload"]["combined_scores"],
                    "threshold": score_artifact["payload"]["threshold"],
                    "decisions": score_artifact["payload"]["decisions"],
                },
                "diagnostics": diagnostics_artifact["payload"],
            }
        atomic_write_json(pipeline.reference_path, result)
        return {
            "reference_path": str(pipeline.reference_path),
            "config_version": result["config_version"],
            "tiers": {
                tier: {
                    "artifact_digests": {
                        stage: result["tiers"][tier]["stages"][stage]["artifact_digest"]
                        for stage in STAGES
                    },
                    "score_count": len(
                        result["tiers"][tier]["scores"]["combined_scores"]
                    ),
                }
                for tier in result["tiers"]
            },
        }
    finally:
        lock.release()


def verify_reference(root: str | os.PathLike[str]) -> dict[str, Any]:
    pipeline = Pipeline(root)
    pipeline.refresh()
    reference = pipeline._load_reference()
    if reference is None:
        return {"verified": False, "reason": "reference file is missing"}
    if reference.get("config_version") != config_version(pipeline.config):
        return {
            "verified": False,
            "reason": "reference was generated for a different configuration",
            "reference_config_version": reference.get("config_version"),
            "current_config_version": config_version(pipeline.config),
        }
    tiers: dict[str, Any] = {}
    for tier in reference.get("tiers", {}):
        try:
            artifacts = pipeline.current_artifacts(tier)
        except Exception as exc:
            tiers[tier] = {
                "verified": False,
                "error": exc.to_dict() if hasattr(exc, "to_dict") else {"message": str(exc)},
            }
            continue
        tier_result: dict[str, Any] = {}
        for stage in STAGES:
            expected = reference["tiers"][tier]["stages"][stage]["artifact_digest"]
            actual = artifacts[stage]["artifact_digest"]
            tier_result[stage] = {
                "match": expected == actual,
                "reference_digest": expected,
                "actual_digest": actual,
            }
        tiers[tier] = tier_result
    return {
        "verified": all(
            isinstance(tier, dict)
            and "error" not in tier
            and all(stage["match"] for stage in tier.values())
            for tier in tiers.values()
        ),
        "config_version": reference["config_version"],
        "tiers": tiers,
    }
