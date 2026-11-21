"""Reference diagnostic metric files and tolerance comparison."""

from __future__ import annotations

import os

import yaml

from ..errors import InvalidConfigError
from .checkpoints import atomic_write

METRIC_KEYS = ("mean_bias", "bias_peak", "diff_abs_mean",
               "monotonic_violations", "change_rate")


def build_reference(report) -> dict:
    """Project a full diagnostic report into the reference metric file."""
    reference = {
        "reference_version": 1,
        "config_fingerprint": report["config_fingerprint"],
        "schemes": {},
    }
    for scheme in report["schemes"]:
        bins = {}
        for record in scheme["bins"]:
            bins[record["bin_id"]] = {
                key: record.get(key) for key in METRIC_KEYS
            }
        reference["schemes"][scheme["scheme"]] = {
            "overall_bias_peak": scheme["overall_bias_peak"],
            "bins": bins,
        }
    return reference


def write_reference(path, reference):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(reference, handle, sort_keys=True,
                       allow_unicode=True, default_flow_style=False)


def load_reference(path):
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict) or "schemes" not in data:
        raise InvalidConfigError(
            "reference file must contain a schemes mapping",
            stage="tolerance")
    return data


def compare(report, reference, tolerance) -> dict:
    results = []
    all_pass = True
    for scheme in report["schemes"]:
        sid = scheme["scheme"]
        ref_scheme = reference.get("schemes", {}).get(sid)
        if ref_scheme is None:
            raise InvalidConfigError(
                f"missing reference entry for scheme {sid}",
                stage="tolerance", scheme=sid)
        scheme_result = {"scheme": sid, "passed": True, "violations": []}
        peak_tol = float(tolerance.get("bias_peak", 0.0))
        passed, violation = _compare_scalar(
            sid, "overall_bias_peak", bin_id=None,
            estimate=scheme["overall_bias_peak"],
            reference=ref_scheme["overall_bias_peak"],
            tolerance=peak_tol)
        if not passed:
            scheme_result["passed"] = False
            scheme_result["violations"].append(violation)
        for record in scheme["bins"]:
            ref_bin = ref_scheme.get("bins", {}).get(record["bin_id"])
            if ref_bin is None:
                raise InvalidConfigError(
                    f"missing reference bin {record['bin_id']}",
                    stage="tolerance", scheme=sid, bin_id=record["bin_id"])
            for key in METRIC_KEYS:
                estimate = record.get(key)
                ref_value = ref_bin.get(key)
                if estimate is None and ref_value is None:
                    continue
                if estimate is None or ref_value is None:
                    raise InvalidConfigError(
                        "reference/estimate empty-bin mismatch",
                        stage="tolerance", scheme=sid,
                        bin_id=record["bin_id"],
                        context={"metric": key})
                tol = float(tolerance.get(key, 0.0))
                passed, violation = _compare_scalar(
                    sid, key, bin_id=record["bin_id"], estimate=estimate,
                    reference=ref_value, tolerance=tol,
                    integer=(key == "monotonic_violations"))
                if not passed:
                    scheme_result["passed"] = False
                    scheme_result["violations"].append(violation)
        if not scheme_result["passed"]:
            all_pass = False
        results.append(scheme_result)
    return {"passed": all_pass, "scheme_results": results}


def _compare_scalar(scheme, metric, *, bin_id, estimate, reference,
                    tolerance, integer=False):
    if integer:
        delta = abs(int(estimate) - int(reference))
        passed = delta <= int(tolerance)
    else:
        delta = abs(float(estimate) - float(reference))
        passed = delta <= tolerance
    violation = None
    if not passed:
        violation = {
            "scheme": scheme,
            "bin_id": bin_id,
            "metric": metric,
            "reference": reference,
            "estimate": estimate,
            "delta": delta,
            "tolerance": tolerance,
        }
    return passed, violation
