"""End-to-end reproducible demonstration.

Steps:
  1. regenerate deterministic samples into a clean demo work directory
  2. full four-stage run and persist the fixed summary
  3. separate clean directory: graceful stop, resume, assert byte equality
  4. run every supported anomaly injection and report business codes
"""

import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from generate_samples import main as generate_main
from iqa.pipeline import Pipeline
from iqa.config import Config
from iqa.injection import Injection


def _read(path):
    with open(path, "rb") as handle:
        return handle.read()


def main():
    demo_dir = os.path.join(ROOT, "work_demo")
    shutil.rmtree(demo_dir, ignore_errors=True)
    generate_main(demo_dir)

    full = Pipeline(demo_dir, Config().validate())
    full_summary = full.run()
    full_bytes = _read(os.path.join(demo_dir, "outputs", "summary.json"))
    print("[1] full run images:", full_summary["image_count"], "max_sequence:", full_summary["max_sequence"])

    stop_dir = os.path.join(ROOT, "work_demo_stop")
    shutil.rmtree(stop_dir, ignore_errors=True)
    generate_main(stop_dir)
    stopped_pipeline = Pipeline(stop_dir)
    stopped_pipeline.store.request_stop()
    stopped = stopped_pipeline.run()
    print("[2] graceful stop after images:", stopped["image_count"], "sequence:", stopped["max_sequence"])

    resumed_pipeline = Pipeline(stop_dir)
    resumed_pipeline.store.clear_stop()
    resumed = resumed_pipeline.run()
    resume_bytes = _read(os.path.join(stop_dir, "outputs", "summary.json"))
    print("[3] resume images:", resumed["image_count"], "max_sequence:", resumed["max_sequence"])
    print("    byte-identical to full run:", resume_bytes == full_bytes)

    print("[4] anomaly injections:")
    for kind in ("dimension_mismatch", "missing_reference", "nan_pixels", "negative_weight"):
        injection = Injection(kind=kind, image_id="img_001_blocks")
        summary = Pipeline(demo_dir, Config().validate(), injection).run()
        codes = [item["code"] for item in summary["diagnostics"]]
        print(f"    {kind:20s} -> {sorted(set(codes))}")

    report = {
        "full_summary_bytes_equal_on_resume": resume_bytes == full_bytes,
        "full_grade_counts": full_summary["grade_counts"],
        "out_of_range": [item["image_id"] for item in full_summary["out_of_range"]],
        "diagnostic_codes": sorted({item["code"] for item in full_summary["diagnostics"]}),
    }
    report_path = os.path.join(demo_dir, "outputs", "demo_report.json")
    with open(report_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print("[5] demo report written:", os.path.relpath(report_path, ROOT))


if __name__ == "__main__":
    main()
