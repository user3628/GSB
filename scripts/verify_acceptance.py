from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from qworkbench.config import WorkbenchConfig
from qworkbench.errors import BusinessError
from qworkbench.numeric import weighted_geometric_fixed
from qworkbench.pipeline import Pipeline
from qworkbench.samples import dataset_from_directory
from qworkbench.store import CheckpointStore


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    config = WorkbenchConfig.load(ROOT / "configs/demo.json")
    dataset = dataset_from_directory(ROOT / "data/demo")
    report: dict = {
        "dataset": dataset.summary(),
        "config_version": config.version,
        "config_fingerprint": config.fingerprint,
    }
    with tempfile.TemporaryDirectory(prefix="iqw-verify-") as tmp:
        root = Path(tmp)
        complete = Pipeline(config, dataset, root / "complete").run()
        stopped = Pipeline(
            config, dataset, root / "stop-resume", stop_after_stage="normalize"
        ).run()
        resumed = Pipeline(config, dataset, root / "stop-resume").resume()
        complete_mixture = (root / "complete/artifacts/mix/mixture.jsonl").read_bytes()
        resumed_mixture = (root / "stop-resume/artifacts/mix/mixture.jsonl").read_bytes()
        report["complete_run"] = complete
        report["stop_resume"] = {
            "stopped": stopped["status"],
            "resumed": resumed["status"],
            "reused_stages": resumed["reused_stages"],
            "mixture_bytes_identical": complete_mixture == resumed_mixture,
        }

        raw = copy.deepcopy(dict(config.raw))
        raw["quantization"]["fraction_bits"] = 18
        quant_config = WorkbenchConfig.from_dict(raw)
        incremental_dir = root / "incremental"
        Pipeline(config, dataset, incremental_dir).run()
        incremental = Pipeline(quant_config, dataset, incremental_dir).run()
        incremental_status = Pipeline(quant_config, dataset, incremental_dir).status()
        report["incremental_invalidation"] = {
            "reused_stages": incremental["reused_stages"],
            "earliest_stage": incremental_status["last_invalidation"]["earliest_stage"],
            "affected_stages": incremental_status["last_invalidation"]["affected_stages"],
            "normalize_seq": incremental_status["stages"]["normalize"]["seq"],
        }

        errors: dict[str, str] = {}
        try:
            Pipeline(config, dataset, root / "missing-stage").run(start_stage="score")
        except BusinessError as exc:
            errors["missing_predecessor"] = exc.code
        try:
            bad = copy.deepcopy(dict(config.raw))
            bad["quantization"]["fraction_bits"] = 0
            WorkbenchConfig.from_dict(bad)
        except BusinessError as exc:
            errors["illegal_quantization"] = exc.code
        try:
            bad = copy.deepcopy(dict(config.raw))
            bad["allocation"]["source_weights"]["curated-sft"] = 2.0
            WorkbenchConfig.from_dict(bad)
        except BusinessError as exc:
            errors["out_of_range_weight"] = exc.code
        try:
            factors = {
                "length_penalty": 0.5,
                "lexical_overlap": float("nan"),
                "template_diversity": 0.8,
                "source_prior": 0.7,
            }
            weighted_geometric_fixed(
                "verify-numeric", factors, config.scoring.weights, {}, config.quantization
            )
        except BusinessError as exc:
            errors["nan_factor"] = exc.code

        stale_dir = root / "stale"
        store = CheckpointStore(stale_dir)
        artifact = stale_dir / "artifacts/fixture.bin"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_bytes(b"verify-stale\n")
        from qworkbench.util import digest_file

        record = {
            "stage": "normalize",
            "input_fingerprint": "input",
            "config_version": "verify",
            "config_fingerprint": "config",
            "upstream_fingerprint": "upstream",
            "fingerprint": "stage",
            "output_fingerprint": "output",
            "artifacts": {str(artifact.relative_to(stale_dir)): digest_file(artifact)},
            "summary": {"records": 1},
        }
        store.publish_stage(record, seq=1)
        try:
            store.publish_stage(record, seq=1)
        except BusinessError as exc:
            errors["duplicate_publish"] = exc.code
        report["business_errors"] = errors

    test = subprocess.run(
        [sys.executable, "-m", "pytest", "-o", "addopts=", "-q"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    passed_lines = [line for line in test.stdout.splitlines() if " passed" in line]
    report["test_suite"] = {
        "returncode": test.returncode,
        "summary": passed_lines[-1] if passed_lines else test.stdout.strip()[-500:],
    }
    output = ROOT / "artifacts/verification.json"
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if test.returncode == 0 and report["stop_resume"]["mixture_bytes_identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
