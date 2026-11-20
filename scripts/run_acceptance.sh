#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -n "${PYTHON:-}" ]]; then
  PY="$PYTHON"
elif [[ -x "$ROOT/.venv311/bin/python" ]]; then
  PY="$ROOT/.venv311/bin/python"
else
  PY="python3"
fi

mkdir -p artifacts
"$PY" scripts/generate_data.py --seed 17
"$PY" -m pytest -q
"$PY" -m qiu demo --output-dir artifacts > artifacts/demo_stdout.json
"$PY" -m qiu experiment --seed 17 --bootstrap-samples 200 --output artifacts/experiment_seed17_run1.json > /dev/null
"$PY" -m qiu experiment --seed 17 --bootstrap-samples 200 --output artifacts/experiment_seed17_run2.json > /dev/null
"$PY" -m qiu experiment --seed 18 --bootstrap-samples 200 --output artifacts/experiment_seed18.json > /dev/null

cmp artifacts/experiment_seed17_run1.json artifacts/experiment_seed17_run2.json
if cmp -s artifacts/experiment_seed17_run1.json artifacts/experiment_seed18.json; then
  echo "seed-sensitive experiment check failed" >&2
  exit 1
fi

"$PY" - <<'PY' > artifacts/acceptance_summary.json
import hashlib
import json
import platform
from pathlib import Path

import numpy
import pytest

root = Path('.')
seed17_a = (root / 'artifacts/experiment_seed17_run1.json').read_bytes()
seed17_b = (root / 'artifacts/experiment_seed17_run2.json').read_bytes()
seed18 = (root / 'artifacts/experiment_seed18.json').read_bytes()
summary = json.loads(seed17_a)
demo = json.loads((root / 'artifacts/demo_report.json').read_text(encoding='utf-8'))
result = {
    'acceptance_schema': 'qiu-acceptance-v1',
    'environment': {
        'numpy': numpy.__version__,
        'platform': platform.platform(),
        'pytest': pytest.__version__,
        'python': platform.python_version(),
    },
    'checks': {
        'different_seed_changes_summary': seed17_a != seed18,
        'error_codes_reproduced': all(item['reproduced_byte_identically'] for item in demo['error_injection']),
        'fallback_levels_observed': [item['used_fallback_level'] for item in demo['fallback_chain']],
        'invalidation_rebuild_observed': [item['changed_components'] for item in demo['invalidation_rebuild'][1:]],
        'slot_overlap_diagnostics_observed': len(demo['slot_overlap']['suppressed']) >= 2,
        'tests_passed': True,
        'two_seed17_runs_byte_identical': seed17_a == seed17_b,
    },
    'experiment_metrics': summary['metrics'],
    'hashes': {
        'seed17': hashlib.sha256(seed17_a).hexdigest(),
        'seed18': hashlib.sha256(seed18).hexdigest(),
    },
    'run_fingerprints': {
        'seed17': summary['run_fingerprint'],
        'seed18': json.loads(seed18)['run_fingerprint'],
    },
}
print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
PY

echo "acceptance-ok"
