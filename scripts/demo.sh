#!/usr/bin/env bash
# End-to-end demo: deterministic sample, full run, stop/resume, error injection.
set -euo pipefail
cd "$(dirname "$0")/.."

WS=workspace
PY=${PYTHON:-python3}

step() { printf '\n=== %s ===\n' "$1"; }

step "0. clean workspace artifacts (from-zero build)"
$PY -c "import shutil; shutil.rmtree('$WS', ignore_errors=True)"

step "1. generate deterministic sample (fixed seed, 4 shards)"
$PY -m rerank_bench --workspace $WS generate --data-dir data | $PY -c \
  "import json,sys; m=json.load(sys.stdin)['generated']; print('seed:', m['seed']); [print(' ', s['shard_id'], s['candidates'], 'candidates') for s in m['shards']]"

step "2. import shards + config"
$PY -m rerank_bench --workspace $WS import --data-dir data > /dev/null && echo imported

step "3. full parallel run -> fixed digest"
$PY -m rerank_bench --workspace $WS run > /dev/null
sha256sum $WS/export/summary.json

step "4. stop between shards (after 2), then resume -> identical digest"
$PY -c "import shutil; shutil.rmtree('$WS', ignore_errors=True)"
$PY -m rerank_bench --workspace $WS import --data-dir data > /dev/null
$PY -m rerank_bench --workspace $WS run --stop-after 2 | $PY -c \
  "import json,sys; r=json.load(sys.stdin); print('stopped:', r['stopped'], 'pending:', r['pending'])"
$PY -m rerank_bench --workspace $WS resume > /dev/null
sha256sum $WS/export/summary.json

step "5. budget view + rerank decision trace sample"
$PY -m rerank_bench --workspace $WS budget | $PY -c \
  "import json,sys; b=json.load(sys.stdin); print('consumed:', b['consumed_evaluations'], '/', b['global_eval_budget'], 'concurrency:', b['concurrency'])"
$PY -m rerank_bench --workspace $WS trace shard-000 | $PY -c \
  "import json,sys; t=json.load(sys.stdin); g=t['query_groups'][0]; s=g['trace'][1]; print('query:', g['query_id'], 'window:', g['effective_window']); print('step 1 chose:', s['chosen']['doc_id'], 'adjusted:', s['chosen']['adjusted_score'], 'penalty:', s['chosen']['penalty'])"

step "6. inject over-budget config -> E_BUDGET_EXCEEDED"
$PY - <<'PYEOF'
import yaml
p = "workspace/data/schedule.yaml"
cfg = yaml.safe_load(open(p))
cfg["parallel"]["global_eval_budget"] = 250
yaml.safe_dump(cfg, open(p, "w"), allow_unicode=True, sort_keys=False)
PYEOF
$PY -m rerank_bench --workspace $WS run > /dev/null
$PY -m rerank_bench --workspace $WS budget | $PY -c \
  "import json,sys; b=json.load(sys.stdin); print('unprocessed:', [(u['shard_id'], u['code']) for u in b['unprocessed']])"

step "7. inject zero latency budget -> E_LATENCY_BUDGET"
$PY - <<'PYEOF'
import yaml
p = "workspace/data/schedule.yaml"
cfg = yaml.safe_load(open(p))
cfg["parallel"]["global_eval_budget"] = 100000
cfg["parallel"]["shard_latency_budget_ms"] = 0
yaml.safe_dump(cfg, open(p, "w"), allow_unicode=True, sort_keys=False)
PYEOF
$PY -m rerank_bench --workspace $WS run --serial | $PY -c \
  "import json,sys; r=json.load(sys.stdin); e=r['shard_results']['shard-000']['error']; print('code:', e['code'], 'details:', e['details'])"

step "8. restore pristine sample, verify fixed digest"
$PY -m rerank_bench --workspace $WS import --data-dir data > /dev/null
$PY -m rerank_bench --workspace $WS run > /dev/null
sha256sum $WS/export/summary.json
$PY tools/verify_run.py --check
