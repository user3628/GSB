#!/usr/bin/env bash
# client.sh - thin local HTTP client for the gpusched service.
# Uses only curl + python3 (for pretty printing). No external services.
set -euo pipefail

BASE="${BASE_URL:-http://127.0.0.1:8080}"

usage() {
  cat <<'USAGE'
gpusched local client

Usage:
  scripts/client.sh load-sample
  scripts/client.sh quota
  scripts/client.sh nodes
  scripts/client.sh groups
  scripts/client.sh requests
  scripts/client.sh admit <request_id>
  scripts/client.sh transition <request_id> <TO_STATE>
  scripts/client.sh preempt <request_id>
  scripts/client.sh round [virtual_budget_consumed_ms]
  scripts/client.sh batches
  scripts/client.sh policy
  scripts/client.sh recovery
  scripts/client.sh submit <request.json>
USAGE
}

pp() { python3 -m json.tool 2>/dev/null || cat; }

cmd="${1:-}"
case "$cmd" in
  load-sample) curl -sS -X POST "$BASE/v1/spec/load-sample" | pp ;;
  quota)       curl -sS "$BASE/v1/quota" | pp ;;
  nodes)       curl -sS "$BASE/v1/nodes" | pp ;;
  groups)      curl -sS "$BASE/v1/groups" | pp ;;
  requests)    curl -sS "$BASE/v1/requests" | pp ;;
  batches)     curl -sS "$BASE/v1/batches" | pp ;;
  policy)      curl -sS "$BASE/v1/policy/latest" | pp ;;
  recovery)    curl -sS "$BASE/v1/recovery" | pp ;;
  admit)
    [[ $# -ge 2 ]] || { usage; exit 2; }
    curl -sS -X POST "$BASE/v1/requests/$2/admit" | pp ;;
  transition)
    [[ $# -ge 3 ]] || { usage; exit 2; }
    curl -sS -X POST "$BASE/v1/requests/$2/transition" \
      -H 'Content-Type: application/json' -d "{\"to\":\"$3\"}" | pp ;;
  preempt)
    [[ $# -ge 2 ]] || { usage; exit 2; }
    curl -sS -X POST "$BASE/v1/requests/$2/preempt" | pp ;;
  round)
    budget="${2:-0}"
    curl -sS -X POST "$BASE/v1/scheduler/round" \
      -H 'Content-Type: application/json' \
      -d "{\"virtual_budget_consumed_ms\":$budget}" | pp ;;
  submit)
    [[ $# -ge 2 ]] || { usage; exit 2; }
    curl -sS -X POST "$BASE/v1/requests" \
      -H 'Content-Type: application/json' --data-binary "@$2" | pp ;;
  *) usage; exit 2 ;;
esac
