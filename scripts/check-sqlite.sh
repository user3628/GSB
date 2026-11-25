#!/usr/bin/env bash
set -euo pipefail
DB="${1:-artifacts/verify-work/main.db}"
command -v sqlite3 >/dev/null || { echo "sqlite3 CLI not found" >&2; exit 1; }
echo "== tables =="; sqlite3 "$DB" '.tables'
echo "== head =="; sqlite3 -header -column "$DB" 'SELECT * FROM head_state;'
echo "== snapshots =="; sqlite3 -header -column "$DB" 'SELECT version,kind,base_version,status,model_count FROM snapshots ORDER BY version;'
echo "== artifacts =="; sqlite3 -header -column "$DB" 'SELECT artifact,status,COUNT(*) n FROM derived_artifacts GROUP BY artifact,status ORDER BY artifact,status;'
echo "== bindings =="; sqlite3 -header -column "$DB" 'SELECT request_id,model_key,snapshot_version,status FROM inflight_bindings ORDER BY request_id;'
echo "== intents =="; sqlite3 -header -column "$DB" 'SELECT intent_id,kind,target_version,status FROM intents ORDER BY created_at_ns;'
