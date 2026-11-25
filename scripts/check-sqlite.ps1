param(
  [string]$Db = "artifacts/verify-work/main.db"
)

$ErrorActionPreference = "Stop"
if (-not (Get-Command sqlite3 -ErrorAction SilentlyContinue)) {
  throw "sqlite3 CLI was not found on PATH. Install SQLite tools and reopen PowerShell."
}

Write-Host "== tables =="
sqlite3 $Db ".tables"
Write-Host "`n== head =="
sqlite3 -header -column $Db "SELECT * FROM head_state;"
Write-Host "`n== snapshots =="
sqlite3 -header -column $Db "SELECT version,kind,base_version,status,model_count FROM snapshots ORDER BY version;"
Write-Host "`n== artifact status =="
sqlite3 -header -column $Db "SELECT artifact,status,COUNT(*) AS n FROM derived_artifacts GROUP BY artifact,status ORDER BY artifact,status;"
Write-Host "`n== bindings =="
sqlite3 -header -column $Db "SELECT request_id,model_key,snapshot_version,status FROM inflight_bindings ORDER BY request_id;"
Write-Host "`n== intents =="
sqlite3 -header -column $Db "SELECT intent_id,kind,target_version,status FROM intents ORDER BY created_at_ns;"
