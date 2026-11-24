param(
    [string]$BaseUrl = "http://127.0.0.1:18080"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

function Post-Sample {
    param([string]$Path, [string]$Sample)
    $body = Get-Content -Raw (Join-Path $root "samples/$Sample")
    return Invoke-RestMethod -Method Post -Uri "$BaseUrl$Path" -ContentType "application/json" -Body $body
}

$policy = Post-Sample "/api/v1/snapshots" "policy_v1.json"
if ($policy.state_revision -ne 1) { throw "policy revision was not 1" }

$r1 = Post-Sample "/api/v1/checkpoints" "round1_increment.json"
if ($r1.writes.Count -ne 2 -or $r1.deferred.Count -ne 2) { throw "round1 write split mismatch" }

$r2 = Post-Sample "/api/v1/checkpoints" "round2_increment.json"
if ($r2.evictions.Count -ne 2) { throw "round2 did not evict two shards" }
if ($r2.writes.Count -ne 2 -or $r2.deferred.Count -ne 4) { throw "round2 write split mismatch" }

$pools = Invoke-RestMethod -Uri "$BaseUrl/api/v1/pools"
$vram = ($pools.pools | Where-Object { $_.pool -eq "VRAM" }).used_bytes
$persisted = ($pools.pools | Where-Object { $_.pool -eq "PERSISTED" }).used_bytes
if ($vram -ne 2000 -or $persisted -ne 9376) { throw "pool accounting mismatch: VRAM=$vram PERSISTED=$persisted" }

$driftRejected = $false
try {
    Post-Sample "/api/v1/checkpoints" "round3_drift_increment.json" | Out-Null
} catch {
    $status = [int]$_.Exception.Response.StatusCode
    $message = [string]$_.ErrorDetails.Message
    if ($status -ne 422 -or $message -notmatch "E_PRECISION_DRIFT") {
        throw "drift response was not E_PRECISION_DRIFT/422: $status $message"
    }
    $driftRejected = $true
}
if (-not $driftRejected) { throw "expected the drift fixture to be rejected" }

$r4 = Post-Sample "/api/v1/checkpoints" "round4_complete_increment.json"
if ($r4.writes.Count -ne 2) { throw "round4 did not write both shards" }
$rounds = Invoke-RestMethod -Uri "$BaseUrl/api/v1/rounds?round=4"
if ($rounds.rounds[0].status -ne "PASS") { throw "round4 consistency was not PASS" }

$replay = Post-Sample "/api/v1/checkpoints" "round1_increment.json"
if (-not $replay.idempotent_replay) { throw "round1 replay was not idempotent" }

$policy2 = Post-Sample "/api/v1/snapshots" "policy_v2.json"
if ($policy2.state_revision -ne 5) { throw "policy2 revision was not 5" }

$report = Invoke-RestMethod -Uri "$BaseUrl/api/v1/recovery-report"
if (-not $report.facts_basis) { throw "recovery report has no facts basis" }

Write-Host "SMOKE PASS: write split, evictions, pool accounting, drift code, complete round, replay, policy version, recovery report"
