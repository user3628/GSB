param(
  [string]$Addr = "127.0.0.1:18788"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$base = "http://$Addr"
$runId = Get-Date -Format "yyyyMMddHHmmss"
$runtime = Join-Path $root ".runtime"
$buildDir = Join-Path $root ".build"
$db = Join-Path $runtime "crash-$runId.db"
New-Item -ItemType Directory -Force -Path $runtime, $buildDir | Out-Null

$previousCGO = $env:CGO_ENABLED
$env:CGO_ENABLED = "0"
& go build -trimpath -o (Join-Path $buildDir "workbench.exe") ./cmd/workbench
if ($LASTEXITCODE -ne 0) { throw "go build failed" }
if ($null -eq $previousCGO) { Remove-Item Env:CGO_ENABLED } else { $env:CGO_ENABLED = $previousCGO }
$exe = Join-Path $buildDir "workbench.exe"

function Wait-Health([int]$Attempts = 80) {
  for ($i = 0; $i -lt $Attempts; $i++) {
    try {
      $null = Invoke-RestMethod -Uri "$base/healthz" -TimeoutSec 1
      return
    } catch {
      Start-Sleep -Milliseconds 200
    }
  }
  throw "workbench did not become healthy at $base"
}

$crashed = $null
$restarted = $null
try {
  Write-Host "Phase 1: start with WORKBENCH_CRASH_POINT=after_writes"
  $env:WORKBENCH_CRASH_POINT = "after_writes"
  $serverArgs = '-config "samples/config.json" -db "' + $db + '" -addr "' + $Addr + '"'
  $crashed = Start-Process -FilePath $exe -WorkingDirectory $root -PassThru `
    -ArgumentList $serverArgs `
    -RedirectStandardOutput (Join-Path $runtime "crash-$runId.phase1.out.log") `
    -RedirectStandardError (Join-Path $runtime "crash-$runId.phase1.err.log")
  Remove-Item Env:WORKBENCH_CRASH_POINT
  Wait-Health
  $body = Get-Content -Raw -LiteralPath (Join-Path $root "samples\batches\09-sample-recovery-009.json")
  $requestFailed = $false
  try {
    $null = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $body -TimeoutSec 5
  } catch {
    $requestFailed = $true
  }
  if (-not $requestFailed) { throw "expected the process to terminate during the batch" }
  Write-Host "Request connection ended as expected."
  $deadline = (Get-Date).AddSeconds(15)
  while (-not $crashed.HasExited -and (Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 100
    $crashed.Refresh()
  }
  if (-not $crashed.HasExited) { throw "crashed process did not exit within 15 seconds" }
  $crashed.Refresh()
  Write-Host "Forced termination observed. ExitCode=$($crashed.ExitCode) (expected 71)"

  Write-Host "Phase 2: restart without crash hook"
  $restarted = Start-Process -FilePath $exe -WorkingDirectory $root -PassThru `
    -ArgumentList $serverArgs `
    -RedirectStandardOutput (Join-Path $runtime "crash-$runId.phase2.out.log") `
    -RedirectStandardError (Join-Path $runtime "crash-$runId.phase2.err.log")
  Wait-Health

  $recovery = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/recovery"
  Write-Host "Recovery report: pending=$($recovery.data.pending_intents) aborted=$($recovery.data.aborted) confirmedCommitted=$($recovery.data.confirmed_committed)"
  $recovery.data.details | ConvertTo-Json -Depth 10 | Write-Host

  $visibleBeforeRetry = $true
  try {
    $null = Invoke-RestMethod -Uri "$base/api/v1/adjudications/09-sample-recovery-009"
  } catch {
    $visibleBeforeRetry = $false
  }
  if ($visibleBeforeRetry) { throw "aborted batch is unexpectedly visible" }
  Write-Host "Decision before retry: not visible (as required)."

  $retry = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $body
  Write-Host "Retry after recovery: outcome=$($retry.data.outcome) version=$($retry.data.window_version) code=$($retry.data.code)"
  $snapshot = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/snapshot?budget_ms=40"
  Write-Host "Snapshot after retry: version=$($snapshot.data.snapshot_window_version) complete=$($snapshot.data.complete) active=$($snapshot.data.occupancy.active_count)"
  Write-Host "CRASH RECOVERY DEMO OK"
} finally {
  if ($crashed) {
    Stop-Process -Id $crashed.Id -Force -ErrorAction SilentlyContinue
  }
  if ($restarted) {
    Stop-Process -Id $restarted.Id -Force -ErrorAction SilentlyContinue
  }
}
