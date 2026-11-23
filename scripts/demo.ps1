param(
  [string]$Addr = "127.0.0.1:18787"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$base = "http://$Addr"
$runId = Get-Date -Format "yyyyMMddHHmmss"
$runtime = Join-Path $root ".runtime"
$buildDir = Join-Path $root ".build"
$db = Join-Path $runtime "demo-$runId.db"
$stdout = Join-Path $runtime "demo-$runId.stdout.log"
$stderr = Join-Path $runtime "demo-$runId.stderr.log"
New-Item -ItemType Directory -Force -Path $runtime, $buildDir | Out-Null

$previousCGO = $env:CGO_ENABLED
$env:CGO_ENABLED = "0"
& go build -trimpath -o (Join-Path $buildDir "workbench.exe") ./cmd/workbench
if ($LASTEXITCODE -ne 0) { throw "go build failed" }
if ($null -eq $previousCGO) { Remove-Item Env:CGO_ENABLED } else { $env:CGO_ENABLED = $previousCGO }

$exe = Join-Path $buildDir "workbench.exe"
$process = $null

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

function Invoke-Decision([string]$Name) {
  $path = Join-Path $root "samples\batches\$Name"
  $body = Get-Content -Raw -LiteralPath $path
  $response = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $body
  $decision = $response.data
  Write-Host ("{0,-40} v{1,-3} {2,-9} {3,-24} confidence={4}" -f $decision.batch_id, $decision.window_version, $decision.outcome, $decision.code, $decision.confidence)
  $decision.items | Select-Object ordinal, request_id, result, code, offset_ms, triggered_boundary, first_occupied_at_ms, remaining_window_ms, prechecked | Format-Table -AutoSize | Out-String | Write-Host
  return $decision
}

try {
  $serverArgs = '-config "samples/config.json" -db "' + $db + '" -addr "' + $Addr + '"'
  $process = Start-Process -FilePath $exe -WorkingDirectory $root -PassThru `
    -ArgumentList $serverArgs `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  Wait-Health
  Write-Host "Workbench: $base  (db=$db)"

  $accept = Invoke-Decision "01-sample-accept-001.json"
  $boundary = Invoke-Decision "02-sample-boundary-002.json"
  $signature = Invoke-Decision "03-sample-signature-003.json"
  $replay = Invoke-Decision "04-sample-replay-004.json"
  $fast = Invoke-Decision "05-sample-fast-005.json"
  $baseReclaim = Invoke-Decision "06-sample-reclaim-base-006.json"
  $reclaim = Invoke-Decision "07-sample-reclaim-trigger-007.json"
  $reoccupy = Invoke-Decision "08-sample-reoccupy-008.json"
  $mode = Invoke-Decision "10-sample-mode-full-010.json"

  $idem = Invoke-Decision "01-sample-accept-001.json"
  Write-Host "Idempotent replay: $($idem.idempotent_replay), reused window version $($idem.window_version)"

  $invalidBody = Get-Content -Raw -LiteralPath (Join-Path $root "samples\batches\11-sample-invalid-nonce.json")
  $invalidStatus = 0
  try {
    $null = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $invalidBody
  } catch {
    $invalidStatus = $_.Exception.Response.StatusCode.value__
  }
  if ($invalidStatus -ne 400) { throw "invalid batch status=$invalidStatus, expected 400" }
  Write-Host "Invalid batch rejected: 400 E_REQUEST_INVALID"
  $overflowBody = Get-Content -Raw -LiteralPath (Join-Path $root "samples\batches\invalid-weight-overflow.json")
  $overflowStatus = 0
  try {
    $null = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $overflowBody
  } catch {
    $overflowStatus = $_.Exception.Response.StatusCode.value__
  }
  if ($overflowStatus -ne 400) { throw "weight overflow status=$overflowStatus, expected 400" }
  Write-Host "Weight overflow rejected: 400 E_REQUEST_INVALID"
  $conflictBody = Get-Content -Raw -LiteralPath (Join-Path $root "samples\batches\conflict-same-id-001.json")
  $conflictStatus = 0
  try {
    $null = Invoke-RestMethod -Method Post -Uri "$base/api/v1/adjudications" -ContentType "application/json" -Body $conflictBody
  } catch {
    $conflictStatus = $_.Exception.Response.StatusCode.value__
  }
  if ($conflictStatus -ne 409) { throw "conflict status=$conflictStatus, expected 409" }
  Write-Host "Conflicting batch rejected: 409 E_CONFLICTING_REQUEST"

  $modeDifference = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/mode-differences?batch_id=$($mode.batch_id)&budget_ms=40"
  Write-Host "Mode differences: $($modeDifference.data.differences); predicted fast outcome=$($modeDifference.data.predicted_fast_outcome)"
  $modeDifference.data.items | Select-Object ordinal, request_id, full_result, full_code, fast_result, fast_code, differs | Format-Table -AutoSize | Out-String | Write-Host

  $viewBody = @{
    batch_id = $baseReclaim.batch_id
    budget_ms = 40
    views = @(
      @{ client_id = "reader-old"; picked_window_version = $baseReclaim.window_version },
      @{ client_id = "reader-new"; picked_window_version = $reoccupy.window_version }
    )
  } | ConvertTo-Json -Depth 10
  $inconsistency = Invoke-RestMethod -Method Post -Uri "$base/api/v1/diagnostics/inconsistencies" -ContentType "application/json" -Body $viewBody
  Write-Host "Inconsistency differs=$($inconsistency.data.differs)"
  $inconsistency.data.views | Select-Object client_id, picked_window_version, decision_batch_code, viewed_batch_code, differs | Format-Table -AutoSize | Out-String | Write-Host

  $snapshot = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/snapshot?budget_ms=40"
  Write-Host "Snapshot: version=$($snapshot.data.snapshot_window_version) complete=$($snapshot.data.complete) sections=$($snapshot.data.completed_sections -join ',')"
  $partial = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/snapshot?budget_ms=0"
  Write-Host "Zero-budget snapshot: partial=$($partial.data.partial) completed=$($partial.data.completed_sections -join ',') reason=$($partial.data.reason)"

  $recovery = Invoke-RestMethod -Uri "$base/api/v1/diagnostics/recovery"
  Write-Host "Startup recovery: pending=$($recovery.data.pending_intents) aborted=$($recovery.data.aborted)"
  Write-Host "DEMO OK"
} finally {
  if ($process) {
    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
  }
}
