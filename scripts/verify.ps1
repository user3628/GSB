[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $Root

$env:GOFLAGS = '-mod=vendor'
$env:GOPROXY = 'off'

function Wait-FileContent {
    param([Parameter(Mandatory = $true)][string]$Path, [int]$TimeoutSeconds = 15)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ((Test-Path $Path) -and ((Get-Item $Path).Length -gt 0)) {
            return (Get-Content $Path -Raw)
        }
        Start-Sleep -Milliseconds 100
    }
    throw "timed out waiting for $Path"
}

function Invoke-Checked {
    param([Parameter(Mandatory = $true)][string[]]$CliArgs)
    & $script:Bin @CliArgs | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "command failed ($LASTEXITCODE): $($CliArgs -join ' ')"
    }
}

function Invoke-ExpectedBusinessCode {
    param([Parameter(Mandatory = $true)][string]$Code, [Parameter(Mandatory = $true)][string[]]$CliArgs)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $script:Bin @CliArgs 2>&1 | ForEach-Object { $_.ToString() } | Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -eq 0 -or $output -notmatch [regex]::Escape($Code)) {
        throw "expected $Code and non-zero exit for: $($CliArgs -join ' ')`nexit=$exitCode`n$output"
    }
}

Write-Host '==> go build ./...'
go build ./...
if ($LASTEXITCODE -ne 0) { throw 'go build failed' }

Write-Host '==> go test ./... -count=1'
go test ./... -count=1
if ($LASTEXITCODE -ne 0) { throw 'go test failed' }

New-Item -ItemType Directory -Force -Path (Join-Path $Root 'artifacts\bin') | Out-Null
$Bin = Join-Path $Root 'artifacts\bin\converge.exe'
& go build -o $Bin .\cmd\converge
if ($LASTEXITCODE -ne 0) { throw 'CLI build failed' }

$Work = Join-Path $Root 'artifacts\verify-windows'
if (Test-Path $Work) { Remove-Item -Recurse -Force $Work }
New-Item -ItemType Directory -Force -Path $Work | Out-Null

& $Bin gen-samples --dir (Join-Path $Root 'samples') | Out-Null

$AddrFile = Join-Path $Work 'target.addr'
$TargetOut = Join-Path $Work 'target.out'
$TargetErr = Join-Path $Work 'target.err'
$Snapshot = Join-Path $Root 'samples\snapshot-v1.json'
$targetArguments = "target --addr 127.0.0.1:0 --snapshot `"$Snapshot`" --addr-file `"$AddrFile`""
$target = Start-Process -FilePath $Bin -ArgumentList $targetArguments -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $TargetOut -RedirectStandardError $TargetErr

try {
    $Endpoint = (Wait-FileContent -Path $AddrFile).Trim()
    if ($Endpoint -notmatch '^http://127\.0\.0\.1:\d+$') {
        throw "bad endpoint: $Endpoint"
    }
    Write-Host "endpoint=$Endpoint"

    $Db = Join-Path $Work 'convergence.db'
    Invoke-Checked @('commit-full', '--db', $Db, '--file', (Join-Path $Root 'samples\snapshot-v1.json'), '--out', (Join-Path $Work 'full-commit.json'))
    Invoke-Checked @('apply', '--db', $Db, '--endpoint', $Endpoint, '--all', '--out', (Join-Path $Work 'full-apply.json'))

    Invoke-Checked @('commit-delta', '--db', $Db, '--file', (Join-Path $Root 'samples\delta-v2.json'), '--out', (Join-Path $Work 'delta-commit.json'))
    Invoke-Checked @('apply', '--db', $Db, '--endpoint', $Endpoint, '--all', '--out', (Join-Path $Work 'delta-apply.json'))

    Invoke-ExpectedBusinessCode -Code 'E_STALE_BASE' -CliArgs @(
        'commit-delta', '--db', $Db, '--file', (Join-Path $Root 'samples\delta-stale-v99.json')
    )
    Invoke-ExpectedBusinessCode -Code 'E_SPEC_INVALID' -CliArgs @(
        'commit-full', '--db', (Join-Path $Work 'invalid.db'), '--file', (Join-Path $Root 'samples\invalid-v1.json')
    )
    Invoke-ExpectedBusinessCode -Code 'E_DEPENDENCY_CYCLE' -CliArgs @(
        'commit-full', '--db', (Join-Path $Work 'cycle.db'), '--file', (Join-Path $Root 'samples\cycle-v1.json')
    )

    $CrashDb = Join-Path $Work 'crash.db'
    Invoke-Checked @('commit-full', '--db', $CrashDb, '--file', $Snapshot)
    $ReadyFile = Join-Path $Work 'crash-ready.json'
    $CrashOut = Join-Path $Work 'crash.out'
    $CrashErr = Join-Path $Work 'crash.err'
    $crashArguments = "apply --db `"$CrashDb`" --endpoint `"$Endpoint`" --batch B001-S1 --crash-after-write-intent --ready-file `"$ReadyFile`""
    $crash = Start-Process -FilePath $Bin -ArgumentList $crashArguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $CrashOut -RedirectStandardError $CrashErr
    Wait-FileContent -Path $ReadyFile | Out-Null
    Stop-Process -Id $crash.Id -Force
    Wait-Process -Id $crash.Id -ErrorAction SilentlyContinue

    $RecoveryPath = Join-Path $Work 'recovery.json'
    Invoke-Checked @('recover', '--db', $CrashDb, '--out', $RecoveryPath)
    $ReapplyPath = Join-Path $Work 'reapply.json'
    Invoke-Checked @('apply', '--db', $CrashDb, '--endpoint', $Endpoint, '--batch', 'B001-S1', '--out', $ReapplyPath)

    $fullApply = Get-Content (Join-Path $Work 'full-apply.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $deltaCommit = Get-Content (Join-Path $Work 'delta-commit.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $deltaApply = Get-Content (Join-Path $Work 'delta-apply.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $recoveryEnvelope = Get-Content $RecoveryPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $reapply = Get-Content $ReapplyPath -Raw -Encoding UTF8 | ConvertFrom-Json

    $fullBatchCount = @($fullApply.round.batch_results.PSObject.Properties).Count
    $deltaBatchCount = @($deltaApply.round.batch_results.PSObject.Properties).Count
    if (-not $fullApply.ok -or $fullApply.round.status -ne 'succeeded' -or $fullBatchCount -lt 1) { throw 'full apply failed assertions' }
    if ($deltaCommit.version -ne 2) { throw 'delta version assertion failed' }
    if (-not $deltaApply.ok -or $deltaApply.round.status -ne 'succeeded') { throw 'delta apply failed assertions' }
    if ($recoveryEnvelope.recovery.rolled_back_write_batches.Count -ne 1) { throw 'recovery rollback assertion failed' }
    if ($recoveryEnvelope.recovery.rolled_back_write_batches[0].new_status -ne 'pending') { throw 'batch was not restored to pending' }
    if ($recoveryEnvelope.recovery.exit_report_promise -ne $false) { throw 'unexpected exit report promise' }
    if (-not $reapply.ok -or $reapply.result.status -ne 'succeeded') { throw 'reapply assertion failed' }

    $report = [ordered]@{
        ok = $true
        full_batches = $fullBatchCount
        delta_batches = $deltaBatchCount
        recovery = $recoveryEnvelope.recovery
    }
    $report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 (Join-Path $Work 'verification-summary.json')
    Write-Host "VERIFY_PS1_OK $Work\verification-summary.json"
}
finally {
    if ($target -and -not $target.HasExited) {
        Stop-Process -Id $target.Id -Force -ErrorAction SilentlyContinue
    }
}
