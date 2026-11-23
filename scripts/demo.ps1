$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Tmp = Join-Path $env:TEMP ("upool-demo-{0}" -f ([guid]::NewGuid().ToString('N')))
New-Item -ItemType Directory -Path $Tmp | Out-Null
$Bin = Join-Path $Tmp 'poolctl.exe'
$Http = 'http://127.0.0.1:18700'
$CrashHttp = 'http://127.0.0.1:18710'
$Processes = @()

function Wait-Health([string]$BaseUrl) {
    for ($i = 0; $i -lt 200; $i++) {
        try {
            Invoke-RestMethod -Uri "$BaseUrl/healthz" -TimeoutSec 1 | Out-Null
            return
        } catch { Start-Sleep -Milliseconds 50 }
    }
    throw "service did not become healthy: $BaseUrl"
}
function Submit-Track([string]$BaseUrl, [string]$Path, [string]$Out) {
    $body = [IO.File]::ReadAllText((Join-Path $Root $Path))
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/tracks" -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 100 | Set-Content -Encoding utf8 $Out
}
function Start-Pool([string]$Arguments, [string]$Log) {
    $p = Start-Process -FilePath $Bin -ArgumentList $Arguments -RedirectStandardOutput $Log -RedirectStandardError "$Log.err" -PassThru -WindowStyle Hidden
    $script:Processes += $p
    return $p
}

try {
    Push-Location $Root
    go build -o $Bin ./cmd/poolctl
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }

    Start-Pool 'upstream -listen 127.0.0.1:18701' (Join-Path $Tmp 'upstream-18701.log') | Out-Null
    Start-Pool 'upstream -listen 127.0.0.1:18702' (Join-Path $Tmp 'upstream-18702.log') | Out-Null
    Start-Pool "serve -config samples/lab.json -db $Tmp\workbench.db" (Join-Path $Tmp 'workbench.log') | Out-Null
    Wait-Health $Http

    Submit-Track $Http 'samples/track-full.json' (Join-Path $Tmp 'full.json')
    Submit-Track $Http 'samples/track-segment-1.json' (Join-Path $Tmp 'seg1.json')
    Submit-Track $Http 'samples/track-segment-2.json' (Join-Path $Tmp 'seg2.json')
    $full = (Get-Content (Join-Path $Tmp 'full.json') -Raw | ConvertFrom-Json).snapshot
    $seg = (Get-Content (Join-Path $Tmp 'seg2.json') -Raw | ConvertFrom-Json).snapshot
    $full.track_id = ''; $seg.track_id = ''
    if (($full | ConvertTo-Json -Depth 100) -ne ($seg | ConvertTo-Json -Depth 100)) { throw 'whole/segmented snapshots differ' }
    Write-Host 'PASS snapshot equality: whole == segmented'

    Submit-Track $Http 'samples/track-exhaustion.json' (Join-Path $Tmp 'exhaustion.json')
    Submit-Track $Http 'samples/track-dial-failure.json' (Join-Path $Tmp 'dial.json')
    $ex = Get-Content (Join-Path $Tmp 'exhaustion.json') -Raw | ConvertFrom-Json
    $wait = $ex.requests | Where-Object request_id -eq 'wait-1'
    if ($wait.error.code -ne 'E_POOL_ACQUIRE_TIMEOUT' -or $wait.error.details.waited_ms -ne 120) { throw 'queue timeout mismatch' }
    $dial = (Get-Content (Join-Path $Tmp 'dial.json') -Raw | ConvertFrom-Json).requests[0]
    if ($dial.error.code -ne 'E_DIAL_FAILED') { throw 'dial failure mismatch' }
    Write-Host 'PASS queue timeout, dial failure'

    $env:DIAG_CRASH_AFTER_PLAN_INTENT_MS = '30000'
    $crash = Start-Pool "serve -config samples/lab.json -listen 127.0.0.1:18710 -db $Tmp\crash.db" (Join-Path $Tmp 'crash-first.log')
    Remove-Item Env:DIAG_CRASH_AFTER_PLAN_INTENT_MS
    Wait-Health $CrashHttp
    $body = [IO.File]::ReadAllText((Join-Path $Root 'samples/track-crash-segment-1.json'))
    $job = Start-Job -ScriptBlock { param($u,$b) try { Invoke-RestMethod -Method Post -Uri "$u/v1/tracks" -ContentType 'application/json' -Body $b } catch {} } -ArgumentList $CrashHttp,$body
    for ($i=0; $i -lt 200; $i++) { if (Select-String -Quiet -Path (Join-Path $Tmp 'crash-first.log') -Pattern 'DIAG_PLAN_INTENT_READY') { break }; Start-Sleep -Milliseconds 50 }
    if (-not (Select-String -Quiet -Path (Join-Path $Tmp 'crash-first.log') -Pattern 'DIAG_PLAN_INTENT_READY')) { throw 'crash pause was not reached' }
    Stop-Process -Id $crash.Id -Force
    Wait-Job $job -Timeout 5 | Out-Null; Remove-Job $job -Force

    Start-Pool "serve -config samples/lab.json -listen 127.0.0.1:18710 -db $Tmp\crash.db" (Join-Path $Tmp 'crash-second.log') | Out-Null
    Wait-Health $CrashHttp
    $rec = Invoke-RestMethod -Uri "$CrashHttp/v1/recovery"
    if ($rec.recovery.aborted_intents.Count -lt 1 -or $rec.recovery.resumed_batches.Count -lt 1) { throw 'recovery report did not show aborted/resumed work' }
    Submit-Track $CrashHttp 'samples/track-crash-segment-2.json' (Join-Path $Tmp 'crash-final.json')
    $resumed = (Get-Content (Join-Path $Tmp 'crash-final.json') -Raw | ConvertFrom-Json).snapshot
    $baseline = (Get-Content (Join-Path $Tmp 'full.json') -Raw | ConvertFrom-Json).snapshot
    $resumed.track_id = ''; $baseline.track_id = ''
    if (($resumed | ConvertTo-Json -Depth 100) -ne ($baseline | ConvertTo-Json -Depth 100)) { throw 'resumed snapshot differs from uninterrupted baseline' }
    Write-Host 'PASS forced termination + recovery continuation equality'
    & $Bin inspect -db (Join-Path $Tmp 'workbench.db')
    Write-Host "demo artifacts: $Tmp"
} finally {
    foreach ($p in $Processes) { if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } }
    Pop-Location
}

