param(
    [int]$Port = 18181,
    [string]$GoBin = "go"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Base = "http://127.0.0.1:$Port"
$Out = Join-Path $Root "artifacts\demo"
$Db = Join-Path $Out "releasegate-demo.db"
$Bin = Join-Path $Out "releasegate-demo.exe"
New-Item -ItemType Directory -Force -Path $Out | Out-Null
Get-ChildItem $Out -File -ErrorAction SilentlyContinue | Remove-Item -Force

function Wait-Health {
    for ($i = 0; $i -lt 100; $i++) {
        try {
            $health = Invoke-RestMethod -Method Get -Uri "$Base/healthz"
            if ($health.service -ne "releasegate") { throw "unexpected service on port" }
            return
        } catch { Start-Sleep -Milliseconds 50 }
    }
    throw "server did not become healthy"
}

function Start-GateServer([string]$LogName) {
    $old = $env:RELEASEGATE_ENABLE_FAULT_INJECTION
    $env:RELEASEGATE_ENABLE_FAULT_INJECTION = "1"
    $p = Start-Process -FilePath $Bin -ArgumentList @("-listen", "127.0.0.1:$Port", "-db", $Db) `
        -RedirectStandardOutput (Join-Path $Out $LogName) -RedirectStandardError (Join-Path $Out ($LogName + ".err")) -PassThru
    $env:RELEASEGATE_ENABLE_FAULT_INJECTION = $old
    Wait-Health
    return $p
}

function Invoke-Json([string]$Method, [string]$Path, $Body) {
    $params = @{ Method = $Method; Uri = "$Base$Path"; ContentType = "application/json" }
    if ($null -ne $Body) { $params.Body = ($Body | ConvertTo-Json -Depth 100 -Compress) }
    return Invoke-RestMethod @params
}

Write-Host "== fixture generation =="
& $GoBin run ./cmd/fixturegen -out testdata -seed 20261124
Write-Host "== build =="
& $GoBin build -o $Bin ./cmd/releasegate

$server = Start-GateServer "server-first.log"
try {
    $created = Invoke-Json "POST" "/v1/releases" (Get-Content testdata/release.json -Raw | ConvertFrom-Json)
    $started = Invoke-Json "POST" "/v1/releases/demo-progressive-release/start" $null
    $stage1 = Invoke-Json "POST" "/v1/releases/demo-progressive-release/observations" (Get-Content testdata/observations-stage-1.json -Raw | ConvertFrom-Json)
    $stage1Stats = Invoke-Json "GET" "/v1/releases/demo-progressive-release/stats?version=1&scope=stage&stage=1" $null
    $contract = Invoke-Json "GET" "/v1/releases/demo-progressive-release/error-contract?version=1&scope=stage&stage=1" $null
    $gate1 = Invoke-Json "POST" "/v1/releases/demo-progressive-release/gate" @{ stats_version = 1 }
    if ($gate1.decision.outcome -ne "PROMOTE") { throw "expected PROMOTE" }
    $stage2 = Invoke-Json "POST" "/v1/releases/demo-progressive-release/observations" (Get-Content testdata/observations-stage-2.json -Raw | ConvertFrom-Json)
    try {
        Invoke-Json "POST" "/v1/releases/demo-progressive-release/gate" @{ stats_version = 2 } | Out-Null
        throw "expected gate failure"
    } catch {
        $body = $_.ErrorDetails.Message | ConvertFrom-Json
        if ($body.error.code -ne "E_GATE_FAILED") { throw }
        $gate2 = $body
    }
    $paused = Invoke-Json "GET" "/v1/releases/demo-progressive-release" $null

    try {
        Invoke-Json "POST" "/v1/releases/demo-progressive-release/faults/terminate-after-intent" @{ kind = "TRANSITION" } | Out-Null
    } catch { }
    $server.WaitForExit()
    if ($server.ExitCode -ne 70) { throw "expected forced exit 70, got $($server.ExitCode)" }
} finally {
    if (-not $server.HasExited) { Stop-Process -Id $server.Id -Force }
}

$server = Start-GateServer "server-restart.log"
try {
    $recovery = Invoke-Json "GET" "/v1/releases/demo-progressive-release/recovery-report" $null
    if ($recovery.pending_intents_found -ne 1 -or $recovery.intents_recovered -ne 1) { throw "recovery mismatch" }
    $recovered = Invoke-Json "GET" "/v1/releases/demo-progressive-release" $null
    if ($recovered.state -ne "OBSERVING") { throw "recovered state mismatch" }
    $rolledBack = Invoke-Json "POST" "/v1/releases/demo-progressive-release/transitions" @{ to = "ROLLED_BACK"; reason = "operator confirmed rollback after recovered pause" }
    if ($rolledBack.state -ne "ROLLED_BACK" -or $rolledBack.traffic_per_mille -ne 0) { throw "rollback mismatch" }
    $decisions = Invoke-Json "GET" "/v1/releases/demo-progressive-release/decisions" $null

    $result = [ordered]@{
        release_created = $created; started = $started; stage1_ingest = $stage1; stage1_stats = $stage1Stats
        stage1_error_contract = $contract; gate1 = $gate1; stage2_ingest = $stage2; gate2_failure = $gate2
        paused_release = $paused; recovery_report = $recovery; recovered_release = $recovered
        rolled_back = $rolledBack; decisions = $decisions
    }
    $result | ConvertTo-Json -Depth 100 | Set-Content (Join-Path $Out "demo-output.json") -Encoding UTF8
    Write-Host "gate1=PROMOTE"
    Write-Host "gate2=E_GATE_FAILED"
    Write-Host "recovered=OBSERVING"
    Write-Host "final=ROLLED_BACK"
    Write-Host "output=$(Join-Path $Out 'demo-output.json')"
} finally {
    if (-not $server.HasExited) { Stop-Process -Id $server.Id -Force }
}
